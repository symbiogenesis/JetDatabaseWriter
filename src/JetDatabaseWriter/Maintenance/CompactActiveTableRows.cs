namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>Holds independently agreed active observations while preserving every unassigned physical row.</summary>
/// <remarks>
/// The caller keeps the exact source and definition immutable throughout this disposable scope.
/// A native table map alone never selects rows: the complete supported covering tree, exact stored keys,
/// every mapped row and declared count must agree. Unmapped readable same-owner rows are delivered as
/// opaque pinned observations during derivation and every consumption. This grants no incoming-reference,
/// global-discovery, reclamation or complete-database proof. Caller retention and shared pool overhead are separate.
/// </remarks>
internal sealed partial class CompactActiveTableRows : IDisposable
{
    private readonly IPageSource source;
    private readonly JetFormat format;
    private readonly TDefImage definition;
    private readonly CompactActiveTableBudget budget;
    private readonly CompactDiscoveryAccounting accounting;
    private readonly EvidencePages pages;
    private readonly Func<CompactActiveTableRowRecord, CancellationToken, ValueTask> observer;
    private readonly Dictionary<(long Page, int Row), RowEvidence> rows = [];
    private readonly HashSet<long> mappedPages = [];
    private readonly List<IDisposable> storage = [];
    private readonly long sourcePageCount;
    private readonly int descriptorIndex;
    private int disposed;
    private int storageReleased;
    private int activeScan;
    private int activeIndexAudit;
    private bool failed;

    private CompactActiveTableRows(IPageSource source, JetFormat format, long owner, TDefImage definition, int descriptorIndex, CompactActiveTableBudget budget, Func<CompactActiveTableRowRecord, CancellationToken, ValueTask> observer)
    {
        this.source = source;
        this.format = format;
        this.OwnerTDefPage = owner;
        this.definition = definition;
        this.descriptorIndex = descriptorIndex;
        this.budget = budget;
        this.observer = observer;
        this.sourcePageCount = source.PageCount;
        this.accounting = new(budget.Work, budget.Accounting);
        this.pages = new(this);
    }

    /// <summary>Gets the exact original table owner covered by this evidence.</summary>
    internal long OwnerTDefPage { get; }

    /// <summary>Gets successful scoped counts; no global proof is granted.</summary>
    internal CompactActiveTableResult Result { get; private set; } = new(0, 0, 0);

    /// <summary>Derives disposable source-bound active observations from independent native evidence.</summary>
    /// <param name="source">The exact immutable decrypted source, without discovery counter wrappers.</param>
    /// <param name="format">The verified source format.</param>
    /// <param name="ownerTDefPage">An ordinary original table owner; catalog recovery is separate.</param>
    /// <param name="definition">The exact immutable original definition object.</param>
    /// <param name="coveringIndexOrdinal">The explicitly selected supported physical covering index.</param>
    /// <param name="budget">Lifetime cumulative admission limits.</param>
    /// <param name="observer">Mandatory awaited observer for all unassigned original bytes and terminal counts.</param>
    /// <param name="cancellationToken">Cancels native inspection and observer delivery.</param>
    /// <returns>Evidence valid only for this source and image until disposed or a consumption fails.</returns>
    /// <exception cref="JetCorruptDataException">Independent source, metadata, map, tree or row evidence disagrees.</exception>
    /// <exception cref="JetNotSupportedException">The explicitly selected index cannot be derived faithfully.</exception>
    /// <exception cref="JetLimitationException">Lifetime work or simultaneous storage admission is exhausted.</exception>
    internal static async ValueTask<CompactActiveTableRows> ValidateAsync(IPageSource source, JetFormat format, long ownerTDefPage, TDefImage definition, int coveringIndexOrdinal, CompactActiveTableBudget budget, Func<CompactActiveTableRowRecord, CancellationToken, ValueTask> observer, CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.Work, nameof(budget));
        Guard.NotNull(observer, nameof(observer));
        Guard.InRange(budget.MaxActiveRows, 0L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxKeyBytes, nameof(budget));
        source.ThrowIfDisposedOrCancelled(cancellationToken);
        if (ownerTDefPage <= 2 || ownerTDefPage >= source.PageCount || definition.TDefPageNumber != ownerTDefPage
            || source.PageSize != format.PageSize || source.PageCount < 3)
        {
            throw Corrupt(ownerTDefPage, "Active row evidence requires the exact original ordinary table owner and source geometry.");
        }

        if (source.PageCount > budget.Work.MaxPages || definition.LogicalByteLength > budget.Work.MaxDefinitionBytes
            || definition.Header.Counters.RowCount > budget.MaxActiveRows)
        {
            throw Limit(ownerTDefPage, "Active table source, definition or declared rows exceed admission limits.");
        }

        if (!CompactIndexRowAudit.SupportsDescriptor(definition, coveringIndexOrdinal))
        {
            throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, "Active membership requires a supported faithful scalar covering index.");
        }

        CompactActiveTableRows? pending = new(source, format, ownerTDefPage, definition, coveringIndexOrdinal, budget, observer);
        try
        {
            await pending.DeriveAsync(cancellationToken).ConfigureAwait(false);
            CompactActiveTableRows result = pending;
            pending = null;
            return result;
        }
        finally
        {
            pending?.Dispose();
        }
    }

    /// <summary>Requires the same exact live source and immutable original image used to derive this authority.</summary>
    /// <param name="source">The original source object.</param>
    /// <param name="definition">The original image object.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetCorruptDataException">The source, image or fixed geometry differs.</exception>
    /// <exception cref="JetOperationException">A failed scan has expired this evidence.</exception>
    /// <exception cref="ObjectDisposedException">This disposable evidence has expired.</exception>
    internal void CheckSource(IPageSource source, TDefImage definition, CancellationToken cancellationToken)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(Volatile.Read(ref this.disposed) != 0, this);
#else
        if (Volatile.Read(ref this.disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(CompactActiveTableRows));
        }
#endif

        if (this.failed)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "A failed active-table scan has expired its source-bound evidence.");
        }

        this.source.ThrowIfDisposedOrCancelled(cancellationToken);
        if (!ReferenceEquals(this.source, source) || !ReferenceEquals(this.definition, definition)
            || source.PageSize != this.format.PageSize || source.PageCount != this.sourcePageCount
            || definition.TDefPageNumber != this.OwnerTDefPage)
        {
            throw Corrupt(this.OwnerTDefPage, "The active-table evidence does not belong to this exact source, image and fixed geometry.");
        }
    }

    /// <summary>Rechecks every physical row, streams active rows, and replays all unassigned pinned originals.</summary>
    /// <param name="activeVisitor">Consumes borrowed active payloads only after exact evidence comparison.</param>
    /// <param name="cancellationToken">Cancels scanning and observer delivery.</param>
    /// <returns>A task completing only after every original physical observation is accounted for.</returns>
    /// <exception cref="JetCorruptDataException">Any original row coordinate or complete payload changed.</exception>
    /// <exception cref="JetOperationException">Another scan is active or a prior scan expired the evidence.</exception>
    internal async ValueTask ScanRowsAsync(Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> activeVisitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(activeVisitor, nameof(activeVisitor));
        this.CheckSource(this.source, this.definition, cancellationToken);
        if (Interlocked.CompareExchange(ref this.activeScan, 1, 0) != 0)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "An active-table evidence scan is already running.");
        }

        try
        {
            await this.ScanRowsCoreAsync(activeVisitor, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
            if (Volatile.Read(ref this.disposed) != 0)
            {
                this.ReleaseStorage();
            }
        }
    }

    /// <summary>Independently compares every physical stored index with the agreed active rows.</summary>
    /// <param name="cancellationToken">Cancels repeated row scans, key derivation and strict tree/bookmark inspection.</param>
    /// <returns>The number of completely compared physical descriptors.</returns>
    /// <exception cref="JetNotSupportedException">Any descriptor lacks a faithful supported key encoding.</exception>
    /// <exception cref="JetCorruptDataException">A stored key, bookmark, exact separator, routing or active-row coverage disagrees.</exception>
    /// <exception cref="JetLimitationException">Lifetime work or simultaneous storage admission is exhausted.</exception>
    /// <exception cref="JetOperationException">Another scan or index audit is active, or a prior operation expired this evidence.</exception>
    internal async ValueTask<int> ValidateIndexesAsync(CancellationToken cancellationToken)
    {
        this.CheckSource(this.source, this.definition, cancellationToken);
        if (Interlocked.CompareExchange(ref this.activeScan, 1, 0) != 0)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "An active-table scan or index audit is already running.");
        }

        Volatile.Write(ref this.activeIndexAudit, 1);
        try
        {
            for (int ordinal = 0; ordinal < this.definition.RealIndexes.Count; ordinal++)
            {
                this.accounting.Charge(CompactDiscoveryCharge.Objects, 1, this.OwnerTDefPage);
                _ = await CompactIndexRowAudit.ValidateAsync(
                    this.source,
                    this.format,
                    this.OwnerTDefPage,
                    this.definition,
                    ordinal,
                    this.RowBudget(),
                    this.IndexBudget(),
                    new()
                    {
                        Accounting = this.accounting,
                        MaxRows = this.budget.MaxActiveRows,
                        MaxKeyBytes = this.budget.MaxKeyBytes,
                        MaxWorkingBytes = this.budget.Work.MaxWorkingBytes,
                    },
                    cancellationToken,
                    this).ConfigureAwait(false);
            }

            this.CheckSource(this.source, this.definition, cancellationToken);
            return this.definition.RealIndexes.Count;
        }
        catch
        {
            this.failed = true;
            throw;
        }
        finally
        {
            Volatile.Write(ref this.activeIndexAudit, 0);
            Volatile.Write(ref this.activeScan, 0);
            if (Volatile.Read(ref this.disposed) != 0)
            {
                this.ReleaseStorage();
            }
        }
    }

    /// <summary>Rechecks complete physical rows within the already serialized all-index comparison.</summary>
    /// <param name="visitor">Consumes independently compared active payloads.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>A task completing after all original physical observations are accounted for.</returns>
    /// <exception cref="JetOperationException">No serialized index comparison owns this row consumption.</exception>
    internal ValueTask ScanIndexRowsAsync(Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(visitor, nameof(visitor));
        this.CheckSource(this.source, this.definition, cancellationToken);
        if (Volatile.Read(ref this.activeIndexAudit) == 0)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "Active index rows require an enclosing serialized index audit.");
        }

        return this.ScanRowsCoreAsync(visitor, cancellationToken);
    }

    /// <summary>Gets the lifetime-admitted page source after checking exact active-membership identity.</summary>
    /// <param name="source">The exact original source.</param>
    /// <param name="definition">The exact original definition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The private admitted page source for strict stored-index comparison.</returns>
    internal IPageSource IndexAuditPages(IPageSource source, TDefImage definition, CancellationToken cancellationToken)
    {
        this.CheckSource(source, definition, cancellationToken);
        return this.pages;
    }

    /// <summary>Expires this evidence and releases all retained admission, leaving the caller's source open.</summary>
    public void Dispose()
    {
        _ = Interlocked.Exchange(ref this.disposed, 1);
        if (Volatile.Read(ref this.activeScan) == 0)
        {
            this.ReleaseStorage();
        }
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptIndex, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private async ValueTask ScanRowsCoreAsync(Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> activeVisitor, CancellationToken cancellationToken)
    {
        try
        {
            this.accounting.Charge(CompactDiscoveryCharge.Objects, 1, this.OwnerTDefPage);
            using IDisposable scratch = this.accounting.ReserveWorkingBytes(65_536, this.OwnerTDefPage);
            long count = 0;
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            await CompactRawRowReader.ScanAsync(this.pages, this.format, this.OwnerTDefPage, this.RowBudget(), InspectAsync, cancellationToken).ConfigureAwait(false);
            this.CheckSource(this.source, this.definition, cancellationToken);
            if (count != this.Result.PhysicalRows)
            {
                throw Corrupt(this.OwnerTDefPage, "An original physical row observation disappeared during active-table consumption.");
            }

            await this.EmitAsync(new() { Kind = "active-membership-terminal", OwnerTDefPage = this.OwnerTDefPage, Result = this.Result }, cancellationToken).ConfigureAwait(false);

            async ValueTask InspectAsync(RowLocation location, ReadOnlyMemory<byte> payload, CancellationToken token)
            {
                this.accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, location.PageNumber);
                hash.AppendData(payload.Span);
                if (!this.rows.TryGetValue((location.PageNumber, location.RowIndex), out RowEvidence? evidence)
                    || evidence.Location != location || !hash.GetHashAndReset().AsSpan().SequenceEqual(evidence.Hash))
                {
                    throw Corrupt(location.PageNumber, "A physical row changed after independently derived active-table evidence.");
                }

                count++;
                if (evidence.Active)
                {
                    await activeVisitor(location, payload, token).ConfigureAwait(false);
                }
                else
                {
                    await this.EmitUnassignedAsync(location, payload, token).ConfigureAwait(false);
                }
            }
        }
        catch
        {
            this.failed = true;
            throw;
        }
    }

    private async ValueTask DeriveAsync(CancellationToken token)
    {
        this.accounting.Charge(CompactDiscoveryCharge.Objects, 1, this.OwnerTDefPage);
        long scratch = (16L * this.definition.LogicalByteLength) + (64L * this.format.PageSize) + (32L * this.budget.MaxKeyBytes) + 65_536;
        using IDisposable reservation = this.accounting.ReserveWorkingBytes(scratch, this.OwnerTDefPage);
        this.accounting.Charge(CompactDiscoveryCharge.SchemaBytes, this.definition.LogicalByteLength, this.OwnerTDefPage);
        using (var reader = new TableDefReader(this.pages, this.format, cacheResults: false, this.definition.LogicalByteLength))
        {
            TDefImage? reopened = await reader.ReadImageAsync(this.OwnerTDefPage, token).ConfigureAwait(false);
            if (reopened is null || !CompactDatabasePlanValidator.OriginalImageMatches(reopened, this.definition))
            {
                throw Corrupt(this.OwnerTDefPage, "The active-table definition bytes or parsed native metadata disagree with the independently reopened source.");
            }
        }

        await this.ReadOwnedMapAsync(token).ConfigureAwait(false);
        long active = 0;
        long physical = 0;
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        await CompactRawRowReader.ScanAsync(this.pages, this.format, this.OwnerTDefPage, this.RowBudget(), InspectRowAsync, token).ConfigureAwait(false);
        if (active != this.definition.Header.Counters.RowCount)
        {
            throw Corrupt(this.OwnerTDefPage, "Independently mapped live rows disagree with the declared table count.");
        }

        long indexed = 0;
        bool terminal = false;
        long root = this.definition.RealIndexes[this.descriptorIndex].RootPage;
        this.accounting.Charge(CompactDiscoveryCharge.Objects, 1, root);
        await CompactIndexBookmarkAudit.ScanAsync(
            new MappedBookmarkPages(this),
            this.format,
            this.OwnerTDefPage,
            root,
            this.IndexBudget(),
            new()
            {
                Accounting = this.accounting,
                MaxPageReads = this.budget.Work.MaxPageReads,
                MaxDirectorySlots = this.budget.Work.MaxDirectorySlots,
                MaxWorkingBytes = this.budget.Work.MaxWorkingBytes,
            },
            InspectIndexAsync,
            token).ConfigureAwait(false);
        foreach (RowEvidence row in this.rows.Values)
        {
            token.ThrowIfCancellationRequested();
            this.accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, row.Location.PageNumber);
            if (row.Active && !row.Indexed)
            {
                throw Corrupt(row.Location.PageNumber, "A mapped active row is absent from the independently covering index.");
            }
        }

        if (!terminal || indexed != active)
        {
            throw Corrupt(root, "The independent covering index did not prove exact active-row membership and count.");
        }

        this.Result = new(active, physical - active, physical);
        await this.EmitAsync(new() { Kind = "active-membership-terminal", OwnerTDefPage = this.OwnerTDefPage, Result = this.Result }, token).ConfigureAwait(false);
        this.CheckSource(this.source, this.definition, token);

        async ValueTask InspectRowAsync(RowLocation location, ReadOnlyMemory<byte> payload, CancellationToken rowToken)
        {
            bool isActive = this.mappedPages.Contains(location.PageNumber);
            byte[] key = [];
            if (isActive)
            {
                if (active >= this.budget.MaxActiveRows || active >= this.definition.Header.Counters.RowCount)
                {
                    throw Corrupt(location.PageNumber, "Mapped live rows exceed the independently declared active count.");
                }

                if (!this.mappedPages.Contains(location.DataPageNumber))
                {
                    throw Corrupt(location.DataPageNumber, "An active moved row resolves outside the independently inspected native table map.");
                }

                bool allNull;
                (key, allNull) = await CompactIndexRowAudit.ReadKeyAsync(this.format, this.definition, this.descriptorIndex, location, payload, this.budget.MaxKeyBytes, this.accounting, rowToken).ConfigureAwait(false);
                if (allNull && (this.definition.RealIndexes[this.descriptorIndex].Slot.Flags & Constants.TableDefinition.IgnoreNullsIndexFlag) != 0)
                {
                    throw Corrupt(location.PageNumber, "The selected index omits an all-null active row and cannot cover this table.");
                }

                active++;
            }

            this.Retain(384L + key.Length, location.PageNumber);
            hash.AppendData(payload.Span);
            if (!this.rows.TryAdd((location.PageNumber, location.RowIndex), new(location, key, hash.GetHashAndReset(), isActive)))
            {
                throw Corrupt(location.PageNumber, "Physical row discovery repeated a header bookmark.");
            }

            physical++;
            if (!isActive)
            {
                await this.EmitUnassignedAsync(location, payload, rowToken).ConfigureAwait(false);
            }
        }

        ValueTask InspectIndexAsync(CompactIndexRecord record, CancellationToken indexToken)
        {
            indexToken.ThrowIfCancellationRequested();
            if (record.Kind == "leaf-entry")
            {
                this.accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, record.PageNumber);
                if (!this.rows.TryGetValue((record.TargetPageNumber!.Value, record.TargetRowIndex!.Value), out RowEvidence? row)
                    || !row.Active || row.Indexed || !record.CanonicalKey!.AsSpan().SequenceEqual(row.Key)
                    || record.BookmarkDataPageNumber != row.Location.DataPageNumber || record.BookmarkDataRowIndex != row.Location.DataRowIndex)
                {
                    throw Corrupt(record.PageNumber, "A covering leaf key or bookmark disagrees with the independently mapped scalar row, or repeats a target.");
                }

                row.Indexed = true;
                indexed++;
            }
            else if (record.Kind == "index-terminal")
            {
                terminal = record.NavigableGraphAcyclic && record.EntryEdgeFencesValidated && record.AllObservedEdgeFencesValidated
                    && record.ObservedLeafChainOrdered && record.ObservedKeyRoutingValidated
                    && record.ObservedChildMaximumSeparatorsValidated && record.BookmarkTargetsValidated;
                if (!terminal)
                {
                    throw Corrupt(record.PageNumber, "The selected covering index lacks independently validated graph, chain, routing, exact separators or bookmark targets.");
                }
            }

            return default;
        }
    }

    private CompactRawRowBudget RowBudget() => new()
    {
        Accounting = this.accounting,
        MaxPages = this.budget.Work.MaxPages,
        MaxPageReads = this.budget.Work.MaxPageReads,
        MaxRows = this.budget.Work.MaxRows,
        MaxDirectorySlots = this.budget.Work.MaxDirectorySlots,
        MaxOverflowTargets = this.budget.Work.MaxOverflowTargets,
        MaxWorkingBytes = this.budget.Work.MaxWorkingBytes,
    };

    private CompactIndexBudget IndexBudget() => new()
    {
        Accounting = this.accounting,
        MaxTreePages = checked((int)Math.Min(int.MaxValue, this.budget.Work.MaxIndexPages)),
        MaxEntries = this.budget.Work.MaxIndexEntries,
        MaxCanonicalBytes = this.budget.Work.MaxIndexCanonicalBytes,
        MaxRetainedKeyBytes = this.budget.Work.MaxWorkingBytes,
        MaxReferences = this.budget.Work.MaxReferences,
        MaxGraphSteps = this.budget.Work.MaxGraphSteps,
        MaxRecords = this.budget.Work.MaxRecords,
        MaxWorkingBytes = this.budget.Work.MaxWorkingBytes,
    };

    private void Retain(long bytes, long page)
    {
        IDisposable? reservation = null;
        try
        {
            reservation = this.accounting.ReserveWorkingBytes(bytes, page);
            this.storage.Add(reservation);
            reservation = null;
        }
        finally
        {
            reservation?.Dispose();
        }
    }

    private void ReleaseStorage()
    {
        if (Interlocked.Exchange(ref this.storageReleased, 1) == 0)
        {
            foreach (IDisposable reservation in this.storage)
            {
                reservation.Dispose();
            }

            this.storage.Clear();
            this.rows.Clear();
            this.mappedPages.Clear();
        }
    }

    private ValueTask EmitUnassignedAsync(RowLocation location, ReadOnlyMemory<byte> payload, CancellationToken token)
        => this.EmitAsync(new() { Kind = "unassigned-row", OwnerTDefPage = this.OwnerTDefPage, Location = location, RawBytes = payload }, token);

    private async ValueTask EmitAsync(CompactActiveTableRowRecord record, CancellationToken token)
    {
        this.CheckSource(this.source, this.definition, token);
        this.accounting.Charge(CompactDiscoveryCharge.Records, 1, record.Location?.PageNumber ?? this.OwnerTDefPage);
        await this.observer(record, token).ConfigureAwait(false);
        this.CheckSource(this.source, this.definition, token);
    }

    private sealed class RowEvidence(RowLocation location, byte[] key, byte[] hash, bool active)
    {
        internal RowLocation Location { get; } = location;

        internal byte[] Key { get; } = key;

        internal byte[] Hash { get; } = hash;

        internal bool Active { get; } = active;

        internal bool Indexed { get; set; }
    }

    private sealed class EvidencePages(CompactActiveTableRows owner) : IPageSource
    {
        public int PageSize => owner.format.PageSize;

        public long PageCount => owner.sourcePageCount;

        public bool IsDisposed => owner.source.IsDisposed || Volatile.Read(ref owner.disposed) != 0;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            owner.CheckSource(owner.source, owner.definition, cancellationToken);
            if (pageNumber < 0 || pageNumber >= this.PageCount)
            {
                throw Corrupt(pageNumber, "An active-table physical reference lies outside the immutable source.");
            }

            owner.accounting.Charge(CompactDiscoveryCharge.PageReads, 1, pageNumber);
            byte[] page = await owner.source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                owner.CheckSource(owner.source, owner.definition, cancellationToken);
                if (page.Length < this.PageSize)
                {
                    throw Corrupt(pageNumber, "The active-table source returned a truncated page.");
                }

                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        public void ThrowIfDisposed() => owner.CheckSource(owner.source, owner.definition, CancellationToken.None);

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => owner.CheckSource(owner.source, owner.definition, cancellationToken);
    }

    private sealed class MappedBookmarkPages(CompactActiveTableRows owner) : IPageSource
    {
        public int PageSize => owner.pages.PageSize;

        public long PageCount => owner.pages.PageCount;

        public bool IsDisposed => owner.pages.IsDisposed;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            byte[] page = await owner.pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] == Constants.PageTypes.Data && !owner.mappedPages.Contains(pageNumber))
            {
                PageBuffers.Return(page);
                throw Corrupt(pageNumber, "A covering bookmark or moved-row hop targets storage outside the independently inspected table map.");
            }

            return page;
        }

        public void ThrowIfDisposed() => owner.pages.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => owner.pages.ThrowIfDisposedOrCancelled(cancellationToken);
    }
}
