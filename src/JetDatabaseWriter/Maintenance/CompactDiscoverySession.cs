namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;

/// <summary>Composes bounded raw discovery and staging copies across multiple tables, roots and values.</summary>
/// <remarks>
/// The caller keeps the decrypted source immutable and owns its lifetime. This session owns no cache.
/// Root scans run serially; their awaited visitors may audit or copy one long value at a time.
/// No method authorizes relocation, reclamation, repair or publication of a partial destination.
/// Runtime overhead, shared-pool retention and caller-owned buffers, destinations and retained observations are separate from admission.
/// </remarks>
internal sealed class CompactDiscoverySession
{
    private readonly CompactDiscoveryBudget budget;
    private readonly DiscoveryPageSource pages;
    private int activeScan;
    private int activeValue;

    /// <summary>Initializes a new instance of the <see cref="CompactDiscoverySession"/> class without reading the source.</summary>
    /// <param name="pages">The caller-owned immutable decrypted page source.</param>
    /// <param name="format">The verified source format.</param>
    /// <param name="budget">The immutable whole-session limits.</param>
    /// <exception cref="JetLimitationException">The source physical size exceeds the session limit.</exception>
    /// <exception cref="JetCorruptDataException">The source geometry disagrees with the verified format.</exception>
    internal CompactDiscoverySession(IPageSource pages, JetFormat format, CompactDiscoveryBudget budget)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        Guard.InRange(budget.MaxPages, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxValueBytes, 0, LongValueDescriptor.MaxLength, nameof(budget));
        Guard.InRange(budget.MaxChainRows, 0, int.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxDefinitionBytes, nameof(budget));
        Guard.InRange(budget.MaxOverflowTargets, 0, int.MaxValue, nameof(budget));
        this.Accounting = new CompactDiscoveryAccounting(budget);
        this.SourceFormat = format;
        this.budget = budget;
        this.SourcePages = pages;
        pages.ThrowIfDisposed();
        int pageSize = pages.PageSize;
        long pageCount = pages.PageCount;
        if (pageCount > budget.MaxPages)
        {
            throw Limit("The source physical size exceeds MaxPages.");
        }

        if (pageSize != format.PageSize || pageCount < 3)
        {
            throw Corrupt(0, "The discovery source geometry does not match the verified format.");
        }

        this.pages = new DiscoveryPageSource(pages, this.Accounting, pageSize, pageCount);
        this.pages.CheckGeometry(0, default);
    }

    /// <summary>Gets the session's cumulative work and current storage admission.</summary>
    internal CompactDiscoveryAccounting Accounting { get; }

    /// <summary>Gets the verified format retained by this session.</summary>
    internal JetFormat SourceFormat { get; }

    /// <summary>Gets the exact immutable unwrapped source for independently bound preservation comparisons.</summary>
    internal IPageSource SourcePages { get; }

    /// <summary>Inventories physical storage and discovered catalog identities using the session's persistent allowances.</summary>
    /// <param name="visitor">The awaited observation consumer, which may audit or copy a long value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="activeCatalog">Optional independently compared exact active membership for this destination only.</param>
    /// <returns>A task completing after the bounded inventory succeeds.</returns>
    /// <remarks>Missing ownership can hide catalog identities. Successful inventory never proves complete discovery or permission to reclaim storage.</remarks>
    internal async ValueTask ScanInventoryAsync(Func<CompactStorageRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken, CompactActiveCatalogRows? activeCatalog = null)
    {
        Guard.NotNull(visitor, nameof(visitor));
        this.EnterScan(cancellationToken);
        try
        {
            await this.ScanInventoryCoreAsync(visitor, cancellationToken, activeCatalog: activeCatalog).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Inventories the source and streams raw rows and index trees for every discovered local definition, including the catalog.</summary>
    /// <param name="storageVisitor">The awaited inventory observation consumer.</param>
    /// <param name="rowVisitor">Consumes each borrowed row with its owning definition; it may audit or copy identified long values before returning.</param>
    /// <param name="indexVisitor">The awaited raw-index observation consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="ownershipVisitor">Optionally consumes known cross-root ownership and sharing evidence.</param>
    /// <returns>A task completing after all discovered local tables and defined index roots have been scanned.</returns>
    /// <remarks>
    /// Catalog-only links and opaque objects remain pinned and are never executed. The caller identifies long-value fields in the borrowed rows.
    /// Damaged ownership can hide identities, and the component audits do not prove complete discovery, global index validity or a valid compact result.
    /// Ordinary index bookmarks are verified against live same-owner headers and bounded moved-row chains.
    /// Exact accepted catalog recoveries are also streamed and can resolve catalog bookmarks after their complete stored Id is rechecked;
    /// recovered observations disclose damaged ownership and withhold the ordinary same-owner proof. Stored-key/row correspondence remains unproven.
    /// Inventory and definition storage remain admitted throughout all awaited row, index and nested value callbacks.
    /// </remarks>
    internal async ValueTask ScanDatabaseAsync(
        Func<CompactStorageRecord, CancellationToken, ValueTask> storageVisitor,
        Func<long, RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> rowVisitor,
        Func<CompactIndexRecord, CancellationToken, ValueTask> indexVisitor,
        CancellationToken cancellationToken,
        Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask>? ownershipVisitor = null)
    {
        Guard.NotNull(storageVisitor, nameof(storageVisitor));
        Guard.NotNull(rowVisitor, nameof(rowVisitor));
        Guard.NotNull(indexVisitor, nameof(indexVisitor));
        this.EnterScan(cancellationToken);
        try
        {
            await this.ScanDatabaseCoreAsync(
                storageVisitor,
                (root, _, _, location, bytes, token) => rowVisitor(root, location, bytes, token),
                indexVisitor,
                ownershipVisitor ?? IgnoreOwnership,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Discovers stored fields and automatically audits each non-null long value in every discovered local table.</summary>
    /// <param name="storageVisitor">The awaited inventory observation consumer.</param>
    /// <param name="fieldVisitor">Consumes each borrowed field with its table identity, including null and absent fields.</param>
    /// <param name="longValueVisitor">Consumes borrowed raw long-value observations with their original field and table identities.</param>
    /// <param name="indexVisitor">The awaited raw-index observation consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="ownershipVisitor">Optionally consumes known cross-root ownership and sharing evidence.</param>
    /// <returns>A task completing after the discovered fields, long values and index bookmarks have been audited.</returns>
    /// <remarks>
    /// Stored bytes are never converted or recalculated. Native envelope semantics, hidden catalog identities and incoming ownership remain unproven.
    /// Calculated ResultType properties are hydrated through bounded intact-owner and independently recovered raw catalog reads; expressions are never evaluated.
    /// Recovered catalog bookmarks retain their original identities and explicit ownership ambiguity; recovered index observations withhold the same-owner proof.
    /// Assigned calculated storage without a supported persisted result type is retained as a raw observation and then explicitly refused.
    /// Visitors borrow memory only until their awaited call returns. All operations share cumulative work and simultaneous storage limits.
    /// Successful discovery does not authorize relocation, reclamation or publication.
    /// </remarks>
    internal async ValueTask ScanDatabaseFieldsAsync(
        Func<CompactStorageRecord, CancellationToken, ValueTask> storageVisitor,
        Func<long, CompactRawField, CancellationToken, ValueTask> fieldVisitor,
        Func<long, CompactRawField, CompactLongValueRecord, CancellationToken, ValueTask> longValueVisitor,
        Func<CompactIndexRecord, CancellationToken, ValueTask> indexVisitor,
        CancellationToken cancellationToken,
        Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask>? ownershipVisitor = null)
    {
        Guard.NotNull(storageVisitor, nameof(storageVisitor));
        Guard.NotNull(fieldVisitor, nameof(fieldVisitor));
        Guard.NotNull(longValueVisitor, nameof(longValueVisitor));
        Guard.NotNull(indexVisitor, nameof(indexVisitor));
        this.EnterScan(cancellationToken);
        try
        {
            await this.ScanDatabaseCoreAsync(
                storageVisitor,
                (root, image, columns, location, bytes, token) => CompactRawFieldReader.ScanAsync(
                    this.SourceFormat,
                    image,
                    location,
                    bytes,
                    async (field, fieldToken) =>
                    {
                        this.Accounting.Charge(CompactDiscoveryCharge.Fields, 1, location.PageNumber);
                        this.Accounting.Charge(CompactDiscoveryCharge.Records, 1, location.PageNumber);
                        await fieldVisitor(root, field, fieldToken).ConfigureAwait(false);
                        this.pages.CheckGeometry(location.PageNumber, fieldToken);
                        if (field.Kind == CompactRawFieldKind.Stored && !field.IsStorageTypeKnown)
                        {
                            const string reason = "Automatic long-value discovery requires a supported persisted ResultType for the calculated column; its stored bytes remain opaque and pinned.";
                            throw new JetLimitationException(
                                JetErrorCode.FeatureNotSupported,
                                reason,
                                new JetErrorInfo { ColumnName = field.Column.Name, PageNumber = location.DataPageNumber, Reason = reason });
                        }

                        if (field.IsLongValue && field.Kind == CompactRawFieldKind.Stored)
                        {
                            await this.AuditLongValueAsync(
                                field.RawBytes,
                                (record, valueToken) => longValueVisitor(root, field, record, valueToken),
                                fieldToken).ConfigureAwait(false);
                        }
                    },
                    token,
                    columns),
                indexVisitor,
                ownershipVisitor ?? IgnoreOwnership,
                cancellationToken,
                hydrateCalculated: true).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Streams one table's raw rows using allowances retained across every invocation.</summary>
    /// <param name="ownerTDefPage">The expected table owner.</param>
    /// <param name="visitor">The awaited borrowed-row consumer, which may copy or audit a long value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after this table's raw scan.</returns>
    internal async ValueTask ScanRowsAsync(long ownerTDefPage, Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.EnterScan(cancellationToken);
        try
        {
            await this.ScanRowsCoreAsync(ownerTDefPage, visitor, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Audits one index root using allowances retained across every invocation.</summary>
    /// <param name="ownerTDefPage">The expected table owner.</param>
    /// <param name="rootPage">The native index root.</param>
    /// <param name="visitor">The awaited observation consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after this root's raw audit.</returns>
    internal async ValueTask ScanIndexAsync(long ownerTDefPage, long rootPage, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.EnterScan(cancellationToken);
        try
        {
            await this.ScanIndexCoreAsync(ownerTDefPage, rootPage, visitor, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Audits one index root and verifies that every observed bookmark resolves to a live row identity of its table.</summary>
    /// <param name="ownerTDefPage">The expected table owner.</param>
    /// <param name="rootPage">The native index root.</param>
    /// <param name="visitor">The awaited observation consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after the tree and bookmark checks succeed.</returns>
    /// <remarks>Read, directory, reference and working-storage allowances are shared with other session operations. Stored-key semantics and incoming ownership remain unproven.</remarks>
    internal async ValueTask ScanIndexBookmarksAsync(long ownerTDefPage, long rootPage, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.EnterScan(cancellationToken);
        try
        {
            await this.ScanIndexBookmarksCoreAsync(ownerTDefPage, rootPage, visitor, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Audits complete stored long-value regions without interpreting their payload.</summary>
    /// <param name="column">The borrowed original descriptor and column bytes.</param>
    /// <param name="visitor">The awaited borrowed-observation consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after this value's raw audit.</returns>
    internal async ValueTask AuditLongValueAsync(ReadOnlyMemory<byte> column, Func<CompactLongValueRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.EnterValue(cancellationToken);
        try
        {
            await CompactLongValueAudit.AuditAsync(
                this.pages,
                this.SourceFormat,
                column,
                new()
                {
                    MaxPages = this.budget.MaxPages,
                    MaxPageReads = this.budget.MaxPageReads,
                    MaxChainRows = this.budget.MaxChainRows,
                    MaxDirectorySlots = this.budget.MaxDirectorySlots,
                    MaxRawBytes = this.budget.MaxLongValueBytes,
                    MaxWorkingBytes = this.budget.MaxWorkingBytes,
                },
                visitor,
                cancellationToken,
                this.Accounting).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeValue, 0);
        }
    }

    /// <summary>Streams a declared raw payload into a private destination using cumulative byte, row and read limits.</summary>
    /// <param name="column">The borrowed original descriptor and column bytes.</param>
    /// <param name="destination">The caller-owned staging destination, left open and unflushed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after the declared payload is written.</returns>
    /// <remarks>Payload bytes are charged before copying, including bytes of an interrupted copy. Complete row bytes, trailing content and reference ownership require auditing separately.</remarks>
    /// <exception cref="JetCorruptDataException">The original column has a truncated descriptor or unreadable payload.</exception>
    /// <exception cref="ArgumentException">The destination is not writable.</exception>
    internal async ValueTask CopyLongValueAsync(ReadOnlyMemory<byte> column, Stream destination, CancellationToken cancellationToken)
    {
        Guard.NotNull(destination, nameof(destination));
        this.EnterValue(cancellationToken);
        try
        {
            if (!LongValueDescriptor.TryRead(column.Span, out LongValueDescriptor descriptor))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptPage, "The raw long-value column is shorter than its descriptor.");
            }

            ReadOnlyMemory<byte> inline = column[Constants.LongValue.HeaderSize..];
            LongValueStore.ValidatePayload(descriptor, inline.Length, this.SourceFormat.PageSize, this.budget.MaxValueBytes, this.budget.MaxChainRows);
            if (!destination.CanWrite)
            {
                throw new ArgumentException("The raw long-value staging destination must be writable.", nameof(destination));
            }

            this.Accounting.Charge(CompactDiscoveryCharge.LongValues, 1);
            this.Accounting.Charge(CompactDiscoveryCharge.LongValuePayloadBytes, descriptor.Length);
            int capacity = (this.SourceFormat.PageSize - this.SourceFormat.DataPage.RowsStart) / 2;
            long storage = 65_536L + (2L * this.SourceFormat.PageSize) + (32L * capacity) + (descriptor.IsInline ? 0 : 64L * this.budget.MaxChainRows);
            using IDisposable admission = this.Accounting.ReserveWorkingBytes(storage);
            using var cache = new ReaderPageCache(this.SourceFormat, new CopyPageSource(this.pages, this.SourceFormat, this.Accounting), 0);
            var decoder = new LongValueDecoder(this.SourceFormat, cache, this.budget.MaxValueBytes);
            await LongValueStore.CopyPayloadAsync(
                descriptor,
                inline,
                destination,
                this.SourceFormat.PageSize,
                this.budget.MaxValueBytes,
                this.budget.MaxChainRows,
                LocateAsync,
                cancellationToken,
                cache.ReturnUncachedPage).ConfigureAwait(false);
            this.pages.CheckGeometry(0, cancellationToken);

            ValueTask<LvalRowLocation> LocateAsync(uint pointer, CancellationToken token)
            {
                this.Accounting.Charge(CompactDiscoveryCharge.References, 1, LongValueStore.PageNumber(pointer));
                this.Accounting.Charge(CompactDiscoveryCharge.LongValueRows, 1, LongValueStore.PageNumber(pointer));
                this.Accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, LongValueStore.PageNumber(pointer));
                return decoder.LocateLvalRowAsync(pointer, token);
            }
        }
        finally
        {
            Volatile.Write(ref this.activeValue, 0);
        }
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static ValueTask IgnoreOwnership(CompactIndexOwnershipRecord record, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return default;
    }

    private async ValueTask ScanInventoryCoreAsync(Func<CompactStorageRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken, Func<long, TDefImage, CancellationToken, ValueTask>? definitionVisitor = null, Func<CompactCatalogRecovery.RecoveredRow, CancellationToken, ValueTask>? recoveredCatalogVisitor = null, CompactActiveCatalogRows? activeCatalog = null)
    {
        int schemaBytes = checked((int)Math.Min(int.MaxValue, this.budget.MaxSchemaBytes));
        using var definitions = new TableDefReader(this.SourcePages, this.SourceFormat, cacheResults: false, this.budget.MaxDefinitionBytes, schemaBytes);
        await CompactStorageInventory.ScanAsync(
            this.SourcePages,
            this.SourceFormat,
            definitions,
            new()
            {
                Accounting = this.Accounting,
                MaxPages = this.budget.MaxPages,
                MaxObjects = checked((int)Math.Min(int.MaxValue, this.budget.MaxCatalogObjects)),
                MaxReferences = this.budget.MaxReferences,
                MaxDiagnostics = checked((int)Math.Min(int.MaxValue, this.budget.MaxDiagnostics)),
                MaxSchemaBytes = schemaBytes,
                MaxPageReads = this.budget.MaxPageReads,
                MaxMapSteps = this.budget.MaxMapSteps,
                MaxRecords = this.budget.MaxRecords,
                MaxWorkingBytes = this.budget.MaxWorkingBytes,
            },
            visitor,
            cancellationToken,
            definitionVisitor,
            recoveredCatalogVisitor,
            activeCatalog).ConfigureAwait(false);
        this.pages.CheckGeometry(0, cancellationToken);
    }

    private async ValueTask ScanDatabaseCoreAsync(
        Func<CompactStorageRecord, CancellationToken, ValueTask> storageVisitor,
        Func<long, TDefImage, IReadOnlyList<ColumnInfo>?, RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> rowVisitor,
        Func<CompactIndexRecord, CancellationToken, ValueTask> indexVisitor,
        Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> ownershipVisitor,
        CancellationToken cancellationToken,
        bool hydrateCalculated = false)
    {
        using var recovered = new CompactRecoveredCatalogRows(this.pages, this.SourceFormat, this.budget, this.Accounting);
        using CompactCalculatedColumnHydrator? hydrator = hydrateCalculated ? new CompactCalculatedColumnHydrator(this.SourceFormat, this.budget, this.Accounting, ScanCatalogRowsAsync, this.AuditLongValueAsync) : null;
        long indexCoordinates = Math.Min(int.MaxValue, this.budget.MaxIndexPages);
        long mapCoordinates = Math.Min(int.MaxValue, this.budget.MaxObjects);
        using var ownership = new CompactIndexOwnershipAudit(
            this.pages.PageCount,
            new()
            {
                Accounting = this.Accounting,
                MaxTrackedPages = checked((int)Math.Min(int.MaxValue, indexCoordinates + mapCoordinates)),
                MaxMapRows = checked((int)Math.Min(int.MaxValue, this.budget.MaxObjects)),
                MaxRootDescriptors = this.budget.MaxObjects,
                MaxReferences = this.budget.MaxReferences,
                MaxGraphSteps = this.budget.MaxGraphSteps,
                MaxRecords = this.budget.MaxRecords,
                MaxWorkingBytes = this.budget.MaxWorkingBytes,
            });
        await this.ScanInventoryCoreAsync(
            storageVisitor,
            cancellationToken,
            async (root, image, token) =>
            {
                if (root == 2)
                {
                    recovered.SetCatalog(image);
                    hydrator?.SetCatalog(image);
                }

                if (hydrator is null)
                {
                    await ScanTableRowsAsync(null, token).ConfigureAwait(false);
                }
                else
                {
                    await hydrator.WithColumnsAsync(root, image, ScanTableRowsAsync, token).ConfigureAwait(false);
                }

                for (int ordinal = 0; ordinal < image.RealIndexes.Count; ordinal++)
                {
                    token.ThrowIfCancellationRequested();
                    TDefIndexDescriptor index = image.RealIndexes[ordinal];
                    int mapOffset = index.Slot.FirstDpOffset - 4;
                    if (mapOffset < 0 || mapOffset > image.LogicalByteLength - 4)
                    {
                        throw Corrupt(root, "A physical index descriptor has a truncated ownership-map pointer.");
                    }

                    uint mapPointer = BinaryPrimitives.ReadUInt32LittleEndian(image.Bytes.Slice(mapOffset, 4));
                    UsageMapPointer ownedPages = new(checked((int)(mapPointer & 0xFFu)), checked((int)(mapPointer >> 8)));
                    await ownership.DeclareDescriptorAsync(root, index.RootPage, ownedPages, ordinal, ownershipVisitor, token).ConfigureAwait(false);
                    if (index.RootPage != 0)
                    {
                        await this.ScanIndexBookmarksCoreAsync(
                            root,
                            index.RootPage,
                            async (record, indexToken) =>
                            {
                                await ownership.ObserveAsync(record, ownershipVisitor, indexToken).ConfigureAwait(false);
                                await indexVisitor(record, indexToken).ConfigureAwait(false);
                                this.pages.CheckGeometry(record.PageNumber, indexToken);
                            },
                            token,
                            root == 2 ? recovered.ResolveAsync : null).ConfigureAwait(false);
                    }
                }

                ValueTask ScanTableRowsAsync(IReadOnlyList<ColumnInfo>? columns, CancellationToken rowToken)
                    => ScanCatalogRowsAsync(root, (location, bytes, fieldToken) => rowVisitor(root, image, columns, location, bytes, fieldToken), rowToken);
            },
            (evidence, token) =>
            {
                recovered.Accept(evidence.Identity.Id, evidence.Location, token, evidence.HasDamagedMovedOwner);
                return default;
            }).ConfigureAwait(false);
        await ownership.CompleteAsync(ownershipVisitor, cancellationToken).ConfigureAwait(false);
        this.pages.CheckGeometry(0, cancellationToken);

        async ValueTask ScanCatalogRowsAsync(long owner, Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken token)
        {
            await this.ScanRowsCoreAsync(owner, visitor, token, recovered.Contains).ConfigureAwait(false);
            if (owner == 2)
            {
                await recovered.ScanAsync(visitor, token).ConfigureAwait(false);
            }
        }
    }

    private ValueTask ScanRowsCoreAsync(long ownerTDefPage, Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken, Func<RowLocation, bool>? excludeRow = null)
    {
        this.Accounting.Charge(CompactDiscoveryCharge.Objects, 1, ownerTDefPage);
        return CompactRawRowReader.ScanAsync(
            this.pages,
            this.SourceFormat,
            ownerTDefPage,
            new()
            {
                Accounting = this.Accounting,
                MaxPages = this.budget.MaxPages,
                MaxPageReads = this.budget.MaxPageReads,
                MaxRows = this.budget.MaxRows,
                MaxOverflowTargets = this.budget.MaxOverflowTargets,
                MaxDirectorySlots = this.budget.MaxDirectorySlots,
                MaxWorkingBytes = this.budget.MaxWorkingBytes,
            },
            visitor,
            cancellationToken,
            excludeRow);
    }

    private ValueTask ScanIndexCoreAsync(long ownerTDefPage, long rootPage, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.Accounting.Charge(CompactDiscoveryCharge.Objects, 1, rootPage);
        return CompactIndexAudit.ScanAsync(
            this.pages,
            this.SourceFormat,
            ownerTDefPage,
            rootPage,
            this.CreateIndexBudget(),
            visitor,
            cancellationToken);
    }

    private ValueTask ScanIndexBookmarksCoreAsync(long ownerTDefPage, long rootPage, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken, Func<long, int, CancellationToken, ValueTask<RowLocation?>>? recoveredCatalogResolver = null)
    {
        this.Accounting.Charge(CompactDiscoveryCharge.Objects, 1, rootPage);
        return CompactIndexBookmarkAudit.ScanAsync(
            this.pages,
            this.SourceFormat,
            ownerTDefPage,
            rootPage,
            this.CreateIndexBudget(),
            new()
            {
                Accounting = this.Accounting,
                MaxPageReads = this.budget.MaxPageReads,
                MaxDirectorySlots = this.budget.MaxDirectorySlots,
                MaxWorkingBytes = this.budget.MaxWorkingBytes,
            },
            visitor,
            cancellationToken,
            recoveredCatalogResolver);
    }

    private CompactIndexBudget CreateIndexBudget()
        => new()
        {
            Accounting = this.Accounting,
            MaxTreePages = checked((int)Math.Min(int.MaxValue, this.budget.MaxIndexPages)),
            MaxEntries = this.budget.MaxIndexEntries,
            MaxCanonicalBytes = this.budget.MaxIndexCanonicalBytes,
            MaxReferences = this.budget.MaxReferences,
            MaxGraphSteps = this.budget.MaxGraphSteps,
            MaxRecords = this.budget.MaxRecords,
            MaxWorkingBytes = this.budget.MaxWorkingBytes,
        };

    private void EnterScan(CancellationToken cancellationToken)
    {
        this.pages.CheckGeometry(0, cancellationToken);
        if (Interlocked.CompareExchange(ref this.activeScan, 1, 0) != 0)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "A compact discovery root scan is already active.");
        }
    }

    private void EnterValue(CancellationToken cancellationToken)
    {
        this.pages.CheckGeometry(0, cancellationToken);
        if (Interlocked.CompareExchange(ref this.activeValue, 1, 0) != 0)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "A compact discovery long-value operation is already active.");
        }
    }

    private sealed class DiscoveryPageSource(IPageSource inner, CompactDiscoveryAccounting accounting, int pageSize, long pageCount) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount => pageCount;

        public bool IsDisposed => inner.IsDisposed;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            this.CheckGeometry(pageNumber, cancellationToken);
            if (pageNumber < 0 || pageNumber >= this.PageCount)
            {
                throw Corrupt(pageNumber, "The discovery page reference is outside the source.");
            }

            accounting.Charge(CompactDiscoveryCharge.PageReads, 1, pageNumber);
            byte[] page = await inner.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                this.CheckGeometry(pageNumber, cancellationToken);
                if (page.Length < this.PageSize)
                {
                    throw Corrupt(pageNumber, "The discovery source returned a truncated physical page.");
                }

                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
        {
            inner.ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
        }

        internal void CheckGeometry(long page, CancellationToken cancellationToken)
        {
            this.ThrowIfDisposedOrCancelled(cancellationToken);
            if (inner.PageSize != this.PageSize || inner.PageCount != this.PageCount)
            {
                throw Corrupt(page, "The source geometry changed during discovery.");
            }
        }
    }

    private sealed class CopyPageSource(DiscoveryPageSource pages, JetFormat format, CompactDiscoveryAccounting accounting) : IPageSource
    {
        public int PageSize => pages.PageSize;

        public long PageCount => pages.PageCount;

        public bool IsDisposed => pages.IsDisposed;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (!LongValueStore.IsLvalPage(page))
                {
                    throw Corrupt(pageNumber, "A raw long-value copy reference does not target an LVAL data page.");
                }

                int count = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
                if (page[0] == Constants.PageTypes.Data && count <= (format.PageSize - format.DataPage.RowsStart) / 2)
                {
                    accounting.Charge(CompactDiscoveryCharge.DirectorySlots, count, pageNumber);
                }

                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        public void ThrowIfDisposed() => pages.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => pages.ThrowIfDisposedOrCancelled(cancellationToken);
    }
}
