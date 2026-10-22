namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Coordinates independent reopened private table, global-map, pinned-prefix and complete allocation comparisons.</summary>
/// <remarks>
/// The caller owns an immutable decrypted source and independently reopened immutable private destination and global
/// allocation sidecar on distinct backing storage, including through wrappers. Only this invocation uses those streams.
/// Source inventory independently binds complete local-table membership before any ledger factory is invoked. Every
/// source and private read request shares one cumulative admission across phases, including partial-read retries and
/// failed requests. No phase resets it. Each invocation has a new one-shot allowance; failures return no completion
/// evidence. Table sidecars open serially and may be disk-backed, avoiding coordinator-owned whole-table snapshots.
/// The coordinator never writes source, destination or sidecars, and restores borrowed stream positions on every exit.
/// This is private composition evidence; unknown incoming references, active destination catalog recovery, complete
/// damaged-source discovery, native acceptance, reclamation and publication remain separate requirements.
/// </remarks>
internal static class CompactPinnedDatabaseValidator
{
    /// <summary>Validates every discovered local table and every byte/page of its retained-prefix private assembly.</summary>
    /// <param name="source">The immutable complete original decrypted page source.</param>
    /// <param name="format">The matching verified native format.</param>
    /// <param name="tables">Complete immutable original table evidence and private allocations in append order.</param>
    /// <param name="reopenedDestination">The independently reopened immutable readable seekable private destination.</param>
    /// <param name="globalMap">Independent pre-map EOF evidence and native-map limits.</param>
    /// <param name="reopenedAllocationLedger">The independently reopened immutable readable seekable complete allocation sidecar.</param>
    /// <param name="allocation">Untrusted complete allocation-sidecar geometry.</param>
    /// <param name="budget">One-shot cumulative and per-component admission.</param>
    /// <param name="cancellationToken">Cancels inventory, provider opens, every traversal and every read request.</param>
    /// <returns>Composed private comparisons without whole-database, ownership or publication acceptance.</returns>
    /// <exception cref="JetCorruptDataException">Membership, native storage, retained bytes or complete allocations disagree.</exception>
    /// <exception cref="JetLimitationException">A cumulative or active-component allowance is exhausted.</exception>
    internal static async ValueTask<CompactPinnedDatabaseValidatorResult> ValidateAsync(
        IPageSource source,
        JetFormat format,
        IReadOnlyList<CompactPinnedDatabaseTable> tables,
        Stream reopenedDestination,
        CompactPinnedGlobalMapValidation globalMap,
        Stream reopenedAllocationLedger,
        CompactGlobalAllocationBuilderResult allocation,
        CompactPinnedDatabaseValidatorBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(tables, nameof(tables));
        Guard.NotNull(globalMap, nameof(globalMap));
        Guard.NotNull(globalMap.Budget, nameof(globalMap));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.Tables, nameof(budget));
        Guard.NotNull(budget.GlobalMap, nameof(budget));
        Guard.NotNull(budget.Prefix, nameof(budget));
        Guard.NotNull(budget.Allocation, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, 0x1000000L, nameof(budget));
        Guard.InRange(budget.MaxTables, 0, int.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        ValidateStreams(reopenedDestination, reopenedAllocationLedger);
        var admission = new Admission(source, format, reopenedDestination, budget, cancellationToken);
        if (tables.Count > budget.MaxTables)
        {
            throw Limit("Private database comparison exceeds MaxTables.");
        }

        // Include plan/reference objects and arrays, plus transient component result
        // objects while their scalar references are extracted after comparison.
        long retained = 4096L + (1024L * tables.Count);
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guard.NotNull(table, nameof(tables));
            Guard.NotNull(table.Definition, nameof(tables));
            Guard.NotNull(table.Indexes, nameof(tables));
            Guard.NotNull(table.Allocation, nameof(tables));
            Guard.NotNull(table.Allocation.Table, nameof(tables));
            Guard.NotNull(table.Allocation.Table.LongValueMaps, nameof(tables));
            Guard.NotNull(table.OpenRows, nameof(tables));
            Guard.NotNull(table.OpenLongValues, nameof(tables));
            if (table.Indexes.Count > Constants.TableDefinition.MaxIndexes || table.Allocation.Table.LongValueMaps.Count > Constants.TableDefinition.MaxColumns)
            {
                throw Corrupt("Private database table evidence exceeds native descriptor capacities.");
            }

            retained += (256L * table.Indexes.Count) + (256L * table.Allocation.Table.LongValueMaps.Count);
        }

        if (retained >= budget.MaxWorkingBytes)
        {
            throw Limit("Private database retained comparison references exceed MaxWorkingBytes.");
        }

        long working = budget.MaxWorkingBytes - retained;
        var session = new CompactPinnedTableAssemblySession(admission.Source, format, TableBudget(budget.Tables, working));
        var plan = new CompactDatabasePlanTable[tables.Count];
        var references = new CompactPinnedTDefReferences[tables.Count];
        for (int index = 0; index < tables.Count; index++)
        {
            CompactPinnedDatabaseTable table = tables[index];
            plan[index] = new(table.Owner, table.Definition);
        }

        long destinationPosition = reopenedDestination.Position;
        long allocationPosition = reopenedAllocationLedger.Position;
        try
        {
            await CompactDatabasePlanValidator.ValidateAsync(admission.Source, format, plan, budget.Tables.Discovery, cancellationToken, session.Discovery.Accounting).ConfigureAwait(false);
            await using var destination = new AdmittedStream(reopenedDestination, admission);
            await using var ledger = new AdmittedStream(reopenedAllocationLedger, admission);
            for (int index = 0; index < tables.Count; index++)
            {
                CompactPinnedDatabaseTable table = tables[index];
                admission.Check(cancellationToken);
                Stream rows = await table.OpenRows(cancellationToken).ConfigureAwait(false);
                Guard.NotNull(rows, nameof(tables));
                RejectBorrowedAlias(rows, reopenedDestination, reopenedAllocationLedger);
                try
                {
                    admission.Check(cancellationToken);
                    Stream values = await table.OpenLongValues(cancellationToken).ConfigureAwait(false);
                    Guard.NotNull(values, nameof(tables));
                    RejectBorrowedAlias(values, reopenedDestination, reopenedAllocationLedger);
                    try
                    {
                        admission.Check(cancellationToken);
                        ValidateStreams(rows, values, reopenedDestination, reopenedAllocationLedger);
                        references[index] = await ValidateTableAsync(session, table, destination, rows, values, admission, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        await values.DisposeAsync().ConfigureAwait(false);
                    }
                }
                finally
                {
                    await rows.DisposeAsync().ConfigureAwait(false);
                }
            }

            CompactGlobalMapBudget mapBudget = MapBudget(budget.GlobalMap, globalMap.Budget, working);
            CompactGlobalMapResult map = await CompactGlobalMapValidator.ValidateAsync(admission.Source, format, globalMap.AppendPointPages, destination, mapBudget, cancellationToken).ConfigureAwait(false);
            CompactPinnedPrefixValidatorResult prefix = await CompactPinnedPrefixValidator.ValidateAsync(
                admission.Source, format, references, destination, PrefixBudget(budget.Prefix, working), cancellationToken, new(globalMap.AppendPointPages, mapBudget)).ConfigureAwait(false);
            CompactGlobalAllocationValidatorResult allocations = await CompactGlobalAllocationValidator.ValidateAsync(
                format, source.PageCount, AllocationsAsync(tables, map, cancellationToken), destination, ledger, allocation, AllocationBudget(budget.Allocation, working), cancellationToken).ConfigureAwait(false);
            admission.Check(cancellationToken);
            return new(tables.Count, map, prefix, allocations, admission.ReadBytes);
        }
        finally
        {
            reopenedDestination.Position = destinationPosition;
            reopenedAllocationLedger.Position = allocationPosition;
        }
    }

    private static async ValueTask<CompactPinnedTDefReferences> ValidateTableAsync(
        CompactPinnedTableAssemblySession session,
        CompactPinnedDatabaseTable table,
        Stream destination,
        Stream rows,
        Stream values,
        Admission admission,
        CancellationToken token)
    {
        await using var admittedRows = new AdmittedStream(rows, admission);
        await using var admittedValues = new AdmittedStream(values, admission);
        CompactPinnedTableAssemblyResult verified = await CompactPinnedTableAssembly.ValidateAsync(
            session, table.Owner, table.Definition, table.Indexes, destination, admittedRows, admittedValues, table.Allocation, token, table.HydratedColumns).ConfigureAwait(false);
        var indexes = new CompactTDefIndexReferences[verified.Indexes.Count];
        for (int slot = 0; slot < indexes.Length; slot++)
        {
            indexes[slot] = verified.Indexes[slot].References;
        }

        var longValues = new CompactTDefLongValueReferences[table.Allocation.Table.LongValueMaps.Count];
        for (int slot = 0; slot < longValues.Length; slot++)
        {
            CompactRawTableLongValueMaps column = table.Allocation.Table.LongValueMaps[slot];
            longValues[slot] = new(column.ColumnNumber, column.OwnedPages.Pointer, column.FreePages.Pointer);
        }

        return new(table.Owner, table.Definition, table.Allocation.Table.OwnedPages.Pointer, table.Allocation.Table.FreePages.Pointer, indexes, longValues);
    }

    private static CompactPinnedTableAssemblyBudget TableBudget(CompactPinnedTableAssemblyBudget budget, long working)
        => new()
        {
            MaxPages = budget.MaxPages,
            MaxOutputBytes = budget.MaxOutputBytes,
            MaxReadBytes = budget.MaxReadBytes,
            MaxWriteBytes = budget.MaxWriteBytes,
            MaxObservedEntries = budget.MaxObservedEntries,
            MaxObservedKeyBytes = budget.MaxObservedKeyBytes,
            MaxWorkingBytes = Math.Min(budget.MaxWorkingBytes, working),
            Discovery = budget.Discovery,
            TableBuilder = budget.TableBuilder,
            TableValidator = budget.TableValidator,
            Index = budget.Index,
            Definition = budget.Definition,
        };

    private static CompactGlobalMapBudget MapBudget(CompactGlobalMapBudget first, CompactGlobalMapBudget second, long working)
        => new()
        {
            MaxPages = Math.Min(first.MaxPages, second.MaxPages),
            MaxBitmapPages = Math.Min(first.MaxBitmapPages, second.MaxBitmapPages),
            MaxReadBytes = Math.Min(first.MaxReadBytes, second.MaxReadBytes),
            MaxWriteBytes = Math.Min(first.MaxWriteBytes, second.MaxWriteBytes),
            MaxWorkingBytes = Math.Min(working, Math.Min(first.MaxWorkingBytes, second.MaxWorkingBytes)),
        };

    private static CompactPinnedPrefixValidatorBudget PrefixBudget(CompactPinnedPrefixValidatorBudget budget, long working)
        => new()
        {
            MaxPages = budget.MaxPages,
            MaxDefinitions = budget.MaxDefinitions,
            MaxDefinitionPages = budget.MaxDefinitionPages,
            MaxReadBytes = budget.MaxReadBytes,
            MaxWorkingBytes = Math.Min(working, budget.MaxWorkingBytes),
        };

    private static CompactGlobalAllocationBudget AllocationBudget(CompactGlobalAllocationBudget budget, long working)
        => new()
        {
            MaxPages = budget.MaxPages,
            MaxRecords = budget.MaxRecords,
            MaxLedgerBytes = budget.MaxLedgerBytes,
            MaxReadBytes = budget.MaxReadBytes,
            MaxWorkingBytes = Math.Min(working, budget.MaxWorkingBytes),
        };

    private static void ValidateStreams(params Stream[] streams)
    {
        for (int index = 0; index < streams.Length; index++)
        {
            Stream stream = streams[index];
            Guard.NotNull(stream, nameof(streams));
            Guard.RequireReadableSeekableStream(stream, nameof(streams));
            for (int earlier = 0; earlier < index; earlier++)
            {
                if (Aliases(stream, streams[earlier]))
                {
                    throw new ArgumentException("Private comparison streams must have distinct backing storage, including through wrappers.", nameof(streams));
                }
            }
        }
    }

    private static void RejectBorrowedAlias(Stream stream, Stream destination, Stream ledger)
    {
        if (ReferenceEquals(stream, destination) || ReferenceEquals(stream, ledger))
        {
            throw new ArgumentException("A ledger factory cannot transfer a borrowed destination or global ledger.", nameof(stream));
        }
    }

    private static bool Aliases(Stream first, Stream second)
        => ReferenceEquals(first, second) || (first is FileStream file && second is FileStream other
            && string.Equals(file.Name, other.Name, StringComparison.OrdinalIgnoreCase));

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> AllocationsAsync(
        IReadOnlyList<CompactPinnedDatabaseTable> tables, CompactGlobalMapResult map, [EnumeratorCancellation] CancellationToken token)
    {
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            await foreach (CompactGlobalAllocationRecord record in table.Allocation.AllocationsAsync(token).ConfigureAwait(false))
            {
                yield return record;
            }
        }

        token.ThrowIfCancellationRequested();
        if (map.BitmapPages > 0)
        {
            yield return new(CompactGlobalAllocationKind.UsageMap, 1, new(map.AppendPointPages, map.BitmapPages));
        }
    }

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.CorruptPage, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private sealed class Admission
    {
        private readonly IPageSource original;
        private readonly Stream destination;
        private readonly CompactPinnedDatabaseValidatorBudget budget;
        private readonly long sourcePages;
        private readonly long destinationLength;
        private readonly int pageSize;
        private readonly CancellationToken token;

        internal Admission(IPageSource source, JetFormat format, Stream destination, CompactPinnedDatabaseValidatorBudget budget, CancellationToken token)
        {
            this.original = source;
            this.destination = destination;
            this.budget = budget;
            this.sourcePages = source.PageCount;
            this.destinationLength = destination.Length;
            this.pageSize = format.PageSize;
            this.token = token;
            this.Source = new AdmittedSource(this);
            this.Check(token);
            if (this.sourcePages < 3 || this.destinationLength % this.pageSize != 0 || this.destinationLength / this.pageSize < this.sourcePages)
            {
                throw Corrupt("The private database does not retain the complete page-aligned original prefix.");
            }

            if (this.sourcePages > budget.MaxPages || this.destinationLength / this.pageSize > budget.MaxPages)
            {
                throw Limit("Private database source or destination exceeds MaxPages.");
            }
        }

        internal IPageSource Source { get; }

        internal long ReadBytes { get; private set; }

        internal void Check(CancellationToken cancellationToken)
        {
            this.original.ThrowIfDisposedOrCancelled(this.token);
            cancellationToken.ThrowIfCancellationRequested();
            if (this.original.PageCount != this.sourcePages || this.original.PageSize != this.pageSize || this.destination.Length != this.destinationLength)
            {
                throw Corrupt("The immutable original or reopened private database geometry changed during comparison.");
            }
        }

        internal void Read(int bytes, CancellationToken cancellationToken)
        {
            this.Check(cancellationToken);
            if (bytes > this.budget.MaxReadBytes - this.ReadBytes)
            {
                throw Limit("Private database comparison exceeds cumulative source and private MaxReadBytes.");
            }

            this.ReadBytes += bytes;
        }

        private sealed class AdmittedSource(Admission admission) : IPageSource
        {
            public int PageSize => admission.pageSize;

            public long PageCount => admission.sourcePages;

            public bool IsDisposed => admission.original.IsDisposed;

            public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
            {
                admission.Read(this.PageSize, cancellationToken);
                byte[] page = await admission.original.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                try
                {
                    admission.Check(cancellationToken);
                    return page;
                }
                catch
                {
                    PageBuffers.Return(page);
                    throw;
                }
            }

            public void ThrowIfDisposed() => admission.original.ThrowIfDisposed();

            public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => admission.Check(cancellationToken);
        }
    }

    private sealed class AdmittedStream : Stream
    {
#pragma warning disable CA2213 // The coordinator borrows streams and disposes transferred factory streams explicitly.
        private readonly Stream inner;
#pragma warning restore CA2213
        private readonly Admission admission;
        private readonly long length;

        internal AdmittedStream(Stream inner, Admission admission)
        {
            this.inner = inner;
            this.admission = admission;
            this.length = inner.Length;
        }

        public override bool CanRead => this.inner.CanRead;

        public override bool CanSeek => this.inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => this.inner.Length;

        public override long Position { get => this.inner.Position; set => this.inner.Position = value; }

        public override void Flush() => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            this.Check(default);
            this.admission.Read(count, default);
            int read = this.inner.Read(buffer, offset, count);
            this.Check(default);
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            this.Check(cancellationToken);
            this.admission.Read(buffer.Length, cancellationToken);
            int read = await this.inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            this.Check(cancellationToken);
            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => this.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => this.inner.Seek(offset, origin);

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        private void Check(CancellationToken token)
        {
            this.admission.Check(token);
            if (this.inner.Length != this.length)
            {
                throw Corrupt("An immutable reopened private comparison stream changed length.");
            }
        }
    }
}
