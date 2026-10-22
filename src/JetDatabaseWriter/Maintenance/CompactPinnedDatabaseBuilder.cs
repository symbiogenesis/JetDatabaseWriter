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

/// <summary>Builds a complete discovered-local-table private assembly and independently compares its closed output.</summary>
/// <remarks>
/// The immutable decrypted source and all caller-provisioned private backing stores are distinct, including through
/// wrappers. The complete local-table plan is independently checked before any output factory runs. Only one source
/// page is retained while copying the original prefix; table sidecars can spill to disk and open serially. Every source
/// page stays pinned. Raw DATA/LVAL, physical stored indexes, known TDEF pointers, native global allocation and the
/// complete allocation sidecar are composed without converting stored values or regenerating stored keys.
/// Construction read/write admission is cumulative across all phases and failed requests. Independent reopened
/// validation has its own allowance. Every transferred stream is closed on every exit; partial private output is
/// retained for caller-controlled diagnosis and cleanup. This is a private append-only assembly, not compaction,
/// complete source recovery, native acceptance, proof of incoming ownership or permission to replace the source.
/// </remarks>
internal static class CompactPinnedDatabaseBuilder
{
    /// <summary>Constructs, closes, independently reopens and compares one complete private pinned database assembly.</summary>
    /// <param name="source">The caller-owned immutable decrypted original page source.</param>
    /// <param name="format">The independently verified matching native format.</param>
    /// <param name="tables">Immutable complete discovered-local-table evidence in the requested append order.</param>
    /// <param name="workspace">Exclusive separate private storage, with transferred stream ownership.</param>
    /// <param name="budget">Cumulative construction and separately bounded independent validation admission.</param>
    /// <param name="cancellationToken">Cancels inventory, factories, traversal, copying, writes and comparison.</param>
    /// <returns>Private compared component evidence that never authorizes reclamation or publication.</returns>
    /// <exception cref="JetCorruptDataException">The plan, native storage, compared retained bytes or allocations disagree.</exception>
    /// <exception cref="JetLimitationException">A cumulative or component allowance is exhausted.</exception>
    internal static async ValueTask<CompactPinnedDatabaseBuilderResult> BuildAndValidateAsync(
        IPageSource source,
        JetFormat format,
        IReadOnlyList<CompactPinnedDatabaseBuildTable> tables,
        CompactPinnedDatabaseWorkspace workspace,
        CompactPinnedDatabaseBuilderBudget budget,
        CancellationToken cancellationToken)
    {
        ValidateArguments(source, format, tables, workspace, budget);
        var admission = new Admission(source, format, budget, cancellationToken);
        if (tables.Count > budget.MaxTables)
        {
            throw Limit("Private database construction exceeds MaxTables.");
        }

        // Include retained patched logical images, per-table allocation/reference arrays,
        // closed stream identities and transient coordinator/component result objects.
        long retained = 8192L + (tables.Count * 4096L);
        foreach (CompactPinnedDatabaseBuildTable table in tables)
        {
            Guard.NotNull(table, nameof(tables));
            Guard.NotNull(table.Definition, nameof(tables));
            Guard.NotNull(table.Indexes, nameof(tables));
            if (table.Indexes.Count > Constants.TableDefinition.MaxIndexes || table.Definition.Columns.Count > Constants.TableDefinition.MaxColumns)
            {
                throw Corrupt("A private database plan exceeds native descriptor capacities.");
            }

            retained += (2L * table.Definition.LogicalByteLength) + (1024L * table.Indexes.Count) + (512L * table.Definition.Columns.Count);
        }

        if (retained + (2L * format.PageSize) >= budget.MaxWorkingBytes)
        {
            throw Limit("Private database retained assembly evidence exceeds MaxWorkingBytes.");
        }

        long working = budget.MaxWorkingBytes - retained;
        var assembly = new CompactPinnedTableAssemblySession(admission.Source, format, TableBudget(budget.Tables, working));
        var plan = new CompactDatabasePlanTable[tables.Count];
        var compared = new CompactPinnedDatabaseTable[tables.Count];
        var created = new List<Stream>(checked((tables.Count * 4) + 2));
        for (int index = 0; index < tables.Count; index++)
        {
            plan[index] = new(tables[index].Owner, tables[index].Definition);
        }

        CompactDatabasePlanResult membership = await CompactDatabasePlanValidator.ValidateAsync(
            admission.Source, format, plan, budget.Tables.Discovery, cancellationToken, assembly.Discovery.Accounting).ConfigureAwait(false);
        CompactGlobalMapResult global;
        CompactGlobalAllocationBuilderResult allocation;
        int activeTables = 0;
        long unassignedRows = 0;
        await using (Stream destinationOutput = await CreateAsync(workspace, CompactPinnedDatabaseFile.Destination, 0, created, admission, cancellationToken).ConfigureAwait(false))
        {
            await using var destination = new AdmittedStream(destinationOutput, admission, destination: true);
            await CopyPrefixAsync(admission, destination, cancellationToken).ConfigureAwait(false);
            await assembly.Discovery.WithCatalogRecoveryAsync(
                async buildToken =>
                {
                    for (int index = 0; index < tables.Count; index++)
                    {
                        CompactPinnedDatabaseBuildTable table = tables[index];
                        (CompactPinnedTableAssemblyAllocation built, CompactActiveTableResult? active) = await BuildTableAsync(assembly, table, workspace, destination, created, admission, budget.ActiveTables, buildToken).ConfigureAwait(false);
                        if (active is not null)
                        {
                            activeTables++;
                            unassignedRows = checked(unassignedRows + active.UnassignedRows);
                        }

                        long owner = table.Owner;
                        compared[index] = new(
                            owner,
                            table.Definition,
                            table.Indexes,
                            built,
                            token => workspace.Open(CompactPinnedDatabaseFile.Rows, owner, token),
                            token => workspace.Open(CompactPinnedDatabaseFile.LongValues, owner, token),
                            table.HydratedColumns,
                            table.ActiveIndexOrdinal);
                    }

                    return true;
                },
                cancellationToken).ConfigureAwait(false);

            long appendPoint = destination.Length / format.PageSize;
            global = await CompactGlobalMapInstaller.InstallAsync(admission.Source, format, appendPoint, destination, MapBudget(budget.GlobalMap, working), cancellationToken).ConfigureAwait(false);
            await using (Stream allocationOutput = await CreateAsync(workspace, CompactPinnedDatabaseFile.AllocationLedger, 0, created, admission, cancellationToken).ConfigureAwait(false))
            {
                await using var ledger = new AdmittedStream(allocationOutput, admission, destination: false);
                allocation = await CompactGlobalAllocationBuilder.BuildAsync(
                    format, source.PageCount, AllocationsAsync(compared, global, cancellationToken), destination, ledger, AllocationBudget(budget.Allocation, working), cancellationToken).ConfigureAwait(false);
            }

            admission.Check(cancellationToken);
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // No construction handle remains open. Factory ownership is transferred
        // before cancellation is checked again, so even a late cancellation closes it.
        admission.Check(cancellationToken);
        Stream reopenedDestination = await workspace.Open(CompactPinnedDatabaseFile.Destination, 0, cancellationToken).ConfigureAwait(false);
        Guard.NotNull(reopenedDestination, nameof(workspace));
        await using (reopenedDestination.ConfigureAwait(false))
        {
            RejectClosedWriter(reopenedDestination, created);
            admission.Check(cancellationToken);
            Stream reopenedLedger = await workspace.Open(CompactPinnedDatabaseFile.AllocationLedger, 0, cancellationToken).ConfigureAwait(false);
            Guard.NotNull(reopenedLedger, nameof(workspace));
            await using (reopenedLedger.ConfigureAwait(false))
            {
                RejectClosedWriter(reopenedLedger, created);
                CompactPinnedDatabaseValidatorResult validation = await CompactPinnedDatabaseValidator.ValidateAsync(
                    source, format, compared, reopenedDestination, new(global.AppendPointPages, budget.GlobalMap), reopenedLedger, allocation, ValidationBudget(budget.Validation, working, budget.MaxPages, budget.MaxTables), cancellationToken, workspace.ObserveUnassignedRow).ConfigureAwait(false);
                if (validation.ActiveTables != activeTables || validation.UnassignedRows != unassignedRows)
                {
                    throw Corrupt("Independent active-table and pinned unassigned-row coverage differs from construction.");
                }

                admission.Check(cancellationToken);
                return new(membership, validation, admission.ReadBytes, admission.WrittenBytes);
            }
        }
    }

    private static void ValidateArguments(IPageSource source, JetFormat format, IReadOnlyList<CompactPinnedDatabaseBuildTable> tables, CompactPinnedDatabaseWorkspace workspace, CompactPinnedDatabaseBuilderBudget budget)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(tables, nameof(tables));
        Guard.NotNull(workspace, nameof(workspace));
        Guard.NotNull(workspace.Create, nameof(workspace));
        Guard.NotNull(workspace.Open, nameof(workspace));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.Tables, nameof(budget));
        Guard.NotNull(budget.ActiveTables, nameof(budget));
        Guard.NotNull(budget.GlobalMap, nameof(budget));
        Guard.NotNull(budget.Allocation, nameof(budget));
        Guard.NotNull(budget.Validation, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, 0x1000000L, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxTables, 0, int.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWriteBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        foreach (CompactPinnedDatabaseBuildTable table in tables)
        {
            Guard.NotNull(table, nameof(tables));
            if (table.ActiveIndexOrdinal.HasValue && workspace.ObserveUnassignedRow is null)
            {
                throw new ArgumentException("Active-table selection requires a pinned unassigned-row observer before any output is created.", nameof(workspace));
            }
        }
    }

    private static async ValueTask CopyPrefixAsync(Admission admission, Stream destination, CancellationToken token)
    {
        for (long pageNumber = 0; pageNumber < admission.Source.PageCount; pageNumber++)
        {
            byte[] page = await admission.Source.ReadPageAsync(pageNumber, token).ConfigureAwait(false);
            try
            {
                await destination.WriteAsync(page.AsMemory(0, admission.Source.PageSize), token).ConfigureAwait(false);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }
    }

    private static async ValueTask<(CompactPinnedTableAssemblyAllocation Allocation, CompactActiveTableResult? Active)> BuildTableAsync(
        CompactPinnedTableAssemblySession assembly, CompactPinnedDatabaseBuildTable table, CompactPinnedDatabaseWorkspace workspace, Stream destination, List<Stream> created, Admission admission, CompactActiveTableBudget budget, CancellationToken token)
    {
        if (table.ActiveIndexOrdinal is not int ordinal)
        {
            return (await BuildTableStorageAsync(assembly, table, workspace, destination, created, admission, token).ConfigureAwait(false), null);
        }

        using CompactActiveTableRows active = await CompactActiveTableRows.ValidateAsync(
            assembly.Discovery.SourcePages,
            assembly.Format,
            table.Owner,
            table.Definition,
            ordinal,
            new() { Work = budget.Work, Accounting = assembly.Discovery.Accounting, MaxActiveRows = budget.MaxActiveRows, MaxKeyBytes = budget.MaxKeyBytes },
            workspace.ObserveUnassignedRow!,
            token).ConfigureAwait(false);
        CompactPinnedTableAssemblyAllocation built = await assembly.Discovery.WithActiveTableRowsAsync(
            active,
            table.Definition,
            scopedToken => BuildTableStorageAsync(assembly, table, workspace, destination, created, admission, scopedToken),
            token).ConfigureAwait(false);
        return (built, active.Result);
    }

    private static async ValueTask<CompactPinnedTableAssemblyAllocation> BuildTableStorageAsync(
        CompactPinnedTableAssemblySession assembly, CompactPinnedDatabaseBuildTable table, CompactPinnedDatabaseWorkspace workspace, Stream destination, List<Stream> created, Admission admission, CancellationToken token)
    {
        await using Stream rowOutput = await CreateAsync(workspace, CompactPinnedDatabaseFile.Rows, table.Owner, created, admission, token).ConfigureAwait(false);
        await using Stream valueOutput = await CreateAsync(workspace, CompactPinnedDatabaseFile.LongValues, table.Owner, created, admission, token).ConfigureAwait(false);
        await using Stream firstOutput = await CreateAsync(workspace, CompactPinnedDatabaseFile.FirstFence, table.Owner, created, admission, token).ConfigureAwait(false);
        await using Stream secondOutput = await CreateAsync(workspace, CompactPinnedDatabaseFile.SecondFence, table.Owner, created, admission, token).ConfigureAwait(false);
        await using var rows = new AdmittedStream(rowOutput, admission, destination: false);
        await using var values = new AdmittedStream(valueOutput, admission, destination: false);
        await using var first = new AdmittedStream(firstOutput, admission, destination: false);
        await using var second = new AdmittedStream(secondOutput, admission, destination: false);
        CompactPinnedTableAssemblyAllocation result = await CompactPinnedTableAssembly.BuildAsync(
            assembly, table.Owner, table.Definition, table.Indexes, destination, rows, values, first, second, token, table.HydratedColumns).ConfigureAwait(false);
        await rows.FlushAsync(token).ConfigureAwait(false);
        await values.FlushAsync(token).ConfigureAwait(false);
        return result;
    }

    private static async ValueTask<Stream> CreateAsync(CompactPinnedDatabaseWorkspace workspace, CompactPinnedDatabaseFile role, long owner, List<Stream> created, Admission admission, CancellationToken token)
    {
        admission.Check(token);
        Stream stream = await workspace.Create(role, owner, token).ConfigureAwait(false);
        Guard.NotNull(stream, nameof(workspace));
        foreach (Stream earlier in created)
        {
            if (ReferenceEquals(stream, earlier) || (stream is FileStream file && earlier is FileStream other
                && string.Equals(file.Name, other.Name, StringComparison.OrdinalIgnoreCase)))
            {
                // A returned existing handle is still owned by its earlier scope.
                if (!ReferenceEquals(stream, earlier))
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }

                throw new ArgumentException("Every private role and owner requires distinct backing storage, including through wrappers.", nameof(workspace));
            }
        }

        try
        {
            admission.Check(token);
            if (!stream.CanRead || !stream.CanWrite || !stream.CanSeek || stream.Length != 0)
            {
                throw new ArgumentException("Private construction factories must return empty readable writable seekable streams.", nameof(workspace));
            }

            stream.Position = 0;
            created.Add(stream);
            return stream;
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static void RejectClosedWriter(Stream reopened, List<Stream> created)
    {
        foreach (Stream writer in created)
        {
            if (ReferenceEquals(reopened, writer))
            {
                throw new ArgumentException("Comparison must independently reopen closed construction output.", nameof(reopened));
            }
        }
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

    private static CompactGlobalMapBudget MapBudget(CompactGlobalMapBudget budget, long working)
        => new()
        {
            MaxPages = budget.MaxPages,
            MaxBitmapPages = budget.MaxBitmapPages,
            MaxReadBytes = budget.MaxReadBytes,
            MaxWriteBytes = budget.MaxWriteBytes,
            MaxWorkingBytes = Math.Min(budget.MaxWorkingBytes, working),
        };

    private static CompactGlobalAllocationBudget AllocationBudget(CompactGlobalAllocationBudget budget, long working)
        => new()
        {
            MaxPages = budget.MaxPages,
            MaxRecords = budget.MaxRecords,
            MaxLedgerBytes = budget.MaxLedgerBytes,
            MaxReadBytes = budget.MaxReadBytes,
            MaxWorkingBytes = Math.Min(budget.MaxWorkingBytes, working),
        };

    private static CompactPinnedDatabaseValidatorBudget ValidationBudget(CompactPinnedDatabaseValidatorBudget budget, long working, long pages, int tables)
        => new()
        {
            MaxPages = Math.Min(budget.MaxPages, pages),
            MaxTables = Math.Min(budget.MaxTables, tables),
            MaxReadBytes = budget.MaxReadBytes,
            MaxWorkingBytes = Math.Min(budget.MaxWorkingBytes, working),
            Tables = budget.Tables,
            GlobalMap = budget.GlobalMap,
            Prefix = budget.Prefix,
            Allocation = budget.Allocation,
            Inventory = budget.Inventory,
            ActiveCatalog = budget.ActiveCatalog,
            ActiveTables = budget.ActiveTables,
        };

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> AllocationsAsync(IReadOnlyList<CompactPinnedDatabaseTable> tables, CompactGlobalMapResult global, [EnumeratorCancellation] CancellationToken token)
    {
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            await foreach (CompactGlobalAllocationRecord record in table.Allocation.AllocationsAsync(token).ConfigureAwait(false))
            {
                yield return record;
            }
        }

        token.ThrowIfCancellationRequested();
        if (global.BitmapPages > 0)
        {
            yield return new(CompactGlobalAllocationKind.UsageMap, 1, new(global.AppendPointPages, global.BitmapPages));
        }
    }

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.CorruptPage, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private sealed class Admission
    {
        private readonly IPageSource original;
        private readonly JetFormat format;
        private readonly CompactPinnedDatabaseBuilderBudget budget;
        private readonly long sourcePages;
        private readonly CancellationToken token;

        internal Admission(IPageSource source, JetFormat format, CompactPinnedDatabaseBuilderBudget budget, CancellationToken token)
        {
            this.original = source;
            this.format = format;
            this.budget = budget;
            this.sourcePages = source.PageCount;
            this.token = token;
            this.Source = new AdmittedSource(this);
            this.Check(token);
            if (this.sourcePages < 3)
            {
                throw Corrupt("The original database does not contain a complete native prefix.");
            }

            this.Output(checked(this.sourcePages * format.PageSize));
        }

        internal IPageSource Source { get; }

        internal long ReadBytes { get; private set; }

        internal long WrittenBytes { get; private set; }

        internal void Check(CancellationToken token)
        {
            this.original.ThrowIfDisposedOrCancelled(this.token);
            token.ThrowIfCancellationRequested();
            if (this.original.PageCount != this.sourcePages || this.original.PageSize != this.format.PageSize)
            {
                throw Corrupt("The immutable original database geometry changed during private construction.");
            }
        }

        internal void Read(int bytes, CancellationToken token)
        {
            this.Check(token);
            if (bytes > this.budget.MaxReadBytes - this.ReadBytes)
            {
                throw Limit("Private database construction exceeds cumulative MaxReadBytes.");
            }

            this.ReadBytes += bytes;
        }

        internal void Write(long position, int bytes, bool destination, CancellationToken token)
        {
            this.Check(token);
            if (bytes > this.budget.MaxWriteBytes - this.WrittenBytes)
            {
                throw Limit("Private database construction exceeds cumulative MaxWriteBytes.");
            }

            if (destination)
            {
                this.Output(checked(position + bytes));
            }

            this.WrittenBytes += bytes;
        }

        private void Output(long bytes)
        {
            if (bytes > this.budget.MaxOutputBytes || bytes > this.budget.MaxPages * this.format.PageSize)
            {
                throw Limit("Private database complete output exceeds MaxPages or MaxOutputBytes.");
            }
        }

        private sealed class AdmittedSource(Admission admission) : IPageSource
        {
            public int PageSize => admission.format.PageSize;

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

    /// <summary>Non-owning wrapper sharing construction admission without changing factory ownership.</summary>
    /// <param name="inner">The separately owned factory stream.</param>
    /// <param name="admission">The whole-build cumulative request admission.</param>
    /// <param name="destination">Whether complete destination output bounds also apply.</param>
    private sealed class AdmittedStream(Stream inner, Admission admission, bool destination) : Stream
    {
        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => inner.CanWrite;

        public override long Length => inner.Length;

        public override long Position { get => inner.Position; set => inner.Position = value; }

        public override void Flush() => inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count)
        {
            admission.Read(count, default);
            return inner.Read(buffer, offset, count);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            admission.Read(buffer.Length, cancellationToken);
            return inner.ReadAsync(buffer, cancellationToken);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => this.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value)
        {
            if (destination || value < 0 || value > inner.Length)
            {
                throw new NotSupportedException();
            }

            inner.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            admission.Write(inner.Position, count, destination, default);
            inner.Write(buffer, offset, count);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            admission.Write(inner.Position, buffer.Length, destination, cancellationToken);
            return inner.WriteAsync(buffer, cancellationToken);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => this.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }
}
