namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;

/// <summary>Composes one raw table, all physical stored indexes and pointer-only installation on its pinned chain.</summary>
/// <remarks>
/// The immutable decrypted source, exclusive private decrypted destination and four disk-capable scratch streams
/// have distinct backing storage, including through wrappers. All caller evidence and projections remain immutable.
/// Construction appends raw DATA/LVAL/maps, relocates stored index bookmarks without decoding rows or regenerating
/// keys, patches only known pointer words and installs that unvalidated image on its original definition identities.
/// Reopened validation independently checks original entries and ledgers, exact private allocations and installed
/// definition bytes; it consumes no builder fences or installer completion proof. It accepts a later private suffix
/// for other tables or global maps, which requires separate allocation and database validation.
/// Physical root/map sharing is refused conservatively; logical sharing within a physical descriptor is retained.
/// Every source page remains pinned. Catalog completeness, global allocation, encryption, unknown incoming references,
/// native mutation acceptance and durable publication remain separate requirements. Completion never authorizes
/// reclamation or database replacement. Failures leave private partial output and no completion evidence.
/// </remarks>
internal static class CompactPinnedTableAssembly
{
    /// <summary>Stages and installs one private table using caller-supplied independent original stored entries.</summary>
    /// <param name="assembly">The explicitly serialized source and cumulative database-wide admission.</param>
    /// <param name="owner">The original pinned table identity.</param>
    /// <param name="definition">The complete immutable original raw definition.</param>
    /// <param name="indexes">Complete original observations in physical descriptor order.</param>
    /// <param name="privateDestination">The distinct private page-aligned source clone or assembly destination.</param>
    /// <param name="rowLedger">The empty readable writable seekable row ledger.</param>
    /// <param name="allocationLedger">The distinct empty readable writable seekable LVAL ledger.</param>
    /// <param name="firstFenceStream">The distinct empty readable writable seekable first fence scratch stream.</param>
    /// <param name="secondFenceStream">The distinct empty readable writable seekable second fence scratch stream.</param>
    /// <param name="cancellationToken">Cancels admission, copying, index enumeration, installation and I/O.</param>
    /// <param name="hydratedColumns">Optional independently verified calculated ResultType projections.</param>
    /// <returns>Unvalidated complete private allocations, requiring independent reopened comparison.</returns>
    /// <exception cref="ArgumentException">The source identities, private destination or distinct empty scratch streams are invalid.</exception>
    /// <exception cref="JetLimitationException">A cumulative allowance is exhausted or physical sharing is unsupported.</exception>
    /// <exception cref="JetCorruptDataException">Raw storage, stored index entries or pinned chain bytes disagree.</exception>
    internal static async ValueTask<CompactPinnedTableAssemblyAllocation> BuildAsync(
        CompactPinnedTableAssemblySession assembly,
        long owner,
        TDefImage definition,
        IReadOnlyList<CompactPinnedTableIndexSource> indexes,
        Stream privateDestination,
        Stream rowLedger,
        Stream allocationLedger,
        Stream firstFenceStream,
        Stream secondFenceStream,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null)
    {
        Guard.NotNull(assembly, nameof(assembly));
        IPageSource source = assembly.Source;
        JetFormat format = assembly.Format;
        CompactPinnedTableAssemblyBudget budget = assembly.Budget;
        ValidateInputs(source, format, owner, definition, indexes, budget, cancellationToken);
        ValidateStreams(writable: true, privateDestination, rowLedger, allocationLedger, firstFenceStream, secondFenceStream);
        if (privateDestination.Length % format.PageSize != 0 || privateDestination.Length / format.PageSize < source.PageCount)
        {
            throw new ArgumentException("The private append point must retain the complete page-aligned pinned source prefix.", nameof(privateDestination));
        }

        long firstPage = privateDestination.Length / format.PageSize;
        using CompactPinnedTableAssemblySession.Operation admission = assembly.Enter(privateDestination.Length, cancellationToken);
        await using var destination = new AdmittedStream(privateDestination, admission, destination: true, writable: true);
        await using var rows = new AdmittedStream(rowLedger, admission, destination: false, writable: true);
        await using var values = new AdmittedStream(allocationLedger, admission, destination: false, writable: true);
        await using var firstFence = new AdmittedStream(firstFenceStream, admission, destination: false, writable: true);
        await using var secondFence = new AdmittedStream(secondFenceStream, admission, destination: false, writable: true);
        CompactDiscoverySession session = assembly.Discovery;
        long retained = Reservation(definition, indexes.Count);
        using IDisposable composition = session.Accounting.ReserveWorkingBytes(retained, owner);
        long activeWorkingBytes = RemainingWorkingBytes(budget, session);
        CompactTDefIndexReferences[] references = OriginalReferences(indexes);
        CompactRawTableBuilderResult table = await CompactRawTableBuilder.BuildAsync(
            session, format, owner, definition, destination, rows, values, references, TableBuilderBudget(budget.TableBuilder, activeWorkingBytes), cancellationToken, hydratedColumns).ConfigureAwait(false);
        var allocations = new CompactRawIndexAssemblyAllocation[indexes.Count];
        CompactRowRelocationMap relocation = await CompactRowRelocationMap.OpenAsync(rows, table.Data.RowCount, Math.Max(1, budget.MaxReadBytes), cancellationToken).ConfigureAwait(false);
        for (int index = 0; index < allocations.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            firstFence.SetLength(0);
            secondFence.SetLength(0);
            firstFence.Position = 0;
            secondFence.Position = 0;
            CompactRawIndexAssemblyAllocation allocation = await CompactRawIndexAssembly.BuildAsync(
                format, owner, admission.ObserveAsync(indexes[index].Entries, cancellationToken), relocation, destination, firstFence, secondFence, IndexBudget(budget.Index, activeWorkingBytes), cancellationToken).ConfigureAwait(false);
            allocations[index] = allocation;
            references[index] = new(checked((uint)allocation.Tree.RootPageNumber), allocation.OwnedPages.Pointer);
        }

        var valueReferences = new CompactTDefLongValueReferences[table.LongValueMaps.Count];
        for (int index = 0; index < valueReferences.Length; index++)
        {
            CompactRawTableLongValueMaps column = table.LongValueMaps[index];
            valueReferences[index] = new(column.ColumnNumber, column.OwnedPages.Pointer, column.FreePages.Pointer);
        }

        table = table with
        {
            PatchedDefinition = CompactTDefReferencePatcher.Patch(format, definition, table.OwnedPages.Pointer, table.FreePages.Pointer, references, valueReferences, activeWorkingBytes, cancellationToken),
        };
        await CompactTDefChainInstaller.InstallAsync(assembly.DefinitionSource, format, [new(owner, definition, table.PatchedDefinition)], destination, ChainBudget(budget.Definition, activeWorkingBytes), cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        destination.Position = destination.Length;
        return new(owner, firstPage, (destination.Length / format.PageSize) - firstPage, table, Array.AsReadOnly(allocations));
    }

    /// <summary>Independently compares a reopened table pipeline using immutable original evidence and reopened ledgers.</summary>
    /// <param name="assembly">The independent serialized source and cumulative database-wide comparison admission.</param>
    /// <param name="owner">The independently known original pinned table identity.</param>
    /// <param name="definition">The complete independent immutable original definition.</param>
    /// <param name="indexes">Independent immutable original stored entries in physical descriptor order.</param>
    /// <param name="reopenedDestination">The immutable reopened readable seekable private decrypted destination.</param>
    /// <param name="rowLedger">The distinct reopened complete immutable row ledger.</param>
    /// <param name="allocationLedger">The distinct reopened complete immutable per-column LVAL ledger.</param>
    /// <param name="allocation">The untrusted complete private allocation claim.</param>
    /// <param name="cancellationToken">Cancels admission, every comparison, source observation and private read.</param>
    /// <param name="hydratedColumns">Optional independently verified calculated ResultType projections.</param>
    /// <returns>Private component comparison evidence, without a complete database or ownership verdict.</returns>
    /// <exception cref="ArgumentException">The immutable source identities or distinct reopened streams are invalid.</exception>
    /// <exception cref="JetCorruptDataException">Raw values, independent observations, allocations, maps or pinned chain bytes disagree.</exception>
    /// <exception cref="JetLimitationException">A cumulative comparison allowance is exhausted or physical sharing is unsupported.</exception>
    internal static async ValueTask<CompactPinnedTableAssemblyResult> ValidateAsync(
        CompactPinnedTableAssemblySession assembly,
        long owner,
        TDefImage definition,
        IReadOnlyList<CompactPinnedTableIndexSource> indexes,
        Stream reopenedDestination,
        Stream rowLedger,
        Stream allocationLedger,
        CompactPinnedTableAssemblyAllocation allocation,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null)
    {
        Guard.NotNull(assembly, nameof(assembly));
        IPageSource source = assembly.Source;
        JetFormat format = assembly.Format;
        CompactPinnedTableAssemblyBudget budget = assembly.Budget;
        ValidateInputs(source, format, owner, definition, indexes, budget, cancellationToken);
        ValidateStreams(writable: false, reopenedDestination, rowLedger, allocationLedger);
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(allocation.Table, nameof(allocation));
        Guard.NotNull(allocation.Table.LongValueMaps, nameof(allocation));
        Guard.NotNull(allocation.Indexes, nameof(allocation));
        if (allocation.Owner != owner || allocation.Indexes.Count != indexes.Count || allocation.FirstPageNumber < source.PageCount
            || allocation.Table.LongValueMaps.Count > Constants.TableDefinition.MaxColumns
            || allocation.FirstPageNumber is < 3 or > 0xFFFFFF || allocation.PageCount < 1
            || allocation.PageCount > 0x1000000L - allocation.FirstPageNumber || reopenedDestination.Length % format.PageSize != 0
            || allocation.FirstPageNumber + allocation.PageCount > reopenedDestination.Length / format.PageSize)
        {
            throw Corrupt("The complete private table allocation, original identity or physical index count disagrees.");
        }

        if (allocation.PageCount > budget.MaxPages || allocation.PageCount > budget.MaxOutputBytes / format.PageSize)
        {
            throw Limit("Private table comparison exceeds aggregate MaxPages or MaxOutputBytes.");
        }

        using CompactPinnedTableAssemblySession.Operation admission = assembly.Enter(reopenedDestination.Length, cancellationToken);
        admission.Compare(allocation.PageCount);
        long next = allocation.FirstPageNumber;
        long allocationEnd = allocation.FirstPageNumber + allocation.PageCount;
        await foreach (CompactGlobalAllocationRecord record in allocation.AllocationsAsync(cancellationToken).ConfigureAwait(false))
        {
            if (record.Pages.FirstPageNumber != next || record.Pages.PageCount <= 0 || record.Pages.PageCount > allocationEnd - next)
            {
                throw Corrupt("Complete private table allocations overlap, have gaps or exceed native page storage.");
            }

            next += record.Pages.PageCount;
        }

        if (allocation.Table.Data.FirstPageNumber != allocation.FirstPageNumber || next != allocation.FirstPageNumber + allocation.PageCount)
        {
            throw Corrupt("The complete private table extent disagrees with its independently checked allocation sequence.");
        }

        CompactDiscoverySession session = assembly.Discovery;
        long retained = Reservation(definition, indexes.Count);
        using IDisposable composition = session.Accounting.ReserveWorkingBytes(retained, owner);
        long activeWorkingBytes = RemainingWorkingBytes(budget, session);
        await using var destination = new AdmittedStream(reopenedDestination, admission, destination: true, writable: false);
        await using var rows = new AdmittedStream(rowLedger, admission, destination: false, writable: false);
        await using var values = new AdmittedStream(allocationLedger, admission, destination: false, writable: false);
        CompactRowRelocationMap relocation = await CompactRowRelocationMap.OpenAsync(rows, allocation.Table.Data.RowCount, Math.Max(1, budget.MaxReadBytes), cancellationToken).ConfigureAwait(false);
        var verifiedIndexes = new CompactRawIndexAssemblyResult[indexes.Count];
        var references = new CompactTDefIndexReferences[indexes.Count];
        for (int index = 0; index < indexes.Count; index++)
        {
            verifiedIndexes[index] = await CompactRawIndexAssembly.ValidateAsync(
                format, owner, destination, admission.ObserveAsync(indexes[index].Entries, cancellationToken), relocation, allocation.Indexes[index], IndexBudget(budget.Index, activeWorkingBytes), cancellationToken).ConfigureAwait(false);
            references[index] = verifiedIndexes[index].References;
        }

        CompactRawTableValidatorResult table = await CompactRawTableValidator.ValidateAsync(
            session, owner, definition, destination, rows, values, allocation.Table, references, TableValidatorBudget(budget.TableValidator, activeWorkingBytes), cancellationToken, hydratedColumns).ConfigureAwait(false);
        CompactTDefChainResult chain = await CompactTDefChainValidator.ValidateAsync(
            assembly.DefinitionSource, format, [new(owner, definition, allocation.Table.PatchedDefinition)], destination, ChainBudget(budget.Definition, activeWorkingBytes), cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return new(table, Array.AsReadOnly(verifiedIndexes), chain, admission.ReadBytes);
    }

    private static void ValidateInputs(IPageSource source, JetFormat format, long owner, TDefImage definition, IReadOnlyList<CompactPinnedTableIndexSource> indexes, CompactPinnedTableAssemblyBudget budget, CancellationToken token)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(indexes, nameof(indexes));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.Discovery, nameof(budget));
        Guard.NotNull(budget.TableBuilder, nameof(budget));
        Guard.NotNull(budget.TableValidator, nameof(budget));
        Guard.NotNull(budget.Index, nameof(budget));
        Guard.NotNull(budget.Definition, nameof(budget));
        token.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWriteBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxObservedEntries, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxObservedKeyBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (source.PageSize != format.PageSize || owner <= 0 || owner >= source.PageCount
            || definition.Issues != TDefParseIssues.None || (definition.TDefPageNumber.HasValue && definition.TDefPageNumber != owner)
            || definition.RealIndexes.Count != indexes.Count || indexes.Count > Constants.TableDefinition.MaxIndexes)
        {
            throw new ArgumentException("The complete original definition, pinned identity and physical index observations must agree.", nameof(definition));
        }

        if (Reservation(definition, indexes.Count) >= budget.MaxWorkingBytes)
        {
            throw Limit("Pinned table composition exceeds MaxWorkingBytes.");
        }

        byte[] bytes = definition.CopyBytes();
        for (int index = 0; index < indexes.Count; index++)
        {
            token.ThrowIfCancellationRequested();
            CompactPinnedTableIndexSource observed = indexes[index];
            Guard.NotNull(observed, nameof(indexes));
            Guard.NotNull(observed.Entries, nameof(indexes));
            if (observed.RootPage is 0 or > 0xFFFFFF || observed.RootPage != definition.RealIndexes[index].RootPage
                || !UsageMap.TryReadPointer(bytes, definition.RealIndexes[index].Slot.FirstDpOffset - 4, out UsageMapPointer map)
                || observed.OwnedPages != map || map.PageNumber <= 0 || map.PageNumber >= source.PageCount
                || observed.RootPage >= source.PageCount)
            {
                throw new ArgumentException("Original stored-index observations must match their complete physical descriptor identities.", nameof(indexes));
            }

            for (int earlier = 0; earlier < index; earlier++)
            {
                if (indexes[earlier].RootPage == observed.RootPage || indexes[earlier].OwnedPages == observed.OwnedPages)
                {
                    throw new JetLimitationException(JetErrorCode.FeatureNotSupported, "Aliased physical index roots or ownership-map rows require independent sharing support.");
                }
            }
        }
    }

    private static long RemainingWorkingBytes(CompactPinnedTableAssemblyBudget budget, CompactDiscoverySession session)
    {
        long available = budget.MaxWorkingBytes - session.Accounting.ActiveWorkingBytes;
        if (available <= 0)
        {
            throw Limit("Pinned table composition leaves no active-component MaxWorkingBytes allowance.");
        }

        return available;
    }

    private static long Reservation(TDefImage definition, int indexes)
        => (2L * definition.LogicalByteLength) + 131_072 + (1024L * indexes)
            + (1024L * Math.Min(Constants.TableDefinition.MaxColumns, definition.LogicalByteLength / 10));

    private static CompactRawTableBuilderBudget TableBuilderBudget(CompactRawTableBuilderBudget supplied, long workingBytes)
        => new()
        {
            MaxDataRows = supplied.MaxDataRows,
            MaxValues = supplied.MaxValues,
            MaxLongValueRows = supplied.MaxLongValueRows,
            MaxPages = supplied.MaxPages,
            MaxOutputBytes = supplied.MaxOutputBytes,
            MaxRowLedgerBytes = supplied.MaxRowLedgerBytes,
            MaxAllocationLedgerBytes = supplied.MaxAllocationLedgerBytes,
            MaxRawBytes = supplied.MaxRawBytes,
            MaxReadBytes = supplied.MaxReadBytes,
            MaxWorkingBytes = Math.Min(supplied.MaxWorkingBytes, workingBytes),
        };

    private static CompactRawTableValidatorBudget TableValidatorBudget(CompactRawTableValidatorBudget supplied, long workingBytes)
        => new()
        {
            RawValues = supplied.RawValues,
            Maps = supplied.Maps,
            MaxReadBytes = supplied.MaxReadBytes,
            MaxWorkingBytes = Math.Min(supplied.MaxWorkingBytes, workingBytes),
        };

    private static CompactRawIndexAssemblyBudget IndexBudget(CompactRawIndexAssemblyBudget supplied, long workingBytes)
        => new()
        {
            MaxPages = supplied.MaxPages,
            MaxOutputBytes = supplied.MaxOutputBytes,
            MaxEntries = supplied.MaxEntries,
            MaxCanonicalBytes = supplied.MaxCanonicalBytes,
            MaxFenceBytes = supplied.MaxFenceBytes,
            MaxReadBytes = supplied.MaxReadBytes,
            MaxLevels = supplied.MaxLevels,
            MaxWorkingBytes = Math.Min(supplied.MaxWorkingBytes, workingBytes),
            IndexAudit = supplied.IndexAudit,
        };

    private static CompactTDefChainBudget ChainBudget(CompactTDefChainBudget supplied, long workingBytes)
        => new()
        {
            MaxPages = supplied.MaxPages,
            MaxReadBytes = supplied.MaxReadBytes,
            MaxWriteBytes = supplied.MaxWriteBytes,
            MaxWorkingBytes = Math.Min(supplied.MaxWorkingBytes, workingBytes),
        };

    private static CompactTDefIndexReferences[] OriginalReferences(IReadOnlyList<CompactPinnedTableIndexSource> indexes)
    {
        var references = new CompactTDefIndexReferences[indexes.Count];
        for (int index = 0; index < references.Length; index++)
        {
            references[index] = new(indexes[index].RootPage, indexes[index].OwnedPages);
        }

        return references;
    }

    private static void ValidateStreams(bool writable, params Stream[] streams)
    {
        for (int index = 0; index < streams.Length; index++)
        {
            Stream stream = streams[index];
            Guard.NotNull(stream, nameof(streams));
            if (!stream.CanRead || !stream.CanSeek || (writable && (!stream.CanWrite || (index > 0 && stream.Length != 0))))
            {
                throw new ArgumentException("Streams must be readable and seekable; construction requires writable output and empty writable scratch.", nameof(streams));
            }

            for (int earlier = 0; earlier < index; earlier++)
            {
                if (ReferenceEquals(stream, streams[earlier]) || (stream is FileStream file && streams[earlier] is FileStream other
                    && string.Equals(file.Name, other.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new ArgumentException("The destination and every scratch stream must have distinct backing storage.", nameof(streams));
                }
            }
        }
    }

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.MalformedValue, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    /// <summary>Non-owning wrapper sharing cumulative admission across all private output and scratch streams.</summary>
    /// <param name="inner">The caller stream, whose lifetime remains caller-owned.</param>
    /// <param name="admission">The whole-phase shared admission.</param>
    /// <param name="destination">Whether append output bounds apply in addition to cumulative I/O bounds.</param>
    /// <param name="writable">Whether this phase permits writing.</param>
    private sealed class AdmittedStream(Stream inner, CompactPinnedTableAssemblySession.Operation admission, bool destination, bool writable) : Stream
    {
        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => writable && inner.CanWrite;

        public override long Length => inner.Length;

        public override long Position { get => inner.Position; set => inner.Position = value; }

        public override void Flush() => inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count)
        {
            admission.Read(count);
            return inner.Read(buffer, offset, count);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            admission.Read(buffer.Length);
            return inner.ReadAsync(buffer, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value)
        {
            if (!writable || destination || value < 0 || value > inner.Length)
            {
                throw new NotSupportedException();
            }

            inner.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!writable)
            {
                throw new NotSupportedException();
            }

            admission.Write(inner.Position, count, destination);
            inner.Write(buffer, offset, count);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!writable)
            {
                throw new NotSupportedException();
            }

            admission.Write(inner.Position, buffer.Length, destination);
            return inner.WriteAsync(buffer, cancellationToken);
        }
    }
}
