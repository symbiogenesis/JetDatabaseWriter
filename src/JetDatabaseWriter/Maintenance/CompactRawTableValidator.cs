namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>Composes independent raw comparison, original-column identities and exact private map validation.</summary>
/// <remarks>
/// The source, reopened destination and both ledgers must retain distinct immutable backing storage, including through
/// wrappers. Original definitions, projections, allocation evidence and caller-verified index references remain immutable.
/// Expected map membership comes from independently compared rows and the completely compared original-column ledger,
/// never from the maps under test. Every data, LVAL and map page must occupy its exact contiguous assembly position.
/// The logical patched image is compared independently; installing that image on its pinned chain requires separate
/// validation. Index keys, catalog completeness, global allocation and unknown incoming ownership remain separate.
/// Completion does not authorize reclamation, publication or replacement and does not establish complete compaction.
/// </remarks>
internal static class CompactRawTableValidator
{
    /// <summary>Checks a reopened private table, every map and its complete raw logical definition patch.</summary>
    /// <param name="session">The immutable source and cumulative discovery accounting.</param>
    /// <param name="owner">The original pinned TDEF identity.</param>
    /// <param name="definition">The verified immutable original logical definition.</param>
    /// <param name="destination">The reopened immutable readable seekable decrypted destination.</param>
    /// <param name="rowLedger">The distinct reopened complete row relocation ledger.</param>
    /// <param name="allocationLedger">The distinct reopened complete per-column allocation ledger.</param>
    /// <param name="allocation">The complete raw table assembly evidence to compare.</param>
    /// <param name="indexes">Independently verified complete physical index replacements in original order.</param>
    /// <param name="budget">Cumulative private I/O, map work and simultaneous storage limits.</param>
    /// <param name="cancellationToken">Cancels preflight, source scans, map enumeration and every read.</param>
    /// <param name="hydratedColumns">Optional verified persisted ResultType projections.</param>
    /// <returns>Raw bytes, column correspondence, map membership and private allocation coverage evidence only.</returns>
    /// <exception cref="ArgumentException">Streams or immutable inputs cannot be compared independently.</exception>
    /// <exception cref="JetCorruptDataException">Raw bytes, identities, maps, image or allocation coverage disagree.</exception>
    /// <exception cref="JetLimitationException">A cumulative work, read or working-storage limit is exhausted.</exception>
    internal static async ValueTask<CompactRawTableValidatorResult> ValidateAsync(
        CompactDiscoverySession session,
        long owner,
        TDefImage definition,
        Stream destination,
        Stream rowLedger,
        Stream allocationLedger,
        CompactRawTableBuilderResult allocation,
        IReadOnlyList<CompactTDefIndexReferences> indexes,
        CompactRawTableValidatorBudget budget,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(rowLedger, nameof(rowLedger));
        Guard.NotNull(allocationLedger, nameof(allocationLedger));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(indexes, nameof(indexes));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.RawValues, nameof(budget));
        Guard.NotNull(budget.Maps, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        if (Aliases(destination, rowLedger) || Aliases(destination, allocationLedger) || Aliases(rowLedger, allocationLedger)
            || !destination.CanRead || !destination.CanSeek || !rowLedger.CanRead || !rowLedger.CanSeek
            || !allocationLedger.CanRead || !allocationLedger.CanSeek)
        {
            throw new ArgumentException("The reopened destination and both ledgers must be distinct readable seekable streams.", nameof(destination));
        }

        JetFormat format = session.SourceFormat;
        long destinationLength = destination.Length;
        long reservation = (2L * definition.LogicalByteLength) + format.PageSize + 65_536
            + (256L * Math.Min(Constants.TableDefinition.MaxColumns, definition.LogicalByteLength / 10));
        if (reservation >= budget.MaxWorkingBytes)
        {
            throw Limit("Complete raw table validation exceeds MaxWorkingBytes.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(reservation, owner);
        if (!LongValueUsageMaps.TryRead(format, definition.CopyBytes(), out List<LongValueUsageMap> originalMaps, out _)
            || originalMaps.Count != allocation.LongValueMaps.Count)
        {
            throw Corrupt("The complete private table must retain every original LVAL map identity.");
        }

        long next = allocation.Data.FirstPageNumber;
        Advance(allocation.Data.FirstPageNumber, allocation.Data.PageCount, ref next);
        Advance(allocation.LongValues.FirstPageNumber, allocation.LongValues.PageCount, ref next);
        Advance(allocation.OwnedPages.FirstPageNumber, allocation.OwnedPages.PageCount, ref next);
        Advance(allocation.FreePages.FirstPageNumber, allocation.FreePages.PageCount, ref next);
        var references = new CompactTDefLongValueReferences[originalMaps.Count];
        for (int i = 0; i < originalMaps.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CompactRawTableLongValueMaps column = allocation.LongValueMaps[i];
            if (column.ColumnNumber != originalMaps[i].ColumnNumber)
            {
                throw Corrupt("Private table map identities or original trailer order disagree.");
            }

            Advance(column.OwnedPages.FirstPageNumber, column.OwnedPages.PageCount, ref next);
            Advance(column.FreePages.FirstPageNumber, column.FreePages.PageCount, ref next);
            references[i] = new(column.ColumnNumber, column.OwnedPages.Pointer, column.FreePages.Pointer);
        }

        if (destination.Length % format.PageSize != 0 || next > destination.Length / format.PageSize)
        {
            throw Corrupt("The complete private table allocation extends beyond the reopened destination.");
        }

        byte[] expected = CompactTDefReferencePatcher.Patch(format, definition, allocation.OwnedPages.Pointer, allocation.FreePages.Pointer, indexes, references, reservation, cancellationToken);
        if (!expected.AsSpan().SequenceEqual(allocation.PatchedDefinition))
        {
            throw Corrupt("The private logical definition differs from the independently reconstructed pointer-only patch.");
        }

        var io = new ReadAdmission(budget.MaxReadBytes);
        await using var data = new AdmittedStream(destination, io);
        await using var rows = new AdmittedStream(rowLedger, io);
        await using var values = new AdmittedStream(allocationLedger, io);
        CompactRawDataLongValueValidatorBudget raw = budget.RawValues;
        CompactRawTableAllocationValidatorResult rawResult = await CompactRawTableAllocationValidator.ValidateAsync(
            session,
            owner,
            definition,
            data,
            rows,
            values,
            allocation,
            new()
            {
                MaxDataRows = raw.MaxDataRows,
                MaxValues = raw.MaxValues,
                MaxLongValueRows = raw.MaxLongValueRows,
                MaxPages = raw.MaxPages,
                MaxRawBytes = raw.MaxRawBytes,
                MaxReadBytes = raw.MaxReadBytes,
                MaxWorkingBytes = Math.Min(raw.MaxWorkingBytes, budget.MaxWorkingBytes - reservation),
            },
            budget.MaxReadBytes,
            cancellationToken,
            hydratedColumns).ConfigureAwait(false);
        var validator = new Validator(format, data, values, allocation, budget.Maps, budget.MaxWorkingBytes - reservation);
        long mapStorage = (2L * format.PageSize) + 4096;
        using IDisposable mapAdmission = session.Accounting.ReserveWorkingBytes(mapStorage, owner);
        await validator.MapAsync(validator.DataRunsAsync(freeOnly: false, cancellationToken), allocation.OwnedPages, cancellationToken).ConfigureAwait(false);
        await validator.MapAsync(validator.DataRunsAsync(freeOnly: true, cancellationToken), allocation.FreePages, cancellationToken).ConfigureAwait(false);
        foreach (CompactRawTableLongValueMaps column in allocation.LongValueMaps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await validator.MapAsync(validator.ColumnRunsAsync(column.ColumnNumber, freeOnly: false, cancellationToken), column.OwnedPages, cancellationToken).ConfigureAwait(false);
            await validator.MapAsync(validator.ColumnRunsAsync(column.ColumnNumber, freeOnly: true, cancellationToken), column.FreePages, cancellationToken).ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (destination.Length != destinationLength || rowLedger.Length != allocation.Data.RelocationBytes
            || allocationLedger.Length != allocation.AllocationRecords * 12)
        {
            throw Corrupt("The reopened private table or its complete ledgers changed length during validation.");
        }

        return new(rawResult, next - allocation.Data.FirstPageNumber, 2L + (2L * originalMaps.Count), io.ReadBytes);
    }

    private static void Advance(long first, long count, ref long next)
    {
        if (first != next || first is < 3 or > 0xFFFFFF || count < 0 || count > 0x1000000L - first)
        {
            throw Corrupt("Private table allocations overlap, have gaps or exceed the native page range.");
        }

        next += count;
    }

    private static bool Aliases(Stream first, Stream second)
        => ReferenceEquals(first, second) || (first is FileStream firstFile && second is FileStream secondFile
            && string.Equals(firstFile.Name, secondFile.Name, StringComparison.OrdinalIgnoreCase));

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.MalformedValue, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private sealed class Validator(JetFormat format, Stream destination, Stream ledger, CompactRawTableBuilderResult allocation, CompactUsageMapValidatorBudget budget, long workingBytes)
    {
        private readonly byte[] page = new byte[format.PageSize];
        private readonly byte[] record = new byte[12];
        private long mapPages;
        private long listedPages;
        private long inputRuns;
        private long bitmapBytes;
        private long mapReadBytes;

        internal async ValueTask MapAsync(IAsyncEnumerable<CompactPageRun> runs, CompactUsageMapBuilderResult map, CancellationToken token)
        {
            CompactUsageMapValidatorResult result = await CompactUsageMapValidator.ValidateAsync(
                format,
                runs,
                destination,
                map,
                new()
                {
                    MaxPages = budget.MaxPages - this.mapPages,
                    MaxListedPages = budget.MaxListedPages - this.listedPages,
                    MaxRuns = budget.MaxRuns - this.inputRuns,
                    MaxBitmapBytes = budget.MaxBitmapBytes - this.bitmapBytes,
                    MaxReadBytes = budget.MaxReadBytes - this.mapReadBytes,
                    MaxWorkingBytes = Math.Min(budget.MaxWorkingBytes, workingBytes),
                },
                token).ConfigureAwait(false);
            this.mapPages += result.Pages;
            this.listedPages += result.ListedPages;
            this.inputRuns += result.InputRuns;
            this.bitmapBytes += result.BitmapBytes;
            this.mapReadBytes += result.ReadBytes;
        }

        internal async IAsyncEnumerable<CompactPageRun> DataRunsAsync(bool freeOnly, [EnumeratorCancellation] CancellationToken token)
        {
            if (!freeOnly)
            {
                if (allocation.Data.PageCount > 0)
                {
                    yield return new(allocation.Data.FirstPageNumber, allocation.Data.PageCount);
                }

                yield break;
            }

            for (long number = allocation.Data.FirstPageNumber; number < allocation.Data.FirstPageNumber + allocation.Data.PageCount; number++)
            {
                if (await this.HasSpaceAsync(number, token).ConfigureAwait(false))
                {
                    yield return new(number, 1);
                }
            }
        }

        internal async IAsyncEnumerable<CompactPageRun> ColumnRunsAsync(int column, bool freeOnly, [EnumeratorCancellation] CancellationToken token)
        {
            ledger.Position = 0;
            for (long index = 0; index < allocation.AllocationRecords; index++)
            {
                await ReadExactlyAsync(ledger, this.record, token).ConfigureAwait(false);
                if (BinaryPrimitives.ReadInt32LittleEndian(this.record) != column)
                {
                    continue;
                }

                int first = BinaryPrimitives.ReadInt32LittleEndian(this.record.AsSpan(4));
                int count = BinaryPrimitives.ReadInt32LittleEndian(this.record.AsSpan(8));
                if (!freeOnly)
                {
                    yield return new(first, count);
                    continue;
                }

                for (long number = first; number < (long)first + count; number++)
                {
                    if (await this.HasSpaceAsync(number, token).ConfigureAwait(false))
                    {
                        yield return new(number, 1);
                    }
                }
            }

            if (ledger.Length != allocation.AllocationRecords * this.record.Length)
            {
                throw Corrupt("The independently compared per-column allocation ledger changed length.");
            }
        }

        private static async ValueTask ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken token)
        {
            int offset = 0;
            while (offset < buffer.Length)
            {
                token.ThrowIfCancellationRequested();
                int read = await stream.ReadAsync(buffer.AsMemory(offset), token).ConfigureAwait(false);
                if (read <= 0 || read > buffer.Length - offset)
                {
                    throw Corrupt("A reopened private table or ledger is truncated.");
                }

                offset += read;
            }
        }

        private async ValueTask<bool> HasSpaceAsync(long number, CancellationToken token)
        {
            destination.Position = number * format.PageSize;
            await ReadExactlyAsync(destination, this.page, token).ConfigureAwait(false);
            return BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(2)) >= 3
                && BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.NumRows)) < Constants.DataPage.MaxRowsPerPage;
        }
    }

    private sealed class ReadAdmission(long maximum)
    {
        internal long ReadBytes { get; private set; }

        internal void Read(int count)
        {
            if (count > maximum - this.ReadBytes)
            {
                throw Limit("Complete raw table validation exceeds cumulative MaxReadBytes.");
            }

            this.ReadBytes += count;
        }
    }

    /// <summary>Non-owning read-only wrapper admitting all nested private read requests.</summary>
    /// <param name="inner">The borrowed immutable caller stream.</param>
    /// <param name="admission">The shared cumulative admission.</param>
    private sealed class AdmittedStream(Stream inner, ReadAdmission admission) : Stream
    {
        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => inner.Length;

        public override long Position { get => inner.Position; set => inner.Position = value; }

        public override void Flush() => throw new NotSupportedException();

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

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
