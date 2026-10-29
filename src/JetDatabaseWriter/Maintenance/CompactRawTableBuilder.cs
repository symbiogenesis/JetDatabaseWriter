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

/// <summary>Composes bounded raw data/LVAL packing, native per-owner maps and a pointer-only pinned TDEF image.</summary>
/// <remarks>
/// The immutable source, exclusive private decrypted destination and two empty disk-capable ledgers must have distinct
/// backing storage, including through wrappers. Definitions, projections and index replacements remain immutable.
/// The table and its TDEF continuation identities remain pinned; returned bytes require installation on that original
/// chain while preserving continuation headers. Original storage is retained. Index replacements are caller-verified
/// inputs; index rebuilding, global maps, opaque incoming ownership and complete reopened validation remain separate.
/// This does not produce a complete compact database or authorize reclamation, publication or replacement.
/// Failures leave private partial bytes and return no completion evidence.
/// </remarks>
internal static class CompactRawTableBuilder
{
    /// <summary>Stages a known table and returns complete private allocations plus its raw patched definition.</summary>
    /// <param name="session">The immutable source and cumulative traversal/storage accounting.</param>
    /// <param name="format">The matching preserved native format.</param>
    /// <param name="owner">The original pinned table-definition identity.</param>
    /// <param name="definition">The verified original logical definition, including padding.</param>
    /// <param name="privateDestination">The exclusive readable writable seekable page-aligned private destination.</param>
    /// <param name="rowLedger">The distinct empty readable writable seekable row relocation ledger.</param>
    /// <param name="allocationLedger">The distinct empty readable writable seekable LVAL allocation ledger.</param>
    /// <param name="indexes">Verified replacements in complete original physical index slot order.</param>
    /// <param name="budget">Aggregate output, ledger, private I/O and assembly storage limits.</param>
    /// <param name="cancellationToken">Cancels every scan, copy, patch, read and flush.</param>
    /// <param name="hydratedColumns">Optional verified persisted ResultType projections.</param>
    /// <returns>Unvalidated private table staging evidence; source and global allocation remain pinned.</returns>
    /// <exception cref="ArgumentException">The source, streams or complete reference lists are invalid.</exception>
    /// <exception cref="JetCorruptDataException">Raw bytes, allocation geometry or known metadata disagree.</exception>
    /// <exception cref="JetLimitationException">An allowance is exhausted or an external column has no known map record.</exception>
    internal static async ValueTask<CompactRawTableBuilderResult> BuildAsync(
        CompactDiscoverySession session,
        JetFormat format,
        long owner,
        TDefImage definition,
        Stream privateDestination,
        Stream rowLedger,
        Stream allocationLedger,
        IReadOnlyList<CompactTDefIndexReferences> indexes,
        CompactRawTableBuilderBudget budget,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(rowLedger, nameof(rowLedger));
        Guard.NotNull(allocationLedger, nameof(allocationLedger));
        Guard.NotNull(indexes, nameof(indexes));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        ValidateBudget(budget);
        if (format != session.SourceFormat || owner <= 0 || (definition.TDefPageNumber.HasValue && definition.TDefPageNumber != owner)
            || definition.Issues != TDefParseIssues.None || indexes.Count != definition.RealIndexes.Count)
        {
            throw new ArgumentException("The verified original table, format and complete physical index references must agree.", nameof(definition));
        }

        if (!privateDestination.CanRead || !privateDestination.CanWrite || !privateDestination.CanSeek
            || privateDestination.Length % format.PageSize != 0 || owner >= privateDestination.Length / format.PageSize)
        {
            throw new ArgumentException("The pinned owner must precede a readable writable seekable page-aligned private append point.", nameof(privateDestination));
        }

        ValidateLedger(rowLedger);
        ValidateLedger(allocationLedger);
        if (Aliases(privateDestination, rowLedger) || Aliases(privateDestination, allocationLedger) || Aliases(rowLedger, allocationLedger))
        {
            throw new ArgumentException("The private destination and both ledgers must have distinct backing storage.", nameof(allocationLedger));
        }

        long possibleMaps = Math.Min(Constants.TableDefinition.MaxColumns, definition.LogicalByteLength / 10);
        long reservation = (2L * definition.LogicalByteLength) + format.PageSize + 131_072 + (256 * possibleMaps);
        if (reservation > budget.MaxWorkingBytes)
        {
            throw Limit("Raw table assembly exceeds MaxWorkingBytes.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(reservation, owner);
        if (!LongValueUsageMaps.TryRead(format, definition.CopyBytes(), out List<LongValueUsageMap> maps, out _))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptTableDefinition, "Raw table assembly requires complete known LVAL trailer boundaries.");
        }

        long resultReservation = 256L * maps.Count;
        if (resultReservation > budget.MaxWorkingBytes - reservation)
        {
            throw Limit("Raw table assembly's retained per-column map results exceed MaxWorkingBytes.");
        }

        using IDisposable resultAdmission = session.Accounting.ReserveWorkingBytes(resultReservation, owner);

        var io = new IoAdmission(budget, privateDestination.Length, format.PageSize);
        await using var destination = new AdmittedStream(privateDestination, io, boundsOutput: true);
        await using var rows = new AdmittedStream(rowLedger, io, boundsOutput: false);
        await using var allocations = new AdmittedStream(allocationLedger, io, boundsOutput: false);
        var builder = new Builder(format, owner, destination, allocations, maps, budget);
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(
            session,
            format,
            owner,
            owner,
            destination,
            rows,
            new()
            {
                MaxRows = budget.MaxDataRows,
                MaxPages = Math.Max(1, budget.MaxPages),
                MaxOutputBytes = Math.Max(1, budget.MaxOutputBytes),
                MaxRelocationBytes = Math.Max(1, budget.MaxRowLedgerBytes),
            },
            cancellationToken).ConfigureAwait(false);
        if (data.RowCount > budget.MaxDataRows || data.RelocationBytes > budget.MaxRowLedgerBytes)
        {
            throw Limit("Raw table assembly exceeds MaxDataRows or MaxRowLedgerBytes.");
        }

        CompactRowRelocationMap relocation = await CompactRowRelocationMap.OpenAsync(rows, data.RowCount, Math.Max(1, budget.MaxReadBytes), cancellationToken).ConfigureAwait(false);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(
            session,
            format,
            owner,
            owner,
            definition,
            data,
            relocation,
            destination,
            new()
            {
                MaxDataRows = budget.MaxDataRows,
                MaxValues = budget.MaxValues,
                MaxLongValueRows = budget.MaxLongValueRows,
                MaxPages = budget.MaxPages,
                MaxOutputBytes = budget.MaxOutputBytes,
                MaxRawBytes = budget.MaxRawBytes,
                MaxDestinationReadBytes = budget.MaxReadBytes,
            },
            cancellationToken,
            hydratedColumns,
            builder.RecordAsync).ConfigureAwait(false);
        await allocations.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        CompactUsageMapBuilderResult owned = await builder.MapAsync(builder.DataRunsAsync(data, freeOnly: false, cancellationToken), cancellationToken).ConfigureAwait(false);
        CompactUsageMapBuilderResult free = await builder.MapAsync(builder.DataRunsAsync(data, freeOnly: true, cancellationToken), cancellationToken).ConfigureAwait(false);
        var longValueMaps = new CompactRawTableLongValueMaps[maps.Count];
        var references = new CompactTDefLongValueReferences[maps.Count];
        for (int i = 0; i < maps.Count; i++)
        {
            int column = maps[i].ColumnNumber;
            CompactUsageMapBuilderResult columnOwned = await builder.MapAsync(builder.ColumnRunsAsync(column, freeOnly: false, cancellationToken), cancellationToken).ConfigureAwait(false);
            CompactUsageMapBuilderResult columnFree = await builder.MapAsync(builder.ColumnRunsAsync(column, freeOnly: true, cancellationToken), cancellationToken).ConfigureAwait(false);
            longValueMaps[i] = new(column, columnOwned, columnFree);
            references[i] = new(column, columnOwned.Pointer, columnFree.Pointer);
        }

        byte[] patched = CompactTDefReferencePatcher.Patch(format, definition, owned.Pointer, free.Pointer, indexes, references, budget.MaxWorkingBytes, cancellationToken);
        await rows.FlushAsync(cancellationToken).ConfigureAwait(false);
        await allocations.FlushAsync(cancellationToken).ConfigureAwait(false);
        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        destination.Position = destination.Length;
        return new(data, values, owned, free, longValueMaps, patched, builder.AllocationRecords, io.ReadBytes);
    }

    private static void ValidateBudget(CompactRawTableBuilderBudget budget)
    {
        Guard.InRange(budget.MaxDataRows, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxValues, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxLongValueRows, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRowLedgerBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxAllocationLedgerBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRawBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
    }

    private static void ValidateLedger(Stream stream)
    {
        if (!stream.CanRead || !stream.CanWrite || !stream.CanSeek || stream.Length != 0)
        {
            throw new ArgumentException("Each private ledger must be empty, readable, writable and seekable.", nameof(stream));
        }
    }

    private static bool Aliases(Stream first, Stream second)
        => ReferenceEquals(first, second) || (first is FileStream firstFile && second is FileStream secondFile
            && string.Equals(firstFile.Name, secondFile.Name, StringComparison.OrdinalIgnoreCase));

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private sealed class Builder(JetFormat format, long owner, Stream destination, Stream ledger, List<LongValueUsageMap> maps, CompactRawTableBuilderBudget budget)
    {
        private readonly byte[] record = new byte[12];
        private readonly byte[] page = new byte[format.PageSize];

        internal long AllocationRecords { get; private set; }

        internal async ValueTask RecordAsync(CompactRawField field, CompactRawLongValueBuilderResult allocation, CancellationToken token)
        {
            if (allocation.PageCount == 0)
            {
                return;
            }

            if (!maps.Exists(map => map.ColumnNumber == field.Column.ColNum))
            {
                throw new JetLimitationException(JetErrorCode.FeatureNotSupported, "An external long-value column has no verified original native map record.");
            }

            if (this.AllocationRecords >= budget.MaxAllocationLedgerBytes / this.record.Length)
            {
                throw Limit("Raw table assembly exceeds MaxAllocationLedgerBytes.");
            }

            BinaryPrimitives.WriteInt32LittleEndian(this.record, field.Column.ColNum);
            BinaryPrimitives.WriteInt32LittleEndian(this.record.AsSpan(4), checked((int)allocation.FirstPageNumber));
            BinaryPrimitives.WriteInt32LittleEndian(this.record.AsSpan(8), checked((int)allocation.PageCount));
            ledger.Position = ledger.Length;
            await ledger.WriteAsync(this.record.AsMemory(), token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            this.AllocationRecords++;
        }

        internal ValueTask<CompactUsageMapBuilderResult> MapAsync(IAsyncEnumerable<CompactPageRun> runs, CancellationToken token)
            => CompactUsageMapBuilder.BuildAsync(
                format,
                runs,
                destination,
                new()
                {
                    MaxRuns = budget.MaxPages,
                    MaxListedPages = budget.MaxPages,
                    MaxPages = Math.Max(1, budget.MaxPages),
                    MaxOutputBytes = Math.Max(1, budget.MaxOutputBytes),
                },
                token);

        internal async IAsyncEnumerable<CompactPageRun> DataRunsAsync(CompactRawDataBuilderResult data, bool freeOnly, [EnumeratorCancellation] CancellationToken token)
        {
            if (!freeOnly)
            {
                if (data.PageCount > 0)
                {
                    yield return new(data.FirstPageNumber, data.PageCount);
                }

                yield break;
            }

            for (long pageNumber = data.FirstPageNumber; pageNumber < data.FirstPageNumber + data.PageCount; pageNumber++)
            {
                if (await this.HasSpaceAsync(pageNumber, longValue: false, token).ConfigureAwait(false))
                {
                    yield return new(pageNumber, 1);
                }
            }
        }

        internal async IAsyncEnumerable<CompactPageRun> ColumnRunsAsync(int column, bool freeOnly, [EnumeratorCancellation] CancellationToken token)
        {
            ledger.Position = 0;
            for (long index = 0; index < this.AllocationRecords; index++)
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

                for (long pageNumber = first; pageNumber < (long)first + count; pageNumber++)
                {
                    if (await this.HasSpaceAsync(pageNumber, longValue: true, token).ConfigureAwait(false))
                    {
                        yield return new(pageNumber, 1);
                    }
                }
            }

            if (ledger.Length != this.AllocationRecords * this.record.Length)
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "The complete private LVAL allocation ledger changed length.");
            }
        }

        private static async ValueTask ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken token)
        {
            int read = await StreamRead.ReadAtLeastAsync(stream, buffer, buffer.Length, throwOnEndOfStream: false, token).ConfigureAwait(false);
            if (read != buffer.Length)
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "A private table map input was truncated.");
            }
        }

        private async ValueTask<bool> HasSpaceAsync(long number, bool longValue, CancellationToken token)
        {
            destination.Position = number * format.PageSize;
            await ReadExactlyAsync(destination, this.page, token).ConfigureAwait(false);
            uint actualOwner = BinaryPrimitives.ReadUInt32LittleEndian(this.page.AsSpan(format.DataPage.TDefOff));
            if (this.page[0] != Constants.PageTypes.Data || (longValue ? !this.page.AsSpan(4, 4).SequenceEqual("LVAL"u8) : actualOwner != owner))
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "A private page does not retain its expected raw table or LVAL owner.");
            }

            return BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(2)) >= 3
                && BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.NumRows)) < Constants.DataPage.MaxRowsPerPage;
        }
    }

    private sealed class IoAdmission(CompactRawTableBuilderBudget budget, long initialLength, int pageSize)
    {
        internal long ReadBytes { get; private set; }

        internal void Read(int count)
        {
            if (count > budget.MaxReadBytes - this.ReadBytes)
            {
                throw Limit("Raw table assembly exceeds cumulative MaxReadBytes.");
            }

            this.ReadBytes += count;
        }

        internal void Write(long position, int count)
        {
            long limit = Math.Min(budget.MaxOutputBytes, Math.Min(budget.MaxPages, 0x1000000L) * pageSize);
            if (position < 0 || position > long.MaxValue - count || position + count > 0x1000000L * pageSize
                || (position + count > initialLength && position + count - initialLength > limit))
            {
                throw Limit("Raw table assembly exceeds aggregate MaxPages, MaxOutputBytes or the native page range.");
            }
        }
    }

    /// <summary>Non-owning wrappers share admission across all phases, including helpers' partial-read retries.</summary>
    /// <param name="inner">The borrowed caller-owned stream.</param>
    /// <param name="admission">The shared cumulative private I/O admission.</param>
    /// <param name="boundsOutput">Whether writes also enforce the destination's aggregate append limit.</param>
    private sealed class AdmittedStream(Stream inner, IoAdmission admission, bool boundsOutput) : Stream
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

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (boundsOutput)
            {
                admission.Write(inner.Position, count);
            }

            inner.Write(buffer, offset, count);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (boundsOutput)
            {
                admission.Write(inner.Position, buffer.Length);
            }

            return inner.WriteAsync(buffer, cancellationToken);
        }
    }
}
