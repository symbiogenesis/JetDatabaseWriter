namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>Composes raw data packing, bookmark lookup and whole raw LVAL staging for one known table.</summary>
/// <remarks>
/// The caller exclusively owns the private decrypted destination and the complete immutable row ledger, distinct
/// from each other and the source, including through wrappers or backing files. Every original row must still exactly match its
/// mapped dense destination. The successful complete raw-data allocation and its exact matching ledger are caller
/// preconditions; this pass does not independently enumerate every allocation slot or prove absence of extra ledger records.
/// Only after comparing that complete row does this pass start changing it. Only defined external descriptor pointer words
/// are overwritten; all other row, field, envelope and trailing bytes are retained without decoding or recalculation.
/// The pass must precede any other row-reference rewrite. Definitions and calculated result types must be verified;
/// supplied projections may change only persisted ResultType metadata and remain immutable through completion,
/// including during awaited callbacks. Unassigned slots remain opaque.
/// Appended rows are private copies, with no global deduplication or ownership claim. Original LVAL storage, page
/// padding, unreachable content, catalog/TDEF/map references and unknown incoming references remain pinned.
/// This is neither a complete compact database nor authorization to reclaim, publish or replace source storage.
/// Failed reads, copies, patches or flushes leave a private partial destination and yield no completion result.
/// </remarks>
internal static class CompactRawDataLongValueRelocator
{
    /// <summary>Verifies packed rows, stages their assigned raw long values and patches their defined first pointers.</summary>
    /// <param name="session">The immutable source session, retaining cumulative source and simultaneous storage limits.</param>
    /// <param name="format">The source and destination format, matching the session.</param>
    /// <param name="sourceOwner">The original table-definition page.</param>
    /// <param name="destinationOwner">The private destination table-definition page.</param>
    /// <param name="definition">The verified original definition, retained unchanged throughout the pass.</param>
    /// <param name="data">The successful raw data builder's complete allocation evidence.</param>
    /// <param name="relocations">The validated complete immutable original-header bookmark ledger.</param>
    /// <param name="privateDestination">The separate readable writable seekable page-aligned private destination.</param>
    /// <param name="budget">The aggregate pass limits.</param>
    /// <param name="cancellationToken">Cancels source scanning, destination reads, copying, patches and flushing.</param>
    /// <param name="hydratedColumns">Optional verified persisted ResultType projections of the original descriptors.</param>
    /// <param name="allocationVisitor">Optional awaited allocation delivery with original column/row identity, for bounded ledger or future usage-map assembly; source field bytes remain borrowed.</param>
    /// <returns>Private per-table assembly evidence requiring independent complete database validation.</returns>
    /// <exception cref="ArgumentException">The supplied format, definition, streams or allocation are invalid.</exception>
    /// <exception cref="JetCorruptDataException">A packed row differs from its source or cannot safely receive a pointer.</exception>
    /// <exception cref="JetLimitationException">An aggregate limit is exhausted or assigned calculated storage is unverified.</exception>
    internal static async ValueTask<CompactRawDataLongValueRelocatorResult> RelocateAsync(
        CompactDiscoverySession session,
        JetFormat format,
        long sourceOwner,
        long destinationOwner,
        TDefImage definition,
        CompactRawDataBuilderResult data,
        CompactRowRelocationMap relocations,
        Stream privateDestination,
        CompactRawDataLongValueRelocatorBudget budget,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null,
        Func<CompactRawField, CompactRawLongValueBuilderResult, CancellationToken, ValueTask>? allocationVisitor = null)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(data, nameof(data));
        Guard.NotNull(relocations, nameof(relocations));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        ValidateBudget(budget);
        if (format != session.SourceFormat || sourceOwner <= 0 || destinationOwner <= 0
            || (definition.TDefPageNumber.HasValue && definition.TDefPageNumber != sourceOwner))
        {
            throw new ArgumentException("The verified source format and table identities must match the staged table.", nameof(definition));
        }

        if (!privateDestination.CanRead || !privateDestination.CanWrite || !privateDestination.CanSeek
            || privateDestination.Length % format.PageSize != 0)
        {
            throw new ArgumentException("The private destination must be readable, writable, seekable and page aligned.", nameof(privateDestination));
        }

        long firstPage = privateDestination.Length / format.PageSize;
        if (firstPage > 0xFFFFFF || data.FirstPageNumber < 3 || destinationOwner >= data.FirstPageNumber
            || data.FirstPageNumber > firstPage || data.PageCount < 0 || data.PageCount > firstPage - data.FirstPageNumber
            || data.RowCount < 0 || data.RowCount > long.MaxValue / 8 || data.RelocationBytes != data.RowCount * 8)
        {
            throw new ArgumentException("The complete raw-data allocation must precede the private LVAL allocation.", nameof(data));
        }

        ValidateProjections(definition, hydratedColumns, cancellationToken);
        long reservation = format.PageSize + 65_536L;
        if (reservation > budget.MaxWorkingBytes || data.RowCount > budget.MaxDataRows)
        {
            throw Limit("The raw data/LVAL relocation pass exceeds MaxWorkingBytes or MaxDataRows.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(reservation, sourceOwner);
        return await new Relocator(session, format, destinationOwner, definition, data, relocations, privateDestination, budget, hydratedColumns, allocationVisitor, firstPage)
            .RunAsync(sourceOwner, cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateBudget(CompactRawDataLongValueRelocatorBudget budget)
    {
        Guard.InRange(budget.MaxDataRows, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxValues, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxLongValueRows, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRawBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxDestinationReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPointerBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxValueWorkingBytes, 1L, long.MaxValue, nameof(budget));
    }

    private static void ValidateProjections(TDefImage definition, IReadOnlyList<ColumnInfo>? columns, CancellationToken token)
    {
        if (columns is null)
        {
            return;
        }

        if (columns.Count != definition.Columns.Count || columns.Count > Constants.TableDefinition.MaxColumns)
        {
            throw new ArgumentException("Hydrated columns must retain every original descriptor.", nameof(columns));
        }

        for (int i = 0; i < columns.Count; i++)
        {
            token.ThrowIfCancellationRequested();
            if (columns[i] != definition.Columns[i] with { CalculatedResultType = columns[i].CalculatedResultType })
            {
                throw new ArgumentException("Hydrated columns may change only persisted ResultType metadata.", nameof(columns));
            }
        }
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private sealed class Relocator(
        CompactDiscoverySession session,
        JetFormat format,
        long destinationOwner,
        TDefImage definition,
        CompactRawDataBuilderResult data,
        CompactRowRelocationMap relocations,
        Stream destination,
        CompactRawDataLongValueRelocatorBudget budget,
        IReadOnlyList<ColumnInfo>? columns,
        Func<CompactRawField, CompactRawLongValueBuilderResult, CancellationToken, ValueTask>? allocationVisitor,
        long firstPage)
    {
        private readonly byte[] page = new byte[format.PageSize];
        private long rows;
        private long values;
        private long lvalRows;
        private long pages;
        private long rawBytes;
        private long readBytes;
        private long pointerBytes;
        private long expectedLength = firstPage * format.PageSize;

        internal async ValueTask<CompactRawDataLongValueRelocatorResult> RunAsync(long sourceOwner, CancellationToken token)
        {
            await session.ScanRowsAsync(sourceOwner, this.RelocateRowAsync, token).ConfigureAwait(false);
            if (this.rows != data.RowCount)
            {
                throw Corrupt(sourceOwner, "The source row count disagrees with the complete packed allocation.");
            }

            this.CheckLength();
            destination.Position = destination.Length;
            await destination.FlushAsync(token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            this.CheckLength();
            return new(firstPage, this.pages, this.rows, this.values, this.lvalRows, this.rawBytes, this.readBytes, this.pointerBytes);
        }

        private async ValueTask RelocateRowAsync(RowLocation location, ReadOnlyMemory<byte> original, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            this.CheckLength();
            if (this.rows >= budget.MaxDataRows || this.rows >= data.RowCount)
            {
                throw Limit("The source row scan exceeds MaxDataRows or the complete packed row count.");
            }

            if (location.RowIndex is < 0 or > byte.MaxValue)
            {
                throw Corrupt(location.PageNumber, "The original header has no representable native row bookmark.");
            }

            IndexEntry mapped = await relocations.RelocateAsync(new([], location.PageNumber, checked((byte)location.RowIndex)), token).ConfigureAwait(false);
            if (mapped.DataPage < data.FirstPageNumber || mapped.DataPage - data.FirstPageNumber >= data.PageCount)
            {
                throw Corrupt(mapped.DataPage, "A mapped destination row is outside the complete packed data allocation.");
            }

            await this.ReadPageAsync(mapped.DataPage, token).ConfigureAwait(false);
            int rowStart = this.LocateRow(mapped, original.Length, token);
            if (!original.Span.SequenceEqual(this.page.AsSpan(rowStart, original.Length)))
            {
                throw Corrupt(mapped.DataPage, "The mapped packed row differs from its complete original raw bytes.");
            }

            this.rows++;
            await CompactRawFieldReader.ScanAsync(
                format,
                definition,
                location,
                original,
                (field, fieldToken) => this.RelocateFieldAsync(field, (mapped.DataPage * format.PageSize) + rowStart, fieldToken),
                token,
                columns).ConfigureAwait(false);
        }

        private async ValueTask RelocateFieldAsync(CompactRawField field, long destinationRowOffset, CancellationToken token)
        {
            session.Accounting.Charge(CompactDiscoveryCharge.Fields, 1, field.Location.PageNumber);
            session.Accounting.Charge(CompactDiscoveryCharge.Records, 1, field.Location.PageNumber);
            if (field.Kind != CompactRawFieldKind.Stored)
            {
                return;
            }

            if (!field.IsStorageTypeKnown)
            {
                const string reason = "Assigned calculated storage requires a verified persisted ResultType before raw LVAL relocation.";
                throw new JetLimitationException(
                    JetErrorCode.FeatureNotSupported,
                    reason,
                    new JetErrorInfo { ColumnName = field.Column.Name, PageNumber = field.Location.DataPageNumber, Reason = reason });
            }

            if (!field.IsLongValue)
            {
                return;
            }

            bool patch = LongValueDescriptor.TryRead(field.RawBytes.Span, out LongValueDescriptor descriptor) && descriptor.IsExternal && descriptor.FirstDp != 0;
            if (this.values >= budget.MaxValues || (patch && budget.MaxPointerBytes - this.pointerBytes < sizeof(uint)))
            {
                throw Limit("Raw data/LVAL relocation exceeds MaxValues or MaxPointerBytes.");
            }

            this.values++;
            this.CheckLength();
            CompactRawLongValueBuilderResult value = await CompactRawLongValueBuilder.BuildAsync(
                session,
                format,
                field.RawBytes,
                destination,
                new()
                {
                    MaxRows = budget.MaxLongValueRows - this.lvalRows,
                    MaxPages = budget.MaxPages - this.pages,
                    MaxOutputBytes = budget.MaxOutputBytes - (this.pages * format.PageSize),
                    MaxRawBytes = budget.MaxRawBytes - this.rawBytes,
                    MaxWorkingBytes = budget.MaxValueWorkingBytes,
                },
                token).ConfigureAwait(false);
            this.pages += value.PageCount;
            this.lvalRows += value.RowCount;
            this.rawBytes += value.RawBytes;
            this.expectedLength += value.PageCount * format.PageSize;
            this.CheckLength();
            if (patch)
            {
                token.ThrowIfCancellationRequested();
                this.pointerBytes += sizeof(uint);
                destination.Position = destinationRowOffset + field.RowOffset + sizeof(uint);
                await destination.WriteAsync(value.Column.Slice(sizeof(uint), sizeof(uint)), token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                this.CheckLength();
            }

            if (allocationVisitor is not null)
            {
                session.Accounting.Charge(CompactDiscoveryCharge.Records, 1, field.Location.PageNumber);
                await allocationVisitor(field, value, token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                this.CheckLength();
            }
        }

        private async ValueTask ReadPageAsync(long number, CancellationToken token)
        {
            destination.Position = number * format.PageSize;
            int offset = 0;
            while (offset < this.page.Length)
            {
                token.ThrowIfCancellationRequested();
                int requested = this.page.Length - offset;
                if (requested > budget.MaxDestinationReadBytes - this.readBytes)
                {
                    throw Limit("Raw data/LVAL relocation exceeds MaxDestinationReadBytes.");
                }

                this.readBytes += requested;
                int read = await destination.ReadAsync(this.page.AsMemory(offset, requested), token).ConfigureAwait(false);
                if (read <= 0 || read > requested)
                {
                    throw Corrupt(number, "The private destination returned a truncated packed data page.");
                }

                offset = StreamRead.AdvanceRead(this.page.Length, offset, read);
            }

            this.CheckLength();
        }

        private int LocateRow(IndexEntry mapped, int length, CancellationToken token)
        {
            int count = BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.NumRows));
            if (this.page[0] != Constants.PageTypes.Data || BinaryPrimitives.ReadUInt32LittleEndian(this.page.AsSpan(format.DataPage.TDefOff)) != destinationOwner
                || count is 0 or > Constants.DataPage.MaxRowsPerPage || mapped.DataRow >= count)
            {
                throw Corrupt(mapped.DataPage, "The mapped destination is not a direct packed row of its expected table.");
            }

            int directoryEnd = format.DataPage.RowsStart + (count * sizeof(ushort));
            int start = BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.RowsStart + (mapped.DataRow * sizeof(ushort))));
            int end = format.PageSize;
            for (int i = 0; i < count; i++)
            {
                token.ThrowIfCancellationRequested();
                int other = BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.RowsStart + (i * sizeof(ushort))));
                if (other < directoryEnd || other >= format.PageSize || (i != mapped.DataRow && other == start))
                {
                    throw Corrupt(mapped.DataPage, "The packed destination row directory has invalid bounds, flags or aliases.");
                }

                if (other > start && other < end)
                {
                    end = other;
                }
            }

            if (end - start != length)
            {
                throw Corrupt(mapped.DataPage, "The mapped packed row length differs from its original raw row.");
            }

            return start;
        }

        private void CheckLength()
        {
            if (destination.Length != this.expectedLength)
            {
                throw Corrupt(0, "The private destination geometry changed during raw data/LVAL relocation.");
            }
        }
    }
}
