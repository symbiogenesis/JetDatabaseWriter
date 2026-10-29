namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>Independently compares reopened packed rows, their complete ledger and all copied raw LVAL storage.</summary>
/// <remarks>
/// The immutable source, reopened destination and ledger must have distinct backing storage, including through wrappers.
/// Verified definitions and optional persisted ResultType projections remain immutable until completion. Every data and
/// LVAL slot in the declared disjoint allocations is visited once in dense order. Only descriptor first pointers and defined
/// chain pointers may differ; every such pointer must name the next exclusively covered destination slot, or terminal zero.
/// Complete opaque row, field, envelope, payload and trailing bytes are compared without decoding or recalculation.
/// This proves private allocation coverage, not complete database validity, global incoming ownership or safe source reclamation.
/// Catalog, TDEF, maps, indexes, unreachable storage and unknown incoming references require separate validation.
/// </remarks>
internal static class CompactRawDataLongValueValidator
{
    /// <summary>Validates the complete reopened private data/LVAL allocation against the immutable source.</summary>
    /// <param name="session">The source session and its cumulative read, work and simultaneous storage limits.</param>
    /// <param name="sourceOwner">The original table-definition identity.</param>
    /// <param name="destinationOwner">The expected private table-definition identity.</param>
    /// <param name="definition">The verified original definition, with no storage-coordinate changes.</param>
    /// <param name="destination">The reopened readable seekable decrypted destination.</param>
    /// <param name="ledger">The reopened readable seekable complete original-header row relocation ledger.</param>
    /// <param name="data">The complete packed data allocation.</param>
    /// <param name="longValues">The aggregate raw LVAL relocation evidence to check independently.</param>
    /// <param name="budget">The aggregate private validation limits.</param>
    /// <param name="cancellationToken">Cancels source auditing, directory checks and every private read.</param>
    /// <param name="hydratedColumns">Optional verified projections changing only persisted ResultType metadata.</param>
    /// <param name="allocationVisitor">Optional awaited delivery of independently compared nonempty external allocations with their original field identities; later failure still refuses completion.</param>
    /// <returns>Independent comparison evidence, without authorizing publication or source reclamation.</returns>
    /// <exception cref="ArgumentException">Streams, definitions or projections cannot be validated independently.</exception>
    /// <exception cref="JetCorruptDataException">Stored bytes, pointers, ownership, coverage or evidence disagree.</exception>
    /// <exception cref="JetLimitationException">A source or private validation allowance is exhausted.</exception>
    internal static async ValueTask<CompactRawDataLongValueValidatorResult> ValidateAsync(
        CompactDiscoverySession session,
        long sourceOwner,
        long destinationOwner,
        TDefImage definition,
        Stream destination,
        Stream ledger,
        CompactRawDataBuilderResult data,
        CompactRawDataLongValueRelocatorResult longValues,
        CompactRawDataLongValueValidatorBudget budget,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null,
        Func<CompactRawField, CompactPageRun, CancellationToken, ValueTask>? allocationVisitor = null)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(ledger, nameof(ledger));
        Guard.NotNull(data, nameof(data));
        Guard.NotNull(longValues, nameof(longValues));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        ValidateBudget(budget);
        if (!destination.CanRead || !destination.CanSeek || !ledger.CanRead || !ledger.CanSeek
            || ReferenceEquals(destination, ledger)
            || (destination is FileStream destinationFile && ledger is FileStream ledgerFile
                && string.Equals(destinationFile.Name, ledgerFile.Name, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException("The reopened destination and complete ledger must be distinct readable seekable streams.", nameof(destination));
        }

        if (sourceOwner <= 0 || (definition.TDefPageNumber.HasValue && definition.TDefPageNumber != sourceOwner))
        {
            throw new ArgumentException("The verified definition must retain its original table identity.", nameof(definition));
        }

        ValidateProjections(definition, hydratedColumns, cancellationToken);
        JetFormat format = session.SourceFormat;
        long length = destination.Length;
        long destinationPages = length / format.PageSize;
        if (length % format.PageSize != 0 || data.FirstPageNumber is < 3 or > 0xFFFFFF
            || destinationOwner <= 0 || destinationOwner >= data.FirstPageNumber
            || data.PageCount < 0 || data.PageCount > 0x1000000L - data.FirstPageNumber
            || data.FirstPageNumber + data.PageCount > destinationPages
            || data.RowCount < data.PageCount || data.RowCount > data.PageCount * Constants.DataPage.MaxRowsPerPage
            || data.RelocationBytes != data.RowCount * 8 || ledger.Length != data.RelocationBytes
            || longValues.FirstPageNumber < data.FirstPageNumber + data.PageCount || longValues.FirstPageNumber > 0xFFFFFF
            || longValues.PageCount < 0 || longValues.PageCount > 0x1000000L - longValues.FirstPageNumber
            || longValues.FirstPageNumber + longValues.PageCount > destinationPages
            || longValues.DataRows != data.RowCount || longValues.Values < 0
            || longValues.LongValueRows < longValues.PageCount || longValues.LongValueRows > longValues.PageCount * Constants.DataPage.MaxRowsPerPage
            || longValues.RawBytes < 0 || longValues.PointerBytes < 0 || longValues.PointerBytes % sizeof(uint) != 0
            || longValues.PointerBytes / sizeof(uint) > longValues.Values)
        {
            throw Corrupt("The complete private data/LVAL allocations or row ledger have invalid geometry.");
        }

        long reservation = (3L * format.PageSize) + 65_536;
        if (reservation > budget.MaxWorkingBytes || data.RowCount > budget.MaxDataRows || longValues.Values > budget.MaxValues
            || longValues.LongValueRows > budget.MaxLongValueRows || longValues.RawBytes > budget.MaxRawBytes
            || data.PageCount + longValues.PageCount > budget.MaxPages)
        {
            throw Limit("The declared private data/LVAL validation exceeds its aggregate limits.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(reservation, sourceOwner);
        return await new Validator(session, format, destinationOwner, definition, destination, ledger, data, longValues, budget, hydratedColumns, allocationVisitor, length)
            .RunAsync(sourceOwner, cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateBudget(CompactRawDataLongValueValidatorBudget budget)
    {
        Guard.InRange(budget.MaxDataRows, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxValues, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxLongValueRows, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRawBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
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

        for (int index = 0; index < columns.Count; index++)
        {
            token.ThrowIfCancellationRequested();
            if (columns[index] != definition.Columns[index] with { CalculatedResultType = columns[index].CalculatedResultType })
            {
                throw new ArgumentException("Hydrated columns may change only persisted ResultType metadata.", nameof(columns));
            }
        }
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { Reason = reason });

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private sealed class Validator(
        CompactDiscoverySession session,
        JetFormat format,
        long owner,
        TDefImage definition,
        Stream destination,
        Stream ledger,
        CompactRawDataBuilderResult data,
        CompactRawDataLongValueRelocatorResult longValues,
        CompactRawDataLongValueValidatorBudget budget,
        IReadOnlyList<ColumnInfo>? columns,
        Func<CompactRawField, CompactPageRun, CancellationToken, ValueTask>? allocationVisitor,
        long destinationLength)
    {
        private readonly PageCursor dataPages = new(format, data.FirstPageNumber, data.PageCount);
        private readonly PageCursor lvalPages = new(format, longValues.FirstPageNumber, longValues.PageCount);
        private readonly byte[] expectedRow = new byte[format.PageSize];
        private readonly byte[] record = new byte[8];
        private long rows;
        private long values;
        private long lvalRows;
        private long rawBytes;
        private long pointerBytes;
        private long readBytes;

        internal async ValueTask<CompactRawDataLongValueValidatorResult> RunAsync(long sourceOwner, CancellationToken token)
        {
            ledger.Position = 0;
            await session.ScanRowsAsync(sourceOwner, this.CompareRowAsync, token).ConfigureAwait(false);
            if (this.rows != data.RowCount || this.values != longValues.Values || this.lvalRows != longValues.LongValueRows
                || this.rawBytes != longValues.RawBytes || this.pointerBytes != longValues.PointerBytes)
            {
                throw Corrupt("Independent private data/LVAL observations disagree with the declared complete evidence.");
            }

            this.dataPages.RequireComplete();
            this.lvalPages.RequireComplete();
            this.CheckLengths();
            token.ThrowIfCancellationRequested();
            return new(this.rows, this.values, this.lvalRows, this.lvalPages.Pages, this.rawBytes, this.readBytes);
        }

        private async ValueTask CompareRowAsync(RowLocation location, ReadOnlyMemory<byte> original, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            this.CheckLengths();
            if (this.rows >= data.RowCount || location.PageNumber is <= 0 or > 0xFFFFFF || location.RowIndex is < 0 or > byte.MaxValue)
            {
                throw Corrupt("A source row has no representable entry in the complete private allocation.");
            }

            ReadOnlyMemory<byte> stored = await this.ReadRowAsync(this.dataPages, false, token).ConfigureAwait(false);
            await this.ReadAsync(ledger, this.record, token).ConfigureAwait(false);
            uint source = checked(((uint)location.PageNumber << 8) | (uint)location.RowIndex);
            if (BinaryPrimitives.ReadUInt32LittleEndian(this.record) != source
                || BinaryPrimitives.ReadUInt32LittleEndian(this.record.AsSpan(4)) != this.dataPages.CurrentBookmark
                || stored.Length != original.Length)
            {
                throw Corrupt("The complete row ledger or dense private row length disagrees with its original identity.");
            }

            original.Span.CopyTo(this.expectedRow);
            await CompactRawFieldReader.ScanAsync(format, definition, location, original, this.CompareFieldAsync, token, columns).ConfigureAwait(false);
            if (!stored.Span.SequenceEqual(this.expectedRow.AsSpan(0, original.Length)))
            {
                throw Corrupt("A private raw row differs outside its independently verified descriptor pointer words.");
            }

            this.rows++;
        }

        private async ValueTask CompareFieldAsync(CompactRawField field, CancellationToken token)
        {
            session.Accounting.Charge(CompactDiscoveryCharge.Fields, 1, field.Location.PageNumber);
            session.Accounting.Charge(CompactDiscoveryCharge.Records, 1, field.Location.PageNumber);
            if (field.Kind != CompactRawFieldKind.Stored)
            {
                return;
            }

            if (!field.IsStorageTypeKnown)
            {
                const string reason = "Assigned calculated storage requires a verified persisted ResultType before independent raw LVAL validation.";
                throw new JetLimitationException(JetErrorCode.FeatureNotSupported, reason, new JetErrorInfo { ColumnName = field.Column.Name, PageNumber = field.Location.DataPageNumber, Reason = reason });
            }

            if (!field.IsLongValue)
            {
                return;
            }

            if (this.values >= budget.MaxValues)
            {
                throw Limit("Independent raw LVAL validation exceeds MaxValues.");
            }

            this.values++;
            this.AddRawBytes(field.RawBytes.Length);
            if (LongValueDescriptor.TryRead(field.RawBytes.Span, out LongValueDescriptor descriptor) && descriptor.IsExternal && descriptor.FirstDp != 0)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(this.expectedRow.AsSpan(field.RowOffset + sizeof(uint)), this.lvalPages.NextValueBookmark);
                this.pointerBytes += sizeof(uint);
            }

            long firstPage = longValues.FirstPageNumber + this.lvalPages.Pages;
            await session.AuditLongValueAsync(field.RawBytes, this.CompareLongValueRecordAsync, token).ConfigureAwait(false);
            this.lvalPages.RequireValueComplete();
            long pageCount = longValues.FirstPageNumber + this.lvalPages.Pages - firstPage;
            if (pageCount > 0 && allocationVisitor is not null)
            {
                session.Accounting.Charge(CompactDiscoveryCharge.Records, 1, field.Location.PageNumber);
                await allocationVisitor(field, new(firstPage, pageCount), token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                this.CheckLengths();
            }
        }

        private async ValueTask CompareLongValueRecordAsync(CompactLongValueRecord record, CancellationToken token)
        {
            if (record.Kind != "lval-row")
            {
                return;
            }

            if (this.lvalRows >= budget.MaxLongValueRows)
            {
                throw Limit("Independent raw LVAL validation exceeds MaxLongValueRows.");
            }

            this.AddRawBytes(record.RawBytes.Length);
            ReadOnlyMemory<byte> stored = await this.ReadRowAsync(this.lvalPages, true, token).ConfigureAwait(false);
            uint? pageToken = format.IsJet3 ? null : BinaryPrimitives.ReadUInt32LittleEndian(this.lvalPages.Buffer.AsSpan(8));
            if (pageToken != record.PageToken)
            {
                throw Corrupt("A private LVAL page token differs from its independently observed original page token.");
            }

            int prefix = record.NextPointer.HasValue ? sizeof(uint) : 0;
            if (stored.Length != record.RawBytes.Length || stored.Length < prefix
                || !stored.Span[prefix..].SequenceEqual(record.RawBytes.Span[prefix..]))
            {
                throw Corrupt("A private LVAL row differs in its complete raw payload or trailing storage.");
            }

            if (record.NextPointer.HasValue)
            {
                uint expected = record.NextPointer.Value == 0 ? 0 : this.lvalPages.NextRowBookmark;
                if (BinaryPrimitives.ReadUInt32LittleEndian(stored.Span) != expected)
                {
                    throw Corrupt("A private LVAL chain pointer does not name the next exclusively covered slot or terminal zero.");
                }
            }

            this.lvalRows++;
        }

        private async ValueTask<ReadOnlyMemory<byte>> ReadRowAsync(PageCursor cursor, bool longValue, CancellationToken token)
        {
            if (cursor.NeedsPage)
            {
                destination.Position = cursor.NextPageNumber * format.PageSize;
                await this.ReadAsync(destination, cursor.Buffer, token).ConfigureAwait(false);
                cursor.OpenPage(owner, longValue, token);
            }

            return cursor.TakeRow();
        }

        private async ValueTask ReadAsync(Stream stream, Memory<byte> buffer, CancellationToken token)
        {
            int offset = 0;
            while (offset < buffer.Length)
            {
                token.ThrowIfCancellationRequested();
                this.CheckLengths();
                int requested = buffer.Length - offset;
                if (requested > budget.MaxReadBytes - this.readBytes)
                {
                    throw Limit("Independent raw data/LVAL validation exceeds MaxReadBytes.");
                }

                this.readBytes += requested;
                int read = await stream.ReadAsync(buffer[offset..], token).ConfigureAwait(false);
                if (read <= 0 || read > requested)
                {
                    throw Corrupt("The reopened private destination or row ledger ended before a complete read.");
                }

                offset = StreamRead.AdvanceRead(buffer.Length, offset, read);
            }

            this.CheckLengths();
        }

        private void AddRawBytes(int count)
        {
            if (count > budget.MaxRawBytes - this.rawBytes)
            {
                throw Limit("Independent raw LVAL validation exceeds MaxRawBytes, including trailing storage.");
            }

            this.rawBytes += count;
        }

        private void CheckLengths()
        {
            if (destination.Length != destinationLength || ledger.Length != data.RelocationBytes)
            {
                throw Corrupt("The reopened private destination or complete row ledger changed length during validation.");
            }
        }
    }

    private sealed class PageCursor(JetFormat format, long firstPage, long pageCount)
    {
        private int rowCount;
        private int nextRow;

        internal byte[] Buffer { get; } = new byte[format.PageSize];

        internal long Pages { get; private set; }

        internal bool NeedsPage => this.nextRow == this.rowCount;

        internal long NextPageNumber => this.Pages < pageCount ? firstPage + this.Pages : throw Corrupt("The source rows exceed the complete declared private page allocation.");

        internal uint CurrentBookmark => checked(((uint)(firstPage + this.Pages - 1) << 8) | (uint)(this.nextRow - 1));

        internal uint NextValueBookmark => this.NeedsPage ? checked((uint)this.NextPageNumber << 8) : throw Corrupt("Private LVAL values share an allocation page.");

        internal uint NextRowBookmark => this.NeedsPage ? checked((uint)this.NextPageNumber << 8) : checked(((uint)(firstPage + this.Pages - 1) << 8) | (uint)this.nextRow);

        internal void OpenPage(long owner, bool longValue, CancellationToken token)
        {
            this.Pages++;
            this.rowCount = BinaryPrimitives.ReadUInt16LittleEndian(this.Buffer.AsSpan(format.DataPage.NumRows));
            if (this.Buffer[0] != Constants.PageTypes.Data || this.Buffer[1] != 1 || this.rowCount is <= 0 or > Constants.DataPage.MaxRowsPerPage
                || (longValue ? !this.Buffer.AsSpan(4, 4).SequenceEqual("LVAL"u8) : BinaryPrimitives.ReadUInt32LittleEndian(this.Buffer.AsSpan(format.DataPage.TDefOff)) != owner))
            {
                throw Corrupt("A private allocation page has an unexpected type, owner or row count.");
            }

            int directoryEnd = format.DataPage.RowsStart + (this.rowCount * sizeof(ushort));
            int prior = format.PageSize;
            for (int row = 0; row < this.rowCount; row++)
            {
                token.ThrowIfCancellationRequested();
                int start = BinaryPrimitives.ReadUInt16LittleEndian(this.Buffer.AsSpan(format.DataPage.RowsStart + (row * sizeof(ushort))));
                int capacity = longValue ? format.PageSize - format.DataPage.RowsStart - sizeof(ushort) : format.DataPage.MaxRowLength(format.PageSize);
                if (start < directoryEnd || start >= prior || prior - start > capacity)
                {
                    throw Corrupt("A private allocation page does not contain dense unflagged direct rows.");
                }

                prior = start;
            }

            if (BinaryPrimitives.ReadUInt16LittleEndian(this.Buffer.AsSpan(2)) != prior - directoryEnd)
            {
                throw Corrupt("A private allocation page's free-space count disagrees with its row directory.");
            }

            this.nextRow = 0;
        }

        internal ReadOnlyMemory<byte> TakeRow()
        {
            int start = BinaryPrimitives.ReadUInt16LittleEndian(this.Buffer.AsSpan(format.DataPage.RowsStart + (this.nextRow * sizeof(ushort))));
            int end = this.nextRow == 0 ? format.PageSize : BinaryPrimitives.ReadUInt16LittleEndian(this.Buffer.AsSpan(format.DataPage.RowsStart + ((this.nextRow - 1) * sizeof(ushort))));
            this.nextRow++;
            return this.Buffer.AsMemory(start, end - start);
        }

        internal void RequireValueComplete()
        {
            if (!this.NeedsPage)
            {
                throw Corrupt("A private LVAL value leaves undeclared or shared allocation slots.");
            }
        }

        internal void RequireComplete()
        {
            if (this.Pages != pageCount || !this.NeedsPage)
            {
                throw Corrupt("The source observations do not cover every declared private allocation page and row.");
            }
        }
    }
}
