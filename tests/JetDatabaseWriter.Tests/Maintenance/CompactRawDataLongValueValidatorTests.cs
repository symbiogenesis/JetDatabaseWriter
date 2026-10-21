namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

public sealed class CompactRawDataLongValueValidatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Validate_ReopenedRowsAndCompleteSharedSourceChainsAreIndependentlyCovered(DatabaseFormat kind, bool calculated)
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(kind, calculated);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        await using var reopenedLedger = new MemoryStream(ledger.ToArray(), writable: false);

        CompactRawDataLongValueValidatorResult result = await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, reopened, reopenedLedger, data, values, new(), Ct);

        Assert.Equal(1, result.DataRows);
        Assert.Equal(2, result.Values);
        Assert.Equal(4, result.LongValueRows);
        Assert.Equal(2, result.LongValuePages);
        Assert.Equal(values.RawBytes, result.RawBytes);
        Assert.False(result.IsValidated);
        Assert.False(result.OwnershipProven);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Validate_ChangedNativeLvalPageTokenRefusesEvenWhenRowsAndPointersMatch(DatabaseFormat kind)
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(kind, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);
        byte[] bytes = destination.ToArray();
        bytes[checked((int)(values.FirstPageNumber * source.PageSize)) + 8] ^= 1;
        await using var reopened = new MemoryStream(bytes, writable: false);
        await using var reopenedLedger = new MemoryStream(ledger.ToArray(), writable: false);

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, reopened, reopenedLedger, data, values, new(), Ct));

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(13)]
    [InlineData(14)]
    public async Task Validate_DescriptorPayloadTrailingLinksOwnershipCoverageAndLedgerTamperingRefuse(int change)
    {
        (SourceImage source, TDefImage definition, byte[] row) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);
        byte[] bytes = destination.ToArray();
        byte[] records = ledger.ToArray();
        List<CompactRawField> fields = await FieldsAsync(source.Format, definition, row);
        int dataStart = (6 * source.PageSize) - row.Length;
        int firstLvalStart = (7 * source.PageSize) - 5;
        int trailingStart = firstLvalStart - 5;
        switch (change)
        {
            case 0:
                bytes[dataStart + fields[0].RowOffset] ^= 1;
                break;
            case 1:
                bytes[dataStart + fields[0].RowOffset + 4] ^= 1;
                break;
            case 2:
                bytes[dataStart + fields[0].RowOffset + 8] ^= 1;
                break;
            case 3:
                bytes[firstLvalStart + 4] ^= 1;
                break;
            case 4:
                bytes[trailingStart + 4] ^= 1;
                break;
            case 5:
                bytes[firstLvalStart] ^= 1;
                break;
            case 6:
                bytes[trailingStart] = 1;
                break;
            case 7:
                bytes[(6 * source.PageSize) + 4] ^= 1;
                break;
            case 8:
                records[4] ^= 1;
                break;
            case 9:
                values = values with { PageCount = 1 };
                break;
            case 10:
                values = values with { LongValueRows = 3 };
                break;
            case 11:
                bytes[(5 * source.PageSize) + source.Format.DataPage.TDefOff] ^= 1;
                break;
            case 12:
                bytes[dataStart + fields[2].RowOffset] ^= 1;
                break;
            case 13:
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(dataStart + fields[1].RowOffset + 4), 0x600);
                break;
            default:
                Array.Resize(ref records, records.Length + 8);
                break;
        }

        await using var reopened = new MemoryStream(bytes, writable: false);
        await using var reopenedLedger = new MemoryStream(records, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, reopened, reopenedLedger, data, values, new(), Ct));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task Validate_AggregateBudgetsRefuseAndReleaseStorage(int limit)
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);
        CompactRawDataLongValueValidatorBudget budget = limit switch
        {
            0 => new() { MaxValues = 1 },
            1 => new() { MaxReadBytes = 1 },
            2 => new() { MaxLongValueRows = 1 },
            3 => new() { MaxRawBytes = 1 },
            _ => new() { MaxWorkingBytes = 1 },
        };

        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, destination, ledger, data, values, budget, Ct));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Validate_ChainsSpanningPrivatePagesRetainAllTrailingBytes()
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        byte[] large = new byte[source.PageSize - 20];
        large.AsSpan().Fill(0xFE);
        BinaryPrimitives.WriteUInt32LittleEndian(large, 0x800);
        source.SetRow(7, large, true);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);

        CompactRawDataLongValueValidatorResult result = await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, destination, ledger, data, values, new(), Ct);

        Assert.Equal(4, result.LongValuePages);
        Assert.Equal(4, result.LongValueRows);
        Assert.Equal(values.RawBytes, result.RawBytes);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Validate_InlineAndEmptyExternalValuesConsumeNoPrivatePages(bool inline)
    {
        (SourceImage source, TDefImage definition, byte[] row) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        List<CompactRawField> fields = await FieldsAsync(source.Format, definition, row);
        LongValueDescriptor descriptor = inline ? LongValueDescriptor.Inline(0) : LongValueDescriptor.Chained(0, 0, 0);
        descriptor.WriteTo(row.AsSpan(fields[0].RowOffset));
        descriptor.WriteTo(row.AsSpan(fields[1].RowOffset));
        source.SetRow(3, row, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);

        CompactRawDataLongValueValidatorResult result = await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, destination, ledger, data, values, new(), Ct);

        Assert.Equal(2, result.Values);
        Assert.Equal(0, result.LongValuePages);
        Assert.Equal(0, result.LongValueRows);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Validate_PartialReadsIoFailureChangingLengthsAndCancellation(int mode)
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var failure = new IOException("private validation read failed");
        await using var reopened = new ReadStream(destination.ToArray(), mode, failure, cancellation);
        if (mode == 0)
        {
            CompactRawDataLongValueValidatorResult result = await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, reopened, ledger, data, values, new(), Ct);
            Assert.True(result.ReadBytes > (3L * source.PageSize) + data.RelocationBytes);
        }
        else if (mode == 1)
        {
            IOException actual = await Assert.ThrowsAsync<IOException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, reopened, ledger, data, values, new(), Ct));
            Assert.Same(failure, actual);
        }
        else if (mode == 2)
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, reopened, ledger, data, values, new(), Ct));
        }
        else
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, reopened, ledger, data, values, new(), cancellation.Token));
        }

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Validate_CalculatedStorageRequiresPersistedProjectionAndRejectsCoordinateChanges()
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.AceAccdb, true);
        var unknown = new TDefImage([], definition.Header, [definition.Columns[0] with { CalculatedResultType = default }, definition.Columns[1], definition.Columns[2]], false, TDefParseIssues.None, null, [], []);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);

        JetLimitationException refusal = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, unknown, destination, ledger, data, values, new(), Ct));
        Assert.Equal(JetErrorCode.FeatureNotSupported, refusal.ErrorCode);
        CompactRawDataLongValueValidatorResult result = await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, unknown, destination, ledger, data, values, new(), Ct, hydratedColumns: definition.Columns);
        Assert.Equal(2, result.Values);
        ColumnInfo[] changed = [.. definition.Columns];
        changed[0] = changed[0] with { VarIdx = 1 };
        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, unknown, destination, ledger, data, values, new(), Ct, hydratedColumns: changed));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Validate_EmptyTableRequiresEmptyCompleteAllocationsAndLedger()
    {
        (_, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var source = new SourceImage(DatabaseFormat.Jet4Mdb, 9);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);

        CompactRawDataLongValueValidatorResult result = await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, destination, ledger, data, values, new(), Ct);

        Assert.Equal(0, result.DataRows);
        Assert.Equal(0, result.Values);
        Assert.Equal(0, result.ReadBytes);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Validate_MultipleDataPagesAndValuesHaveCompleteDisjointCoverage()
    {
        (_, TDefImage definition, byte[] row) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var source = new SourceImage(DatabaseFormat.Jet4Mdb, 96);
        int rows = 0;
        for (int page = 3; page < source.PageCount; page++)
        {
            if (page is not 7 and not 8)
            {
                source.SetRow(page, row, false);
                rows++;
            }
        }

        source.SetRow(7, [0, 8, 0, 0, 0xFE], true);
        source.SetRow(8, [0, 0, 0, 0, 0xFF], true);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        Assert.True(data.PageCount > 1);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 1_000_000, Ct);
        CompactRawDataLongValueRelocatorResult values = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);

        CompactRawDataLongValueValidatorResult result = await CompactRawDataLongValueValidator.ValidateAsync(session, 2, 2, definition, destination, ledger, data, values, new(), Ct);

        Assert.Equal(rows, result.DataRows);
        Assert.Equal(rows * 2, result.Values);
        Assert.Equal(rows * 4, result.LongValueRows);
        Assert.Equal(rows * 2, result.LongValuePages);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    private static async Task<(SourceImage Source, TDefImage Definition, byte[] Row)> CreateSourceAsync(DatabaseFormat kind, bool calculated)
    {
        var source = new SourceImage(kind, 9);
        var definition = new TDefImage(
            [],
            new(3, 0, 0, default),
            [new() { ColNum = 0, VarIdx = 0, Name = "A", Type = ColumnType.MemoType, ExtraFlags = calculated ? (byte)0xC0 : (byte)0, CalculatedResultType = calculated ? ColumnType.MemoType : default },
            new() { ColNum = 1, VarIdx = 1, Name = "B", Type = ColumnType.MemoType },
            new() { ColNum = 2, VarIdx = 2, Name = "Unassigned", Type = ColumnType.MemoType },
            ],
            false,
            TDefParseIssues.None,
            null,
            [],
            []);
        byte[] row = new RowEncoder(source.Format).SerializeRow(definition.Definition, ["abc", "def", "opaque"]);
        List<CompactRawField> fields = await FieldsAsync(source.Format, definition, row);
        LongValueDescriptor.Chained(1, 0x700, 0xDEADBEEF).WriteTo(row.AsSpan(fields[0].RowOffset));
        LongValueDescriptor.Chained(1, 0x700, 0xFEEDABCD).WriteTo(row.AsSpan(fields[1].RowOffset));
        row.AsSpan(fields[2].RowOffset, 12).Fill(0xFF);
        row[^1] = (byte)(row[^1] & 0xFB);
        source.SetRow(3, row, false);
        source.SetRow(7, [0, 8, 0, 0, 0xFE], true);
        source.SetRow(8, [0, 0, 0, 0, 0xFF], true);
        return (source, definition, row);
    }

    private static async Task<List<CompactRawField>> FieldsAsync(JetFormat format, TDefImage definition, ReadOnlyMemory<byte> row)
    {
        var fields = new List<CompactRawField>();
        await CompactRawFieldReader.ScanAsync(
            format,
            definition,
            new(3, 0, 0, row.Length),
            row,
            (field, _) =>
            {
                fields.Add(field);
                return ValueTask.CompletedTask;
            },
            Ct);
        return fields;
    }

    private sealed class ReadStream(byte[] bytes, int mode, IOException failure, CancellationTokenSource cancellation) : MemoryStream(bytes)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (mode == 1)
            {
                throw failure;
            }

            int read = await base.ReadAsync(mode == 0 ? buffer[..Math.Min(3, buffer.Length)] : buffer, cancellationToken);
            if (mode == 2)
            {
                this.SetLength(this.Length - 1);
            }
            else if (mode == 3)
            {
                await cancellation.CancelAsync();
            }

            return read;
        }
    }

    private sealed class SourceImage : IPageSource
    {
        private readonly byte[][] pages;

        internal SourceImage(DatabaseFormat kind, int count)
        {
            this.Format = JetFormat.ForNewDatabase(kind);
            this.pages = new byte[count][];
            for (int i = 0; i < count; i++)
            {
                this.pages[i] = new byte[this.PageSize];
            }
        }

        internal SourceImage(DatabaseFormat kind, byte[] image)
            : this(kind, image.Length / JetFormat.ForNewDatabase(kind).PageSize)
        {
            for (int i = 0; i < this.pages.Length; i++)
            {
                image.AsSpan(i * this.PageSize, this.PageSize).CopyTo(this.pages[i]);
            }
        }

        public int PageSize => this.Format.PageSize;

        public long PageCount => this.pages.Length;

        public bool IsDisposed => false;

        internal JetFormat Format { get; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] rented = ArrayPool<byte>.Shared.Rent(this.PageSize);
            this.pages[checked((int)pageNumber)].CopyTo(rented, 0);
            return ValueTask.FromResult(rented);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();

        internal void SetRow(int number, byte[] row, bool longValue)
        {
            byte[] page = this.pages[number];
            page[0] = 1;
            if (longValue)
            {
                "LVAL"u8.CopyTo(page.AsSpan(4));
            }
            else
            {
                BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(this.Format.DataPage.TDefOff), 2);
            }

            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.NumRows), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.RowsStart), checked((ushort)(this.PageSize - row.Length)));
            row.CopyTo(page, this.PageSize - row.Length);
        }
    }
}
