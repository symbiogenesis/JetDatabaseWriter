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

public sealed class CompactRawDataLongValueRelocatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Relocate_PackedRowsReferencePrivateRawChainsAndRetainEveryOtherByte(DatabaseFormat kind, bool calculated)
    {
        (SourceImage source, TDefImage definition, byte[] row) = await CreateSourceAsync(kind, calculated);
        byte[] original = (byte[])row.Clone();
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        var allocations = new List<(int Column, long Page, long Count)>();

        CompactRawDataLongValueRelocatorResult result = await CompactRawDataLongValueRelocator.RelocateAsync(
            session,
            source.Format,
            2,
            2,
            definition,
            data,
            map,
            destination,
            new(),
            Ct,
            allocationVisitor: (field, allocation, _) =>
            {
                Assert.Equal(3, field.Location.PageNumber);
                Assert.Equal(0, field.Location.RowIndex);
                allocations.Add((field.Column.ColNum, allocation.FirstPageNumber, allocation.PageCount));
                return ValueTask.CompletedTask;
            });

        Assert.False(result.IsValidated);
        Assert.False(result.OwnershipProven);
        Assert.Equal(1, result.DataRows);
        Assert.Equal(2, result.Values);
        Assert.Equal(4, result.LongValueRows);
        Assert.Equal(2, result.PageCount);
        Assert.Equal(8, result.PointerBytes);
        Assert.Equal<(int, long, long)>([(0, 6, 1), (1, 7, 1)], allocations);
        List<CompactRawField> fields = await FieldsAsync(source.Format, definition, row);
        byte[] expectedRow = (byte[])row.Clone();
        BinaryPrimitives.WriteUInt32LittleEndian(expectedRow.AsSpan(fields[0].RowOffset + 4), 0x600);
        BinaryPrimitives.WriteUInt32LittleEndian(expectedRow.AsSpan(fields[1].RowOffset + 4), 0x700);
        var staged = new SourceImage(kind, destination.ToArray());
        int observedRows = 0;
        await CompactRawRowReader.ScanAsync(
            staged,
            staged.Format,
            2,
            new(),
            async (location, bytes, token) =>
            {
                Assert.Equal<byte>(expectedRow, bytes.Span);
                observedRows++;
                await CompactRawFieldReader.ScanAsync(
                    staged.Format,
                    definition,
                    location,
                    bytes,
                    async (field, valueToken) =>
                    {
                        if (field.Kind == CompactRawFieldKind.Stored)
                        {
                            int observed = 0;
                            await CompactLongValueAudit.AuditAsync(
                                staged,
                                staged.Format,
                                field.RawBytes,
                                new(),
                                (record, _) =>
                                {
                                    if (record.Kind == "lval-row")
                                    {
                                        Assert.True(record.PageNumber >= 6);
                                        Assert.Equal<byte>(observed++ == 0 ? [0xFE] : [0xFF], record.RawBytes.Span[4..]);
                                    }

                                    return ValueTask.CompletedTask;
                                },
                                valueToken);
                            Assert.Equal(2, observed);
                        }
                    },
                    token);
            },
            Ct);
        Assert.Equal(1, observedRows);
        Assert.Equal(original, row);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Relocate_ChangedDestinationRowRefusesBeforeAppendingOrPatching()
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        byte[] image = destination.ToArray();
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(image.AsSpan((5 * source.PageSize) + source.Format.DataPage.RowsStart));
        destination.Position = (5L * source.PageSize) + rowStart;
        destination.WriteByte(0xFF);
        byte[] before = destination.ToArray();

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct));

        Assert.Equal(before, destination.ToArray());
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task Relocate_AggregateLimitsRefuseWithNoCompletion(int limit)
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        CompactRawDataLongValueRelocatorBudget budget = limit switch
        {
            0 => new() { MaxValues = 0 },
            1 => new() { MaxDestinationReadBytes = 1 },
            2 => new() { MaxPages = 0 },
            3 => new() { MaxPointerBytes = 0 },
            _ => new() { MaxWorkingBytes = 1 },
        };

        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, budget, Ct));

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Relocate_UnverifiedCalculatedStorageRefuses()
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.AceAccdb, true);
        var unknown = new TDefImage([], definition.Header, [definition.Columns[0] with { CalculatedResultType = default }, definition.Columns[1], definition.Columns[2]], false, TDefParseIssues.None, null, [], []);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);

        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, unknown, data, map, destination, new(), Ct));

        Assert.Equal(JetErrorCode.FeatureNotSupported, error.ErrorCode);
        Assert.Equal("A", error.ErrorInfo.ColumnName);
        Assert.Equal(3, error.ErrorInfo.PageNumber);
        Assert.Equal(6L * source.PageSize, destination.Length);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);

        CompactRawDataLongValueRelocatorResult recovered = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, unknown, data, map, destination, new(), Ct, hydratedColumns: definition.Columns);
        Assert.Equal(2, recovered.Values);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Relocate_AllocationVisitorFailureStopsBeforeNextValueAndRefusesCompletion()
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        var failure = new IOException("allocation ledger failed");
        int observations = 0;

        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await CompactRawDataLongValueRelocator.RelocateAsync(
            session,
            source.Format,
            2,
            2,
            definition,
            data,
            map,
            destination,
            new(),
            Ct,
            allocationVisitor: (_, _, _) =>
            {
                observations++;
                return ValueTask.FromException(failure);
            }));

        Assert.Same(failure, actual);
        Assert.Equal(1, observations);
        Assert.Equal(7L * source.PageSize, destination.Length);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Relocate_InlineAllocationsDeliverColumnIdentityWithoutAppendingPages()
    {
        (SourceImage source, TDefImage definition, byte[] row) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        List<CompactRawField> fields = await FieldsAsync(source.Format, definition, row);
        LongValueDescriptor.Inline(0).WriteTo(row.AsSpan(fields[0].RowOffset));
        LongValueDescriptor.Inline(0).WriteTo(row.AsSpan(fields[1].RowOffset));
        source.SetRow(3, row, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        int observed = 0;

        CompactRawDataLongValueRelocatorResult result = await CompactRawDataLongValueRelocator.RelocateAsync(
            session,
            source.Format,
            2,
            2,
            definition,
            data,
            map,
            destination,
            new() { MaxPages = 0, MaxPointerBytes = 0, MaxOutputBytes = 0 },
            Ct,
            allocationVisitor: (field, allocation, _) =>
            {
                Assert.Equal(observed++, field.Column.ColNum);
                Assert.Equal(6, allocation.FirstPageNumber);
                Assert.Equal(0, allocation.PageCount);
                Assert.Equal(0, allocation.RowCount);
                Assert.Equal<byte>(field.RawBytes.Span, allocation.Column.Span);
                return ValueTask.CompletedTask;
            });

        Assert.Equal(2, observed);
        Assert.Equal(0, result.PageCount);
        Assert.Equal(0, result.PointerBytes);
        Assert.Equal(6L * source.PageSize, destination.Length);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task Relocate_PartialReadsIoFailuresAndPatchCancellationAreHandled(int mode)
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        var failure = new IOException("private destination failure");
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var destination = new FaultStream(mode, failure, cancellation);
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        destination.Enabled = true;

        if (mode == 0)
        {
            CompactRawDataLongValueRelocatorResult result = await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct);
            Assert.Equal(2, result.Values);
            Assert.True(result.DestinationReadBytes > source.PageSize);
        }
        else if (mode == 4)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), cancellation.Token));
        }
        else
        {
            IOException actual = await Assert.ThrowsAsync<IOException>(async () => await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct));
            Assert.Same(failure, actual);
        }

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Relocate_HydratedStorageCoordinateChangesRefuseBeforeMutation()
    {
        (SourceImage source, TDefImage definition, _) = await CreateSourceAsync(DatabaseFormat.Jet4Mdb, false);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, data.RowCount, 4096, Ct);
        ColumnInfo[] altered = [.. definition.Columns];
        altered[0] = altered[0] with { VarIdx = 1 };
        byte[] before = destination.ToArray();

        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawDataLongValueRelocator.RelocateAsync(session, source.Format, 2, 2, definition, data, map, destination, new(), Ct, hydratedColumns: altered));

        Assert.Equal(before, destination.ToArray());
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

    private sealed class FaultStream(int mode, IOException failure, CancellationTokenSource cancellation) : MemoryStream
    {
        private int flushes;

        internal bool Enabled { get; set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.Enabled && mode == 1)
            {
                return ValueTask.FromException<int>(failure);
            }

            return base.ReadAsync(this.Enabled && mode == 0 ? buffer[..Math.Min(3, buffer.Length)] : buffer, cancellationToken);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.Enabled && buffer.Length == 4 && mode == 2)
            {
                throw failure;
            }

            await base.WriteAsync(buffer, cancellationToken);
            if (this.Enabled && buffer.Length == 4 && mode == 4)
            {
                await cancellation.CancelAsync();
            }
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            if (this.Enabled)
            {
                this.flushes++;
                if (mode == 3 || (mode == 5 && this.flushes == 3))
                {
                    return Task.FromException(failure);
                }
            }

            return base.FlushAsync(cancellationToken);
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
