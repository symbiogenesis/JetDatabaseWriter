namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class CompactRawRowReaderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(nameof(TestDatabases.OverflowTestV1997))]
    [InlineData(nameof(TestDatabases.OverflowTestV2000))]
    [InlineData(nameof(TestDatabases.OverflowTestV2003))]
    [InlineData(nameof(TestDatabases.OverflowTestV2007))]
    [InlineData(nameof(TestDatabases.OverflowTestV2010))]
    public async Task Scan_ExistingOverflowSpecimensRetainEveryStoredPayload(string fieldName)
    {
        string path = (string)typeof(TestDatabases).GetField(fieldName)!.GetValue(null)!;
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: Ct);
        CatalogEntry? entry = await reader.GetCatalogEntryAsync("Table1", Ct);
        Assert.NotNull(entry);
        var bookmarks = new HashSet<(long Page, int Row)>();
        await CompactRawRowReader.ScanAsync(
            reader.Database.Pages,
            reader.Database.Format,
            entry.TDefPage,
            new(),
            async (location, bytes, token) =>
            {
                byte[] physical = await reader.ReadPageCopyAsync(location.DataPageNumber, token);
                Assert.Equal(physical.AsSpan(location.RowStart, location.RowSize).ToArray(), bytes.ToArray());
                Assert.True(bookmarks.Add((location.PageNumber, location.RowIndex)));
            },
            Ct);
        Assert.Equal(7, bookmarks.Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Scan_PreservesOpaqueBytesAndSlotIdentities(DatabaseFormat databaseFormat)
    {
        var source = new SyntheticPages(databaseFormat);
        byte[] first = [0xFF, 0, 0x80, 0x7F, 0, 0xFE];
        byte[] second = [0xFF, 2, 1];
        source.SetPage(3, (0, first), (0x8000, new byte[] { 8, 7 }), (0, second));
        source.SetPage(4, (0, new byte[] { 0xFF }));
        BinaryPrimitives.WriteInt32LittleEndian(source.Pages[4].AsSpan(source.Format.DataPage.TDefOff), 99);
        List<(RowLocation Location, byte[] Bytes)> rows = await ScanAsync(source);

        Assert.Equal(2, rows.Count);
        Assert.Equal(first, rows[0].Bytes);
        Assert.Equal(second, rows[1].Bytes);
        Assert.Equal(0, rows[0].Location.RowIndex);
        Assert.Equal(2, rows[1].Location.RowIndex);
        Assert.All(rows, row => Assert.Equal(3, row.Location.PageNumber));
        Assert.All(rows, row => Assert.False(row.Location.IsOverflow));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 0)]
    [InlineData(DatabaseFormat.Jet3Mdb, 1)]
    [InlineData(DatabaseFormat.Jet3Mdb, 2)]
    [InlineData(DatabaseFormat.Jet4Mdb, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, 1)]
    [InlineData(DatabaseFormat.Jet4Mdb, 2)]
    [InlineData(DatabaseFormat.AceAccdb, 0)]
    [InlineData(DatabaseFormat.AceAccdb, 1)]
    [InlineData(DatabaseFormat.AceAccdb, 2)]
    public async Task Scan_OverflowKeepsHeaderBookmarkAndDeletedPayload(DatabaseFormat databaseFormat, int layout)
    {
        SyntheticPages source = Overflow(databaseFormat, layout);
        List<(RowLocation Location, byte[] Bytes)> rows = await ScanAsync(source);
        (RowLocation location, byte[] bytes) = Assert.Single(rows);

        Assert.Equal(new byte[] { 0xFE, 0xFF, 0, 0x80, 3 }, bytes);
        Assert.Equal(3, location.PageNumber);
        Assert.Equal(0, location.RowIndex);
        Assert.Equal(layout switch { 1 => 3, 2 => 5, _ => 4 }, location.DataPageNumber);
        Assert.Equal(layout == 1 ? 1 : 0, location.DataRowIndex);
        Assert.True(location.IsOverflow);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task Scan_UnresolvedOverflowRefusesWithLocation(int damage)
    {
        SyntheticPages source = Overflow(DatabaseFormat.Jet4Mdb, 0);
        switch (damage)
        {
            case 0:
                source.SetPage(3, (0x4000, new byte[] { 0, 0xFF, 0xFF, 0xFF }));
                break;
            case 1:
                source.SetPage(3, (0x4000, new byte[] { 0, 4 }));
                break;
            case 2:
                source.SetPage(4, (0xC000, new byte[] { 0, 3, 0, 0 }));
                break;
            case 3:
                BinaryPrimitives.WriteInt32LittleEndian(source.Pages[4].AsSpan(source.Format.DataPage.TDefOff), 99);
                break;
            case 4:
                source.SetPage(3, (0x4000, new byte[] { 7, 4, 0, 0 }));
                break;
            default:
                source.SetPage(4, (0, new byte[] { 1, 2, 3 }));
                break;
        }

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source));
        Assert.Equal(JetErrorCode.MalformedValue, error.ErrorCode);
        Assert.NotNull(error.ErrorInfo.PageNumber);
        Assert.NotNull(error.ErrorInfo.Reason);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task Scan_InvalidDirectoryIsNeverClampedOrSkipped(int damage)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetPage(3, (0, new byte[] { 1, 2 }), (0, new byte[] { 3, 4 }));
        int directory = source.Format.DataPage.RowsStart;
        if (damage == 0)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(source.Pages[3].AsSpan(source.Format.DataPage.NumRows), ushort.MaxValue);
        }
        else if (damage == 1)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(source.Pages[3].AsSpan(directory), 1);
        }
        else if (damage == 2)
        {
            source.Pages[3].AsSpan(directory, 2).CopyTo(source.Pages[3].AsSpan(directory + 2, 2));
        }
        else if (damage == 3)
        {
            source.Pages[3][0] = 0x7F;
        }
        else
        {
            ushort raw = BinaryPrimitives.ReadUInt16LittleEndian(source.Pages[3].AsSpan(directory));
            BinaryPrimitives.WriteUInt16LittleEndian(source.Pages[3].AsSpan(directory), checked((ushort)(raw | 0x2000)));
        }

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source));
        Assert.Equal(3, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Scan_DeletedAliasDoesNotTruncateOrDuplicateLivePayload()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetPage(3, (0, new byte[] { 1, 2, 3 }), (0x8000, Array.Empty<byte>()));
        int directory = source.Format.DataPage.RowsStart;
        ushort start = BinaryPrimitives.ReadUInt16LittleEndian(source.Pages[3].AsSpan(directory));
        BinaryPrimitives.WriteUInt16LittleEndian(source.Pages[3].AsSpan(directory + 2), checked((ushort)(start | 0x8000)));
        Assert.Equal(new byte[] { 1, 2, 3 }, Assert.Single(await ScanAsync(source)).Bytes);
    }

    [Theory]
    [InlineData(0, "MaxPages")]
    [InlineData(1, "MaxWorkingBytes")]
    public async Task Scan_AdmissionRefusesBeforeIo(int kind, string reason)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        CompactRawRowBudget budget = kind == 0 ? new CompactRawRowBudget { MaxPages = 1 } : new CompactRawRowBudget { MaxWorkingBytes = 1 };
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, budget));
        Assert.Contains(reason, error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(0, source.Reads);
    }

    [Fact]
    public async Task Scan_RowLimitStopsBeforeSecondVisitor()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetPage(3, (0, new byte[] { 1 }), (0, new byte[] { 2 }));
        int visits = 0;
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawRowReader.ScanAsync(
            source,
            source.Format,
            2,
            new() { MaxRows = 1 },
            (_, _, _) =>
            {
                visits++;
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Contains("MaxRows", error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(1, visits);
    }

    [Fact]
    public async Task Scan_ReadAndSlotBudgetsIncludeRepeatedOverflowReads()
    {
        SyntheticPages source = Overflow(DatabaseFormat.Jet4Mdb, 0);
        JetLimitationException reads = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, new() { MaxPageReads = 1 }));
        Assert.Contains("MaxPageReads", reads.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(1, source.Reads);

        source = Overflow(DatabaseFormat.Jet4Mdb, 0);
        JetLimitationException slots = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, new() { MaxDirectorySlots = 2 }));
        Assert.Contains("MaxDirectorySlots", slots.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(3, source.Reads);
    }

    [Fact]
    public async Task Scan_OverflowHopBudgetRefusesBeforeFollowingAnotherPointer()
    {
        SyntheticPages source = Overflow(DatabaseFormat.Jet4Mdb, 2);
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, new() { MaxOverflowHops = 1 }));
        Assert.Contains("MaxOverflowHops", error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(2, source.Reads);
    }

    [Fact]
    public async Task Scan_CancellationAndActualIoFailuresPropagate()
    {
        SyntheticPages source = Overflow(DatabaseFormat.Jet4Mdb, 0);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawRowReader.ScanAsync(
            source, source.Format, 2, new(), async (_, _, _) => await cancellation.CancelAsync(), cancellation.Token));

        var failure = new InvalidDataException("The underlying source read failed.");
        source = Overflow(DatabaseFormat.Jet4Mdb, 0);
        source.Failure = failure;
        source.FailureOnRead = 2;
        InvalidDataException actual = await Assert.ThrowsAsync<InvalidDataException>(async () => await ScanAsync(source));
        Assert.Same(failure, actual);
    }

    [Fact]
    public async Task Scan_TruncatedOrChangingSourceRefuses()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb) { ShortRead = true };
        JetCorruptDataException truncated = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source));
        Assert.Equal(3, truncated.ErrorInfo.PageNumber);
        source = new SyntheticPages(DatabaseFormat.Jet4Mdb) { ChangingGeometry = true };
        JetCorruptDataException changed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source));
        Assert.Equal(3, changed.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Scan_GeometryChangeDuringAdmissionRefusesBeforeIo()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb) { ChangeCountOnPageSizeRead = true };
        JetCorruptDataException changed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, new() { MaxPages = 6 }));
        Assert.Equal(3, changed.ErrorInfo.PageNumber);
        Assert.Equal(0, source.Reads);
    }

    private static SyntheticPages Overflow(DatabaseFormat databaseFormat, int layout)
    {
        var source = new SyntheticPages(databaseFormat);
        byte[] payload = [0xFE, 0xFF, 0, 0x80, 3];
        if (layout == 1)
        {
            source.SetPage(3, (0x4000, new byte[] { 1, 3, 0, 0 }), (0x8000, payload));
        }
        else
        {
            source.SetPage(3, (0x4000, new byte[] { 0, 4, 0, 0 }));
            source.SetPage(4, layout == 2 ? (0xC000, [0, 5, 0, 0]) : (0x8000, payload));
            if (layout == 2)
            {
                source.SetPage(5, (0x8000, payload));
            }
        }

        return source;
    }

    private static async Task<List<(RowLocation Location, byte[] Bytes)>> ScanAsync(SyntheticPages source, CompactRawRowBudget? budget = null)
    {
        var result = new List<(RowLocation, byte[])>();
        await CompactRawRowReader.ScanAsync(
            source,
            source.Format,
            2,
            budget ?? new(),
            (location, bytes, _) =>
            {
                result.Add((location, bytes.ToArray()));
                return ValueTask.CompletedTask;
            },
            Ct);
        return result;
    }

    private sealed class SyntheticPages(DatabaseFormat databaseFormat) : IPageSource
    {
        private bool pageSizeObserved;

        internal JetFormat Format { get; } = JetFormat.ForNewDatabase(databaseFormat);

        internal byte[][] Pages { get; } = new byte[6][];

        internal int Reads { get; private set; }

        internal Exception? Failure { get; set; }

        internal int FailureOnRead { get; set; }

        internal bool ShortRead { get; init; }

        internal bool ChangingGeometry { get; init; }

        internal bool ChangeCountOnPageSizeRead { get; init; }

        public int PageSize
        {
            get
            {
                this.pageSizeObserved = true;
                return this.Format.PageSize;
            }
        }

        public long PageCount => this.Pages.Length + ((this.ChangingGeometry && this.Reads > 0) || (this.ChangeCountOnPageSizeRead && this.pageSizeObserved) ? 1 : 0);

        public bool IsDisposed => false;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            if (this.Reads == this.FailureOnRead && this.Failure is { } error)
            {
                return ValueTask.FromException<byte[]>(error);
            }

            byte[] page = ArrayPool<byte>.Shared.Rent(this.ShortRead ? 128 : this.PageSize);
            page.AsSpan().Clear();
            if (pageNumber < this.Pages.Length && this.Pages[checked((int)pageNumber)] is { } stored && !this.ShortRead)
            {
                stored.CopyTo(page, 0);
            }

            return ValueTask.FromResult(page);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();

        internal void SetPage(int pageNumber, params (int Flags, byte[] Bytes)[] rows)
        {
            byte[] page = new byte[this.PageSize];
            page[0] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(this.Format.DataPage.TDefOff), 2);
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.NumRows), checked((ushort)rows.Length));
            int offset = this.PageSize;
            for (int index = 0; index < rows.Length; index++)
            {
                offset -= rows[index].Bytes.Length;
                rows[index].Bytes.CopyTo(page, offset);
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.RowsStart + (index * 2)), checked((ushort)(offset | rows[index].Flags)));
            }

            this.Pages[pageNumber] = page;
        }
    }
}
