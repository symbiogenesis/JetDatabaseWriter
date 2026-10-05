namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// A table's usage-map rows keep every page they name once the table outgrows
/// an INLINE row's 512-page window: the owned-pages and free-space rows are
/// promoted to REFERENCE when they already list a page, an empty one moves its
/// window instead, and an index row whose rebuilt tree spans two windows is
/// written as REFERENCE. The bitmap pages follow the REFERENCE owned-pages maps
/// of binIdxTestV2010.accdb and testEmoticonsV2010.accdb.
/// </summary>
public sealed class OwnedUsageMapGrowthTests
{
    private const string WideTable = "Wide";
    private const int WideRowCount = 700;
    private const int BinaryColumns = 10;
    private const int InlineWindow = Constants.UsageMap.InlineBitmapBits;

    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    /// <summary>The first bytes of a REFERENCE bitmap page, as page 87 of binIdxTestV2010.accdb holds them.</summary>
    private static readonly byte[] BitmapPageHeader = [Constants.PageTypes.UsageMap, 0x01, 0x00, 0x00];

    public static TheoryData<DatabaseFormat, WriteMode> Modes => WriteModes.Combine(DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    public static TheoryData<DatabaseFormat> Formats => [DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// 700 rows that each fill a data page take the table past page 512. Its
    /// owned-pages row lists every one of its data pages, as REFERENCE bitmap
    /// pages, where the INLINE row lost the pages outside its window, and so
    /// does its free-space row; the reader takes the map instead of scanning
    /// the whole file.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task OnePageRows_PastTheInlineWindow_AreAllInTheOwnedMap(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format, WideTable);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => _ = await writer.InsertRowsAsync(WideTable, WideRows(1, WideRowCount), Ct), Ct);
        }

        await using var backing = new MemoryStream(stream.ToArray(), writable: false);
        await using var counting = new CountingStream(backing);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(counting, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        long tdefPage = await TDefPageAsync(harness, WideTable);

        counting.Reset();
        IReadOnlyList<long> ownedPages = await db.OwnedPages.GetOwnedDataPagesAsync(tdefPage, Ct);
        HashSet<long> pagesRead = counting.PagesRead(db.PageSizeBytes);
        Assert.False(
            pagesRead.IsSupersetOf(Enumerable.Range(3, checked((int)db.PageCount) - 3).Select(page => (long)page)),
            "The reader rejected the owned-pages map and took the whole-file owned-page pass.");

        HashSet<long> dataPages = await ReadDataPagesOfAsync(db, tdefPage);
        Assert.Equal(WideRowCount, dataPages.Count);
        Assert.Contains(dataPages, page => page >= InlineWindow);
        Assert.Equal(dataPages.Order(), ownedPages.Order());

        UsageMapRow owned = await ReadTableMapRowAsync(db, tdefPage, freeSpace: false);
        Assert.Equal(Constants.UsageMap.ReferenceMapType, owned.Type);
        Assert.Equal(dataPages.Order(), owned.Pages.Order());
        await AssertBitmapPagesAsync(db, owned);

        UsageMapRow free = await ReadTableMapRowAsync(db, tdefPage, freeSpace: true);
        Assert.Equal(dataPages.Order(), free.Pages.Order());
    }

    /// <summary>
    /// A table whose first data page lies past page 512 moves its empty INLINE
    /// owned-pages row to that page's window instead of promoting it, so a
    /// table that fits one window keeps an INLINE map.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task TableStartingPastTheInlineWindow_KeepsAnInlineOwnedMap(DatabaseFormat format)
    {
        const string lateTable = "Late";
        const int lateRows = 40;
        await using MemoryStream stream = await CreateDatabaseAsync(format, WideTable, lateTable);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            _ = await writer.InsertRowsAsync(WideTable, WideRows(1, 600), Ct);
            _ = await writer.InsertRowsAsync(lateTable, WideRows(1, lateRows), Ct);
        }

        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        long tdefPage = await TDefPageAsync(harness, lateTable);
        HashSet<long> dataPages = await ReadDataPagesOfAsync(db, tdefPage);
        Assert.Equal(lateRows, dataPages.Count);
        Assert.All(dataPages, page => Assert.True(page >= InlineWindow, $"Data page {page} of {lateTable} is inside the first window."));

        UsageMapRow owned = await ReadTableMapRowAsync(db, tdefPage, freeSpace: false);
        Assert.Equal(Constants.UsageMap.InlineMapType, owned.Type);
        Assert.Equal(dataPages.Min() / 8 * 8, owned.InlineStartPage);
        Assert.Equal(dataPages.Order(), owned.Pages.Order());
    }

    /// <summary>
    /// Dropping a table past page 512 frees the bitmap pages of its REFERENCE
    /// owned-pages and free-space rows with the rest of its pages.
    /// <see cref="PageAudit.FindAllocatedPagesAsync"/> counts those pages as
    /// allocated until then; it leaves out only the global map's.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task DropTable_PastTheInlineWindow_FreesItsBitmapPages(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format, WideTable);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            _ = await writer.InsertRowsAsync(WideTable, WideRows(1, WideRowCount), Ct);
        }

        SortedSet<long> bitmapPages = await ReadWideTableBitmapPagesAsync(stream);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.DropTableAsync(WideTable, Ct);
        }

        await AssertPagesFreeAsync(stream, bitmapPages);
    }

    /// <summary>
    /// A schema rewrite of a table past page 512 frees the bitmap pages of the
    /// old table's REFERENCE rows once the rebuilt table, whose own REFERENCE
    /// rows list every one of its data pages, takes its place.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task AddColumn_PastTheInlineWindow_FreesTheOldBitmapPages(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format, WideTable);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            _ = await writer.InsertRowsAsync(WideTable, WideRows(1, WideRowCount), Ct);
        }

        SortedSet<long> bitmapPages = await ReadWideTableBitmapPagesAsync(stream);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.AddColumnAsync(WideTable, new ColumnDefinition("Extra", typeof(int)), Ct);
        }

        SortedSet<long> rebuiltBitmapPages = await ReadWideTableBitmapPagesAsync(stream);
        Assert.Empty(rebuiltBitmapPages.Intersect(bitmapPages));
        await AssertPagesFreeAsync(stream, bitmapPages);
    }

    /// <summary>
    /// A full index rebuild whose tree straddles page 512 lists every page of
    /// each tree in its index's usage-map row, the straddling one as a
    /// REFERENCE row, and the next rebuild, which moves the trees again, keeps
    /// that row's bitmap pages. The relationship sends every write to Keys
    /// through the full rebuild; before, the straddling tree threw
    /// <see cref="NotSupportedException"/>. ACCDB only: the Jet4 files the
    /// writer creates have no <c>MSysRelationships</c> table.
    /// </summary>
    [Fact]
    public async Task FullIndexRebuild_AcrossTheInlineWindow_ListsEveryTreePage()
    {
        const int keyRows = 3_000;
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb, WideTable);
        await using WriterHarness writer = await WriterHarness.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), cancellationToken: Ct);
        DatabaseFile db = writer.Database;
        await writer.CreateTableAsync("Parent", [new("Id", typeof(int)) { IsNullable = false }], [new IndexDefinition("PK_Parent", "Id") { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync(
            "Keys",
            [new("Id", typeof(int)) { IsNullable = false }, new("ParentId", typeof(int)), new("Code", typeof(string), maxLength: 255)],
            [new IndexDefinition("PK_Keys", "Id") { IsPrimaryKey = true }, new IndexDefinition("IX_Keys_Code", "Code")],
            Ct);
        await writer.InsertRowAsync("Parent", [1], Ct);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Keys_Parent", "Parent", "Id", "Keys", "ParentId"), Ct);
        _ = await writer.InsertRowsAsync("Keys", Enumerable.Range(1, keyRows).Select(KeyRow), Ct);

        CatalogEntry? entry = await writer.Services.Catalog.GetCatalogEntryAsync("Keys", Ct);
        Assert.NotNull(entry);
        long tdefPage = entry.TDefPage;
        int largestTree = (await ReadIndexMapsAsync(db, tdefPage)).Max(map => map.Tree.Count);

        // One-page rows take the end of file to about half a tree below page
        // 512, so the trees the next rebuild appends straddle it.
        long target = InlineWindow - (largestTree / 2);
        Assert.True(db.PageCount < target, $"The file already has {db.PageCount} pages; the next tree ({largestTree} pages) would start past page {target}.");
        int nextId = 1;
        while (db.PageCount < target)
        {
            int rows = checked((int)(target - db.PageCount));
            _ = await writer.InsertRowsAsync(WideTable, WideRows(nextId, rows), Ct);
            nextId += rows;
        }

        await writer.InsertRowAsync("Keys", KeyRow(keyRows + 1), Ct);
        List<IndexMap> maps = await ReadIndexMapsAsync(db, tdefPage);
        int code = Enumerable.Range(0, maps.Count).MaxBy(index => maps[index].Tree.Count);
        Assert.Contains(maps[code].Tree, page => page < InlineWindow);
        Assert.Contains(maps[code].Tree, page => page >= InlineWindow);
        Assert.All(maps, map => Assert.Equal(map.Tree.Order(), map.Row.Pages.Order()));
        Assert.Equal(Constants.UsageMap.ReferenceMapType, maps[code].Row.Type);
        await AssertBitmapPagesAsync(db, maps[code].Row);

        await writer.InsertRowAsync("Keys", KeyRow(keyRows + 2), Ct);
        List<IndexMap> rebuilt = await ReadIndexMapsAsync(db, tdefPage);
        Assert.NotEqual(maps[code].Tree.Order(), rebuilt[code].Tree.Order());
        Assert.All(rebuilt, map => Assert.Equal(map.Tree.Order(), map.Row.Pages.Order()));
        Assert.Equal(Constants.UsageMap.ReferenceMapType, rebuilt[code].Row.Type);
        Assert.Equal(maps[code].Row.BitmapPages, rebuilt[code].Row.BitmapPages);
    }

    /// <summary>
    /// DAO reads every row of a writer-built table whose data pages run past
    /// page 512: a snapshot of a table with no index walks the owned-pages
    /// map, which is REFERENCE by then, and a compacted copy keeps every row.
    /// </summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task Dao_RecordCount_SeesRowsOnPagesPastInlineWindow()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateTableAsync(WideTable, WideColumns(), Ct);
            _ = await writer.InsertRowsAsync(WideTable, WideRows(1, WideRowCount), Ct);
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            $$"""
            $rs = $db.OpenRecordset('SELECT * FROM [{{WideTable}}]', 4)
            try {
                $rs.MoveLast()
                Write-Output "RECORDCOUNT=$($rs.RecordCount)"
            } finally {
                $rs.Close()
            }
            """,
            DaoTimeout);
        Assert.True(
            result.ExitCode == 0,
            $"DAO OpenRecordset and CompactDatabase failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
        Assert.Contains($"RECORDCOUNT={WideRowCount}", result.StdOut, StringComparison.Ordinal);

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        int rows = 0;
        await foreach (object[] row in reader.Rows(WideTable, cancellationToken: Ct))
        {
            Assert.Equal(1 + BinaryColumns, row.Length);
            rows++;
        }

        Assert.Equal(WideRowCount, rows);
    }

    /// <summary>An Id column and ten 255-byte binary columns, so each full row fills a data page of its own.</summary>
    private static List<ColumnDefinition> WideColumns()
    {
        List<ColumnDefinition> columns = [new("Id", typeof(int))];
        for (int i = 0; i < BinaryColumns; i++)
        {
            columns.Add(new ColumnDefinition("B" + i.ToString(CultureInfo.InvariantCulture), typeof(byte[]), maxLength: 255));
        }

        return columns;
    }

    /// <summary>Returns <paramref name="count"/> full rows for a <see cref="WideColumns"/> table, numbered from <paramref name="firstId"/>.</summary>
    /// <param name="firstId">The first row's Id.</param>
    /// <param name="count">The number of rows.</param>
    private static IEnumerable<object?[]> WideRows(int firstId, int count)
    {
        for (int id = firstId; id < firstId + count; id++)
        {
            object?[] row = new object?[1 + BinaryColumns];
            row[0] = id;
            for (int c = 1; c < row.Length; c++)
            {
                byte[] blob = new byte[255];
                Array.Fill(blob, (byte)(id & 0xFF));
                row[c] = blob;
            }

            yield return row;
        }
    }

    /// <summary>A Keys row whose 127-character code, the most a text index key holds, gives the code index a tree of many pages.</summary>
    /// <param name="id">The row's Id.</param>
    private static object?[] KeyRow(int id) => [id, 1, id.ToString("D6", CultureInfo.InvariantCulture) + new string('k', 121)];

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, params string[] wideTables)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            foreach (string table in wideTables)
            {
                await writer.CreateTableAsync(table, WideColumns(), Ct);
            }
        }

        stream.Position = 0;
        return stream;
    }

    private static async Task<long> TDefPageAsync(ReaderHarness harness, string tableName)
    {
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        return entry.TDefPage;
    }

    /// <summary>Returns every data page whose owner field names <paramref name="tdefPage"/>, read page by page.</summary>
    /// <param name="db">The database file.</param>
    /// <param name="tdefPage">The table's TDEF page.</param>
    private static async Task<HashSet<long>> ReadDataPagesOfAsync(DatabaseFile db, long tdefPage)
    {
        var pages = new HashSet<long>();
        for (long pageNumber = 3; pageNumber < db.PageCount; pageNumber++)
        {
            byte[] page = await db.ReadPageCopyAsync(pageNumber, Ct);
            if (page[0] == Constants.PageTypes.Data && Ri32(page, db.DataPage.TDefOff) == tdefPage)
            {
                _ = pages.Add(pageNumber);
            }
        }

        return pages;
    }

    /// <summary>
    /// Checks that <see cref="WideTable"/>'s owned-pages and free-space rows are
    /// REFERENCE rows that list each of its <see cref="WideRowCount"/> data
    /// pages, and that <see cref="PageAudit.FindAllocatedPagesAsync"/> counts
    /// their bitmap pages as allocated; returns those bitmap pages.
    /// </summary>
    /// <param name="stream">The database.</param>
    private static async Task<SortedSet<long>> ReadWideTableBitmapPagesAsync(MemoryStream stream)
    {
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(WideTable, Ct);
        Assert.NotNull(entry);
        HashSet<long> dataPages = await ReadDataPagesOfAsync(db, entry.TDefPage);
        Assert.Equal(WideRowCount, dataPages.Count);

        UsageMapRow[] rows = [await ReadTableMapRowAsync(db, entry.TDefPage, freeSpace: false), await ReadTableMapRowAsync(db, entry.TDefPage, freeSpace: true)];
        var bitmapPages = new SortedSet<long>();
        foreach (UsageMapRow row in rows)
        {
            Assert.Equal(Constants.UsageMap.ReferenceMapType, row.Type);
            Assert.Equal(dataPages.Order(), row.Pages.Order());
            await AssertBitmapPagesAsync(db, row);
            bitmapPages.UnionWith(row.BitmapPages.Select(page => (long)page));
        }

        SortedSet<long> allocated = await PageAudit.FindAllocatedPagesAsync(db, harness.Services.PageAllocator, Ct);
        Assert.Empty(bitmapPages.Except(allocated));
        return bitmapPages;
    }

    /// <summary>Checks that the global usage map lists each of <paramref name="pages"/> as free.</summary>
    /// <param name="stream">The database.</param>
    /// <param name="pages">The pages that must be free.</param>
    private static async Task AssertPagesFreeAsync(MemoryStream stream, SortedSet<long> pages)
    {
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), cancellationToken: Ct);
        foreach (long page in pages)
        {
            Assert.True(await harness.Services.PageAllocator.IsPageFreeAsync(page, Ct), $"Bitmap page {page} should be free.");
        }
    }

    private static async Task<UsageMapRow> ReadTableMapRowAsync(DatabaseFile db, long tdefPage, bool freeSpace)
    {
        byte[] tdef = await db.ReadPageCopyAsync(tdefPage, Ct);
        int pointer = freeSpace ? db.TDef.FreePages : db.TDef.UsedPages;
        return await ReadUsageMapRowAsync(db, UsageMap.ReadUInt24(tdef, pointer + 1), tdef[pointer]);
    }

    /// <summary>Returns the tree pages of every real index of the table at <paramref name="tdefPage"/> and the usage-map row its <c>used_pages</c> names, by real-index number.</summary>
    /// <param name="db">The database file.</param>
    /// <param name="tdefPage">The table's TDEF page.</param>
    private static async Task<List<IndexMap>> ReadIndexMapsAsync(DatabaseFile db, long tdefPage)
    {
        byte[]? tdef = await db.ReadTDefBytesAsync(tdefPage, Ct);
        Assert.NotNull(tdef);
        int numCols = Ru16(tdef, db.TDef.NumCols);
        int numRealIdx = Ri32(tdef, db.TDef.NumRealIdx);
        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(db, tdef, numCols, numRealIdx);
        Assert.True(realIdxDescStart >= 0, $"The TDEF at page {tdefPage} could not be walked.");

        var maps = new List<IndexMap>(numRealIdx);
        for (int realIdx = 0; realIdx < numRealIdx; realIdx++)
        {
            Assert.True(db.IndexLayoutInfo.TryReadRealIdxSlot(tdef, realIdxDescStart, realIdx, out RealIdxSlot slot));
            int usedPages = slot.FirstDpOffset - 4;
            HashSet<long> tree = await IndexLeafChain.ReadTreePagesAsync(db, tdefPage, (uint)Ri32(tdef, slot.FirstDpOffset), Ct);
            maps.Add(new IndexMap(tree, await ReadUsageMapRowAsync(db, UsageMap.ReadUInt24(tdef, usedPages + 1), tdef[usedPages])));
        }

        return maps;
    }

    private static async Task<UsageMapRow> ReadUsageMapRowAsync(DatabaseFile db, int usageMapPage, int rowIndex)
    {
        byte[] page = await db.ReadPageCopyAsync(usageMapPage, Ct);
        Assert.True(
            UsageMap.TryGetRowBound(page, db.DataPage, db.PageSizeBytes, rowIndex, out RowBound bound),
            $"Usage-map page {usageMapPage} has no row {rowIndex}.");

        var pages = new List<long>();
        Assert.True(
            await UsageMap.TryEnumeratePagesAsync(page, bound, db.PageSizeBytes, db.PageCount, minimumPageNumber: 1, strict: true, db.ReadPageAsync, DatabaseFile.ReturnPage, pages, Ct),
            $"Row {rowIndex} of usage-map page {usageMapPage} is not a usage map whose pages all lie in the file.");

        byte type = page[bound.RowStart];
        var bitmapPages = new List<int>();
        if (type == Constants.UsageMap.ReferenceMapType)
        {
            for (int offset = bound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset; offset + 4 <= bound.RowStart + bound.RowSize; offset += 4)
            {
                int bitmapPage = Ri32(page, offset);
                if (bitmapPage != 0)
                {
                    bitmapPages.Add(bitmapPage);
                }
            }
        }

        int inlineStartPage = type == Constants.UsageMap.InlineMapType ? Ri32(page, bound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset) : 0;
        return new UsageMapRow(type, inlineStartPage, pages, bitmapPages);
    }

    private static async Task AssertBitmapPagesAsync(DatabaseFile db, UsageMapRow row)
    {
        Assert.NotEmpty(row.BitmapPages);
        foreach (int bitmapPage in row.BitmapPages)
        {
            byte[] page = await db.ReadPageCopyAsync(bitmapPage, Ct);
            Assert.Equal(BitmapPageHeader, page[..BitmapPageHeader.Length]);
        }
    }

    /// <summary>One usage-map row: its type, an INLINE row's start page, the pages it lists, and a REFERENCE row's bitmap pages in pointer order.</summary>
    /// <param name="Type">The row's type byte.</param>
    /// <param name="InlineStartPage">An INLINE row's start page, or 0.</param>
    /// <param name="Pages">The pages the row lists.</param>
    /// <param name="BitmapPages">The bitmap pages a REFERENCE row names.</param>
    private sealed record UsageMapRow(byte Type, int InlineStartPage, List<long> Pages, List<int> BitmapPages);

    /// <summary>A real index's tree pages and its usage-map row.</summary>
    /// <param name="Tree">The pages reachable from the index's root.</param>
    /// <param name="Row">The row its <c>used_pages</c> names.</param>
    private sealed record IndexMap(HashSet<long> Tree, UsageMapRow Row);
}
