namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Deleting overflow chains recovers space without changing surviving row identities or payloads.</summary>
public sealed class OverflowFreeSpaceTests
{
    private const string TableName = "Items";
    private const int ReplacementId = 10_000;

    public static TheoryData<DatabaseFormat, OverflowRowLayout> Layouts
    {
        get
        {
            var cases = new TheoryData<DatabaseFormat, OverflowRowLayout>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                cases.Add(format, OverflowRowLayout.CrossPage);
                cases.Add(format, OverflowRowLayout.SamePage);
                cases.Add(format, OverflowRowLayout.TwoHop);
            }

            return cases;
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task DeleteOverflow_ReclaimsEveryChainPageAndReusesSpaceAfterReopen(DatabaseFormat format, OverflowRowLayout layout)
    {
        byte[] image = await SyntheticOverflowRows.CreateTableAsync(format, TableName, 300, primaryKey: true, Ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(image, TableName, layout, Ct);
        if (layout != OverflowRowLayout.SamePage)
        {
            _ = await SyntheticOverflowRows.MoveRowAsync(image, TableName, OverflowRowLayout.SamePage, Ct);
        }

        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, Ct);
        Dictionary<int, string> expected;
        long tdefPage;
        long[] affected = [.. new[] { moved.HeaderPage, moved.DataPage }.Distinct().Order()];
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            CatalogEntry? table = await writer.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
            Assert.NotNull(table);
            tdefPage = table.TDefPage;
            List<LocatedRow> rows = await writer.Services.Snapshots.ReadRowsAsync(tdefPage, Ct);
            LocatedRow deleted = Assert.Single(rows, row => row.Location.PageNumber == moved.HeaderPage && row.Location.RowIndex == moved.HeaderRow);
            int deletedId = Assert.IsType<int>(deleted.Values[0]);
            expected = rows.ToDictionary(row => Assert.IsType<int>(row.Values[0]), row => Assert.IsType<string>(row.Values[1]));
            Assert.True(expected.Remove(deletedId));
            var beforePages = new Dictionary<long, byte[]>();
            var survivors = new Dictionary<(long Page, int Row), byte[]>();
            foreach (long pageNumber in affected)
            {
                byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);

                // The synthetic mover appends slots directly; normalize its
                // physical free-byte header before comparing native packing.
                Wu16(page, 2, FreeBytes(writer.Database.Format, page));
                await writer.Pager.WritePageAsync(pageNumber, page, Ct);
                beforePages.Add(pageNumber, page);
                foreach (RowBound row in DataPageRows.EnumerateLiveRowBounds(writer.Database.Format, page))
                {
                    survivors.Add((pageNumber, row.RowIndex), page.AsSpan(row.RowStart, row.RowSize).ToArray());
                }
            }

            await ClearFreeMapBitsAsync(writer, tdefPage, affected);
            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "Id", deletedId, Ct));
            List<long> free = await ReadFreeMapAsync(writer, tdefPage);
            foreach ((long pageNumber, byte[] before) in beforePages)
            {
                byte[] after = await writer.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);
                Assert.True(FreeBytes(writer.Database.Format, after) > FreeBytes(writer.Database.Format, before), $"Overflow deletion did not reclaim payload on page {pageNumber} ({layout}, {format}).");
                Assert.Equal(FreeBytes(writer.Database.Format, after), Ru16(after, 2));
                Assert.Equal(Ru16(before, writer.Database.Format.DataPage.NumRows), Ru16(after, writer.Database.Format.DataPage.NumRows));
                Assert.Contains(pageNumber, free);
            }

            foreach (((long pageNumber, int rowIndex), byte[] bytes) in survivors)
            {
                byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);
                Assert.True(DataPageRows.TryGetSlotBound(writer.Database.Format, page, rowIndex, out RowBound row));
                Assert.Equal(bytes, page.AsSpan(row.RowStart, row.RowSize).ToArray());
            }
        }

        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.InsertRowAsync(TableName, [ReplacementId, "replacement"], Ct);
            LocatedRow inserted = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(tdefPage, Ct), row => (int)row.Values[0] == ReplacementId);
            Assert.Contains(inserted.Location.PageNumber, affected);
        }

        expected.Add(ReplacementId, "replacement");
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        var actual = new Dictionary<int, string>();
        await foreach (object[] row in reader.Rows(TableName, cancellationToken: Ct))
        {
            Assert.True(actual.TryAdd(Assert.IsType<int>(row[0]), Assert.IsType<string>(row[1])));
        }

        Assert.Equal(expected.OrderBy(row => row.Key), actual.OrderBy(row => row.Key));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task DeleteOverflow_RollbackRestoresPayloadsSlotsAndFreeMaps(DatabaseFormat format, OverflowRowLayout layout)
    {
        byte[] image = await SyntheticOverflowRows.CreateTableAsync(format, TableName, 300, primaryKey: true, Ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(image, TableName, layout, Ct);
        if (layout != OverflowRowLayout.SamePage)
        {
            _ = await SyntheticOverflowRows.MoveRowAsync(image, TableName, OverflowRowLayout.SamePage, Ct);
        }

        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, Ct);
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        CatalogEntry? table = await writer.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(table);
        LocatedRow row = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.TDefPage, Ct), candidate => candidate.Location.PageNumber == moved.HeaderPage && candidate.Location.RowIndex == moved.HeaderRow);
        await writer.Pager.FlushAsync(toDisk: false, Ct);
        byte[] before = stream.ToArray();
        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "Id", row.Values[0], Ct));
        await transaction.RollbackAsync(Ct);
        await writer.Pager.FlushAsync(toDisk: false, Ct);
        Assert.Equal(before, stream.ToArray());
    }

    private static int FreeBytes(JetFormat format, byte[] page)
    {
        int rows = Ru16(page, format.DataPage.NumRows);
        int first = format.PageSize;
        for (int row = 0; row < rows; row++)
        {
            int start = Ru16(page, format.DataPage.RowsStart + (row * 2)) & Constants.DataPage.RowOffsetMask;
            if (start > 0)
            {
                first = Math.Min(first, start);
            }
        }

        return first - format.DataPage.RowsStart - (rows * 2);
    }

    private static async Task ClearFreeMapBitsAsync(AccessWriter writer, long tdefPage, IReadOnlyList<long> pages)
    {
        JetFormat format = writer.Database.Format;
        byte[] tdef = (await writer.Database.TableDefs.ReadTDefBytesAsync(tdefPage, Ct))!;
        Assert.True(UsageMap.TryReadPointer(tdef, format.TDef.FreePages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
        Assert.Equal(Constants.UsageMap.InlineMapType, map[row.RowStart]);
        foreach (long page in pages)
        {
            Assert.True(UsageMap.TrySetInlinePageState(map, row.RowStart, row.RowSize, page, isMarked: false));
        }

        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
    }

    private static async Task<List<long>> ReadFreeMapAsync(AccessWriter writer, long tdefPage)
    {
        JetFormat format = writer.Database.Format;
        byte[] tdef = (await writer.Database.TableDefs.ReadTDefBytesAsync(tdefPage, Ct))!;
        Assert.True(UsageMap.TryReadPointer(tdef, format.TDef.FreePages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
        var pages = new List<long>();
        Assert.True(UsageMap.TryEnumerateInlinePages(map, row, format.PageSize, writer.Database.Pages.PageCount, 1, true, pages));
        return pages;
    }
}
