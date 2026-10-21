namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Release preparation leaves unused column maps intact while validating their storage.</summary>
public sealed class LongValueReleasePreparationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "row")]
    [InlineData(DatabaseFormat.Jet3Mdb, "bitmap")]
    [InlineData(DatabaseFormat.Jet3Mdb, "zero")]
    [InlineData(DatabaseFormat.Jet4Mdb, "row")]
    [InlineData(DatabaseFormat.Jet4Mdb, "bitmap")]
    [InlineData(DatabaseFormat.Jet4Mdb, "zero")]
    [InlineData(DatabaseFormat.AceAccdb, "row")]
    [InlineData(DatabaseFormat.AceAccdb, "bitmap")]
    [InlineData(DatabaseFormat.AceAccdb, "zero")]
    public async Task DeletingAnotherColumn_PreservesUnusedSharedOrZeroMapPointers(DatabaseFormat kind, string sharing)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Unused1", typeof(byte[])), new("Unused2", typeof(byte[])), new("Active", typeof(byte[]))], Ct);
        await writer.InsertRowAsync("T", [1, DBNull.Value, DBNull.Value, new byte[9_000]], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct)).TDefPage;
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
        List<LongValueUsageMap> maps = ReadMaps(writer, chain.Bytes);
        LongValueUsageMap first = maps.Single(map => map.ColumnNumber == 1);
        LongValueUsageMap second = maps.Single(map => map.ColumnNumber == 2);
        if (sharing == "bitmap")
        {
            long bitmap = await AllocateEmptyBitmapAsync(writer);
            foreach (UsageMapPointer pointer in new[] { first.Owned, first.Free, second.Owned, second.Free })
            {
                await SetReferenceAsync(writer, pointer, bitmap);
            }
        }
        else
        {
            UsageMapPointer owned = sharing == "zero" ? default : second.Owned;
            UsageMapPointer free = sharing == "zero" ? default : second.Free;
            for (int index = 0; index < (sharing == "zero" ? 2 : 1); index++)
            {
                LongValueUsageMap map = index == 0 ? first : second;
                UsageMap.WritePointer(chain.Bytes, map.OwnedOffset, owned.RowIndex, owned.PageNumber);
                UsageMap.WritePointer(chain.Bytes, map.FreeOffset, free.RowIndex, free.PageNumber);
            }

            await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        }

        List<LongValueUsageMap> before = ReadMaps(writer, (await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct))!);
        Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, Ct));
        List<LongValueUsageMap> after = ReadMaps(writer, (await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct))!);
        foreach (int column in new[] { 1, 2 })
        {
            Assert.Equal(before.Single(map => map.ColumnNumber == column), after.Single(map => map.ColumnNumber == column));
        }

        await writer.InsertRowAsync("T", [2, new byte[500], DBNull.Value, DBNull.Value], Ct);
        List<LongValueUsageMap> allocated = ReadMaps(writer, (await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct))!);
        Assert.NotEqual(default, allocated.Single(map => map.ColumnNumber == 1).Owned);
        Assert.Equal(before.Single(map => map.ColumnNumber == 2), allocated.Single(map => map.ColumnNumber == 2));
        Assert.Single(await MembersAsync(writer, allocated.Single(map => map.ColumnNumber == 1).Owned));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EmptyColumnMaps_SharingGlobalBitmap_AreDetachedBeforeFreeingPages(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Unused", typeof(byte[])), new("Active", typeof(byte[]))], Ct);
        await writer.InsertRowAsync("T", [1, DBNull.Value, new byte[9_000]], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct)).TDefPage;
        List<LongValueUsageMap> maps = ReadMaps(writer, (await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct))!);
        LongValueUsageMap unused = maps.Single(map => map.ColumnNumber == 1);
        List<long> active = await MembersAsync(writer, maps.Single(map => map.ColumnNumber == 2).Owned);
        long bitmap = await AllocateEmptyBitmapAsync(writer);
        await SetReferenceAsync(writer, unused.Owned, bitmap);
        await SetReferenceAsync(writer, unused.Free, bitmap);
        byte[] global = await writer.Database.Pages.ReadPageCopyAsync(1, Ct);
        Assert.True(UsageMap.TryGetFirstRowBound(global, writer.Database.Format.DataPage, writer.Database.Format.PageSize, out RowBound row));
        Array.Clear(global, row.RowStart, row.RowSize);
        global[row.RowStart] = Constants.UsageMap.ReferenceMapType;
        Wi32(global, row.RowStart + 1, checked((int)bitmap));
        await writer.Pager.WritePageAsync(1, global, Ct);

        Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, Ct));
        LongValueUsageMap after = ReadMaps(writer, (await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct))!).Single(map => map.ColumnNumber == 1);
        Assert.NotEqual(unused.Owned, after.Owned);
        Assert.NotEqual(unused.Free, after.Free);
        Assert.Empty(await MembersAsync(writer, after.Owned));
        Assert.Empty(await MembersAsync(writer, after.Free));
        foreach (long page in active)
        {
            Assert.True(await writer.Services.PageAllocator.IsPageFreeAsync(page, Ct));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ActivePair_WithOneZeroPointer_IsPreparedAndCleared(bool zeroOwned)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Blob", typeof(byte[]))], Ct);
        await writer.InsertRowAsync("T", [1, new byte[500]], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct)).TDefPage;
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
        LongValueUsageMap map = Assert.Single(ReadMaps(writer, chain.Bytes));
        long member = Assert.Single(await MembersAsync(writer, map.Owned));
        UsageMap.WritePointer(chain.Bytes, zeroOwned ? map.OwnedOffset : map.FreeOffset, 0, 0);
        await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        LongValueUsageMapMaintainer maintainer = CreateMaintainer(writer);
        (UsageMapPointer owned, UsageMapPointer free) = Assert.Single(await maintainer.PrepareTableAsync(root, Ct));
        Assert.NotEqual(default, owned);
        Assert.NotEqual(default, free);
        await maintainer.ClearPageAsync([(owned, free)], member, Ct);
        Assert.Empty(await MembersAsync(writer, owned));
        Assert.Empty(await MembersAsync(writer, free));
    }

    [Theory]
    [InlineData("pointer")]
    [InlineData("owner")]
    [InlineData("page-type")]
    [InlineData("row")]
    [InlineData("map-type")]
    [InlineData("inline-base")]
    [InlineData("bitmap-pointer")]
    [InlineData("bitmap-header")]
    [InlineData("page-zero")]
    [InlineData("past-end")]
    public async Task UnusedMaps_MalformedStorageStillRefusesPreparation(string corruption)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Blob", typeof(byte[]))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct)).TDefPage;
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
        LongValueUsageMap entry = Assert.Single(ReadMaps(writer, chain.Bytes));
        if (corruption == "pointer")
        {
            UsageMap.WritePointer(chain.Bytes, entry.OwnedOffset, 0, 1);
            await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        }
        else
        {
            byte[] map = await writer.Database.Pages.ReadPageCopyAsync(entry.Owned.PageNumber, Ct);
            Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, entry.Owned.RowIndex, out RowBound row));
            switch (corruption)
            {
                case "owner":
                    Wi32(map, writer.Database.Format.DataPage.TDefOff, checked((int)root));
                    break;
                case "page-type":
                    map[0] = Constants.PageTypes.Freed;
                    break;
                case "row":
                    Wu16(map, writer.Database.Format.DataPage.RowsStart + (entry.Owned.RowIndex * 2), writer.Database.Format.PageSize);
                    break;
                case "map-type":
                    map[row.RowStart] = 0x77;
                    break;
                case "inline-base":
                    Wi32(map, row.RowStart + 1, -1);
                    break;
                case "page-zero":
                    Assert.True(UsageMap.TrySetInlinePageState(map, row.RowStart, row.RowSize, 0, isMarked: true));
                    break;
                case "past-end":
                    Assert.True(UsageMap.TrySetInlinePageState(map, row.RowStart, row.RowSize, writer.Pager.PageCount, isMarked: true));
                    break;
                default:
                    map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
                    Wi32(map, row.RowStart + 1, checked((int)(corruption == "bitmap-pointer" ? writer.Pager.PageCount : root)));
                    break;
            }

            await writer.Pager.WritePageAsync(entry.Owned.PageNumber, map, Ct);
        }

        byte[] before = stream.ToArray();
        await Assert.ThrowsAnyAsync<IOException>(() => CreateMaintainer(writer).PrepareTableAsync(root, Ct).AsTask());
        Assert.Equal(before, stream.ToArray());
    }

    private static LongValueUsageMapMaintainer CreateMaintainer(AccessWriter writer)
        => new(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Services.Catalog);

    private static List<LongValueUsageMap> ReadMaps(AccessWriter writer, byte[] definition)
    {
        Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, definition, out List<LongValueUsageMap> maps, out _));
        return maps;
    }

    private static async Task<long> AllocateEmptyBitmapAsync(AccessWriter writer)
    {
        byte[] bitmap = new byte[writer.Database.Format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[1] = 1;
        return await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
    }

    private static async Task SetReferenceAsync(AccessWriter writer, UsageMapPointer pointer, long bitmap)
    {
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(page, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound row));
        Array.Clear(page, row.RowStart, row.RowSize);
        page[row.RowStart] = Constants.UsageMap.ReferenceMapType;
        Wi32(page, row.RowStart + 1, checked((int)bitmap));
        await writer.Pager.WritePageAsync(pointer.PageNumber, page, Ct);
    }

    private static async Task<List<long>> MembersAsync(AccessWriter writer, UsageMapPointer pointer)
    {
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(page, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound row));
        var members = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(page, row, writer.Database.Format.PageSize, writer.Pager.PageCount, 1, true, writer.Pager.ReadPageAsync, PageBuffers.Return, members, Ct));
        return members;
    }
}
