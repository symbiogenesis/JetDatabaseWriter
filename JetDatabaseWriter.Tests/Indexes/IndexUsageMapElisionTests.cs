namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Refreshing index membership edits only maps whose validated membership changes.</summary>
public sealed class IndexUsageMapElisionTests
{
    public static TheoryData<DatabaseFormat, bool> FormatsAndPaths =>
    [
        (DatabaseFormat.Jet3Mdb, false),
        (DatabaseFormat.Jet3Mdb, true),
        (DatabaseFormat.Jet4Mdb, false),
        (DatabaseFormat.Jet4Mdb, true),
        (DatabaseFormat.AceAccdb, false),
        (DatabaseFormat.AceAccdb, true),
    ];

    public static TheoryData<DatabaseFormat, bool, bool> FormatsPathsAndMaps
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, bool, bool>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (bool rebuild in new[] { false, true })
                {
                    data.Add(format, rebuild, false);
                    data.Add(format, rebuild, true);
                }
            }

            return data;
        }
    }

    public static TheoryData<DatabaseFormat, bool, string> MalformedMaps
    {
        get
        {
            var cases = new TheoryData<DatabaseFormat, bool, string>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (bool rebuild in new[] { false, true })
                {
                    foreach (string corruption in new[] { "owner", "signature", "flags", "type", "bound", "missing-member" })
                    {
                        cases.Add(format, rebuild, corruption);
                    }
                }
            }

            return cases;
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(FormatsPathsAndMaps))]
    public async Task UnchangedMembership_KeepsSharedMapStorageAndPointer(DatabaseFormat format, bool rebuild, bool referenceMap)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, format);
        await writer.InsertRowAsync("T", [1], Ct);
        if (rebuild)
        {
            await writer.InsertRowAsync("T", [2], Ct);
        }

        CatalogEntry table = await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct);
        UsageMapPointer original = await IsolateMapAsync(writer, table.TDefPage);
        long bitmapPage = 0;
        byte[]? originalBitmap = null;
        if (referenceMap)
        {
            (_, List<long> originalMembers) = await ReadMapAsync(writer, table.TDefPage);
            byte[] bitmap = new byte[writer.Database.Format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            foreach (long page in originalMembers)
            {
                Assert.True(UsageMap.TrySetReferencePageState(bitmap, writer.Database.Format.PageSize, page, isMarked: true));
            }

            bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
            byte[] map = await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
            Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, original.RowIndex, out RowBound row));
            map.AsSpan(row.RowStart, row.RowSize).Clear();
            map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
            Wi32(map, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset, checked((int)bitmapPage));
            await writer.Pager.WritePageAsync(original.PageNumber, map, Ct);
            originalBitmap = bitmap;
        }

        await AliasMapAsync(writer, original);
        byte[] originalMap = await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
        if (rebuild)
        {
            await RebuildAsync(writer, table.TDefPage);
        }
        else
        {
            await writer.InsertRowAsync("T", [2], Ct);
        }

        (UsageMapPointer pointer, List<long> members) = await ReadMapAsync(writer, table.TDefPage);
        Assert.Equal(original, pointer);
        Assert.Equal(originalMap, await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct));
        if (originalBitmap is not null)
        {
            Assert.Equal(originalBitmap, await writer.Database.Pages.ReadPageCopyAsync(bitmapPage, Ct));
        }

        Assert.Single(members);
        long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
        await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, table.TDefPage, root, Ct);
        Assert.Equal(2, (await writer.Services.Snapshots.ReadRowsAsync(table.TDefPage, Ct)).Count);
    }

    [Theory]
    [MemberData(nameof(FormatsAndPaths))]
    public async Task ChangedTreeMembership_DetachesAndPreservesTheSurvivingAlias(DatabaseFormat format, bool rebuild)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, format);
        await writer.InsertRowsAsync("T", Enumerable.Range(0, rebuild ? 4000 : 1).Select(id => new object?[] { id }), Ct);
        CatalogEntry table = await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct);
        UsageMapPointer original = await IsolateMapAsync(writer, table.TDefPage);
        await AliasMapAsync(writer, original);
        byte[] originalMap = await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
        (_, List<long> originalMembers) = await ReadMapAsync(writer, table.TDefPage);
        if (rebuild)
        {
            await RebuildAsync(writer, table.TDefPage);
        }
        else
        {
            await writer.InsertRowsAsync("T", Enumerable.Range(1, 3999).Select(id => new object?[] { id }), Ct);
        }

        (UsageMapPointer pointer, List<long> members) = await ReadMapAsync(writer, table.TDefPage);
        Assert.NotEqual(original, pointer);
        Assert.Equal(originalMap, await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct));
        foreach (long page in originalMembers)
        {
            Assert.False(await writer.Services.PageAllocator.IsPageFreeAsync(page, Ct));
        }

        long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
        Assert.Equal((await IndexLeafChain.ReadTreePagesAsync(writer.Database, table.TDefPage, root, Ct)).Order(), members.Order());
        await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, table.TDefPage, root, Ct);
    }

    [Theory]
    [MemberData(nameof(MalformedMaps))]
    public async Task InvalidOrStaleMap_UsesTheExistingPrivateRefreshPath(DatabaseFormat format, bool rebuild, string corruption)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, format);
        await writer.InsertRowAsync("T", [1], Ct);
        CatalogEntry table = await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct);
        UsageMapPointer original = await IsolateMapAsync(writer, table.TDefPage);
        await AliasMapAsync(writer, original);
        byte[] originalMap = await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(originalMap, writer.Database.Format.DataPage, writer.Database.Format.PageSize, original.RowIndex, out RowBound row));
        switch (corruption)
        {
            case "owner":
                Wi32(originalMap, writer.Database.Format.DataPage.TDefOff, checked((int)table.TDefPage));
                break;
            case "signature":
                originalMap[1] = 0;
                break;
            case "flags":
                Wu16(originalMap, writer.Database.Format.DataPage.RowsStart + (original.RowIndex * 2), row.RowStart | 0x2000);
                break;
            case "type":
                originalMap[row.RowStart] = byte.MaxValue;
                break;
            case "bound":
                Wu16(originalMap, writer.Database.Format.DataPage.RowsStart + (original.RowIndex * 2), 0);
                break;
            case "missing-member":
                Assert.True(UsageMap.TryWriteInlineRow(originalMap, row.RowStart, row.RowSize, []));
                break;
            default:
                Assert.Fail("Unexpected corruption case.");
                break;
        }

        await writer.Pager.WritePageAsync(original.PageNumber, originalMap, Ct);
        if (rebuild)
        {
            await RebuildAsync(writer, table.TDefPage);
        }
        else
        {
            await writer.InsertRowAsync("T", [2], Ct);
        }

        (UsageMapPointer pointer, List<long> members) = await ReadMapAsync(writer, table.TDefPage);
        Assert.NotEqual(original, pointer);
        Assert.Equal(originalMap, await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct));
        long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
        Assert.Equal(root, Assert.Single(members));
        await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, table.TDefPage, root, Ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MixedRefresh_LeavesTheUnchangedMapAndDetachesOnlyTheGrowingIndex(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Name", typeof(string), maxLength: 100)], [new IndexDefinition("PK", "Id") { IsPrimaryKey = true }, new IndexDefinition("ByName", "Name")], Ct);
        await writer.CreateTableAsync("Alias", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Alias2", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.InsertRowsAsync("T", Enumerable.Range(0, 5).Select(Row), Ct);
        CatalogEntry table = await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct);
        UsageMapPointer primaryMap = await IsolateMapAsync(writer, table.TDefPage, 0);
        UsageMapPointer growingMap = await IsolateMapAsync(writer, table.TDefPage, 1);
        await AliasMapAsync(writer, primaryMap);
        await AliasMapAsync(writer, growingMap, "Alias2");
        byte[] primaryBytes = await writer.Database.Pages.ReadPageCopyAsync(primaryMap.PageNumber, Ct);
        byte[] growingBytes = await writer.Database.Pages.ReadPageCopyAsync(growingMap.PageNumber, Ct);
        await writer.InsertRowsAsync("T", Enumerable.Range(5, 75).Select(Row), Ct);
        (UsageMapPointer primaryAfter, List<long> primaryMembers) = await ReadMapAsync(writer, table.TDefPage, 0);
        (UsageMapPointer growingAfter, List<long> growingMembers) = await ReadMapAsync(writer, table.TDefPage, 1);
        Assert.Equal(primaryMap, primaryAfter);
        Assert.NotEqual(growingMap, growingAfter);
        Assert.Single(primaryMembers);
        Assert.True(growingMembers.Count > 1);
        Assert.Equal(primaryBytes, await writer.Database.Pages.ReadPageCopyAsync(primaryMap.PageNumber, Ct));
        Assert.Equal(growingBytes, await writer.Database.Pages.ReadPageCopyAsync(growingMap.PageNumber, Ct));
        foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct))
        {
            await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, table.TDefPage, root, Ct);
        }

        static object?[] Row(int id) => [id, FormattableString.Invariant($"{new string((char)('A' + (id % 26)), 90)}{id:D4}")];
    }

    private static async ValueTask<AccessWriter> CreateAsync(MemoryStream stream, DatabaseFormat format)
    {
        AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        try
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("Alias", [new ColumnDefinition("Id", typeof(int))], Ct);
            return writer;
        }
        catch
        {
            await writer.DisposeAsync();
            throw;
        }
    }

    private static async ValueTask RebuildAsync(AccessWriter writer, long tdefPage)
    {
        TableDef definition = await writer.Database.TableDefs.ReadRequiredTableDefAsync(tdefPage, "T", Ct);
        await writer.Services.Indexes.MaintainIndexesAsync(tdefPage, definition, "T", Ct);
    }

    private static async ValueTask<UsageMapPointer> IsolateMapAsync(AccessWriter writer, long tdefPage, int index = 0)
    {
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(tdefPage, Ct);
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(tdefPage, Ct));
        int offset = image.RealIndexes[index].Slot.FirstDpOffset - 4;
        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, tdefPage, offset, Ct);
        UsageMapPointer pointer = await UsageMapOwnership.CreatePrivateCopyAsync(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, proof, Ct);
        UsageMap.WritePointer(chain.Bytes, offset, pointer.RowIndex, pointer.PageNumber);
        await writer.Services.TDefWriter.WriteChainInPlaceAsync(chain, Ct);
        return pointer;
    }

    private static async ValueTask AliasMapAsync(AccessWriter writer, UsageMapPointer pointer, string tableName = "Alias")
    {
        CatalogEntry alias = await writer.Services.Catalog.GetRequiredCatalogEntryAsync(tableName, Ct);
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(alias.TDefPage, Ct);
        UsageMap.WritePointer(chain.Bytes, writer.Database.Format.TDef.FreePages, pointer.RowIndex, pointer.PageNumber);
        await writer.Services.TDefWriter.WriteChainInPlaceAsync(chain, Ct);
    }

    private static async ValueTask<(UsageMapPointer Pointer, List<long> Members)> ReadMapAsync(AccessWriter writer, long tdefPage, int index = 0)
    {
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(tdefPage, Ct));
        Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), image.RealIndexes[index].Slot.FirstDpOffset - 4, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound row));
        var members = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, row, writer.Database.Format.PageSize, writer.Pager.PageCount, 1, true, writer.Pager.ReadPageAsync, PageBuffers.Return, members, Ct));
        return (pointer, members);
    }
}
