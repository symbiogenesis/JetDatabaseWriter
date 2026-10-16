namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Release preparation survives only writes that cannot change map ownership.</summary>
public sealed class LongValueReleaseScopeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task StableClearAndDedicatedRelease_ReusePreparedPointers(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, databaseFormat);
        (long root, LongValueUsageMap map, LongValueUsageMapMaintainer maintainer) = await CreateLongTableAsync(writer, "T");
        int preparations = 0;
        using var scope = new LongValueReleaseScope(writer.Database.Format, writer.Pager, async ct =>
        {
            preparations++;
            return await maintainer.PrepareTableAsync(root, ct);
        });
        IReadOnlyList<(UsageMapPointer Owned, UsageMapPointer Free)> pointers = await scope.GetMapsAsync(Ct);
        _ = await scope.GetMapsAsync(Ct);
        Assert.Equal(1, preparations);
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(map.Owned.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(page, writer.Database.Format.DataPage, writer.Database.Format.PageSize, map.Owned.RowIndex, out RowBound row));
        var members = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(page, row, writer.Database.Format.PageSize, writer.Pager.PageCount, 1, true, writer.Pager.ReadPageAsync, PageBuffers.Return, members, Ct));
        long released = members[0];
        await writer.Services.PageAllocator.DeallocatePageAsync(released, Ct);
        Assert.Same(pointers, await scope.GetMapsAsync(Ct));
        await maintainer.ClearPageAsync(pointers, released, Ct);
        Assert.Same(pointers, await scope.GetMapsAsync(Ct));
        Assert.Equal(1, preparations);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task AliasAddedBetweenGets_IsolatesBeforeClearAndPreservesAlias(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, databaseFormat);
        (long root, LongValueUsageMap map, LongValueUsageMapMaintainer maintainer) = await CreateLongTableAsync(writer, "T");
        (long otherRoot, LongValueUsageMap otherMap, _) = await CreateLongTableAsync(writer, "Other");
        using var scope = new LongValueReleaseScope(writer.Database.Format, writer.Pager, ct => maintainer.PrepareTableAsync(root, ct));
        UsageMapPointer original = Assert.Single(await scope.GetMapsAsync(Ct)).Owned;
        LogicalTDefChain other = await writer.Database.TableDefs.ReadTDefChainAsync(otherRoot, Ct);
        UsageMap.WritePointer(other.Bytes, otherMap.OwnedOffset, original.RowIndex, original.PageNumber);
        await other.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        byte[] aliasedBefore = await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(aliasedBefore, writer.Database.Format.DataPage, writer.Database.Format.PageSize, original.RowIndex, out RowBound originalRow));
        byte[] originalBytes = aliasedBefore.AsSpan(originalRow.RowStart, originalRow.RowSize).ToArray();
        List<long> members = await ReadMembersAsync(writer, original);
        IReadOnlyList<(UsageMapPointer Owned, UsageMapPointer Free)> refreshed = await scope.GetMapsAsync(Ct);
        Assert.NotEqual(original, Assert.Single(refreshed).Owned);
        await maintainer.ClearPageAsync(refreshed, members[0], Ct);
        byte[] aliasedAfter = await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
        Assert.Equal(originalBytes, aliasedAfter.AsSpan(originalRow.RowStart, originalRow.RowSize).ToArray());
        Assert.DoesNotContain(members[0], await ReadMembersAsync(writer, Assert.Single(refreshed).Owned));
        Assert.Equal(map.ColumnNumber, otherMap.ColumnNumber);
    }

    [Theory]
    [InlineData("tdef")]
    [InlineData("tdef-created")]
    [InlineData("map-created")]
    [InlineData("map-deleted")]
    [InlineData("map-row-count")]
    [InlineData("map-directory")]
    [InlineData("map-type")]
    [InlineData("inline-base")]
    [InlineData("reference-pointer")]
    [InlineData("bitmap-header")]
    [InlineData("bitmap-tag")]
    [InlineData("catalog-data")]
    public async Task TopologyMutationThroughPager_DiscardsPreparedPointers(string mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, DatabaseFormat.Jet3Mdb);
        JetFormat format = writer.Database.Format;
        byte[] page = CreateMutationPage(format, mutation);
        long number = await writer.Pager.AppendPageAsync(page, Ct);
        int preparations = 0;
        using var scope = new LongValueReleaseScope(format, writer.Pager, _ =>
        {
            preparations++;
            return new ValueTask<List<(UsageMapPointer Owned, UsageMapPointer Free)>>([]);
        });
        _ = await scope.GetMapsAsync(Ct);
        if (mutation is "map-created" or "tdef-created")
        {
            page = mutation == "map-created" ? NewMapPage(format) : new byte[format.PageSize];
            page[0] = mutation == "map-created" ? Constants.PageTypes.Data : Constants.PageTypes.TableDefinition;
        }
        else if (mutation is "map-deleted" or "bitmap-tag")
        {
            page[0] = Constants.PageTypes.Freed;
        }
        else if (mutation == "map-row-count")
        {
            Wu16(page, format.DataPage.NumRows, 0);
        }
        else if (mutation == "map-directory")
        {
            Wu16(page, format.DataPage.RowsStart, format.PageSize - format.UsageMapRowSize - 1);
        }
        else if (mutation == "map-type")
        {
            page[format.PageSize - format.UsageMapRowSize] = Constants.UsageMap.ReferenceMapType;
        }
        else if (mutation is "inline-base" or "reference-pointer")
        {
            Wi32(page, format.PageSize - format.UsageMapRowSize + 1, 8);
        }
        else
        {
            page[mutation == "bitmap-header" ? 1 : 12] ^= 1;
        }

        await writer.Pager.WritePageAsync(number, page, Ct);
        _ = await scope.GetMapsAsync(Ct);
        Assert.Equal(2, preparations);
    }

    [Fact]
    public async Task GlobalPromotionAndInvalidation_RequireFreshPreparation()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, DatabaseFormat.Jet3Mdb);

        // Native files can have allocated pages beyond the inline global
        // window. Releasing one must promote that map before a column clear.
        while (writer.Pager.PageCount <= 600)
        {
            _ = await writer.Pager.ReserveZeroedPageAsync(Ct);
        }

        long farPage = writer.Pager.PageCount - 1;
        int preparations = 0;
        using var scope = new LongValueReleaseScope(writer.Database.Format, writer.Pager, _ =>
        {
            preparations++;
            return new ValueTask<List<(UsageMapPointer Owned, UsageMapPointer Free)>>([]);
        });
        _ = await scope.GetMapsAsync(Ct);
        await writer.Services.PageAllocator.DeallocatePageAsync(farPage, Ct);
        _ = await scope.GetMapsAsync(Ct);
        Assert.Equal(2, preparations);
        scope.OnInvalidateAll();
        _ = await scope.GetMapsAsync(Ct);
        Assert.Equal(3, preparations);
    }

    [Theory]
    [InlineData("bitmap-bits")]
    [InlineData("inline-bits")]
    [InlineData("unchanged")]
    public async Task MembershipAndIdenticalWrites_RetainPreparedPointers(string mutation)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, DatabaseFormat.Jet3Mdb);
        JetFormat format = writer.Database.Format;
        byte[] page = NewMapPage(format);
        if (mutation == "bitmap-bits")
        {
            page[0] = Constants.PageTypes.UsageMap;
        }

        long number = await writer.Pager.AppendPageAsync(page, Ct);
        int preparations = 0;
        using var scope = new LongValueReleaseScope(format, writer.Pager, _ =>
        {
            preparations++;
            return new ValueTask<List<(UsageMapPointer Owned, UsageMapPointer Free)>>([]);
        });
        _ = await scope.GetMapsAsync(Ct);
        if (mutation != "unchanged")
        {
            page[mutation == "bitmap-bits" ? 12 : format.PageSize - format.UsageMapRowSize + 8] ^= 1;
        }

        await writer.Pager.WritePageAsync(number, page, Ct);
        _ = await scope.GetMapsAsync(Ct);
        Assert.Equal(1, preparations);
    }

    [Fact]
    public async Task PreparationWritesTopology_ResultIsImmediateButNotCached()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, DatabaseFormat.Jet3Mdb);
        byte[] page = new byte[writer.Database.Format.PageSize];
        page[0] = Constants.PageTypes.TableDefinition;
        long number = await writer.Pager.AppendPageAsync(page, Ct);
        int preparations = 0;
        using var scope = new LongValueReleaseScope(writer.Database.Format, writer.Pager, async ct =>
        {
            preparations++;
            if (preparations == 1)
            {
                page[12] ^= 1;
                await writer.Pager.WritePageAsync(number, page, ct);
            }

            return [];
        });
        _ = await scope.GetMapsAsync(Ct);
        Assert.Equal(1, preparations);
        _ = await scope.GetMapsAsync(Ct);
        _ = await scope.GetMapsAsync(Ct);
        Assert.Equal(2, preparations);
    }

    [Fact]
    public async Task CanceledOrFailedPreparation_DisposesObserverAndNeverCaches()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, DatabaseFormat.Jet3Mdb);
        FieldInfo observers = Assert.IsType<FieldInfo>(typeof(Pager).GetField("observers", BindingFlags.Instance | BindingFlags.NonPublic), exactMatch: false);
        int before = Assert.IsType<IPageWriteObserver[]>(observers.GetValue(writer.Pager)).Length;
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        using (var scope = new LongValueReleaseScope(writer.Database.Format, writer.Pager, _ => throw new IOException("Injected preparation failure.")))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => scope.GetMapsAsync(canceled.Token).AsTask());
            await Assert.ThrowsAsync<IOException>(() => scope.GetMapsAsync(Ct).AsTask());
        }

        Assert.Equal(before, Assert.IsType<IPageWriteObserver[]>(observers.GetValue(writer.Pager)).Length);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BulkRelease_GuardsNoOpsAndPreparationFailuresBeforeFreeing(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, databaseFormat);
        (long root, LongValueUsageMap map, LongValueUsageMapMaintainer maintainer) = await CreateLongTableAsync(writer, "T");
        var encoder = new LongValueEncoder(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, new AccessWriterOptions { UseLockFile = false }, maintainer);
        List<LongValueDescriptor> roots = await ReadRootsAsync(writer, encoder, root);
        Assert.True(Assert.Single(roots).UsesChainedPages);
        List<long> members = await ReadMembersAsync(writer, map.Owned);
        byte[] before = stream.ToArray();
        await encoder.DeallocateLongValuesAsync([], Ct, root);
        await encoder.DeallocateLongValuesAsync([LongValueDescriptor.Inline(20), LongValueDescriptor.SinglePage(20, 0, 0)], Ct, root);
        Assert.Equal(before, stream.ToArray());
        FieldInfo observers = Assert.IsType<FieldInfo>(typeof(Pager).GetField("observers", BindingFlags.Instance | BindingFlags.NonPublic), exactMatch: false);
        int observerCount = Assert.IsType<IPageWriteObserver[]>(observers.GetValue(writer.Pager)).Length;
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => encoder.DeallocateLongValuesAsync(roots, canceled.Token, root).AsTask());
        byte[] mapPage = await writer.Database.Pages.ReadPageCopyAsync(map.Owned.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(mapPage, writer.Database.Format.DataPage, writer.Database.Format.PageSize, map.Owned.RowIndex, out RowBound bound));
        mapPage[bound.RowStart] = 0x7F;
        await writer.Pager.WritePageAsync(map.Owned.PageNumber, mapPage, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(() => encoder.DeallocateLongValuesAsync(roots, Ct, root).AsTask());
        Assert.Equal(observerCount, Assert.IsType<IPageWriteObserver[]>(observers.GetValue(writer.Pager)).Length);
        foreach (long page in members)
        {
            Assert.True(LongValueStore.IsLvalPage(await writer.Database.Pages.ReadPageCopyAsync(page, Ct)));
            Assert.False(await writer.Services.PageAllocator.IsPageFreeAsync(page, Ct));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BulkRelease_ChainsClearEveryOwnedPageAndUnregisterObserver(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream, databaseFormat);
        (long root, LongValueUsageMap map, LongValueUsageMapMaintainer maintainer) = await CreateLongTableAsync(writer, "T");
        await writer.InsertRowAsync("T", [2, new byte[8_200]], Ct);
        var encoder = new LongValueEncoder(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, new AccessWriterOptions { UseLockFile = false }, maintainer);
        List<LongValueDescriptor> roots = await ReadRootsAsync(writer, encoder, root);
        Assert.Equal(2, roots.Count);
        List<long> members = await ReadMembersAsync(writer, map.Owned);
        FieldInfo observers = Assert.IsType<FieldInfo>(typeof(Pager).GetField("observers", BindingFlags.Instance | BindingFlags.NonPublic), exactMatch: false);
        int observerCount = Assert.IsType<IPageWriteObserver[]>(observers.GetValue(writer.Pager)).Length;
        await encoder.DeallocateLongValuesAsync(roots, Ct, root);
        Assert.Equal(observerCount, Assert.IsType<IPageWriteObserver[]>(observers.GetValue(writer.Pager)).Length);
        foreach (long page in members)
        {
            Assert.True(await writer.Services.PageAllocator.IsPageFreeAsync(page, Ct));
        }

        Assert.Empty(await ReadMembersAsync(writer, map.Owned));
        Assert.Empty(await ReadMembersAsync(writer, map.Free));
    }

    private static async Task<List<LongValueDescriptor>> ReadRootsAsync(AccessWriter writer, LongValueEncoder encoder, long root)
    {
        TableDef definition = Assert.IsType<TableDef>(await writer.Database.TableDefs.ReadTableDefAsync(root, Ct));
        var roots = new List<LongValueDescriptor>();
        await writer.Database.OwnedPages.ForEachLiveTableRowAsync(
            root,
            (row, _) =>
            {
                roots.AddRange(encoder.CollectLongValueRoots(row.Page, new RowBound(row.Location.RowIndex, row.Location.RowStart, row.Location.RowSize), definition));
                return new ValueTask<bool>(true);
            },
            Ct);
        return roots;
    }

    private static async Task<List<long>> ReadMembersAsync(AccessWriter writer, UsageMapPointer pointer)
    {
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(page, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound row));
        var members = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(page, row, writer.Database.Format.PageSize, writer.Pager.PageCount, 1, true, writer.Pager.ReadPageAsync, PageBuffers.Return, members, Ct));
        return members;
    }

    private static byte[] CreateMutationPage(JetFormat format, string mutation)
    {
        byte[] page = mutation.StartsWith("map-", StringComparison.Ordinal) || mutation is "inline-base" or "reference-pointer" ? NewMapPage(format) : new byte[format.PageSize];
        if (mutation is "tdef-created" or "map-created")
        {
            page[0] = Constants.PageTypes.Freed;
        }
        else if (mutation == "tdef")
        {
            page[0] = Constants.PageTypes.TableDefinition;
        }
        else if (mutation is "bitmap-header" or "bitmap-tag")
        {
            page[0] = Constants.PageTypes.UsageMap;
            page[1] = 1;
        }
        else if (mutation == "catalog-data")
        {
            page[0] = Constants.PageTypes.Data;
            Wi32(page, format.DataPage.TDefOff, 2);
        }
        else if (mutation == "reference-pointer")
        {
            page[format.PageSize - format.UsageMapRowSize] = Constants.UsageMap.ReferenceMapType;
        }

        return page;
    }

    private static byte[] NewMapPage(JetFormat format)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        page[1] = 1;
        Wu16(page, format.DataPage.NumRows, 1);
        Wu16(page, format.DataPage.RowsStart, format.PageSize - format.UsageMapRowSize);
        return page;
    }

    private static ValueTask<AccessWriter> CreateWriterAsync(MemoryStream stream, DatabaseFormat format)
        => AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);

    private static async ValueTask<(long Root, LongValueUsageMap Map, LongValueUsageMapMaintainer Maintainer)> CreateLongTableAsync(AccessWriter writer, string name)
    {
        await writer.CreateTableAsync(name, [new("Id", typeof(int)), new("Blob", typeof(byte[]))], Ct);
        await writer.InsertRowAsync(name, [1, new byte[8_100]], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync(name, Ct)).TDefPage;
        byte[] bytes = Assert.IsType<byte[]>(await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct));
        Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, bytes, out List<LongValueUsageMap> maps, out _));
        var maintainer = new LongValueUsageMapMaintainer(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Services.Catalog);
        return (root, Assert.Single(maps), maintainer);
    }
}
