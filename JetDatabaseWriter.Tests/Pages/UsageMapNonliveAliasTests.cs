namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Detached maps retain storage still named through opaque nonlive directory aliases.</summary>
public sealed class UsageMapNonliveAliasTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, true)]
    public async Task DetachedOwnedMap_NonliveOpaqueAliasStillProtectsOldStorage(DatabaseFormat databaseFormat, bool referenceMap, bool onlyNonlivePayload)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        JetFormat format = writer.Database.Format;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer pointer));
        UsageMapOwnership.Proof original = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        long physical = Assert.Single(original.Pages);
        long member = await writer.Services.PageAllocator.AllocatePageAsync(new byte[format.PageSize], Ct);
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound owned));
        long bitmapPage = 0;
        if (referenceMap)
        {
            byte[] bitmap = new byte[format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            Assert.True(UsageMap.TrySetReferencePageState(bitmap, format.PageSize, physical, isMarked: true));
            Assert.True(UsageMap.TrySetReferencePageState(bitmap, format.PageSize, member, isMarked: true));
            bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
            Array.Clear(map, owned.RowStart, owned.RowSize);
            map[owned.RowStart] = Constants.UsageMap.ReferenceMapType;
            Wi32(map, owned.RowStart + 1, checked((int)bitmapPage));
        }
        else
        {
            Assert.True(UsageMap.TrySetInlinePageState(map, owned.RowStart, owned.RowSize, member, isMarked: true));
        }

        Wu16(map, format.DataPage.NumRows, 3);
        Wu16(map, format.DataPage.RowsStart + 4, Constants.DataPage.NonLiveRowFlags | owned.RowStart);
        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        UsageMap.WritePointer(other, other.Length - 4, 2, pointer.PageNumber);
        await writer.Pager.WritePageAsync(otherRoot, other, Ct);

        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        byte[] isolated = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(isolated, format.TDef.UsedPages, out UsageMapPointer replacement));
        Assert.NotEqual(pointer, replacement);
        Assert.Equal(map, await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct));
        if (onlyNonlivePayload)
        {
            Wu16(map, format.DataPage.RowsStart + (pointer.RowIndex * 2), Constants.DataPage.NonLiveRowFlags | owned.RowStart);
            await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
        }

        UsageMapOwnership.Proof fresh = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        Assert.Contains(member, fresh.OtherMappedPages);
        if (referenceMap)
        {
            HashSet<long> reclaimable = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [bitmapPage], 0, Ct);
            Assert.DoesNotContain(bitmapPage, reclaimable);
            Assert.False(await writer.Services.PageAllocator.IsPageFreeAsync(bitmapPage, Ct));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CanonicalEmptyNonliveSlot_PreservesProofAndReferencedStorage(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        JetFormat format = writer.Database.Format;
        byte[] empty = new byte[format.PageSize];
        empty[0] = Constants.PageTypes.Data;
        empty[1] = 1;
        Wu16(empty, format.DataPage.NumRows, 3);
        Wu16(empty, 2, format.PageSize - format.DataPage.RowsStart - 6);
        for (int index = 0; index < 3; index++)
        {
            Wu16(empty, format.DataPage.RowsStart + (index * 2), Constants.DataPage.NonLiveRowFlags | format.PageSize);
        }

        long emptyPage = await writer.Services.PageAllocator.AllocatePageAsync(empty, Ct);
        UsageMapOwnership.Proof before = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        UsageMap.WritePointer(other, other.Length - 4, 0, emptyPage);
        await writer.Pager.WritePageAsync(otherRoot, other, Ct);

        UsageMapOwnership.Proof after = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        Assert.Equal(before.Pages, after.Pages);
        Assert.True(before.OtherMappedPages.SetEquals(after.OtherMappedPages));
        Assert.DoesNotContain(emptyPage, after.OtherMappedPages);
        Assert.Empty(await UsageMapOwnership.FindUnreferencedStoragePagesAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [emptyPage], 0, Ct));
        Assert.False(await writer.Services.PageAllocator.IsPageFreeAsync(emptyPage, Ct));
        Assert.Equal(empty, await writer.Database.Pages.ReadPageCopyAsync(emptyPage, Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NonliveOpaqueAlias_WithInvalidPayloadBoundsRefusesReclamation(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        JetFormat format = writer.Database.Format;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Wu16(map, format.DataPage.NumRows, 3);
        Wu16(map, format.DataPage.RowsStart + 4, Constants.DataPage.NonLiveRowFlags | format.DataPage.RowsStart);
        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        UsageMap.WritePointer(other, other.Length - 4, 2, pointer.PageNumber);
        await writer.Pager.WritePageAsync(otherRoot, other, Ct);
        byte[] before = stream.ToArray();

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await UsageMapOwnership.FindUnreferencedStoragePagesAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [pointer.PageNumber], 0, Ct));
        Assert.Equal(before, stream.ToArray());
        Assert.False(await writer.Services.PageAllocator.IsPageFreeAsync(pointer.PageNumber, Ct));
    }
}
