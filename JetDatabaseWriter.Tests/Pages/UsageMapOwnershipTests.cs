namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Usage-map mutations distinguish shared rows, shared pages and shared bitmap storage.</summary>
public sealed class UsageMapOwnershipTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task IndexMap_ExclusiveRowOnSharedPage_ProtectsOtherMapMembers(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
        int offset = Assert.Single(image.RealIndexes).Slot.FirstDpOffset - 4;
        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, offset, Ct);
        Assert.True(proof.IsExclusiveRow);
        Assert.True(proof.HasExclusiveBitmapPages);
        Assert.False(proof.IsDedicatedPage);
        Assert.Equal(Assert.Single(image.RealIndexes).RootPage, Assert.Single(proof.Pages));
        Assert.NotEmpty(proof.OtherMappedPages);
        Assert.DoesNotContain(Assert.Single(proof.Pages), proof.OtherMappedPages);
    }

    [Theory]
    [InlineData("table")]
    [InlineData("index")]
    [InlineData("opaque")]
    public async Task IndexMap_AliasedAcrossDescriptors_ProtectsEveryOriginalMember(string alias)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
        int offset = Assert.Single(image.RealIndexes).Slot.FirstDpOffset - 4;
        byte[] definition = await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct) ?? throw new InvalidOperationException();
        Assert.True(UsageMap.TryReadPointer(definition, offset, out UsageMapPointer pointer));
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        int otherOffset = alias switch
        {
            "table" => writer.Database.Format.TDef.UsedPages,
            "index" => Assert.Single(Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(otherRoot, Ct)).RealIndexes).Slot.FirstDpOffset - 4,
            _ => other.Length - 4,
        };
        UsageMap.WritePointer(other, otherOffset, pointer.RowIndex, pointer.PageNumber);
        await writer.Pager.WritePageAsync(otherRoot, other, Ct);

        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, offset, Ct);

        Assert.False(proof.IsExclusiveRow);
        Assert.False(proof.HasExclusiveBitmapPages);
        Assert.False(proof.IsDedicatedPage);
        Assert.NotEmpty(proof.Pages);
        Assert.All(proof.Pages, page => Assert.Contains(page, proof.OtherMappedPages));
        UsageMapPointer copy = await UsageMapOwnership.CreatePrivateCopyAsync(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, proof, Ct);
        Assert.NotEqual(pointer.PageNumber, copy.PageNumber);
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(copy.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, copy.RowIndex, out RowBound row));
        var copied = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, row, writer.Database.Format.PageSize, writer.Database.Pages.PageCount, 1, true, writer.Database.Pages.ReadPageAsync, JetDatabaseWriter.Pages.Paging.PageBuffers.Return, copied, Ct));
        Assert.Equal(proof.Pages, copied);
        await UsageMapOwnership.ReleasePrivateCopyAsync(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, copy);
        Assert.Equal(Constants.PageTypes.Data, (await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct))[0]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MapStorage_ReleasedOnlyAfterEverySurvivingReferenceIsRemoved(bool opaqueAlias)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, writer.Database.Format.TDef.UsedPages, out UsageMapPointer pointer));
        if (opaqueAlias)
        {
            long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
            byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
            UsageMap.WritePointer(other, other.Length - 4, pointer.RowIndex, pointer.PageNumber);
            await writer.Pager.WritePageAsync(otherRoot, other, Ct);
        }

        HashSet<long> referenced = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [pointer.PageNumber], 0, Ct);
        Assert.Empty(referenced);
        HashSet<long> afterDrop = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [pointer.PageNumber], root, Ct);
        if (opaqueAlias)
        {
            Assert.Empty(afterDrop);
        }
        else
        {
            Assert.Equal(pointer.PageNumber, Assert.Single(afterDrop));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task CachedWritableUserMap_DescriptorAliasInvalidatesProofAndCopiesStorage(DatabaseFormat databaseFormat, bool sharedBitmap)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        JetFormat format = writer.Database.Format;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer original));
        Assert.True(UsageMap.TryReadPointer(other, format.TDef.UsedPages, out UsageMapPointer otherPointer));
        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        if (sharedBitmap)
        {
            byte[] bitmap = new byte[format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            bitmap[1] = 1;
            long bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
            foreach (UsageMapPointer pointer in new[] { original, otherPointer })
            {
                byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
                Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
                Array.Clear(map, row.RowStart, row.RowSize);
                map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
                JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 1, checked((int)bitmapPage));
                await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            }
        }
        else
        {
            UsageMap.WritePointer(other, format.TDef.UsedPages, original.RowIndex, original.PageNumber);
            await writer.Pager.WritePageAsync(otherRoot, other, Ct);
        }

        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        byte[] isolatedDefinition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(isolatedDefinition, format.TDef.UsedPages, out UsageMapPointer isolated));
        Assert.NotEqual(original, isolated);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        await writer.InsertRowAsync("Other", new RowValues { ["Id"] = 2 }, Ct);
        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        UsageMapOwnership.Proof otherProof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, otherRoot, format.TDef.UsedPages, Ct);
        Assert.NotEqual(Assert.Single(proof.Pages), Assert.Single(otherProof.Pages));
    }

    [Fact]
    public async Task StorageCleanup_NeverReclaimsGlobalReferenceBitmap()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        _ = await writer.Services.PageAllocator.ReserveContiguousPagesAsync(600, Ct);
        byte[] global = await writer.Database.Pages.ReadPageCopyAsync(1, Ct);
        Assert.True(UsageMap.TryGetFirstRowBound(global, writer.Database.Format.DataPage, writer.Database.Format.PageSize, out RowBound row));
        Assert.Equal(Constants.UsageMap.ReferenceMapType, global[row.RowStart]);
        long bitmap = JetDatabaseWriter.Schema.JetTypeInfo.Ri32(global, row.RowStart + 1);
        Assert.True(bitmap > 1);
        HashSet<long> reclaimable = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [bitmap], 0, Ct);
        Assert.Empty(reclaimable);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ReopenedAliasedTableMaps_RecoverOwnPhysicalPagesAndExcludeForeignPages(DatabaseFormat databaseFormat, bool deletedSlot)
    {
        await using var stream = new MemoryStream();
        long root;
        long otherRoot;
        long physical;
        long foreign;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
            await writer.InsertRowAsync("Other", new RowValues { ["Id"] = 2 }, Ct);
            root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
            otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
            physical = Assert.Single(await writer.Database.OwnedPages.GetOwnedDataPagesAsync(root, Ct));
            foreign = Assert.Single(await writer.Database.OwnedPages.GetOwnedDataPagesAsync(otherRoot, Ct));
            if (deletedSlot)
            {
                JetFormat format = writer.Database.Format;
                byte[] data = await writer.Database.Pages.ReadPageCopyAsync(physical, Ct);
                JetDatabaseWriter.Schema.JetTypeInfo.Wu16(data, format.DataPage.NumRows, 2);
                JetDatabaseWriter.Schema.JetTypeInfo.Wu16(data, format.DataPage.RowsStart + 2, Constants.DataPage.NonLiveRowFlags | format.PageSize);
                await writer.Pager.WritePageAsync(physical, data, Ct);
            }

            byte[] target = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
            byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
            Assert.True(UsageMap.TryReadPointer(other, writer.Database.Format.TDef.UsedPages, out UsageMapPointer otherMap));
            UsageMap.WritePointer(target, writer.Database.Format.TDef.UsedPages, otherMap.RowIndex, otherMap.PageNumber);
            UsageMap.WritePointer(target, writer.Database.Format.TDef.FreePages, otherMap.RowIndex, otherMap.PageNumber);
            await writer.Pager.WritePageAsync(root, target, Ct);
        }

        stream.Position = 0;
        await using AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        Assert.True(await reopened.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        UsageMapOwnership.Proof owned = await UsageMapOwnership.InspectAsync(reopened.Database.Format, reopened.Database.TableDefs, reopened.Services.CatalogRows, reopened.Database.Pages, root, reopened.Database.Format.TDef.UsedPages, Ct);
        UsageMapOwnership.Proof free = await UsageMapOwnership.InspectAsync(reopened.Database.Format, reopened.Database.TableDefs, reopened.Services.CatalogRows, reopened.Database.Pages, root, reopened.Database.Format.TDef.FreePages, Ct);
        Assert.Equal(physical, Assert.Single(owned.Pages));
        Assert.Equal(physical, Assert.Single(free.Pages));
        Assert.DoesNotContain(foreign, owned.Pages);
        Assert.DoesNotContain(foreign, free.Pages);
        await reopened.InsertRowAsync("M", new RowValues { ["Id"] = 3 }, Ct);
        Assert.Equal(physical, Assert.Single(await reopened.Database.OwnedPages.GetOwnedDataPagesAsync(root, Ct)));
        Assert.Equal(foreign, Assert.Single(await reopened.Database.OwnedPages.GetOwnedDataPagesAsync(otherRoot, Ct)));
    }

    [Fact]
    public async Task GlobalBitmapAlias_DetachesAndPreservesAllocationOwnership()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        _ = await writer.Services.PageAllocator.ReserveContiguousPagesAsync(600, Ct);
        JetFormat format = writer.Database.Format;
        byte[] global = await writer.Database.Pages.ReadPageCopyAsync(1, Ct);
        Assert.True(UsageMap.TryGetFirstRowBound(global, format.DataPage, format.PageSize, out RowBound globalRow));
        long bitmapPage = JetDatabaseWriter.Schema.JetTypeInfo.Ri32(global, globalRow.RowStart + 1);
        byte[] bitmap = await writer.Database.Pages.ReadPageCopyAsync(bitmapPage, Ct);
        bitmap.AsSpan(Constants.UsageMap.ReferenceMapBitmapOffset).Clear();
        await writer.Pager.WritePageAsync(bitmapPage, bitmap, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
        Array.Clear(map, row.RowStart, row.RowSize);
        map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
        JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 1, checked((int)bitmapPage));
        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);

        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        Assert.False(proof.HasExclusiveBitmapPages);
        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        Assert.True(UsageMap.TrySetReferencePageState(bitmap, format.PageSize, pointer.PageNumber, isMarked: true));
        Assert.Equal(bitmap, await writer.Database.Pages.ReadPageCopyAsync(bitmapPage, Ct));
    }

    [Fact]
    public async Task NonliveDirectoryAlias_RetainsOpaqueReferenceBytesAndCopiesOwnedMap()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        JetFormat format = writer.Database.Format;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound owned));
        JetDatabaseWriter.Schema.JetTypeInfo.Wu16(map, format.DataPage.NumRows, 3);
        JetDatabaseWriter.Schema.JetTypeInfo.Wu16(map, format.DataPage.RowsStart + 4, Constants.DataPage.NonLiveRowFlags | owned.RowStart);
        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        UsageMap.WritePointer(other, other.Length - 4, 2, pointer.PageNumber);
        await writer.Pager.WritePageAsync(otherRoot, other, Ct);

        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        Assert.False(proof.IsExclusiveRow);
        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        byte[] isolated = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(isolated, format.TDef.UsedPages, out UsageMapPointer replacement));
        Assert.NotEqual(pointer, replacement);
        Assert.Equal(map, await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct));
    }
}
