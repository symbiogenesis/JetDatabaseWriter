namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Reopened Jet3 core tables maintain private INLINE and REFERENCE map storage.</summary>
public sealed class Jet3SystemOwnedMapTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task NativeEmptyRelationships_FirstAllocationEntersItsOwnedMap()
    {
        await using MemoryStream stream = await CopyNativeAsync();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
            Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(page, Ct));
            await CreateParentAndChildAsync(writer);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId"), Ct);
        }

        await AssertRelationshipPageMappedAsync(stream);
    }

    [Fact]
    public async Task FreshDatabase_ReopenedBeforeItsFirstRelationship_ListsTheAllocatedPage()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await CreateParentAndChildAsync(created);
        }

        stream.Position = 0;
        await using (AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await reopened.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId"), Ct);
        }

        await AssertRelationshipPageMappedAsync(stream);
    }

    [Fact]
    public async Task NativeRelationships_ReferenceMaps_FirstAllocationEntersItsOwnedMap()
    {
        await using MemoryStream stream = await CopyNativeAsync();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await CreateParentAndChildAsync(writer);
            long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
            byte[] target = await writer.Database.Pages.ReadPageCopyAsync(page, Ct);
            foreach (int offset in new[] { writer.Database.Format.TDef.UsedPages, writer.Database.Format.TDef.FreePages })
            {
                Assert.True(UsageMap.TryReadPointer(target, offset, out UsageMapPointer pointer));
                byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
                Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound bound));
                Assert.True(UsageMap.IsInlineBitmapEmpty(map, bound.RowStart, bound.RowSize));
                Array.Clear(map, bound.RowStart, bound.RowSize);
                map[bound.RowStart] = Constants.UsageMap.ReferenceMapType;
                await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            }
        }

        stream.Position = 0;
        await using (AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await reopened.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId"), Ct);
        }

        await AssertRelationshipPageMappedAsync(stream);
    }

    [Theory]
    [InlineData("table")]
    [InlineData("index")]
    [InlineData("opaque-trailer")]
    [InlineData("free-row")]
    [InlineData("row-alias")]
    public async Task SharedMap_FirstAllocationCopiesItsRowsAndPreservesAliases(string alias)
    {
        await using MemoryStream stream = await CopyNativeAsync();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await CreateParentAndChildAsync(writer);
            long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
            byte[] target = await writer.Database.Pages.ReadPageCopyAsync(page, Ct);
            Assert.True(UsageMap.TryReadPointer(target, writer.Database.Format.TDef.UsedPages, out UsageMapPointer owned));
            Assert.True(UsageMap.TryReadPointer(target, writer.Database.Format.TDef.FreePages, out UsageMapPointer free));
            if (alias is "row-alias" or "reference")
            {
                byte[] map = await writer.Database.Pages.ReadPageCopyAsync(owned.PageNumber, Ct);
                Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, owned.RowIndex, out RowBound bound));
                if (alias == "row-alias")
                {
                    Assert.Equal(owned.PageNumber, free.PageNumber);
                    Wu16(map, writer.Database.Format.DataPage.RowsStart + (free.RowIndex * 2), bound.RowStart);
                }
                else
                {
                    map[bound.RowStart] = Constants.UsageMap.ReferenceMapType;
                }

                await writer.Pager.WritePageAsync(owned.PageNumber, map, Ct);
            }
            else
            {
                long changedPage = alias == "free-row"
                    ? page
                    : (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Native", Ct)).TDefPage;
                byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(changedPage, Ct);
                int offset = alias switch
                {
                    "table" => writer.Database.Format.TDef.UsedPages,
                    "free-row" => writer.Database.Format.TDef.FreePages,
                    "index" => Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(changedPage, Ct)).RealIndexes[0].Slot.FirstDpOffset - 4,
                    _ => definition.Length - 4,
                };
                UsageMap.WritePointer(definition, offset, owned.RowIndex, owned.PageNumber);
                await writer.Pager.WritePageAsync(changedPage, definition, Ct);
            }
        }

        stream.Position = 0;
        await using (AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            long page = await reopened.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
            byte[] before = await reopened.Database.Pages.ReadPageCopyAsync(page, Ct);
            Assert.True(UsageMap.TryReadPointer(before, reopened.Database.Format.TDef.UsedPages, out UsageMapPointer original));
            byte[] originalMap = await reopened.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
            Assert.True(UsageMap.TryGetRowBound(originalMap, reopened.Database.Format.DataPage, reopened.Database.Format.PageSize, original.RowIndex, out RowBound originalRow));
            byte[] originalBytes = originalMap.AsSpan(originalRow.RowStart, originalRow.RowSize).ToArray();

            await reopened.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId"), Ct);

            byte[] after = await reopened.Database.Pages.ReadPageCopyAsync(page, Ct);
            Assert.True(UsageMap.TryReadPointer(after, reopened.Database.Format.TDef.UsedPages, out UsageMapPointer isolated));
            Assert.NotEqual(original, isolated);
            byte[] preserved = await reopened.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
            Assert.Equal(originalBytes, preserved.AsSpan(originalRow.RowStart, originalRow.RowSize).ToArray());
        }

        await AssertRelationshipPageMappedAsync(stream);
    }

    [Theory]
    [InlineData("map-type")]
    [InlineData("bitmap-pointer")]
    [InlineData("row-pointer")]
    public async Task InvalidMap_RefusesFirstAllocationAndRestoresBytes(string damage)
    {
        await using MemoryStream stream = await CopyNativeAsync();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await CreateParentAndChildAsync(writer);
            long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
            byte[] target = await writer.Database.Pages.ReadPageCopyAsync(page, Ct);
            Assert.True(UsageMap.TryReadPointer(target, writer.Database.Format.TDef.UsedPages, out UsageMapPointer pointer));
            if (damage == "row-pointer")
            {
                UsageMap.WritePointer(target, writer.Database.Format.TDef.UsedPages, 0, 0xFFFFFF);
                await writer.Pager.WritePageAsync(page, target, Ct);
            }
            else
            {
                byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
                Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound bound));
                map[bound.RowStart] = damage == "map-type" ? (byte)2 : Constants.UsageMap.ReferenceMapType;
                if (damage == "bitmap-pointer")
                {
                    Wi32(map, bound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset, checked((int)writer.Database.Pages.PageCount));
                }

                await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            }
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await reopened.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId"), Ct));
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task SharedReferenceBitmap_FirstAllocationCopiesBothMaps()
    {
        await using MemoryStream stream = await CopyNativeAsync();
        long bitmapPage;
        byte[] bitmap;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await CreateParentAndChildAsync(writer);
            long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
            byte[] target = await writer.Database.Pages.ReadPageCopyAsync(page, Ct);
            bitmap = new byte[writer.Database.Format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            bitmap[1] = 1;
            bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
            long nativeRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Native", Ct)).TDefPage;
            byte[] nativeDefinition = await writer.Database.Pages.ReadPageCopyAsync(nativeRoot, Ct);
            Wi32(nativeDefinition, nativeDefinition.Length - 4, checked((int)bitmapPage));
            await writer.Pager.WritePageAsync(nativeRoot, nativeDefinition, Ct);
            foreach (int offset in new[] { writer.Database.Format.TDef.UsedPages, writer.Database.Format.TDef.FreePages })
            {
                Assert.True(UsageMap.TryReadPointer(target, offset, out UsageMapPointer pointer));
                byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
                Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound bound));
                Array.Clear(map, bound.RowStart, bound.RowSize);
                map[bound.RowStart] = Constants.UsageMap.ReferenceMapType;
                Wi32(map, bound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset, checked((int)bitmapPage));
                await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            }
        }

        stream.Position = 0;
        await using (AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await reopened.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId"), Ct);
            Assert.Equal(bitmap, await reopened.Database.Pages.ReadPageCopyAsync(bitmapPage, Ct));
        }

        await AssertRelationshipPageMappedAsync(stream);
    }

    private static async Task<MemoryStream> CopyNativeAsync()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb");
        var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, Ct), Ct);
        stream.Position = 0;
        return stream;
    }

    private static async Task CreateParentAndChildAsync(AccessWriter writer)
    {
        await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))], Ct);
    }

    private static async Task AssertRelationshipPageMappedAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        DatabaseFile db = reader.Database;
        long page = await reader.Services.Catalog.FindSystemTablePageAsync("MSysRelationships", Ct);
        byte[] definition = await db.Pages.ReadPageCopyAsync(page, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, db.Format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await db.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, db.Format.DataPage, db.Format.PageSize, pointer.RowIndex, out RowBound bound));
        var mapped = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, bound, db.Format.PageSize, db.Pages.PageCount, 1, true, db.Pages.ReadPageAsync, PageBuffers.Return, mapped, Ct));
        var physical = new List<long>();
        for (long candidate = 3; candidate < db.Pages.PageCount; candidate++)
        {
            byte[] data = await db.Pages.ReadPageCopyAsync(candidate, Ct);
            if (data[0] == Constants.PageTypes.Data && Ri32(data, db.Format.DataPage.TDefOff) == page)
            {
                physical.Add(candidate);
            }
        }

        Assert.NotEmpty(physical);
        Assert.Equal(physical.Order(), mapped.Order());
        Assert.Equal("P_C", Assert.Single(await reader.ListRelationshipsAsync(Ct)).Name);
    }
}
