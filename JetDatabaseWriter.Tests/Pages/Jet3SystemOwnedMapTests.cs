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

/// <summary>Reopened Jet3 core tables extend only exclusively owned INLINE map rows.</summary>
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

    [Theory]
    [InlineData("table")]
    [InlineData("index")]
    [InlineData("opaque-trailer")]
    [InlineData("free-row")]
    [InlineData("row-alias")]
    [InlineData("reference")]
    public async Task UnprovenMap_RefusesFirstAllocationAndRestoresBytes(string alias)
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

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            JetNotSupportedException error = await Assert.ThrowsAsync<JetNotSupportedException>(async () =>
                await reopened.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId"), Ct));
            Assert.Equal("MSysRelationships", error.ErrorInfo.TableName);
        }

        Assert.Equal(before, stream.ToArray());
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
