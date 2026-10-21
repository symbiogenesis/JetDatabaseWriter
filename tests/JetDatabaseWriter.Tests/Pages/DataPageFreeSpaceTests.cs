namespace JetDatabaseWriter.Tests.Pages;

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
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Data free-space maps guide insertion and recover space after ordinary deletion.</summary>
public sealed class DataPageFreeSpaceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A page rejected by a full row leaves the free map, as in native DAO inserts.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FailedRowFit_RemovesPageFromFreeMap(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Pad", typeof(byte[]), maxLength: 200)], Ct);
        await writer.InsertRowsAsync("Items", Enumerable.Range(1, 45).Select(id => new object?[] { id, new byte[200] }), Ct);
        CatalogEntry? entry = await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct);
        Assert.NotNull(entry);
        IReadOnlyList<long> owned = await writer.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct);
        Assert.True(owned.Count > 1);
        List<long> free = await ReadFreePagesAsync(writer, entry.TDefPage);
        Assert.DoesNotContain(owned[0], free);
        Assert.Contains(owned[^1], free);
    }

    /// <summary>Deletion packs the removed bytes without changing surviving slots; reopening reuses the page.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DeletedRow_ReclaimsPayloadAndReusesFreePageAfterReopen(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        long firstPage;
        int freeBefore;
        long tdefPage;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Pad", typeof(byte[]), maxLength: 200)], Ct);
            await writer.InsertRowsAsync("Items", Enumerable.Range(1, 45).Select(id => new object?[] { id, new byte[200] }), Ct);
            CatalogEntry? entry = await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct);
            Assert.NotNull(entry);
            tdefPage = entry.TDefPage;
            firstPage = (await writer.Database.OwnedPages.GetOwnedDataPagesAsync(tdefPage, Ct))[0];
            byte[] before = await writer.Database.Pages.ReadPageCopyAsync(firstPage, Ct);
            freeBefore = Ru16(before, 2);
            Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 1, Ct));
            byte[] after = await writer.Database.Pages.ReadPageCopyAsync(firstPage, Ct);
            Assert.True(Ru16(after, 2) > freeBefore);
            Assert.Equal(Ru16(before, writer.Database.Format.DataPage.NumRows), Ru16(after, writer.Database.Format.DataPage.NumRows));
            Assert.Contains(firstPage, await ReadFreePagesAsync(writer, tdefPage));
        }

        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.InsertRowAsync("Items", [46, new byte[200]], Ct);
            byte[] page = await writer.Database.Pages.ReadPageCopyAsync(firstPage, Ct);
            Assert.True(Ru16(page, 2) <= freeBefore);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        var ids = new List<int>();
        await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
        {
            ids.Add((int)row[0]);
            Assert.Equal(new byte[200], (byte[])row[1]);
        }

        Assert.Equal(Enumerable.Range(2, 45), ids.Order());
    }

    /// <summary>Repacking and map changes roll back with the deleted row and its indexes.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DeletedRow_RollbackRestoresExactDatabase(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Pad", typeof(byte[]), maxLength: 200)], Ct);
        await writer.InsertRowsAsync("Items", Enumerable.Range(1, 25).Select(id => new object?[] { id, new byte[200] }), Ct);
        await writer.Pager.FlushAsync(toDisk: false, Ct);
        byte[] before = stream.ToArray();
        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 1, Ct));
        await transaction.RollbackAsync(Ct);
        await writer.Pager.FlushAsync(toDisk: false, Ct);
        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<List<long>> ReadFreePagesAsync(AccessWriter writer, long tdefPage)
    {
        JetFormat format = writer.Database.Format;
        byte[] tdef = await writer.Database.Pages.ReadPageCopyAsync(tdefPage, Ct);
        int mapPage = UsageMap.ReadUInt24(tdef, format.TDef.FreePagesPage);
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(mapPage, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, tdef[format.TDef.FreePages], out RowBound bound));
        var pages = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, bound, format.PageSize, writer.Pager.PageCount, 1, true, writer.Pager.ReadPageAsync, PageBuffers.Return, pages, Ct));
        return pages;
    }
}
