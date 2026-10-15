namespace JetDatabaseWriter.Tests.Pages;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Native catalog scans can reach new MSysObjects rows through its literal owned-pages map.</summary>
public sealed class CatalogOwnedUsageMapTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task NativeAceSchemaRewrite_ListsCatalogRowPageInOwnedMap()
    {
        await using MemoryStream stream = await CopyFixtureAsync();
        HashSet<long> before;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            before = await ReadCatalogDataPagesAsync(reader.Database);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.RenameColumnAsync("Table4", "name", "Label", Ct);
        }

        await using AccessReader after = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        DatabaseFile db = after.Database;
        List<long> owned = await ReadLiteralCatalogOwnedMapAsync(db);
        long rowPage = await FindCatalogRowPageAsync(db, "Table4");
        Assert.DoesNotContain(rowPage, before);
        Assert.Contains(rowPage, owned);
        Assert.All(await ReadCatalogDataPagesAsync(db), page => Assert.Contains(page, owned));
    }

    [Fact]
    public async Task NativeAceCatalogGrowth_ListsEveryNewDataPageInOwnedMap()
    {
        await using MemoryStream stream = await CopyFixtureAsync();
        HashSet<long> before;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            before = await ReadCatalogDataPagesAsync(reader.Database);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            for (int index = 0; index < 32; index++)
            {
                await writer.CreateTableAsync($"CatalogCapacity_{index:D2}", [new ColumnDefinition("Id", typeof(int))], Ct);
            }
        }

        await using AccessReader after = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        DatabaseFile db = after.Database;
        List<long> owned = await ReadLiteralCatalogOwnedMapAsync(db);
        HashSet<long> physical = await ReadCatalogDataPagesAsync(db);
        Assert.NotEmpty(physical.Except(before));
        Assert.All(physical, page => Assert.Contains(page, owned));
        Assert.Contains(await FindCatalogRowPageAsync(db, "CatalogCapacity_31"), owned);
    }

    private static async Task<MemoryStream> CopyFixtureAsync()
    {
        byte[] fixture = await File.ReadAllBytesAsync(TestDatabases.TestV2010, Ct);
        var stream = new MemoryStream();
        await stream.WriteAsync(fixture, Ct);
        stream.Position = 0;
        return stream;
    }

    private static async Task<HashSet<long>> ReadCatalogDataPagesAsync(DatabaseFile db)
    {
        var pages = new HashSet<long>();
        for (long pageNumber = 3; pageNumber < db.Pages.PageCount; pageNumber++)
        {
            byte[] page = await db.Pages.ReadPageCopyAsync(pageNumber, Ct);
            if (page[0] == Constants.PageTypes.Data && Ri32(page, db.Format.DataPage.TDefOff) == 2)
            {
                _ = pages.Add(pageNumber);
            }
        }

        return pages;
    }

    private static async Task<List<long>> ReadLiteralCatalogOwnedMapAsync(DatabaseFile db)
    {
        byte[] tdef = await db.Pages.ReadPageCopyAsync(2, Ct);
        Assert.True(UsageMap.TryReadPointer(tdef, db.Format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await db.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, db.Format.DataPage, db.Format.PageSize, pointer.RowIndex, out RowBound bound));
        var pages = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, bound, db.Format.PageSize, db.Pages.PageCount, minimumPageNumber: 1, strict: true, db.Pages.ReadPageAsync, PageBuffers.Return, pages, Ct));
        return pages;
    }

    private static async Task<long> FindCatalogRowPageAsync(DatabaseFile db, string tableName)
    {
        TableDef definition = Assert.IsType<TableDef>(await db.TableDefs.ReadTableDefAsync(2, Ct));
        ColumnInfo name = Assert.IsType<ColumnInfo>(definition.FindColumn("Name"));
        var locations = new List<long>();
        foreach (long pageNumber in await ReadCatalogDataPagesAsync(db))
        {
            byte[] page = await db.Pages.ReadPageCopyAsync(pageNumber, Ct);
            foreach (RowLocation row in DataPageRows.EnumerateLiveRowLocations(db.Format, pageNumber, page))
            {
                if (ScalarColumnReader.DecodeSimpleColumnValue(db.Format, page, row.RowStart, row.RowSize, name) == tableName)
                {
                    locations.Add(pageNumber);
                }
            }
        }

        return Assert.Single(locations);
    }
}
