namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Regression coverage for writer-created linked-table catalog rows in
/// <c>MSysObjects</c>.
/// </summary>
public sealed class LinkedTableCatalogWriterTests : IDisposable
{
    private readonly List<string> tempFiles = [];

    [Fact]
    public async Task CreateLinkedTableAsync_AllocatesCatalogIdAndSplicesMsysObjectsIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkedCatalogSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedCatalogFE");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: ct))
        {
            await writer.CreateTableAsync(
                "Products",
                [new ColumnDefinition("Id", typeof(int))],
                ct);
        }

        int catalogLeafEntriesBefore = await CountMsysObjectsLeafEntriesAsync(frontEndPath, ct);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTableAsync("LinkedProducts", sourcePath, "Products", ct);
        }

        int catalogLeafEntriesAfter = await CountMsysObjectsLeafEntriesAsync(frontEndPath, ct);
        Assert.True(
            catalogLeafEntriesAfter > catalogLeafEntriesBefore,
            $"Expected MSysObjects index leaves to gain entries for the linked-table catalog row. Before={catalogLeafEntriesBefore}, after={catalogLeafEntriesAfter}.");

        CatalogObjectSnapshot catalogObject = await GetCatalogObjectAsync(frontEndPath, "LinkedProducts", ct);
        Assert.True(catalogObject.Id < 0, $"Expected linked-table MSysObjects.Id to be a non-table catalog object id, got {catalogObject.Id}.");
        Assert.Equal(Constants.SystemObjects.LinkedTableFlags, catalogObject.Flags);
        Assert.True(catalogObject.LvPropLength > 0, "Expected linked-table MSysObjects.LvProp to be non-null.");
        Assert.True(catalogObject.AceCount >= 2, $"Expected linked-table object ACE rows, got {catalogObject.AceCount}.");
        Assert.Equal(0, catalogObject.Low24CollisionCount);
    }

    [Fact]
    public async Task CreateLinkedOdbcTableAsync_AllocatesCatalogId()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedOdbcCatalog");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedOdbcTableAsync(
                "LinkedOrders",
                "ODBC;DSN=Sales",
                "dbo.Orders",
                ct);
        }

        CatalogObjectSnapshot catalogObject = await GetCatalogObjectAsync(frontEndPath, "LinkedOrders", ct);
        Assert.True(catalogObject.Id < 0, $"Expected ODBC-linked MSysObjects.Id to be a non-table catalog object id, got {catalogObject.Id}.");
        Assert.Equal(Constants.SystemObjects.LinkedOdbcFlags, catalogObject.Flags);
        Assert.True(catalogObject.LvPropLength > 0, "Expected ODBC-linked MSysObjects.LvProp to be non-null.");
        Assert.True(catalogObject.AceCount >= 2, $"Expected ODBC-linked object ACE rows, got {catalogObject.AceCount}.");
        Assert.Equal(0, catalogObject.Low24CollisionCount);
    }

    [Fact]
    public async Task CreateLinkedTextTableAsync_AllocatesCatalogId()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedTextCatalog");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedCsv",
                @"C:\Data",
                "data.csv",
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        CatalogObjectSnapshot catalogObject = await GetCatalogObjectAsync(frontEndPath, "LinkedCsv", ct);
        Assert.True(catalogObject.Id < 0, $"Expected text-linked MSysObjects.Id to be a non-table catalog object id, got {catalogObject.Id}.");
        Assert.Equal(Constants.SystemObjects.LinkedTableFlags, catalogObject.Flags);
        Assert.True(catalogObject.LvPropLength > 0, "Expected text-linked MSysObjects.LvProp to be non-null.");
        Assert.True(catalogObject.AceCount >= 2, $"Expected text-linked object ACE rows, got {catalogObject.AceCount}.");
        Assert.Equal(0, catalogObject.Low24CollisionCount);
    }

    [Fact]
    public async Task CreateLinkedTableAsync_DuplicateLinkedTableName_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedDupFE");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        await writer.CreateLinkedTableAsync("LinkedData", @"C:\Data\source.accdb", "Data", ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.CreateLinkedOdbcTableAsync("LinkedData", "ODBC;DSN=Other", "dbo.Data", ct).AsTask());
    }

    public void Dispose()
    {
        foreach (string path in tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private async ValueTask<string> CreateTempAccdbDatabaseAsync(string prefix)
    {
        string temp = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.accdb");
        await using (await AccessWriter.CreateDatabaseAsync(
            temp,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
        }

        tempFiles.Add(temp);
        return temp;
    }

    private static async ValueTask<CatalogObjectSnapshot> GetCatalogObjectAsync(string dbPath, string objectName, CancellationToken cancellationToken)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(dbPath, cancellationToken: cancellationToken);
        DataTable objects = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: cancellationToken);
        DataRow row = objects.AsEnumerable().Single(r => string.Equals(
            Convert.ToString(r["Name"], CultureInfo.InvariantCulture),
            objectName,
            StringComparison.OrdinalIgnoreCase));

        int objectId = Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture);
        int objectLow24 = objectId & 0x00FFFFFF;
        int low24CollisionCount = objects.AsEnumerable().Count(r =>
        {
            int id = Convert.ToInt32(r["Id"], CultureInfo.InvariantCulture);
            return id != objectId && id != 0 && (id & 0x00FFFFFF) == objectLow24;
        });

        DataTable aces = await reader.ReadDataTableAsync("MSysACEs", cancellationToken: cancellationToken);
        int aceCount = aces.AsEnumerable().Count(r => Convert.ToInt32(r["ObjectId"], CultureInfo.InvariantCulture) == objectId);

        return new CatalogObjectSnapshot(
            objectId,
            Convert.ToInt32(row["Flags"], CultureInfo.InvariantCulture),
            row["LvProp"] is byte[] lvProp ? lvProp.Length : 0,
            aceCount,
            low24CollisionCount);
    }

    private static async ValueTask<int> CountMsysObjectsLeafEntriesAsync(string dbPath, CancellationToken cancellationToken)
    {
        byte[] fileBytes = await File.ReadAllBytesAsync(dbPath, cancellationToken);
        int pageSize = Constants.PageSizes.Jet4;
        int count = 0;
        for (int pageNumber = 0; pageNumber < fileBytes.Length / pageSize; pageNumber++)
        {
            int pageOffset = pageNumber * pageSize;
            if (fileBytes[pageOffset] == Constants.IndexLeafPage.PageTypeLeaf
                && BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(pageOffset + 4, 4)) == 2)
            {
                count += CountLeafEntries(fileBytes, pageOffset);
            }
        }

        return count;
    }

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        int count = 1;
        for (int i = Constants.IndexLeafPage.Jet4.BitmaskOffset; i < Constants.IndexLeafPage.Jet4.FirstEntryOffset; i++)
        {
            byte value = fileBytes[leafOffset + i];
            for (int bit = 0; bit < 8; bit++)
            {
                if ((value & (1 << bit)) != 0)
                {
                    count++;
                }
            }
        }

        return Math.Max(0, count - 1);
    }

    private sealed record CatalogObjectSnapshot(int Id, int Flags, int LvPropLength, int AceCount, int Low24CollisionCount);
}
