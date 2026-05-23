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

        int catalogId = await GetCatalogIdAsync(frontEndPath, "LinkedProducts", ct);
        Assert.True(catalogId < 0, $"Expected linked-table MSysObjects.Id to be a non-table catalog object id, got {catalogId}.");
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

        int catalogId = await GetCatalogIdAsync(frontEndPath, "LinkedOrders", ct);
        Assert.True(catalogId < 0, $"Expected ODBC-linked MSysObjects.Id to be a non-table catalog object id, got {catalogId}.");
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

        int catalogId = await GetCatalogIdAsync(frontEndPath, "LinkedCsv", ct);
        Assert.True(catalogId < 0, $"Expected text-linked MSysObjects.Id to be a non-table catalog object id, got {catalogId}.");
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

    private static async ValueTask<int> GetCatalogIdAsync(string dbPath, string objectName, CancellationToken cancellationToken)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(dbPath, cancellationToken: cancellationToken);
        DataTable objects = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: cancellationToken);
        DataRow row = objects.AsEnumerable().Single(r => string.Equals(
            Convert.ToString(r["Name"], CultureInfo.InvariantCulture),
            objectName,
            StringComparison.OrdinalIgnoreCase));

        return Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture);
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
}
