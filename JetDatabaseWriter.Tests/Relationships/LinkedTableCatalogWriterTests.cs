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
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
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

        int catalogLeafEntriesBefore = await CountMsysObjectsLeafEntriesAsync(frontEndPath, DatabaseFormat.AceAccdb, ct);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTableAsync("LinkedProducts", sourcePath, "Products", ct);
        }

        int catalogLeafEntriesAfter = await CountMsysObjectsLeafEntriesAsync(frontEndPath, DatabaseFormat.AceAccdb, ct);
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

    [Fact]
    public async Task CreateTableAsync_AccessAuthoredJet3Mdb_SplicesMsysObjectsIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.IndexTestV1997))
        {
            Assert.Skip("Jet3 index fixture is unavailable on this machine.");
        }

        string frontEndPath = await CopyTempDatabaseAsync(TestDatabases.IndexTestV1997, "Jet3CatalogSplice", ".mdb", ct);
        int catalogLeafEntriesBefore = await CountMsysObjectsLeafEntriesAsync(frontEndPath, DatabaseFormat.Jet3Mdb, ct);
        Assert.True(catalogLeafEntriesBefore > 0, "Expected the Access-authored Jet3 fixture to have indexed MSysObjects leaves.");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            frontEndPath,
            new AccessWriterOptions { UseLockFile = false },
            ct))
        {
            await writer.CreateTableAsync(
                "SplicedJet3Catalog",
                [new ColumnDefinition("Id", typeof(int))],
                ct);
        }

        int catalogLeafEntriesAfter = await CountMsysObjectsLeafEntriesAsync(frontEndPath, DatabaseFormat.Jet3Mdb, ct);
        Assert.True(
            catalogLeafEntriesAfter > catalogLeafEntriesBefore,
            $"Expected Jet3 MSysObjects index leaves to gain entries for the new catalog row. Before={catalogLeafEntriesBefore}, after={catalogLeafEntriesAfter}.");

        await using AccessReader reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        Assert.Contains("SplicedJet3Catalog", await reader.ListTablesAsync(ct));
    }

    [Fact]
    public async Task CreateLinkedTableApis_Jet3Mdb_CreateCatalogRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempMdbDatabaseAsync("Jet3LinkedCatalog");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            frontEndPath,
            new AccessWriterOptions { UseLockFile = false },
            ct))
        {
            await writer.CreateLinkedTableAsync("LinkedAccess", @"C:\Data\source.mdb", "Products", ct);
            await writer.CreateLinkedOdbcTableAsync("LinkedOdbc", "ODBC;DSN=Sales", "dbo.Orders", ct);
            await writer.CreateLinkedTextTableAsync("LinkedCsv", @"C:\Data", "data.csv", "Text;HDR=YES", ct);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        List<LinkedTableInfo> linkedTables = await reader.ListLinkedTablesAsync(ct);
        Assert.Contains(linkedTables, table => string.Equals(table.Name, "LinkedAccess", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(linkedTables, table => string.Equals(table.Name, "LinkedOdbc", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(linkedTables, table => string.Equals(table.Name, "LinkedCsv", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task InsertCatalogObjectAsync_DuplicateParentIdName_ThrowsBeforeSplice()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("CatalogObjectDup");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        await writer.InsertCatalogObjectAsync(
            objectId: -500,
            parentId: Constants.SystemObjects.TablesParentId,
            objectName: "LowLevelDuplicate",
            objectType: (short)Constants.SystemObjects.LinkedTableType,
            catalogFlags: Constants.SystemObjects.LinkedTableFlags,
            owner: Constants.SystemObjects.DefaultOwnerBlob,
            lvProp: Constants.SystemObjects.DefaultLvPropPlaceholder,
            ct);

        await CorruptMsysObjectsFirstIndexRootPageTypeAsync(writer, ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.InsertCatalogObjectAsync(
                objectId: -501,
                parentId: Constants.SystemObjects.TablesParentId,
                objectName: "lowlevelduplicate",
                objectType: (short)Constants.SystemObjects.LinkedTableType,
                catalogFlags: Constants.SystemObjects.LinkedTableFlags,
                owner: Constants.SystemObjects.DefaultOwnerBlob,
                lvProp: Constants.SystemObjects.DefaultLvPropPlaceholder,
                ct).AsTask());

        Assert.Contains("already exists", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Could not maintain MSysObjects catalog indexes", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InsertCatalogObjectAsync_ManyRows_PromotesFreshMsysObjectsIndexesToIntermediateRoots()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("CatalogObjectSplit");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        int intermediateRootsBefore = await CountMsysObjectsIntermediateRootsAsync(writer, ct);

        string lastName = string.Empty;
        for (int i = 0; i < 180; i++)
        {
            lastName = FormattableString.Invariant($"LinkedSplit_{i:D4}_Padding_For_Index_Split");
            await writer.InsertCatalogObjectAsync(
                objectId: -20_000 - i,
                parentId: Constants.SystemObjects.TablesParentId,
                objectName: lastName,
                objectType: (short)Constants.SystemObjects.LinkedTableType,
                catalogFlags: Constants.SystemObjects.LinkedTableFlags,
                owner: Constants.SystemObjects.DefaultOwnerBlob,
                lvProp: Constants.SystemObjects.DefaultLvPropPlaceholder,
                ct);
        }

        int intermediateRootsAfter = await CountMsysObjectsIntermediateRootsAsync(writer, ct);
        Assert.True(
            intermediateRootsAfter > intermediateRootsBefore,
            $"Expected catalog-only inserts to promote at least one MSysObjects index root to an intermediate page. Before={intermediateRootsBefore}, after={intermediateRootsAfter}.");
        Assert.True(await CatalogObjectExistsAsync(writer, lastName, ct));
    }

    [Fact]
    public async Task CreateLinkedTableAsync_Throws_WhenMsysObjectsCatalogSpliceCannotMaintainIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedSpliceFail");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        await CorruptMsysObjectsFirstIndexRootPageTypeAsync(writer, ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.CreateLinkedTableAsync("LinkedData", @"C:\Data\source.accdb", "Data", ct).AsTask());
        Assert.Contains("Could not maintain MSysObjects catalog indexes", ex.Message, StringComparison.Ordinal);
        Assert.False(
            await CatalogObjectExistsAsync(writer, "LinkedData", ct),
            "The failed catalog splice should roll back the linked-table MSysObjects row.");
    }

    [Fact]
    public async Task DropTableAsync_Throws_WhenMsysObjectsCatalogDeleteCannotMaintainIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("CatalogDeleteSpliceFail");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        await writer.CreateTableAsync(
            "Victim",
            [new ColumnDefinition("Id", typeof(int))],
            ct);
        await CorruptMsysObjectsFirstIndexRootPageTypeAsync(writer, ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.DropTableAsync("Victim", ct).AsTask());
        Assert.Contains("Could not maintain MSysObjects catalog indexes while dropping table 'Victim'", ex.Message, StringComparison.Ordinal);
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

    private static async ValueTask<bool> CatalogObjectExistsAsync(AccessWriter writer, string objectName, CancellationToken cancellationToken)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken);
        var rows = await writer.GetCatalogRowsAsync(msys, cancellationToken);
        return rows.Any(row => string.Equals(row.Name, objectName, StringComparison.OrdinalIgnoreCase));
    }

    private static async ValueTask<int> CountMsysObjectsIntermediateRootsAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        byte[] tdef = await writer.ReadPageAsync(2, cancellationToken);
        try
        {
            int numCols = AccessBase.Ru16(tdef, writer._tdef.NumCols);
            int numRealIdx = AccessBase.Ri32(tdef, writer._tdef.NumRealIdx);

            int colStart = writer._tdef.BlockEnd + (numRealIdx * writer._tdef.RealIdxEntrySz);
            int namePos = colStart + (numCols * writer._colDesc.Size);
            for (int i = 0; i < numCols; i++)
            {
                int nameLength = writer.ReadColumnName(tdef, ref namePos, out _);
                Assert.True(nameLength >= 0, $"Failed to walk MSysObjects column name {i}.");
            }

            int realIdxDescStart = namePos;
            IndexLayout layout = writer._indexLayout;
            int intermediateRoots = 0;
            for (int ri = 0; ri < numRealIdx; ri++)
            {
                int physStart = layout.RealIdxPhysOffset(realIdxDescStart, ri);
                int firstDp = AccessBase.Ri32(tdef, layout.FirstDpAbsoluteOffset(physStart));
                if (firstDp <= 0)
                {
                    continue;
                }

                byte[] root = await writer.ReadPageAsync(firstDp, cancellationToken);
                try
                {
                    if (root[0] == Constants.IndexLeafPage.PageTypeIntermediate)
                    {
                        intermediateRoots++;
                    }
                }
                finally
                {
                    AccessBase.ReturnPage(root);
                }
            }

            return intermediateRoots;
        }
        finally
        {
            AccessBase.ReturnPage(tdef);
        }
    }

    private static async ValueTask CorruptMsysObjectsFirstIndexRootPageTypeAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        byte[] tdef = await writer.ReadPageAsync(2, cancellationToken);
        try
        {
            int numCols = AccessBase.Ru16(tdef, writer._tdef.NumCols);
            int numRealIdx = AccessBase.Ri32(tdef, writer._tdef.NumRealIdx);
            Assert.True(numRealIdx > 0, "Expected MSysObjects to declare at least one real index.");

            int colStart = writer._tdef.BlockEnd + (numRealIdx * writer._tdef.RealIdxEntrySz);
            int namePos = colStart + (numCols * writer._colDesc.Size);
            for (int i = 0; i < numCols; i++)
            {
                int nameLength = writer.ReadColumnName(tdef, ref namePos, out _);
                Assert.True(nameLength >= 0, $"Failed to walk MSysObjects column name {i}.");
            }

            int realIdxDescStart = namePos;
            IndexLayout layout = writer._indexLayout;
            int physStart = layout.RealIdxPhysOffset(realIdxDescStart, 0);
            int firstDp = AccessBase.Ri32(tdef, layout.FirstDpAbsoluteOffset(physStart));
            Assert.True(firstDp > 0, "Expected MSysObjects first real-index root page to be allocated.");

            byte[] root = await writer.ReadPageAsync(firstDp, cancellationToken);
            try
            {
                Assert.True(
                    root[0] == Constants.IndexLeafPage.PageTypeLeaf || root[0] == Constants.IndexLeafPage.PageTypeIntermediate,
                    $"Expected MSysObjects index root page {firstDp} to be an index page, got 0x{root[0]:X2}.");
                root[0] = 0x01;
                await writer.WritePageAsync(firstDp, root, cancellationToken);
            }
            finally
            {
                AccessBase.ReturnPage(root);
            }
        }
        finally
        {
            AccessBase.ReturnPage(tdef);
        }
    }

    private static async ValueTask<int> CountMsysObjectsLeafEntriesAsync(string dbPath, DatabaseFormat format, CancellationToken cancellationToken)
    {
        byte[] fileBytes = await File.ReadAllBytesAsync(dbPath, cancellationToken);
        int pageSize = format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        int count = 0;
        for (int pageNumber = 0; pageNumber < fileBytes.Length / pageSize; pageNumber++)
        {
            int pageOffset = pageNumber * pageSize;
            if (fileBytes[pageOffset] == Constants.IndexLeafPage.PageTypeLeaf
                && BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(pageOffset + 4, 4)) == 2)
            {
                count += CountLeafEntries(fileBytes, pageOffset, layout);
            }
        }

        return count;
    }

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset, IndexLeafPageBuilder.LeafPageLayout layout)
    {
        int count = 1;
        for (int i = layout.BitmaskOffset; i < layout.FirstEntryOffset; i++)
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

    private async ValueTask<string> CreateTempMdbDatabaseAsync(string prefix)
    {
        string temp = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.mdb");
        await using (await AccessWriter.CreateDatabaseAsync(
            temp,
            DatabaseFormat.Jet3Mdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
        }

        tempFiles.Add(temp);
        return temp;
    }

    private async ValueTask<string> CopyTempDatabaseAsync(string sourcePath, string prefix, string extension, CancellationToken cancellationToken)
    {
        string temp = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}{extension}");
        await using (FileStream source = File.OpenRead(sourcePath))
        await using (FileStream destination = File.Create(temp))
        {
            await source.CopyToAsync(destination, cancellationToken).ConfigureAwait(false);
        }

        tempFiles.Add(temp);
        return temp;
    }

    private sealed record CatalogObjectSnapshot(int Id, int Flags, int LvPropLength, int AceCount, int Low24CollisionCount);
}
