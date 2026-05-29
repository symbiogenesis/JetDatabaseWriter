namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Regression coverage for writer-created linked-table catalog rows in
/// <c>MSysObjects</c>.
/// </summary>
/// <remarks>
/// Access/DAO-authored linked-table fixture rows and cached-schema payloads are
/// the source-of-truth side of comparisons. Writer-created linked rows are the
/// subject under test.
/// </remarks>
public sealed class LinkedTableCatalogWriterTests : IDisposable
{
    private readonly List<string> tempFiles = [];
    private readonly List<string> tempDirs = [];

    [Fact]
    public async Task CreateLinkedTableAsync_AllocatesCatalogIdAndSplicesMsysObjectsIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourcePath = await this.CreateTempAccdbDatabaseAsync("LinkedCatalogSrc");
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedCatalogFE");

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
        Assert.Equal(0, catalogObject.LvPropLength);
        Assert.True(catalogObject.AceCount >= 2, $"Expected linked-table object ACE rows, got {catalogObject.AceCount}.");
        Assert.Equal(0, catalogObject.Low24CollisionCount);
    }

    [Fact]
    public async Task CreateLinkedTableAsync_AccessSourceLeavesDaoCacheColumnsNull()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.LinkeeTest))
        {
            Assert.Skip("Linkee fixture is unavailable on this machine.");
        }

        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedCatalogCacheNulls");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            frontEndPath,
            new AccessWriterOptions { UseLockFile = false },
            ct))
        {
            await writer.CreateTableAsync(
                "LocalAnchor",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                ct);
            await writer.CreateLinkedTableAsync("LinkedTable1", TestDatabases.LinkeeTest, "Table1", ct);
        }

        LinkedCacheColumnSnapshot cacheColumns = await GetLinkedCacheColumnSnapshotAsync(frontEndPath, "LinkedTable1", ct);
        Assert.Equal(TestDatabases.LinkeeTest, cacheColumns.Database);
        Assert.Equal("Table1", cacheColumns.ForeignName);
        Assert.False(cacheColumns.HasLv);
        Assert.False(cacheColumns.HasLvProp);
        Assert.False(cacheColumns.HasLvModule);
        Assert.False(cacheColumns.HasLvExtra);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task CreateLinkedTableAsync_AccessSource_DaoCompactAndOpenRecordsetSucceed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.LinkeeTest))
        {
            Assert.Skip("Linkee fixture is unavailable on this machine.");
        }

        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedCatalogDao");
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            frontEndPath,
            new AccessWriterOptions { UseLockFile = false },
            ct))
        {
            await writer.CreateTableAsync(
                "LocalAnchor",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                ct);
            await writer.InsertRowAsync("LocalAnchor", [1], ct);
            await writer.CreateLinkedTableAsync("LinkedTable1", TestDatabases.LinkeeTest, "Table1", ct);
        }

        string workDir = Path.GetDirectoryName(frontEndPath)!;
        string compactedPath = Path.Combine(Path.GetTempPath(), $"LinkedCatalogDao_{Guid.NewGuid():N}.accdb");
        this.tempFiles.Add(compactedPath);
        AssertDaoSuccess(
            AccessRoundTripEnvironment.RunDaoCompactThenDatabaseScript(
                frontEndPath,
                compactedPath,
                LinkedTableCountScript("LinkedTable1"),
                workDir,
                TimeSpan.FromSeconds(60)),
            "DAO CompactDatabase and OpenRecordset linked table");
    }

    [Fact]
    public async Task CreateLinkedOdbcTableAsync_MetadataOnly_GeneratesRealTableLvProp()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedOdbcCatalog");

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
        Assert.False(Constants.SystemObjects.DefaultLvPropPlaceholder.SequenceEqual(catalogObject.LvProp ?? []));
        var block = ColumnPropertyBlock.Parse(catalogObject.LvProp, DatabaseFormat.AceAccdb);
        Assert.NotNull(block);
        ColumnPropertyTarget tableTarget = Assert.Single(block.Targets);
        Assert.Equal(string.Empty, tableTarget.Name);
        ColumnPropertyEntry nameMap = Assert.Single(tableTarget.Entries, entry => string.Equals(entry.Name, "NameMap", StringComparison.Ordinal));
        Assert.Equal(Constants.ColumnTypes.OleType, nameMap.DataType);
        Assert.True(ContainsBytes(nameMap.Value, Encoding.Unicode.GetBytes("Orders")));
        Assert.True(catalogObject.AceCount >= 2, $"Expected ODBC-linked object ACE rows, got {catalogObject.AceCount}.");
        Assert.Equal(0, catalogObject.Low24CollisionCount);
    }

    [Fact]
    public async Task CreateLinkedOdbcTableAsync_SourceColumnsGeneratesRealSchemaLvProp()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedOdbcGeneratedSchema");
        ColumnDefinition[] sourceColumns =
        [
            new("OrderId", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
            new("CustomerName", typeof(string), maxLength: 100),
            new("Total", typeof(decimal)) { NumericPrecision = 18, NumericScale = 2 },
        ];

        await using (AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedOdbcTableAsync(
                "LinkedOrders",
                "ODBC;DSN=Sales",
                "dbo.Orders",
                sourceColumns,
                ct);
        }

        CatalogObjectSnapshot catalogObject = await GetCatalogObjectAsync(frontEndPath, "LinkedOrders", ct);
        Assert.Equal(Constants.SystemObjects.LinkedOdbcFlags, catalogObject.Flags);
        Assert.False(Constants.SystemObjects.DefaultLvPropPlaceholder.SequenceEqual(catalogObject.LvProp ?? []));

        var block = ColumnPropertyBlock.Parse(catalogObject.LvProp, DatabaseFormat.AceAccdb);
        Assert.NotNull(block);
        Assert.Equal(sourceColumns.Length + 1, block.Targets.Count);

        ColumnPropertyTarget tableTarget = Assert.Single(block.Targets, target => target.Name.Length == 0);
        ColumnPropertyEntry nameMap = Assert.Single(tableTarget.Entries, entry => string.Equals(entry.Name, "NameMap", StringComparison.Ordinal));
        Assert.Equal(Constants.ColumnTypes.OleType, nameMap.DataType);
        Assert.True(ContainsBytes(nameMap.Value, Encoding.Unicode.GetBytes("Orders")));
        Assert.True(ContainsBytes(nameMap.Value, Encoding.Unicode.GetBytes("CustomerName")));

        ColumnPropertyTarget orderId = block.FindTarget("OrderId")!;
        Assert.NotNull(orderId.Find("GUID"));
        Assert.True(orderId.GetBooleanValue(Constants.ColumnPropertyNames.Required));

        ColumnPropertyTarget customerName = block.FindTarget("CustomerName")!;
        Assert.NotNull(customerName.Find("GUID"));
        Assert.True(customerName.GetBooleanValue(Constants.ColumnPropertyNames.AllowZeroLength));

        ColumnPropertyTarget total = block.FindTarget("Total")!;
        Assert.NotNull(total.Find("GUID"));
        Assert.NotNull(total.Find("CurrencyLCID"));
    }

    [Fact]
    public async Task CreateLinkedOdbcTableAsync_SourceColumnsRejectsEmptySchema()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedOdbcGeneratedSchemaReject");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            writer.CreateLinkedOdbcTableAsync(
                "LinkedOrders",
                "ODBC;DSN=Sales",
                "dbo.Orders",
                [],
                ct).AsTask());

        Assert.Contains("source column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateLinkedOdbcTableAsync_CachedSchemaLvPropStoresRealPayload()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.OdbcLinkerTestV2007))
        {
            Assert.Skip("ODBC linker fixture is unavailable on this machine.");
        }

        LinkedOdbcFixtureSnapshot fixture = await GetOdbcFixtureSnapshotAsync(ct);
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedOdbcCachedSchema");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedOdbcTableAsync(
                "LinkedOrders",
                fixture.Connect,
                fixture.ForeignName,
                fixture.LvProp,
                ct);
        }

        CatalogObjectSnapshot catalogObject = await GetCatalogObjectAsync(frontEndPath, "LinkedOrders", ct);
        Assert.True(catalogObject.Id < 0, $"Expected ODBC-linked MSysObjects.Id to be a non-table catalog object id, got {catalogObject.Id}.");
        Assert.Equal(Constants.SystemObjects.LinkedOdbcFlags, catalogObject.Flags);
        Assert.True(fixture.LvProp.SequenceEqual(catalogObject.LvProp ?? []));
        Assert.NotEqual(Constants.SystemObjects.DefaultLvPropPlaceholder, catalogObject.LvProp);

        var block = ColumnPropertyBlock.Parse(catalogObject.LvProp, DatabaseFormat.AceAccdb);
        Assert.NotNull(block);
        Assert.True(block.Targets.Count > 0, "Expected cached ODBC LvProp to contain property targets.");
    }

    [Fact]
    public async Task CreateLinkedOdbcTableAsync_CachedSchemaLvPropRejectsPlaceholder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedOdbcCachedSchemaReject");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            writer.CreateLinkedOdbcTableAsync(
                "LinkedOrders",
                "ODBC;DSN=Sales",
                "dbo.Orders",
                Constants.SystemObjects.DefaultLvPropPlaceholder,
                ct).AsTask());

        Assert.Contains("placeholder", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task CreateLinkedOdbcTableAsync_CachedSchemaLvProp_DaoCompactSucceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.OdbcLinkerTestV2007))
        {
            Assert.Skip("ODBC linker fixture is unavailable on this machine.");
        }

        LinkedOdbcFixtureSnapshot fixture = await GetOdbcFixtureSnapshotAsync(ct);
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedOdbcCachedSchemaDao");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            frontEndPath,
            new AccessWriterOptions { UseLockFile = false },
            ct))
        {
            await writer.CreateTableAsync(
                "LocalAnchor",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                ct);
            await writer.InsertRowAsync("LocalAnchor", [1], ct);
            await writer.CreateLinkedOdbcTableAsync(
                "LinkedOrders",
                fixture.Connect,
                fixture.ForeignName,
                fixture.LvProp,
                ct);
        }

        string compactedPath = Path.Combine(Path.GetTempPath(), $"LinkedOdbcCachedSchemaDao_{Guid.NewGuid():N}.accdb");
        this.tempFiles.Add(compactedPath);
        AssertDaoSuccess(
            AccessRoundTripEnvironment.RunDaoCompact(frontEndPath, compactedPath, TimeSpan.FromSeconds(60)),
            "DAO CompactDatabase cached-schema ODBC linked table");

        CatalogObjectSnapshot compactedObject = await GetCatalogObjectAsync(compactedPath, "LinkedOrders", ct);
        var block = ColumnPropertyBlock.Parse(compactedObject.LvProp, DatabaseFormat.AceAccdb);
        Assert.NotNull(block);
        Assert.True(block.Targets.Count > 0, "Expected compacted ODBC LvProp to retain property targets.");
        Assert.NotEqual(Constants.SystemObjects.DefaultLvPropPlaceholder, compactedObject.LvProp);
    }

    [Fact]
    public async Task CreateLinkedTextTableAsync_AllocatesCatalogId()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedTextCatalog");

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
        Assert.Equal(Constants.SystemObjects.LinkedTextTableFlags, catalogObject.Flags);
        Assert.Equal(0, catalogObject.LvPropLength);
        Assert.Equal("data#csv", catalogObject.ForeignName);
        Assert.True(catalogObject.AceCount >= 2, $"Expected text-linked object ACE rows, got {catalogObject.AceCount}.");
        Assert.Equal(0, catalogObject.Low24CollisionCount);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task CreateLinkedTextTableAsync_TextSource_DaoCompactAndOpenRecordsetSucceed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourceDirectory = Path.Combine(Path.GetTempPath(), $"LinkedTextSource_{Guid.NewGuid():N}");
        Directory.CreateDirectory(sourceDirectory);
        this.tempDirs.Add(sourceDirectory);
        await File.WriteAllTextAsync(Path.Combine(sourceDirectory, "data.csv"), "ID,Name\r\n1,Ada\r\n2,Grace\r\n", ct);

        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedTextDao");
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            frontEndPath,
            new AccessWriterOptions { UseLockFile = false },
            ct))
        {
            await writer.CreateTableAsync(
                "LocalAnchor",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                ct);
            await writer.InsertRowAsync("LocalAnchor", [1], ct);
            await writer.CreateLinkedTextTableAsync(
                "LinkedCsv",
                sourceDirectory,
                "data.csv",
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        CatalogObjectSnapshot catalogObject = await GetCatalogObjectAsync(frontEndPath, "LinkedCsv", ct);
        Assert.Equal(Constants.SystemObjects.LinkedTextTableFlags, catalogObject.Flags);
        Assert.Equal(sourceDirectory, catalogObject.Database);
        Assert.Equal("data#csv", catalogObject.ForeignName);
        Assert.Equal(0, catalogObject.LvPropLength);

        await using (AccessReader reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            List<LinkedTableInfo> links = await reader.ListLinkedTablesAsync(ct);
            LinkedTableInfo link = Assert.Single(links, table => string.Equals(table.Name, "LinkedCsv", StringComparison.OrdinalIgnoreCase));
            Assert.Equal("data.csv", link.SourceObjectName);
        }

        string workDir = Path.GetDirectoryName(frontEndPath)!;
        string compactedPath = Path.Combine(Path.GetTempPath(), $"LinkedTextDao_{Guid.NewGuid():N}.accdb");
        this.tempFiles.Add(compactedPath);
        AssertDaoSuccess(
            AccessRoundTripEnvironment.RunDaoCompactThenDatabaseScript(
                frontEndPath,
                compactedPath,
                LinkedCsvDataScript("LinkedCsv"),
                workDir,
                TimeSpan.FromSeconds(60)),
            "DAO CompactDatabase and OpenRecordset compacted text linked table data");
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task CreateLinkedTextTableAsync_TextSource_DaoOpenRecordsetTrimsValueWhitespace()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourceDirectory = Path.Combine(Path.GetTempPath(), $"LinkedTextWhitespaceSource_{Guid.NewGuid():N}");
        Directory.CreateDirectory(sourceDirectory);
        this.tempDirs.Add(sourceDirectory);
        const string csvText =
            "ID,Unquoted,Quoted,AfterQuote,LeadingSpaceBeforeQuote\r\n" +
            "1,  unquoted  ,\"  quoted  \",\"closed\"  , \"not-starting-quote\" \r\n";
        await File.WriteAllTextAsync(
            Path.Combine(sourceDirectory, "data.csv"),
            csvText,
            ct);

        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedTextWhitespaceDao");
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            frontEndPath,
            new AccessWriterOptions { UseLockFile = false },
            ct))
        {
            await writer.CreateTableAsync(
                "LocalAnchor",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                ct);
            await writer.InsertRowAsync("LocalAnchor", [1], ct);
            await writer.CreateLinkedTextTableAsync(
                "LinkedCsv",
                sourceDirectory,
                "data.csv",
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        string workDir = Path.GetDirectoryName(frontEndPath)!;
        string compactedPath = Path.Combine(Path.GetTempPath(), $"LinkedTextWhitespaceDao_{Guid.NewGuid():N}.accdb");
        this.tempFiles.Add(compactedPath);
        AssertDaoSuccess(
            AccessRoundTripEnvironment.RunDaoCompactThenDatabaseScript(
                frontEndPath,
                compactedPath,
                LinkedCsvWhitespaceScript("LinkedCsv"),
                workDir,
                TimeSpan.FromSeconds(60)),
            "DAO CompactDatabase and OpenRecordset linked text whitespace data");
    }

    [Fact]
    public async Task CreateLinkedTableAsync_DuplicateLinkedTableName_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedDupFE");

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

        string frontEndPath = await this.CopyTempDatabaseAsync(TestDatabases.IndexTestV1997, "Jet3CatalogSplice", ".mdb", ct);
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
        string frontEndPath = await this.CreateTempMdbDatabaseAsync("Jet3LinkedCatalog");

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
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("CatalogObjectDup");

        await using AccessWriter writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct);
        await writer.InsertCatalogObjectAsync(
            objectId: -500,
            parentId: Constants.SystemObjects.TablesParentId,
            objectName: "LowLevelDuplicate",
            objectType: Constants.SystemObjects.LinkedTableType,
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
                objectType: Constants.SystemObjects.LinkedTableType,
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
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("CatalogObjectSplit");

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
                objectType: Constants.SystemObjects.LinkedTableType,
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
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("LinkedSpliceFail");

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
        string frontEndPath = await this.CreateTempAccdbDatabaseAsync("CatalogDeleteSpliceFail");

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
        foreach (string path in this.tempFiles)
        {
            try
            {
                File.Delete(path);
                File.Delete(GetLockFilePath(path));
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }

        foreach (string path in this.tempDirs)
        {
            try
            {
                Directory.Delete(path, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
            catch (UnauthorizedAccessException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private static string GetLockFilePath(string databasePath) =>
        string.Equals(Path.GetExtension(databasePath), ".accdb", StringComparison.OrdinalIgnoreCase)
            ? Path.ChangeExtension(databasePath, ".laccdb")
            : Path.ChangeExtension(databasePath, ".ldb");

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
        byte[]? lvProp = row["LvProp"] is byte[] lvPropBytes ? (byte[])lvPropBytes.Clone() : null;

        return new CatalogObjectSnapshot(
            objectId,
            Convert.ToInt32(row["Flags"], CultureInfo.InvariantCulture),
            lvProp?.Length ?? 0,
            aceCount,
            low24CollisionCount,
            lvProp,
            Convert.ToString(row["Database"], CultureInfo.InvariantCulture),
            Convert.ToString(row["ForeignName"], CultureInfo.InvariantCulture));
    }

    private static async ValueTask<LinkedOdbcFixtureSnapshot> GetOdbcFixtureSnapshotAsync(CancellationToken cancellationToken)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(TestDatabases.OdbcLinkerTestV2007, cancellationToken: cancellationToken);
        DataTable objects = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: cancellationToken);
        DataRow row = objects.AsEnumerable().Single(r =>
            Convert.ToInt32(r["Type"], CultureInfo.InvariantCulture) == Constants.SystemObjects.LinkedOdbcType);

        byte[] lvProp = Assert.IsType<byte[]>(row["LvProp"]);
        var block = ColumnPropertyBlock.Parse(lvProp, DatabaseFormat.AceAccdb);
        Assert.NotNull(block);
        Assert.True(block.Targets.Count > 0, "Expected fixture ODBC LvProp to contain property targets.");

        return new LinkedOdbcFixtureSnapshot(
            Convert.ToString(row["Connect"], CultureInfo.InvariantCulture) ?? string.Empty,
            Convert.ToString(row["ForeignName"], CultureInfo.InvariantCulture) ?? string.Empty,
            (byte[])lvProp.Clone());
    }

    private static async ValueTask<bool> CatalogObjectExistsAsync(AccessWriter writer, string objectName, CancellationToken cancellationToken)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken);
        List<CatalogRow> rows = await writer.GetCatalogRowsAsync(msys, cancellationToken);
        return rows.Any(row => string.Equals(row.Name, objectName, StringComparison.OrdinalIgnoreCase));
    }

    private static async ValueTask<LinkedCacheColumnSnapshot> GetLinkedCacheColumnSnapshotAsync(string dbPath, string objectName, CancellationToken cancellationToken)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(dbPath, cancellationToken: cancellationToken);
        DataTable objects = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: cancellationToken);
        DataRow row = objects.AsEnumerable().Single(r => string.Equals(
            Convert.ToString(r["Name"], CultureInfo.InvariantCulture),
            objectName,
            StringComparison.OrdinalIgnoreCase));

        return new LinkedCacheColumnSnapshot(
            row["Lv"] is byte[],
            row["LvProp"] is byte[],
            row["LvModule"] is byte[],
            row["LvExtra"] is byte[],
            Convert.ToString(row["Database"], CultureInfo.InvariantCulture),
            Convert.ToString(row["ForeignName"], CultureInfo.InvariantCulture));
    }

    private static string LinkedTableCountScript(string tableName)
    {
        string escapedName = tableName.Replace("'", "''", StringComparison.Ordinal);
        return $$"""
            $rs = $db.OpenRecordset('SELECT COUNT(*) AS Cnt FROM [{{escapedName}}]', 4)
            try {
                $count = [int]$rs.Fields.Item('Cnt').Value
                if ($count -lt 0) { throw 'Invalid linked-table count.' }
            } finally {
                $rs.Close()
            }
            """;
    }

    private static string LinkedCsvDataScript(string tableName)
    {
        string escapedName = tableName.Replace("'", "''", StringComparison.Ordinal);
        return $$"""
            $rs = $db.OpenRecordset('SELECT [ID], [Name] FROM [{{escapedName}}]', 4)
            try {
                if ($rs.EOF) { throw 'Expected first CSV row.' }
                $firstId = [int]$rs.Fields.Item('ID').Value
                $firstName = [string]$rs.Fields.Item('Name').Value
                if ($firstId -ne 1) { throw "Unexpected first CSV ID: $firstId" }
                if ($firstName -ne 'Ada') { throw "Unexpected first CSV Name: $firstName" }

                $rs.MoveNext()
                if ($rs.EOF) { throw 'Expected second CSV row.' }
                $secondId = [int]$rs.Fields.Item('ID').Value
                $secondName = [string]$rs.Fields.Item('Name').Value
                if ($secondId -ne 2) { throw "Unexpected second CSV ID: $secondId" }
                if ($secondName -ne 'Grace') { throw "Unexpected second CSV Name: $secondName" }

                $rs.MoveNext()
                if (-not $rs.EOF) { throw 'Expected exactly two CSV rows.' }
            } finally {
                $rs.Close()
            }
            """;
    }

    private static string LinkedCsvWhitespaceScript(string tableName)
    {
        string escapedName = tableName.Replace("'", "''", StringComparison.Ordinal);
        string expectedUnquoted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral("unquoted");
        string expectedQuoted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral("  quoted");
        string expectedAfterQuote = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral("closed");
        string expectedLeadingSpaceBeforeQuote = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral("not-starting-quote");
        return $$"""
            $rs = $db.OpenRecordset('SELECT [ID], [Unquoted], [Quoted], [AfterQuote], [LeadingSpaceBeforeQuote] FROM [{{escapedName}}]', 4)
            try {
                if ($rs.EOF) { throw 'Expected first CSV row.' }
                $id = [int]$rs.Fields.Item('ID').Value
                if ($id -ne 1) { throw "Unexpected CSV ID: $id" }

                $unquoted = [string]$rs.Fields.Item('Unquoted').Value
                $quoted = [string]$rs.Fields.Item('Quoted').Value
                $afterQuote = [string]$rs.Fields.Item('AfterQuote').Value
                $leadingSpaceBeforeQuote = [string]$rs.Fields.Item('LeadingSpaceBeforeQuote').Value

                if ($unquoted -cne {{expectedUnquoted}} -or
                    $quoted -cne {{expectedQuoted}} -or
                    $afterQuote -cne {{expectedAfterQuote}} -or
                    $leadingSpaceBeforeQuote -cne {{expectedLeadingSpaceBeforeQuote}}) {
                    throw "Unexpected whitespace values: Unquoted=[$unquoted] length=$($unquoted.Length); Quoted=[$quoted] length=$($quoted.Length); AfterQuote=[$afterQuote] length=$($afterQuote.Length); LeadingSpaceBeforeQuote=[$leadingSpaceBeforeQuote] length=$($leadingSpaceBeforeQuote.Length)"
                }

                $rs.MoveNext()
                if (-not $rs.EOF) { throw 'Expected exactly one CSV row.' }
            } finally {
                $rs.Close()
            }
            """;
    }

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result, string operation) => Assert.True(
            result.ExitCode == 0,
            $"{operation} failed with exit code {result.ExitCode}. StdOut: {result.StdOut} StdErr: {result.StdErr}");

    private static bool ContainsBytes(byte[] source, byte[] sequence)
    {
        for (int offset = 0; offset <= source.Length - sequence.Length; offset++)
        {
            if (source.AsSpan(offset, sequence.Length).SequenceEqual(sequence))
            {
                return true;
            }
        }

        return sequence.Length == 0;
    }

    private static async ValueTask<int> CountMsysObjectsIntermediateRootsAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        byte[] tdef = await writer.ReadPageAsync(2, cancellationToken);
        try
        {
            int numCols = Ru16(tdef, writer.TDef.NumCols);
            int numRealIdx = Ri32(tdef, writer.TDef.NumRealIdx);

            int colStart = writer.TDef.BlockEnd + (numRealIdx * writer.TDef.RealIdxEntrySz);
            int namePos = colStart + (numCols * writer.ColumnDescriptor.Size);
            for (int i = 0; i < numCols; i++)
            {
                int nameLength = writer.ReadColumnName(tdef, ref namePos, out _);
                Assert.True(nameLength >= 0, $"Failed to walk MSysObjects column name {i}.");
            }

            int realIdxDescStart = namePos;
            IndexLayout layout = writer.IndexLayoutInfo;
            int intermediateRoots = 0;
            for (int ri = 0; ri < numRealIdx; ri++)
            {
                int physStart = layout.RealIdxPhysOffset(realIdxDescStart, ri);
                int firstDp = Ri32(tdef, layout.FirstDpAbsoluteOffset(physStart));
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
            int numCols = Ru16(tdef, writer.TDef.NumCols);
            int numRealIdx = Ri32(tdef, writer.TDef.NumRealIdx);
            Assert.True(numRealIdx > 0, "Expected MSysObjects to declare at least one real index.");

            int colStart = writer.TDef.BlockEnd + (numRealIdx * writer.TDef.RealIdxEntrySz);
            int namePos = colStart + (numCols * writer.ColumnDescriptor.Size);
            for (int i = 0; i < numCols; i++)
            {
                int nameLength = writer.ReadColumnName(tdef, ref namePos, out _);
                Assert.True(nameLength >= 0, $"Failed to walk MSysObjects column name {i}.");
            }

            int realIdxDescStart = namePos;
            IndexLayout layout = writer.IndexLayoutInfo;
            int physStart = layout.RealIdxPhysOffset(realIdxDescStart, 0);
            int firstDp = Ri32(tdef, layout.FirstDpAbsoluteOffset(physStart));
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

        this.tempFiles.Add(temp);
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

        this.tempFiles.Add(temp);
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

        this.tempFiles.Add(temp);
        return temp;
    }

    private sealed record CatalogObjectSnapshot(
        int Id,
        int Flags,
        int LvPropLength,
        int AceCount,
        int Low24CollisionCount,
        byte[]? LvProp = null,
        string? Database = null,
        string? ForeignName = null);

    private sealed record LinkedCacheColumnSnapshot(bool HasLv, bool HasLvProp, bool HasLvModule, bool HasLvExtra, string? Database, string? ForeignName);

    private sealed record LinkedOdbcFixtureSnapshot(string Connect, string ForeignName, byte[] LvProp);
}
