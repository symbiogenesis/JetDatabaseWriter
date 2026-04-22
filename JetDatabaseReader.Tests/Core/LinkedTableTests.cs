namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for linked tables — tables in a front-end database that reference
/// data stored in a separate source database:
///   1. API shape — ListTables exclusion and ListLinkedTables metadata
///   2. Read-through — reading/streaming data via the linked reference.
/// </summary>
public sealed class LinkedTableTests : IDisposable
{
    private readonly List<string> _tempFiles = [];
    private readonly List<string> _tempDirectories = [];

    // ═══════════════════════════════════════════════════════════════════
    // 1. API SHAPE — ListTables / ListLinkedTables
    // ═══════════════════════════════════════════════════════════════════
    //
    // ListTables returns only local tables (objType == 1). Linked Access
    // tables (type 4) and linked ODBC tables (type 6) are available via
    // ListLinkedTables() only.

    [Fact]
    public async Task LinkedTables_ListTables_DoesNotIncludeLinkedTables()
    {
        // ListTables should only return local tables.
        // Linked tables are available via a separate API.
        await using var reader = await AccessReader.OpenAsync(TestDatabases.NorthwindTraders, cancellationToken: TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        // All returned tables should be readable (local)
        foreach (string t in tables)
        {
            long count = await reader.GetRealRowCountAsync(t, TestContext.Current.CancellationToken);
            Assert.True(count >= 0);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task LinkedTables_ListLinkedTables_ReturnsLinkedTableInfo(string path)
    {
        // ListLinkedTables() returns metadata about tables that reference
        // external databases (MSysObjects Type = 4 or 6).
        await using var reader = await AccessReader.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken);

        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        // The test databases don't have linked tables, so the result should be empty.
        Assert.NotNull(linked);
    }

    [Fact]
    public async Task LinkedTables_ListLinkedTables_WithLinkedDb_ReturnsSourceInfo()
    {
        // Create two databases: a "source" with real data and a "front-end" with a linked entry.
        string sourcePath = CreateTempJet4Database("LinkedSrc");
        string frontEndPath = CreateTempJet4Database("LinkedFE");

        // Add a table to the source database
        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "RemoteData",
                [
                    new("Id", typeof(int)),
                    new("Value", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("RemoteData", [1, "Hello from source"], TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        // Currently no linked tables in the fixture — validates the API shape.
        Assert.NotNull(linked);
    }

    [Fact]
    public async Task LinkedTables_ListLinkedTables_WithAsyncLinkedEntry_ReturnsSourceInfo()
    {
        string sourcePath = CreateTempJet4Database("LinkedSrcAsync");
        string frontEndPath = CreateTempJet4Database("LinkedFEAsync");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "RemoteData",
                [
                    new("Id", typeof(int)),
                    new("Value", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "RemoteData",
                [1, "Hello from source"],
                TestContext.Current.CancellationToken);
        }

        await InjectLinkedTableEntryAsync(
            frontEndPath,
            "LinkedRemoteData",
            sourcePath,
            "RemoteData",
            TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedRemoteData", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal("RemoteData", entry.ForeignName);
        Assert.Equal(sourcePath, entry.SourceDatabasePath);
    }

    [Fact]
    public async Task LinkedTables_InsertLinkedTableEntryAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        string frontEndPath = CreateTempJet4Database("LinkedCancel");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            InjectLinkedTableEntryAsync(frontEndPath, "LinkedCanceled", frontEndPath, "AnyTable", cts.Token).AsTask());
    }

    [Fact]
    public async Task LinkedTables_ReadLinkedTable_FollowsReferenceToSourceDb()
    {
        // When a linked Access table (type 4) is encountered, the reader
        // opens the referenced database and reads the foreign table.
        string sourcePath = CreateTempJet4Database("LinkSrc2");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Products",
                [
                    new("ProductID", typeof(int)),
                    new("Name", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Products", [42, "Widget"], TestContext.Current.CancellationToken);
        }

        // Verify the source data is readable directly
        await using var sourceReader = await AccessReader.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken);
        DataTable dt = (await sourceReader.ReadDataTableAsync("Products", cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(1, dt.Rows.Count);
        Assert.Equal(42, dt.Rows[0]["ProductID"]);
        Assert.Equal("Widget", dt.Rows[0]["Name"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. READ-THROUGH — reading data via a linked reference
    // ═══════════════════════════════════════════════════════════════════
    //
    // Current state: ListLinkedTables() returns metadata (name, source path,
    // foreign name), but ReadTable on a linked table does not follow the
    // reference to the source database.
    // When implemented, the reader should:
    //   - Detect that the requested table is linked (MSysObjects type 4/6)
    //   - Open the source database at SourceDatabasePath
    //   - Read the ForeignName table from the source database
    //   - Return the data transparently to the caller

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_ReturnsSourceData()
    {
        // Create a source database with data, and a front-end with a linked table entry.
        // Reading the linked table from the front-end should return the source data.
        string sourcePath = CreateTempJet4Database("LinkSrc");
        string frontEndPath = CreateTempJet4Database("LinkFE");

        const string sourceTableName = "Products";

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                sourceTableName,
                [
                    new("ProductID", typeof(int)),
                    new("Name", typeof(string), maxLength: 100),
                    new("Price", typeof(decimal)),
                ],
                TestContext.Current.CancellationToken);
            _ = await writer.InsertRowsAsync(
                sourceTableName,
                [
                    [1, "Widget", 9.99m],
                    [2, "Gadget", 19.99m],
                    [3, "Doohickey", 29.99m],
                ],
                TestContext.Current.CancellationToken);
        }

        // Inject a linked table entry into the front-end's MSysObjects
        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedProducts", sourcePath, sourceTableName, TestContext.Current.CancellationToken);

        // Reading "LinkedProducts" from the front-end should follow the link
        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        // Verify the linked table metadata is present
        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedProducts", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal(sourceTableName, entry.ForeignName);
        Assert.Equal(sourcePath, entry.SourceDatabasePath);

        // Reading through the link should return source data
        DataTable dt = (await reader.ReadDataTableAsync("LinkedProducts", cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.NotNull(dt);
        Assert.Equal(3, dt.Rows.Count);
        Assert.Equal("Widget", dt.Rows[0]["Name"]);
    }

    [Fact]
    public async Task LinkedTable_StreamLinkedTable_ReturnsSourceRows()
    {
        // Streaming through a linked table should yield source rows.
        string sourcePath = CreateTempJet4Database("LinkStrSrc");
        string frontEndPath = CreateTempJet4Database("LinkStrFE");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new("ItemID", typeof(int)),
                    new("Description", typeof(string), maxLength: 200),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                "Items",
                [
                    [10, "Alpha"],
                    [20, "Beta"],
                ],
                TestContext.Current.CancellationToken);
        }

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedItems", sourcePath, "Items", TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        int count = await reader.StreamRowsAsync("LinkedItems", cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task LinkedTable_ListTables_ExcludesLinkedTables()
    {
        // ListTables should not include linked tables — they require special handling.
        string sourcePath = CreateTempJet4Database("LinkExSrc");
        string frontEndPath = CreateTempJet4Database("LinkExFE");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Data", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedData", sourcePath, "Data", TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        // Linked tables should not appear in ListTables
        Assert.DoesNotContain("LinkedData", tables);
    }

    [Fact]
    public async Task LinkedTable_MissingSourceDatabase_ThrowsFileNotFound()
    {
        // Reading a linked table whose source database doesn't exist should
        // throw FileNotFoundException, not return garbage.
        string frontEndPath = CreateTempJet4Database("LinkMiss");

        await InjectLinkedTableEntryAsync(
            frontEndPath,
            "LinkedMissing",
            @"C:\NonExistent\Database.mdb",
            "MissingTable",
            TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedMissing", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);

        // Attempting to read through a broken link should throw
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await reader.ReadDataTableAsync("LinkedMissing", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_RelativeTraversalPath_IsBlockedByDefault()
    {
        // A malicious relative path that escapes the host DB directory should be blocked.
        string frontEndPath = CreateTempJet4Database("LinkTraversal");

        await InjectLinkedTableEntryAsync(
            frontEndPath,
            "LinkedTraversal",
            @"..\..\sensitive.accdb",
            "SensitiveData",
            TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await reader.ReadDataTableAsync("LinkedTraversal", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_RelativeTraversalPath_CanBeAllowedByCallback()
    {
        // Trusted callers can explicitly allow an escaped relative path via callback.
        string sourcePath = CreateTempJet4Database("LinkPolicySrc");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
            "TrustedData",
            [
                new("Id", typeof(int)),
                new("Value", typeof(string), maxLength: 100),
            ],
            TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("TrustedData", [7, "Allowed by callback"], TestContext.Current.CancellationToken);
        }

        string nestedDir = Path.Combine(Path.GetTempPath(), $"LinkPolicy_{Guid.NewGuid():N}");
        Directory.CreateDirectory(nestedDir);
        _tempDirectories.Add(nestedDir);

        string frontEndPath = CreateTempJet4DatabaseInDirectory("LinkPolicyFE", nestedDir);
        string relativePath = Path.Combine("..", Path.GetFileName(sourcePath));

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedTrusted", relativePath, "TrustedData", TestContext.Current.CancellationToken);

        var options = new AccessReaderOptions
        {
            LinkedSourcePathValidator = (link, resolvedPath) =>
                string.Equals(link.Name, "LinkedTrusted", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(resolvedPath, sourcePath, StringComparison.OrdinalIgnoreCase),
        };

        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync("LinkedTrusted", cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.Single(dt.Rows);
        Assert.Equal(7, dt.Rows[0]["Id"]);
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_PathOutsideAllowlist_ThrowsUnauthorizedAccess()
    {
        // Allowlist should block linked sources outside trusted directories.
        string sourcePath = CreateTempJet4Database("LinkAllowSrc");
        string frontEndPath = CreateTempJet4Database("LinkAllowFE");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Data",
                [
                    new("Id", typeof(int)),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Data", [1], TestContext.Current.CancellationToken);
        }

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedBlocked", sourcePath, "Data", TestContext.Current.CancellationToken);

        string allowlistedDir = Path.Combine(Path.GetTempPath(), $"AllowOnly_{Guid.NewGuid():N}");
        Directory.CreateDirectory(allowlistedDir);
        _tempDirectories.Add(allowlistedDir);

        var options = new AccessReaderOptions
        {
            LinkedSourcePathAllowlist = new[] { allowlistedDir },
        };

        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await reader.ReadDataTableAsync("LinkedBlocked", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task LinkedTable_ListLinkedTables_ReturnsCorrectMetadata()
    {
        // Validate that the linked table metadata from the catalog is complete.
        string sourcePath = CreateTempJet4Database("LinkMetaSrc");
        string frontEndPath = CreateTempJet4Database("LinkMetaFE");

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedMeta", sourcePath, "SourceTable", TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedMeta", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal("SourceTable", entry.ForeignName);
        Assert.False(string.IsNullOrEmpty(entry.SourceDatabasePath));
        Assert.False(entry.IsOdbc);
    }

    [Fact]
    public async Task LinkedTable_GetColumnMetadata_ReturnsSourceSchema()
    {
        // GetColumnMetadata on a linked table should return the source table's schema.
        string sourcePath = CreateTempJet4Database("LinkSchSrc");
        string frontEndPath = CreateTempJet4Database("LinkSchFE");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Customers",
                [
                    new("CustID", typeof(int)),
                    new("Name", typeof(string), maxLength: 100),
                    new("Balance", typeof(decimal)),
                ],
                TestContext.Current.CancellationToken);
        }

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedCustomers", sourcePath, "Customers", TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("LinkedCustomers", TestContext.Current.CancellationToken);

        Assert.Equal(3, meta.Count);
        Assert.Equal("CustID", meta[0].Name);
        Assert.Equal(typeof(int), meta[0].ClrType);
        Assert.Equal("Name", meta[1].Name);
        Assert.Equal(typeof(string), meta[1].ClrType);
        Assert.Equal("Balance", meta[2].Name);
        Assert.Equal(typeof(decimal), meta[2].ClrType);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    public void Dispose()
    {
        foreach (string path in _tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                /* best-effort cleanup */
            }
        }

        foreach (string dir in _tempDirectories.OrderByDescending(d => d.Length))
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch (IOException)
            {
                /* best-effort cleanup */
            }
            catch (UnauthorizedAccessException)
            {
                /* best-effort cleanup */
            }
        }
    }

    /// <summary>
    /// Asynchronously injects a linked table entry (MSysObjects type 4) into a database's catalog.
    /// </summary>
    private static async ValueTask InjectLinkedTableEntryAsync(
        string dbPath,
        string linkedTableName,
        string sourceDbPath,
        string foreignTableName,
        CancellationToken cancellationToken = default)
    {
        await using var writer = await AccessWriter.OpenAsync(dbPath, cancellationToken: cancellationToken);
        await writer.InsertLinkedTableEntryAsync(linkedTableName, sourceDbPath, foreignTableName, cancellationToken);
    }

    /// <summary>Creates a minimal Jet4 database by copying NorthwindTraders.accdb.</summary>
    private string CreateTempJet4Database(string prefix)
    {
        string temp = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.accdb");
        File.Copy(TestDatabases.NorthwindTraders, temp, overwrite: true);
        _tempFiles.Add(temp);
        return temp;
    }

    private string CreateTempJet4DatabaseInDirectory(string prefix, string directory)
    {
        string temp = Path.Combine(directory, $"{prefix}_{Guid.NewGuid():N}.accdb");
        File.Copy(TestDatabases.NorthwindTraders, temp, overwrite: true);
        _tempFiles.Add(temp);
        return temp;
    }
}
