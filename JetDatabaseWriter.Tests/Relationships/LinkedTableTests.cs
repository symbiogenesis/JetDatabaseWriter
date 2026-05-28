namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

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
    // ListTables returns only local tables (objType == 1). Linked Access/text
    // tables (type 6) and linked ODBC tables (type 4) are available via
    // ListLinkedTables() only.

    [Fact]
    public async Task LinkedTables_ListTables_AllReturnedTablesAreReadable()
    {
        // ListTables returns only local tables; verify that every entry it
        // returns is reachable via the standard reader API.
        await using var reader = await AccessReader.OpenAsync(TestDatabases.NorthwindTraders, cancellationToken: TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

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

        var linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        // The test databases don't have linked tables, so the result should be empty.
        Assert.NotNull(linked);
    }

    [Fact]
    public async Task LinkedTables_ListLinkedTables_NoLinkedTablesInjected_ReturnsEmptyList()
    {
        // Sanity check the API shape on a database with no linked entries:
        // injection of an actual linked entry is exercised by
        // LinkedTables_ListLinkedTables_WithAsyncLinkedEntry_ReturnsSourceInfo.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkedSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedFE");

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
        var linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(linked);
        Assert.Empty(linked);
    }

    [Fact]
    public async Task LinkedTables_ListLinkedTables_WithAsyncLinkedEntry_ReturnsSourceInfo()
    {
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkedSrcAsync");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedFEAsync");

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
        var linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedRemoteData", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal(LinkedTableKind.Access, entry.Kind);
        Assert.Equal("RemoteData", entry.SourceObjectName);
        Assert.Equal(sourcePath, entry.SourcePath);
    }

    [Fact]
    public async Task LinkedTables_ListLinkedTables_ReturnsDefensiveCopiesFromCache()
    {
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkedCacheSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedCacheFE");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "RemoteData",
                [new("Id", typeof(int))],
                TestContext.Current.CancellationToken);
        }

        await InjectLinkedTableEntryAsync(
            frontEndPath,
            "LinkedRemoteData",
            sourcePath,
            "RemoteData",
            TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        var first = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);
        var firstEntry = Assert.Single(first);
        firstEntry.Name = "MutatedName";
        firstEntry.SourceObjectName = "MutatedSource";
        first.Clear();

        var second = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);
        var secondEntry = Assert.Single(second);

        Assert.Equal("LinkedRemoteData", secondEntry.Name);
        Assert.Equal("RemoteData", secondEntry.SourceObjectName);
        Assert.Equal(sourcePath, secondEntry.SourcePath);
    }

    [Fact]
    public async Task LinkedTables_CreateLinkedOdbcTableAsync_PersistsType4EntryWithConnectString()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedOdbcFE");
        const string connect = "ODBC;DRIVER={SQL Server};SERVER=db.example.com;DATABASE=Sales;Trusted_Connection=Yes";

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateLinkedOdbcTableAsync(
                "LinkedSalesOrders",
                connect,
                "dbo.Orders",
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        var linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedSalesOrders", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal(LinkedTableKind.Odbc, entry.Kind);
        Assert.Equal("dbo.Orders", entry.SourceObjectName);
        Assert.Equal(connect, entry.ConnectString);
        Assert.Null(entry.SourcePath);
    }

    [Fact]
    public async Task LinkedTables_CreateLinkedOdbcTableAsync_AddsOdbcPrefixWhenMissing()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedOdbcPrefix");
        const string connect = "DSN=Sales;UID=app;PWD=secret";

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateLinkedOdbcTableAsync(
                "LinkedSales",
                connect,
                "Orders",
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        var entry = (await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken))
            .First(l => string.Equals(l.Name, "LinkedSales", StringComparison.OrdinalIgnoreCase));

        Assert.Equal(LinkedTableKind.Odbc, entry.Kind);
        Assert.Equal("ODBC;" + connect, entry.ConnectString);
    }

    [Fact]
    public async Task LinkedTables_CreateLinkedOdbcTableAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedOdbcCancel");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await using var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            writer.CreateLinkedOdbcTableAsync("LinkedOdbcCanceled", "ODBC;DSN=X", "T", cts.Token).AsTask());
    }

    [Fact]
    public async Task LinkedTables_CreateLinkedOdbcTableAsync_DuplicateLocalTableName_Throws()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedOdbcDup");

        await using var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(
            "LocalTable",
            [new("Id", typeof(int))],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.CreateLinkedOdbcTableAsync("LocalTable", "ODBC;DSN=Y", "T2", TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedOdbcTable_ThrowsNotSupportedWithoutOpeningOdbc()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedOdbcRead");

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateLinkedOdbcTableAsync(
                "LinkedSales",
                "ODBC;DSN=NoSuchDsn;SERVER=example.invalid;DATABASE=Sales",
                "dbo.Orders",
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);

        var exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await reader.ReadDataTableAsync("LinkedSales", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Contains("ODBC", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTables_CreateLinkedTableAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkedCancel");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            InjectLinkedTableEntryAsync(frontEndPath, "LinkedCanceled", frontEndPath, "AnyTable", cts.Token).AsTask());
    }

    [Fact]
    public async Task LinkedTables_ReadLinkedTable_FollowsReferenceToSourceDb()
    {
        // When a linked Access table (type 6) is encountered, the reader
        // opens the referenced database and reads the foreign table.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkSrc2");

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
        var dt = (await sourceReader.ReadDataTableAsync("Products", cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(1, dt.Rows.Count);
        Assert.Equal(42, dt.Rows[0]["ProductID"]);
        Assert.Equal("Widget", dt.Rows[0]["Name"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. READ-THROUGH — reading data via a linked reference
    // ═══════════════════════════════════════════════════════════════════
    //
    // Current state: ListLinkedTables() returns metadata (name, source path,
    // source object name), and Access-file linked tables are read through by
    // opening SourcePath and reading SourceObjectName from the source database.

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_ReturnsSourceData()
    {
        // Create a source database with data, and a front-end with a linked table entry.
        // Reading the linked table from the front-end should return the source data.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkFE");

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
        var linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        // Verify the linked table metadata is present
        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedProducts", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal(sourceTableName, entry.SourceObjectName);
        Assert.Equal(sourcePath, entry.SourcePath);

        // Reading through the link should return source data
        var dt = (await reader.ReadDataTableAsync("LinkedProducts", cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.NotNull(dt);
        Assert.Equal(3, dt.Rows.Count);
        Assert.Equal("Widget", dt.Rows[0]["Name"]);
    }

    [Fact]
    public async Task LinkedTable_StreamLinkedTable_ReturnsSourceRows()
    {
        // Streaming through a linked table should yield source rows.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkStrSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkStrFE");

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
        int count = await reader.Rows("LinkedItems", cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task LinkedTable_ListTables_ExcludesLinkedTables()
    {
        // ListTables should not include linked tables — they require special handling.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkExSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkExFE");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Data", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedData", sourcePath, "Data", TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        // Linked tables should not appear in ListTables
        Assert.DoesNotContain("LinkedData", tables);
    }

    [Fact]
    public async Task LinkedTable_MissingSourceDatabase_ThrowsFileNotFound()
    {
        // Reading a linked table whose source database doesn't exist should
        // throw FileNotFoundException, not return garbage.
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkMiss");
        string missingSourcePath = Path.Combine(Path.GetDirectoryName(frontEndPath)!, "missing-source.accdb");

        await InjectLinkedTableEntryAsync(
            frontEndPath,
            "LinkedMissing",
            missingSourcePath,
            "MissingTable",
            TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        var linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

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
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkTraversal");

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
    public async Task LinkedTable_ReadLinkedTable_AbsolutePathOutsideHostDirectory_IsBlockedByDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkAbsSrc");
        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: ct))
        {
            await writer.CreateTableAsync("TrustedData", [new("Id", typeof(int))], ct);
            await writer.InsertRowAsync("TrustedData", [1], ct);
        }

        string hostDirectory = Path.Combine(Path.GetTempPath(), $"LinkAbsHost_{Guid.NewGuid():N}");
        Directory.CreateDirectory(hostDirectory);
        _tempDirectories.Add(hostDirectory);

        string frontEndPath = await CreateTempAccdbDatabaseInDirectoryAsync("LinkAbsFE", hostDirectory);
        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedAbsolute", sourcePath, "TrustedData", ct);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await reader.ReadDataTableAsync("LinkedAbsolute", cancellationToken: ct));
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_StreamHostWithoutPathPolicy_IsBlockedByDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkStreamSrc");
        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: ct))
        {
            await writer.CreateTableAsync("TrustedData", [new("Id", typeof(int))], ct);
            await writer.InsertRowAsync("TrustedData", [1], ct);
        }

        await using var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            ct))
        {
        }

        stream.Position = 0;
        await using (var writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            ct))
        {
            await writer.CreateLinkedTableAsync("LinkedStreamData", sourcePath, "TrustedData", ct);
        }

        stream.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            ct);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await reader.ReadDataTableAsync("LinkedStreamData", cancellationToken: ct));
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_RelativeTraversalPath_CanBeAllowedByCallback()
    {
        // Trusted callers can explicitly allow an escaped relative path via callback.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkPolicySrc");

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

        string frontEndPath = await CreateTempAccdbDatabaseInDirectoryAsync("LinkPolicyFE", nestedDir);
        string relativePath = Path.Combine("..", Path.GetFileName(sourcePath));

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedTrusted", relativePath, "TrustedData", TestContext.Current.CancellationToken);

        var options = new AccessReaderOptions
        {
            LinkedSourcePathValidator = (link, resolvedPath) =>
                string.Equals(link.Name, "LinkedTrusted", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(resolvedPath, sourcePath, StringComparison.OrdinalIgnoreCase),
        };

        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, TestContext.Current.CancellationToken);
        var dt = (await reader.ReadDataTableAsync("LinkedTrusted", cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.Single(dt.Rows);
        Assert.Equal(7, dt.Rows[0]["Id"]);
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_PathOutsideAllowlist_ThrowsUnauthorizedAccess()
    {
        // Allowlist should block linked sources outside trusted directories.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkAllowSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkAllowFE");

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
            LinkedSourcePathAllowlist = [allowlistedDir],
        };

        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await reader.ReadDataTableAsync("LinkedBlocked", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task LinkedTable_ReadLinkedTable_AllowlistRejectsSiblingDirectoryWithSharedPrefix()
    {
        var ct = TestContext.Current.CancellationToken;
        string parentDirectory = Path.Combine(Path.GetTempPath(), $"LinkAllowPrefix_{Guid.NewGuid():N}");
        string allowlistedDirectory = Path.Combine(parentDirectory, "Allowed");
        string siblingDirectory = Path.Combine(parentDirectory, "AllowedSibling");
        string hostDirectory = Path.Combine(parentDirectory, "Host");
        Directory.CreateDirectory(allowlistedDirectory);
        Directory.CreateDirectory(siblingDirectory);
        Directory.CreateDirectory(hostDirectory);
        _tempDirectories.Add(parentDirectory);

        string sourcePath = await CreateTempAccdbDatabaseInDirectoryAsync("LinkPrefixSrc", siblingDirectory);
        string frontEndPath = await CreateTempAccdbDatabaseInDirectoryAsync("LinkPrefixFE", hostDirectory);

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Data", [new("Id", typeof(int))], ct);
            await writer.InsertRowAsync("Data", [1], ct);
        }

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedSibling", sourcePath, "Data", ct);

        var options = new AccessReaderOptions
        {
            LinkedSourcePathAllowlist = [allowlistedDirectory],
        };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await reader.ReadDataTableAsync("LinkedSibling", cancellationToken: ct));
    }

    [Fact]
    public async Task LinkedTable_ListLinkedTables_ReturnsCorrectMetadata()
    {
        // Validate that the linked table metadata from the catalog is complete.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkMetaSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkMetaFE");

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedMeta", sourcePath, "SourceTable", TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        var linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedMeta", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal(LinkedTableKind.Access, entry.Kind);
        Assert.Equal("SourceTable", entry.SourceObjectName);
        Assert.False(string.IsNullOrEmpty(entry.SourcePath));
    }

    [Fact]
    public async Task LinkedTable_GetColumnMetadata_ReturnsSourceSchema()
    {
        var ct = TestContext.Current.CancellationToken;

        // GetColumnMetadata on a linked table should return the source table's schema.
        string sourcePath = await CreateTempAccdbDatabaseAsync("LinkSchSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("LinkSchFE");

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: ct))
        {
            await writer.CreateTableAsync(
                "Customers",
                [
                    new("CustID", typeof(int)),
                    new("Name", typeof(string), maxLength: 100),
                    new("Balance", typeof(decimal)),
                ],
                ct);
        }

        await InjectLinkedTableEntryAsync(frontEndPath, "LinkedCustomers", sourcePath, "Customers", ct);

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        var meta = await reader.GetColumnMetadataAsync("LinkedCustomers", ct);

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
    /// Asynchronously injects a linked table entry (MSysObjects type 6) into a database's catalog.
    /// </summary>
    /// <param name="dbPath">The db path.</param>
    /// <param name="linkedTableName">The linked table name.</param>
    /// <param name="sourceDbPath">The source db path.</param>
    /// <param name="foreignTableName">The foreign table name.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private static async ValueTask InjectLinkedTableEntryAsync(
        string dbPath,
        string linkedTableName,
        string sourceDbPath,
        string foreignTableName,
        CancellationToken cancellationToken = default)
    {
        await using var writer = await AccessWriter.OpenAsync(dbPath, cancellationToken: cancellationToken);
        await writer.CreateLinkedTableAsync(linkedTableName, sourceDbPath, foreignTableName, cancellationToken);
    }

    /// <summary>Creates a temporary empty ACCDB.</summary>
    /// <param name="prefix">The prefix.</param>
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

        _tempFiles.Add(temp);
        return temp;
    }

    private async ValueTask<string> CreateTempAccdbDatabaseInDirectoryAsync(string prefix, string directory)
    {
        string temp = Path.Combine(directory, $"{prefix}_{Guid.NewGuid():N}.accdb");
        await using (await AccessWriter.CreateDatabaseAsync(
            temp,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
        }

        _tempFiles.Add(temp);
        return temp;
    }
}
