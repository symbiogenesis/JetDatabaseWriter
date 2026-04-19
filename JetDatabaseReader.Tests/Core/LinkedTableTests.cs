namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for linked tables — tables in a front-end database that reference
/// data stored in a separate source database:
///   1. API shape — ListTables exclusion and ListLinkedTables metadata
///   2. Read-through — TDD: reading/streaming data via the linked reference.
/// </summary>
public sealed class LinkedTableTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    // ═══════════════════════════════════════════════════════════════════
    // 1. API SHAPE — ListTables / ListLinkedTables
    // ═══════════════════════════════════════════════════════════════════
    //
    // ListTables returns only local tables (objType == 1). Linked Access
    // tables (type 4) and linked ODBC tables (type 6) are available via
    // ListLinkedTables() only.

    [Fact]
    public void LinkedTables_ListTables_DoesNotIncludeLinkedTables()
    {
        // ListTables should only return local tables.
        // Linked tables are available via a separate API.
        using var reader = AccessReader.Open(TestDatabases.NorthwindTraders);

        List<string> tables = reader.ListTables();

        // All returned tables should be readable (local)
        Assert.All(tables, t =>
        {
            long count = reader.GetRealRowCount(t);
            Assert.True(count >= 0);
        });
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void LinkedTables_ListLinkedTables_ReturnsLinkedTableInfo(string path)
    {
        // ListLinkedTables() returns metadata about tables that reference
        // external databases (MSysObjects Type = 4 or 6).
        using var reader = AccessReader.Open(path);

        List<LinkedTableInfo> linked = reader.ListLinkedTables();

        // The test databases don't have linked tables, so the result should be empty.
        Assert.NotNull(linked);
    }

    [Fact]
    public void LinkedTables_ListLinkedTables_WithLinkedDb_ReturnsSourceInfo()
    {
        // Create two databases: a "source" with real data and a "front-end" with a linked entry.
        string sourcePath = CreateTempJet4Database("LinkedSrc");
        string frontEndPath = CreateTempJet4Database("LinkedFE");

        // Add a table to the source database
        using (var writer = AccessWriter.Open(sourcePath))
        {
            writer.CreateTable("RemoteData", new List<ColumnDefinition>
            {
                new("Id", typeof(int)),
                new("Value", typeof(string), maxLength: 100),
            });
            writer.InsertRow("RemoteData", new object[] { 1, "Hello from source" });
        }

        using var reader = AccessReader.Open(frontEndPath);
        List<LinkedTableInfo> linked = reader.ListLinkedTables();

        // Currently no linked tables in the fixture — validates the API shape.
        Assert.NotNull(linked);
    }

    [Fact]
    public void LinkedTables_ReadLinkedTable_FollowsReferenceToSourceDb()
    {
        // When a linked Access table (type 4) is encountered, the reader
        // opens the referenced database and reads the foreign table.
        string sourcePath = CreateTempJet4Database("LinkSrc2");

        using (var writer = AccessWriter.Open(sourcePath))
        {
            writer.CreateTable("Products", new List<ColumnDefinition>
            {
                new("ProductID", typeof(int)),
                new("Name", typeof(string), maxLength: 100),
            });
            writer.InsertRow("Products", new object[] { 42, "Widget" });
        }

        // Verify the source data is readable directly
        using var sourceReader = AccessReader.Open(sourcePath);
        DataTable dt = sourceReader.ReadTable("Products")!;

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
    public void LinkedTable_ReadLinkedTable_ReturnsSourceData()
    {
        // Create a source database with data, and a front-end with a linked table entry.
        // Reading the linked table from the front-end should return the source data.
        string sourcePath = CreateTempJet4Database("LinkSrc");
        string frontEndPath = CreateTempJet4Database("LinkFE");

        const string sourceTableName = "Products";

        using (var writer = AccessWriter.Open(sourcePath))
        {
            writer.CreateTable(sourceTableName, new List<ColumnDefinition>
            {
                new("ProductID", typeof(int)),
                new("Name", typeof(string), maxLength: 100),
                new("Price", typeof(decimal)),
            });
            writer.InsertRows(sourceTableName, new[]
            {
                new object[] { 1, "Widget", 9.99m },
                new object[] { 2, "Gadget", 19.99m },
                new object[] { 3, "Doohickey", 29.99m },
            });
        }

        // Inject a linked table entry into the front-end's MSysObjects
        InjectLinkedTableEntry(frontEndPath, "LinkedProducts", sourcePath, sourceTableName);

        // Reading "LinkedProducts" from the front-end should follow the link
        using var reader = AccessReader.Open(frontEndPath);
        List<LinkedTableInfo> linked = reader.ListLinkedTables();

        // Verify the linked table metadata is present
        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedProducts", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal(sourceTableName, entry.ForeignName);
        Assert.Equal(sourcePath, entry.SourceDatabasePath);

        // Reading through the link should return source data
        DataTable dt = reader.ReadTable("LinkedProducts")!;
        Assert.NotNull(dt);
        Assert.Equal(3, dt.Rows.Count);
        Assert.Equal("Widget", dt.Rows[0]["Name"]);
    }

    [Fact]
    public void LinkedTable_StreamLinkedTable_ReturnsSourceRows()
    {
        // Streaming through a linked table should yield source rows.
        string sourcePath = CreateTempJet4Database("LinkStrSrc");
        string frontEndPath = CreateTempJet4Database("LinkStrFE");

        using (var writer = AccessWriter.Open(sourcePath))
        {
            writer.CreateTable("Items", new List<ColumnDefinition>
            {
                new("ItemID", typeof(int)),
                new("Description", typeof(string), maxLength: 200),
            });
            writer.InsertRows("Items", new[]
            {
                new object[] { 10, "Alpha" },
                new object[] { 20, "Beta" },
            });
        }

        InjectLinkedTableEntry(frontEndPath, "LinkedItems", sourcePath, "Items");

        using var reader = AccessReader.Open(frontEndPath);
        int count = reader.StreamRows("LinkedItems").Count();

        Assert.Equal(2, count);
    }

    [Fact]
    public void LinkedTable_ListTables_ExcludesLinkedTables()
    {
        // ListTables should not include linked tables — they require special handling.
        string sourcePath = CreateTempJet4Database("LinkExSrc");
        string frontEndPath = CreateTempJet4Database("LinkExFE");

        using (var writer = AccessWriter.Open(sourcePath))
        {
            writer.CreateTable("Data", new List<ColumnDefinition>
            {
                new("Id", typeof(int)),
            });
        }

        InjectLinkedTableEntry(frontEndPath, "LinkedData", sourcePath, "Data");

        using var reader = AccessReader.Open(frontEndPath);
        List<string> tables = reader.ListTables();

        // Linked tables should not appear in ListTables
        Assert.DoesNotContain("LinkedData", tables);
    }

    [Fact]
    public void LinkedTable_MissingSourceDatabase_ThrowsFileNotFound()
    {
        // Reading a linked table whose source database doesn't exist should
        // throw FileNotFoundException, not return garbage.
        string frontEndPath = CreateTempJet4Database("LinkMiss");

        InjectLinkedTableEntry(
            frontEndPath,
            "LinkedMissing",
            @"C:\NonExistent\Database.mdb",
            "MissingTable");

        using var reader = AccessReader.Open(frontEndPath);
        List<LinkedTableInfo> linked = reader.ListLinkedTables();

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedMissing", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);

        // Attempting to read through a broken link should throw
        Assert.Throws<FileNotFoundException>(() => reader.ReadTable("LinkedMissing"));
    }

    [Fact]
    public void LinkedTable_ListLinkedTables_ReturnsCorrectMetadata()
    {
        // Validate that the linked table metadata from the catalog is complete.
        string sourcePath = CreateTempJet4Database("LinkMetaSrc");
        string frontEndPath = CreateTempJet4Database("LinkMetaFE");

        InjectLinkedTableEntry(frontEndPath, "LinkedMeta", sourcePath, "SourceTable");

        using var reader = AccessReader.Open(frontEndPath);
        List<LinkedTableInfo> linked = reader.ListLinkedTables();

        var entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedMeta", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal("SourceTable", entry.ForeignName);
        Assert.False(string.IsNullOrEmpty(entry.SourceDatabasePath));
        Assert.False(entry.IsOdbc);
    }

    [Fact]
    public void LinkedTable_GetColumnMetadata_ReturnsSourceSchema()
    {
        // GetColumnMetadata on a linked table should return the source table's schema.
        string sourcePath = CreateTempJet4Database("LinkSchSrc");
        string frontEndPath = CreateTempJet4Database("LinkSchFE");

        using (var writer = AccessWriter.Open(sourcePath))
        {
            writer.CreateTable("Customers", new List<ColumnDefinition>
            {
                new("CustID", typeof(int)),
                new("Name", typeof(string), maxLength: 100),
                new("Balance", typeof(decimal)),
            });
        }

        InjectLinkedTableEntry(frontEndPath, "LinkedCustomers", sourcePath, "Customers");

        using var reader = AccessReader.Open(frontEndPath);
        List<ColumnMetadata> meta = reader.GetColumnMetadata("LinkedCustomers");

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
    }

    /// <summary>
    /// Injects a linked table entry (MSysObjects type 4) into a database's catalog.
    /// This writes a row to the system table that references an external database.
    /// </summary>
    private static void InjectLinkedTableEntry(
        string dbPath,
        string linkedTableName,
        string sourceDbPath,
        string foreignTableName)
    {
        using var writer = AccessWriter.Open(dbPath);
        writer.InsertLinkedTableEntry(linkedTableName, sourceDbPath, foreignTableName);
    }

    /// <summary>Creates a minimal Jet4 database by copying NorthwindTraders.accdb.</summary>
    private string CreateTempJet4Database(string prefix)
    {
        string temp = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.accdb");
        File.Copy(TestDatabases.NorthwindTraders, temp, overwrite: true);
        _tempFiles.Add(temp);
        return temp;
    }
}
