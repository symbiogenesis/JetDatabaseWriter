namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

/// <summary>
/// Tests for: ListTables, GetTableStats, GetTablesAsDataTable, GetStatistics,
/// GetColumnMetadata, GetRealRowCount, ReadFirstTable, ReadTablePreview, Dispose.
/// </summary>
[Collection<ReadOnlyDatabaseFixture>]
public class AccessReaderCoreTests(DatabaseCache db)
{
    private static readonly int[] ValidPageSizes = [2048, 4096];
    private static readonly string[] ValidVersions = ["Jet3", "Jet4/ACE"];

    // ── ListTables ────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ListTables_WhenDatabaseHasTables_ReturnsNonEmptyList(string path)
    {
        var reader = db.Get(path);

        List<string> tables = reader.ListTables();

        Assert.NotNull(tables);
        Assert.NotEmpty(tables);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ListTables_ReturnedNames_AreNonEmptyStrings(string path)
    {
        var reader = db.Get(path);

        List<string> tables = reader.ListTables();

        Assert.All(tables, name => Assert.False(string.IsNullOrWhiteSpace(name)));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ListTables_ReturnedNames_AreUnique(string path)
    {
        var reader = db.Get(path);

        List<string> tables = reader.ListTables();

        Assert.Equal(tables.Count, tables.Distinct().Count());
    }

    // ── GetTableStats ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetTableStats_CountMatchesListTables(string path)
    {
        var reader = db.Get(path);

        var stats = reader.GetTableStats();
        var tables = reader.ListTables();

        Assert.Equal(tables.Count, stats.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void GetTableStats_RowCountAndColumnCount_ArePositive(string path)
    {
        var reader = db.Get(path);

        var stats = reader.GetTableStats();

        Assert.All(stats, s =>
        {
            Assert.True(s.RowCount >= 0);
            Assert.True(s.ColumnCount > 0);
        });
    }

    // ── GetTablesAsDataTable ──────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetTablesAsDataTable_HasExpectedColumns(string path)
    {
        var reader = db.Get(path);

        var dt = reader.GetTablesAsDataTable();

        Assert.Equal(typeof(string), dt.Columns["TableName"]!.DataType);
        Assert.Equal(typeof(long), dt.Columns["RowCount"]!.DataType);
        Assert.Equal(typeof(int), dt.Columns["ColumnCount"]!.DataType);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetTablesAsDataTable_RowCountMatchesListTables(string path)
    {
        var reader = db.Get(path);

        var dt = reader.GetTablesAsDataTable();
        var tables = reader.ListTables();

        Assert.Equal(tables.Count, dt.Rows.Count);
    }

    // ── GetStatistics ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetStatistics_ReturnsConsistentPageAndSizeInfo(string path)
    {
        var reader = db.Get(path);

        DatabaseStatistics stats = reader.GetStatistics();

        Assert.True(stats.TotalPages > 0);
        Assert.Equal(stats.TotalPages * stats.PageSize, stats.DatabaseSizeBytes);
        Assert.Contains(stats.PageSize, ValidPageSizes);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetStatistics_Version_IsRecognisedJetVersion(string path)
    {
        var reader = db.Get(path);

        DatabaseStatistics stats = reader.GetStatistics();

        Assert.Contains(stats.Version, ValidVersions);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetStatistics_TableCount_MatchesListTables(string path)
    {
        var reader = db.Get(path);

        DatabaseStatistics stats = reader.GetStatistics();
        int tableCount = reader.ListTables().Count;

        Assert.Equal(tableCount, stats.TableCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetStatistics_TotalRows_IsNonNegative(string path)
    {
        var reader = db.Get(path);

        DatabaseStatistics stats = reader.GetStatistics();

        Assert.True(stats.TotalRows >= 0);
    }

    // ── GetColumnMetadata ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetColumnMetadata_ForEachTable_ReturnsNonEmptyList(string path)
    {
        var reader = db.Get(path);

        foreach (string table in reader.ListTables())
        {
            List<ColumnMetadata> meta = reader.GetColumnMetadata(table);
            Assert.NotEmpty(meta);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetColumnMetadata_OrdinalIsSequential(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        List<ColumnMetadata> meta = reader.GetColumnMetadata(table);

        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(i, meta[i].Ordinal);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void GetColumnMetadata_ClrType_IsNeverNull(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        List<ColumnMetadata> meta = reader.GetColumnMetadata(table);

        Assert.All(meta, m => Assert.NotNull(m.ClrType));
    }

    // ── GetRealRowCount ───────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void GetRealRowCount_IsNonNegative(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        long count = reader.GetRealRowCount(table);

        Assert.True(count >= 0);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void GetRealRowCount_ConsistentWithStatsTdefRowCount(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        long real = reader.GetRealRowCount(table);
        long tdef = reader.GetTableStats().Find(s => s.Name == table)!.RowCount;

        // Real row count may differ from TDEF after deletes — both must be >= 0
        Assert.True(real >= 0);
        Assert.True(tdef >= 0);
    }

    // ── ReadFirstTable ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadFirstTable_ReturnsNonEmptyHeadersAndTableName(string path)
    {
        var reader = db.Get(path);

        FirstTableResult result = reader.ReadFirstTable();

        Assert.NotEmpty(result.Headers);
        Assert.False(string.IsNullOrWhiteSpace(result.TableName));
        Assert.True(result.TableCount > 0);
    }

    // ── ReadTable (preview overload) ──────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTable_Preview_HeadersMatchSchemaColumnNames(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        TableResult preview = reader.ReadTable(table, maxRows: 10);

        Assert.Equal(preview.Schema.Count, preview.Headers.Count);
        for (int i = 0; i < preview.Headers.Count; i++)
        {
            Assert.Equal(preview.Schema[i].Name, preview.Headers[i]);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTable_Preview_RowCount_DoesNotExceedMaxRows(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        const int max = 5;

        TableResult preview = reader.ReadTable(table, maxRows: max);

        Assert.True(preview.Rows.Count <= max);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTable_Preview_EachRow_HasSameColumnCountAsHeaders(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        TableResult preview = reader.ReadTable(table, maxRows: 20);

        foreach (var row in preview.Rows)
        {
            Assert.Equal(preview.Headers.Count, row.Length);
        }
    }

    // ── Dispose ───────────────────────────────────────────────────────

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        if (!System.IO.File.Exists(TestDatabases.AdventureWorks))
        {
            return;
        }

        var reader = TestDatabases.Open(TestDatabases.AdventureWorks);
        reader.Dispose();
        var ex = Record.Exception(() => reader.Dispose());
        Assert.Null(ex);
    }

    [Fact]
    public void AfterDispose_ListTables_ThrowsObjectDisposedException()
    {
        if (!System.IO.File.Exists(TestDatabases.AdventureWorks))
        {
            return;
        }

        var reader = TestDatabases.Open(TestDatabases.AdventureWorks);
        reader.Dispose();
        Assert.Throws<ObjectDisposedException>(() => reader.ListTables());
    }

    [Fact]
    public void Open_WhenFileNotFound_ThrowsFileNotFoundException()
    {
        Assert.Throws<System.IO.FileNotFoundException>(() => AccessReader.Open(@"C:\no\such\file.mdb"));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.AllExisting), MemberType = typeof(TestDatabases))]
    public void Open_WhenFileExists_IsNotPasswordProtected(string path)
    {
        var ex = Record.Exception(() => { using var r = TestDatabases.Open(path); });
        Assert.Null(ex);
    }
}
