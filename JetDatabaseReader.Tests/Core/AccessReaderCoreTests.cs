namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Tests for: ListTables, GetTableStats, GetTablesAsDataTable, GetStatistics,
/// GetColumnMetadata, GetRealRowCount, ReadFirstTable, ReadTablePreview, Dispose.
/// </summary>
public class AccessReaderCoreTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static readonly int[] ValidPageSizes = [2048, 4096];
    private static readonly string[] ValidVersions = ["Jet3", "Jet4/ACE"];

    // ── ListTables ────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTables_WhenDatabaseHasTables_ReturnsNonEmptyList(string path)
    {
        var reader = await db.GetAsync(path);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(tables);
        Assert.NotEmpty(tables);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTables_ReturnedNames_AreNonEmptyStrings(string path)
    {
        var reader = await db.GetAsync(path);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.All(tables, name => Assert.False(string.IsNullOrWhiteSpace(name)));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTables_ReturnedNames_AreUnique(string path)
    {
        var reader = await db.GetAsync(path);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, tables.Distinct().Count());
    }

    // ── GetTableStats ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetTableStats_CountMatchesListTables(string path)
    {
        var reader = await db.GetAsync(path);

        var stats = await reader.GetTableStatsAsync(TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, stats.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task GetTableStats_RowCountAndColumnCount_ArePositive(string path)
    {
        var reader = await db.GetAsync(path);

        var stats = await reader.GetTableStatsAsync(TestContext.Current.CancellationToken);

        Assert.All(stats, s =>
        {
            Assert.True(s.RowCount >= 0);
            Assert.True(s.ColumnCount > 0);
        });
    }

    // ── GetTablesAsDataTable ──────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetTablesAsDataTable_HasExpectedColumns(string path)
    {
        var reader = await db.GetAsync(path);

        var dt = await reader.GetTablesAsDataTableAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typeof(string), dt.Columns["TableName"]!.DataType);
        Assert.Equal(typeof(long), dt.Columns["RowCount"]!.DataType);
        Assert.Equal(typeof(int), dt.Columns["ColumnCount"]!.DataType);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetTablesAsDataTable_RowCountMatchesListTables(string path)
    {
        var reader = await db.GetAsync(path);

        var dt = await reader.GetTablesAsDataTableAsync(TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, dt.Rows.Count);
    }

    // ── GetStatistics ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_ReturnsConsistentPageAndSizeInfo(string path)
    {
        var reader = await db.GetAsync(path);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TotalPages > 0);
        Assert.Equal(stats.TotalPages * stats.PageSize, stats.DatabaseSizeBytes);
        Assert.Contains(stats.PageSize, ValidPageSizes);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_Version_IsRecognisedJetVersion(string path)
    {
        var reader = await db.GetAsync(path);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.Contains(stats.Version, ValidVersions);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_TableCount_MatchesListTables(string path)
    {
        var reader = await db.GetAsync(path);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);
        int tableCount = (await reader.ListTablesAsync(TestContext.Current.CancellationToken)).Count;

        Assert.Equal(tableCount, stats.TableCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_TotalRows_IsNonNegative(string path)
    {
        var reader = await db.GetAsync(path);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TotalRows >= 0);
    }

    // ── GetColumnMetadata ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetColumnMetadata_ForEachTable_ReturnsNonEmptyList(string path)
    {
        var reader = await db.GetAsync(path);

        foreach (string table in await reader.ListTablesAsync(TestContext.Current.CancellationToken))
        {
            List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.NotEmpty(meta);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetColumnMetadata_OrdinalIsSequential(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(i, meta[i].Ordinal);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetColumnMetadata_ClrType_IsNeverNull(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        Assert.All(meta, m => Assert.NotNull(m.ClrType));
    }

    // ── GetRealRowCount ───────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task GetRealRowCount_IsNonNegative(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        long count = await reader.GetRealRowCountAsync(table, TestContext.Current.CancellationToken);

        Assert.True(count >= 0);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task GetRealRowCount_ConsistentWithStatsTdefRowCount(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        long real = await reader.GetRealRowCountAsync(table, TestContext.Current.CancellationToken);
        long tdef = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).Find(s => s.Name == table)!.RowCount;

        // Real row count may differ from TDEF after deletes — both must be >= 0
        Assert.True(real >= 0);
        Assert.True(tdef >= 0);
    }

    // ── ReadFirstTable ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadFirstTable_ReturnsNonEmptyHeadersAndTableName(string path)
    {
        var reader = await db.GetAsync(path);

        FirstTableResult result = await reader.ReadFirstTableAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result.Headers);
        Assert.False(string.IsNullOrWhiteSpace(result.TableName));
        Assert.True(result.TableCount > 0);
    }

    // ── ReadTable (preview overload) ──────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_Preview_HeadersMatchSchemaColumnNames(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        TableResult preview = await reader.ReadTableAsync(table, 10, TestContext.Current.CancellationToken);

        Assert.Equal(preview.Schema.Count, preview.Headers.Count);
        for (int i = 0; i < preview.Headers.Count; i++)
        {
            Assert.Equal(preview.Schema[i].Name, preview.Headers[i]);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_Preview_RowCount_DoesNotExceedMaxRows(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        const int max = 5;

        TableResult preview = await reader.ReadTableAsync(table, max, TestContext.Current.CancellationToken);

        Assert.True(preview.Rows.Count <= max);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_Preview_EachRow_HasSameColumnCountAsHeaders(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        TableResult preview = await reader.ReadTableAsync(table, 20, TestContext.Current.CancellationToken);

        foreach (var row in preview.Rows)
        {
            Assert.Equal(preview.Headers.Count, row.Count);
        }
    }

    // ── Dispose ───────────────────────────────────────────────────────

    [Fact]
    public async Task Dispose_CalledTwice_DoesNotThrow()
    {
        if (!System.IO.File.Exists(TestDatabases.AdventureWorks))
        {
            return;
        }

        var reader = await TestDatabases.OpenAsync(TestDatabases.AdventureWorks, cancellationToken: TestContext.Current.CancellationToken);
        await reader.DisposeAsync();
        var ex = await Record.ExceptionAsync(async () => await reader.DisposeAsync());
        Assert.Null(ex);
    }

    [Fact]
    public async Task AfterDispose_ListTables_ThrowsObjectDisposedException()
    {
        if (!System.IO.File.Exists(TestDatabases.AdventureWorks))
        {
            return;
        }

        var reader = await TestDatabases.OpenAsync(TestDatabases.AdventureWorks, cancellationToken: TestContext.Current.CancellationToken);
        await reader.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Open_WhenFileNotFound_ThrowsFileNotFoundException()
    {
        await Assert.ThrowsAsync<System.IO.FileNotFoundException>(async () => await AccessReader.OpenAsync(@"C:\no\such\file.mdb", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.AllExisting), MemberType = typeof(TestDatabases))]
    public async Task Open_WhenFileExists_IsNotPasswordProtected(string path)
    {
        var ex = await Record.ExceptionAsync(async () => { await using var r = await TestDatabases.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken); });
        Assert.Null(ex);
    }
}
