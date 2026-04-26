namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Tests for: ListTables, GetTableStats, GetTablesAsDataTable, GetStatistics,
/// GetColumnMetadata, GetRealRowCount, ReadFirstTable, ReadTablePreview, Dispose.
/// </summary>
public class AccessReaderCatalogTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static readonly int[] ValidPageSizes = [2048, 4096];
    private static readonly string[] ValidVersions = ["Jet3", "Jet4/ACE"];

    // ── ListTables ────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTables_WhenDatabaseHasTables_ReturnsNonEmptyList(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(tables);
        Assert.NotEmpty(tables);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task ListTables_ReturnedNames_AreNonEmptyStrings(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.All(tables, name => Assert.False(string.IsNullOrWhiteSpace(name)));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task ListTables_ReturnedNames_AreUnique(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, tables.Distinct().Count());
    }

    // ── GetTableStats ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetTableStats_CountMatchesListTables(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        var stats = await reader.GetTableStatsAsync(TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, stats.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task GetTableStats_RowCountAndColumnCount_ArePositive(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

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
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetTablesAsDataTable_HasExpectedColumns(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        var dt = await reader.GetTablesAsDataTableAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typeof(string), dt.Columns["TableName"]!.DataType);
        Assert.Equal(typeof(long), dt.Columns["RowCount"]!.DataType);
        Assert.Equal(typeof(int), dt.Columns["ColumnCount"]!.DataType);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetTablesAsDataTable_RowCountMatchesListTables(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        var dt = await reader.GetTablesAsDataTableAsync(TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, dt.Rows.Count);
    }

    // ── GetStatistics ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_ReturnsConsistentPageAndSizeInfo(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TotalPages > 0);
        Assert.Equal(stats.TotalPages * stats.PageSize, stats.DatabaseSizeBytes);
        Assert.Contains(stats.PageSize, ValidPageSizes);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_Version_IsRecognisedJetVersion(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.Contains(stats.Version, ValidVersions);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_TableCount_MatchesListTables(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);
        int tableCount = (await reader.ListTablesAsync(TestContext.Current.CancellationToken)).Count;

        Assert.Equal(tableCount, stats.TableCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetStatistics_TotalRows_IsNonNegative(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TotalRows >= 0);
    }

    // ── GetColumnMetadata ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task GetColumnMetadata_ForEachTable_ReturnsNonEmptyList(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

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
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
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
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        Assert.All(meta, m => Assert.NotNull(m.ClrType));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetColumnMetadata_TypeName_IsNeverRawHex(string path)
    {
        // No column should surface a raw hex TypeName like "0x11" / "0x12";
        // the reader must always resolve a friendly name (incl. for complex columns).
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        foreach (string table in await reader.ListTablesAsync(TestContext.Current.CancellationToken))
        {
            List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.All(meta, col =>
                Assert.False(
                    col.TypeName.StartsWith("0x", StringComparison.Ordinal),
                    $"Column '{col.Name}' in table '{table}' has raw hex TypeName '{col.TypeName}'"));
        }
    }

    // ── GetRealRowCount ───────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task GetRealRowCount_MatchesReadDataTableRowCount(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        long real = await reader.GetRealRowCountAsync(table, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(dt.Rows.Count, real);
    }

    // ── ReadFirstTable ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadFirstTable_ReturnsNonEmptyHeadersAndTableName(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        DataTable table = await reader.ReadFirstTableAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(table.Columns.Count > 0);
        Assert.False(string.IsNullOrWhiteSpace(table.TableName));
    }

    // ── ReadDataTable (preview) ──────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_Preview_ColumnMetadata_Matches(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable? preview = await reader.ReadDataTableAsync(table, 10, cancellationToken: TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        Assert.NotNull(preview);
        Assert.Equal(meta.Count, preview.Columns.Count);
        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(meta[i].Name, preview.Columns[i].ColumnName);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_Preview_RowCount_DoesNotExceedMaxRows(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        const int max = 5;

        DataTable? preview = await reader.ReadDataTableAsync(table, max, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(preview);
        Assert.True(preview.Rows.Count <= max);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_Preview_EachRow_HasSameColumnCount(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable? preview = await reader.ReadDataTableAsync(table, 20, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(preview);
        foreach (DataRow row in preview.Rows)
        {
            Assert.Equal(preview.Columns.Count, row.ItemArray.Length);
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
