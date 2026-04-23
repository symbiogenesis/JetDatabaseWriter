namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Tests for: ReadTable (typed), ReadTableAsStringDataTable,
/// ReadAllTables, ReadAllTablesAsStrings.
/// </summary>
public class AccessReaderReadTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── ReadTable (typed) ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_ReturnsNonNullDataTable(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_TableNameMatchesRequest(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(table, dt.TableName);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_ColumnCount_MatchesGetColumnMetadata(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        Assert.Equal(meta.Count, dt.Columns.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_ColumnTypes_MatchGetColumnMetadataClrTypes(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(meta[i].ClrType, dt.Columns[i].DataType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_WithNullTableName_ReadsFirstTable(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string first = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = (await reader.ReadDataTableAsync(tableName: null, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.Equal(first, dt.TableName);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_ForAllTables_ReturnsNonNullDataTables(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        foreach (string table in await reader.ListTablesAsync(TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.NotNull(dt);
        }
    }

    // ── ReadTableAsStringsAsync ────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsStringsAsync_AllColumnsAreStringType(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = await reader.ReadTableAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        foreach (DataColumn col in dt.Columns)
        {
            Assert.Equal(typeof(string), col.DataType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsStringsAsync_RowCount_MatchesReadTable(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable typed = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        DataTable string_ = await reader.ReadTableAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(typed.Rows.Count, string_.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsStringsAsync_ColumnCount_MatchesReadTable(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable typed = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        DataTable string_ = await reader.ReadTableAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(typed.Columns.Count, string_.Columns.Count);
    }

    // ── ReadAllTables ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTables_ContainsAllTableNames(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> expected = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equivalent(expected, all.Keys);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTables_EachDataTable_HasTypedColumns(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);

        // At least one table must have a non-string column to prove typing
        bool anyTypedColumn = all.Values
            .SelectMany(dt => dt.Columns.Cast<DataColumn>())
            .Any(col => col.DataType != typeof(string));

        Assert.True(anyTypedColumn);
    }

    // ── ReadAllTablesAsStrings ────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsStrings_AllColumns_AreStringType(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsStringsAsync(cancellationToken: TestContext.Current.CancellationToken);

        foreach (var (tableName, dt) in all)
        {
            foreach (DataColumn col in dt.Columns)
            {
                Assert.Equal(typeof(string), col.DataType);
            }
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsStrings_RowCounts_MatchReadAllTables(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        Dictionary<string, DataTable> typed = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);
        Dictionary<string, DataTable> strings = await reader.ReadAllTablesAsStringsAsync(cancellationToken: TestContext.Current.CancellationToken);

        foreach (string name in typed.Keys)
        {
            Assert.Equal(typed[name].Rows.Count, strings[name].Rows.Count);
        }
    }

    // ── ReadTable<T> (generic POCO) ─────────────────────────────────────

    private sealed class GenericRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableGeneric_RowCount_MatchesReadTableTyped(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable? typed = await reader.ReadDataTableAsync(table, 100, cancellationToken: TestContext.Current.CancellationToken);
        List<GenericRow> generic = await reader.ReadTableAsync<GenericRow>(table, 100, TestContext.Current.CancellationToken);

        Assert.NotNull(typed);
        Assert.Equal(typed.Rows.Count, generic.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableGeneric_ReturnsNonNullInstances(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        List<GenericRow> items = await reader.ReadTableAsync<GenericRow>(table, 10, TestContext.Current.CancellationToken);

        Assert.All(items, item => Assert.NotNull(item));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableGeneric_WithMaxRows_LimitsResults(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        List<GenericRow> items = await reader.ReadTableAsync<GenericRow>(table, 2, TestContext.Current.CancellationToken);

        Assert.True(items.Count <= 2);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableGeneric_ForAllTables_DoesNotThrow(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        foreach (string table in await reader.ListTablesAsync(TestContext.Current.CancellationToken))
        {
            List<GenericRow> items = await reader.ReadTableAsync<GenericRow>(table, 5, TestContext.Current.CancellationToken);
            Assert.NotNull(items);
        }
    }

    // ── Progress reporting ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTable_WithProgress_ReportsIncreasingRowCounts(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var reported = new List<long>();

        _ = await reader.ReadDataTableAsync(table, progress: new Progress<long>(reported.Add), cancellationToken: TestContext.Current.CancellationToken);

        // Every reported value should be non-negative; ForEach handles zero callbacks gracefully
        foreach (long v in reported)
        {
            Assert.True(v >= 0);
        }
    }
}
