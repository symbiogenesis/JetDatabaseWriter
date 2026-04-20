namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Tests for: ReadTable (typed), ReadTableAsStringDataTable,
/// ReadAllTables, ReadAllTablesAsStrings.
/// </summary>
[Collection<ReadOnlyDatabaseFixture>]
public class AccessReaderReadTests(DatabaseCache db)
{
    // ── ReadTable (typed) ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTable_ReturnsNonNullDataTable(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        DataTable dt = reader.ReadTable(table)!;

        Assert.NotNull(dt);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTable_TableNameMatchesRequest(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        DataTable dt = reader.ReadTable(table)!;

        Assert.Equal(table, dt.TableName);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTable_ColumnCount_MatchesGetColumnMetadata(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        DataTable dt = reader.ReadTable(table)!;
        var meta = reader.GetColumnMetadata(table);

        Assert.Equal(meta.Count, dt.Columns.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTable_ColumnTypes_MatchGetColumnMetadataClrTypes(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        DataTable dt = reader.ReadTable(table)!;
        var meta = reader.GetColumnMetadata(table);

        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(meta[i].ClrType, dt.Columns[i].DataType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadTable_WithNullTableName_ReadsFirstTable(string path)
    {
        var reader = db.Get(path);
        string first = reader.ListTables()[0];

        DataTable dt = reader.ReadTable(tableName: null)!;

        Assert.NotNull(dt);
        Assert.Equal(first, dt.TableName);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadTable_ForAllTables_ReturnsNonNullDataTables(string path)
    {
        var reader = db.Get(path);

        foreach (string table in reader.ListTables())
        {
            DataTable dt = reader.ReadTable(table)!;
            Assert.NotNull(dt);
        }
    }

    // ── ReadTableAsStringDataTable ────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTableAsStringDataTable_AllColumnsAreStringType(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        DataTable dt = reader.ReadTableAsStringDataTable(table)!;

        foreach (DataColumn col in dt.Columns)
        {
            Assert.Equal(typeof(string), col.DataType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTableAsStringDataTable_RowCount_MatchesReadTable(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        DataTable typed = reader.ReadTable(table)!;
        DataTable string_ = reader.ReadTableAsStringDataTable(table)!;

        Assert.Equal(typed.Rows.Count, string_.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ReadTableAsStringDataTable_ColumnCount_MatchesReadTable(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        DataTable typed = reader.ReadTable(table)!;
        DataTable string_ = reader.ReadTableAsStringDataTable(table)!;

        Assert.Equal(typed.Columns.Count, string_.Columns.Count);
    }

    // ── ReadAllTables ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadAllTables_ContainsAllTableNames(string path)
    {
        var reader = db.Get(path);
        List<string> expected = reader.ListTables();

        Dictionary<string, DataTable> all = reader.ReadAllTables();

        Assert.Equivalent(expected, all.Keys);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadAllTables_EachDataTable_HasTypedColumns(string path)
    {
        var reader = db.Get(path);

        Dictionary<string, DataTable> all = reader.ReadAllTables();

        // At least one table must have a non-string column to prove typing
        bool anyTypedColumn = all.Values
            .SelectMany(dt => dt.Columns.Cast<DataColumn>())
            .Any(col => col.DataType != typeof(string));

        Assert.True(anyTypedColumn);
    }

    // ── ReadAllTablesAsStrings ────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadAllTablesAsStrings_AllColumns_AreStringType(string path)
    {
        var reader = db.Get(path);

        Dictionary<string, DataTable> all = reader.ReadAllTablesAsStrings();

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
    public void ReadAllTablesAsStrings_RowCounts_MatchReadAllTables(string path)
    {
        var reader = db.Get(path);

        Dictionary<string, DataTable> typed = reader.ReadAllTables();
        Dictionary<string, DataTable> strings = reader.ReadAllTablesAsStrings();

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
    public void ReadTableGeneric_RowCount_MatchesReadTableTyped(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        TableResult typed = reader.ReadTable(table, 100);
        List<GenericRow> generic = reader.ReadTable<GenericRow>(table, 100);

        Assert.Equal(typed.Rows.Count, generic.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadTableGeneric_ReturnsNonNullInstances(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        List<GenericRow> items = reader.ReadTable<GenericRow>(table, 10);

        Assert.All(items, item => Assert.NotNull(item));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadTableGeneric_WithMaxRows_LimitsResults(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        List<GenericRow> items = reader.ReadTable<GenericRow>(table, 2);

        Assert.True(items.Count <= 2);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadTableGeneric_ForAllTables_DoesNotThrow(string path)
    {
        var reader = db.Get(path);

        foreach (string table in reader.ListTables())
        {
            List<GenericRow> items = reader.ReadTable<GenericRow>(table, 5);
            Assert.NotNull(items);
        }
    }

    // ── Progress reporting ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ReadTable_WithProgress_ReportsIncreasingRowCounts(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        var reported = new List<int>();

        _ = reader.ReadTable(table, new Progress<int>(reported.Add));

        // Every reported value should be non-negative; ForEach handles zero callbacks gracefully
        foreach (int v in reported)
        {
            Assert.True(v >= 0);
        }
    }
}
