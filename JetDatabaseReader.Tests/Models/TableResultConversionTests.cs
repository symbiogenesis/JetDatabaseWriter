namespace JetDatabaseReader.Tests;

using System;
using System.Data;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Tests for <see cref="TableResult.ToDataTable"/> and <see cref="StringTableResult.ToDataTable"/>.
/// </summary>
public class TableResultConversionTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── TableResult.ToDataTable — null / empty guards ─────────────────

    [Fact]
    public void TableResult_ToDataTable_WhenHeadersIsNull_ReturnsEmptyDataTable()
    {
        var result = new TableResult();

        DataTable dt = result.ToDataTable();

        Assert.NotNull(dt);
        Assert.Empty(dt.Columns);
        Assert.Equal(0, dt.Rows.Count);
    }

    [Fact]
    public void TableResult_ToDataTable_WhenRowsIsNull_ReturnsColumnsWithNoRows()
    {
        var result = new TableResult
        {
            Headers = ["Id", "Name"],
            Schema =
            [
                new() { Name = "Id",   Type = typeof(int) },
                new() { Name = "Name", Type = typeof(string) },
            ],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal(2, dt.Columns.Count);
        Assert.Equal(0, dt.Rows.Count);
    }

    // ── TableResult.ToDataTable — structural mapping ──────────────────

    [Fact]
    public void TableResult_ToDataTable_TableName_MatchesResultTableName()
    {
        var result = new TableResult { TableName = "Orders", Headers = [] };

        DataTable dt = result.ToDataTable();

        Assert.Equal("Orders", dt.TableName);
    }

    [Fact]
    public void TableResult_ToDataTable_ColumnNames_MatchHeaders()
    {
        var result = new TableResult
        {
            Headers = ["Id", "Name", "Amount"],
            Rows = [],
        };

        DataTable dt = result.ToDataTable();

        for (int i = 0; i < result.Headers.Count; i++)
        {
            Assert.Equal(result.Headers[i], dt.Columns[i].ColumnName);
        }
    }

    [Fact]
    public void TableResult_ToDataTable_ColumnTypes_MatchSchema()
    {
        var result = new TableResult
        {
            Headers = ["Id", "CreatedAt", "Total"],
            Schema =
            [
                new() { Name = "Id",        Type = typeof(int) },
                new() { Name = "CreatedAt", Type = typeof(DateTime) },
                new() { Name = "Total",     Type = typeof(decimal) },
            ],
            Rows = [],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal(typeof(int), dt.Columns["Id"]!.DataType);
        Assert.Equal(typeof(DateTime), dt.Columns["CreatedAt"]!.DataType);
        Assert.Equal(typeof(decimal), dt.Columns["Total"]!.DataType);
    }

    [Fact]
    public void TableResult_ToDataTable_WhenSchemaIsNull_ColumnType_IsObject()
    {
        var result = new TableResult
        {
            Headers = ["Col1"],
            Rows = [],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal(typeof(object), dt.Columns["Col1"]!.DataType);
    }

    // ── TableResult.ToDataTable — row data ────────────────────────────

    [Fact]
    public void TableResult_ToDataTable_RowCount_MatchesRowsCount()
    {
        var result = new TableResult
        {
            Headers = ["Id"],
            Schema = [new() { Name = "Id", Type = typeof(int) }],
            Rows = [new object[] { 1 }, new object[] { 2 }, new object[] { 3 }],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal(3, dt.Rows.Count);
    }

    [Fact]
    public void TableResult_ToDataTable_RowValues_ArePreserved()
    {
        var result = new TableResult
        {
            Headers = ["Id", "Name"],
            Schema =
            [
                new() { Name = "Id",   Type = typeof(int) },
                new() { Name = "Name", Type = typeof(string) },
            ],
            Rows = [new object[] { 42, "Alice" }],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal(42, dt.Rows[0]["Id"]);
        Assert.Equal("Alice", dt.Rows[0]["Name"]);
    }

    [Fact]
    public void TableResult_ToDataTable_NullValues_AreStoredAsDBNull()
    {
        var result = new TableResult
        {
            Headers = ["Name"],
            Schema = [new() { Name = "Name", Type = typeof(string) }],
            Rows = [new object[] { null! }],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal(DBNull.Value, dt.Rows[0]["Name"]);
    }

    // ── TableResult.ToDataTable — integration ─────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task TableResult_ToDataTable_ColumnCount_MatchesHeaders(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableResult result = await reader.ReadTableAsync((await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0], 50, TestContext.Current.CancellationToken);

        DataTable dt = result.ToDataTable();

        Assert.Equal(result.Headers.Count, dt.Columns.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task TableResult_ToDataTable_RowCount_MatchesResultRowCount(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableResult result = await reader.ReadTableAsync((await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0], 50, TestContext.Current.CancellationToken);

        DataTable dt = result.ToDataTable();

        Assert.Equal(result.RowCount, dt.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task TableResult_ToDataTable_ColumnTypes_MatchSchemaTypes(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableResult result = await reader.ReadTableAsync((await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0], 1, TestContext.Current.CancellationToken);

        DataTable dt = result.ToDataTable();

        for (int i = 0; i < result.Schema.Count; i++)
        {
            Assert.Equal(result.Schema[i].Type, dt.Columns[i].DataType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task TableResult_ToDataTable_ColumnLayout_MatchesDirectReadTable(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable direct = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        DataTable converted = (await reader.ReadTableAsync(table, int.MaxValue, TestContext.Current.CancellationToken)).ToDataTable();

        Assert.Equal(direct.Columns.Count, converted.Columns.Count);
        for (int i = 0; i < direct.Columns.Count; i++)
        {
            Assert.Equal(direct.Columns[i].ColumnName, converted.Columns[i].ColumnName);
            Assert.Equal(direct.Columns[i].DataType, converted.Columns[i].DataType);
        }
    }

    // ── StringTableResult.ToDataTable — null / empty guards ───────────

    [Fact]
    public void StringTableResult_ToDataTable_WhenHeadersIsNull_ReturnsEmptyDataTable()
    {
        var result = new StringTableResult();

        DataTable dt = result.ToDataTable();

        Assert.NotNull(dt);
        Assert.Empty(dt.Columns);
        Assert.Equal(0, dt.Rows.Count);
    }

    [Fact]
    public void StringTableResult_ToDataTable_WhenRowsIsNull_ReturnsColumnsWithNoRows()
    {
        var result = new StringTableResult { Headers = ["Id", "Name"] };

        DataTable dt = result.ToDataTable();

        Assert.Equal(2, dt.Columns.Count);
        Assert.Equal(0, dt.Rows.Count);
    }

    // ── StringTableResult.ToDataTable — structural mapping ────────────

    [Fact]
    public void StringTableResult_ToDataTable_TableName_MatchesResultTableName()
    {
        var result = new StringTableResult { TableName = "Customers", Headers = [] };

        DataTable dt = result.ToDataTable();

        Assert.Equal("Customers", dt.TableName);
    }

    [Fact]
    public void StringTableResult_ToDataTable_AllColumns_AreStringType()
    {
        var result = new StringTableResult
        {
            Headers = ["Id", "Name", "Date"],
            Rows = [["1", "Alice", "2024-01-01"]],
        };

        DataTable dt = result.ToDataTable();

        foreach (DataColumn col in dt.Columns)
        {
            Assert.Equal(typeof(string), col.DataType);
        }
    }

    [Fact]
    public void StringTableResult_ToDataTable_ColumnNames_MatchHeaders()
    {
        var result = new StringTableResult
        {
            Headers = ["Id", "Name", "Amount"],
            Rows = [],
        };

        DataTable dt = result.ToDataTable();

        for (int i = 0; i < result.Headers.Count; i++)
        {
            Assert.Equal(result.Headers[i], dt.Columns[i].ColumnName);
        }
    }

    // ── StringTableResult.ToDataTable — row data ──────────────────────

    [Fact]
    public void StringTableResult_ToDataTable_RowCount_MatchesRowsCount()
    {
        var result = new StringTableResult
        {
            Headers = ["Id"],
            Rows = [["1"], ["2"]],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal(2, dt.Rows.Count);
    }

    [Fact]
    public void StringTableResult_ToDataTable_RowValues_ArePreserved()
    {
        var result = new StringTableResult
        {
            Headers = ["Id", "Name"],
            Rows = [["7", "Bob"]],
        };

        DataTable dt = result.ToDataTable();

        Assert.Equal("7", dt.Rows[0]["Id"]);
        Assert.Equal("Bob", dt.Rows[0]["Name"]);
    }

    // ── StringTableResult.ToDataTable — integration ───────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StringTableResult_ToDataTable_ColumnCount_MatchesHeaders(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        StringTableResult result = await reader.ReadTableAsStringsAsync((await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0], 50, TestContext.Current.CancellationToken);

        DataTable dt = result.ToDataTable();

        Assert.Equal(result.Headers.Count, dt.Columns.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StringTableResult_ToDataTable_RowCount_MatchesResultRowCount(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        StringTableResult result = await reader.ReadTableAsStringsAsync((await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0], 50, TestContext.Current.CancellationToken);

        DataTable dt = result.ToDataTable();

        Assert.Equal(result.RowCount, dt.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StringTableResult_ToDataTable_ColumnLayout_MatchesReadTableAsStringDataTable(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable direct = (await reader.ReadTableAsStringDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        DataTable converted = (await reader.ReadTableAsStringsAsync(table, int.MaxValue, TestContext.Current.CancellationToken)).ToDataTable();

        Assert.Equal(direct.Columns.Count, converted.Columns.Count);
        for (int i = 0; i < direct.Columns.Count; i++)
        {
            Assert.Equal(direct.Columns[i].ColumnName, converted.Columns[i].ColumnName);
        }
    }
}
