using System;
using System.Collections.Generic;
using System.Data;
using FluentAssertions;
using Xunit;

namespace JetDatabaseReader.Tests
{
    /// <summary>
    /// Tests for <see cref="TableResult.ToDataTable"/> and <see cref="StringTableResult.ToDataTable"/>.
    /// </summary>
    public class TableResultConversionTests
    {
        // ── TableResult.ToDataTable — null / empty guards ─────────────────

        [Fact]
        public void TableResult_ToDataTable_WhenHeadersIsNull_ReturnsEmptyDataTable()
        {
            var result = new TableResult();

            DataTable dt = result.ToDataTable();

            dt.Should().NotBeNull();
            dt.Columns.Should().BeEmpty();
            dt.Rows.Should().BeEmpty();
        }

        [Fact]
        public void TableResult_ToDataTable_WhenRowsIsNull_ReturnsColumnsWithNoRows()
        {
            var result = new TableResult
            {
                Headers = new List<string> { "Id", "Name" },
                Schema  = new List<TableColumn>
                {
                    new TableColumn { Name = "Id",   Type = typeof(int)    },
                    new TableColumn { Name = "Name", Type = typeof(string) }
                }
            };

            DataTable dt = result.ToDataTable();

            dt.Columns.Should().HaveCount(2);
            dt.Rows.Should().BeEmpty();
        }

        // ── TableResult.ToDataTable — structural mapping ──────────────────

        [Fact]
        public void TableResult_ToDataTable_TableName_MatchesResultTableName()
        {
            var result = new TableResult { TableName = "Orders", Headers = new List<string>() };

            DataTable dt = result.ToDataTable();

            dt.TableName.Should().Be("Orders");
        }

        [Fact]
        public void TableResult_ToDataTable_ColumnNames_MatchHeaders()
        {
            var result = new TableResult
            {
                Headers = new List<string> { "Id", "Name", "Amount" },
                Rows    = new List<object[]>()
            };

            DataTable dt = result.ToDataTable();

            for (int i = 0; i < result.Headers.Count; i++)
                dt.Columns[i].ColumnName.Should().Be(result.Headers[i]);
        }

        [Fact]
        public void TableResult_ToDataTable_ColumnTypes_MatchSchema()
        {
            var result = new TableResult
            {
                Headers = new List<string> { "Id", "CreatedAt", "Total" },
                Schema  = new List<TableColumn>
                {
                    new TableColumn { Name = "Id",        Type = typeof(int)      },
                    new TableColumn { Name = "CreatedAt", Type = typeof(DateTime) },
                    new TableColumn { Name = "Total",     Type = typeof(decimal)  }
                },
                Rows = new List<object[]>()
            };

            DataTable dt = result.ToDataTable();

            dt.Columns["Id"].DataType.Should().Be(typeof(int));
            dt.Columns["CreatedAt"].DataType.Should().Be(typeof(DateTime));
            dt.Columns["Total"].DataType.Should().Be(typeof(decimal));
        }

        [Fact]
        public void TableResult_ToDataTable_WhenSchemaIsNull_ColumnType_IsObject()
        {
            var result = new TableResult
            {
                Headers = new List<string> { "Col1" },
                Rows    = new List<object[]>()
            };

            DataTable dt = result.ToDataTable();

            dt.Columns["Col1"].DataType.Should().Be(typeof(object));
        }

        // ── TableResult.ToDataTable — row data ────────────────────────────

        [Fact]
        public void TableResult_ToDataTable_RowCount_MatchesRowsCount()
        {
            var result = new TableResult
            {
                Headers = new List<string> { "Id" },
                Schema  = new List<TableColumn> { new TableColumn { Name = "Id", Type = typeof(int) } },
                Rows    = new List<object[]> { new object[] { 1 }, new object[] { 2 }, new object[] { 3 } }
            };

            DataTable dt = result.ToDataTable();

            dt.Rows.Count.Should().Be(3);
        }

        [Fact]
        public void TableResult_ToDataTable_RowValues_ArePreserved()
        {
            var result = new TableResult
            {
                Headers = new List<string> { "Id", "Name" },
                Schema  = new List<TableColumn>
                {
                    new TableColumn { Name = "Id",   Type = typeof(int)    },
                    new TableColumn { Name = "Name", Type = typeof(string) }
                },
                Rows = new List<object[]> { new object[] { 42, "Alice" } }
            };

            DataTable dt = result.ToDataTable();

            dt.Rows[0]["Id"].Should().Be(42);
            dt.Rows[0]["Name"].Should().Be("Alice");
        }

        [Fact]
        public void TableResult_ToDataTable_NullValues_AreStoredAsDBNull()
        {
            var result = new TableResult
            {
                Headers = new List<string> { "Name" },
                Schema  = new List<TableColumn> { new TableColumn { Name = "Name", Type = typeof(string) } },
                Rows    = new List<object[]> { new object[] { null } }
            };

            DataTable dt = result.ToDataTable();

            dt.Rows[0]["Name"].Should().Be(DBNull.Value);
        }

        // ── TableResult.ToDataTable — integration ─────────────────────────

        [Theory]
        [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
        public void TableResult_ToDataTable_ColumnCount_MatchesHeaders(string path)
        {
            using var reader = TestDatabases.Open(path);
            TableResult result = reader.ReadTable(reader.ListTables()[0], maxRows: 50);

            DataTable dt = result.ToDataTable();

            dt.Columns.Count.Should().Be(result.Headers.Count);
        }

        [Theory]
        [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
        public void TableResult_ToDataTable_RowCount_MatchesResultRowCount(string path)
        {
            using var reader = TestDatabases.Open(path);
            TableResult result = reader.ReadTable(reader.ListTables()[0], maxRows: 50);

            DataTable dt = result.ToDataTable();

            dt.Rows.Count.Should().Be(result.RowCount);
        }

        [Theory]
        [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
        public void TableResult_ToDataTable_ColumnTypes_MatchSchemaTypes(string path)
        {
            using var reader = TestDatabases.Open(path);
            TableResult result = reader.ReadTable(reader.ListTables()[0], maxRows: 1);

            DataTable dt = result.ToDataTable();

            for (int i = 0; i < result.Schema.Count; i++)
                dt.Columns[i].DataType.Should().Be(result.Schema[i].Type,
                    because: $"column '{result.Headers[i]}' should use its schema CLR type");
        }

        [Theory]
        [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
        public void TableResult_ToDataTable_ColumnLayout_MatchesDirectReadTable(string path)
        {
            using var reader = TestDatabases.Open(path);
            string table = reader.ListTables()[0];

            DataTable direct    = reader.ReadTable(table);
            DataTable converted = reader.ReadTable(table, maxRows: int.MaxValue).ToDataTable();

            converted.Columns.Count.Should().Be(direct.Columns.Count);
            for (int i = 0; i < direct.Columns.Count; i++)
            {
                converted.Columns[i].ColumnName.Should().Be(direct.Columns[i].ColumnName,
                    because: $"column {i} name should match");
                converted.Columns[i].DataType.Should().Be(direct.Columns[i].DataType,
                    because: $"column '{direct.Columns[i].ColumnName}' type should match");
            }
        }

        // ── StringTableResult.ToDataTable — null / empty guards ───────────

        [Fact]
        public void StringTableResult_ToDataTable_WhenHeadersIsNull_ReturnsEmptyDataTable()
        {
            var result = new StringTableResult();

            DataTable dt = result.ToDataTable();

            dt.Should().NotBeNull();
            dt.Columns.Should().BeEmpty();
            dt.Rows.Should().BeEmpty();
        }

        [Fact]
        public void StringTableResult_ToDataTable_WhenRowsIsNull_ReturnsColumnsWithNoRows()
        {
            var result = new StringTableResult { Headers = new List<string> { "Id", "Name" } };

            DataTable dt = result.ToDataTable();

            dt.Columns.Should().HaveCount(2);
            dt.Rows.Should().BeEmpty();
        }

        // ── StringTableResult.ToDataTable — structural mapping ────────────

        [Fact]
        public void StringTableResult_ToDataTable_TableName_MatchesResultTableName()
        {
            var result = new StringTableResult { TableName = "Customers", Headers = new List<string>() };

            DataTable dt = result.ToDataTable();

            dt.TableName.Should().Be("Customers");
        }

        [Fact]
        public void StringTableResult_ToDataTable_AllColumns_AreStringType()
        {
            var result = new StringTableResult
            {
                Headers = new List<string> { "Id", "Name", "Date" },
                Rows    = new List<List<string>> { new List<string> { "1", "Alice", "2024-01-01" } }
            };

            DataTable dt = result.ToDataTable();

            foreach (DataColumn col in dt.Columns)
                col.DataType.Should().Be(typeof(string),
                    because: "StringTableResult.ToDataTable should produce only string columns");
        }

        [Fact]
        public void StringTableResult_ToDataTable_ColumnNames_MatchHeaders()
        {
            var result = new StringTableResult
            {
                Headers = new List<string> { "Id", "Name", "Amount" },
                Rows    = new List<List<string>>()
            };

            DataTable dt = result.ToDataTable();

            for (int i = 0; i < result.Headers.Count; i++)
                dt.Columns[i].ColumnName.Should().Be(result.Headers[i]);
        }

        // ── StringTableResult.ToDataTable — row data ──────────────────────

        [Fact]
        public void StringTableResult_ToDataTable_RowCount_MatchesRowsCount()
        {
            var result = new StringTableResult
            {
                Headers = new List<string> { "Id" },
                Rows    = new List<List<string>> { new List<string> { "1" }, new List<string> { "2" } }
            };

            DataTable dt = result.ToDataTable();

            dt.Rows.Count.Should().Be(2);
        }

        [Fact]
        public void StringTableResult_ToDataTable_RowValues_ArePreserved()
        {
            var result = new StringTableResult
            {
                Headers = new List<string> { "Id", "Name" },
                Rows    = new List<List<string>> { new List<string> { "7", "Bob" } }
            };

            DataTable dt = result.ToDataTable();

            dt.Rows[0]["Id"].Should().Be("7");
            dt.Rows[0]["Name"].Should().Be("Bob");
        }

        // ── StringTableResult.ToDataTable — integration ───────────────────

        [Theory]
        [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
        public void StringTableResult_ToDataTable_ColumnCount_MatchesHeaders(string path)
        {
            using var reader = TestDatabases.Open(path);
            StringTableResult result = reader.ReadTableAsStrings(reader.ListTables()[0], maxRows: 50);

            DataTable dt = result.ToDataTable();

            dt.Columns.Count.Should().Be(result.Headers.Count);
        }

        [Theory]
        [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
        public void StringTableResult_ToDataTable_RowCount_MatchesResultRowCount(string path)
        {
            using var reader = TestDatabases.Open(path);
            StringTableResult result = reader.ReadTableAsStrings(reader.ListTables()[0], maxRows: 50);

            DataTable dt = result.ToDataTable();

            dt.Rows.Count.Should().Be(result.RowCount);
        }

        [Theory]
        [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
        public void StringTableResult_ToDataTable_ColumnLayout_MatchesReadTableAsStringDataTable(string path)
        {
            using var reader = TestDatabases.Open(path);
            string table = reader.ListTables()[0];

            DataTable direct    = reader.ReadTableAsStringDataTable(table);
            DataTable converted = reader.ReadTableAsStrings(table, maxRows: int.MaxValue).ToDataTable();

            converted.Columns.Count.Should().Be(direct.Columns.Count);
            for (int i = 0; i < direct.Columns.Count; i++)
                converted.Columns[i].ColumnName.Should().Be(direct.Columns[i].ColumnName,
                    because: $"column {i} name should match ReadTableAsStringDataTable");
        }
    }
}
