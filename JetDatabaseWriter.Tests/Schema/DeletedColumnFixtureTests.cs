namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper

/// <summary>
/// Fixture-based tests for databases with deleted columns using the Jackcess
/// <c>delColTestV*.mdb/.accdb</c> fixtures. These databases have table
/// definitions that include deleted-column slots (marked with
/// <c>col_type = 0x00</c> and a null/empty name in the TDEF), which must
/// be skipped during schema enumeration and row decode. A bug here can
/// silently shift column values or crash on fixed-offset miscalculation.
///
/// <para>Jackcess analogue: <c>DatabaseTest.testDeleteColumn</c>.
/// </para>
/// </summary>
public sealed class DeletedColumnFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The fixture lists at least one user table without throwing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_ListTables_ReturnsNonEmpty(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// Every table reports columns via metadata — deleted-column slots must
    /// be excluded from the public schema.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_AllTables_HaveVisibleColumns(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> cols =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.NotEmpty(cols);
        }
    }

    /// <summary>
    /// No column exposed by the reader has a blank or null name (which would
    /// indicate a deleted-column slot leaking through).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_NoColumnHasBlankName(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> cols =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            foreach (ColumnMetadata col in cols)
            {
                Assert.False(
                    string.IsNullOrWhiteSpace(col.Name),
                    $"Table '{table}' in '{path}' exposed a column with blank/null name (deleted slot leak).");
            }
        }
    }

    /// <summary>
    /// Tables with deleted-column gaps read successfully even when stale rows
    /// reference the pre-deletion column count. The surviving columns' absolute
    /// offsets (ColNum, FixedOff, VarIdx) are stable across deletions.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_RowDecode_SucceedsForAllFixtures(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            DataTable dt = await reader.ReadDataTableAsync(
                table, cancellationToken: TestContext.Current.CancellationToken);
            Assert.NotNull(dt);
        }
    }

    /// <summary>
    /// For all deleted-column fixtures, the DataTable column count matches the
    /// metadata column count — ensures the schema and data paths agree on
    /// deleted-column exclusion.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_ReadableTables_DataTableColumnCount_MatchesMetadata(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> meta =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

            DataTable dt = await reader.ReadDataTableAsync(
                table, maxRows: 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(meta.Count, dt.Columns.Count);
        }
    }

    /// <summary>
    /// The <c>Rows()</c> async enumerable path (<c>CrackRowTypedAsync</c>)
    /// decodes stale rows without throwing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_RowsEnumerable_DecodesAllRows(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            int count = 0;
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
                count++;
            }

            Assert.True(count > 0, $"Table '{table}' in '{path}' produced zero rows via Rows().");
        }
    }

    /// <summary>
    /// The <c>RowsAsStrings()</c> path exercises <c>CrackRowAsync</c> which
    /// has its own deleted-column guard.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_RowsAsStrings_DecodesAllRows(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            int count = 0;
            await foreach (string[] row in reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
                count++;
            }

            Assert.True(count > 0, $"Table '{table}' in '{path}' produced zero rows via RowsAsStrings().");
        }
    }

    /// <summary>
    /// The <c>ReadTableAsStringsAsync</c> path exercises <c>CrackRowAsync</c>
    /// via a DataTable return. The column count must match metadata.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_ReadTableAsStrings_ColumnCountMatchesMetadata(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> meta =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

            DataTable dt = await reader.ReadTableAsStringsAsync(
                table, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(meta.Count, dt.Columns.Count);
            Assert.True(dt.Rows.Count > 0, $"Table '{table}' in '{path}' produced zero rows via ReadTableAsStringsAsync.");
        }
    }

    /// <summary>
    /// All read paths produce the same row count — verifies no path silently
    /// drops stale rows that another path accepts.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_AllPaths_ProduceSameRowCount(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            DataTable dt = await reader.ReadDataTableAsync(
                table, cancellationToken: TestContext.Current.CancellationToken);

            int rowsCount = 0;
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                _ = row;
                rowsCount++;
            }

            int stringsCount = 0;
            await foreach (string[] row in reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                _ = row;
                stringsCount++;
            }

            Assert.Equal(dt.Rows.Count, rowsCount);
            Assert.Equal(dt.Rows.Count, stringsCount);
        }
    }

    /// <summary>
    /// Values decoded via <c>Rows()</c> match those from <c>ReadDataTableAsync</c>,
    /// verifying no column misalignment from the stale-row fix.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_Rows_ValuesMatchDataTable(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            DataTable dt = await reader.ReadDataTableAsync(
                table, cancellationToken: TestContext.Current.CancellationToken);

            var rowsList = new List<object[]>();
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                rowsList.Add(row);
            }

            Assert.Equal(dt.Rows.Count, rowsList.Count);

            for (int r = 0; r < dt.Rows.Count; r++)
            {
                DataRow dr = dt.Rows[r];
                object[] row = rowsList[r];
                for (int c = 0; c < dt.Columns.Count; c++)
                {
                    object expected = dr[c];
                    object actual = row[c];

                    // Both DBNull and null represent "no value" depending on path.
                    if (expected is DBNull)
                    {
                        Assert.True(
                            actual is DBNull || actual is null,
                            $"Row {r} col {c} in table '{table}': expected DBNull but got '{actual}'.");
                    }
                    else
                    {
                        Assert.Equal(expected, actual);
                    }
                }
            }
        }
    }

    /// <summary>
    /// The <c>ReadTableAsync&lt;T&gt;</c> full-map path exercises
    /// <c>CrackMappedRowAsync</c> with a POCO that binds all columns.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_ReadTableAsyncFullMap_DecodesAllRows(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<DelColFullRow> rows = await reader.ReadTableAsync<DelColFullRow>(
            "Table1", cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        Assert.All(rows, row => Assert.NotNull(row.Data));
    }

    /// <summary>
    /// The <c>ReadTableAsync&lt;T&gt;</c> projected path exercises
    /// <c>CrackProjectedRowAsync</c> with a POCO that binds a subset of columns.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_ReadTableAsyncProjected_DecodesAllRows(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<DelColProjectedRow> rows = await reader.ReadTableAsync<DelColProjectedRow>(
            "Table1", cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        Assert.All(rows, row => Assert.NotNull(row.Data));
    }

    /// <summary>
    /// The typed <c>Rows&lt;T&gt;</c> path exercises the streaming generic
    /// enumerable over deleted-column fixtures.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_GenericRowsEnumerable_DecodesAllRows(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        int count = 0;
        await foreach (DelColFullRow row in reader.Rows<DelColFullRow>("Table1", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.NotNull(row.Data);
            count++;
        }

        Assert.True(count > 0, $"Rows<T> produced zero rows for '{path}'.");
    }

    /// <summary>Full-map POCO matching all 4 visible columns in delColTest Table1.</summary>
    private sealed class DelColFullRow
    {
        public int Id { get; set; }

        public int Id2 { get; set; }

        public string Data { get; set; } = string.Empty;

        public string Data2 { get; set; } = string.Empty;
    }

    /// <summary>Projected POCO binding only a subset of Table1 columns.</summary>
    private sealed class DelColProjectedRow
    {
        public int Id { get; set; }

        public string Data { get; set; } = string.Empty;
    }
}
