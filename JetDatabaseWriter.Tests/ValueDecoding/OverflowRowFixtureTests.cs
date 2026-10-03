namespace JetDatabaseWriter.Tests.ValueDecoding;

using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fixture-based tests for rows with overflow pointers using the Jackcess
/// <c>overflowTest</c> fixtures. Each fixture's <c>Table1</c> has 7 rows. In
/// the V2000 and V2003 files, rows 3 and 5 (in Jackcess terms) are overflow
/// rows: their slots are flagged <c>0x4000</c> and hold a pointer to the row
/// data on another slot, which the reader follows. <see cref="Pages.OverflowRowTests"/>
/// checks the rows' values on every version.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class OverflowRowFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The fixture can be opened and its table listed without throwing.
    /// </summary>
    [Fact]
    public async Task OverflowTestV2010_OpensAndListsTable()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.OverflowTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Contains("Table1", tables);
    }

    /// <summary>
    /// Reading <c>Table1</c> completes without throwing. The Jackcess fixture
    /// has 7 rows total; the V2010 reader is able to read all of them.
    /// </summary>
    [Fact]
    public async Task Table1_ReadsAllRows_WithoutThrowing()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.OverflowTestV2010,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(7, dt.Rows.Count);
    }

    /// <summary>
    /// The rows that are read have valid (non-null) row arrays
    /// and at least one column.
    /// </summary>
    [Fact]
    public async Task Table1_AllRows_HaveValidData()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.OverflowTestV2010,
            TestContext.Current.CancellationToken);

        int rowCount = 0;
        await foreach (object[] row in reader.Rows(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.NotNull(row);
            Assert.True(row.Length > 0, "Each row should have at least one column.");
            rowCount++;
        }

        Assert.True(rowCount >= 1, "Should stream at least 1 row.");
    }

    /// <summary>
    /// The overflow fixture of every format variant opens and reads all 7 rows
    /// of <c>Table1</c>, including the overflow rows of V2000 and V2003.
    /// </summary>
    /// <param name="fieldName">The field name.</param>
    [Theory]
    [InlineData(nameof(TestDatabases.OverflowTestV1997))]
    [InlineData(nameof(TestDatabases.OverflowTestV2000))]
    [InlineData(nameof(TestDatabases.OverflowTestV2003))]
    [InlineData(nameof(TestDatabases.OverflowTestV2007))]
    [InlineData(nameof(TestDatabases.OverflowTestV2010))]
    public async Task OverflowFixture_EveryFormat_ReadsAllRows(string fieldName)
    {
        string path = (string)typeof(TestDatabases)
            .GetField(fieldName)!
            .GetValue(null)!;

        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains("Table1", tables);

        foreach (string table in tables)
        {
            int rows = 0;
            await foreach (object[] row in reader.Rows(
                table,
                cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
                rows++;
            }

            if (table == "Table1")
            {
                Assert.Equal(7, rows);
            }
        }
    }
}
