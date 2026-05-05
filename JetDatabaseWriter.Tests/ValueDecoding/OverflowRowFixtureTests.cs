namespace JetDatabaseWriter.Tests.ValueDecoding;

using System.Data;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fixture-based tests for rows with overflow pointers using the Jackcess
/// <c>overflowTestV2010.accdb</c> fixture. The fixture's <c>Table1</c> has
/// 7 rows, 2 of which (rows 3 and 5 in Jackcess terms) use overflow
/// pointers (<c>0x4000</c>-flagged row offsets). The reader follows
/// overflow pointers in V2007+/ACE; older format variants (V2000/V2003)
/// may throw <c>JetLimitationException</c> due to deleted-column schema
/// gaps in the fixture. Covers §5 of
/// <c>docs/design/test-coverage-gaps.md</c>.
/// </summary>
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

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

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
    /// The overflow fixture across ACE format variants (V2007–V2010) opens
    /// and reads without throwing. V2000 and V2003 overflow fixtures have
    /// deleted-column schema gaps that trigger
    /// <c>JetLimitationException</c> — those are excluded.
    /// </summary>
    [Theory]
    [InlineData(nameof(TestDatabases.OverflowTestV2010))]
    [InlineData(nameof(TestDatabases.OverflowTestV2007))]
    public async Task OverflowFixture_AceFormats_ReadsWithoutThrowing(string fieldName)
    {
        string path = (string)typeof(TestDatabases)
            .GetField(fieldName)!
            .GetValue(null)!;

        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        foreach (string table in tables)
        {
            await foreach (object[] row in reader.Rows(
                table,
                cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
            }
        }
    }
}
