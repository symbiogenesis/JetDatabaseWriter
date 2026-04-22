namespace JetDatabaseReader.Tests;

using System.Data;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Integration tests for ReadDataTableAsync and ReadTableAsStringsAsync.
/// </summary>
public class DataTableIntegrationTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── ReadDataTableAsync ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_ReturnsNonNullWithColumns(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable? dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.True(dt.Columns.Count > 0);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_MaxRows_LimitsRowCount(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        const int max = 5;

        DataTable? dt = await reader.ReadDataTableAsync(table, max, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count <= max);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_ColumnMetadata_MatchesGetColumnMetadata(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable? dt = await reader.ReadDataTableAsync(table, 1, cancellationToken: TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.Equal(meta.Count, dt.Columns.Count);
        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(meta[i].Name, dt.Columns[i].ColumnName);
            Assert.Equal(meta[i].ClrType, dt.Columns[i].DataType);
        }
    }

    // ── ReadTableAsStringsAsync returns DataTable — integration ──────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsStrings_AllColumnsAreStringType(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = await reader.ReadTableAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(dt.Columns.Count > 0);
        foreach (DataColumn col in dt.Columns)
        {
            Assert.Equal(typeof(string), col.DataType);
        }
    }
}
