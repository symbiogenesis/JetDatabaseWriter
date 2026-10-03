namespace JetDatabaseWriter.Tests.Catalog;

using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Checks that the reader lists every user table an Access-authored file holds
/// and returns every live row of it. The rows here were hidden by the row-bound
/// rule: Access leaves deleted row-offset slots pointing at the same offset as
/// a live row, and the live row was given the bytes up to the next equal
/// offset, which is zero bytes, so it was skipped as undecodable.
/// </summary>
public sealed class CatalogCompletenessTests
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    /// <summary>
    /// Gets the Access-authored tables whose <c>MSysObjects</c> row shares its
    /// offset with deleted slots, with the number of rows each holds.
    /// </summary>
    public static TheoryData<string, string, int> TablesWithSharedOffsetCatalogRows => new()
    {
        { TestDatabases.IndexTestV1997, "Table3", 4 },
        { TestDatabases.IndexTestV2000, "Table3", 4 },
        { TestDatabases.IndexTestV2003, "Table3", 4 },
        { TestDatabases.IndexTestV2007, "Table3", 4 },
        { TestDatabases.IndexTestV2010, "Table3", 4 },
        { TestDatabases.CompIndexTestV2003, "Table1", 512 },
        { TestDatabases.LinkeeTest, "Table2", 1 },
        { TestDatabases.Test2V2003, "MSP_PROJECTS", 1 },
        { TestDatabases.Test2V2007, "MSP_PROJECTS", 1 },
        { TestDatabases.TestPromotionV2007, "jobDB1", 0 },
    };

    /// <summary>Gets the Jackcess <c>delTest</c> fixtures.</summary>
    public static TheoryData<string> DelTestFixtures =>
    [
        TestDatabases.DelTestV1997,
        TestDatabases.DelTestV2000,
        TestDatabases.DelTestV2003,
        TestDatabases.DelTestV2007,
        TestDatabases.DelTestV2010,
    ];

    /// <summary>
    /// A table whose catalog row shares its offset with deleted slots is listed,
    /// has its columns, and returns the rows its TDEF counts.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    /// <param name="tableName">The table whose catalog row shares an offset.</param>
    /// <param name="expectedRows">The number of rows the table holds.</param>
    [Theory]
    [MemberData(nameof(TablesWithSharedOffsetCatalogRows))]
    public async Task ListTables_RowSharingDeletedRowOffset_IsListed(string path, string tableName, int expectedRows)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: this.ct);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(this.ct);
        using DataTable data = await reader.ReadDataTableAsync(tableName, cancellationToken: this.ct);
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(this.ct);

        Assert.Contains(tableName, tables);
        Assert.True(data.Columns.Count > 0, $"'{tableName}' read with no columns.");
        Assert.Equal(expectedRows, data.Rows.Count);
        Assert.Equal(stats.Single(s => s.Name == tableName).RowCount, data.Rows.Count);
    }

    /// <summary>
    /// The <c>delTest</c> fixtures hold two live rows in <c>Table</c> next to
    /// deleted ones (Jackcess <c>DatabaseTest.testReadDeletedRows</c>). The second
    /// live row shares its offset with deleted slots, and every read API returns it.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    [Theory]
    [MemberData(nameof(DelTestFixtures))]
    public async Task ReadDeletedRowsFixture_ReturnsBothLiveRows(string path)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: this.ct);

        using DataTable data = await reader.ReadDataTableAsync("Table", cancellationToken: this.ct);
        int rows = await CountAsync(reader.Rows("Table", cancellationToken: this.ct));
        int stringRows = await CountAsync(reader.RowsAsStrings("Table", cancellationToken: this.ct));
        long realCount = await reader.GetRealRowCountAsync("Table", this.ct);
        int queryCount = reader.Query<EmptyRow>("Table").Count();

        Assert.Equal(2, data.Rows.Count);
        Assert.Equal(2, rows);
        Assert.Equal(2, stringRows);
        Assert.Equal(2, realCount);
        Assert.Equal(2, queryCount);
    }

    private static async ValueTask<int> CountAsync<T>(IAsyncEnumerable<T> source)
    {
        int count = 0;
        await foreach (T item in source)
        {
            _ = item;
            count++;
        }

        return count;
    }

    private sealed class EmptyRow;
}
