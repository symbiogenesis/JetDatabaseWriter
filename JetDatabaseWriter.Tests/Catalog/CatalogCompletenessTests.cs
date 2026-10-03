namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
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

    /// <summary>Gets fixtures whose catalogs hold overflow rows, with their table counts and some of those tables.</summary>
    public static TheoryData<string, int, string[]> OverflowCatalogFixtures => new()
    {
        { TestDatabases.NorthwindTraders, 28, ["Orders", "Employees", "Products", "PurchaseOrderStatus", "Welcome"] },
        { TestDatabases.MdbtoolsNwind, 9, ["Categories", "Customers", "Employees", "Suppliers"] },
        { TestDatabases.TestIndexCodesV2010, 30, [] },
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
    /// Gets every readable unencrypted fixture: the in-repo databases, the Jackcess tree
    /// and the mdbtools corpus.
    /// </summary>
    public static TheoryData<string> AllFixtures
    {
        get
        {
            var data = new TheoryData<string>();
            string[] others =
            [
                TestDatabases.NorthwindTraders,
                TestDatabases.AdventureWorks,
                TestDatabases.Jet3Test,
                TestDatabases.ComplexFields,
                TestDatabases.CompositeTextIndex,
                TestDatabases.MdbtoolsNwind,
                TestDatabases.MdbtoolsASampleDatabase,
                TestDatabases.MdbtoolsDateTestDatabase,
            ];
            foreach (string path in others.Concat(TestDatabases.AllJackcessDatabases).Where(TestDatabases.IsReadable))
            {
                data.Add(path);
            }

            return data;
        }
    }

    /// <summary>
    /// The reader and the writer list exactly the user tables an independent walk of
    /// the <c>MSysObjects</c> pages finds (<see cref="RawCatalogWalker"/>), and every
    /// listed table resolves to its columns. Catalog rows Access moved to another page
    /// (overflow rows) and rows sharing an offset with deleted slots used to be missed.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    [Theory]
    [MemberData(nameof(AllFixtures))]
    public async Task ListTables_MatchesEveryUserTableRowInMSysObjects(string path)
    {
        SortedSet<string> expected;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(path, cancellationToken: this.ct))
        {
            expected = await RawCatalogWalker.GetUserTableNamesAsync(harness, this.ct);
        }

        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: this.ct);
        IReadOnlyList<string> listed = await reader.ListTablesAsync(this.ct);
        Assert.Equal(expected, listed.Order(StringComparer.Ordinal));

        await using var copy = new MemoryStream(await File.ReadAllBytesAsync(path, this.ct));
        await using (WriterHarness writer = await WriterHarness.OpenAsync(copy, cancellationToken: this.ct))
        {
            List<CatalogEntry> writerTables = await writer.Services.Catalog.GetUserTablesAsync(this.ct);
            Assert.Equal(expected, writerTables.Select(e => e.Name).Order(StringComparer.Ordinal));
        }

        foreach (string table in listed)
        {
            using DataTable schema = await reader.ReadDataTableAsync(table, maxRows: 0, cancellationToken: this.ct);
            Assert.True(schema.Columns.Count > 0, $"'{table}' read with no columns.");
        }
    }

    /// <summary>
    /// Pins the table lists of fixtures whose catalogs hold overflow rows: tables such
    /// as Orders and Products in NorthwindTraders.accdb and Customers in the Jet3
    /// nwind.mdb were missing.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    /// <param name="expectedCount">The number of user tables.</param>
    /// <param name="mustInclude">Tables whose catalog rows are overflow rows.</param>
    [Theory]
    [MemberData(nameof(OverflowCatalogFixtures))]
    public async Task ListTables_TablesWithOverflowCatalogRows_AreListed(string path, int expectedCount, string[] mustInclude)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: this.ct);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(this.ct);

        Assert.Equal(expectedCount, tables.Count);
        Assert.All(mustInclude, name => Assert.Contains(name, tables));
    }

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
