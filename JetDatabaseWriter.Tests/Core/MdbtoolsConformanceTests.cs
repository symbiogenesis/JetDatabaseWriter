namespace JetDatabaseWriter.Tests;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Cross-implementation conformance tests against the upstream mdbtools fixture
/// corpus (https://github.com/mdbtools/mdbtestdata/tree/master/data).
///
/// Each test mirrors something mdbtools' own test scripts assert against the
/// same files:
///   * test_script.sh — runs every mdb-* utility against nwind.mdb and
///                      ASampleDatabase.accdb, including the German-named
///                      "Umsätze" table and the "Asset Items" table.
///                      https://github.com/mdbtools/mdbtools/blob/dev/test_script.sh
///   * test_sql.sh    — runs three SELECTs against nwind.mdb's Customers table
///                      via mdb-sql.
///                      https://github.com/mdbtools/mdbtools/blob/dev/test_sql.sh
///                      https://github.com/mdbtools/mdbtestdata/blob/master/sql/nwind.sql
///
/// Tests are skipped (not failed) when the fixture is absent so the suite still
/// runs in environments where the optional mdbtools/ directory has not been
/// populated.
/// </summary>
public sealed class MdbtoolsConformanceTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── nwind.mdb (Jet4 / Access 2000 .mdb) ───────────────────────────────────

    /// <summary>Mirror of <c>mdb-tables nwind.mdb</c> from mdbtools' test_script.sh.</summary>
    /// <remarks>
    /// The mdbtools nwind.mdb fixture is a stripped-down Northwind that retains
    /// only "Order Details", "Orders", "Products", "Shippers" and "Umsätze"
    /// as user tables (verified by hand-decoding MSysObjects). Names like
    /// "Customers"/"Employees"/"Suppliers" appear only as Forms/Macros there.
    /// </remarks>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_ListTables_IncludesOrdersAndUmsätze()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
        Assert.Contains("Orders", tables);
        Assert.Contains("Umsätze", tables);
    }

    /// <summary>Mirror of <c>mdb-count nwind.mdb "Umsätze"</c> from mdbtools' test_script.sh.</summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_Umsätze_HasRows()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Umsätze", TestContext.Current.CancellationToken);

        Assert.True(count > 0, "Umsätze should have at least one row.");
    }

    /// <summary>Mirror of <c>mdb-json nwind.mdb "Umsätze"</c> from mdbtools' test_script.sh — every row decodes without throwing.</summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_Umsätze_StreamsToCompletion()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync("Umsätze", TestContext.Current.CancellationToken);
        int expected = meta.Count;

        long observed = 0;
        await foreach (object[] row in reader.Rows("Umsätze", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(expected, row.Length);
            observed++;
        }

        long reported = await reader.GetRealRowCountAsync("Umsätze", TestContext.Current.CancellationToken);
        Assert.Equal(reported, observed);
    }

    /// <summary>
    /// Mirror of <c>select * from Orders LIMIT 10</c> — the reader returns at
    /// least 10 rows via the LINQ surface. (mdbtestdata/sql/nwind.sql targets
    /// Customers, but the bundled nwind.mdb has Customers only as Forms; Orders
    /// is the closest analogue.)
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_Orders_TakeTen_ReturnsTenRows()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        var rows = await reader.Rows("Orders", cancellationToken: TestContext.Current.CancellationToken)
            .Take(10)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(10, rows.Count);
    }

    /// <summary>
    /// Predicate scan over an existing user table — the LINQ surface produces a
    /// non-empty result for at least one ShipCountry value present in Orders.
    /// (mdbtestdata/sql/nwind.sql tests City='Helsinki' on Customers; Customers
    /// is not present as a table in the bundled fixture.)
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_Orders_WhereShipCountryFinland_ReturnsNonEmpty()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync("Orders", TestContext.Current.CancellationToken);
        int shipCountryOrdinal = meta.Select((c, i) => (c, i)).First(t => t.c.Name == "ShipCountry").i;

        var matches = await reader.Rows("Orders", cancellationToken: TestContext.Current.CancellationToken)
            .Where(row => row[shipCountryOrdinal] is string s && s == "Finland")
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(matches);
    }

    /// <summary>
    /// Non-ASCII predicate scan — exercises the codepage decoder against a
    /// table whose name itself contains a non-ASCII character (Umsätze).
    /// (mdbtestdata/sql/nwind.sql tests CompanyName LIKE 'Océ%' on Customers;
    /// Customers is not present as a table in the bundled fixture.)
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_Umsätze_NonAsciiTableName_StreamsAtLeastOneRow()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        var first = await reader.Rows("Umsätze", cancellationToken: TestContext.Current.CancellationToken)
            .Take(1)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(first);
    }

    /// <summary>Mirror of <c>mdb-ver nwind.mdb</c> from mdbtools' test_script.sh — surfaces a non-empty version string.</summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_Statistics_ReportsVersion()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.False(string.IsNullOrWhiteSpace(stats.Version));
        Assert.True(stats.DatabaseSizeBytes > 0);
        Assert.True(stats.TableCount > 0);
    }

    /// <summary>
    /// Conformance: every user table in nwind.mdb streams to completion with row
    /// counts that match <c>GetRealRowCountAsync</c>. This is the broad "no row
    /// in the file makes the decoder throw" check that mdbtools' shell tests
    /// exit-zero by accident; we make it explicit.
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task Nwind_EveryTable_StreamsToCompletion()
    {
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            return;
        }

        await AssertEveryTableStreamsAsync(TestDatabases.MdbtoolsNwind);
    }

    // ── ASampleDatabase.accdb (ACE / Access 2007+) ────────────────────────────

    /// <summary>Mirror of <c>mdb-tables ASampleDatabase.accdb</c> — the named table is present.</summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task ASampleDatabase_ListTables_ContainsAssetItems()
    {
        if (!File.Exists(TestDatabases.MdbtoolsASampleDatabase))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsASampleDatabase, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Contains("Asset Items", tables);
    }

    /// <summary>Mirror of <c>mdb-count ASampleDatabase.accdb "Asset Items"</c>.</summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task ASampleDatabase_AssetItems_HasRows()
    {
        if (!File.Exists(TestDatabases.MdbtoolsASampleDatabase))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsASampleDatabase, TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Asset Items", TestContext.Current.CancellationToken);

        Assert.True(count > 0, "Asset Items should have at least one row.");
    }

    /// <summary>Mirror of <c>mdb-json ASampleDatabase.accdb "Asset Items"</c> — every row decodes without throwing.</summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task ASampleDatabase_AssetItems_StreamsToCompletion()
    {
        if (!File.Exists(TestDatabases.MdbtoolsASampleDatabase))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsASampleDatabase, TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync("Asset Items", TestContext.Current.CancellationToken);
        int expected = meta.Count;

        long observed = 0;
        await foreach (object[] row in reader.Rows("Asset Items", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(expected, row.Length);
            observed++;
        }

        Assert.True(observed > 0);
    }

    /// <summary>
    /// Conformance: every user table in ASampleDatabase.accdb streams to completion.
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task ASampleDatabase_EveryTable_StreamsToCompletion()
    {
        if (!File.Exists(TestDatabases.MdbtoolsASampleDatabase))
        {
            return;
        }

        await AssertEveryTableStreamsAsync(TestDatabases.MdbtoolsASampleDatabase);
    }

    // ── DateTestDatabase.mdb ──────────────────────────────────────────────────

    /// <summary>
    /// DateTestDatabase.mdb was added to mdbtestdata specifically to regression-test
    /// the date column parser. The fixture must contain at least one Date/Time
    /// column, and every row must decode without throwing.
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task DateTestDatabase_HasDateColumn_AndStreamsToCompletion()
    {
        if (!File.Exists(TestDatabases.MdbtoolsDateTestDatabase))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsDateTestDatabase, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        bool sawDateColumn = false;
        foreach (string table in tables)
        {
            var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            if (meta.Any(c => c.ClrType == typeof(System.DateTime)))
            {
                sawDateColumn = true;
            }

            await foreach (object[] discarded in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                _ = discarded; // drain
            }
        }

        Assert.True(sawDateColumn, "DateTestDatabase was expected to contain at least one Date/Time column.");
    }

    /// <summary>
    /// All Date/Time values in DateTestDatabase.mdb decode to <see cref="System.DateTime"/>
    /// (or <see cref="System.DBNull"/>) — never to a string or to the wrong CLR type.
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task DateTestDatabase_DateColumns_ProduceDateTimeValues()
    {
        if (!File.Exists(TestDatabases.MdbtoolsDateTestDatabase))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsDateTestDatabase, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool sawAnyDateValue = false;
        foreach (string table in tables)
        {
            var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            int[] dateOrdinals = meta
                .Select((c, i) => (c, i))
                .Where(t => t.c.ClrType == typeof(System.DateTime))
                .Select(t => t.i)
                .ToArray();

            if (dateOrdinals.Length == 0)
            {
                continue;
            }

            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                foreach (int o in dateOrdinals)
                {
                    object v = row[o];
                    Assert.True(
                        v is System.DateTime || v is System.DBNull,
                        $"Date column at ordinal {o} produced {v?.GetType().FullName ?? "null"}");
                    if (v is System.DateTime)
                    {
                        sawAnyDateValue = true;
                    }
                }
            }
        }

        Assert.True(sawAnyDateValue, "Expected at least one non-null DateTime value across DateTestDatabase.");
    }

    /// <summary>
    /// <see cref="AccessReader.ReadDataTableAsync"/> returns a <see cref="DataTable"/> whose
    /// <c>DataType</c> for the date column is <see cref="System.DateTime"/>.
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task DateTestDatabase_DataTable_DateColumnHasDateTimeDataType()
    {
        if (!File.Exists(TestDatabases.MdbtoolsDateTestDatabase))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.MdbtoolsDateTestDatabase, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool checkedAtLeastOne = false;
        foreach (string table in tables)
        {
            DataTable? dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
            Assert.NotNull(dt);

            foreach (DataColumn col in dt!.Columns)
            {
                if (col.DataType == typeof(System.DateTime))
                {
                    checkedAtLeastOne = true;
                }
            }
        }

        Assert.True(checkedAtLeastOne, "Expected at least one DataColumn typed as DateTime.");
    }

    // ── shared helpers ────────────────────────────────────────────────────────

    private async ValueTask AssertEveryTableStreamsAsync(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        foreach (string table in tables)
        {
            var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            int expected = meta.Count;

            long observed = 0;
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.Equal(expected, row.Length);
                observed++;
            }

            long reported = await reader.GetRealRowCountAsync(table, TestContext.Current.CancellationToken);
            Assert.Equal(reported, observed);
        }
    }
}
