namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper

/// <summary>
/// Tests for: StreamRows (typed object[]) and StreamRowsAsStrings (string[]).
/// Includes bounded retained-memory smoke tests on required native fixtures.
/// </summary>
/// <param name="db">The database input.</param>
public class AccessReaderStreamTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── StreamRows (typed) ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_YieldsAtLeastOneRow(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);

        Assert.True(await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).AnyAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_EachRow_HasSameColumnCountAsMetadata(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        int colCount = (await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken)).Count;

        int count = 0;
        await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).Take(50))
        {
            Assert.Equal(colCount, row.Length);
            count++;
        }

        Assert.True(count > 0, $"The populated table '{table}' must yield rows.");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_TotalCount_MatchesReadTableRowCount(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);

        int streamCount = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        using System.Data.DataTable rows = await reader.ReadTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        int dtCount = rows.Rows.Count;

        Assert.True(streamCount > 0, $"The populated table '{table}' must yield rows.");
        Assert.Equal(dtCount, streamCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_NumericAndDateColumns_AreNotStrings(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        int numericColIdx = FindColumnIndex(meta, m => m.ClrType != typeof(string));
        Assert.True(numericColIdx >= 0, $"The required fixture table '{table}' must contain a non-text column.");
        bool checkedValue = false;
        await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).Take(50))
        {
            if (row[numericColIdx] != DBNull.Value)
            {
                Assert.IsNotType<string>(row[numericColIdx]);
                checkedValue = true;
            }
        }

        Assert.True(checkedValue, $"The required fixture table '{table}' must exercise a non-null typed value.");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_WithProgress_ReportsNonNegativeValues(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        var reported = new List<long>();

        // Use synchronous IProgress<T> to avoid Progress<T>'s thread-pool dispatch,
        // which can fire callbacks after the foreach completes and cause a collection
        // modification exception when iterating for assertion.
        await foreach (object[] row in reader.Rows(table, new SyncProgress<long>(reported.Add), TestContext.Current.CancellationToken))
        {
            _ = row;
        }

        Assert.NotEmpty(reported);
        foreach (long v in reported)
        {
            Assert.True(v >= 0);
        }
    }

    // ── StreamRowsAsStrings ───────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsAsStrings_YieldsAtLeastOneRow(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);

        Assert.True(await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).AnyAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsAsStrings_CellCount_MatchesColumnMetadata(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        int colCount = (await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken)).Count;

        int count = 0;
        await foreach (string[] row in reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).Take(50))
        {
            Assert.Equal(colCount, row.Length);
            count++;
        }

        Assert.True(count > 0, $"The populated table '{table}' must yield rows.");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsAsStrings_TotalCount_MatchesStreamRowsCount(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);

        int typedCount = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int stringCount = await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.True(typedCount > 0, $"The populated table '{table}' must yield rows.");
        Assert.Equal(typedCount, stringCount);
    }

    // ── Northwind — retained-memory smoke test ───────────────────────

    [Fact(DisableParallelization = true)] // Full collections and retained-heap measurements exclude unrelated tests.
    public async Task StreamRows_NorthwindTraders_DoesNotExceedReasonableMemory()
    {
        string path = TestDatabases.NorthwindTraders;
        long before = GC.GetTotalMemory(forceFullCollection: true);

        await using AccessReader reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 256, UseLockFile = false }, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        int count = 0;

        await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
        {
            _ = row;
            count++;
            if (count >= 100_000)
            {
                break; // process first 100 K rows
            }
        }

        long after = GC.GetTotalMemory(forceFullCollection: true);
        long deltaMb = (after - before) / (1024 * 1024);

        Assert.True(count > 0, "The required fixture must exercise streaming before memory is measured.");

        // Bound retained heap after traversing the fixture, up to 100 K rows.
        Assert.True(deltaMb < 200, $"Streaming should not load the whole file into memory (delta: {deltaMb} MB)");
    }

    [Fact]
    public async Task StreamRows_NorthwindTraders_ReadsAllTablesWithoutException()
    {
        string path = TestDatabases.NorthwindTraders;
        await using AccessReader reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 512, UseLockFile = false }, TestContext.Current.CancellationToken);
        await AssertAllTablesStreamAsync(reader);
    }

    // ── Jackcess — memory-efficiency smoke test ─────────────────────

    [Theory(DisableParallelization = true)] // Full collections and retained-heap measurements exclude unrelated tests.
    [MemberData(nameof(TestDatabases.Jackcess), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_Jackcess_DoesNotExceedReasonableMemory(string path)
    {
        long before = GC.GetTotalMemory(forceFullCollection: true);

        await using AccessReader reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 256, UseLockFile = false }, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        int count = 0;

        await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
        {
            _ = row;
            count++;
            if (count >= 100_000)
            {
                break; // process first 100 K rows
            }
        }

        long after = GC.GetTotalMemory(forceFullCollection: true);
        long deltaMb = (after - before) / (1024 * 1024);

        Assert.True(count > 0, "The required fixture must exercise streaming before memory is measured.");
        Assert.True(deltaMb < 200, $"Streaming should not load the whole file into memory (delta: {deltaMb} MB)");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Jackcess), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_Jackcess_ReadsAllTablesWithoutException(string path)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 512, UseLockFile = false }, TestContext.Current.CancellationToken);
        await AssertAllTablesStreamAsync(reader);
    }

    // ── StreamRows<T> (generic POCO) ──────────────────────────────────

    private sealed class StreamGenericRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsGeneric_Count_MatchesStreamRows(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);

        int typedCount = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int genericCount = await reader.Rows<StreamGenericRow>(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.True(typedCount > 0, $"The populated table '{table}' must yield rows.");
        Assert.Equal(typedCount, genericCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsGeneric_YieldsNonNullInstances(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);

        int count = 0;
        await foreach (StreamGenericRow? item in reader.Rows<StreamGenericRow>(table, cancellationToken: TestContext.Current.CancellationToken).Take(50))
        {
            Assert.NotNull(item);
            count++;
        }

        Assert.True(count > 0, $"The populated table '{table}' must yield rows.");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsGeneric_WithProgress_ReportsNonNegativeValues(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        var reported = new List<long>();

        await foreach (StreamGenericRow item in reader.Rows<StreamGenericRow>(table, new SyncProgress<long>(reported.Add), TestContext.Current.CancellationToken))
        {
            _ = item;
        }

        Assert.NotEmpty(reported);
        foreach (long v in reported)
        {
            Assert.True(v >= 0);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsGeneric_IsLazy_CanBreakEarly(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = await GetPopulatedTableAsync(reader);
        int count = 0;

        await foreach (StreamGenericRow item in reader.Rows<StreamGenericRow>(table, cancellationToken: TestContext.Current.CancellationToken))
        {
            _ = item;
            count++;
            if (count >= 3)
            {
                break;
            }
        }

        Assert.InRange(count, 1, 3);
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static async Task<string> GetPopulatedTableAsync(AccessReader reader)
    {
        TableStat? table = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(stat => stat.RowCount > 0);
        Assert.NotNull(table);
        return table.Name;
    }

    private static async Task AssertAllTablesStreamAsync(AccessReader reader)
    {
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);
        int totalRows = 0;
        foreach (string table in tables)
        {
            int columns = (await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken)).Count;
            Assert.True(columns > 0, $"The fixture table '{table}' must retain its columns.");
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).Take(1000))
            {
                Assert.Equal(columns, row.Length);
                totalRows++;
            }
        }

        Assert.True(totalRows > 0, "The required fixture must exercise at least one streamed row.");
    }

    private static int FindColumnIndex(IReadOnlyList<ColumnMetadata> columns, Predicate<ColumnMetadata> match)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (match(columns[i]))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Synchronous IProgress&lt;T&gt; that invokes the callback directly on the reporting
    /// thread. Use in tests instead of Progress&lt;T&gt; to avoid thread-pool dispatch races
    /// when asserting the collected values immediately after iteration.
    /// </summary>
    /// <typeparam name="T">The progress value type reported by the helper.</typeparam>
    /// <param name="action">The action.</param>
    private sealed class SyncProgress<T>(Action<T> action) : IProgress<T>
    {
        public void Report(T value) => action(value);
    }
}
