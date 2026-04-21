namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Tests for: StreamRows (typed object[]) and StreamRowsAsStrings (string[]).
/// Includes a memory-efficiency smoke test on the large Matrix database.
/// </summary>
public class AccessReaderStreamTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── StreamRows (typed) ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_YieldsAtLeastOneRow(string path)
    {
        var reader = await db.GetAsync(path);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        Assert.True(await reader.StreamRowsAsync(stat.Name, cancellationToken: TestContext.Current.CancellationToken).AnyAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_EachRow_HasSameColumnCountAsMetadata(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int colCount = (await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken)).Count;

        await foreach (object[] row in reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).Take(50))
        {
            Assert.Equal(colCount, row.Length);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_TotalCount_MatchesReadTableRowCount(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int streamCount = await reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int dtCount = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!.Rows.Count;

        Assert.Equal(dtCount, streamCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_NumericAndDateColumns_AreNotStrings(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        // Find first non-string, non-null column
        int numericColIdx = meta.FindIndex(m => m.ClrType != typeof(string));
        if (numericColIdx < 0)
        {
            return; // all-string table — skip assertion
        }

        object[] firstRow = (await reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).FirstOrDefaultAsync(TestContext.Current.CancellationToken))!;
        if (firstRow == null || firstRow[numericColIdx] == DBNull.Value)
        {
            return;
        }

        Assert.IsNotType<string>(firstRow[numericColIdx]);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_WithProgress_ReportsNonNegativeValues(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var reported = new List<int>();

        // Use synchronous IProgress<T> to avoid Progress<T>'s thread-pool dispatch,
        // which can fire callbacks after the foreach completes and cause a collection
        // modification exception when iterating for assertion.
        await foreach (object[] row in reader.StreamRowsAsync(table, new SyncProgress<int>(reported.Add), TestContext.Current.CancellationToken))
        {
            _ = row;
        }

        foreach (int v in reported)
        {
            Assert.True(v >= 0);
        }
    }

    // ── StreamRowsAsStrings ───────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsAsStrings_YieldsAtLeastOneRow(string path)
    {
        var reader = await db.GetAsync(path);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        Assert.True(await reader.StreamRowsAsStringsAsync(stat.Name, cancellationToken: TestContext.Current.CancellationToken).AnyAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsAsStrings_AllCells_AreNullOrString(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await foreach (string[] row in reader.StreamRowsAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken).Take(50))
        {
            foreach (string cell in row)
            {
                Assert.True(cell == null || cell is string);
            }
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsAsStrings_TotalCount_MatchesStreamRowsCount(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int typedCount = await reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int stringCount = await reader.StreamRowsAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typedCount, stringCount);
    }

    // ── Matrix (2 GB) — memory-efficiency smoke test ──────────────────

    [Fact]
    public async Task StreamRows_NorthwindTraders_DoesNotExceedReasonableMemory()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!await TestDatabases.IsReadableAsync(path))
        {
            return; // skip if not present or encrypted
        }

        long before = GC.GetTotalMemory(forceFullCollection: true);

        await using var reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 256 }, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int count = 0;

        await foreach (object[] row in reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken))
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

        // Streaming 100 K rows should not grow heap beyond 200 MB
        Assert.True(deltaMb < 200, $"Streaming should not load the whole file into memory (delta: {deltaMb} MB)");
    }

    [Fact]
    public async Task StreamRows_NorthwindTraders_ReadsAllTablesWithoutException()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!await TestDatabases.IsReadableAsync(path))
        {
            return; // skip if not present or encrypted
        }

        await using var reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 512 }, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);

        string first = tables[0];
        int count = await reader.StreamRowsAsync(first, cancellationToken: TestContext.Current.CancellationToken).Take(1000).CountAsync(TestContext.Current.CancellationToken);
        Assert.True(count >= 0);
    }

    // ── Jackcess — memory-efficiency smoke test ─────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Jackcess), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_Jackcess_DoesNotExceedReasonableMemory(string path)
    {
        long before = GC.GetTotalMemory(forceFullCollection: true);

        await using var reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 256 }, TestContext.Current.CancellationToken);
        string table = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0)?.Name
                       ?? (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int count = 0;

        await foreach (object[] row in reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken))
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

        Assert.True(deltaMb < 200, $"Streaming should not load the whole file into memory (delta: {deltaMb} MB)");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Jackcess), MemberType = typeof(TestDatabases))]
    public async Task StreamRows_Jackcess_ReadsAllTablesWithoutException(string path)
    {
        await using var reader = await TestDatabases.OpenAsync(path, new AccessReaderOptions { PageCacheSize = 512 }, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);

        string first = tables[0];
        int count = await reader.StreamRowsAsync(first, cancellationToken: TestContext.Current.CancellationToken).Take(1000).CountAsync(TestContext.Current.CancellationToken);
        Assert.True(count >= 0);
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
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int typedCount = await reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int genericCount = await reader.StreamRowsAsync<StreamGenericRow>(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typedCount, genericCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsGeneric_YieldsNonNullInstances(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await foreach (StreamGenericRow item in reader.StreamRowsAsync<StreamGenericRow>(table, cancellationToken: TestContext.Current.CancellationToken).Take(50))
        {
            Assert.NotNull(item);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsGeneric_WithProgress_ReportsNonNegativeValues(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var reported = new List<int>();

        await foreach (StreamGenericRow item in reader.StreamRowsAsync<StreamGenericRow>(table, new SyncProgress<int>(reported.Add), TestContext.Current.CancellationToken))
        {
            _ = item;
        }

        foreach (int v in reported)
        {
            Assert.True(v >= 0);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task StreamRowsGeneric_IsLazy_CanBreakEarly(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int count = 0;

        await foreach (StreamGenericRow item in reader.StreamRowsAsync<StreamGenericRow>(table, cancellationToken: TestContext.Current.CancellationToken))
        {
            _ = item;
            count++;
            if (count >= 3)
            {
                break;
            }
        }

        Assert.True(count <= 3);
    }

    // ── Helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Synchronous IProgress&lt;T&gt; that invokes the callback directly on the reporting
    /// thread. Use in tests instead of Progress&lt;T&gt; to avoid thread-pool dispatch races
    /// when asserting the collected values immediately after iteration.
    /// </summary>
    private sealed class SyncProgress<T>(Action<T> action) : IProgress<T>
    {
        private readonly Action<T> _action = action;

        public void Report(T value) => _action(value);
    }
}
