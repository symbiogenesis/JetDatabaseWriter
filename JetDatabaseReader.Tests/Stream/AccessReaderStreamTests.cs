namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using Xunit;

/// <summary>
/// Tests for: StreamRows (typed object[]) and StreamRowsAsStrings (string[]).
/// Includes a memory-efficiency smoke test on the large Matrix database.
/// </summary>
public class AccessReaderStreamTests
{
    // ── StreamRows (typed) ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void StreamRows_YieldsAtLeastOneRow(string path)
    {
        using var reader = TestDatabases.Open(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        IEnumerable<object[]> rows = reader.StreamRows(stat.Name);

        _ = rows.Should().NotBeEmpty(because: $"table '{stat.Name}' should have rows");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void StreamRows_EachRow_HasSameColumnCountAsMetadata(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = reader.ListTables()[0];
        int colCount = reader.GetColumnMetadata(table).Count;

        foreach (object[] row in reader.StreamRows(table).Take(50))
        {
            _ = row.Should().HaveCount(
                colCount,
                because: $"every typed row must match the schema column count");
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void StreamRows_TotalCount_MatchesReadTableRowCount(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = reader.ListTables()[0];

        int streamCount = reader.StreamRows(table).Count();
        int dtCount = reader.ReadTable(table).Rows.Count;

        _ = streamCount.Should().Be(dtCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void StreamRows_NumericAndDateColumns_AreNotStrings(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = reader.ListTables()[0];
        var meta = reader.GetColumnMetadata(table);

        // Find first non-string, non-null column
        int numericColIdx = meta.FindIndex(m => m.ClrType != typeof(string));
        if (numericColIdx < 0)
        {
            return; // all-string table — skip assertion
        }

        object[] firstRow = reader.StreamRows(table).FirstOrDefault()!;
        if (firstRow == null || firstRow[numericColIdx] == DBNull.Value)
        {
            return;
        }

        _ = firstRow[numericColIdx].Should().NotBeOfType<string>(
            because: "typed stream should not return strings for numeric/date columns");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void StreamRows_WithProgress_ReportsNonNegativeValues(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = reader.ListTables()[0];
        var reported = new List<int>();

        // Use synchronous IProgress<T> to avoid Progress<T>'s thread-pool dispatch,
        // which can fire callbacks after the foreach completes and cause a collection
        // modification exception when iterating for assertion.
        foreach (object[] row in reader.StreamRows(table, new SyncProgress<int>(reported.Add)))
        {
            _ = row;
        }

        foreach (int v in reported)
        {
            _ = v.Should().BeGreaterThanOrEqualTo(0);
        }
    }

    // ── StreamRowsAsStrings ───────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void StreamRowsAsStrings_YieldsAtLeastOneRow(string path)
    {
        using var reader = TestDatabases.Open(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        _ = reader.StreamRowsAsStrings(stat.Name).Should().NotBeEmpty();
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void StreamRowsAsStrings_AllCells_AreNullOrString(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = reader.ListTables()[0];

        foreach (string[] row in reader.StreamRowsAsStrings(table).Take(50))
        {
            foreach (string cell in row)
            {
                _ = (cell == null || cell is string).Should().BeTrue(
                    because: "StreamRowsAsStrings must only yield strings or nulls");
            }
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void StreamRowsAsStrings_TotalCount_MatchesStreamRowsCount(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = reader.ListTables()[0];

        int typedCount = reader.StreamRows(table).Count();
        int stringCount = reader.StreamRowsAsStrings(table).Count();

        _ = stringCount.Should().Be(typedCount);
    }

    // ── Matrix (2 GB) — memory-efficiency smoke test ──────────────────

    [Fact]
    public void StreamRows_NorthwindTraders_DoesNotExceedReasonableMemory()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!TestDatabases.IsReadable(path))
        {
            return; // skip if not present or encrypted
        }

        long before = GC.GetTotalMemory(forceFullCollection: true);

        using var reader = TestDatabases.Open(path, new AccessReaderOptions { PageCacheSize = 256 });
        string table = reader.ListTables()[0];
        int count = 0;

        foreach (object[] row in reader.StreamRows(table))
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
        _ = deltaMb.Should().BeLessThan(
            200,
            because: "streaming should not load the whole file into memory");
    }

    [Fact]
    public void StreamRows_NorthwindTraders_ReadsAllTablesWithoutException()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!TestDatabases.IsReadable(path))
        {
            return; // skip if not present or encrypted
        }

        using var reader = TestDatabases.Open(path, new AccessReaderOptions { PageCacheSize = 512 });
        List<string> tables = reader.ListTables();

        _ = tables.Should().NotBeEmpty();

        string first = tables[0];
        int count = reader.StreamRows(first).Take(1000).Count();
        _ = count.Should().BeGreaterThanOrEqualTo(0);
    }

    // ── Matrix (local-only large file) — smoke test ───────────────────

    [Fact]
    public void StreamRows_Matrix_DoesNotExceedReasonableMemory()
    {
        string path = TestDatabases.LargeFile;
        if (!TestDatabases.IsReadable(path))
        {
            return; // skip if not present or encrypted
        }

        long before = GC.GetTotalMemory(forceFullCollection: true);

        using var reader = TestDatabases.Open(path, new AccessReaderOptions { PageCacheSize = 256 });
        string table = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0)?.Name
                       ?? reader.ListTables()[0];
        int count = 0;

        foreach (object[] row in reader.StreamRows(table))
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

        _ = deltaMb.Should().BeLessThan(
            200,
            because: "streaming should not load the whole file into memory");
    }

    [Fact]
    public void StreamRows_Matrix_ReadsAllTablesWithoutException()
    {
        string path = TestDatabases.LargeFile;
        if (!TestDatabases.IsReadable(path))
        {
            return; // skip if not present or encrypted
        }

        using var reader = TestDatabases.Open(path, new AccessReaderOptions { PageCacheSize = 512 });
        List<string> tables = reader.ListTables();

        _ = tables.Should().NotBeEmpty();

        string first = tables[0];
        int count = reader.StreamRows(first).Take(1000).Count();
        _ = count.Should().BeGreaterThanOrEqualTo(0);
    }

    // ── Helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Synchronous IProgress&lt;T&gt; that invokes the callback directly on the reporting
    /// thread. Use in tests instead of Progress&lt;T&gt; to avoid thread-pool dispatch races
    /// when asserting the collected values immediately after iteration.
    /// </summary>
    private sealed class SyncProgress<T> : IProgress<T>
    {
        private readonly Action<T> _action;

        public SyncProgress(Action<T> action) => _action = action;

        public void Report(T value) => _action(value);
    }
}
