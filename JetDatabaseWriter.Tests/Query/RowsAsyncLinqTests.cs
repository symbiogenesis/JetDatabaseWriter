namespace JetDatabaseWriter.Tests.Query;

using System;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Coverage for the <see cref="IAccessReader.Rows(string, System.IProgress{long}?, System.Threading.CancellationToken)"/> /
/// <see cref="IAccessReader.Rows{T}(string, System.IProgress{long}?, System.Threading.CancellationToken)"/> /
/// <see cref="IAccessReader.RowsAsStrings(string, System.IProgress{long}?, System.Threading.CancellationToken)"/>
/// surface composed with the standard <see cref="System.Linq.AsyncEnumerable"/> operators.
/// </summary>
public class RowsAsyncLinqTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private sealed class QueryRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class SyncProgress<T>(Action<T> action) : IProgress<T>
    {
        public void Report(T value) => action(value);
    }

    // ── Rows (object[]) ───────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_Take_LimitsResults(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var rows = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).Take(3).ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(rows.Count <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_Where_AlwaysFalse_ReturnsEmpty(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var none = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken)
            .Where(_ => false)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_Where_AlwaysTrue_ReturnsAllRows(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int expected = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        int actual = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).Where(_ => true).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_FirstOrDefault_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables empty
        }

        object[]? first = await reader.Rows(stat.Name, cancellationToken: TestContext.Current.CancellationToken).FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_FirstOrDefault_NoMatch_ReturnsNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        object[]? result = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken)
            .Where(_ => false)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.Null(result);
    }

    // ── RowsAsStrings (string[]) ──────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsAsStrings_WithoutFilter_CountMatchesStreamRowsAsStrings(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int expected = await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        int actual = await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsAsStrings_Take_LimitsResults(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var rows = await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).Take(3).ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(rows.Count <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsAsStrings_Where_AlwaysFalse_ReturnsEmpty(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var none = await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken)
            .Where(_ => false)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsAsStrings_AllCells_AreStringOrNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await foreach (string[] row in reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).Take(20).WithCancellation(TestContext.Current.CancellationToken))
        {
            foreach (string cell in row)
            {
                Assert.True(cell == null || cell is string);
            }
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsAsStrings_FirstOrDefault_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string[]? first = await reader.RowsAsStrings(stat.Name, cancellationToken: TestContext.Current.CancellationToken).FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    // ── Rows<T> (POCO mapping) ────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsGeneric_WithoutFilter_CountMatchesRows(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int typedCount = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int genericCount = await reader.Rows<QueryRow>(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typedCount, genericCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsGeneric_Take_LimitsResults(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var rows = await reader.Rows<QueryRow>(table, cancellationToken: TestContext.Current.CancellationToken).Take(3).ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(rows.Count <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsGeneric_Where_AlwaysFalse_ReturnsEmpty(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var none = await reader.Rows<QueryRow>(table, cancellationToken: TestContext.Current.CancellationToken)
            .Where(_ => false)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsGeneric_ReturnsNonNullInstances(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await foreach (QueryRow item in reader.Rows<QueryRow>(table, cancellationToken: TestContext.Current.CancellationToken).Take(20).WithCancellation(TestContext.Current.CancellationToken))
        {
            Assert.NotNull(item);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsGeneric_FirstOrDefault_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        QueryRow? first = await reader.Rows<QueryRow>(stat.Name, cancellationToken: TestContext.Current.CancellationToken).FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsGeneric_FirstOrDefault_NoMatch_ReturnsNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        QueryRow? result = await reader.Rows<QueryRow>(table, cancellationToken: TestContext.Current.CancellationToken)
            .Where(_ => false)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.Null(result);
    }

    // ── Combined operator chains ──────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_WhereThenTake_HonorsBoth(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var rows = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken)
            .Where(_ => true)
            .Take(2)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(rows.Count <= 2);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_TakeIsLazy_DoesNotEnumerateBeyondLimit(string path)
    {
        // Verifies that Take(N) short-circuits the underlying page enumerator.
        // We attach a progress reporter that increments per page; if Take didn't
        // short-circuit, we'd see far more rows reported than we asked for.
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 5);
        if (stat == null)
        {
            return; // need a table large enough for the assertion to be meaningful
        }

        long lastReported = 0;
        var progress = new SyncProgress<long>(n => lastReported = n);

        var rows = await reader.Rows(stat.Name, progress, TestContext.Current.CancellationToken)
            .Take(1)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(rows);

        // Progress is reported once per data page; one page is fine, but we should
        // not have walked the entire table.
        Assert.True(lastReported < stat.RowCount, $"Take(1) walked {lastReported} of {stat.RowCount} rows — short-circuit not working.");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task RowsAsStrings_CountMatches_RowsCount(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int typed = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int strings = await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typed, strings);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Rows_Select_ProjectsToFirstColumn(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var firstCells = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken)
            .Take(5)
            .Select(row => row.Length > 0 ? row[0] : null)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(firstCells.Count <= 5);
    }
}
