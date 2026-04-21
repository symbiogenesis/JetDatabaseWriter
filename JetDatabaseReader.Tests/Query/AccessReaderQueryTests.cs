namespace JetDatabaseReader.Tests;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Tests for TableQuery — both the typed chain and the string chain.
///
/// Typed chain:  Where → Execute / FirstOrDefault / Count
/// String chain: WhereAsStrings → ExecuteAsStrings / FirstOrDefaultAsStrings / CountAsStrings.
/// </summary>
public class AccessReaderQueryTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── Typed chain: Execute ──────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Execute_WithoutFilter_ReturnsAllRows(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int expected = await reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        IAsyncEnumerable<object[]> result = reader.Query(table).ExecuteAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected, await result.CountAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Execute_WithTake_LimitsResults(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        IAsyncEnumerable<object[]> result = reader.Query(table).Take(3).ExecuteAsync(TestContext.Current.CancellationToken);

        Assert.True(await result.CountAsync(TestContext.Current.CancellationToken) <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Execute_WithWhere_FiltersRows(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        // Always-false filter should return zero rows
        var none = await reader.Query(table)
            .Where(_ => false)
            .ExecuteAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Execute_WithWhere_AlwaysTrue_ReturnsAllRows(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int expected = await reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        IAsyncEnumerable<object[]> all = reader.Query(table)
            .Where(_ => true)
            .ExecuteAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected, await all.CountAsync(TestContext.Current.CancellationToken));
    }

    // ── Typed chain: FirstOrDefault ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefault_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = await db.GetAsync(path);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        string table = stat.Name;
        object[]? first = await reader.Query(table).FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefault_WhenNoRowMatches_ReturnsNull(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        object[]? result = await reader.Query(table)
            .Where(_ => false)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.Null(result);
    }

    // ── Typed chain: Count ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Count_WithoutFilter_MatchesStreamRowsCount(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int expected = await reader.StreamRowsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        int count = await reader.Query(table).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Count_WithAlwaysFalseFilter_ReturnsZero(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int count = await reader.Query(table).Where(_ => false).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, count);
    }

    // ── String chain: ExecuteAsStrings ────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteAsStrings_WithoutFilter_ReturnsAllRows(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        int expected = await reader.StreamRowsAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        IAsyncEnumerable<string[]> result = reader.Query(table).ExecuteAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected, await result.CountAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteAsStrings_WithTake_LimitsResults(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        IAsyncEnumerable<string[]> result = reader.Query(table).Take(3).ExecuteAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.True(await result.CountAsync(TestContext.Current.CancellationToken) <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteAsStrings_WithWhereAsStrings_FiltersRows(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        // Always-false filter should return zero rows
        var none = await reader.Query(table)
            .WhereAsStrings(_ => false)
            .ExecuteAsStringsAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteAsStrings_AllCells_AreStringOrNull(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await foreach (string[] row in reader.Query(table).Take(20).ExecuteAsStringsAsync(TestContext.Current.CancellationToken))
        {
            foreach (string cell in row)
            {
                Assert.True(cell == null || cell is string);
            }
        }
    }

    // ── String chain: FirstOrDefaultAsStrings ─────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefaultAsStrings_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = await db.GetAsync(path);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        string table = stat.Name;
        string[]? first = await reader.Query(table).FirstOrDefaultAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefaultAsStrings_WhenNoRowMatches_ReturnsNull(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        string[]? result = await reader.Query(table)
            .WhereAsStrings(_ => false)
            .FirstOrDefaultAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.Null(result);
    }

    // ── String chain: CountAsStrings ──────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task CountAsStrings_WithoutFilter_MatchesCount(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int typed = await reader.Query(table).CountAsync(TestContext.Current.CancellationToken);
        int string_ = await reader.Query(table).CountAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typed, string_);
    }

    // ── Take affects both chains ──────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Take_AppliedOnce_AffectsBothExecuteAndExecuteAsStrings(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        const int limit = 2;

        TableQuery query = reader.Query(table).Take(limit);

        Assert.True(await query.ExecuteAsync(TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken) <= limit);
        Assert.True(await query.ExecuteAsStringsAsync(TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken) <= limit);
    }

    // ── Generic chain: Execute<T> ─────────────────────────────────────

    private sealed class QueryRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteGeneric_WithoutFilter_RowCountMatchesExecute(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int typedCount = await reader.Query(table).ExecuteAsync(TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        int genericCount = await reader.Query(table).ExecuteAsync<QueryRow>(TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(typedCount, genericCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteGeneric_WithTake_LimitsResults(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        IAsyncEnumerable<QueryRow> result = reader.Query(table).Take(3).ExecuteAsync<QueryRow>(TestContext.Current.CancellationToken);

        Assert.True(await result.CountAsync(TestContext.Current.CancellationToken) <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteGeneric_WithWhere_FiltersRows(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var none = await reader.Query(table)
            .Where(_ => false)
            .ExecuteAsync<QueryRow>(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ExecuteGeneric_ReturnsNonNullInstances(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await foreach (QueryRow item in reader.Query(table).Take(20).ExecuteAsync<QueryRow>(TestContext.Current.CancellationToken))
        {
            Assert.NotNull(item);
        }
    }

    // ── Generic chain: FirstOrDefault<T> ──────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefaultGeneric_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = await db.GetAsync(path);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        QueryRow? first = await reader.Query(table).FirstOrDefaultAsync<QueryRow>(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefaultGeneric_WhenNoRowMatches_ReturnsNull(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        QueryRow? result = await reader.Query(table)
            .Where(_ => false)
            .FirstOrDefaultAsync<QueryRow>(TestContext.Current.CancellationToken);

        Assert.Null(result);
    }

    // ── Generic chain: Take affects Execute<T> ────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task Take_AffectsExecuteGeneric(string path)
    {
        var reader = await db.GetAsync(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        const int limit = 2;

        Assert.True(await reader.Query(table).Take(limit).ExecuteAsync<QueryRow>(TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken) <= limit);
    }
}
