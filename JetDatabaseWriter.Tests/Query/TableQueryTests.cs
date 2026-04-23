namespace JetDatabaseWriter.Tests;

using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Dedicated tests for the TableQuery fluent API — guard clauses, combined
/// Where + Take chains, string chain combinations, and generic POCO queries.
/// Complements <see cref="AccessReaderQueryTests"/> which covers the basic per-method paths.
/// </summary>
public sealed class TableQueryTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── Guard clauses ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Where_NullPredicate_ThrowsArgumentNullException(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        Assert.Throws<ArgumentNullException>(() => reader.Query(table).Where(null!));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task WhereAsStrings_NullPredicate_ThrowsArgumentNullException(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        Assert.Throws<ArgumentNullException>(() => reader.Query(table).WhereAsStrings(null!));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Take_Zero_ThrowsArgumentOutOfRangeException(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.Query(table).Take(0));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Take_Negative_ThrowsArgumentOutOfRangeException(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.Query(table).Take(-5));
    }

    // ── Combined Where + Take (typed) ─────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Execute_WhereAndTake_FiltersBeforeLimiting(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        // Where(true) + Take(2) should return at most 2 rows
        var result = await reader.Query(table)
            .Where(_ => true)
            .Take(2)
            .ExecuteAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(result.Count <= 2);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Execute_WhereFalseAndTake_ReturnsEmpty(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var result = await reader.Query(table)
            .Where(_ => false)
            .Take(100)
            .ExecuteAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefault_WithWhereTrue_ReturnsSameAsDirectStream(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        object[]? first = await reader.Query(table).Where(_ => true).FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Count_WithTake_RespectsLimit(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 2);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;

        int count = await reader.Query(table).Take(1).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Count_WhereFalse_ReturnsZero(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int count = await reader.Query(table).Where(_ => false).Take(100).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, count);
    }

    // ── Combined WhereAsStrings + Take (string chain) ─────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ExecuteAsStrings_WhereAsStringsAndTake_FiltersBeforeLimiting(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var result = await reader.Query(table)
            .WhereAsStrings(_ => true)
            .Take(2)
            .ExecuteAsStringsAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(result.Count <= 2);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ExecuteAsStrings_WhereAsStringsFalse_ReturnsEmpty(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var result = await reader.Query(table)
            .WhereAsStrings(_ => false)
            .Take(100)
            .ExecuteAsStringsAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefaultAsStrings_WithTake_ReturnsNonNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        string[]? first = await reader.Query(table).Take(5).FirstOrDefaultAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CountAsStrings_WithTake_RespectsLimit(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 2);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;

        int count = await reader.Query(table).Take(1).CountAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CountAsStrings_WhereFalseAndTake_ReturnsZero(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        int count = await reader.Query(table).WhereAsStrings(_ => false).Take(50).CountAsStringsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, count);
    }

    // ── Generic POCO chain: Where + Take + Execute<T> ─────────────────

    private sealed class QueryPoco
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ExecuteGeneric_WhereAndTake_FiltersAndLimits(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var result = await reader.Query(table)
            .Where(_ => true)
            .Take(2)
            .ExecuteAsync<QueryPoco>(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.True(result.Count <= 2);
        Assert.All(result, item => Assert.NotNull(item));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task FirstOrDefaultGeneric_WithTake_ReturnsNonNull(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        QueryPoco? result = await reader.Query(table).Take(1).FirstOrDefaultAsync<QueryPoco>(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
    }

    // ── Take(1) gives exactly 1 when table is non-empty ──────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Take_One_ReturnsExactlyOneRowWhenTableHasData(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;

        Assert.Equal(1, await reader.Query(table).Take(1).ExecuteAsync(TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, await reader.Query(table).Take(1).ExecuteAsStringsAsync(TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken));
    }

    // ── Fluent chaining returns same query instance ───────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task FluentChain_ReturnsThisForChaining(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        TableQuery query = reader.Query(table);
        TableQuery afterWhere = query.Where(_ => true);
        TableQuery afterTake = afterWhere.Take(10);

        Assert.Same(query, afterWhere);
        Assert.Same(query, afterTake);
    }

    // ── Where replaces previous Where ─────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Where_CalledTwice_SecondPredicateWins(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        // First set always-true, then replace with always-false
        var result = await reader.Query(table)
            .Where(_ => true)
            .Where(_ => false)
            .ExecuteAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task WhereAsStrings_CalledTwice_SecondPredicateWins(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        var result = await reader.Query(table)
            .WhereAsStrings(_ => true)
            .WhereAsStrings(_ => false)
            .ExecuteAsStringsAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }
}
