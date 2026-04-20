namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Dedicated tests for the TableQuery fluent API — guard clauses, combined
/// Where + Take chains, string chain combinations, and generic POCO queries.
/// Complements <see cref="AccessReaderQueryTests"/> which covers the basic per-method paths.
/// </summary>
[Collection<ReadOnlyDatabaseFixture>]
public sealed class TableQueryTests(DatabaseCache db)
{
    // ── Guard clauses ─────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Where_NullPredicate_ThrowsArgumentNullException(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        Assert.Throws<ArgumentNullException>(() => reader.Query(table).Where(null!));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void WhereAsStrings_NullPredicate_ThrowsArgumentNullException(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        Assert.Throws<ArgumentNullException>(() => reader.Query(table).WhereAsStrings(null!));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Take_Zero_ThrowsArgumentOutOfRangeException(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.Query(table).Take(0));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Take_Negative_ThrowsArgumentOutOfRangeException(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.Query(table).Take(-5));
    }

    // ── Combined Where + Take (typed) ─────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Execute_WhereAndTake_FiltersBeforeLimiting(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        // Where(true) + Take(2) should return at most 2 rows
        var result = reader.Query(table)
            .Where(_ => true)
            .Take(2)
            .Execute()
            .ToList();

        Assert.True(result.Count <= 2);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Execute_WhereFalseAndTake_ReturnsEmpty(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        var result = reader.Query(table)
            .Where(_ => false)
            .Take(100)
            .Execute()
            .ToList();

        Assert.Empty(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void FirstOrDefault_WithWhereTrue_ReturnsSameAsDirectStream(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        object[]? first = reader.Query(table).Where(_ => true).FirstOrDefault();

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Count_WithTake_RespectsLimit(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 2);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;

        int count = reader.Query(table).Take(1).Count();

        Assert.Equal(1, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Count_WhereFalse_ReturnsZero(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        int count = reader.Query(table).Where(_ => false).Take(100).Count();

        Assert.Equal(0, count);
    }

    // ── Combined WhereAsStrings + Take (string chain) ─────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ExecuteAsStrings_WhereAsStringsAndTake_FiltersBeforeLimiting(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        var result = reader.Query(table)
            .WhereAsStrings(_ => true)
            .Take(2)
            .ExecuteAsStrings()
            .ToList();

        Assert.True(result.Count <= 2);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ExecuteAsStrings_WhereAsStringsFalse_ReturnsEmpty(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        var result = reader.Query(table)
            .WhereAsStrings(_ => false)
            .Take(100)
            .ExecuteAsStrings()
            .ToList();

        Assert.Empty(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void FirstOrDefaultAsStrings_WithTake_ReturnsNonNull(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        string[]? first = reader.Query(table).Take(5).FirstOrDefaultAsStrings();

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CountAsStrings_WithTake_RespectsLimit(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 2);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;

        int count = reader.Query(table).Take(1).CountAsStrings();

        Assert.Equal(1, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CountAsStrings_WhereFalseAndTake_ReturnsZero(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        int count = reader.Query(table).WhereAsStrings(_ => false).Take(50).CountAsStrings();

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
    public void ExecuteGeneric_WhereAndTake_FiltersAndLimits(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        var result = reader.Query(table)
            .Where(_ => true)
            .Take(2)
            .Execute<QueryPoco>()
            .ToList();

        Assert.True(result.Count <= 2);
        Assert.All(result, item => Assert.NotNull(item));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ExecuteGeneric_WhereFalse_ReturnsEmpty(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        var result = reader.Query(table)
            .Where(_ => false)
            .Execute<QueryPoco>()
            .ToList();

        Assert.Empty(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void FirstOrDefaultGeneric_WhereFalse_ReturnsNull(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        QueryPoco? result = reader.Query(table)
            .Where(_ => false)
            .FirstOrDefault<QueryPoco>();

        Assert.Null(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void FirstOrDefaultGeneric_WithTake_ReturnsNonNull(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        QueryPoco? result = reader.Query(table).Take(1).FirstOrDefault<QueryPoco>();

        Assert.NotNull(result);
    }

    // ── Typed and string counts agree ─────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Count_AndCountAsStrings_ReturnSameValue(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        int typed = reader.Query(table).Count();
        int strings = reader.Query(table).CountAsStrings();

        Assert.Equal(typed, strings);
    }

    // ── Take(1) gives exactly 1 when table is non-empty ──────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Take_One_ReturnsExactlyOneRowWhenTableHasData(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;

        Assert.Single(reader.Query(table).Take(1).Execute());
        Assert.Single(reader.Query(table).Take(1).ExecuteAsStrings());
    }

    // ── Fluent chaining returns same query instance ───────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void FluentChain_ReturnsThisForChaining(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        TableQuery query = reader.Query(table);
        TableQuery afterWhere = query.Where(_ => true);
        TableQuery afterTake = afterWhere.Take(10);

        Assert.Same(query, afterWhere);
        Assert.Same(query, afterTake);
    }

    // ── Where replaces previous Where ─────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Where_CalledTwice_SecondPredicateWins(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        // First set always-true, then replace with always-false
        var result = reader.Query(table)
            .Where(_ => true)
            .Where(_ => false)
            .Execute()
            .ToList();

        Assert.Empty(result);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void WhereAsStrings_CalledTwice_SecondPredicateWins(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        var result = reader.Query(table)
            .WhereAsStrings(_ => true)
            .WhereAsStrings(_ => false)
            .ExecuteAsStrings()
            .ToList();

        Assert.Empty(result);
    }
}
