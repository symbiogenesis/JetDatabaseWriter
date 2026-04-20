namespace JetDatabaseReader.Tests;

using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Tests for TableQuery — both the typed chain and the string chain.
///
/// Typed chain:  Where → Execute / FirstOrDefault / Count
/// String chain: WhereAsStrings → ExecuteAsStrings / FirstOrDefaultAsStrings / CountAsStrings.
/// </summary>
[Collection<ReadOnlyDatabaseFixture>]
public class AccessReaderQueryTests(DatabaseCache db)
{
    // ── Typed chain: Execute ──────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Execute_WithoutFilter_ReturnsAllRows(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        int expected = reader.StreamRows(table).Count();

        IEnumerable<object[]> result = reader.Query(table).Execute();

        Assert.Equal(expected, result.Count());
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Execute_WithTake_LimitsResults(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        IEnumerable<object[]> result = reader.Query(table).Take(3).Execute();

        Assert.True(result.Count() <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Execute_WithWhere_FiltersRows(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        // Always-false filter should return zero rows
        IEnumerable<object[]> none = reader.Query(table)
            .Where(_ => false)
            .Execute();

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Execute_WithWhere_AlwaysTrue_ReturnsAllRows(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        int expected = reader.StreamRows(table).Count();

        IEnumerable<object[]> all = reader.Query(table)
            .Where(_ => true)
            .Execute();

        Assert.Equal(expected, all.Count());
    }

    // ── Typed chain: FirstOrDefault ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void FirstOrDefault_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        string table = stat.Name;
        object[]? first = reader.Query(table).FirstOrDefault();

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void FirstOrDefault_WhenNoRowMatches_ReturnsNull(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        object[]? result = reader.Query(table)
            .Where(_ => false)
            .FirstOrDefault();

        Assert.Null(result);
    }

    // ── Typed chain: Count ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Count_WithoutFilter_MatchesStreamRowsCount(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        int expected = reader.StreamRows(table).Count();

        int count = reader.Query(table).Count();

        Assert.Equal(expected, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Count_WithAlwaysFalseFilter_ReturnsZero(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        int count = reader.Query(table).Where(_ => false).Count();

        Assert.Equal(0, count);
    }

    // ── String chain: ExecuteAsStrings ────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteAsStrings_WithoutFilter_ReturnsAllRows(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        int expected = reader.StreamRowsAsStrings(table).Count();

        IEnumerable<string[]> result = reader.Query(table).ExecuteAsStrings();

        Assert.Equal(expected, result.Count());
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteAsStrings_WithTake_LimitsResults(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        IEnumerable<string[]> result = reader.Query(table).Take(3).ExecuteAsStrings();

        Assert.True(result.Count() <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteAsStrings_WithWhereAsStrings_FiltersRows(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        // Always-false filter should return zero rows
        IEnumerable<string[]> none = reader.Query(table)
            .WhereAsStrings(_ => false)
            .ExecuteAsStrings();

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteAsStrings_AllCells_AreStringOrNull(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        foreach (string[] row in reader.Query(table).Take(20).ExecuteAsStrings())
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
    public void FirstOrDefaultAsStrings_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return; // all tables are empty — nothing to assert
        }

        string table = stat.Name;
        string[]? first = reader.Query(table).FirstOrDefaultAsStrings();

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void FirstOrDefaultAsStrings_WhenNoRowMatches_ReturnsNull(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        string[]? result = reader.Query(table)
            .WhereAsStrings(_ => false)
            .FirstOrDefaultAsStrings();

        Assert.Null(result);
    }

    // ── String chain: CountAsStrings ──────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void CountAsStrings_WithoutFilter_MatchesCount(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        int typed = reader.Query(table).Count();
        int string_ = reader.Query(table).CountAsStrings();

        Assert.Equal(typed, string_);
    }

    // ── Take affects both chains ──────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Take_AppliedOnce_AffectsBothExecuteAndExecuteAsStrings(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        const int limit = 2;

        TableQuery query = reader.Query(table).Take(limit);

        Assert.True(query.Execute().Count() <= limit);
        Assert.True(query.ExecuteAsStrings().Count() <= limit);
    }

    // ── Generic chain: Execute<T> ─────────────────────────────────────

    private sealed class QueryRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteGeneric_WithoutFilter_RowCountMatchesExecute(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        int typedCount = reader.Query(table).Execute().Count();
        int genericCount = reader.Query(table).Execute<QueryRow>().Count();

        Assert.Equal(typedCount, genericCount);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteGeneric_WithTake_LimitsResults(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        IEnumerable<QueryRow> result = reader.Query(table).Take(3).Execute<QueryRow>();

        Assert.True(result.Count() <= 3);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteGeneric_WithWhere_FiltersRows(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        IEnumerable<QueryRow> none = reader.Query(table)
            .Where(_ => false)
            .Execute<QueryRow>();

        Assert.Empty(none);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void ExecuteGeneric_ReturnsNonNullInstances(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        foreach (QueryRow item in reader.Query(table).Take(20).Execute<QueryRow>())
        {
            Assert.NotNull(item);
        }
    }

    // ── Generic chain: FirstOrDefault<T> ──────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void FirstOrDefaultGeneric_WithoutFilter_ReturnsNonNull(string path)
    {
        var reader = db.Get(path);
        TableStat? stat = reader.GetTableStats().FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        string table = stat.Name;
        QueryRow? first = reader.Query(table).FirstOrDefault<QueryRow>();

        Assert.NotNull(first);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void FirstOrDefaultGeneric_WhenNoRowMatches_ReturnsNull(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];

        QueryRow? result = reader.Query(table)
            .Where(_ => false)
            .FirstOrDefault<QueryRow>();

        Assert.Null(result);
    }

    // ── Generic chain: Take affects Execute<T> ────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public void Take_AffectsExecuteGeneric(string path)
    {
        var reader = db.Get(path);
        string table = reader.ListTables()[0];
        const int limit = 2;

        Assert.True(reader.Query(table).Take(limit).Execute<QueryRow>().Count() <= limit);
    }
}
