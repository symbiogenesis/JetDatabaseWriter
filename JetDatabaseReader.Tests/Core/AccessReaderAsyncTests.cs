namespace JetDatabaseReader.Tests;

using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Tests for all async methods:
/// ListTablesAsync, ReadTableAsync, GetStatisticsAsync, ReadAllTablesAsync, ReadAllTablesAsStringsAsync.
/// </summary>
public class AccessReaderAsyncTests
{
    // ── ListTablesAsync ───────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTablesAsync_ReturnsNonEmptyList(string path)
    {
        using var reader = TestDatabases.Open(path);

        List<string> tables = await reader.ListTablesAsync();

        _ = tables.Should().NotBeNullOrEmpty();
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTablesAsync_MatchesSyncListTables(string path)
    {
        using var reader = TestDatabases.Open(path);

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        List<string> sync = reader.ListTables();
#pragma warning restore CA1849
        List<string> async_ = await reader.ListTablesAsync();

        _ = async_.Should().BeEquivalentTo(sync);
    }

    // ── ReadTableAsync ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsync_ReturnsNonNullDataTable(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = (await reader.ListTablesAsync())[0];

        DataTable dt = (await reader.ReadTableAsync(table))!;

        _ = dt.Should().NotBeNull();
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsync_ColumnTypes_AreTyped(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = (await reader.ListTablesAsync())[0];
        var meta = reader.GetColumnMetadata(table);

        DataTable dt = (await reader.ReadTableAsync(table))!;

        for (int i = 0; i < meta.Count; i++)
        {
            _ = dt.Columns[i].DataType.Should().Be(meta[i].ClrType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsync_RowCount_MatchesSyncReadTable(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = (await reader.ListTablesAsync())[0];

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        DataTable syncDt = reader.ReadTable(table)!;
#pragma warning restore CA1849
        DataTable asyncDt = (await reader.ReadTableAsync(table))!;

        _ = asyncDt.Rows.Count.Should().Be(syncDt.Rows.Count);
    }

    // ── GetStatisticsAsync ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetStatisticsAsync_MatchesSyncGetStatistics(string path)
    {
        using var reader = TestDatabases.Open(path);

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        DatabaseStatistics sync = reader.GetStatistics();
#pragma warning restore CA1849
        DatabaseStatistics async_ = await reader.GetStatisticsAsync();

        _ = async_.TotalPages.Should().Be(sync.TotalPages);
        _ = async_.TableCount.Should().Be(sync.TableCount);
        _ = async_.Version.Should().Be(sync.Version);
    }

    // ── ReadAllTablesAsync ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsync_ContainsAllTableNames(string path)
    {
        using var reader = TestDatabases.Open(path);
        List<string> expected = await reader.ListTablesAsync();

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync();

        _ = all.Keys.Should().BeEquivalentTo(expected);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsync_RowCounts_MatchSyncReadAllTables(string path)
    {
        using var reader = TestDatabases.Open(path);

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        Dictionary<string, DataTable> sync = reader.ReadAllTables();
#pragma warning restore CA1849
        Dictionary<string, DataTable> async_ = await reader.ReadAllTablesAsync();

        foreach (string name in sync.Keys)
        {
            _ = async_[name].Rows.Count.Should().Be(
                sync[name].Rows.Count,
                because: $"table '{name}' row count should match");
        }
    }

    // ── ReadAllTablesAsStringsAsync ────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsStringsAsync_AllColumns_AreStringType(string path)
    {
        using var reader = TestDatabases.Open(path);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsStringsAsync();

        foreach (var (_, dt) in all)
            foreach (DataColumn col in dt.Columns)
            {
                _ = col.DataType.Should().Be<string>();
            }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsStringsAsync_RowCounts_MatchReadAllTablesAsync(string path)
    {
        using var reader = TestDatabases.Open(path);

        Dictionary<string, DataTable> typed = await reader.ReadAllTablesAsync();
        Dictionary<string, DataTable> strings = await reader.ReadAllTablesAsStringsAsync();

        foreach (string name in typed.Keys)
        {
            _ = strings[name].Rows.Count.Should().Be(typed[name].Rows.Count);
        }
    }

    // ── ReadTableAsync<T> (generic POCO) ──────────────────────────────

    private sealed class AsyncGenericRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsyncGeneric_RowCount_MatchesSyncGeneric(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = (await reader.ListTablesAsync())[0];

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        List<AsyncGenericRow> sync = reader.ReadTable<AsyncGenericRow>(table, 100);
#pragma warning restore CA1849
        List<AsyncGenericRow> async_ = await reader.ReadTableAsync<AsyncGenericRow>(table, 100);

        _ = async_.Should().HaveCount(sync.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsyncGeneric_ReturnsNonNullInstances(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = (await reader.ListTablesAsync())[0];

        List<AsyncGenericRow> items = await reader.ReadTableAsync<AsyncGenericRow>(table, 10);

        _ = items.Should().AllSatisfy(item => item.Should().NotBeNull());
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsyncGeneric_RowCount_MatchesNonGenericAsync(string path)
    {
        using var reader = TestDatabases.Open(path);
        string table = (await reader.ListTablesAsync())[0];

        TableResult nonGeneric = await reader.ReadTableAsync(table, 100);
        List<AsyncGenericRow> generic = await reader.ReadTableAsync<AsyncGenericRow>(table, 100);

        _ = generic.Should().HaveCount(nonGeneric.Rows.Count);
    }
}
