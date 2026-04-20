namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Tests for all async methods:
/// ListTablesAsync, ReadTableAsync, GetStatisticsAsync, ReadAllTablesAsync, ReadAllTablesAsStringsAsync.
/// </summary>
[Collection<ReadOnlyDatabaseFixture>]
public class AccessReaderAsyncTests(DatabaseCache db)
{
    // ── OpenAsync ─────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.AllExisting), MemberType = typeof(TestDatabases))]
    public async Task OpenAsync_WhenFileExists_IsNotPasswordProtected(string path)
    {
        using var reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        Assert.NotNull(reader);
    }

    [Fact]
    public async Task OpenAsync_WhenFileNotFound_ThrowsFileNotFoundException()
    {
        await Assert.ThrowsAsync<FileNotFoundException>(() =>
            AccessReader.OpenAsync(@"C:\no\such\file.mdb", cancellationToken: TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task OpenAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            AccessReader.OpenAsync(@"C:\cancel\me.mdb", cancellationToken: cts.Token).AsTask());
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task OpenAsync_ReturnedReader_ImplementsIAsyncDisposable(string path)
    {
        await using var reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        Assert.IsAssignableFrom<IAsyncDisposable>(reader);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow(string path)
    {
        var reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        await reader.DisposeAsync();
        Exception? ex = await Record.ExceptionAsync(() => reader.DisposeAsync().AsTask());

        Assert.Null(ex);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ListTables_AfterDisposeAsync_ThrowsObjectDisposedException(string path)
    {
        var reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        await reader.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => reader.ListTables());
    }

    // ── ListTablesAsync ───────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTablesAsync_ReturnsNonEmptyList(string path)
    {
        var reader = db.Get(path);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(tables);
        Assert.NotEmpty(tables);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTablesAsync_MatchesSyncListTables(string path)
    {
        var reader = db.Get(path);

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        List<string> sync = reader.ListTables();
#pragma warning restore CA1849
        List<string> async_ = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equivalent(sync, async_);
    }

    // ── ReadTableAsync ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsync_ReturnsNonNullDataTable(string path)
    {
        var reader = db.Get(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = (await reader.ReadTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsync_ColumnTypes_AreTyped(string path)
    {
        var reader = db.Get(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        DataTable dt = (await reader.ReadTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(meta[i].ClrType, dt.Columns[i].DataType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsync_RowCount_MatchesSyncReadTable(string path)
    {
        var reader = db.Get(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        DataTable syncDt = reader.ReadTable(table)!;
#pragma warning restore CA1849
        DataTable asyncDt = (await reader.ReadTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(syncDt.Rows.Count, asyncDt.Rows.Count);
    }

    // ── GetStatisticsAsync ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetStatisticsAsync_MatchesSyncGetStatistics(string path)
    {
        var reader = db.Get(path);

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        DatabaseStatistics sync = reader.GetStatistics();
#pragma warning restore CA1849
        DatabaseStatistics async_ = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(sync.TotalPages, async_.TotalPages);
        Assert.Equal(sync.TableCount, async_.TableCount);
        Assert.Equal(sync.Version, async_.Version);
    }

    // ── ReadAllTablesAsync ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsync_ContainsAllTableNames(string path)
    {
        var reader = db.Get(path);
        List<string> expected = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equivalent(expected, all.Keys);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsync_RowCounts_MatchSyncReadAllTables(string path)
    {
        var reader = db.Get(path);

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        Dictionary<string, DataTable> sync = reader.ReadAllTables();
#pragma warning restore CA1849
        Dictionary<string, DataTable> async_ = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);

        foreach (string name in sync.Keys)
        {
            Assert.Equal(sync[name].Rows.Count, async_[name].Rows.Count);
        }
    }

    // ── ReadAllTablesAsStringsAsync ────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsStringsAsync_AllColumns_AreStringType(string path)
    {
        var reader = db.Get(path);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsStringsAsync(cancellationToken: TestContext.Current.CancellationToken);

        foreach (var (_, dt) in all)
            foreach (DataColumn col in dt.Columns)
            {
                Assert.Equal(typeof(string), col.DataType);
            }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadAllTablesAsStringsAsync_RowCounts_MatchReadAllTablesAsync(string path)
    {
        var reader = db.Get(path);

        Dictionary<string, DataTable> typed = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);
        Dictionary<string, DataTable> strings = await reader.ReadAllTablesAsStringsAsync(cancellationToken: TestContext.Current.CancellationToken);

        foreach (string name in typed.Keys)
        {
            Assert.Equal(typed[name].Rows.Count, strings[name].Rows.Count);
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
        var reader = db.Get(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

#pragma warning disable CA1849 // Intentional: comparing sync result against async result
        List<AsyncGenericRow> sync = reader.ReadTable<AsyncGenericRow>(table, 100);
#pragma warning restore CA1849
        List<AsyncGenericRow> async_ = await reader.ReadTableAsync<AsyncGenericRow>(table, 100, TestContext.Current.CancellationToken);

        Assert.Equal(sync.Count, async_.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsyncGeneric_ReturnsNonNullInstances(string path)
    {
        var reader = db.Get(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        List<AsyncGenericRow> items = await reader.ReadTableAsync<AsyncGenericRow>(table, 10, TestContext.Current.CancellationToken);

        Assert.All(items, item => Assert.NotNull(item));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsyncGeneric_RowCount_MatchesNonGenericAsync(string path)
    {
        var reader = db.Get(path);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        TableResult nonGeneric = await reader.ReadTableAsync(table, 100, TestContext.Current.CancellationToken);
        List<AsyncGenericRow> generic = await reader.ReadTableAsync<AsyncGenericRow>(table, 100, TestContext.Current.CancellationToken);

        Assert.Equal(nonGeneric.Rows.Count, generic.Count);
    }
}
