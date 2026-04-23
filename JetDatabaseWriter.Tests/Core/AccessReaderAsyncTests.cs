namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Tests for async-specific behaviour (cancellation, IAsyncDisposable, idempotency).
/// Core read operations are covered in AccessReaderCoreTests and AccessReaderReadTests.
/// </summary>
public class AccessReaderAsyncTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── OpenAsync ─────────────────────────────────────────────────────

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

    // ── ListTablesAsync ───────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ListTablesAsync_IsIdempotent(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> first = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        List<string> second = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equivalent(first, second);
    }

    // ── ReadTableAsync ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsync_RowCount_IsIdempotent(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable firstDt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        DataTable secondDt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(firstDt.Rows.Count, secondDt.Rows.Count);
    }

    // ── GetStatisticsAsync ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task GetStatisticsAsync_ReturnsValidStatistics(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TotalPages > 0);
        Assert.True(stats.TableCount > 0);
        Assert.False(string.IsNullOrEmpty(stats.Version));
    }
}
