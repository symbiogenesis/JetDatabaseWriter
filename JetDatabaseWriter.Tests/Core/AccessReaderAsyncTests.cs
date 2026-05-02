namespace JetDatabaseWriter.Tests.Core;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for async-specific behaviour (cancellation, IAsyncDisposable, idempotency).
/// Catalog/metadata operations are covered in AccessReaderCatalogTests and table-content reads in AccessReaderReadTests.
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

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DisposeAsync_WaitsForInFlightRead(string path)
    {
        TimeSpan timeout = TimeSpan.FromSeconds(10);

        if (!File.Exists(path))
        {
            return;
        }

        byte[] bytes = await db.GetFileAsync(path, TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: false,
            TestContext.Current.CancellationToken);

        TableStat? stat = (await reader.GetTableStatsAsync(TestContext.Current.CancellationToken))
            .FirstOrDefault(s => s.RowCount > 0);
        if (stat == null)
        {
            return;
        }

        var readReachedProgress = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRead = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        int progressObserved = 0;
        var progress = new SyncProgress<long>(_ =>
        {
            if (Interlocked.Exchange(ref progressObserved, 1) == 0)
            {
                readReachedProgress.TrySetResult(null);
                releaseRead.Task.GetAwaiter().GetResult();
            }
        });

        Task readTask = Task.Run(
            () => reader.ReadDataTableAsync(
                stat.Name,
                progress: progress,
                cancellationToken: TestContext.Current.CancellationToken).AsTask(),
            TestContext.Current.CancellationToken);

        await readReachedProgress.Task.WaitAsync(timeout, TestContext.Current.CancellationToken);

        Task disposeTask = reader.DisposeAsync().AsTask();

        Assert.False(disposeTask.IsCompleted);

        releaseRead.TrySetResult(null);

        await readTask.WaitAsync(timeout, TestContext.Current.CancellationToken);
        await disposeTask.WaitAsync(timeout, TestContext.Current.CancellationToken);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DisposeAsync_DoesNotBreakNestedPublicCallsOfActiveOperation(string path)
    {
        TimeSpan timeout = TimeSpan.FromSeconds(10);

        if (!File.Exists(path))
        {
            return;
        }

        byte[] bytes = await db.GetFileAsync(path, TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: false,
            TestContext.Current.CancellationToken);

        if ((await reader.ListTablesAsync(TestContext.Current.CancellationToken)).Count == 0)
        {
            return;
        }

        var outerOperationReachedNestedCall = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOuterOperation = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        int progressObserved = 0;
        var progress = new SyncProgress<TableProgress>(_ =>
        {
            if (Interlocked.Exchange(ref progressObserved, 1) == 0)
            {
                outerOperationReachedNestedCall.TrySetResult(null);
                releaseOuterOperation.Task.GetAwaiter().GetResult();
            }
        });

        var readAllTask = Task.Run(
            () => reader.ReadAllTablesAsync(
                progress,
                TestContext.Current.CancellationToken).AsTask(),
            TestContext.Current.CancellationToken);

        await outerOperationReachedNestedCall.Task.WaitAsync(timeout, TestContext.Current.CancellationToken);

        Task disposeTask = reader.DisposeAsync().AsTask();

        Assert.False(disposeTask.IsCompleted);

        releaseOuterOperation.TrySetResult(null);

        var tables = await readAllTask.WaitAsync(timeout, TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);
        await disposeTask.WaitAsync(timeout, TestContext.Current.CancellationToken);
    }

    private sealed class SyncProgress<T>(Action<T> action) : IProgress<T>
    {
        private readonly Action<T> _action = action;

        public void Report(T value) => _action(value);
    }
}
