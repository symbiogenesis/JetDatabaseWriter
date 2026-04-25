namespace JetDatabaseWriter.Tests;

using System;
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
