namespace JetDatabaseWriter.Tests.Writer;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using Xunit;

/// <summary>Checks once-only baseline creation and immutable image ownership across fault shards.</summary>
public sealed class StatementAtomicityBaselineCacheTests
{
    [Fact]
    public async Task ConcurrentShards_ReuseOneBaselineWithoutSharingMutableImages()
    {
        var cache = new StatementAtomicityBaselineCache();
        var complete = new TaskCompletionSource<StatementAtomicityBaseline>(TaskCreationOptions.RunContinuationsAsynchronously);
        int builds = 0;
        Task<StatementAtomicityBaseline> Create()
        {
            Interlocked.Increment(ref builds);
            return complete.Task;
        }

        Task<StatementAtomicityBaseline> firstRequest = cache.GetAsync(DatabaseFormat.Jet3Mdb, "Insert", Create).AsTask();
        Task<StatementAtomicityBaseline> secondRequest = cache.GetAsync(DatabaseFormat.Jet3Mdb, "Insert", Create).AsTask();
        Assert.Equal(1, builds);
        byte[] original = [1, 2];
        byte[] completed = [3, 4, 5];
        var baseline = new StatementAtomicityBaseline(original, completed, writes: 7, reads: 9);
        original[0] = 99;
        completed[0] = 88;
        complete.SetResult(baseline);

        StatementAtomicityBaseline first = await firstRequest;
        Assert.Same(first, await secondRequest);
        Assert.Same(first, await cache.GetAsync(DatabaseFormat.Jet3Mdb, "Insert", Create));
        Assert.Equal(new byte[] { 1, 2 }, first.Original.Span);
        Assert.Equal(new byte[] { 3, 4, 5 }, first.Completed.Span);
        Assert.Equal(7, first.Writes);
        Assert.Equal(9, first.Reads);
        Assert.Equal(1, builds);
    }

    [Fact]
    public async Task FormatAndStatementKeys_KeepIndependentBaselines()
    {
        var cache = new StatementAtomicityBaselineCache();
        var insert = new StatementAtomicityBaseline([1], [2], writes: 3, reads: 4);
        var update = new StatementAtomicityBaseline([5], [6], writes: 7, reads: 8);
        var jet4 = new StatementAtomicityBaseline([9], [10], writes: 11, reads: 12);
        Assert.Same(insert, await cache.GetAsync(DatabaseFormat.Jet3Mdb, "Insert", () => Task.FromResult(insert)));
        Assert.Same(update, await cache.GetAsync(DatabaseFormat.Jet3Mdb, "Update", () => Task.FromResult(update)));
        Assert.Same(jet4, await cache.GetAsync(DatabaseFormat.Jet4Mdb, "Insert", () => Task.FromResult(jet4)));
    }
}
