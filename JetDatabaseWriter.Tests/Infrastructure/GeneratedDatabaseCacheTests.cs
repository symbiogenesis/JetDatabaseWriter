namespace JetDatabaseWriter.Tests.Infrastructure;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/// <summary>Checks generated fixture reuse and mutation isolation between parallel tests.</summary>
public sealed class GeneratedDatabaseCacheTests
{
    [Fact]
    public async Task ConcurrentCopies_BuildSeedOnce_AndRemainIndependentAndExpandable()
    {
        var cache = new GeneratedDatabaseCache();
        int builds = 0;
        var complete = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<byte[]> CreateSeed()
        {
            Interlocked.Increment(ref builds);
            return complete.Task;
        }

        Task<MemoryStream> firstCopy = cache.CopyToStreamAsync("seed", CreateSeed).AsTask();
        Task<MemoryStream> secondCopy = cache.CopyToStreamAsync("seed", CreateSeed).AsTask();
        Assert.Equal(1, builds);
        complete.SetResult([1, 2, 3]);
        await using MemoryStream first = await firstCopy;
        await using MemoryStream second = await secondCopy;

        Assert.Equal(0, first.Position);
        Assert.Equal(0, second.Position);
        first.WriteByte(9);
        first.Position = first.Length;
        first.WriteByte(4);
        Assert.Equal(new byte[] { 9, 2, 3, 4 }, first.ToArray());
        Assert.Equal(new byte[] { 1, 2, 3 }, second.ToArray());

        await using MemoryStream later = await cache.CopyToStreamAsync("seed", CreateSeed);
        Assert.Equal(new byte[] { 1, 2, 3 }, later.ToArray());
        Assert.Equal(1, builds);
    }

    [Fact]
    public async Task ConcurrentArrayCopies_BuildSeedOnce_AndRemainIndependentOfStreams()
    {
        var cache = new GeneratedDatabaseCache();
        int builds = 0;
        var complete = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<byte[]> CreateSeed()
        {
            Interlocked.Increment(ref builds);
            return complete.Task;
        }

        Task<byte[]> firstCopy = cache.CopyToArrayAsync("seed", CreateSeed).AsTask();
        Task<byte[]> secondCopy = cache.CopyToArrayAsync("seed", CreateSeed).AsTask();
        Assert.Equal(1, builds);
        complete.SetResult([1, 2, 3]);
        byte[] first = await firstCopy;
        byte[] second = await secondCopy;

        first[0] = 9;
        Assert.Equal(new byte[] { 1, 2, 3 }, second);
        second[1] = 8;
        Assert.Equal(new byte[] { 9, 2, 3 }, first);

        byte[] later = await cache.CopyToArrayAsync("seed", CreateSeed);
        Assert.Equal(new byte[] { 1, 2, 3 }, later);
        await using MemoryStream stream = await cache.CopyToStreamAsync("seed", CreateSeed);
        Assert.Equal(new byte[] { 1, 2, 3 }, stream.ToArray());
        Assert.Equal(1, builds);
    }

    [Fact]
    public async Task DifferentSeedKeys_BuildSeparateImages()
    {
        var cache = new GeneratedDatabaseCache();
        await using MemoryStream first = await cache.CopyToStreamAsync("Jet3", () => Task.FromResult(new byte[] { 3 }));
        await using MemoryStream second = await cache.CopyToStreamAsync("Jet4", () => Task.FromResult(new byte[] { 4 }));
        Assert.Equal(new byte[] { 3 }, first.ToArray());
        Assert.Equal(new byte[] { 4 }, second.ToArray());
    }
}
