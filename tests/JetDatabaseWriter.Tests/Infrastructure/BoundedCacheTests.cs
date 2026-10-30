namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using Xunit;

/// <summary>Compiled metadata retention is bounded without caching failed construction.</summary>
public sealed class BoundedCacheTests
{
    [Fact]
    public void Hits_PromoteEntriesBeforeEviction()
    {
        var cache = new BoundedCache<string, object>(2);
        object first = cache.GetOrCreate("first", static _ => new object());
        object second = cache.GetOrCreate("second", static _ => new object());
        Assert.Same(first, cache.GetOrCreate("first", static _ => new object()));

        _ = cache.GetOrCreate("third", static _ => new object());

        Assert.Equal(2, cache.Count);
        Assert.Same(first, cache.GetOrCreate("first", static _ => new object()));
        Assert.NotSame(second, cache.GetOrCreate("second", static _ => new object()));
    }

    [Fact]
    public void NullResult_IsRetainedButFailedConstructionCanRetry()
    {
        var cache = new BoundedCache<string, object?>(2);
        Assert.Null(cache.GetOrCreate("unsupported", static _ => null));
        Assert.Null(cache.GetOrCreate("unsupported", static _ => throw new InvalidOperationException()));
        Assert.Throws<InvalidOperationException>(() => cache.GetOrCreate("retry", static _ => throw new InvalidOperationException()));
        Assert.Equal(1, cache.Count);

        object expected = new();
        Assert.Same(expected, cache.GetOrCreate("retry", _ => expected));
        Assert.Equal(2, cache.Count);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void NonpositiveCapacity_DoesNotRetainValues(int capacity)
    {
        var cache = new BoundedCache<string, object>(capacity);
        object first = cache.GetOrCreate("value", static _ => new object());

        Assert.NotSame(first, cache.GetOrCreate("value", static _ => new object()));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task ConcurrentMisses_PublishOneValue()
    {
        var cache = new BoundedCache<int, object>(4);
        int calls = 0;
        object Create(int key)
        {
            Assert.Equal(7, key);
            Interlocked.Increment(ref calls);
            return new object();
        }

        object[] values = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(
            () => cache.GetOrCreate(7, Create), TestContext.Current.CancellationToken)));

        Assert.All(values, value => Assert.Same(values[0], value));
        Assert.Equal(1, calls);
        Assert.Equal(1, cache.Count);
    }
}
