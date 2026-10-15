namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Queries;
using Xunit;

/// <summary>Guards evaluation timing and short-circuiting when query tails compose async LINQ.</summary>
public sealed class TailAsyncLinqParityTests
{
    [Theory]
    [InlineData("Where", 1)]
    [InlineData("Select", 1)]
    [InlineData("SkipWhile", 1)]
    [InlineData("TakeWhile", 1)]
    [InlineData("OrderBy", 3)]
    public async Task CallbackFailure_IsDeferredAndDisposesSource(string operation, int expectedReads)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var probe = new SourceProbe();
        IQueryable<int> root = Array.Empty<int>().AsQueryable();
        IQueryable<int> query = operation switch
        {
            "Where" => root.Where(i => Fail(i) > 0),
            "Select" => root.Select(i => Fail(i)),
            "SkipWhile" => root.SkipWhile(i => Fail(i) > 0),
            "TakeWhile" => root.TakeWhile(i => Fail(i) > 0),
            "OrderBy" => root.OrderBy(i => Fail(i)),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };
        IAsyncEnumerable<object?> tail = InMemoryTail.Apply(probe.ReadAsync(ct), query.Expression, root.Expression, ct);
        await using IAsyncEnumerator<object?> enumerator = tail.GetAsyncEnumerator(ct);

        Assert.Equal(0, probe.Reads);
        Assert.False(probe.Disposed);
        FormatException error = await Assert.ThrowsAsync<FormatException>(async () => await enumerator.MoveNextAsync());

        Assert.Equal("callback", error.Message);
        Assert.Equal(expectedReads, probe.Reads);
        Assert.True(probe.Disposed);
    }

    [Theory]
    [InlineData("Take", -1, 0)]
    [InlineData("Take", 0, 0)]
    [InlineData("Take", 2, 2)]
    [InlineData("TakeLast", -1, 0)]
    [InlineData("TakeLast", 0, 0)]
    [InlineData("Skip", -1, 3)]
    [InlineData("SkipLast", -1, 3)]
    public async Task Paging_ReadsOnlyRequiredElements(string operation, int count, int expectedReads)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var probe = new SourceProbe();
        IQueryable<int> root = Array.Empty<int>().AsQueryable();
        IQueryable<int> query = operation switch
        {
            "Take" => root.Take(count),
            "TakeLast" => root.TakeLast(count),
            "Skip" => root.Skip(count),
            "SkipLast" => root.SkipLast(count),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };

        List<object?> result = await InMemoryTail.Apply(probe.ReadAsync(ct), query.Expression, root.Expression, ct).ToListAsync(ct);

        Assert.Equal(Enumerable.Range(1, expectedReads).Cast<object?>(), result);
        Assert.Equal(expectedReads, probe.Reads);
        Assert.Equal(expectedReads > 0, probe.Disposed);
    }

    [Theory]
    [InlineData("Concat", true)]
    [InlineData("Union", true)]
    [InlineData("Intersect", false)]
    [InlineData("Except", false)]
    public async Task SecondSourceFailure_PreservesSourceEvaluationOrder(string operation, bool readsFirst)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var probe = new SourceProbe();
        IQueryable<int> root = Array.Empty<int>().AsQueryable();
        IEnumerable<int> second = Enumerable.Range(1, 1).Select(Fail);
        IQueryable<int> query = operation switch
        {
            "Concat" => root.Concat(second),
            "Union" => root.Union(second),
            "Intersect" => root.Intersect(second),
            "Except" => root.Except(second),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };
        IAsyncEnumerable<object?> tail = InMemoryTail.Apply(probe.ReadAsync(ct), query.Expression, root.Expression, ct);
        await using IAsyncEnumerator<object?> enumerator = tail.GetAsyncEnumerator(ct);
        Assert.Equal(0, probe.Reads);

        if (readsFirst)
        {
            for (int i = 1; i <= 3; i++)
            {
                Assert.True(await enumerator.MoveNextAsync());
                Assert.Equal(i, enumerator.Current);
            }
        }

        FormatException error = await Assert.ThrowsAsync<FormatException>(async () => await enumerator.MoveNextAsync());

        Assert.Equal("callback", error.Message);
        Assert.Equal(readsFirst ? 3 : 0, probe.Reads);
        Assert.Equal(readsFirst, probe.Disposed);
    }

    [Fact]
    public async Task OrderingTake_DisposesSourceBeforeAllKeyCallbacksAndPreservesLateFailure()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var probe = new SourceProbe();
        IQueryable<int> root = Array.Empty<int>().AsQueryable();
        IQueryable<int> query = root.OrderBy(i => probe.Key(i, "first"))
            .ThenBy(i => probe.Key(i, "second"))
            .Take(1);
        IAsyncEnumerable<object?> tail = InMemoryTail.Apply(probe.ReadAsync(ct), query.Expression, root.Expression, ct);
        await using IAsyncEnumerator<object?> enumerator = tail.GetAsyncEnumerator(ct);

        Assert.Empty(probe.Keys);
        await Assert.ThrowsAsync<FormatException>(async () => await enumerator.MoveNextAsync());

        Assert.Equal(3, probe.Reads);
        Assert.Equal([("first", 1), ("first", 2), ("first", 3), ("second", 1), ("second", 2), ("second", 3)], probe.Keys);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Intersect_ObservesFirstSourceFailureEvenWhenNoMatchesRemain(bool initiallyHasMatch)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var probe = new SourceProbe();
        IQueryable<int> root = Array.Empty<int>().AsQueryable();
        int[] second = initiallyHasMatch ? [1] : [];
        IQueryable<int> query = root.Select(i => i == 2 ? Fail(i) : i).Intersect(second);
        IAsyncEnumerable<object?> tail = InMemoryTail.Apply(probe.ReadAsync(ct), query.Expression, root.Expression, ct);
        await using IAsyncEnumerator<object?> enumerator = tail.GetAsyncEnumerator(ct);

        if (initiallyHasMatch)
        {
            Assert.True(await enumerator.MoveNextAsync());
            Assert.Equal(1, enumerator.Current);
        }

        await Assert.ThrowsAsync<FormatException>(async () => await enumerator.MoveNextAsync());

        Assert.Equal(2, probe.Reads);
        Assert.True(probe.Disposed);
    }

    [Theory]
    [InlineData("Intersect", false)]
    [InlineData("Intersect", true)]
    [InlineData("Except", false)]
    [InlineData("Except", true)]
    public async Task EmptyFirstSource_StillObservesSecondSourceFailure(string operation, bool knownEmpty)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var probe = new SourceProbe();
        IQueryable<int> root = Array.Empty<int>().AsQueryable();
        IQueryable<int> first = knownEmpty ? root.Take(0) : root.Where(i => i < 0);
        IEnumerable<int> second = Enumerable.Range(1, 1).Select(Fail);
        IQueryable<int> query = operation == "Intersect" ? first.Intersect(second) : first.Except(second);
        IAsyncEnumerable<object?> tail = InMemoryTail.Apply(probe.ReadAsync(ct), query.Expression, root.Expression, ct);
        await using IAsyncEnumerator<object?> enumerator = tail.GetAsyncEnumerator(ct);

        await Assert.ThrowsAsync<FormatException>(async () => await enumerator.MoveNextAsync());

        Assert.Equal(0, probe.Reads);
        Assert.False(probe.Disposed);
    }

    private static int Fail(int value) => throw new FormatException("callback");

    private sealed class SourceProbe
    {
        public int Reads { get; private set; }

        public bool Disposed { get; private set; }

        public List<(string Name, int Value)> Keys { get; } = [];

        public int Key(int value, string name)
        {
            Assert.True(this.Disposed);
            this.Keys.Add((name, value));
            return name == "second" && value == 3 ? Fail(value) : 0;
        }

        public async IAsyncEnumerable<object?> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            try
            {
                for (int i = 1; i <= 3; i++)
                {
                    await Task.Yield();
                    cancellationToken.ThrowIfCancellationRequested();
                    this.Reads++;
                    yield return i;
                }
            }
            finally
            {
                this.Disposed = true;
            }
        }
    }
}
