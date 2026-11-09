namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Threading.Tasks;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Default query ordering preserves exact same-type keys and refuses implicit coercion.</summary>
public sealed class QueryKeyOrderingTests
{
    private static readonly object[][] RefusedPairs = [[1, 1.4d], [1m, 1.4d], ["1", 1], [new object(), new object()]];

    [Fact]
    public void SameTypeKeys_ObeyAntisymmetryAndTransitivity()
    {
        object?[][] domains =
        [
            [null, DBNull.Value, int.MinValue, -1, 0, 1, int.MaxValue],
            [null, DBNull.Value, long.MinValue, 9007199254740992L, 9007199254740993L, long.MaxValue],
            [null, DBNull.Value, double.NaN, double.NegativeInfinity, -0d, double.Epsilon, 1.4d, double.PositiveInfinity],
            [null, DBNull.Value, decimal.MinValue, -0.1m, 0m, 0.1m, decimal.MaxValue],
            [null, DBNull.Value, string.Empty, "1", "10", "2", "A", "a"],
        ];
        foreach (object?[] domain in domains)
        {
            foreach (object? left in domain)
            {
                foreach (object? right in domain)
                {
                    int comparison = QueryKeyComparer.Instance.Compare(left, right);
                    Assert.Equal(Math.Sign(comparison), -Math.Sign(QueryKeyComparer.Instance.Compare(right, left)));
                    foreach (object? last in domain)
                    {
                        if (comparison <= 0 && QueryKeyComparer.Instance.Compare(right, last) <= 0)
                        {
                            Assert.True(QueryKeyComparer.Instance.Compare(left, last) <= 0);
                        }
                    }
                }
            }
        }
    }

    [Fact]
    public void MixedOrUnsupportedKeys_RefuseBothArgumentOrders()
    {
        foreach (object[] pair in RefusedPairs)
        {
            Assert.Throws<ArgumentException>(() => QueryKeyComparer.Instance.Compare(pair[0], pair[1]));
            Assert.Throws<ArgumentException>(() => QueryKeyComparer.Instance.Compare(pair[1], pair[0]));
        }
    }

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    [InlineData("cached-tail")]
    public async Task DefaultObjectKeys_PreserveNullsAndExactValuesAcrossPipelines(string pipeline)
    {
        object?[][] domains =
        [
            [2, null, 1, DBNull.Value, 0],
            [9007199254740993L, null, 9007199254740992L, DBNull.Value, long.MinValue],
            [1.4d, null, 1d, DBNull.Value, double.NaN],
            [0.1m, null, decimal.MaxValue, DBNull.Value, decimal.MinValue],
            ["2", null, "10", DBNull.Value, "1"],
        ];
        foreach (object?[] keys in domains)
        {
            Item[] items = keys.Select((key, index) => new Item(index, key)).ToArray();
            IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
            IQueryable<Item> query = root.OrderBy(item => item.Key).ThenBy(item => item.Id);
            Func<IEnumerable<Item>, IAsyncEnumerable<Item>> execute = Prepare(pipeline, root, query);

            List<Item> actual = await execute(items).ToListAsync(TestContext.Current.CancellationToken);

            Assert.Equal(
                items.OrderBy(item => item.Key is DBNull ? null : item.Key).ThenBy(item => item.Id).Select(item => item.Id),
                actual.Select(item => item.Id));
        }
    }

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    [InlineData("cached-tail")]
    public async Task MixedOrUnsupportedKeys_RefuseAcrossPipelines(string pipeline)
    {
        foreach (object[] pair in RefusedPairs)
        {
            foreach (object[] keys in new[] { pair, pair.Reverse().ToArray() })
            {
                Item[] items = keys.Select((key, index) => new Item(index, key)).ToArray();
                IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
                IQueryable<Item> query = root.OrderBy(item => item.Key);
                Func<IEnumerable<Item>, IAsyncEnumerable<Item>> execute = Prepare(pipeline, root, query);

                InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await execute(items).ToListAsync(TestContext.Current.CancellationToken));

                Assert.IsType<ArgumentException>(error.InnerException);
            }
        }
    }

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    [InlineData("cached-tail")]
    public async Task TypedGenericComparableKeys_AgreeAcrossPipelines(string pipeline)
    {
        Item[] items = [new(1, null), new(2, null), new(10, null)];
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IQueryable<Item> query = root.OrderBy(item => new GenericKey(-item.Id));

        List<Item> actual = await Prepare(pipeline, root, query)(items).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([10, 2, 1], actual.Select(item => item.Id));
    }

    [Theory]
    [InlineData("tail")]
    [InlineData("cached-tail")]
    public async Task ExplicitObjectComparer_KeepsItsMixedTypePolicy(string pipeline)
    {
        Item[] items = [new(1, 1.4d), new(2, 1), new(3, 1.2m)];
        IComparer<object?> comparer = Comparer<object?>.Create(static (left, right) =>
            Convert.ToDecimal(left, System.Globalization.CultureInfo.InvariantCulture)
                .CompareTo(Convert.ToDecimal(right, System.Globalization.CultureInfo.InvariantCulture)));
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IQueryable<Item> query = root.OrderBy(item => item.Key, comparer);

        List<Item> actual = await Prepare(pipeline, root, query)(items).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([2, 3, 1], actual.Select(item => item.Id));
    }

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    [InlineData("cached-tail")]
    public async Task InterfaceKeys_RefuseMixedRuntimeTypesAcrossPipelines(string pipeline)
    {
        Item[] items = [new(1, 1.4d), new(2, 1)];
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IQueryable<Item> query = root.OrderBy(item => (IComparable?)item.Key);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Prepare(pipeline, root, query)(items).ToListAsync(TestContext.Current.CancellationToken));

        Assert.IsType<ArgumentException>(error.InnerException);
    }

    [Theory]
    [InlineData("tail")]
    [InlineData("cached-tail")]
    public async Task ExplicitNullComparer_UsesSharedDefaultPolicy(string pipeline)
    {
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IQueryable<Item> query = root.OrderBy(item => item.Key, comparer: null);
        Func<IEnumerable<Item>, IAsyncEnumerable<Item>> execute = Prepare(pipeline, root, query);

        List<Item> actual = await execute([new(1, 1), new(2, DBNull.Value), new(3, null), new(4, 0)])
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([2, 3, 4, 1], actual.Select(item => item.Id));
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await execute([new(1, 1), new(2, 1.4d)]).ToListAsync(TestContext.Current.CancellationToken));
        Assert.IsType<ArgumentException>(error.InnerException);
    }

    [Fact(DisableParallelization = true)]
    public void DefaultComparer_DoesNotRetainCollectibleKeyType()
    {
        (WeakReference context, WeakReference type) = CompareCollectibleType();
        for (int attempt = 0; attempt < 10 && (context.IsAlive || type.IsAlive); attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.False(type.IsAlive, "The default ordering caches retained the collectible key type.");
        Assert.False(context.IsAlive, "The default ordering caches retained the collectible assembly load context.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Context, WeakReference Type) CompareCollectibleType()
    {
        var context = new AssemblyLoadContext("QueryKeyOrdering-" + Guid.NewGuid().ToString("N"), isCollectible: true);
        try
        {
            Type type = context.LoadFromAssemblyPath(typeof(SnapshotPlaylistRow).Assembly.Location)
                .GetType("JetDatabaseWriter.TestSupport.SnapshotPlaylistRow", throwOnError: true)
                ?? throw new InvalidOperationException("The independent key type specimen is missing.");
            Assert.Equal(0, QueryKeyComparer.For(type).Compare(null, DBNull.Value));
            return (new WeakReference(context), new WeakReference(type));
        }
        finally
        {
            context.Unload();
        }
    }

    private static Func<IEnumerable<Item>, IAsyncEnumerable<Item>> Prepare(string pipeline, IQueryable<Item> root, IQueryable<Item> query)
    {
        if (pipeline is "tail" or "cached-tail")
        {
            if (pipeline == "cached-tail")
            {
                Expression expression = new ReplaceSourceVisitor(root.Expression, root.Select(item => item).Expression).Visit(query.Expression)!;
                query = root.Provider.CreateQuery<Item>(expression);
                (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(query.Expression);
                Assert.NotNull(plan.TailTemplate);
                return source => InMemoryTail.Apply(source.Cast<object?>().ToAsyncEnumerable(), query.Expression, boundary, plan, TestContext.Current.CancellationToken)
                    .Select(item => (Item)item!);
            }

            return source => InMemoryTail.Apply(source.Cast<object?>().ToAsyncEnumerable(), query.Expression, root.Expression, TestContext.Current.CancellationToken)
                .Select(item => (Item)item!);
        }

        if (pipeline == "include")
        {
            ParameterExpression parent = Expression.Parameter(typeof(Parent), "parent");
            Expression body = new ReplaceSourceVisitor(root.Expression, Expression.Property(parent, nameof(Parent.Children))).Visit(query.Expression)!;
            var navigation = Expression.Lambda<Func<Parent, IQueryable<Item>>>(body, parent);
            IQueryable<Parent> included = Array.Empty<Parent>().AsQueryable().Include(navigation);
            (AccessQueryPlan plan, _) = AccessQueryTranslator.Translate(included.Expression);
            IReadOnlyList<IncludeOperation> operations = Assert.Single(Assert.Single(plan.IncludePaths)).Operations;
            return source => operations.Aggregate(source.Cast<object>(), static (rows, operation) => operation.Apply(rows))
                .Cast<Item>().ToAsyncEnumerable();
        }

        (AccessQueryPlan rootPlan, _) = AccessQueryTranslator.Translate(query.Expression);
        return source => rootPlan.Stages.Aggregate(source.ToAsyncEnumerable(), static (rows, stage) => stage.Apply(rows, TestContext.Current.CancellationToken));
    }

    private sealed record Item(int Id, object? Key);

    private sealed record GenericKey(int Value) : IComparable<GenericKey>
    {
        public int CompareTo(GenericKey? other) => other is null ? 1 : this.Value.CompareTo(other.Value);
    }

#pragma warning disable CA1812 // Translation inspects this type through the navigation expression.
    private sealed class Parent
#pragma warning restore CA1812
    {
        public IQueryable<Item> Children { get; } = Array.Empty<Item>().AsQueryable();
    }

    private sealed class ReplaceSourceVisitor(Expression source, Expression replacement) : ExpressionVisitor
    {
        public override Expression? Visit(Expression? node) => ReferenceEquals(node, source) ? replacement : base.Visit(node);
    }
}
