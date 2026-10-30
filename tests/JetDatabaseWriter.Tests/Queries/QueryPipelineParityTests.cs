namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Queries;
using Xunit;

/// <summary>The root, per-parent include and tail pipelines preserve the same written operator semantics.</summary>
public sealed class QueryPipelineParityTests
{
    private static readonly Item[] Items = [new(1, 2, 10), new(2, 1, 20), new(3, 2, 10), new(4, 1, 10), new(5, 1, 20)];

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    public async Task SeparateOrderingRuns_PreserveStableTiesAndPaging(string pipeline)
    {
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        static IQueryable<Item> Shape(IQueryable<Item> source) => source
            .Where(item => item.Id > 0).OrderBy(item => item.Group).ThenByDescending(item => item.Score)
            .Skip(1).Take(4).OrderByDescending(item => item.Score).ThenBy(item => item.Group);
        Func<IEnumerable<Item>, IAsyncEnumerable<Item>> execute = Prepare(pipeline, root, Shape(root));

        List<Item> actual = await execute(Items).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Shape(Items.AsQueryable()).Select(item => item.Id), actual.Select(item => item.Id));
        Assert.Equal([5, 4, 1, 3], actual.Select(item => item.Id));
    }

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    public async Task PreparedOperators_KeepFilterAndKeyCapturesLive(string pipeline)
    {
        int minimum = 0;
        bool reverse = false;
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IQueryable<Item> query = root.Where(item => item.Id > minimum)
            .OrderBy(item => reverse ? -item.Score : item.Score).ThenBy(item => item.Group);
        Func<IEnumerable<Item>, IAsyncEnumerable<Item>> execute = Prepare(pipeline, root, query);

        foreach ((int threshold, bool descending) in new[] { (1, true), (2, false) })
        {
            minimum = threshold;
            reverse = descending;
            List<Item> actual = await execute(Items).ToListAsync(TestContext.Current.CancellationToken);

            Assert.Equal(
                Items.Where(item => item.Id > minimum).OrderBy(item => reverse ? -item.Score : item.Score)
                    .ThenBy(item => item.Group).Select(item => item.Id),
                actual.Select(item => item.Id));
        }
    }

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    public async Task TakeZero_DoesNotEnumerateOrEvaluateKeys(string pipeline)
    {
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IQueryable<Item> query = root.OrderBy(item => Fail(item.Id)).ThenBy(item => item.Score).Take(0);
        Func<IEnumerable<Item>, IAsyncEnumerable<Item>> execute = Prepare(pipeline, root, query);

        Assert.Empty(await execute(Items.Select<Item, Item>(item => throw new InvalidOperationException(item.Id.ToString(System.Globalization.CultureInfo.InvariantCulture))))
            .ToListAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("root")]
    [InlineData("include")]
    [InlineData("tail")]
    public async Task LateSecondaryKeyFailure_PreservesKeyEvaluationOrder(string pipeline)
    {
        var evaluated = new List<(string Key, int Id)>();
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IQueryable<Item> query = root.OrderBy(item => Key(evaluated, "first", item.Id))
            .ThenBy(item => Key(evaluated, "second", item.Id)).Take(1);
        Func<IEnumerable<Item>, IAsyncEnumerable<Item>> execute = Prepare(pipeline, root, query);
        Assert.Empty(evaluated);

        FormatException error = await Assert.ThrowsAsync<FormatException>(async () =>
            await execute(Items).ToListAsync(TestContext.Current.CancellationToken));

        Assert.Equal("secondary", error.Message);
        Assert.Equal(Items.Select(item => ("first", item.Id)).Concat(Items.Select(item => ("second", item.Id))), evaluated);
    }

    [Fact]
    public async Task TailCustomComparer_RefinesTheCompleteOrderingRun()
    {
        IQueryable<Item> root = Array.Empty<Item>().AsQueryable();
        IComparer<int> comparer = Comparer<int>.Create(static (left, right) => right.CompareTo(left));
        IQueryable<Item> query = root.Where(item => item.Id > 0).OrderBy(item => item.Group)
            .ThenBy(item => item.Score, comparer);
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(query.Expression);
        Assert.IsType<FilterStage>(Assert.Single(plan.Stages));

        List<object?> actual = await InMemoryTail.Apply(Items.Cast<object?>().ToAsyncEnumerable(), query.Expression, boundary, TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([2, 5, 4, 1, 3], actual.Cast<Item>().Select(item => item.Id));
    }

    private static Func<IEnumerable<Item>, IAsyncEnumerable<Item>> Prepare(string pipeline, IQueryable<Item> root, IQueryable<Item> query)
    {
        if (pipeline == "tail")
        {
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

    private static int Fail(int value) => throw new FormatException(value.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static int Key(List<(string Key, int Id)> evaluated, string key, int id)
    {
        evaluated.Add((key, id));
        return key == "second" && id == Items[^1].Id ? throw new FormatException("secondary") : 0;
    }

    private sealed record Item(int Id, int Group, int Score);

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
