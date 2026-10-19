namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Linq;
using System.Linq.Expressions;
using JetDatabaseWriter.Queries;
using Xunit;

/// <summary>Translation preserves operator boundaries and order without depending on call-stack depth.</summary>
public sealed class QueryTranslationTests
{
    [Fact]
    public void DeepOperatorChain_TranslatesEveryStage()
    {
        const int count = 100_000;
        IQueryable<int> query = Array.Empty<int>().AsQueryable();
        for (int i = 0; i < count; i++)
        {
            query = query.Skip(i);
        }

        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(query.Expression);

        Assert.Same(query.Expression, boundary);
        Assert.Equal(count, plan.Stages.Count);
        Assert.All(plan.Stages, stage => Assert.IsType<SkipStage>(stage));
    }

    [Fact]
    public void MixedOperators_PreserveWrittenStageOrder()
    {
        IQueryable<int> query = Array.Empty<int>().AsQueryable()
            .Where(value => value > 0).Skip(2).OrderBy(value => value).ThenByDescending(value => -value).Take(3);

        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(query.Expression);

        Assert.Same(query.Expression, boundary);
        Assert.Collection(
            plan.Stages,
            stage => Assert.IsType<FilterStage>(stage),
            stage => Assert.IsType<SkipStage>(stage),
            stage => Assert.Equal(2, Assert.IsType<OrderStage>(stage).Keys.Count),
            stage => Assert.IsType<TakeStage>(stage));
    }

    [Fact]
    public void UnsupportedThenBy_MovesEntireOrderingToTail()
    {
        IQueryable<string> prefix = Array.Empty<string>().AsQueryable().Where(value => value.Length > 0).Take(4);
        IQueryable<string> query = prefix.OrderBy(value => value.Length).ThenBy(value => value)
            .ThenByDescending(value => value, StringComparer.Ordinal).Skip(1);

        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(query.Expression);

        Assert.Same(prefix.Expression, boundary);
        Assert.Collection(plan.Stages, stage => Assert.IsType<FilterStage>(stage), stage => Assert.IsType<TakeStage>(stage));
    }

    [Fact]
    public void UnsupportedOperator_DoesNotEvaluateLaterOperands()
    {
        IQueryable<int> prefix = Array.Empty<int>().AsQueryable().Skip(1);
        Expression count = Expression.Call(typeof(QueryTranslationTests), nameof(ThrowCount), Type.EmptyTypes);
        Expression tail = Expression.Call(
            typeof(Queryable), nameof(Queryable.Take), [typeof(int)], prefix.Select(value => value + 1).Expression, count);

        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(tail);

        Assert.Same(prefix.Expression, boundary);
        Assert.IsType<SkipStage>(Assert.Single(plan.Stages));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NonconstantSource_PreservesUnsupportedNodeError(bool withOperator)
    {
        Expression expression = Expression.Parameter(typeof(IQueryable<int>), "source");
        if (withOperator)
        {
            expression = Expression.Call(typeof(Queryable), nameof(Queryable.Skip), [typeof(int)], expression, Expression.Constant(1));
        }

        NotSupportedException error = Assert.Throws<NotSupportedException>(() => AccessQueryTranslator.Translate(expression));

        Assert.Equal("Unsupported query expression node 'Parameter'.", error.Message);
    }

    private static int ThrowCount() => throw new InvalidOperationException("The tail operand must remain deferred.");
}
