namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// <see cref="IQueryProvider"/> for <see cref="AccessQueryable{T}"/>. Translates the
/// supported LINQ operators into an ordered <see cref="QueryStage"/> pipeline and runs
/// the stages in written order: a leading run of filters is pushed into the reader's
/// index inference, later stages (filter / order / page) run over the stream, and
/// includes eager-load inferred relationships onto the final set. The provider is
/// generic on the entity type so it can map rows; <see cref="AccessQueryable{T}"/>
/// reaches it through <see cref="IAccessQueryEngine"/>.
/// </summary>
/// <typeparam name="T">The entity type mapped from the table's rows.</typeparam>
/// <param name="reader">The reader the query executes against.</param>
/// <param name="table">The table being queried.</param>
internal sealed class AccessQueryProvider<T>(AccessReader reader, string table) : IQueryProvider, IAccessQueryEngine
    where T : class, new()
{
    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException("Untyped CreateQuery is not supported; use the generic LINQ query operators.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) =>
        new AccessQueryable<TElement>(this, expression);

    public object? Execute(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);
        List<T> rows = this.MaterializeSync(plan);
        if (ReferenceEquals(boundary, expression))
        {
            return rows;
        }

        (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
        return provider.Execute(rewritten);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));

        // The engine evaluates the supported prefix (so leading filters still infer
        // indexes); any tail — a scalar terminal such as Count/First/Sum, a Select
        // projection, or operators after one — replays over the materialized rows with
        // LINQ-to-Objects for faithful LINQ semantics.
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);
        List<T> rows = this.MaterializeSync(plan);
        if (!ReferenceEquals(boundary, expression))
        {
            (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
            return provider.Execute<TResult>(rewritten);
        }

        if (rows is TResult typed)
        {
            return typed;
        }

        throw new NotSupportedException(
            $"This query yields a sequence of '{typeof(T).Name}'; materialize it with ToList()/ToListAsync() or reduce it with a scalar operator such as Count(), Any(), or First().");
    }

    public IEnumerable ExecuteSyncList(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);
        List<T> rows = this.MaterializeSync(plan);
        if (ReferenceEquals(boundary, expression))
        {
            return rows;
        }

        (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
        return provider.CreateQuery(rewritten);
    }

    public async IAsyncEnumerable<object> ExecuteStreamAsync(Expression expression, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);

        // No tail: stream the engine pipeline straight through so Take/First can
        // short-circuit before the whole table is read.
        if (ReferenceEquals(boundary, expression))
        {
            await foreach (T item in this.ExecuteEngineAsync(plan, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            yield break;
        }

        // A tail (projection or post-projection operators) replays in memory, so the
        // engine prefix is materialized first.
        List<T> rows = await this.MaterializeAsync(plan, cancellationToken).ConfigureAwait(false);
        (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
        foreach (object item in provider.CreateQuery(rewritten))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    private static Expression<Func<T, bool>>? CombinePredicates(List<LambdaExpression> predicates)
    {
        if (predicates.Count == 0)
        {
            return null;
        }

        var combined = (Expression<Func<T, bool>>)predicates[0];
        for (int i = 1; i < predicates.Count; i++)
        {
            combined = Combine(combined, (Expression<Func<T, bool>>)predicates[i]);
        }

        return combined;
    }

    private static Expression<Func<T, bool>> Combine(Expression<Func<T, bool>> first, Expression<Func<T, bool>> second)
    {
        ParameterExpression parameter = first.Parameters[0];
        Expression rebound = new ReplaceParameterVisitor(second.Parameters[0], parameter).Visit(second.Body);
        return Expression.Lambda<Func<T, bool>>(Expression.AndAlso(first.Body, rebound), parameter);
    }

    private static (IQueryProvider Provider, Expression Rewritten) BuildTail(List<T> rows, Expression root, Expression boundary)
    {
        // Replay the tail over the materialized engine rows: rebind the engine boundary to
        // an in-memory queryable and hand the rewritten tree to LINQ-to-Objects, which
        // turns the Queryable operators into their Enumerable equivalents.
        IQueryable<T> materialized = rows.AsQueryable();
        Expression rewritten = new RebindSourceVisitor(boundary, materialized.Expression).Visit(root);
        return (materialized.Provider, rewritten);
    }

    private List<T> MaterializeSync(AccessQueryPlan plan) =>
        this.MaterializeAsync(plan, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    private async ValueTask<List<T>> MaterializeAsync(AccessQueryPlan plan, CancellationToken cancellationToken)
    {
        var list = new List<T>();
        await foreach (T item in this.ExecuteEngineAsync(plan, cancellationToken).ConfigureAwait(false))
        {
            list.Add(item);
        }

        return list;
    }

    private async IAsyncEnumerable<T> ExecuteEngineAsync(AccessQueryPlan plan, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IAsyncEnumerable<T> sequence = this.BuildPipeline(plan, cancellationToken);

        // Without includes the pipeline streams straight through, so Take/First can
        // short-circuit before the whole table is read.
        if (plan.Includes.Count == 0)
        {
            await foreach (T item in sequence.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            yield break;
        }

        // Eager loads stitch onto the final set, so materialize the pipeline first.
        var result = new List<T>();
        await foreach (T item in sequence.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            result.Add(item);
        }

        await IncludeLoader.ApplyAsync(reader, table, result, plan.Includes, cancellationToken).ConfigureAwait(false);
        foreach (T item in result)
        {
            yield return item;
        }
    }

    private IAsyncEnumerable<T> BuildPipeline(AccessQueryPlan plan, CancellationToken cancellationToken)
    {
        List<QueryStage> stages = plan.Stages;

        // Push the leading run of consecutive filters into the reader so its index
        // inference can seek rather than scan. Filters are mutually commutative, so
        // collapsing only the leading run preserves LINQ ordering semantics; every later
        // stage — including a filter that follows ordering or paging — runs in order.
        int next = 0;
        var leading = new List<LambdaExpression>();
        while (next < stages.Count && stages[next] is FilterStage filter)
        {
            leading.Add(filter.Predicate);
            next++;
        }

        Expression<Func<T, bool>>? pushed = CombinePredicates(leading);
        IAsyncEnumerable<T> sequence = pushed is null
            ? reader.Rows<T>(table, progress: null, cancellationToken)
            : reader.Rows(table, pushed, progress: null, cancellationToken);

        for (; next < stages.Count; next++)
        {
            sequence = stages[next].Apply(sequence, cancellationToken);
        }

        return sequence;
    }

    private sealed class ReplaceParameterVisitor(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) =>
            node == from ? to : base.VisitParameter(node);
    }

    private sealed class RebindSourceVisitor(Expression target, Expression replacement) : ExpressionVisitor
    {
        [return: NotNullIfNotNull(nameof(node))]
        public override Expression? Visit(Expression? node) =>
            ReferenceEquals(node, target) ? replacement : base.Visit(node);
    }
}
