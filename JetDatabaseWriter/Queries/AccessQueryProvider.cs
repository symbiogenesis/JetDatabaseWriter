namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
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

    public object? Execute(Expression expression) => this.Execute<object>(expression);

    public TResult Execute<TResult>(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));
        IEnumerable list = this.ExecuteSyncList(expression);
        if (list is TResult typed)
        {
            return typed;
        }

        throw new NotSupportedException(
            "Synchronous scalar query execution is not supported; use the async terminal operators (ToListAsync, FirstOrDefaultAsync, ...).");
    }

    public IEnumerable ExecuteSyncList(Expression expression) =>
        this.ExecuteListAsync(expression, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public async IAsyncEnumerable<object> ExecuteStreamAsync(Expression expression, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (T item in this.ExecuteTypedAsync(expression, cancellationToken).ConfigureAwait(false))
        {
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

    private async ValueTask<List<T>> ExecuteListAsync(Expression expression, CancellationToken cancellationToken)
    {
        var list = new List<T>();
        await foreach (T item in this.ExecuteTypedAsync(expression, cancellationToken).ConfigureAwait(false))
        {
            list.Add(item);
        }

        return list;
    }

    private async IAsyncEnumerable<T> ExecuteTypedAsync(Expression expression, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        AccessQueryPlan plan = AccessQueryTranslator.Translate(expression);
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
}
