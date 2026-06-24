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
/// supported LINQ operators into reader operations: filtering reuses index inference,
/// ordering / paging run in memory after materialization, and includes eager-load
/// inferred relationships. The provider is generic on the entity type so it can map
/// rows; <see cref="AccessQueryable{T}"/> reaches it through <see cref="IAccessQueryEngine"/>.
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

    private static async IAsyncEnumerable<T> StreamWithPagingAsync(
        IAsyncEnumerable<T> source,
        int? skip,
        int? take,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        int skipped = 0;
        int taken = 0;
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (skip is int s && skipped < s)
            {
                skipped++;
                continue;
            }

            if (take is int t && taken >= t)
            {
                yield break;
            }

            taken++;
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

    private static List<T> ApplyOrderings(List<T> buffer, List<(LambdaExpression KeySelector, bool Descending)> orderings)
    {
        if (orderings.Count == 0)
        {
            return buffer;
        }

        Func<T, object?> firstKey = CompileKey(orderings[0].KeySelector);
        IOrderedEnumerable<T> ordered = orderings[0].Descending
            ? buffer.OrderByDescending(firstKey, QueryKeyComparer.Instance)
            : buffer.OrderBy(firstKey, QueryKeyComparer.Instance);
        for (int i = 1; i < orderings.Count; i++)
        {
            Func<T, object?> key = CompileKey(orderings[i].KeySelector);
            ordered = orderings[i].Descending
                ? ordered.ThenByDescending(key, QueryKeyComparer.Instance)
                : ordered.ThenBy(key, QueryKeyComparer.Instance);
        }

        return ordered.ToList();
    }

    private static IReadOnlyList<T> ApplyPaging(IReadOnlyList<T> source, int? skip, int? take)
    {
        if (skip is null && take is null)
        {
            return source;
        }

        IEnumerable<T> sequence = source;
        if (skip is int s)
        {
            sequence = sequence.Skip(s);
        }

        if (take is int t)
        {
            sequence = sequence.Take(t);
        }

        return sequence.ToList();
    }

    private static Func<T, object?> CompileKey(LambdaExpression selector)
    {
        ParameterExpression parameter = selector.Parameters[0];
        Expression body = Expression.Convert(selector.Body, typeof(object));
        return Expression.Lambda<Func<T, object?>>(body, parameter).Compile();
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
        Expression<Func<T, bool>>? predicate = CombinePredicates(plan.Predicates);
        IAsyncEnumerable<T> filtered = predicate is null
            ? reader.Rows<T>(table, progress: null, cancellationToken)
            : reader.Rows<T>(table, predicate, progress: null, cancellationToken);

        // Pure filter (no ordering or includes) streams; skip/take apply on the stream.
        if (plan.Orderings.Count == 0 && plan.Includes.Count == 0)
        {
            await foreach (T item in StreamWithPagingAsync(filtered, plan.Skip, plan.Take, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            yield break;
        }

        // Ordering and includes need the filtered set materialized first.
        var buffer = new List<T>();
        await foreach (T item in filtered.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(item);
        }

        IReadOnlyList<T> result = ApplyPaging(ApplyOrderings(buffer, plan.Orderings), plan.Skip, plan.Take);
        if (plan.Includes.Count > 0)
        {
            await IncludeLoader.ApplyAsync(reader, table, result, plan.Includes, cancellationToken).ConfigureAwait(false);
        }

        foreach (T item in result)
        {
            yield return item;
        }
    }

    private sealed class ReplaceParameterVisitor(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) =>
            node == from ? to : base.VisitParameter(node);
    }
}
