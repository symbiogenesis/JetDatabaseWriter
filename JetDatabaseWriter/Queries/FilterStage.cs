namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>A <c>Where</c> stage that keeps the rows matching its predicate.</summary>
/// <param name="predicate">The row predicate; compiled once per execution and applied to each row.</param>
internal sealed class FilterStage(LambdaExpression predicate) : QueryStage
{
    public LambdaExpression Predicate { get; } = predicate;

    /// <summary>
    /// AND-combines a leading run of filter predicates into one typed predicate so the
    /// reader's index inference can push it as a seek instead of scanning. Filters commute,
    /// so combining the leading run preserves LINQ ordering semantics.
    /// </summary>
    /// <typeparam name="T">The entity type the predicates filter.</typeparam>
    /// <param name="filters">The leading run of filter stages, in written order.</param>
    /// <returns>The combined predicate, or <see langword="null"/> when the run is empty.</returns>
    public static Expression<Func<T, bool>>? Combine<T>(IReadOnlyList<FilterStage> filters)
    {
        if (filters.Count == 0)
        {
            return null;
        }

        var combined = (Expression<Func<T, bool>>)filters[0].Predicate;
        for (int i = 1; i < filters.Count; i++)
        {
            combined = AndAlso(combined, (Expression<Func<T, bool>>)filters[i].Predicate);
        }

        return combined;
    }

    public override async IAsyncEnumerable<T> Apply<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var compiled = (Func<T, bool>)this.Predicate.Compile();
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (compiled(item))
            {
                yield return item;
            }
        }
    }

    private static Expression<Func<T, bool>> AndAlso<T>(Expression<Func<T, bool>> first, Expression<Func<T, bool>> second)
    {
        ParameterExpression parameter = first.Parameters[0];
        Expression rebound = new ReplaceParameterVisitor(second.Parameters[0], parameter).Visit(second.Body);
        return Expression.Lambda<Func<T, bool>>(Expression.AndAlso(first.Body, rebound), parameter);
    }

    private sealed class ReplaceParameterVisitor(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) =>
            node == from ? to : base.VisitParameter(node);
    }
}
