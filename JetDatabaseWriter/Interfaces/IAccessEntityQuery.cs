namespace JetDatabaseWriter.Interfaces;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// A fluent, EF-style entity query over a single table that supports client-side
/// filtering and relationship-inferred eager loading (<see cref="Include{TProperty}"/>).
/// </summary>
/// <remarks>
/// The query is composable and immutable: each operator returns a new query. It also
/// implements <see cref="IAsyncEnumerable{T}"/>, so it can be consumed with
/// <c>await foreach</c> or the async LINQ operators. When one or more includes are
/// present the results are materialized (the related rows are batch-loaded and
/// stitched onto each root before the first element is yielded).
/// </remarks>
/// <typeparam name="T">The entity type mapped from the table's rows.</typeparam>
public interface IAccessEntityQuery<T> : IAsyncEnumerable<T>
    where T : class, new()
{
    /// <summary>
    /// Restricts the query to rows matching <paramref name="predicate"/>. Multiple
    /// calls are combined with logical AND. The predicate also drives automatic index
    /// inference, exactly as <c>Rows&lt;T&gt;(table, predicate)</c> does.
    /// </summary>
    /// <param name="predicate">A row filter expression.</param>
    /// <returns>A new query with the predicate applied.</returns>
    public IAccessEntityQuery<T> Where(Expression<Func<T, bool>> predicate);

    /// <summary>
    /// Eagerly loads the related entity or entities reached through the
    /// <paramref name="navigation"/> property. The relationship is inferred from the
    /// database's <c>MSysRelationships</c> catalog by matching the navigation's target
    /// type to the related table.
    /// </summary>
    /// <typeparam name="TProperty">The navigation property type (a reference entity or a collection of entities).</typeparam>
    /// <param name="navigation">A property-access expression, e.g. <c>o =&gt; o.Customer</c> or <c>c =&gt; c.Orders</c>.</param>
    /// <returns>A new query that will populate the navigation on materialization.</returns>
    public IAccessEntityQuery<T> Include<TProperty>(Expression<Func<T, TProperty>> navigation);

    /// <summary>
    /// Materializes the query — applying every <see cref="Include{TProperty}"/> — into a list.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The matching entities with their included navigations populated.</returns>
    public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default);
}
