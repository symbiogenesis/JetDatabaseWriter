namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// LINQ extensions for the entity queries returned by
/// <see cref="AccessReader.Query{T}(string)"/>: relationship-inferred eager loading
/// (<see cref="Include{T, TProperty}"/>) and async terminal operators.
/// </summary>
public static class AccessQueryExtensions
{
    private static readonly MethodInfo IncludeMethodDefinition =
        typeof(AccessQueryExtensions).GetMethod(nameof(Include))
            ?? throw new InvalidOperationException("The Include method could not be reflected.");

    /// <summary>
    /// Eagerly loads the related entity or entities reached through the
    /// <paramref name="navigation"/> property. The relationship is inferred from the
    /// database's <c>MSysRelationships</c> catalog by matching the navigation's target
    /// type to the related table; the related rows load via an index seek when the join
    /// columns are indexed, otherwise via a single scan.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TProperty">The navigation property type (a reference entity or a collection of entities).</typeparam>
    /// <param name="source">The query to extend.</param>
    /// <param name="navigation">A property-access expression, e.g. <c>o =&gt; o.Customer</c> or <c>c =&gt; c.Orders</c>.</param>
    /// <returns>A new query that will populate the navigation on materialization.</returns>
    public static IQueryable<T> Include<T, TProperty>(this IQueryable<T> source, Expression<Func<T, TProperty>> navigation)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(navigation, nameof(navigation));

        MethodCallExpression call = Expression.Call(
            IncludeMethodDefinition.MakeGenericMethod(typeof(T), typeof(TProperty)),
            source.Expression,
            Expression.Quote(navigation));
        return source.Provider.CreateQuery<T>(call);
    }

    /// <summary>Materializes the query into a list, applying every operator and include.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to materialize.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The matching entities.</returns>
    public static ValueTask<List<T>> ToListAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        => AsAsyncEnumerable(source).ToListAsync(cancellationToken);

    /// <summary>Counts the rows the query produces.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to count.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of matching rows.</returns>
    public static ValueTask<int> CountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        => AsAsyncEnumerable(source).CountAsync(cancellationToken);

    /// <summary>Determines whether the query produces any rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to test.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when at least one row matches.</returns>
    public static ValueTask<bool> AnyAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        => AsAsyncEnumerable(source).AnyAsync(cancellationToken);

    /// <summary>Returns the first matching entity, or <see langword="default"/> when none match.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first entity or <see langword="default"/>.</returns>
    public static async ValueTask<T?> FirstOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        => await AsAsyncEnumerable(source).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Returns the single matching entity, <see langword="default"/> when none match, or throws when more than one matches.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The single entity or <see langword="default"/>.</returns>
    public static async ValueTask<T?> SingleOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        => await AsAsyncEnumerable(source).SingleOrDefaultAsync(cancellationToken).ConfigureAwait(false);

    internal static bool IsIncludeMethod(MethodInfo method) =>
        method.IsGenericMethod && method.GetGenericMethodDefinition() == IncludeMethodDefinition;

    /// <summary>
    /// Exposes the query as an <see cref="IAsyncEnumerable{T}"/> for <c>await foreach</c>
    /// and the async LINQ operators.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to enumerate.</param>
    /// <returns>The query as an async sequence.</returns>
    /// <exception cref="NotSupportedException">Thrown when <paramref name="source"/> was not created by <c>AccessReader.Query&lt;T&gt;(...)</c>.</exception>
    public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source)
    {
        Guard.NotNull(source, nameof(source));
        return source as IAsyncEnumerable<T>
            ?? throw new NotSupportedException("This async operator requires a query created by AccessReader.Query<T>(...).");
    }
}
