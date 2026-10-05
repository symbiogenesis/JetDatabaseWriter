namespace JetDatabaseWriter.Linq;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Queries;

/// <summary>
/// LINQ extensions for the entity queries returned by
/// <see cref="AccessReader.Query{T}(string)"/>: relationship-inferred eager loading
/// (<see cref="Include{T, TProperty}"/>) and async terminal operators. Query results are
/// async-only, so these terminals and <see cref="AsAsyncEnumerable{T}(IQueryable{T})"/> are
/// the ways to run a query: synchronous enumeration and the synchronous LINQ terminals throw
/// <see cref="NotSupportedException"/>. The aggregates (<c>SumAsync</c>, <c>AverageAsync</c>,
/// <c>MinAsync</c> and <c>MaxAsync</c>) take expression selectors, as <see cref="Queryable"/>'s
/// do, and fold the rows as the read returns them, with LINQ to Objects' rules for overflow,
/// nulls and empty queries.
/// </summary>
public static class AccessQueryExtensions
{
    private static readonly MethodInfo IncludeMethodDefinition =
        typeof(AccessQueryExtensions).GetMethod(nameof(Include))
            ?? throw new InvalidOperationException("The Include method could not be reflected.");

    private static readonly MethodInfo ThenIncludeAfterReferenceMethod = ResolveThenInclude(afterCollection: false);

    private static readonly MethodInfo ThenIncludeAfterCollectionMethod = ResolveThenInclude(afterCollection: true);

    /// <summary>
    /// Eagerly loads the related entity or entities reached through the
    /// <paramref name="navigation"/> property. The relationship is inferred from the
    /// database's <c>MSysRelationships</c> catalog by matching the navigation's target
    /// type to the related table by name — ignoring case and non-alphanumeric separators,
    /// or honoring an explicit <c>[Table("...")]</c> attribute on the type; the related
    /// rows load via an index seek when the join
    /// columns are indexed, otherwise via a single scan. A collection navigation may be
    /// filtered, ordered, and paged inline — EF-style — by chaining <c>Where</c>,
    /// <c>OrderBy</c>/<c>OrderByDescending</c>/<c>ThenBy</c>/<c>ThenByDescending</c>,
    /// <c>Skip</c>, and <c>Take</c> onto it; those operators run per parent and a following
    /// <c>ThenInclude</c> descends only into the kept rows. Chain
    /// <see cref="ThenInclude{TEntity, TPreviousProperty, TProperty}(IAccessIncludableQueryable{TEntity, TPreviousProperty}, Expression{Func{TPreviousProperty, TProperty}})"/>
    /// to load a nested navigation off the included entity.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TProperty">The navigation property type (a reference entity or a collection of entities).</typeparam>
    /// <param name="source">The query to extend.</param>
    /// <param name="navigation">A property-access expression (<c>o =&gt; o.Customer</c> or <c>c =&gt; c.Orders</c>), optionally with an inline filter/order/page chain on a collection navigation (<c>c =&gt; c.Orders.Where(o =&gt; o.Open).OrderBy(o =&gt; o.Date).Take(5)</c>).</param>
    /// <returns>A new query that will populate the navigation on materialization.</returns>
    public static IAccessIncludableQueryable<T, TProperty> Include<T, TProperty>(this IQueryable<T> source, Expression<Func<T, TProperty>> navigation)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(navigation, nameof(navigation));

        MethodCallExpression call = Expression.Call(
            IncludeMethodDefinition.MakeGenericMethod(typeof(T), typeof(TProperty)),
            source.Expression,
            Expression.Quote(navigation));
        return new IncludableQueryable<T, TProperty>(source.Provider.CreateQuery<T>(call));
    }

    /// <summary>
    /// Eagerly loads a navigation reached from the entity included by the preceding
    /// <c>Include</c> / <c>ThenInclude</c> (a reference navigation), extending the
    /// eager-load chain one level deeper.
    /// </summary>
    /// <typeparam name="TEntity">The query element type.</typeparam>
    /// <typeparam name="TPreviousProperty">The reference entity type included by the preceding step.</typeparam>
    /// <typeparam name="TProperty">The nested navigation type.</typeparam>
    /// <param name="source">The query whose most recent include targets a reference entity.</param>
    /// <param name="navigation">A property-access expression on the previously included entity, e.g. <c>c =&gt; c.Region</c>.</param>
    /// <returns>A new query that will also populate the nested navigation on materialization.</returns>
    public static IAccessIncludableQueryable<TEntity, TProperty> ThenInclude<TEntity, TPreviousProperty, TProperty>(
        this IAccessIncludableQueryable<TEntity, TPreviousProperty> source,
        Expression<Func<TPreviousProperty, TProperty>> navigation)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(navigation, nameof(navigation));

        MethodCallExpression call = Expression.Call(
            ThenIncludeAfterReferenceMethod.MakeGenericMethod(typeof(TEntity), typeof(TPreviousProperty), typeof(TProperty)),
            source.Expression,
            Expression.Quote(navigation));
        return new IncludableQueryable<TEntity, TProperty>(source.Provider.CreateQuery<TEntity>(call));
    }

    /// <summary>
    /// Eagerly loads a navigation reached from each element of the collection included by
    /// the preceding <c>Include</c> / <c>ThenInclude</c>, extending the eager-load chain
    /// one level deeper.
    /// </summary>
    /// <typeparam name="TEntity">The query element type.</typeparam>
    /// <typeparam name="TPreviousProperty">The element type of the collection included by the preceding step.</typeparam>
    /// <typeparam name="TProperty">The nested navigation type.</typeparam>
    /// <param name="source">The query whose most recent include targets a collection of entities.</param>
    /// <param name="navigation">A property-access expression on the previously included element (<c>i =&gt; i.Product</c>), optionally with an inline filter/order/page chain when it targets a nested collection.</param>
    /// <returns>A new query that will also populate the nested navigation on materialization.</returns>
    public static IAccessIncludableQueryable<TEntity, TProperty> ThenInclude<TEntity, TPreviousProperty, TProperty>(
        this IAccessIncludableQueryable<TEntity, IEnumerable<TPreviousProperty>> source,
        Expression<Func<TPreviousProperty, TProperty>> navigation)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(navigation, nameof(navigation));

        MethodCallExpression call = Expression.Call(
            ThenIncludeAfterCollectionMethod.MakeGenericMethod(typeof(TEntity), typeof(TPreviousProperty), typeof(TProperty)),
            source.Expression,
            Expression.Quote(navigation));
        return new IncludableQueryable<TEntity, TProperty>(source.Provider.CreateQuery<TEntity>(call));
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
    {
        Guard.NotNull(source, nameof(source));
        return source.Provider is IAccessQueryEngine engine
            ? ToInt32CountAsync(engine.CountAsync(source.Expression, cancellationToken))
            : AsAsyncEnumerable(source).CountAsync(cancellationToken);
    }

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

    /// <summary>Returns the first matching entity, or throws when the query produces no rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first entity.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows.</exception>
    public static async ValueTask<T> FirstAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        await foreach (T item in AsAsyncEnumerable(source).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            return item;
        }

        throw NoElements();
    }

    /// <summary>Returns the first entity matching <paramref name="predicate"/>, or throws when none match.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first matching entity.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no row matches.</exception>
    public static ValueTask<T> FirstAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).FirstAsync(cancellationToken);
    }

    /// <summary>Returns the first entity matching <paramref name="predicate"/>, or <see langword="default"/> when none match.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first matching entity or <see langword="default"/>.</returns>
    public static ValueTask<T?> FirstOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).FirstOrDefaultAsync(cancellationToken);
    }

    /// <summary>Returns the single matching entity, or throws when none or more than one match.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The single entity.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows or more than one row.</exception>
    public static async ValueTask<T> SingleAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        await using IAsyncEnumerator<T> enumerator = AsAsyncEnumerable(source).GetAsyncEnumerator(cancellationToken);
        if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
        {
            throw NoElements();
        }

        T single = enumerator.Current;
        if (await enumerator.MoveNextAsync().ConfigureAwait(false))
        {
            throw new InvalidOperationException("The sequence contains more than one element.");
        }

        return single;
    }

    /// <summary>Returns the single entity matching <paramref name="predicate"/>, or throws when none or more than one match.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The single matching entity.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no row matches or more than one row matches.</exception>
    public static ValueTask<T> SingleAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).SingleAsync(cancellationToken);
    }

    /// <summary>Returns the single matching entity, <see langword="default"/> when none match, or throws when more than one matches.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The single matching entity or <see langword="default"/>.</returns>
    public static ValueTask<T?> SingleOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).SingleOrDefaultAsync(cancellationToken);
    }

    /// <summary>Counts the rows matching <paramref name="predicate"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to count.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of matching rows.</returns>
    public static ValueTask<int> CountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).CountAsync(cancellationToken);
    }

    /// <summary>Counts the rows the query produces as a 64-bit value.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to count.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of rows.</returns>
    public static ValueTask<long> LongCountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        return source.Provider is IAccessQueryEngine engine
            ? engine.CountAsync(source.Expression, cancellationToken)
            : CountStreamingAsync(source, cancellationToken);
    }

    /// <summary>Counts the rows matching <paramref name="predicate"/> as a 64-bit value.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to count.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of matching rows.</returns>
    public static ValueTask<long> LongCountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).LongCountAsync(cancellationToken);
    }

    /// <summary>Determines whether any row matches <paramref name="predicate"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to test.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when at least one row matches.</returns>
    public static ValueTask<bool> AnyAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).AnyAsync(cancellationToken);
    }

    /// <summary>Determines whether every row satisfies <paramref name="predicate"/>, reading rows only until one does not.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to test.</param>
    /// <param name="predicate">The condition each row must meet; it runs on each row as the read returns it.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when every row satisfies <paramref name="predicate"/> or the query produces no rows.</returns>
    public static async ValueTask<bool> AllAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        await foreach (bool satisfied in Project(source, predicate, cancellationToken).ConfigureAwait(false))
        {
            if (!satisfied)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Returns the last entity the query produces, or throws when it produces no rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The last entity.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows.</exception>
    public static async ValueTask<T> LastAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        bool any = false;
        T last = default!;
        await foreach (T item in AsAsyncEnumerable(source).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            any = true;
            last = item;
        }

        return any ? last : throw NoElements();
    }

    /// <summary>Returns the last entity matching <paramref name="predicate"/>, or throws when none match.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The last matching entity.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no row matches.</exception>
    public static ValueTask<T> LastAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).LastAsync(cancellationToken);
    }

    /// <summary>Returns the last entity the query produces, or <see langword="default"/> when it produces no rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The last entity or <see langword="default"/>.</returns>
    public static async ValueTask<T?> LastOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        T? last = default;
        await foreach (T item in AsAsyncEnumerable(source).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            last = item;
        }

        return last;
    }

    /// <summary>Returns the last entity matching <paramref name="predicate"/>, or <see langword="default"/> when none match.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="predicate">The row predicate; pushed through the query so an index can be inferred.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The last matching entity or <see langword="default"/>.</returns>
    public static ValueTask<T?> LastOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(predicate, nameof(predicate));
        return source.Where(predicate).LastOrDefaultAsync(cancellationToken);
    }

    /// <summary>
    /// Determines whether the query produces <paramref name="item"/>, comparing with
    /// <see cref="EqualityComparer{T}.Default"/> and reading rows only until one matches.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to search.</param>
    /// <param name="item">The value to look for.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when a row equals <paramref name="item"/>.</returns>
    public static ValueTask<bool> ContainsAsync<T>(this IQueryable<T> source, T item, CancellationToken cancellationToken = default)
        => ContainsAsync(source, item, comparer: null, cancellationToken);

    /// <summary>
    /// Determines whether the query produces <paramref name="item"/>, comparing with
    /// <paramref name="comparer"/> and reading rows only until one matches.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to search.</param>
    /// <param name="item">The value to look for.</param>
    /// <param name="comparer">The equality comparer, or <see langword="null"/> for <see cref="EqualityComparer{T}.Default"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when a row equals <paramref name="item"/>.</returns>
    public static async ValueTask<bool> ContainsAsync<T>(this IQueryable<T> source, T item, IEqualityComparer<T>? comparer, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        comparer ??= EqualityComparer<T>.Default;
        await foreach (T candidate in AsAsyncEnumerable(source).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (comparer.Equals(candidate, item))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Materializes the query into an array, applying every operator and include.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to materialize.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The matching entities.</returns>
    public static async ValueTask<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return [.. list];
    }

    /// <summary>Materializes the query into a dictionary keyed by <paramref name="keySelector"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TKey">The dictionary key type.</typeparam>
    /// <param name="source">The query to materialize.</param>
    /// <param name="keySelector">Produces the key for each entity.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The entities keyed by <paramref name="keySelector"/>.</returns>
    public static ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(
        this IQueryable<T> source,
        Func<T, TKey> keySelector,
        CancellationToken cancellationToken = default)
        where TKey : notnull
        => ToDictionaryAsync(source, keySelector, comparer: null, cancellationToken);

    /// <summary>Materializes the query into a dictionary keyed by <paramref name="keySelector"/> using <paramref name="comparer"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TKey">The dictionary key type.</typeparam>
    /// <param name="source">The query to materialize.</param>
    /// <param name="keySelector">Produces the key for each entity.</param>
    /// <param name="comparer">The key comparer, or <see langword="null"/> for the default.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The entities keyed by <paramref name="keySelector"/>.</returns>
    public static async ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(
        this IQueryable<T> source,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(keySelector, nameof(keySelector));
        var result = new Dictionary<TKey, T>(comparer);
        await foreach (T item in AsAsyncEnumerable(source).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            result.Add(keySelector(item), item);
        }

        return result;
    }

    /// <summary>Materializes the query into a dictionary using <paramref name="keySelector"/> and <paramref name="elementSelector"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TKey">The dictionary key type.</typeparam>
    /// <typeparam name="TElement">The dictionary value type.</typeparam>
    /// <param name="source">The query to materialize.</param>
    /// <param name="keySelector">Produces the key for each entity.</param>
    /// <param name="elementSelector">Produces the value for each entity.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The projected values keyed by <paramref name="keySelector"/>.</returns>
    public static ValueTask<Dictionary<TKey, TElement>> ToDictionaryAsync<T, TKey, TElement>(
        this IQueryable<T> source,
        Func<T, TKey> keySelector,
        Func<T, TElement> elementSelector,
        CancellationToken cancellationToken = default)
        where TKey : notnull
        => ToDictionaryAsync(source, keySelector, elementSelector, comparer: null, cancellationToken);

    /// <summary>Materializes the query into a dictionary using <paramref name="keySelector"/>, <paramref name="elementSelector"/>, and <paramref name="comparer"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TKey">The dictionary key type.</typeparam>
    /// <typeparam name="TElement">The dictionary value type.</typeparam>
    /// <param name="source">The query to materialize.</param>
    /// <param name="keySelector">Produces the key for each entity.</param>
    /// <param name="elementSelector">Produces the value for each entity.</param>
    /// <param name="comparer">The key comparer, or <see langword="null"/> for the default.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The projected values keyed by <paramref name="keySelector"/>.</returns>
    public static async ValueTask<Dictionary<TKey, TElement>> ToDictionaryAsync<T, TKey, TElement>(
        this IQueryable<T> source,
        Func<T, TKey> keySelector,
        Func<T, TElement> elementSelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(keySelector, nameof(keySelector));
        Guard.NotNull(elementSelector, nameof(elementSelector));
        var result = new Dictionary<TKey, TElement>(comparer);
        await foreach (T item in AsAsyncEnumerable(source).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            result.Add(keySelector(item), elementSelector(item));
        }

        return result;
    }

    /// <summary>
    /// Returns the smallest projected value, folding the rows as the read returns them. As in
    /// LINQ to Objects, values compare through <see cref="Comparer{T}.Default"/> and nulls are
    /// skipped.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being compared.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The smallest value, or <see langword="null"/> when there is none and <typeparamref name="TResult"/> is a reference or nullable type.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows and <typeparamref name="TResult"/> is a non-nullable value type.</exception>
    public static ValueTask<TResult?> MinAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default)
        => ExtremeAsync(source, selector, keepGreater: false, cancellationToken);

    /// <summary>
    /// Returns the largest projected value, folding the rows as the read returns them. As in
    /// LINQ to Objects, values compare through <see cref="Comparer{T}.Default"/> and nulls are
    /// skipped.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being compared.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The largest value, or <see langword="null"/> when there is none and <typeparamref name="TResult"/> is a reference or nullable type.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows and <typeparamref name="TResult"/> is a non-nullable value type.</exception>
    public static ValueTask<TResult?> MaxAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken = default)
        => ExtremeAsync(source, selector, keepGreater: true, cancellationToken);

    /// <summary>Sums the projected <see cref="int"/> values as the read returns the rows, in checked arithmetic.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values; 0 when the query produces no rows.</returns>
    /// <exception cref="OverflowException">Thrown when the sum is outside the range of <see cref="int"/>.</exception>
    public static async ValueTask<int> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, int>> selector, CancellationToken cancellationToken = default)
    {
        int sum = 0;
        await foreach (int value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum = checked(sum + value);
        }

        return sum;
    }

    /// <summary>Sums the projected <see cref="int"/> values that are not null as the read returns the rows, in checked arithmetic.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the values that are not null; 0 when there are none.</returns>
    /// <exception cref="OverflowException">Thrown when the sum is outside the range of <see cref="int"/>.</exception>
    public static async ValueTask<int?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, int?>> selector, CancellationToken cancellationToken = default)
    {
        int sum = 0;
        await foreach (int? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum = checked(sum + value.GetValueOrDefault());
        }

        return sum;
    }

    /// <summary>Sums the projected <see cref="long"/> values as the read returns the rows, in checked arithmetic.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values; 0 when the query produces no rows.</returns>
    /// <exception cref="OverflowException">Thrown when the sum is outside the range of <see cref="long"/>.</exception>
    public static async ValueTask<long> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, long>> selector, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        await foreach (long value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum = checked(sum + value);
        }

        return sum;
    }

    /// <summary>Sums the projected <see cref="long"/> values that are not null as the read returns the rows, in checked arithmetic.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the values that are not null; 0 when there are none.</returns>
    /// <exception cref="OverflowException">Thrown when the sum is outside the range of <see cref="long"/>.</exception>
    public static async ValueTask<long?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, long?>> selector, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        await foreach (long? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum = checked(sum + value.GetValueOrDefault());
        }

        return sum;
    }

    /// <summary>Sums the projected <see cref="float"/> values as the read returns the rows, accumulating in <see cref="double"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values; 0 when the query produces no rows.</returns>
    public static async ValueTask<float> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, float>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        await foreach (float value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value;
        }

        return (float)sum;
    }

    /// <summary>Sums the projected <see cref="float"/> values that are not null as the read returns the rows, accumulating in <see cref="double"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the values that are not null; 0 when there are none.</returns>
    public static async ValueTask<float?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, float?>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        await foreach (float? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value.GetValueOrDefault();
        }

        return (float)sum;
    }

    /// <summary>Sums the projected <see cref="double"/> values as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values; 0 when the query produces no rows.</returns>
    public static async ValueTask<double> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, double>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        await foreach (double value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value;
        }

        return sum;
    }

    /// <summary>Sums the projected <see cref="double"/> values that are not null as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the values that are not null; 0 when there are none.</returns>
    public static async ValueTask<double?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, double?>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        await foreach (double? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value.GetValueOrDefault();
        }

        return sum;
    }

    /// <summary>Sums the projected <see cref="decimal"/> values as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values; 0 when the query produces no rows.</returns>
    /// <exception cref="OverflowException">Thrown when the sum is outside the range of <see cref="decimal"/>.</exception>
    public static async ValueTask<decimal> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal>> selector, CancellationToken cancellationToken = default)
    {
        decimal sum = 0;
        await foreach (decimal value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value;
        }

        return sum;
    }

    /// <summary>Sums the projected <see cref="decimal"/> values that are not null as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the values that are not null; 0 when there are none.</returns>
    /// <exception cref="OverflowException">Thrown when the sum is outside the range of <see cref="decimal"/>.</exception>
    public static async ValueTask<decimal?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal?>> selector, CancellationToken cancellationToken = default)
    {
        decimal sum = 0;
        await foreach (decimal? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value.GetValueOrDefault();
        }

        return sum;
    }

    /// <summary>Averages the projected <see cref="int"/> values as the read returns the rows, summing them as a <see cref="long"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows.</exception>
    public static async ValueTask<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, int>> selector, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        long count = 0;
        await foreach (int value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum = checked(sum + value);
            count++;
        }

        return count == 0 ? throw NoElements() : (double)sum / count;
    }

    /// <summary>Averages the projected <see cref="int"/> values that are not null as the read returns the rows, summing them as a <see cref="long"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the values that are not null, or <see langword="null"/> when there are none.</returns>
    public static async ValueTask<double?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, int?>> selector, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        long count = 0;
        await foreach (int? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            if (value is int present)
            {
                sum = checked(sum + present);
                count++;
            }
        }

        return count == 0 ? null : (double)sum / count;
    }

    /// <summary>Averages the projected <see cref="long"/> values as the read returns the rows, summing them in checked arithmetic.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows.</exception>
    /// <exception cref="OverflowException">Thrown when the sum of the values is outside the range of <see cref="long"/>.</exception>
    public static async ValueTask<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, long>> selector, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        long count = 0;
        await foreach (long value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum = checked(sum + value);
            count++;
        }

        return count == 0 ? throw NoElements() : (double)sum / count;
    }

    /// <summary>Averages the projected <see cref="long"/> values that are not null as the read returns the rows, summing them in checked arithmetic.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the values that are not null, or <see langword="null"/> when there are none.</returns>
    /// <exception cref="OverflowException">Thrown when the sum of the values is outside the range of <see cref="long"/>.</exception>
    public static async ValueTask<double?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, long?>> selector, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        long count = 0;
        await foreach (long? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            if (value is long present)
            {
                sum = checked(sum + present);
                count++;
            }
        }

        return count == 0 ? null : (double)sum / count;
    }

    /// <summary>Averages the projected <see cref="float"/> values as the read returns the rows, accumulating in <see cref="double"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows.</exception>
    public static async ValueTask<float> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, float>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        long count = 0;
        await foreach (float value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value;
            count++;
        }

        return count == 0 ? throw NoElements() : (float)(sum / count);
    }

    /// <summary>Averages the projected <see cref="float"/> values that are not null as the read returns the rows, accumulating in <see cref="double"/>.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the values that are not null, or <see langword="null"/> when there are none.</returns>
    public static async ValueTask<float?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, float?>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        long count = 0;
        await foreach (float? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            if (value is float present)
            {
                sum += present;
                count++;
            }
        }

        return count == 0 ? null : (float)(sum / count);
    }

    /// <summary>Averages the projected <see cref="double"/> values as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows.</exception>
    public static async ValueTask<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, double>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        long count = 0;
        await foreach (double value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value;
            count++;
        }

        return count == 0 ? throw NoElements() : sum / count;
    }

    /// <summary>Averages the projected <see cref="double"/> values that are not null as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the values that are not null, or <see langword="null"/> when there are none.</returns>
    public static async ValueTask<double?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, double?>> selector, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        long count = 0;
        await foreach (double? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            if (value is double present)
            {
                sum += present;
                count++;
            }
        }

        return count == 0 ? null : sum / count;
    }

    /// <summary>Averages the projected <see cref="decimal"/> values as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the query produces no rows.</exception>
    /// <exception cref="OverflowException">Thrown when the sum of the values is outside the range of <see cref="decimal"/>.</exception>
    public static async ValueTask<decimal> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal>> selector, CancellationToken cancellationToken = default)
    {
        decimal sum = 0;
        long count = 0;
        await foreach (decimal value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            sum += value;
            count++;
        }

        return count == 0 ? throw NoElements() : sum / count;
    }

    /// <summary>Averages the projected <see cref="decimal"/> values that are not null as the read returns the rows.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the values that are not null, or <see langword="null"/> when there are none.</returns>
    /// <exception cref="OverflowException">Thrown when the sum of the values is outside the range of <see cref="decimal"/>.</exception>
    public static async ValueTask<decimal?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal?>> selector, CancellationToken cancellationToken = default)
    {
        decimal sum = 0;
        long count = 0;
        await foreach (decimal? value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            if (value is decimal present)
            {
                sum += present;
                count++;
            }
        }

        return count == 0 ? null : sum / count;
    }

    internal static bool IsIncludeMethod(MethodInfo method) =>
        method.IsGenericMethod && method.GetGenericMethodDefinition() == IncludeMethodDefinition;

    internal static bool IsThenIncludeMethod(MethodInfo method)
    {
        if (!method.IsGenericMethod)
        {
            return false;
        }

        MethodInfo definition = method.GetGenericMethodDefinition();
        return definition == ThenIncludeAfterReferenceMethod || definition == ThenIncludeAfterCollectionMethod;
    }

    private static MethodInfo ResolveThenInclude(bool afterCollection)
    {
        foreach (MethodInfo method in typeof(AccessQueryExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static))
        {
            if (!string.Equals(method.Name, nameof(ThenInclude), StringComparison.Ordinal))
            {
                continue;
            }

            // Disambiguate the two overloads by the shape of the source parameter's second
            // type argument: the collection overload's previous property is IEnumerable<T>
            // (a constructed generic), the reference overload's is a bare type parameter.
            Type previousProperty = method.GetParameters()[0].ParameterType.GetGenericArguments()[1];
            bool isReferenceOverload = previousProperty.IsGenericParameter;
            if (isReferenceOverload != afterCollection)
            {
                return method;
            }
        }

        throw new InvalidOperationException("A ThenInclude method could not be reflected.");
    }

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

    private static async ValueTask<int> ToInt32CountAsync(ValueTask<long> count) =>
        checked((int)await count.ConfigureAwait(false));

    private static async ValueTask<long> CountStreamingAsync<T>(IQueryable<T> source, CancellationToken cancellationToken)
    {
        long count = 0;
        await foreach (T unused in AsAsyncEnumerable(source).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    private static InvalidOperationException NoElements() => new("The sequence contains no elements.");

    /// <summary>
    /// Streams <paramref name="selector"/>'s value for each row the query produces. The selector
    /// is compiled once and applied to each entity as the read returns it, so an aggregate folds
    /// the values one at a time: no row is buffered and no value is boxed.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to its value.</param>
    /// <param name="cancellationToken">A token used to cancel the enumeration.</param>
    /// <returns>The projected values, in the order the query produces the rows.</returns>
    private static IAsyncEnumerable<TResult> Project<T, TResult>(IQueryable<T> source, Expression<Func<T, TResult>> selector, CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        return ProjectAsync(AsAsyncEnumerable(source), selector.Compile(), cancellationToken);
    }

    private static async IAsyncEnumerable<TResult> ProjectAsync<T, TResult>(
        IAsyncEnumerable<T> rows,
        Func<T, TResult> selector,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (T row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return selector(row);
        }
    }

    /// <summary>
    /// Returns the smallest or largest projected value by LINQ to Objects' rules: values compare
    /// through <see cref="Comparer{T}.Default"/>, nulls are skipped, and when no value is left a
    /// reference or nullable result is <see langword="null"/> while any other throws.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being compared.</param>
    /// <param name="keepGreater"><see langword="true"/> for the largest value, <see langword="false"/> for the smallest.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The smallest or largest value, or <see langword="null"/> when there is none and the result type admits it.</returns>
    /// <exception cref="InvalidOperationException">Thrown when there is no value and <typeparamref name="TResult"/> is a non-nullable value type.</exception>
    private static async ValueTask<TResult?> ExtremeAsync<T, TResult>(
        IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        bool keepGreater,
        CancellationToken cancellationToken)
    {
        bool any = false;
        TResult extreme = default!;
        await foreach (TResult value in Project(source, selector, cancellationToken).ConfigureAwait(false))
        {
            if (value is null)
            {
                continue;
            }

            int order = any ? Comparer<TResult>.Default.Compare(value, extreme) : 0;
            if (!any || (keepGreater ? order > 0 : order < 0))
            {
                extreme = value;
                any = true;
            }
        }

        // A non-nullable value type has no null to stand for "no value", so it throws instead.
        if (!any && typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) is null)
        {
            throw NoElements();
        }

        return extreme;
    }
}
