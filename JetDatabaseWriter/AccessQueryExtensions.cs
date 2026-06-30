namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Queries;

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
    /// <see cref="ThenInclude{TEntity, TPreviousProperty, TProperty}(IIncludableQueryable{TEntity, TPreviousProperty}, Expression{Func{TPreviousProperty, TProperty}})"/>
    /// to load a nested navigation off the included entity.
    /// </summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TProperty">The navigation property type (a reference entity or a collection of entities).</typeparam>
    /// <param name="source">The query to extend.</param>
    /// <param name="navigation">A property-access expression (<c>o =&gt; o.Customer</c> or <c>c =&gt; c.Orders</c>), optionally with an inline filter/order/page chain on a collection navigation (<c>c =&gt; c.Orders.Where(o =&gt; o.Open).OrderBy(o =&gt; o.Date).Take(5)</c>).</param>
    /// <returns>A new query that will populate the navigation on materialization.</returns>
    public static IIncludableQueryable<T, TProperty> Include<T, TProperty>(this IQueryable<T> source, Expression<Func<T, TProperty>> navigation)
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
    public static IIncludableQueryable<TEntity, TProperty> ThenInclude<TEntity, TPreviousProperty, TProperty>(
        this IIncludableQueryable<TEntity, TPreviousProperty> source,
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
    public static IIncludableQueryable<TEntity, TProperty> ThenInclude<TEntity, TPreviousProperty, TProperty>(
        this IIncludableQueryable<TEntity, IEnumerable<TPreviousProperty>> source,
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

        throw new InvalidOperationException("The sequence contains no elements.");
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
            throw new InvalidOperationException("The sequence contains no elements.");
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

    /// <summary>Returns the minimum projected value, ignoring nulls.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being compared.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The minimum projected value, or <see langword="default"/> for an empty sequence of a nullable type.</returns>
    public static async ValueTask<TResult?> MinAsync<T, TResult>(this IQueryable<T> source, Func<T, TResult> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Min(selector);
    }

    /// <summary>Returns the maximum projected value, ignoring nulls.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being compared.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The maximum projected value, or <see langword="default"/> for an empty sequence of a nullable type.</returns>
    public static async ValueTask<TResult?> MaxAsync<T, TResult>(this IQueryable<T> source, Func<T, TResult> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Max(selector);
    }

    /// <summary>Sums the projected <see cref="int"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values.</returns>
    public static async ValueTask<int> SumAsync<T>(this IQueryable<T> source, Func<T, int> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Sum(selector);
    }

    /// <summary>Sums the projected <see cref="long"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values.</returns>
    public static async ValueTask<long> SumAsync<T>(this IQueryable<T> source, Func<T, long> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Sum(selector);
    }

    /// <summary>Sums the projected <see cref="float"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values.</returns>
    public static async ValueTask<float> SumAsync<T>(this IQueryable<T> source, Func<T, float> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Sum(selector);
    }

    /// <summary>Sums the projected <see cref="double"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values.</returns>
    public static async ValueTask<double> SumAsync<T>(this IQueryable<T> source, Func<T, double> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Sum(selector);
    }

    /// <summary>Sums the projected <see cref="decimal"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being summed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The sum of the projected values.</returns>
    public static async ValueTask<decimal> SumAsync<T>(this IQueryable<T> source, Func<T, decimal> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Sum(selector);
    }

    /// <summary>Averages the projected <see cref="int"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    public static async ValueTask<double> AverageAsync<T>(this IQueryable<T> source, Func<T, int> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Average(selector);
    }

    /// <summary>Averages the projected <see cref="long"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    public static async ValueTask<double> AverageAsync<T>(this IQueryable<T> source, Func<T, long> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Average(selector);
    }

    /// <summary>Averages the projected <see cref="float"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    public static async ValueTask<float> AverageAsync<T>(this IQueryable<T> source, Func<T, float> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Average(selector);
    }

    /// <summary>Averages the projected <see cref="double"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    public static async ValueTask<double> AverageAsync<T>(this IQueryable<T> source, Func<T, double> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Average(selector);
    }

    /// <summary>Averages the projected <see cref="decimal"/> values.</summary>
    /// <typeparam name="T">The query element type.</typeparam>
    /// <param name="source">The query to read.</param>
    /// <param name="selector">Projects each entity to the value being averaged.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The mean of the projected values.</returns>
    public static async ValueTask<decimal> AverageAsync<T>(this IQueryable<T> source, Func<T, decimal> selector, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(selector, nameof(selector));
        List<T> list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.Average(selector);
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
}
