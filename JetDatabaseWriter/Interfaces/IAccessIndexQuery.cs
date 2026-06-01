namespace JetDatabaseWriter.Interfaces;

using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Models;

/// <summary>
/// Fluent read-only query over a named Access index. Predicates are explicit
/// index-key operations; standard LINQ operators over <see cref="IAsyncEnumerable{T}"/>
/// remain client-side filters.
/// </summary>
/// <typeparam name="TRow">The row shape returned by the query.</typeparam>
public interface IAccessIndexQuery<TRow>
{
    /// <summary>
    /// Restricts the query to rows whose complete index key equals
    /// <paramref name="keyValues"/>.
    /// </summary>
    /// <param name="keyValues">One value per indexed column, in index-key order.</param>
    /// <returns>A new query with the exact-key predicate applied.</returns>
    public IAccessIndexQuery<TRow> WhereEquals(params object?[] keyValues);

    /// <summary>
    /// Restricts the query to rows whose leading index-key columns equal
    /// <paramref name="prefixValues"/>.
    /// </summary>
    /// <remarks>
    /// This is a composite-key prefix operation, not a text <c>StartsWith</c>
    /// predicate. For example, on an index over <c>(LastName, FirstName)</c>,
    /// passing <c>"Smith"</c> returns rows whose <c>LastName</c> key segment is
    /// exactly <c>"Smith"</c> with any trailing <c>FirstName</c> value.
    /// </remarks>
    /// <param name="prefixValues">Leading key-column values in index-key order.</param>
    /// <returns>A new query with the key-prefix predicate applied.</returns>
    public IAccessIndexQuery<TRow> WhereKeyPrefix(params object?[] prefixValues);

    /// <summary>
    /// Restricts the query to a one-column range over the first indexed column.
    /// </summary>
    /// <param name="lower">Lower bound value for the first indexed column.</param>
    /// <param name="upper">Upper bound value for the first indexed column.</param>
    /// <param name="lowerInclusive">Whether <paramref name="lower"/> is included.</param>
    /// <param name="upperInclusive">Whether <paramref name="upper"/> is included.</param>
    /// <returns>A new query with the range predicate applied.</returns>
    public IAccessIndexQuery<TRow> WhereBetween(
        object? lower,
        object? upper,
        bool lowerInclusive = true,
        bool upperInclusive = true);

    /// <summary>
    /// Restricts the query to an explicit lower/upper index-key range.
    /// </summary>
    /// <remarks>
    /// Each bound may include one or more leading index-key column values. Use
    /// <see langword="null"/> for an unbounded side. Bounds are compared in the
    /// Access index sort order, including per-column descending order.
    /// </remarks>
    /// <param name="lower">Lower index-key bound, or <see langword="null"/> for unbounded.</param>
    /// <param name="upper">Upper index-key bound, or <see langword="null"/> for unbounded.</param>
    /// <returns>A new query with the range predicate applied.</returns>
    public IAccessIndexQuery<TRow> WhereRange(IndexKeyBound? lower, IndexKeyBound? upper);

    /// <summary>
    /// Streams rows that match the index predicate. With no predicate, rows are
    /// returned in index order.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of matching rows.</returns>
    public IAsyncEnumerable<TRow> ToRowsAsync(CancellationToken cancellationToken = default);
}
