namespace JetDatabaseWriter.Models;

using System.Collections;
using System.Collections.Generic;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// A row filter formed by the logical AND of one or more
/// <see cref="ColumnPredicate"/> conditions. Used by the writer's update and
/// delete operations to express multi-column and range filters
/// (for example <c>WHERE a = 1 AND b &gt; 2</c>), which the single-column
/// convenience overloads cannot express.
/// </summary>
/// <remarks>
/// <para>
/// An empty <see cref="RowCriteria"/> matches every row. Build instances fluently:
/// <code>
/// var where = RowCriteria.Where("Region", "West").And(ColumnPredicate.GreaterThan("Score", 80));
/// </code>
/// or with collection-initializer syntax over <see cref="ColumnPredicate"/> values.
/// </para>
/// </remarks>
public sealed class RowCriteria : IEnumerable<ColumnPredicate>
{
    private readonly List<ColumnPredicate> predicates = [];

    /// <summary>Initializes a new instance of the <see cref="RowCriteria"/> class.</summary>
    public RowCriteria()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RowCriteria"/> class from a
    /// sequence of predicates combined with AND.
    /// </summary>
    /// <param name="predicates">The predicates to combine.</param>
    public RowCriteria(IEnumerable<ColumnPredicate> predicates)
    {
        Guard.NotNull(predicates, nameof(predicates));
        foreach (ColumnPredicate predicate in predicates)
        {
            this.Add(predicate);
        }
    }

    /// <summary>Gets the predicates combined by this criteria, in the order added.</summary>
    public IReadOnlyList<ColumnPredicate> Predicates => this.predicates;

    /// <summary>Gets the number of predicates.</summary>
    public int Count => this.predicates.Count;

    /// <summary>Creates an empty criteria that matches every row.</summary>
    /// <returns>A new instance.</returns>
    public static RowCriteria All() => [];

    /// <summary>Starts a criteria with a single column-equality predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Value to match; <see langword="null"/> matches database null.</param>
    /// <returns>A new criteria containing the equality predicate.</returns>
    public static RowCriteria Where(string columnName, object? value)
        => new RowCriteria().And(ColumnPredicate.EqualTo(columnName, value));

    /// <summary>Starts a criteria with a single predicate.</summary>
    /// <param name="predicate">The first predicate.</param>
    /// <returns>A new criteria containing <paramref name="predicate"/>.</returns>
    public static RowCriteria Where(ColumnPredicate predicate)
        => new RowCriteria().And(predicate);

    /// <summary>Adds a predicate (logical AND) and returns this instance for chaining.</summary>
    /// <param name="predicate">The predicate to add.</param>
    /// <returns>This instance.</returns>
    public RowCriteria And(ColumnPredicate predicate)
    {
        this.Add(predicate);
        return this;
    }

    /// <summary>Adds a column-equality predicate (logical AND) and returns this instance for chaining.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Value to match.</param>
    /// <returns>This instance.</returns>
    public RowCriteria And(string columnName, object? value)
        => this.And(ColumnPredicate.EqualTo(columnName, value));

    /// <summary>Adds a predicate. Supports collection-initializer syntax.</summary>
    /// <param name="predicate">The predicate to add.</param>
    public void Add(ColumnPredicate predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        this.predicates.Add(predicate);
    }

    /// <inheritdoc/>
    public IEnumerator<ColumnPredicate> GetEnumerator() => this.predicates.GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
}
