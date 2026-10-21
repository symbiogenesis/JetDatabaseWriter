namespace JetDatabaseWriter.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// A single-column condition: a column name, a comparison operator, and the
/// operand(s) the column value is tested against. Combine predicates with
/// <see cref="RowCriteria"/> to express multi-column filters.
/// </summary>
/// <remarks>
/// Use the static factory methods (for example <see cref="EqualTo"/>,
/// <see cref="GreaterThan"/>, <see cref="Between"/>, <see cref="In(string, object?[])"/>) rather than
/// constructing directly. Column names are matched case-insensitively. Ordered
/// comparisons (<c>&gt;</c>, <c>&gt;=</c>, <c>&lt;</c>, <c>&lt;=</c>,
/// <see cref="ColumnPredicateOperator.Between"/>) coerce the operand to the
/// column's runtime value type before comparing; a database-null column value
/// never satisfies an ordered comparison.
/// </remarks>
public sealed class ColumnPredicate
{
    private ColumnPredicate(
        string columnName,
        ColumnPredicateOperator @operator,
        object? operand,
        object? upperOperand,
        IReadOnlyList<object?>? operands)
    {
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        this.ColumnName = columnName;
        this.Operator = @operator;
        this.Operand = operand;
        this.UpperOperand = upperOperand;
        this.Operands = operands;
    }

    /// <summary>Gets the column name this predicate tests (case-insensitive).</summary>
    public string ColumnName { get; }

    /// <summary>Gets the comparison operator.</summary>
    public ColumnPredicateOperator Operator { get; }

    /// <summary>
    /// Gets the primary operand. For <see cref="ColumnPredicateOperator.Between"/>
    /// this is the inclusive lower bound. Unused for
    /// <see cref="ColumnPredicateOperator.In"/>,
    /// <see cref="ColumnPredicateOperator.IsNull"/>, and
    /// <see cref="ColumnPredicateOperator.IsNotNull"/>.
    /// </summary>
    public object? Operand { get; }

    /// <summary>
    /// Gets the inclusive upper bound for <see cref="ColumnPredicateOperator.Between"/>;
    /// otherwise <see langword="null"/>.
    /// </summary>
    public object? UpperOperand { get; }

    /// <summary>
    /// Gets the candidate set for <see cref="ColumnPredicateOperator.In"/>; otherwise
    /// <see langword="null"/>.
    /// </summary>
    public IReadOnlyList<object?>? Operands { get; }

    /// <summary>Creates an equality predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Value to match; <see langword="null"/> matches database null.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate EqualTo(string columnName, object? value)
        => new(columnName, ColumnPredicateOperator.Equal, value, null, null);

    /// <summary>Creates an inequality predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Value the column must not equal.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate NotEqualTo(string columnName, object? value)
        => new(columnName, ColumnPredicateOperator.NotEqual, value, null, null);

    /// <summary>Creates a strictly-greater-than predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Exclusive lower bound; non-null.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate GreaterThan(string columnName, object value)
    {
        Guard.NotNull(value, nameof(value));
        return new(columnName, ColumnPredicateOperator.GreaterThan, value, null, null);
    }

    /// <summary>Creates a greater-than-or-equal predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Inclusive lower bound; non-null.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate GreaterThanOrEqual(string columnName, object value)
    {
        Guard.NotNull(value, nameof(value));
        return new(columnName, ColumnPredicateOperator.GreaterThanOrEqual, value, null, null);
    }

    /// <summary>Creates a strictly-less-than predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Exclusive upper bound; non-null.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate LessThan(string columnName, object value)
    {
        Guard.NotNull(value, nameof(value));
        return new(columnName, ColumnPredicateOperator.LessThan, value, null, null);
    }

    /// <summary>Creates a less-than-or-equal predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="value">Inclusive upper bound; non-null.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate LessThanOrEqual(string columnName, object value)
    {
        Guard.NotNull(value, nameof(value));
        return new(columnName, ColumnPredicateOperator.LessThanOrEqual, value, null, null);
    }

    /// <summary>Creates an inclusive range predicate (<paramref name="lower"/> &lt;= value &lt;= <paramref name="upper"/>).</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="lower">Inclusive lower bound; non-null.</param>
    /// <param name="upper">Inclusive upper bound; non-null.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate Between(string columnName, object lower, object upper)
    {
        Guard.NotNull(lower, nameof(lower));
        Guard.NotNull(upper, nameof(upper));
        return new(columnName, ColumnPredicateOperator.Between, lower, upper, null);
    }

    /// <summary>Creates a set-membership predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="values">Candidate values; a column value equal to any element satisfies the predicate.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate In(string columnName, params object?[] values)
    {
        Guard.NotNull(values, nameof(values));
        return new(columnName, ColumnPredicateOperator.In, null, null, (object?[])values.Clone());
    }

    /// <summary>Creates a set-membership predicate.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <param name="values">Candidate values; a column value equal to any element satisfies the predicate.</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate In(string columnName, IEnumerable<object?> values)
    {
        Guard.NotNull(values, nameof(values));
        return new(columnName, ColumnPredicateOperator.In, null, null, [.. values]);
    }

    /// <summary>Creates a predicate that matches rows whose column is database null.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate IsNull(string columnName)
        => new(columnName, ColumnPredicateOperator.IsNull, null, null, null);

    /// <summary>Creates a predicate that matches rows whose column is not database null.</summary>
    /// <param name="columnName">Column name (case-insensitive).</param>
    /// <returns>The predicate.</returns>
    public static ColumnPredicate IsNotNull(string columnName)
        => new(columnName, ColumnPredicateOperator.IsNotNull, null, null, null);
}
