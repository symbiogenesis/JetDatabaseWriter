namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Metadata;

/// <summary>
/// Comparison operators eligible for B-tree index seek pushdown.
/// </summary>
public enum JetIndexSeekOperator
{
    /// <summary>Exact equality comparison (<c>==</c>).</summary>
    Equal = 0,

    /// <summary>Strict greater-than comparison (<c>&gt;</c>).</summary>
    GreaterThan = 1,

    /// <summary>Greater-than-or-equal comparison (<c>&gt;=</c>).</summary>
    GreaterThanOrEqual = 2,

    /// <summary>Strict less-than comparison (<c>&lt;</c>).</summary>
    LessThan = 3,

    /// <summary>Less-than-or-equal comparison (<c>&lt;=</c>).</summary>
    LessThanOrEqual = 4,

    /// <summary>Set membership comparison (<c>Contains</c> / <c>IN</c>).</summary>
    In = 5,
}

/// <summary>
/// Represents a single property comparison extracted from a LINQ <c>Where</c> predicate for index seek evaluation.
/// </summary>
public sealed class JetIndexSeekCondition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JetIndexSeekCondition"/> class.
    /// </summary>
    /// <param name="property">The EF Core property compared by the predicate.</param>
    /// <param name="columnName">The resolved JET column name.</param>
    /// <param name="seekOperator">The comparison operator.</param>
    /// <param name="valueEvaluator">A delegate that evaluates the provider-typed comparison value from the runtime query parameters.</param>
    public JetIndexSeekCondition(
        IProperty property,
        string columnName,
        JetIndexSeekOperator seekOperator,
        Func<IReadOnlyDictionary<string, object?>, object?> valueEvaluator)
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentException.ThrowIfNullOrWhiteSpace(columnName);
        ArgumentNullException.ThrowIfNull(valueEvaluator);

        this.Property = property;
        this.ColumnName = columnName;
        this.Operator = seekOperator;
        this.ValueEvaluator = valueEvaluator;
    }

    /// <summary>
    /// Gets the EF Core property compared by the predicate.
    /// </summary>
    public IProperty Property { get; }

    /// <summary>
    /// Gets the resolved JET column name.
    /// </summary>
    public string ColumnName { get; }

    /// <summary>
    /// Gets the comparison operator.
    /// </summary>
    public JetIndexSeekOperator Operator { get; }

    /// <summary>
    /// Gets the delegate that evaluates the provider-typed comparison value from the runtime query parameters.
    /// </summary>
    public Func<IReadOnlyDictionary<string, object?>, object?> ValueEvaluator { get; }
}
