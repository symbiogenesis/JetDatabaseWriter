namespace JetDatabaseWriter.Schema.Models;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Expressions;

/// <summary>
/// Per-column constraint metadata used at insert time to apply default values,
/// auto-increment, required-field, and validation rule semantics.
/// </summary>
internal sealed class ColumnConstraint
{
    public string Name { get; set; } = string.Empty;

    public Type ClrType { get; set; } = typeof(object);

    public bool IsNullable { get; set; } = true;

    public object? DefaultValue { get; set; }

    public bool IsAutoIncrement { get; set; }

    public Func<object?, bool>? ValidationRule { get; set; }

    public bool IsCalculated { get; set; }

    public string? CalculationExpression { get; set; }

    public ColumnType CalculatedResultType { get; set; }

    /// <summary>
    /// Gets or sets lazy-seeded next auto-increment value (max(existing) + 1). Null until first use.
    /// </summary>
    public long? NextAutoValue { get; set; }

    internal CalculatedExpressionPlan? CalculatedExpressionPlan { get; set; }

    public bool HasAnyConstraint =>
        !this.IsNullable || this.DefaultValue != null || this.IsAutoIncrement || this.ValidationRule != null || this.IsCalculated;
}
