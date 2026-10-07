namespace JetDatabaseWriter.Schema.Models;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Expressions;

/// <summary>
/// Per-column constraint metadata used at insert and update time to apply default values,
/// auto-increment, required-field, and validation rule semantics.
/// </summary>
internal sealed class ColumnConstraint
{
    public string Name { get; set; } = string.Empty;

    public ColumnType StorageType { get; set; }

    public Type ClrType { get; set; } = typeof(object);

    public bool IsNullable { get; set; } = true;

    public object? DefaultValue { get; set; }

    public bool IsAutoIncrement { get; set; }

    public Func<object?, bool>? ValidationRule { get; set; }

    /// <summary>
    /// Gets or sets the persisted Access <c>DefaultValue</c> expression. Evaluated on
    /// insert when <see cref="DefaultValue"/> is not set.
    /// </summary>
    public string? DefaultValueExpression { get; set; }

    /// <summary>
    /// Gets or sets the persisted Access column <c>ValidationRule</c> expression.
    /// </summary>
    public string? ValidationRuleExpression { get; set; }

    /// <summary>
    /// Gets or sets the persisted Access <c>ValidationText</c> shown when
    /// <see cref="ValidationRuleExpression"/> rejects a value.
    /// </summary>
    public string? ValidationText { get; set; }

    public bool IsCalculated { get; set; }

    public string? CalculationExpression { get; set; }

    public ColumnType CalculatedResultType { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the column is a complex
    /// (Attachment / multi-value / version-history) column, whose 4-byte slot
    /// holds the row's per-row complex reference. An insert that leaves it
    /// null gets the row's reference, shared by all the row's complex columns.
    /// </summary>
    public bool IsComplexReference { get; set; }

    /// <summary>
    /// Gets or sets lazy-seeded next auto-increment value (max(TDEF counter, existing) + 1). Null until first use.
    /// On a table's first <see cref="IsComplexReference"/> column it is instead the next per-row complex
    /// reference (max(TDEF complex AutoNumber, references in use) + 1).
    /// </summary>
    public long? NextAutoValue { get; set; }

    internal CalculatedExpressionPlan? CalculatedExpressionPlan { get; set; }

    /// <summary>
    /// Gets or sets the parsed <see cref="DefaultValueExpression"/>, or
    /// <see cref="ColumnDefaultValue.Unsupported"/> once it is known not to parse.
    /// </summary>
    internal ColumnDefaultValue? DefaultValuePlan { get; set; }

    /// <summary>
    /// Gets or sets the parsed <see cref="ValidationRuleExpression"/>, or
    /// <see cref="ColumnValidationRule.Unsupported"/> once it is known not to parse.
    /// </summary>
    internal ColumnValidationRule? ValidationRulePlan { get; set; }

    public bool HasAnyConstraint =>
        !this.IsNullable
        || this.DefaultValue != null
        || this.IsAutoIncrement
        || this.ValidationRule != null
        || this.DefaultValueExpression != null
        || this.ValidationRuleExpression != null
        || this.IsCalculated
        || this.IsComplexReference;
}
