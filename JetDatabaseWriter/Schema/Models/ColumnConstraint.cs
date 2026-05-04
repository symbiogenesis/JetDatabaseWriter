namespace JetDatabaseWriter.Schema.Models;

using System;

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

    // Lazy-seeded next auto-increment value (max(existing) + 1). Null until first use.
    public long? NextAutoValue { get; set; }

    public bool HasAnyConstraint =>
        !IsNullable || DefaultValue != null || IsAutoIncrement || ValidationRule != null;
}
