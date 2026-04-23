namespace JetDatabaseWriter;

using System;

/// <summary>
/// Rich metadata about a database column including type, size, and nullability information.
/// </summary>
public sealed record ColumnMetadata
{
    /// <summary>Gets or initializes the column name.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Gets or initializes the Access-friendly type name (e.g., "Text", "Long Integer", "Date/Time").</summary>
    public string TypeName { get; init; } = string.Empty;

    /// <summary>Gets or initializes the CLR type that best represents this column.</summary>
    public Type ClrType { get; init; } = typeof(object);

    /// <summary>Gets or initializes the maximum length for variable-length columns, or null for fixed-length.</summary>
    public int? MaxLength { get; init; }

    /// <summary>Gets a value indicating whether the column allows null values (always true for JET databases).</summary>
    public bool IsNullable { get; init; }

    /// <summary>Gets a value indicating whether the column is fixed-length.</summary>
    public bool IsFixedLength { get; init; }

    /// <summary>Gets or initializes the zero-based ordinal position in the table.</summary>
    public int Ordinal { get; init; }

    /// <summary>Gets or initializes the structured size — use <see cref="ColumnSize.ToString"/> for a human-readable description.</summary>
    public ColumnSize Size { get; init; }
}
