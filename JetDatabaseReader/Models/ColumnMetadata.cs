namespace JetDatabaseReader;

using System;

/// <summary>
/// Rich metadata about a database column including type, size, and nullability information.
/// </summary>
public sealed class ColumnMetadata
{
    /// <summary>Gets or sets the column name.</summary>
    public string Name { get; set; }

    /// <summary>Gets or sets the Access-friendly type name (e.g., "Text", "Long Integer", "Date/Time").</summary>
    public string TypeName { get; set; }

    /// <summary>Gets or sets the CLR type that best represents this column.</summary>
    public Type ClrType { get; set; }

    /// <summary>Gets or sets the maximum length for variable-length columns, or null for fixed-length.</summary>
    public int? MaxLength { get; set; }

    /// <summary>Gets or sets a value indicating whether the column allows null values (always true for JET databases).</summary>
    public bool IsNullable { get; set; }

    /// <summary>Gets or sets a value indicating whether the column is fixed-length.</summary>
    public bool IsFixedLength { get; set; }

    /// <summary>Gets or sets the zero-based ordinal position in the table.</summary>
    public int Ordinal { get; set; }

    /// <summary>Gets or sets the structured size — use <see cref="ColumnSize.ToString"/> for a human-readable description.</summary>
    public ColumnSize Size { get; set; }
}
