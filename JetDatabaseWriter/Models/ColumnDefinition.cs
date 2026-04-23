namespace JetDatabaseWriter;

using System;

/// <summary>
/// Defines a column for use with <see cref="IAccessWriter.CreateTableAsync"/>.
/// </summary>
public sealed record ColumnDefinition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ColumnDefinition"/> class.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="clrType">The CLR type for this column (e.g., typeof(string), typeof(int)).</param>
    /// <param name="maxLength">Maximum length for variable-length types (e.g., string). Ignored for fixed-length types.</param>
    public ColumnDefinition(string name, Type clrType, int maxLength = 0)
    {
        Name = name;
        ClrType = clrType;
        MaxLength = maxLength;
    }

    /// <summary>Gets the column name.</summary>
    public string Name { get; }

    /// <summary>Gets the CLR type that this column stores.</summary>
    public Type ClrType { get; }

    /// <summary>Gets the maximum length for variable-length columns. 0 means default.</summary>
    public int MaxLength { get; }
}
