namespace JetDatabaseReader;

using System;

/// <summary>
/// Schema entry for a single column in a <see cref="TableResult"/>.
/// </summary>
public sealed record TableColumn
{
    /// <summary>Gets or initializes the column name.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Gets or initializes the CLR type that best represents this column (e.g., <see cref="string"/>, <see cref="int"/>, <see cref="DateTime"/>).</summary>
    public Type Type { get; init; } = typeof(object);

    /// <summary>Gets or initializes the structured size — use <see cref="ColumnSize.ToString"/> for a human-readable description.</summary>
    public ColumnSize Size { get; init; }
}
