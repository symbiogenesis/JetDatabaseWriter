namespace JetDatabaseWriter.Models;

/// <summary>
/// Structured progress information reported during bulk table read operations.
/// </summary>
public sealed class TableProgress
{
    /// <summary>Gets the name of the table currently being read.</summary>
    public string TableName { get; init; } = string.Empty;

    /// <summary>Gets the zero-based index of the current table in the list.</summary>
    public int TableIndex { get; init; }

    /// <summary>Gets the total number of tables to read.</summary>
    public int TableCount { get; init; }
}
