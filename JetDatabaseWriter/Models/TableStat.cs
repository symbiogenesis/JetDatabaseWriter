namespace JetDatabaseWriter.Models;

using JetDatabaseWriter.Core;

/// <summary>
/// Name, stored row-count, and column-count for a single user table.
/// Returned as an element of the list from <see cref="IAccessReader.GetTableStatsAsync"/>.
/// </summary>
public sealed record TableStat
{
    /// <summary>Gets the table name.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Gets the row count stored in the TDEF page.
    /// May be stale after bulk deletes or imports without a Compact &amp; Repair.
    /// Use <see cref="IAccessReader.GetRealRowCountAsync"/> for an accurate count.
    /// </summary>
    public long RowCount { get; init; }

    /// <summary>Gets the number of columns defined in the table schema.</summary>
    public int ColumnCount { get; init; }
}
