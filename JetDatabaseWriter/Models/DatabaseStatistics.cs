namespace JetDatabaseWriter.Models;

using System.Collections.Generic;
using System.Collections.ObjectModel;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;

/// <summary>
/// Statistical information about the database including size, table counts, and cache performance.
/// </summary>
public sealed class DatabaseStatistics
{
    private static readonly IReadOnlyDictionary<string, long> EmptyTableRowCounts =
        new ReadOnlyDictionary<string, long>(new Dictionary<string, long>());

    /// <summary>Gets or initializes the total number of pages in the database.</summary>
    public long TotalPages { get; init; }

    /// <summary>Gets or initializes the database file size in bytes.</summary>
    public long DatabaseSizeBytes { get; init; }

    /// <summary>Gets or initializes the number of user tables in the database.</summary>
    public int TableCount { get; init; }

    /// <summary>Gets or initializes the total number of rows across all tables (from TDEF, may be stale).</summary>
    public long TotalRows { get; init; }

    /// <summary>Gets the row count for each table.</summary>
    public IReadOnlyDictionary<string, long> TableRowCounts { get; init => field = FreezeTableRowCounts(value); } = EmptyTableRowCounts;

    /// <summary>Gets or initializes the page cache hit rate percentage (0-100).</summary>
    public int PageCacheHitRate { get; init; }

    /// <summary>Gets or initializes the JET version string (e.g., "Jet4/ACE", "Jet3").</summary>
    public string Version { get; init; } = string.Empty;

    /// <summary>Gets or initializes the database format (Jet3, Jet4, or ACE).</summary>
    public DatabaseFormat Format { get; init; }

    /// <summary>Gets the page size in bytes (2048 for Jet3, 4096 for Jet4/ACE), derived from <see cref="Format"/>.</summary>
    public int PageSize => AccessBase.GetPageSize(Format);

    /// <summary>Gets or initializes the code page identifier used for text encoding.</summary>
    public int CodePage { get; init; }

    private static IReadOnlyDictionary<string, long> FreezeTableRowCounts(IEnumerable<KeyValuePair<string, long>>? tableRowCounts)
    {
        if (tableRowCounts == null)
        {
            return EmptyTableRowCounts;
        }

        var copy = new Dictionary<string, long>();
        foreach ((string key, long value) in tableRowCounts)
        {
            if (string.IsNullOrEmpty(key))
            {
                continue;
            }

            copy[key] = value;
        }

        return copy.Count == 0 ? EmptyTableRowCounts : new ReadOnlyDictionary<string, long>(copy);
    }
}
