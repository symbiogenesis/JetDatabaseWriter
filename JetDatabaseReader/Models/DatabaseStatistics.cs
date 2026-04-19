namespace JetDatabaseReader;

using System.Collections.Generic;

/// <summary>
/// Statistical information about the database including size, table counts, and cache performance.
/// </summary>
public sealed class DatabaseStatistics
{
    /// <summary>Gets or sets the total number of pages in the database.</summary>
    public long TotalPages { get; set; }

    /// <summary>Gets or sets the database file size in bytes.</summary>
    public long DatabaseSizeBytes { get; set; }

    /// <summary>Gets or sets the number of user tables in the database.</summary>
    public int TableCount { get; set; }

    /// <summary>Gets or sets the total number of rows across all tables (from TDEF, may be stale).</summary>
    public long TotalRows { get; set; }

    /// <summary>Gets or sets the row count for each table.</summary>
    public Dictionary<string, long> TableRowCounts { get; set; }

    /// <summary>Gets or sets the page cache hit rate percentage (0-100).</summary>
    public int PageCacheHitRate { get; set; }

    /// <summary>Gets or sets the JET version string (e.g., "Jet4/ACE", "Jet3").</summary>
    public string Version { get; set; }

    /// <summary>Gets or sets the page size in bytes (2048 for Jet3, 4096 for Jet4).</summary>
    public int PageSize { get; set; }

    /// <summary>Gets or sets the code page identifier used for text encoding.</summary>
    public int CodePage { get; set; }
}
