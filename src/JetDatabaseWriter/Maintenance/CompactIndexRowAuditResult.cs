namespace JetDatabaseWriter.Maintenance;

/// <summary>Exact leaf-key correspondence with independently decoded physically discoverable same-owner rows.</summary>
/// <param name="SourceRows">The independently discovered live row count, including omitted all-null rows.</param>
/// <param name="IndexedRows">The row and stored leaf-entry count after the all-null exclusion policy.</param>
/// <param name="ExcludedNullRows">The all-null rows omitted by the physical descriptor's IgnoreNulls flag.</param>
/// <remarks>Missing physical owners, catalog recoveries, constraints and exclusive incoming-reference ownership remain unproven.</remarks>
internal sealed record CompactIndexRowAuditResult(long SourceRows, long IndexedRows, long ExcludedNullRows)
{
    /// <summary>Gets a value indicating whether each observed leaf's exact encoded key matches its independently decoded row.</summary>
    public bool StoredKeysMatchRows { get; } = true;

    /// <summary>Gets a value indicating whether each independently discovered included row has exactly one observed leaf entry.</summary>
    public bool DiscoveredRowsCovered { get; } = true;

    /// <summary>Gets a value indicating whether unknown or damaged physical owners are completely covered; always false.</summary>
    public bool CompleteSourceOwnershipProven { get; }

    /// <summary>Gets a value indicating whether all storage remains pinned.</summary>
    public bool IsPinned { get; } = true;
}
