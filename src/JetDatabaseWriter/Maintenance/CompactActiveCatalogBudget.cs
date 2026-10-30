namespace JetDatabaseWriter.Maintenance;

/// <summary>Bounds independent active catalog comparison and retained exact bookmark membership.</summary>
internal sealed record CompactActiveCatalogBudget
{
    /// <summary>Gets the maximum original live catalog rows retained in both bookmark sets.</summary>
    public long MaxRows { get; init; } = 1_048_576;

    /// <summary>Gets cumulative destination, ledger and retained-prefix comparison read requests, including retries.</summary>
    /// <remarks>Original row and LVAL scans additionally consume the shared discovery session's page-read allowance.</remarks>
    public long MaxReadBytes { get; init; } = 32L * 1024 * 1024 * 1024;

    /// <summary>Gets simultaneous retained membership, scratch and nested raw-table comparison admission.</summary>
    public long MaxWorkingBytes { get; init; } = 512L * 1024 * 1024;

    /// <summary>Gets independent raw catalog, LVAL, allocation-ledger and native-map comparison limits.</summary>
    public CompactRawTableValidatorBudget TableValidator { get; init; } = new();
}
