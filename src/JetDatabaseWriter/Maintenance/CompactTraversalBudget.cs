namespace JetDatabaseWriter.Maintenance;

/// <summary>Hard limits for the physical, catalog, definition and map inventory phase.</summary>
internal sealed record CompactTraversalBudget
{
    /// <summary>Gets optional whole-session counters and simultaneous storage admission.</summary>
    internal CompactDiscoveryAccounting? Accounting { get; init; }

    /// <summary>Gets the maximum physical file size in pages, checked before traversal.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum live catalog object count.</summary>
    public int MaxObjects { get; init; } = 65_536;

    /// <summary>Gets the maximum emitted known-reference count.</summary>
    public long MaxReferences { get; init; } = 4_194_304;

    /// <summary>Gets the maximum uncertainty diagnostic count.</summary>
    public int MaxDiagnostics { get; init; } = 1_024;

    /// <summary>Gets the aggregate logical table-definition byte limit.</summary>
    public int MaxSchemaBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets the total page-read limit, including repeated reads and fallback scans.</summary>
    public long MaxPageReads { get; init; } = 8_388_608;

    /// <summary>Gets the total inspected map bytes and directory slots, including empty ones.</summary>
    public long MaxMapSteps { get; init; } = 268_435_456;

    /// <summary>Gets the maximum number of observations delivered to the visitor.</summary>
    public long MaxRecords { get; init; } = 8_388_608;

    /// <summary>Gets the conservative admission allowance for scan-owned storage, excluding the caller and visitor.</summary>
    /// <remarks>This accounts for collection capacity and simultaneous schema copies; it is not a CLR heap measurement.</remarks>
    public long MaxWorkingBytes { get; init; } = 512 * 1024 * 1024;
}
