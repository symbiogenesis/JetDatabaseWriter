namespace JetDatabaseWriter.Maintenance;

/// <summary>Hard limits retained across declared roots and known incoming index references.</summary>
internal sealed class CompactIndexOwnershipBudget
{
    /// <summary>Gets the optional accounting shared with the surrounding discovery session.</summary>
    internal CompactDiscoveryAccounting? Accounting { get; init; }

    /// <summary>Gets the maximum distinct root, observed-page, reference-target and map-page coordinates retained.</summary>
    public int MaxTrackedPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum distinct physical ownership-map row coordinates retained.</summary>
    public int MaxMapRows { get; init; } = 65_536;

    /// <summary>Gets the maximum physical descriptor declarations, including shared and missing roots.</summary>
    public long MaxRootDescriptors { get; init; } = 65_536;

    /// <summary>Gets the maximum map-pointer declarations and incoming tree-reference observations, including repeated scans.</summary>
    public long MaxReferences { get; init; } = 8_388_608;

    /// <summary>Gets the maximum uncached reads across independent native index-map inspections.</summary>
    public long MaxPageReads { get; init; } = 8_388_608;

    /// <summary>Gets the maximum directory-slot, pointer and bitmap-byte inspections across native maps.</summary>
    public long MaxMapSteps { get; init; } = 67_108_864;

    /// <summary>Gets the maximum retained descriptor-map membership coordinates, including repeated roots with distinct maps.</summary>
    public long MaxMemberships { get; init; } = 8_388_608;

    /// <summary>Gets the maximum declaration, input-observation and final state-inspection steps.</summary>
    public long MaxGraphSteps { get; init; } = 67_108_864;

    /// <summary>Gets the maximum emitted ownership observations.</summary>
    public long MaxRecords { get; init; } = 8_388_608;

    /// <summary>Gets the conservative tracker-owned collection and scratch admission limit.</summary>
    /// <remarks>Caller retention, runtime overhead and other scans require separate allowances.</remarks>
    public long MaxWorkingBytes { get; init; } = 512 * 1024 * 1024;
}
