namespace JetDatabaseWriter.Maintenance;

/// <summary>Hard limits retained across declared roots and known incoming index references.</summary>
internal sealed class CompactIndexOwnershipBudget
{
    /// <summary>Gets the optional accounting shared with the surrounding discovery session.</summary>
    internal CompactDiscoveryAccounting? Accounting { get; init; }

    /// <summary>Gets the maximum distinct root, observed-page and reference-target coordinates retained.</summary>
    public int MaxTrackedPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum physical descriptor declarations, including same-owner shared roots.</summary>
    public long MaxRootDescriptors { get; init; } = 65_536;

    /// <summary>Gets the maximum incoming tree-reference observations, including repeated scans.</summary>
    public long MaxReferences { get; init; } = 8_388_608;

    /// <summary>Gets the maximum declaration, input-observation and final state-inspection steps.</summary>
    public long MaxGraphSteps { get; init; } = 67_108_864;

    /// <summary>Gets the maximum emitted ownership observations.</summary>
    public long MaxRecords { get; init; } = 8_388_608;

    /// <summary>Gets the conservative tracker-owned collection and scratch admission limit.</summary>
    /// <remarks>Caller retention, runtime overhead and other scans require separate allowances.</remarks>
    public long MaxWorkingBytes { get; init; } = 512 * 1024 * 1024;
}
