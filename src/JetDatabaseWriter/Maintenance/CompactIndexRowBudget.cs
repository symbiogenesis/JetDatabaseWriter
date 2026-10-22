namespace JetDatabaseWriter.Maintenance;

/// <summary>Limits independent raw-row key derivation and retained bookmark evidence for one stored index.</summary>
internal sealed class CompactIndexRowBudget
{
    /// <summary>Gets optional whole-session admission shared with the separately bounded row and index scans.</summary>
    internal CompactDiscoveryAccounting? Accounting { get; init; }

    /// <summary>Gets the maximum independently discovered physical rows, including omitted all-null rows.</summary>
    public long MaxRows { get; init; } = 1_048_576;

    /// <summary>Gets the maximum complete encoded key bytes retained for one row.</summary>
    public int MaxKeyBytes { get; init; } = 65_536;

    /// <summary>Gets conservative retained evidence and scalar-decoding scratch storage admission.</summary>
    /// <remarks>The simultaneously active raw-row and raw-index scans, caller image, runtime/code-table initialization and shared-pool retention require separate allowances.</remarks>
    public long MaxWorkingBytes { get; init; } = 128 * 1024 * 1024;
}
