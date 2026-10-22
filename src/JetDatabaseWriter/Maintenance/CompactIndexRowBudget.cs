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

    /// <summary>Gets cumulative Memo payload, complete-chain work and simultaneous value-storage admission for this descriptor.</summary>
    /// <remarks>The enclosing key-evidence allowance additionally admits Memo buffers; a supplied parent retains charges across descriptors and failed attempts.</remarks>
    public CompactDiscoveryBudget LongValues { get; init; } = new() { MaxChainRows = 65_536, MaxWorkingBytes = 128 * 1024 * 1024 };

    /// <summary>Gets combined retained-key, scalar/Memo decoding and composed bookmark-verification storage admission.</summary>
    /// <remarks>Raw-row and raw-index scanners retain their own allowances; a supplied parent can additionally admit every scope together. The caller image, runtime/code-table initialization and shared-pool retention require separate allowances.</remarks>
    public long MaxWorkingBytes { get; init; } = 128 * 1024 * 1024;
}
