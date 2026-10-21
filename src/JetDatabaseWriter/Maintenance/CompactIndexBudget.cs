namespace JetDatabaseWriter.Maintenance;

/// <summary>Hard limits for one read-only raw index-tree observation.</summary>
internal sealed class CompactIndexBudget
{
    /// <summary>Gets the optional whole-session accounting shared by all discovery scans.</summary>
    internal CompactDiscoveryAccounting? Accounting { get; init; }

    /// <summary>Gets the maximum discovered tree pages, including queued pages, and therefore page reads.</summary>
    public int MaxTreePages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum decoded leaf and intermediate entries.</summary>
    public long MaxEntries { get; init; } = 4_194_304;

    /// <summary>Gets the aggregate reconstructed entry byte allowance, including bookmarks and child pointers.</summary>
    public long MaxCanonicalBytes { get; init; } = 1024 * 1024 * 1024;

    /// <summary>Gets the maximum bytes in canonical buffers retained for separator and leaf-boundary proofs, including pointer trailers.</summary>
    public long MaxRetainedKeyBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets the maximum nonzero header, child and bookmark reference observations.</summary>
    public long MaxReferences { get; init; } = 8_388_608;

    /// <summary>Gets the maximum node, edge, separator and leaf-link inspections used by graph and ordering proofs.</summary>
    public long MaxGraphSteps { get; init; } = 67_108_864;

    /// <summary>Gets the maximum visitor records.</summary>
    public long MaxRecords { get; init; } = 8_388_608;

    /// <summary>Gets the conservative scan-owned collection and scratch storage admission limit.</summary>
    /// <remarks>Caller/visitor retention, runtime overhead and shared pool retention are excluded; this is not a heap guarantee.</remarks>
    public long MaxWorkingBytes { get; init; } = 512 * 1024 * 1024;
}
