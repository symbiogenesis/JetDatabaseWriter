namespace JetDatabaseWriter.Maintenance;

/// <summary>Bounds one raw long-value audit, including storage beyond the descriptor's declared payload.</summary>
internal sealed record CompactLongValueBudget
{
    /// <summary>Gets the maximum source size in physical pages.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum external-row reads, including repeated physical pages.</summary>
    public long MaxPageReads { get; init; } = 65_536;

    /// <summary>Gets the maximum external rows and cycle-tracking entries.</summary>
    public int MaxChainRows { get; init; } = 65_536;

    /// <summary>Gets the maximum inspected directory slots, including deleted slots and repeated directories.</summary>
    public long MaxDirectorySlots { get; init; } = 268_435_456;

    /// <summary>Gets the maximum complete column and external-row bytes delivered, including descriptors, pointers and trailing bytes.</summary>
    public long MaxRawBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets the conservative storage admission limit, excluding caller allocations, runtime overhead and shared-pool retention.</summary>
    public long MaxWorkingBytes { get; init; } = 8 * 1024 * 1024;
}
