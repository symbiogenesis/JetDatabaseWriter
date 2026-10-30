namespace JetDatabaseWriter.Maintenance;

/// <summary>Cumulative private construction admission with separately bounded independent reopened validation.</summary>
internal sealed record CompactPinnedDatabaseBuilderBudget
{
    /// <summary>Gets the maximum complete source and destination pages, including pinned and global-map storage.</summary>
    public long MaxPages { get; init; } = 0x1000000;

    /// <summary>Gets the maximum complete destination length, including the original prefix.</summary>
    public long MaxOutputBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum complete discovered local-table plan size.</summary>
    public int MaxTables { get; init; } = 4096;

    /// <summary>Gets cumulative source and private construction read requests, including failed requests and partial retries.</summary>
    public long MaxReadBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets cumulative destination and sidecar construction writes, including failed requests and rewrites.</summary>
    public long MaxWriteBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets conservative simultaneously retained coordinator evidence and active-component storage admission.</summary>
    /// <remarks>Caller evidence, factory work, stream storage, runtime overhead and pool retention remain separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 512L * 1024 * 1024;

    /// <summary>Gets shared source inventory and per-table construction admission.</summary>
    public CompactPinnedTableAssemblyBudget Tables { get; init; } = new();

    /// <summary>Gets per-table active membership limits, also charged to shared construction discovery admission.</summary>
    public CompactActiveTableBudget ActiveTables { get; init; } = new();

    /// <summary>Gets native all-pinned global-map construction limits.</summary>
    public CompactGlobalMapBudget GlobalMap { get; init; } = new();

    /// <summary>Gets complete allocation-sidecar construction limits.</summary>
    public CompactGlobalAllocationBudget Allocation { get; init; } = new();

    /// <summary>Gets separate cumulative independent reopened comparison limits.</summary>
    public CompactPinnedDatabaseValidatorBudget Validation { get; init; } = new();
}
