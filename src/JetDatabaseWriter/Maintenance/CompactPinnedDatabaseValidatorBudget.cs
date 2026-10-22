namespace JetDatabaseWriter.Maintenance;

/// <summary>One-shot cumulative private database comparison admission and active-component limits.</summary>
internal sealed class CompactPinnedDatabaseValidatorBudget
{
    /// <summary>Gets the maximum complete source and private destination pages.</summary>
    public long MaxPages { get; init; } = 0x1000000;

    /// <summary>Gets the maximum local table evidence sets, including the catalog.</summary>
    public int MaxTables { get; init; } = 4096;

    /// <summary>Gets cumulative requested source, destination and sidecar bytes across every phase and partial retry.</summary>
    /// <remarks>Caller-provided observations and ledger-opening provider I/O are outside this admission.</remarks>
    public long MaxReadBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets conservative simultaneously retained coordinator and active-component storage admission.</summary>
    /// <remarks>Immutable caller evidence, stream storage, shared pools and runtime overhead remain separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 512L * 1024 * 1024;

    /// <summary>Gets cumulative inventory, table, stored-index and definition comparison limits.</summary>
    public CompactPinnedTableAssemblyBudget Tables { get; init; } = new();

    /// <summary>Gets native global-map comparison limits.</summary>
    public CompactGlobalMapBudget GlobalMap { get; init; } = new();

    /// <summary>Gets complete pinned-prefix comparison limits.</summary>
    public CompactPinnedPrefixValidatorBudget Prefix { get; init; } = new();

    /// <summary>Gets complete allocation-ledger comparison limits.</summary>
    public CompactGlobalAllocationBudget Allocation { get; init; } = new();
}
