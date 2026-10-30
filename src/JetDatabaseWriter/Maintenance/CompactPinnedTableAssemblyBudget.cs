namespace JetDatabaseWriter.Maintenance;

/// <summary>Bounds the complete private table session, with cumulative source and separate active-component allowances.</summary>
internal sealed record CompactPinnedTableAssemblyBudget
{
    /// <summary>Gets the cumulative appended or independently compared table, index and map page allowance.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the cumulative appended or independently compared destination byte allowance.</summary>
    public long MaxOutputBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets cumulative private destination and scratch read requests across every invocation, including retries.</summary>
    public long MaxReadBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets cumulative destination, ledger and reusable fence write requests during construction.</summary>
    public long MaxWriteBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets cumulative original entries consumed across every physical index, including repeated validation passes.</summary>
    public long MaxObservedEntries { get; init; } = 16_777_216;

    /// <summary>Gets cumulative original key bytes consumed across every index and repeated validation pass.</summary>
    public long MaxObservedKeyBytes { get; init; } = 16L * 1024 * 1024 * 1024;

    /// <summary>Gets simultaneous retained composition and active-component working-storage admission.</summary>
    /// <remarks>Component allowances are capped to the remainder after retained composition; caller evidence, streams and runtime overhead are separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 512L * 1024 * 1024;

    /// <summary>Gets source traversal limits shared across every session invocation; storage is additionally capped by the pipeline allowance.</summary>
    public CompactDiscoveryBudget Discovery { get; init; } = new();

    /// <summary>Gets raw data, LVAL and table-map construction limits.</summary>
    public CompactRawTableBuilderBudget TableBuilder { get; init; } = new();

    /// <summary>Gets independent reopened raw table and map comparison limits.</summary>
    public CompactRawTableValidatorBudget TableValidator { get; init; } = new();

    /// <summary>Gets per-index active tree/map construction and comparison limits, within the aggregate allowances above.</summary>
    public CompactRawIndexAssemblyBudget Index { get; init; } = new();

    /// <summary>Gets pinned-chain source/private traversal, installation and comparison limits.</summary>
    public CompactTDefChainBudget Definition { get; init; } = new();
}
