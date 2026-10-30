namespace JetDatabaseWriter.Maintenance;

/// <summary>Cumulative bounds for one private global allocation ledger construction or comparison.</summary>
internal sealed record CompactGlobalAllocationBudget
{
    /// <summary>Gets the maximum records, including the single synthesized pinned-source record.</summary>
    public long MaxRecords { get; init; } = 1_048_576;

    /// <summary>Gets the maximum total destination pages, including every pinned and map page.</summary>
    public long MaxPages { get; init; } = 0x1000000;

    /// <summary>Gets the maximum complete ledger bytes, including its header.</summary>
    public long MaxLedgerBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets cumulative validator read admission, including the full request before each partial read.</summary>
    public long MaxReadBytes { get; init; } = 128 * 1024 * 1024;

    /// <summary>Gets the conservative fixed buffer and bookkeeping reservation.</summary>
    /// <remarks>Caller streams, iterators and runtime overhead are excluded; this is not an exact heap guarantee.</remarks>
    public long MaxWorkingBytes { get; init; } = 4096;
}
