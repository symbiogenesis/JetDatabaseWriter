namespace JetDatabaseWriter.Maintenance;

/// <summary>Limits for constructing one private native usage-map row.</summary>
internal sealed record CompactUsageMapBuilderBudget
{
    /// <summary>Gets the maximum input allocation runs, including adjacent runs.</summary>
    public long MaxRuns { get; init; } = 1_048_576;

    /// <summary>Gets the maximum included pages, bounding bitmap work independently of the run count.</summary>
    public long MaxListedPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum appended row and reference bitmap pages.</summary>
    public long MaxPages { get; init; } = 1024;

    /// <summary>Gets the maximum appended allocation bytes, excluding the immutable existing prefix.</summary>
    public long MaxOutputBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>Gets the conservative builder-owned buffer reservation limit.</summary>
    /// <remarks>Caller streams, iterators and runtime overhead are excluded; this is not an exact heap guarantee.</remarks>
    public long MaxWorkingBytes { get; init; } = 65_536;
}
