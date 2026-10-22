namespace JetDatabaseWriter.Maintenance;

/// <summary>Limits for private all-pinned native global-map installation or independent comparison.</summary>
internal sealed class CompactGlobalMapBudget
{
    /// <summary>Gets the maximum complete destination pages, including appended map storage.</summary>
    public long MaxPages { get; init; } = 0x1000000;

    /// <summary>Gets the maximum newly appended reference bitmap pages.</summary>
    public long MaxBitmapPages { get; init; } = 1024;

    /// <summary>Gets cumulative source and private read requests, including partial-read retries.</summary>
    public long MaxReadBytes { get; init; } = 32 * 1024 * 1024;

    /// <summary>Gets cumulative complete private page writes, including page one.</summary>
    public long MaxWriteBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Gets the conservative simultaneous page-buffer and bookkeeping reservation.</summary>
    /// <remarks>Caller streams and runtime overhead are excluded; this is not an exact heap guarantee.</remarks>
    public long MaxWorkingBytes { get; init; } = 65_536;
}
