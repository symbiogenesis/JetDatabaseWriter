namespace JetDatabaseWriter.Maintenance;

/// <summary>Aggregate admission for independently checking one private native allocation map.</summary>
internal sealed record CompactUsageMapValidatorBudget
{
    /// <summary>Gets the maximum expected allocation runs.</summary>
    public long MaxRuns { get; init; } = 8_388_608;

    /// <summary>Gets the maximum listed allocation members.</summary>
    public long MaxListedPages { get; init; } = 16_777_216;

    /// <summary>Gets the maximum private row and bitmap pages.</summary>
    public long MaxPages { get; init; } = 1024;

    /// <summary>Gets the maximum inspected bitmap bytes, including zero bytes.</summary>
    public long MaxBitmapBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Gets the maximum requested read bytes, including partial-read retries.</summary>
    public long MaxReadBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets the fixed page-buffer and enumerator storage allowance.</summary>
    /// <remarks>Caller enumeration, streams, runtime overhead and shared pools are separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 32 * 1024;
}
