namespace JetDatabaseWriter.Maintenance;

/// <summary>Aggregate admission for exact original-prefix preservation and reconstructed known definition patches.</summary>
internal sealed class CompactPinnedPrefixValidatorBudget
{
    /// <summary>Gets the maximum number of original physical pages, including opaque storage and the header.</summary>
    public long MaxPages { get; init; } = 1_000_000;

    /// <summary>Gets the maximum number of supplied pinned definition reference sets.</summary>
    public int MaxDefinitions { get; init; } = 4096;

    /// <summary>Gets the maximum aggregate physical pages in the complete supplied definition chains.</summary>
    public long MaxDefinitionPages { get; init; } = 16_384;

    /// <summary>Gets cumulative source/private read admission, including repeated reads and partial-read retries.</summary>
    public long MaxReadBytes { get; init; } = 16L * 1024 * 1024 * 1024;

    /// <summary>Gets conservative additional page, reconstructed image, parser, patch and chain identity storage admission.</summary>
    /// <remarks>Caller-owned original images, collections, stream storage, pooled retention and runtime overhead are separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 16L * 1024 * 1024;
}
