namespace JetDatabaseWriter.Maintenance;

/// <summary>Aggregate limits for one private pinned-definition installation or independent comparison.</summary>
internal sealed class CompactTDefChainBudget
{
    /// <summary>Gets the maximum total retained physical definition pages across every requested chain.</summary>
    public long MaxPages { get; init; } = 16_384;

    /// <summary>Gets the cumulative source and private read admission, including partial-read retries.</summary>
    public long MaxReadBytes { get; init; } = 256L * 1024 * 1024;

    /// <summary>Gets the maximum complete physical page bytes written; admitted before the first write.</summary>
    public long MaxWriteBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>Gets the additional fixed buffers and conservative retained page/dictionary storage allowance.</summary>
    /// <remarks>Caller images, streams, pooled retention and runtime overhead remain separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 4L * 1024 * 1024;
}
