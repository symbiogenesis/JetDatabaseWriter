namespace JetDatabaseWriter.Maintenance;

/// <summary>Limits independent validation of one private stored-key allocation.</summary>
internal sealed record CompactRawIndexValidatorBudget
{
    /// <summary>Gets the maximum private allocation pages.</summary>
    public int MaxTreePages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum original and destination leaf entries.</summary>
    public long MaxEntries { get; init; } = 4_194_304;

    /// <summary>Gets the cumulative canonical bytes inspected during comparison, including repeated child maxima and pointer trailers.</summary>
    public long MaxCanonicalBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the cumulative destination read admission, including partial-read retries and failed attempts.</summary>
    /// <remarks>The separately reopened relocation map has its own cumulative read allowance.</remarks>
    public long MaxReadBytes { get; init; } = 16L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum private tree height, counting leaves.</summary>
    public int MaxLevels { get; init; } = 32;

    /// <summary>Gets the simultaneous audit and fixed comparison storage admission limit.</summary>
    /// <remarks>Caller streams, input enumeration, runtime overhead and shared pool retention are separate; this is not an exact heap guarantee.</remarks>
    public long MaxWorkingBytes { get; init; } = 512 * 1024 * 1024;

    /// <summary>Gets the independent raw graph, routing, ordering and observation allowances.</summary>
    public CompactIndexBudget IndexAudit { get; init; } = new();
}
