namespace JetDatabaseWriter.Maintenance;

/// <summary>Limits for one private raw-key index build, including repeated fence I/O.</summary>
internal sealed record CompactRawIndexBuilderBudget
{
    /// <summary>Gets the maximum input leaf entries.</summary>
    public long MaxEntries { get; init; } = 4_194_304;

    /// <summary>Gets the aggregate input key and four-byte bookmark allowance.</summary>
    public long MaxCanonicalBytes { get; init; } = 1024 * 1024 * 1024;

    /// <summary>Gets the maximum appended leaf and intermediate pages.</summary>
    public long MaxTreePages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum appended destination bytes, excluding its preexisting prefix.</summary>
    public long MaxOutputBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the aggregate bytes read from and written to both fence streams across every level.</summary>
    public long MaxFenceBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum tree height, counting the leaf level.</summary>
    public int MaxLevels { get; init; } = 32;

    /// <summary>Gets the conservative builder-owned key, page and collection storage admission limit.</summary>
    /// <remarks>Caller streams, input enumeration, runtime overhead and caller-retained buffers are excluded; this is not an exact heap guarantee.</remarks>
    public long MaxWorkingBytes { get; init; } = 4 * 1024 * 1024;
}
