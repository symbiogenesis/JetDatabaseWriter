namespace JetDatabaseWriter.Maintenance;

/// <summary>Bounds one private stored-index tree and its exact ownership map.</summary>
internal sealed record CompactRawIndexAssemblyBudget
{
    /// <summary>Gets the maximum appended tree and ownership-map pages together.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum appended destination bytes across both allocations.</summary>
    public long MaxOutputBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum original leaf entries per construction or validation pass.</summary>
    public long MaxEntries { get; init; } = 4_194_304;

    /// <summary>Gets the canonical-byte allowance per phase, including repeated validation comparisons.</summary>
    public long MaxCanonicalBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the aggregate fence scratch bytes read and written during construction.</summary>
    public long MaxFenceBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the aggregate reopened destination read admission for tree and map validation.</summary>
    /// <remarks>The separately opened row relocation map has its own cumulative read allowance.</remarks>
    public long MaxReadBytes { get; init; } = 16L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum tree height, including leaves.</summary>
    public int MaxLevels { get; init; } = 32;

    /// <summary>Gets the simultaneous assembly and active component working-storage allowance.</summary>
    /// <remarks>Caller streams, row map, input enumerators, runtime overhead and shared pools are separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 512 * 1024 * 1024;

    /// <summary>Gets the independent graph, routing, entry, reference and storage allowances for the reopened tree audit.</summary>
    public CompactIndexBudget IndexAudit { get; init; } = new();
}
