namespace JetDatabaseWriter.Maintenance;

/// <summary>Completed private chain installation or comparison evidence, without database acceptance.</summary>
/// <param name="Definitions">The number of completely traversed requested definitions.</param>
/// <param name="Pages">The number of distinct pinned physical pages.</param>
/// <param name="LogicalBytes">The complete logical bytes, including opaque padding.</param>
/// <param name="ReadBytes">The admitted source and private read requests, including partial-read retries.</param>
/// <param name="WrittenBytes">The complete private physical page bytes written; zero for comparison.</param>
internal sealed record CompactTDefChainResult(int Definitions, long Pages, long LogicalBytes, long ReadBytes, long WrittenBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }
}
