namespace JetDatabaseWriter.Maintenance;

/// <summary>Completed private allocation-ledger geometry requiring an independent reopened comparison.</summary>
/// <param name="PinnedPages">The complete retained source prefix, starting at page zero.</param>
/// <param name="DestinationPages">All retained and appended destination pages.</param>
/// <param name="Records">All allocation records, including the pinned prefix.</param>
/// <param name="LedgerBytes">The exact complete private ledger length.</param>
internal sealed record CompactGlobalAllocationBuilderResult(long PinnedPages, long DestinationPages, long Records, long LedgerBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }
}
