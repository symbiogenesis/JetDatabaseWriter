namespace JetDatabaseWriter.Maintenance;

/// <summary>Exact whole-destination interval and independent allocation-evidence correspondence only.</summary>
/// <param name="PinnedPages">The retained source prefix, including opaque and possibly unused storage.</param>
/// <param name="DestinationPages">All destination pages covered exactly once.</param>
/// <param name="Records">The compared allocation records, including the synthesized pinned prefix.</param>
/// <param name="ReadBytes">The cumulative admitted ledger read requests, including partial retries.</param>
internal sealed record CompactGlobalAllocationValidatorResult(long PinnedPages, long DestinationPages, long Records, long ReadBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether global incoming references authorize reclamation; always false.</summary>
    public bool OwnershipProven { get; }
}
