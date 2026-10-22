namespace JetDatabaseWriter.Maintenance;

/// <summary>Exact coverage of independently discovered local definitions by a proposed source plan.</summary>
/// <param name="LocalTables">The matched distinct local definition count, including the catalog.</param>
/// <param name="SourcePages">The unchanged original physical page count.</param>
/// <remarks>Missing catalog ownership can hide objects. This result proves neither complete discovery nor permission to reclaim or publish storage.</remarks>
internal sealed record CompactDatabasePlanResult(int LocalTables, long SourcePages)
{
    /// <summary>Gets a value indicating whether all unknown objects were discovered; always false.</summary>
    public bool DiscoveryComplete { get; }

    /// <summary>Gets a value indicating whether complete incoming ownership permits reclamation; always false.</summary>
    public bool OwnershipProven { get; }

    /// <summary>Gets a value indicating whether the complete destination database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }
}
