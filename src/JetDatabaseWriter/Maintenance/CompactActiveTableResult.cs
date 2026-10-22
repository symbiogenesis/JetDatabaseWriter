namespace JetDatabaseWriter.Maintenance;

/// <summary>Independent map-and-covering-index observations with all other physical rows retained and pinned.</summary>
/// <param name="ActiveRows">Rows independently agreed by the native table map, covering keys and declared count.</param>
/// <param name="UnassignedRows">Other readable same-owner physical rows, without any inactive or reclaimable claim.</param>
/// <param name="PhysicalRows">Every readable same-owner physical row observed in the scan.</param>
internal sealed record CompactActiveTableResult(long ActiveRows, long UnassignedRows, long PhysicalRows)
{
    /// <summary>Gets a value indicating whether global discovery has been proved; always false.</summary>
    public bool DiscoveryComplete { get; }

    /// <summary>Gets a value indicating whether global ownership has been proved; always false.</summary>
    public bool OwnershipProven { get; }

    /// <summary>Gets a value indicating whether the database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }
}
