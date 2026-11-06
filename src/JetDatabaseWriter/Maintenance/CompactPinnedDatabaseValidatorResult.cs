namespace JetDatabaseWriter.Maintenance;

/// <summary>Composed private comparisons of every discovered local table, native global map, prefix and allocation.</summary>
/// <param name="Tables">The complete independently inventoried local table membership compared.</param>
/// <param name="GlobalMap">The independent native all-pinned global-map comparison.</param>
/// <param name="Prefix">The complete retained-prefix and reconstructed pointer comparison.</param>
/// <param name="Allocations">Exact complete destination coverage by independently compared allocation evidence.</param>
/// <param name="ReadBytes">Cumulative requested source and private bytes across all comparison phases.</param>
/// <param name="CatalogObjects">The independently compared complete discovered source and active destination catalog identities.</param>
/// <param name="ActiveTables">The ordinary tables whose scoped active rows were independently corroborated.</param>
/// <param name="UnassignedRows">The physical rows outside those corroborated sets; every such row remains pinned and reported.</param>
/// <remarks>
/// Known local-table membership is bound to an independent source inventory. When validated without a
/// <see cref="CompactReclamationPlan"/>, whole-database, ownership, and reclamation flags remain false.
/// </remarks>
internal sealed record CompactPinnedDatabaseValidatorResult(
    int Tables,
    CompactGlobalMapResult GlobalMap,
    CompactPinnedPrefixValidatorResult Prefix,
    CompactGlobalAllocationValidatorResult Allocations,
    long ReadBytes,
    int CatalogObjects,
    int ActiveTables,
    long UnassignedRows)
{
    /// <summary>Gets a value indicating whether independently discovered source and active destination inventories matched.</summary>
    public bool DestinationInventoryCompared { get; } = true;

    /// <summary>Gets a value indicating whether the complete database has been validated.</summary>
    public bool DatabaseValidated { get; init; }

    /// <summary>Gets a value indicating whether complete incoming-reference ownership is established.</summary>
    public bool OwnershipProven { get; init; }

    /// <summary>Gets a value indicating whether pinned source storage may be reclaimed.</summary>
    public bool ReclamationAuthorized { get; init; }
}
