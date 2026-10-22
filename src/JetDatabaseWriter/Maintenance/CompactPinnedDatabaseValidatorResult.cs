namespace JetDatabaseWriter.Maintenance;

/// <summary>Composed private comparisons of every discovered local table, native global map, prefix and allocation.</summary>
/// <param name="Tables">The complete independently inventoried local table membership compared.</param>
/// <param name="GlobalMap">The independent native all-pinned global-map comparison.</param>
/// <param name="Prefix">The complete retained-prefix and reconstructed pointer comparison.</param>
/// <param name="Allocations">Exact complete destination coverage by independently compared allocation evidence.</param>
/// <param name="ReadBytes">Cumulative requested source and private bytes across all comparison phases.</param>
/// <remarks>
/// Known local-table membership is bound to an independent source inventory. Opaque incoming references, complete
/// discoverability under damaged ownership, active destination catalog recovery, native acceptance, encryption and
/// durable publication remain separate requirements. This result never authorizes database replacement.
/// </remarks>
internal sealed record CompactPinnedDatabaseValidatorResult(
    int Tables,
    CompactGlobalMapResult GlobalMap,
    CompactPinnedPrefixValidatorResult Prefix,
    CompactGlobalAllocationValidatorResult Allocations,
    long ReadBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether complete incoming-reference ownership is established; always false.</summary>
    public bool OwnershipProven { get; }

    /// <summary>Gets a value indicating whether pinned source storage may be reclaimed; always false.</summary>
    public bool ReclamationAuthorized { get; }
}
