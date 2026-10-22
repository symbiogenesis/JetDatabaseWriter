namespace JetDatabaseWriter.Maintenance;

/// <summary>Completed private append-only construction followed by independently reopened component comparisons.</summary>
/// <param name="Plan">The prewrite independent source plan membership check.</param>
/// <param name="Validation">The complete independent private reopened comparison result.</param>
/// <param name="ReadBytes">Cumulative construction source, destination and sidecar read requests.</param>
/// <param name="WrittenBytes">Cumulative construction destination and sidecar write requests.</param>
/// <remarks>
/// Every source page remains pinned. This assembly can grow the file and does not satisfy compaction. Original index
/// observations, complete incoming ownership, native acceptance, encryption and durable replacement remain separate.
/// A result is returned only after every output is closed, independently reopened and compared, then closed again.
/// </remarks>
internal sealed record CompactPinnedDatabaseBuilderResult(
    CompactDatabasePlanResult Plan,
    CompactPinnedDatabaseValidatorResult Validation,
    long ReadBytes,
    long WrittenBytes)
{
    /// <summary>Gets a value indicating whether the complete database is accepted; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether original pinned storage may be reclaimed; always false.</summary>
    public bool ReclamationAuthorized { get; }

    /// <summary>Gets a value indicating whether the private copy may replace the original; always false.</summary>
    public bool PublicationAuthorized { get; }
}
