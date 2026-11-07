namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;

/// <summary>Completed private append-only construction followed by independently reopened component comparisons.</summary>
/// <param name="Plan">The prewrite independent source plan membership check.</param>
/// <param name="Validation">The complete independent private reopened comparison result.</param>
/// <param name="ReadBytes">Cumulative construction source, destination and sidecar read requests.</param>
/// <param name="WrittenBytes">Cumulative construction destination and sidecar write requests.</param>
/// <remarks>
/// A result is returned only after every output is closed, independently reopened and compared, then closed again.
/// When constructed without a <see cref="CompactReclamationPlan"/>, whole-database, reclamation, and publication flags remain false.
/// </remarks>
internal sealed record CompactPinnedDatabaseBuilderResult(
    CompactDatabasePlanResult Plan,
    CompactPinnedDatabaseValidatorResult Validation,
    long ReadBytes,
    long WrittenBytes)
{
    /// <summary>Gets the independently compared per-table allocations in build order.</summary>
    public IReadOnlyList<CompactPinnedDatabaseTable> Tables { get; init; } = [];

    /// <summary>Gets a value indicating whether the complete database is accepted.</summary>
    public bool DatabaseValidated { get; init; }

    /// <summary>Gets a value indicating whether original pinned storage may be reclaimed.</summary>
    public bool ReclamationAuthorized { get; init; }

    /// <summary>Gets a value indicating whether the private copy may replace the original.</summary>
    public bool PublicationAuthorized { get; init; }
}
