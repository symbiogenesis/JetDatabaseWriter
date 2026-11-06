namespace JetDatabaseWriter.Models;

/// <summary>
/// Represents a bounded diagnostic or salvage observation recorded during Compact &amp; Repair.
/// </summary>
/// <param name="Kind">The machine-readable category of the diagnostic (for example, <c>inventory-diagnostic</c>, <c>catalog-recovered-row</c>, <c>unassigned-table-row</c>, <c>unresolved-reference</c>, or <c>unassigned-owner-page</c>).</param>
/// <param name="PageNumber">The physical source page number associated with the observation, if applicable.</param>
/// <param name="OwnerTDefPage">The table-definition owner page associated with the observation, if applicable.</param>
/// <param name="RowIndex">The physical row slot index associated with the observation, if applicable.</param>
/// <param name="Detail">A human-readable description of the diagnostic or salvage decision.</param>
public sealed record AccessCompactDiagnostic(
    string Kind,
    long? PageNumber,
    long? OwnerTDefPage,
    int? RowIndex,
    string Detail);
