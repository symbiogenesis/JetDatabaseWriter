namespace JetDatabaseWriter.Models;

using JetDatabaseWriter.Enums;

/// <summary>
/// Represents a bounded diagnostic or salvage observation recorded during Compact &amp; Repair.
/// </summary>
/// <param name="Kind">The machine-readable category of the diagnostic (for example, <c>inventory-diagnostic</c>, <c>catalog-recovered-row</c>, <c>unassigned-table-row</c>, <c>unresolved-reference</c>, <c>unassigned-owner-page</c>, <c>regenerated-index</c>, <c>salvaged-data-page-header</c>, <c>quarantined-data-page</c>, or <c>repaired-table-counter</c>).</param>
/// <param name="PageNumber">The physical source page number associated with the observation, if applicable.</param>
/// <param name="OwnerTDefPage">The table-definition owner page associated with the observation, if applicable.</param>
/// <param name="RowIndex">The physical row slot index associated with the observation, if applicable.</param>
/// <param name="Detail">A human-readable description of the diagnostic or salvage decision.</param>
/// <param name="Category">The structured diagnostic category.</param>
/// <param name="TableName">The resolved table name from <c>MSysObjects</c>, if applicable.</param>
public sealed record AccessCompactDiagnostic(
    string Kind,
    long? PageNumber,
    long? OwnerTDefPage,
    int? RowIndex,
    string Detail,
    AccessCompactDiagnosticCategory Category = AccessCompactDiagnosticCategory.Other,
    string? TableName = null)
{
    /// <summary>Gets the structured diagnostic category.</summary>
    public AccessCompactDiagnosticCategory Category { get; init; } = Category != AccessCompactDiagnosticCategory.Other
        ? Category
        : MapKindToCategory(Kind);

    /// <summary>Gets a value indicating whether this diagnostic prevents complete reclamation and in-place replacement.</summary>
    internal bool IsBlocking => this.Category != AccessCompactDiagnosticCategory.RepairedTableCounter;

    private static AccessCompactDiagnosticCategory MapKindToCategory(string? kind)
        => kind switch
        {
            "opaque-page" => AccessCompactDiagnosticCategory.OpaquePage,
            "inventory-diagnostic" or "catalog-recovered-row" => AccessCompactDiagnosticCategory.CatalogUncertainty,
            "unresolved-reference" or "unassigned-tdef-page" or "unassigned-owner-page" => AccessCompactDiagnosticCategory.UnassignedStorage,
            "unassigned-table-row" => AccessCompactDiagnosticCategory.UnassignedTableRow,
            "regenerated-index" => AccessCompactDiagnosticCategory.RegeneratedIndex,
            "salvaged-data-page-header" => AccessCompactDiagnosticCategory.SalvagedDataPage,
            "quarantined-data-page" => AccessCompactDiagnosticCategory.QuarantinedDataPage,
            "repaired-table-counter" => AccessCompactDiagnosticCategory.RepairedTableCounter,
            _ => AccessCompactDiagnosticCategory.Other,
        };
}
