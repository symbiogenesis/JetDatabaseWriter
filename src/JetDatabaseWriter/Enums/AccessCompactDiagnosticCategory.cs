namespace JetDatabaseWriter.Enums;

/// <summary>
/// Categorizes a diagnostic observation or repair action recorded during <see cref="AccessDatabaseCompact"/> planning or execution.
/// </summary>
public enum AccessCompactDiagnosticCategory
{
    /// <summary>An unrecognized or uncategorized compact diagnostic.</summary>
    Other = 0,

    /// <summary>A physical page with an unrecognized page-type byte remained opaque and pinned.</summary>
    OpaquePage = 1,

    /// <summary>A catalog row or allocation hint required recovery or remained uncertain.</summary>
    CatalogUncertainty = 2,

    /// <summary>A table definition, data, or index page was marked in use for an unknown or unreferenced owner.</summary>
    UnassignedStorage = 3,

    /// <summary>A physical table row lay outside its table's owned usage map.</summary>
    UnassignedTableRow = 4,

    /// <summary>A damaged stored index B-tree or ownership map was regenerated from verified active table rows.</summary>
    RegeneratedIndex = 5,

    /// <summary>A data page with a damaged page-type header byte or owner pointer had its header repaired after verifying its directory and row fields.</summary>
    SalvagedDataPage = 6,

    /// <summary>An unrecoverable data page was isolated and quarantined so surviving table pages could be compacted.</summary>
    QuarantinedDataPage = 7,

    /// <summary>An out-of-sync table-definition row count or AutoNumber high-water counter was repaired.</summary>
    RepairedTableCounter = 8,

    /// <summary>An individual corrupt row slot or orphaned complex/foreign-key child row was isolated and quarantined while preserving surviving rows on its data page.</summary>
    QuarantinedRow = 9,
}
