namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;
using JetDatabaseWriter.Models;

/// <summary>
/// Holds the discovered table build plan, page-reclamation plan, and diagnostic summary produced by <see cref="CompactDatabasePlanner"/>.
/// </summary>
/// <param name="Tables">The discovered local tables in rebuild order (<c>MSysObjects</c> first, then ascending <c>TDef</c> page numbers).</param>
/// <param name="Reclamation">The proven source-page reclamation plan.</param>
/// <param name="CatalogObjectCount">The number of active catalog objects verified in <c>MSysObjects</c>.</param>
/// <param name="ActiveRowCount">The total number of live rows across all local tables.</param>
/// <param name="RecoveredCatalogRowCount">The number of catalog rows recovered from index bookmarks despite damaged owner headers.</param>
/// <param name="UnassignedRowCount">The number of unassigned physical table rows excluded from rebuilt tables and preserved pinned.</param>
/// <param name="Diagnostics">The bounded list of diagnostics and salvage observations recorded during planning.</param>
/// <param name="TableSummaries">The per-table summaries in deterministic rebuild order.</param>
/// <param name="CounterEvaluations">The per-table counter evaluations and repair patches.</param>
/// <param name="QuarantineRecords">The quarantined unrecoverable data pages, corrupt row slots, orphaned child rows, and excluded unassigned rows collected during planning.</param>
/// <param name="RegeneratedIndexCount">The total number of damaged physical real indexes regenerated from verified active rows.</param>
/// <param name="SalvagedRowCount">The total number of active rows salvaged from data pages with damaged headers, owners, or sibling row corruption.</param>
/// <param name="RepairedCounterTableCount">The total number of tables whose <c>TDef</c> counters were repaired or reset.</param>
/// <param name="QuarantinedRowCount">The total number of individual corrupt row slots or orphaned child rows quarantined across all tables.</param>
internal sealed record CompactDatabasePlannerResult(
    IReadOnlyList<CompactPinnedDatabaseBuildTable> Tables,
    CompactReclamationPlan Reclamation,
    int CatalogObjectCount,
    long ActiveRowCount,
    int RecoveredCatalogRowCount,
    long UnassignedRowCount,
    IReadOnlyList<AccessCompactDiagnostic> Diagnostics,
    IReadOnlyList<AccessCompactTableSummary>? TableSummaries = null,
    IReadOnlyList<CompactTableCounterEvaluation>? CounterEvaluations = null,
    IReadOnlyList<AccessCompactQuarantineRecord>? QuarantineRecords = null,
    int RegeneratedIndexCount = 0,
    long SalvagedRowCount = 0,
    int RepairedCounterTableCount = 0,
    long QuarantinedRowCount = 0);
