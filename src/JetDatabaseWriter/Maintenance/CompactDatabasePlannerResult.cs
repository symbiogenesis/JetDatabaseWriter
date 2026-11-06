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
internal sealed record CompactDatabasePlannerResult(
    IReadOnlyList<CompactPinnedDatabaseBuildTable> Tables,
    CompactReclamationPlan Reclamation,
    int CatalogObjectCount,
    long ActiveRowCount,
    int RecoveredCatalogRowCount,
    long UnassignedRowCount,
    IReadOnlyList<AccessCompactDiagnostic> Diagnostics);
