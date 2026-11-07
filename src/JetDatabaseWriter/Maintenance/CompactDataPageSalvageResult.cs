namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>
/// Pre-planning data-page salvage and quarantine inspection evidence.
/// </summary>
/// <param name="EffectiveSource">The overlay page source presenting salvaged or quarantined pages.</param>
/// <param name="TableNames">Discovered catalog table names keyed by owner TDEF page.</param>
/// <param name="Diagnostics">Diagnostics emitted during salvage and quarantine inspection.</param>
/// <param name="QuarantineRecords">Quarantine records emitted for unrecoverable data pages, corrupt row slots, and orphaned child rows.</param>
/// <param name="SalvagedRowsByOwner">Salvaged row counts keyed by owner TDEF page.</param>
/// <param name="QuarantinedPagesByOwner">Quarantined page counts keyed by owner TDEF page.</param>
/// <param name="ForceRegenerateOwners">Table owners whose indexes must be regenerated from surviving rows.</param>
/// <param name="TotalSalvagedRows">The total number of salvaged rows across all tables.</param>
/// <param name="QuarantinedRowsByOwner">Quarantined individual row counts keyed by owner TDEF page.</param>
/// <param name="TotalQuarantinedRows">The total number of quarantined individual row slots and orphaned child rows across all tables.</param>
/// <param name="InlinePromotedLongValuesByOwner">Counts of small external LVAL values promoted inline keyed by owner TDEF page.</param>
/// <param name="TotalInlinePromotedLongValues">The total number of small external LVAL values promoted inline across all tables.</param>
internal sealed record CompactDataPageSalvageResult(
    IPageSource EffectiveSource,
    IReadOnlyDictionary<long, string> TableNames,
    IReadOnlyList<AccessCompactDiagnostic> Diagnostics,
    IReadOnlyList<AccessCompactQuarantineRecord> QuarantineRecords,
    IReadOnlyDictionary<long, long> SalvagedRowsByOwner,
    IReadOnlyDictionary<long, int> QuarantinedPagesByOwner,
    HashSet<long> ForceRegenerateOwners,
    long TotalSalvagedRows,
    IReadOnlyDictionary<long, long>? QuarantinedRowsByOwner = null,
    long TotalQuarantinedRows = 0,
    IReadOnlyDictionary<long, long>? InlinePromotedLongValuesByOwner = null,
    long TotalInlinePromotedLongValues = 0);
