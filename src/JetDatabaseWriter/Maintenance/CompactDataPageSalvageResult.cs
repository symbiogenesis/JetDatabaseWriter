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
/// <param name="QuarantineRecords">Quarantine records emitted for unrecoverable data pages.</param>
/// <param name="SalvagedRowsByOwner">Salvaged row counts keyed by owner TDEF page.</param>
/// <param name="QuarantinedPagesByOwner">Quarantined page counts keyed by owner TDEF page.</param>
/// <param name="ForceRegenerateOwners">Table owners whose indexes must be regenerated from surviving rows.</param>
/// <param name="TotalSalvagedRows">The total number of salvaged rows across all tables.</param>
internal sealed record CompactDataPageSalvageResult(
    IPageSource EffectiveSource,
    IReadOnlyDictionary<long, string> TableNames,
    IReadOnlyList<AccessCompactDiagnostic> Diagnostics,
    IReadOnlyList<AccessCompactQuarantineRecord> QuarantineRecords,
    IReadOnlyDictionary<long, long> SalvagedRowsByOwner,
    IReadOnlyDictionary<long, int> QuarantinedPagesByOwner,
    HashSet<long> ForceRegenerateOwners,
    long TotalSalvagedRows);
