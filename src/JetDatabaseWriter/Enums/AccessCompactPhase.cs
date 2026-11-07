namespace JetDatabaseWriter.Enums;

/// <summary>
/// Identifies the current execution stage reported via <see cref="AccessCompactOptions.Progress"/>.
/// </summary>
public enum AccessCompactPhase
{
    /// <summary>Scanning physical pages, system catalog, table definitions, and active-row ownership.</summary>
    Planning = 0,

    /// <summary>Rebuilding and validating table data, long values, indexes, and allocation maps.</summary>
    Building = 1,

    /// <summary>Packing rebuilt table allocations into reclaimed interior prefix holes.</summary>
    PackingInteriorPages = 2,

    /// <summary>Repairing out-of-sync table-definition counters.</summary>
    RepairingCounters = 3,

    /// <summary>Re-encoding page-level encryption and validating the encrypted output.</summary>
    Encrypting = 4,

    /// <summary>Publishing the validated compacted database to its destination.</summary>
    Publishing = 5,

    /// <summary>Compaction or inspection has completed.</summary>
    Completed = 6,
}
