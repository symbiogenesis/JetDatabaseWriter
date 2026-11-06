namespace JetDatabaseWriter.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;

/// <summary>
/// Summarizes the results, space reclamation, and independent validation of a Compact &amp; Repair operation.
/// </summary>
public sealed record AccessCompactReport
{
    /// <summary>Gets the native database format of the compacted database.</summary>
    public DatabaseFormat Format { get; init; }

    /// <summary>Gets the physical page size in bytes.</summary>
    public int PageSize { get; init; }

    /// <summary>Gets the total physical page count of the source database before compaction.</summary>
    public long SourcePages { get; init; }

    /// <summary>Gets the total physical page count of the destination database after compaction.</summary>
    public long DestinationPages { get; init; }

    /// <summary>Gets the number of source prefix pages retained in the destination (<c>0 .. RetainedPrefixPages - 1</c>).</summary>
    public long RetainedPrefixPages { get; init; }

    /// <summary>Gets the number of source pages proven reclaimable and either truncated or scrubbed free.</summary>
    public int ReclaimedPages { get; init; }

    /// <summary>Gets the number of interior prefix pages scrubbed to canonical reusable free pages and marked free in the global usage map.</summary>
    public int ScrubbedFreePages { get; init; }

    /// <summary>Gets the number of trailing source pages truncated from the retained prefix before appending rebuilt tables.</summary>
    public int TruncatedSourceTailPages { get; init; }

    /// <summary>Gets the number of local tables (including <c>MSysObjects</c>, system tables, complex flat tables, and user tables) rebuilt and independently validated.</summary>
    public int RebuiltTableCount { get; init; }

    /// <summary>Gets the number of active catalog objects verified in <c>MSysObjects</c>.</summary>
    public int CatalogObjectCount { get; init; }

    /// <summary>Gets the total number of live rows rebuilt across all local tables.</summary>
    public long ActiveRowCount { get; init; }

    /// <summary>Gets the number of catalog rows recovered from index bookmarks despite damaged physical owner headers.</summary>
    public int RecoveredCatalogRowCount { get; init; }

    /// <summary>Gets the number of unassigned physical table rows excluded from rebuilt tables and preserved pinned.</summary>
    public long UnassignedRowCount { get; init; }

    /// <summary>Gets the cumulative bytes read during construction and independent reopened validation.</summary>
    public long ReadBytes { get; init; }

    /// <summary>Gets the cumulative bytes written during construction.</summary>
    public long WrittenBytes { get; init; }

    /// <summary>Gets a value indicating whether the source database was encrypted and re-encoded into the destination.</summary>
    public bool IsEncrypted { get; init; }

    /// <summary>Gets a value indicating whether discovery, ownership, and reconstruction completed without any uncertainty or salvage diagnostics.</summary>
    public bool IsComplete { get; init; }

    /// <summary>Gets a value indicating whether the closed destination passed independent reopened validation.</summary>
    public bool DatabaseValidated { get; init; }

    /// <summary>Gets a value indicating whether source page reclamation was authorized and validated.</summary>
    public bool ReclamationAuthorized { get; init; }

    /// <summary>Gets a value indicating whether the compacted database is eligible to replace the original database in place.</summary>
    public bool ReplacementAuthorized { get; init; }

    /// <summary>Gets the bounded list of diagnostics and salvage observations recorded during planning and construction.</summary>
    public IReadOnlyList<AccessCompactDiagnostic> Diagnostics { get; init; } = [];
}
