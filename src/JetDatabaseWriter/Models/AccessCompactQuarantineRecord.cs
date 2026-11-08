namespace JetDatabaseWriter.Models;

using System.Collections.Generic;

/// <summary>
/// Represents a quarantined unrecoverable page or excluded unassigned row exported during partial-recovery compaction.
/// </summary>
/// <param name="Kind">The quarantine record kind (<c>quarantined-data-page</c> or <c>quarantined-unassigned-row</c>).</param>
/// <param name="PageNumber">The physical page number in the source database.</param>
/// <param name="OwnerTDefPage">The owning table-definition page number, when known.</param>
/// <param name="TableName">The resolved table name from <c>MSysObjects</c>, when known.</param>
/// <param name="RowIndex">The zero-based row slot index for row-level quarantine records, or <see langword="null"/> for page-level records.</param>
/// <param name="Reason">A human-readable description of why the page or row was quarantined.</param>
/// <param name="Sha256">The lowercase hexadecimal SHA-256 digest of the raw quarantined bytes.</param>
/// <param name="RawBytesBase64">The base64-encoded raw bytes of the quarantined page or row payload.</param>
/// <param name="DecodedFields">Best-effort decoded column values keyed by column name for row-level quarantine records, or <see langword="null"/> when unavailable.</param>
public sealed record AccessCompactQuarantineRecord(
    string Kind,
    long PageNumber,
    long? OwnerTDefPage,
    string? TableName,
    int? RowIndex,
    string Reason,
    string Sha256,
    string RawBytesBase64,
    IReadOnlyDictionary<string, object?>? DecodedFields = null);
