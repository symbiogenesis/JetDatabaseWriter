namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;

/// <summary>
/// In-memory representation of a single enforced foreign-key relationship,
/// aggregated from one or more <c>MSysRelationships</c> rows.
/// </summary>
internal sealed record FkRelationship(
    string Name,
    string PrimaryTable,
    IReadOnlyList<string> PrimaryColumns,
    string ForeignTable,
    IReadOnlyList<string> ForeignColumns,
    bool CascadeUpdates,
    bool CascadeDeletes);
