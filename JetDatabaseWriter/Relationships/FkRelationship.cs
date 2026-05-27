namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;

/// <summary>
/// In-memory representation of a single enforced foreign-key relationship,
/// aggregated from one or more <c>MSysRelationships</c> rows.
/// </summary>
/// <param name="Name">The name.</param>
/// <param name="PrimaryTable">The primary table.</param>
/// <param name="PrimaryColumns">The primary columns.</param>
/// <param name="ForeignTable">The foreign table.</param>
/// <param name="ForeignColumns">The foreign columns.</param>
/// <param name="CascadeUpdates">The cascade updates.</param>
/// <param name="CascadeDeletes">The cascade deletes.</param>
internal sealed record FkRelationship(
    string Name,
    string PrimaryTable,
    IReadOnlyList<string> PrimaryColumns,
    string ForeignTable,
    IReadOnlyList<string> ForeignColumns,
    bool CascadeUpdates,
    bool CascadeDeletes);
