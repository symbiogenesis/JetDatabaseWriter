namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;

/// <summary>
/// A relationship whose referenced key an update changes on some rows,
/// resolved before the update rewrites any dependent row.
/// </summary>
/// <param name="Relationship">The relationship.</param>
/// <param name="ChildTable">The relationship's foreign table.</param>
/// <param name="ForeignColumnIndexes">The ordinals of the foreign-key columns in <paramref name="ChildTable"/>.</param>
/// <param name="Changes">
/// Each changed key's old and new values in the relationship's primary
/// columns, by the normalized old key.
/// </param>
internal sealed record ReferencedKeyChange(
    FkRelationship Relationship,
    ResolvedTable ChildTable,
    int[] ForeignColumnIndexes,
    Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)> Changes);
