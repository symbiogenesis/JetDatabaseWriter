namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Pages.Models;

/// <summary>A validated rename that preserves the relationship object's identity and opaque metadata.</summary>
/// <param name="Definition">The catalog definition.</param>
/// <param name="Original">The original object row.</param>
/// <param name="Replacement">The renamed row values.</param>
/// <param name="FixedTextBytes">Unchanged fixed text to preserve byte-for-byte.</param>
internal sealed record RelationshipCatalogRenameArtifact(
    TableDef Definition,
    LocatedRow Original,
    object[] Replacement,
    IReadOnlyDictionary<int, byte[]>? FixedTextBytes);
