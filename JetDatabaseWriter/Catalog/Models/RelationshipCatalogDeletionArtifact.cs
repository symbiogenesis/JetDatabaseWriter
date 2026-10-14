namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Pages.Models;

/// <summary>A validated relationship identity and its full catalog/security rows for index maintenance.</summary>
/// <param name="Identity">The exclusive relationship identity.</param>
/// <param name="Objects">The object catalog definition.</param>
/// <param name="Original">The original object row.</param>
/// <param name="AcesPage">The permissions table definition page.</param>
/// <param name="Aces">The permissions table definition, if present.</param>
/// <param name="AceRows">The permissions belonging only to the relationship identity.</param>
internal sealed record RelationshipCatalogDeletionArtifact(
    CatalogRow Identity,
    TableDef Objects,
    LocatedRow Original,
    long AcesPage,
    TableDef? Aces,
    IReadOnlyList<LocatedRow> AceRows);
