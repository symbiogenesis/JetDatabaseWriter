namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;

internal sealed record CatalogArtifactPlan(
    IReadOnlyList<CatalogTableArtifact> TableArtifacts,
    IReadOnlyList<CatalogObjectArtifact> CatalogObjects)
{
    public IReadOnlyList<UserTableCatalogReplacementArtifact> CatalogReplacements { get; init; } = [];

    public IReadOnlyList<UserTableCatalogDeletionArtifact> CatalogDeletions { get; init; } = [];
}
