namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;

internal sealed record CatalogArtifactPlan(
    IReadOnlyList<CatalogTableArtifact> TableArtifacts,
    IReadOnlyList<CatalogObjectArtifact> CatalogObjects);
