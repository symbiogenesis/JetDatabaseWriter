namespace JetDatabaseWriter.Catalog.Models;

internal sealed record CatalogObjectArtifact(
    int ObjectId,
    int ParentId,
    string ObjectName,
    short ObjectType,
    uint CatalogFlags,
    byte[]? Owner = null,
    byte[]? LvProp = null);
