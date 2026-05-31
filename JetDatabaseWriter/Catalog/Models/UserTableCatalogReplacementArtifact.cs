namespace JetDatabaseWriter.Catalog.Models;

internal sealed record UserTableCatalogReplacementArtifact(
    string ExistingName,
    string ReplacementName,
    long? TDefPage = null,
    byte[]? LvProp = null,
    bool IncludeSystemTables = true,
    string? Operation = null,
    string? MissingMessage = null);
