namespace JetDatabaseWriter.Catalog.Models;

internal sealed record UserTableCatalogDeletionArtifact(
    string TableName,
    long? TDefPage = null,
    bool IncludeSystemTables = true,
    bool ThrowIfNotFound = false,
    string? Operation = null,
    string? MissingMessage = null);
