namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;

internal sealed record UserTableCatalogDeletionResult(
    int DeletedCount,
    IReadOnlyList<long> TDefPages,
    long? FirstTDefPage,
    uint FirstCatalogFlags);
