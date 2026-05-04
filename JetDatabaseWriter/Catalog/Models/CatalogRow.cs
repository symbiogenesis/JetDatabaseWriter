namespace JetDatabaseWriter.Catalog.Models;

/// <summary>A single MSysObjects (or MSysIndexes / MSysQueries) catalog row decoded from a system-table data page.</summary>
internal sealed record CatalogRow(long PageNumber, int RowIndex, string Name, int ObjectType, long Flags, long TDefPage);
