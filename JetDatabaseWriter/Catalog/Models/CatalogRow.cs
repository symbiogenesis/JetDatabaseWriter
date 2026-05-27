namespace JetDatabaseWriter.Catalog.Models;

/// <summary>A single MSysObjects (or MSysIndexes / MSysQueries) catalog row decoded from a system-table data page.</summary>
/// <param name="PageNumber">The page number.</param>
/// <param name="RowIndex">The row index.</param>
/// <param name="Name">The name.</param>
/// <param name="ObjectType">The object type.</param>
/// <param name="Flags">The flags.</param>
/// <param name="TDefPage">The table-definition page number.</param>
/// <param name="Id">The identifier.</param>
/// <param name="ParentId">The parent id.</param>
internal sealed record CatalogRow(long PageNumber, int RowIndex, string Name, int ObjectType, long Flags, long TDefPage, long Id, long ParentId);
