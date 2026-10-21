namespace JetDatabaseWriter.Catalog.Models;

/// <summary>System-catalog entry: a user table's name and its <c>MSysObjects</c> TDef page pointer.</summary>
/// <param name="Name">The name.</param>
/// <param name="TDefPage">The table-definition page number.</param>
internal sealed record CatalogEntry(string Name, long TDefPage);
