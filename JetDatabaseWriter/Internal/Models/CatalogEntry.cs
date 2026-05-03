namespace JetDatabaseWriter.Internal.Models;

/// <summary>System-catalog entry: a user table's name and its <c>MSysObjects</c> TDef page pointer.</summary>
internal sealed record CatalogEntry(string Name, long TDefPage);
