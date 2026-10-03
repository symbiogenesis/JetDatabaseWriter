namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// The dependent rows a cascading delete removes from one table, found
/// before the delete writes any row.
/// </summary>
/// <param name="TableName">The table's name.</param>
/// <param name="Table">The table's catalog entry and definition.</param>
/// <param name="Locations">The rows to delete.</param>
internal sealed record CascadeDelete(string TableName, ResolvedTable Table, List<RowLocation> Locations);
