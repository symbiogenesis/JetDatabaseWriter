namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// The dependent rows a cascading key update rewrites in one table, each with
/// the values that replace it, built before the update writes any row.
/// </summary>
/// <param name="TableName">The table's name.</param>
/// <param name="Table">The table's catalog entry and definition.</param>
/// <param name="Rows">
/// Each row to replace, its original values and its new values in table-column
/// order, carrying every key change the update cascades to that row. A row
/// appears once, however many relationships reach it.
/// </param>
internal sealed record CascadeUpdate(string TableName, ResolvedTable Table, List<(RowLocation Location, object[] OldRow, object[] NewRow)> Rows)
{
    /// <summary>Gets unchanged fixed Text slots retained by each planned row rewrite.</summary>
    internal Dictionary<RowLocation, IReadOnlyDictionary<int, byte[]>?> FixedTextBytes { get; } = [];
}
