namespace JetDatabaseWriter.Pages.Models;

/// <summary>
/// One live row of a table decoded from the data page at <see cref="Location"/>.
/// Writer workflows that change rows they have read mutate exactly this
/// location, so a row that cannot be decoded is left alone rather than shifting
/// which location every later row is paired with.
/// </summary>
/// <param name="Location">Where the row was decoded from.</param>
/// <param name="Values">The row's values in table-column order, with <see cref="System.DBNull.Value"/> for nulls.</param>
internal readonly record struct LocatedRow(RowLocation Location, object[] Values);
