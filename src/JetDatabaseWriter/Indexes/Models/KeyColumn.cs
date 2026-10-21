namespace JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Decoded view of a single populated <c>col_map</c> entry: the column
/// number and ascending/descending direction.
/// </summary>
/// <param name="ColNum">The col number of.</param>
/// <param name="Ascending">The ascending.</param>
internal readonly record struct KeyColumn(int ColNum, bool Ascending);
