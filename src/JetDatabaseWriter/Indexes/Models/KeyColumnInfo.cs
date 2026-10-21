namespace JetDatabaseWriter.Indexes.Models;

using JetDatabaseWriter.Schema.Models;

/// <summary>
/// One real-idx key column resolved against the table's
/// <see cref="ColumnInfo"/> list: the column descriptor, the row-snapshot
/// index (which differs from <c>ColNum</c> when columns have been
/// deleted), and the ascending/descending direction copied from the
/// originating <c>col_map</c> slot.
/// </summary>
/// <param name="Col">The column descriptor.</param>
/// <param name="SnapIdx">The snap index.</param>
/// <param name="Ascending">The ascending.</param>
internal readonly record struct KeyColumnInfo(ColumnInfo Col, int SnapIdx, bool Ascending);
