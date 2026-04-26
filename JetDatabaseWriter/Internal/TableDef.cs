namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;

internal sealed class TableDef
{
    public List<ColumnInfo> Columns { get; set; } = [];

    public long RowCount { get; set; } // num_rows from TDEF page offset 16

    public bool HasDeletedColumns { get; set; } // true if ColNum sequence has gaps

    /// <summary>
    /// Returns the zero-based index of the column whose name matches
    /// <paramref name="columnName"/> case-insensitively, or -1 when no
    /// such column exists.
    /// </summary>
    public int FindColumnIndex(string columnName)
    {
        return Columns.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Returns the column whose name matches <paramref name="columnName"/>
    /// case-insensitively, or <see langword="null"/> when no such column exists.
    /// </summary>
    public ColumnInfo? FindColumn(string columnName)
    {
        return Columns.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
    }
}
