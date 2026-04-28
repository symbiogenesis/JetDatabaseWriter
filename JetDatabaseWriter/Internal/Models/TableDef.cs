namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.IO;

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

    /// <summary>
    /// Resolves <paramref name="columnNames"/> to their <see cref="ColumnInfo.ColNum"/>
    /// values in the supplied order. Returns an empty array if any name is unknown.
    /// </summary>
    public int[] ResolveColNumsOrEmpty(string[] columnNames)
    {
        var result = new int[columnNames.Length];
        for (int i = 0; i < columnNames.Length; i++)
        {
            int idx = FindColumnIndex(columnNames[i]);
            if (idx < 0)
            {
                return [];
            }

            result[i] = Columns[idx].ColNum;
        }

        return result;
    }

    /// <summary>
    /// Stores <paramref name="value"/> into the slot of <paramref name="values"/>
    /// corresponding to <paramref name="columnName"/>. No-op when the column does
    /// not exist.
    /// </summary>
    public void SetValueByName(object[] values, string columnName, object value)
    {
        int index = FindColumnIndex(columnName);
        if (index >= 0)
        {
            values[index] = value;
        }
    }

    /// <summary>
    /// Allocates a row buffer sized to <see cref="Columns"/> with every slot
    /// initialised to <see cref="DBNull.Value"/>. Callers then overwrite the
    /// slots they want populated.
    /// </summary>
    public object[] CreateNullValueRow()
    {
        var values = new object[Columns.Count];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = DBNull.Value;
        }

        return values;
    }

    /// <summary>
    /// Locates the FK back-reference column on a hidden complex-column flat
    /// child table: the single <c>T_LONG</c> (type code <c>0x04</c>) column whose
    /// name starts with <c>"_"</c> per <c>complex-columns-format-notes.md</c> §2.4,
    /// falling back to the first <c>T_LONG</c> column when no underscore-prefixed
    /// candidate exists. Throws when no <c>T_LONG</c> column is present.
    /// </summary>
    public ColumnInfo FindFlatTableForeignKeyColumn()
    {
        const byte longType = 0x04; // T_LONG; mirrors AccessBase.T_LONG.
        return Columns.Find(c => c.Type == longType && c.Name.StartsWith('_'))
            ?? Columns.Find(c => c.Type == longType)
            ?? throw new InvalidDataException("Flat child table is missing a Long FK back-reference column.");
    }
}
