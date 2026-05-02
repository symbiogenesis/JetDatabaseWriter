namespace JetDatabaseWriter.Internal.Models;

using System;
using System.Collections.Generic;
using System.IO;
using static JetDatabaseWriter.Constants.ColumnTypes;

internal sealed class TableDef
{
    public List<ColumnInfo> Columns { get; set; } = [];

    public long RowCount { get; set; } // num_rows from TDEF page offset 16

    public bool HasDeletedColumns { get; set; } // true if ColNum sequence has gaps

    /// <summary>
    /// Gets the per-column CLR projection types, populated by
    /// <see cref="InitializeColumnMetadata"/>. Mirrors the result of
    /// <c>JetTypeInfo.ResolveClrType(col)</c> for each column. The
    /// typed-row cracker reuses this array to avoid resolving the CLR
    /// type per-row.
    /// </summary>
    public Type[] ClrTypes { get; private set; } = [];

    /// <summary>
    /// Gets a value indicating whether at least one column lives in the row's
    /// variable-length area (any column where <see cref="ColumnInfo.IsFixed"/>
    /// is <see langword="false"/>). Cached so the row layout parser can skip
    /// the var-area read when no var columns exist. See
    /// <see cref="InitializeColumnMetadata"/>.
    /// </summary>
    public bool HasVarColumns { get; private set; }

    /// <summary>
    /// Gets a value indicating whether at least one column is a complex/attachment
    /// column (<c>T_COMPLEX</c> or <c>T_ATTACHMENT</c>). Cached so the typed
    /// reader can skip its complex-data prefetch when the table has none.
    /// See <see cref="InitializeColumnMetadata"/>.
    /// </summary>
    public bool HasComplexColumns { get; private set; }

    /// <summary>
    /// Gets a value indicating whether at least one column is flagged as a
    /// Hyperlink (a <c>T_TEXT</c>/<c>T_MEMO</c> column whose Jet column flags
    /// have <c>HYPERLINK_FLAG_MASK = 0x80</c> set). Cached so the typed
    /// reader can skip its hyperlink-wrap pass when the table has none.
    /// </summary>
    public bool HasHyperlinkColumns { get; private set; }

    /// <summary>
    /// Populates the per-table metadata caches (<see cref="ClrTypes"/>,
    /// <see cref="HasVarColumns"/>, <see cref="HasComplexColumns"/>). Must be
    /// invoked after <see cref="Columns"/> is finalised; called once by the
    /// TableDef loader in <c>AccessBase.ReadTableDefAsync</c>.
    /// </summary>
    public void InitializeColumnMetadata()
    {
        var clrTypes = new Type[Columns.Count];
        bool hasVar = false;
        bool hasComplex = false;
        bool hasHyperlink = false;
        for (int i = 0; i < Columns.Count; i++)
        {
            ColumnInfo c = Columns[i];
            Type clr = JetTypeInfo.ResolveClrType(c);
            clrTypes[i] = clr;
            if (!c.IsFixed)
            {
                hasVar = true;
            }

            if (c.Type == T_COMPLEX || c.Type == T_ATTACHMENT)
            {
                hasComplex = true;
            }

            if (clr == typeof(JetDatabaseWriter.Models.Hyperlink))
            {
                hasHyperlink = true;
            }
        }

        ClrTypes = clrTypes;
        HasVarColumns = hasVar;
        HasComplexColumns = hasComplex;
        HasHyperlinkColumns = hasHyperlink;
    }

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
