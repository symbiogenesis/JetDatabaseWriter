namespace JetDatabaseWriter.Catalog.Models;

using System;
using System.Collections.Generic;
using System.IO;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;

internal sealed class TableDef
{
    public List<ColumnInfo> Columns { get; set; } = [];

    /// <summary>
    /// Gets or sets num_rows from TDEF page offset 16.
    /// </summary>
    public long RowCount { get; set; }

    /// <summary>
    /// <c>Gets or sets a value indicating whether true</c> if ColNum sequence has gaps.
    /// </summary>
    public bool HasDeletedColumns { get; set; }

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
    /// column (<c>Complex</c> or <c>Attachment</c>). Cached so the typed
    /// reader can skip its complex-data prefetch when the table has none.
    /// See <see cref="InitializeColumnMetadata"/>.
    /// </summary>
    public bool HasComplexColumns { get; private set; }

    /// <summary>
    /// Gets a value indicating whether at least one column is flagged as a
    /// Hyperlink (a <c>Text</c>/<c>Memo</c> column whose Jet column flags
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
        var clrTypes = new Type[this.Columns.Count];
        bool hasVar = false;
        bool hasComplex = false;
        bool hasHyperlink = false;
        for (int i = 0; i < this.Columns.Count; i++)
        {
            ColumnInfo c = this.Columns[i];
            Type clr = JetTypeInfo.ResolveClrType(c);
            clrTypes[i] = clr;
            if (!c.IsFixed)
            {
                hasVar = true;
            }

            if (c.Type is ComplexType or AttachmentType)
            {
                hasComplex = true;
            }

            if (clr == typeof(JetDatabaseWriter.Models.Hyperlink))
            {
                hasHyperlink = true;
            }
        }

        this.ClrTypes = clrTypes;
        this.HasVarColumns = hasVar;
        this.HasComplexColumns = hasComplex;
        this.HasHyperlinkColumns = hasHyperlink;
    }

    /// <summary>
    /// Returns the zero-based index of the column whose name matches
    /// <paramref name="columnName"/> case-insensitively, or -1 when no
    /// such column exists.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public int FindColumnIndex(string columnName) => this.Columns.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Returns the column whose name matches <paramref name="columnName"/>
    /// case-insensitively, or <see langword="null"/> when no such column exists.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public ColumnInfo? FindColumn(string columnName) => this.Columns.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Resolves <paramref name="columnNames"/> to their <see cref="ColumnInfo.ColNum"/>
    /// values in the supplied order. Returns an empty array if any name is unknown.
    /// </summary>
    /// <param name="columnNames">The column names.</param>
    public int[] ResolveColNumsOrEmpty(string[] columnNames)
    {
        int[] result = new int[columnNames.Length];
        for (int i = 0; i < columnNames.Length; i++)
        {
            int idx = this.FindColumnIndex(columnNames[i]);
            if (idx < 0)
            {
                return [];
            }

            result[i] = this.Columns[idx].ColNum;
        }

        return result;
    }

    /// <summary>
    /// Stores <paramref name="value"/> into the slot of <paramref name="values"/>
    /// corresponding to <paramref name="columnName"/>. No-op when the column does
    /// not exist.
    /// </summary>
    /// <param name="values">The values.</param>
    /// <param name="columnName">The column name.</param>
    /// <param name="value">The value.</param>
    public void SetValueByName(object[] values, string columnName, object value)
    {
        int index = this.FindColumnIndex(columnName);
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
        object[] values = new object[this.Columns.Count];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = DBNull.Value;
        }

        return values;
    }

    /// <summary>
    /// Locates the FK back-reference column on a hidden complex-column flat
    /// child table: the single <c>LongInteger</c> (type code <c>0x04</c>) column whose
    /// name starts with <c>"_"</c> per <see href="complex-columns-format-notes.md" /> §2.4,
    /// falling back to the first <c>LongInteger</c> column when no underscore-prefixed
    /// candidate exists. Throws when no <c>LongInteger</c> column is present.
    /// </summary>
    /// <exception cref="InvalidDataException">Thrown when the flat child table has no Long FK back-reference column.</exception>
    public ColumnInfo FindFlatTableForeignKeyColumn() => this.Columns.Find(c => c.Type == LongIntegerType && c.Name.StartsWith('_'))
            ?? this.Columns.Find(c => c.Type == LongIntegerType)
            ?? throw new InvalidDataException("Flat child table is missing a Long FK back-reference column.");
}
