namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;

/// <summary>
/// Defines a logical index that
/// <see cref="IAccessWriter.CreateTableAsync(string, System.Collections.Generic.IReadOnlyList{ColumnDefinition}, System.Collections.Generic.IReadOnlyList{IndexDefinition}, System.Threading.CancellationToken)"/>
/// emits into the new table's TDEF page chain.
/// </summary>
/// <remarks>
/// <para>
/// Phases W1–W5 of the index-writer roadmap (see
/// <c>docs/design/index-and-relationship-format-notes.md</c>) ship the TDEF
/// schema metadata (real-index physical descriptor + logical-index entry +
/// logical-index name), a single empty B-tree leaf page
/// (<c>page_type = 0x04</c>) per index at table-creation time, and bulk
/// B-tree rebuild on every subsequent row mutation. Phase W8 adds
/// primary-key emission via <see cref="IsPrimaryKey"/>; the multi-column
/// constructor exists primarily to support multi-column primary keys.
/// </para>
/// <para>
/// Constraints (enforced at <c>CreateTableAsync</c> time):
/// </para>
/// <list type="bullet">
///   <item><description>Non-PK indexes must be single-column.</description></item>
///   <item><description>Non-unique unless <see cref="IsPrimaryKey"/> is set (PK is implicitly unique).</description></item>
///   <item><description>Ascending only.</description></item>
///   <item><description>At most one PK per table.</description></item>
///   <item><description>PK key columns are forced non-nullable on the emitted TDEF.</description></item>
///   <item><description>No foreign-key or relationship semantics (W9 territory).</description></item>
///   <item><description>Jet4 / ACE only — Jet3 (<c>.mdb</c> Access 97) databases reject any non-empty index list with <see cref="System.NotSupportedException"/>.</description></item>
/// </list>
/// </remarks>
public sealed record IndexDefinition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IndexDefinition"/> class
    /// referencing a single column.
    /// </summary>
    /// <param name="name">The logical-index name (1-64 characters, matching Access naming rules).</param>
    /// <param name="columnName">The name of the column this index covers. Must match a column on the same table, case-insensitively.</param>
    public IndexDefinition(string name, string columnName)
    {
        Name = name;
        Columns = new[] { columnName };
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexDefinition"/> class
    /// referencing one or more columns. The multi-column form is supported
    /// only when <see cref="IsPrimaryKey"/> is also set; non-PK multi-column
    /// indexes are rejected by <c>CreateTableAsync</c>.
    /// </summary>
    /// <param name="name">The logical-index name.</param>
    /// <param name="columns">The columns that make up the index key, in key order. Must contain at least one entry.</param>
    public IndexDefinition(string name, IReadOnlyList<string> columns)
    {
#if NET6_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(columns);
#else
        if (columns is null)
        {
            throw new ArgumentNullException(nameof(columns));
        }
#endif

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required.", nameof(columns));
        }

        var copy = new string[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            copy[i] = columns[i];
        }

        Name = name;
        Columns = copy;
    }

    /// <summary>Gets the logical-index name.</summary>
    public string Name { get; }

    /// <summary>
    /// Gets the column names that make up the index key, in key order.
    /// Non-PK indexes always contain exactly one entry; PK indexes may
    /// contain up to ten (the JET <c>col_map</c> width).
    /// </summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>
    /// Gets a value indicating whether this index is the primary key of the
    /// table. PK indexes are emitted with <c>index_type = 0x01</c> in the
    /// TDEF logical-index entry, are implicitly unique, and force their key
    /// columns to be non-nullable on the emitted TDEF.
    /// </summary>
    public bool IsPrimaryKey { get; init; }
}
