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
/// The writer emits the TDEF schema metadata (real-index physical
/// descriptor + logical-index entry + logical-index name) and a single empty
/// B-tree leaf page (<c>page_type = 0x04</c>) per index at table-creation
/// time, then maintains the B-tree on every subsequent row mutation.
/// Primary keys are emitted via <see cref="IsPrimaryKey"/>; the multi-column
/// constructor exists primarily to support multi-column primary keys. See
/// <c>docs/design/index-and-relationship-format-notes.md</c> for the on-disk
/// layout.
/// </para>
/// <para>
/// Constraints (enforced at <c>CreateTableAsync</c> time):
/// </para>
/// <list type="bullet">
///   <item><description>At most ten columns per index (the JET <c>col_map</c> width).</description></item>
///   <item><description>At most one PK per table.</description></item>
///   <item><description>PK key columns are forced non-nullable on the emitted TDEF.</description></item>
///   <item><description>No foreign-key or relationship semantics — use <see cref="RelationshipDefinition"/>.</description></item>
///   <item><description>Every entry in <see cref="DescendingColumns"/> must also appear in <see cref="Columns"/> (case-insensitive).</description></item>
/// </list>
/// <para>
/// <see cref="IsUnique"/> emits the real-idx <c>flags</c> bit <c>0x01</c>
/// (index maintenance also throws on duplicate keys after the bulk B-tree
/// rebuild), <see cref="DescendingColumns"/> emits <c>col_order = 0x02</c>
/// in the matching col_map slots, and multi-column non-PK indexes are
/// supported and maintained live.
/// </para>
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
        Columns = [columnName];
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexDefinition"/> class
    /// referencing one or more columns. See the type-level remarks for the
    /// emitted layout and the live B-tree maintenance contract.
    /// </summary>
    /// <param name="name">The logical-index name.</param>
    /// <param name="columns">The columns that make up the index key, in key order. Must contain at least one entry.</param>
    public IndexDefinition(string name, IReadOnlyList<string> columns)
    {
        Guard.NotNull(columns, nameof(columns));

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
    /// May contain up to ten entries (the JET <c>col_map</c> width).
    /// </summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>
    /// Gets a value indicating whether this index is the primary key of the
    /// table. PK indexes are emitted with <c>index_type = 0x01</c> in the
    /// TDEF logical-index entry, are implicitly unique, and force their key
    /// columns to be non-nullable on the emitted TDEF.
    /// </summary>
    public bool IsPrimaryKey { get; init; }

    /// <summary>
    /// Gets a value indicating whether this index enforces uniqueness. When
    /// <see langword="true"/> and <see cref="IsPrimaryKey"/> is
    /// <see langword="false"/>, the writer emits the real-idx <c>flags</c>
    /// bit <c>0x01</c> on the matching physical descriptor (§3.1) and the index maintenance
    /// bulk-rebuild path throws <see cref="System.InvalidOperationException"/>
    /// when two live rows produce the same encoded key. Implicitly true for
    /// primary-key indexes — PKs signal uniqueness via the logical-idx
    /// <c>index_type = 0x01</c> discriminator and Access leaves the
    /// real-idx <c>flags</c> byte at <c>0x00</c> (§3.1 probe note); setting
    /// this property in addition to <see cref="IsPrimaryKey"/> has no extra
    /// effect.
    /// </summary>
    public bool IsUnique { get; init; }

    /// <summary>
    /// Gets the subset of <see cref="Columns"/> that should be sorted
    /// descending in the index. Each entry must match a name in
    /// <see cref="Columns"/> case-insensitively. Columns not listed here are
    /// emitted with <c>col_order = 0x01</c> (ascending); listed columns are
    /// emitted with <c>col_order = 0x02</c>. Defaults to an empty list.
    /// </summary>
    /// <remarks>
    /// The descending byte value (<c>0x02</c>) follows Jackcess
    /// (<c>com.healthmarketscience.jackcess.impl.IndexImpl</c>); the
    /// in-repo format-probe corpus contains no descending fixtures, so the
    /// value has not been independently re-verified against an
    /// Access-authored database. Defer production use until that round-trip
    /// has been performed by hand (see design doc §8).
    /// </remarks>
    public IReadOnlyList<string> DescendingColumns { get; init; } = [];
}
