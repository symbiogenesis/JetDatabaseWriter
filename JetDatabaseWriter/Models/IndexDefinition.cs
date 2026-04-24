namespace JetDatabaseWriter;

using System.Collections.Generic;

/// <summary>
/// Defines a single-column, non-unique, ascending logical index that
/// <see cref="IAccessWriter.CreateTableAsync(string, System.Collections.Generic.IReadOnlyList{ColumnDefinition}, System.Collections.Generic.IReadOnlyList{IndexDefinition}, System.Threading.CancellationToken)"/>
/// emits into the new table's TDEF page chain.
/// </summary>
/// <remarks>
/// <para>
/// Phases W1–W3 of the index-writer roadmap (see
/// <c>docs/design/index-and-relationship-format-notes.md</c>) ship the TDEF
/// schema metadata (real-index physical descriptor + logical-index entry +
/// logical-index name) <em>and</em> a single empty B-tree leaf page
/// (<c>page_type = 0x04</c>) per index, with the leaf's page number patched
/// into the real-index <c>first_dp</c> field. The leaf is empty at table
/// creation time and is <em>not</em> maintained by subsequent
/// <c>InsertRowAsync</c> / <c>InsertRowsAsync</c> / <c>UpdateRowsAsync</c> /
/// <c>DeleteRowsAsync</c> / <c>AddColumnAsync</c> / <c>DropColumnAsync</c> /
/// <c>RenameColumnAsync</c> calls (those W5 hooks are not yet implemented).
/// As a result the index goes stale as soon as the table mutates and Microsoft
/// Access will rebuild it on the next Compact &amp; Repair pass.
/// </para>
/// <para>
/// Constraints (enforced at <c>CreateTableAsync</c> time):
/// </para>
/// <list type="bullet">
///   <item><description>Single column only — multi-column indexes are not supported in this phase.</description></item>
///   <item><description>Non-unique only (<see cref="IndexMetadata.IsUnique"/> always reads back <c>false</c>).</description></item>
///   <item><description>Ascending only.</description></item>
///   <item><description>No primary-key, foreign-key, or relationship semantics.</description></item>
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

    /// <summary>Gets the logical-index name.</summary>
    public string Name { get; }

    /// <summary>
    /// Gets the column names that make up the index key, in key order. In phases
    /// W1–W3 this list always contains exactly one entry.
    /// </summary>
    public IReadOnlyList<string> Columns { get; }
}
