namespace JetDatabaseWriter;

using System.Collections.Generic;

/// <summary>
/// Defines a single-column, non-unique, ascending logical index that
/// <see cref="IAccessWriter.CreateTableAsync(string, System.Collections.Generic.IReadOnlyList{ColumnDefinition}, System.Collections.Generic.IReadOnlyList{IndexDefinition}, System.Threading.CancellationToken)"/>
/// emits into the new table's TDEF page chain.
/// </summary>
/// <remarks>
/// <para>
/// Phase W1 of the index-writer roadmap (see
/// <c>docs/design/index-and-relationship-format-notes.md</c>): only the TDEF
/// schema metadata (real-index physical descriptor + logical-index entry +
/// logical-index name) is written. The B-tree leaf pages that would let
/// Microsoft Access actually seek through the index are <em>not</em> emitted —
/// the physical descriptor's <c>first_dp</c> is set to <c>0</c>. The reader
/// surfaces the resulting metadata via <see cref="IAccessReader.ListIndexesAsync"/>,
/// but Microsoft Access will rebuild (or reject) the index on its next compact /
/// repair pass.
/// </para>
/// <para>
/// Constraints:
/// </para>
/// <list type="bullet">
///   <item><description>Single column only — multi-column indexes are not supported in this phase.</description></item>
///   <item><description>Non-unique only (<see cref="IndexMetadata.IsUnique"/> always reads back <c>false</c>).</description></item>
///   <item><description>Ascending only.</description></item>
///   <item><description>No primary-key, foreign-key, or relationship semantics.</description></item>
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
    /// Gets the column names that make up the index key, in key order. In phase W1
    /// this list always contains exactly one entry.
    /// </summary>
    public IReadOnlyList<string> Columns { get; }
}
