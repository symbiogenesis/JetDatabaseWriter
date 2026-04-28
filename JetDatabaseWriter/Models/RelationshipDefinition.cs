namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;

/// <summary>
/// Defines a foreign-key relationship that
/// <see cref="IAccessWriter.CreateRelationshipAsync(RelationshipDefinition, System.Threading.CancellationToken)"/>
/// emits as one row per FK column into the <c>MSysRelationships</c> system table.
/// </summary>
/// <remarks>
/// <para>
/// The writer emits the <c>MSysRelationships</c> catalog rows that the
/// Microsoft Access Relationships designer reads, and (on Jet4 / ACE) the
/// per-TDEF foreign-key logical-index entries (with <c>index_type = 0x02</c>,
/// <c>rel_idx_num</c>, <c>rel_tbl_page</c>) that drive runtime referential-
/// integrity enforcement by the JET engine. See
/// <c>docs/design/index-and-relationship-format-notes.md</c> for the on-disk
/// layout.
/// </para>
/// <para>
/// The Access GUI convention used by these property names: dragging from
/// <c>Customers.CustomerID</c> (PK side) to <c>Orders.CustomerID</c> (FK side)
/// produces a relationship where <see cref="PrimaryTable"/> = <c>Customers</c>,
/// <see cref="ForeignTable"/> = <c>Orders</c>, and the matching column lists
/// have the same arity.
/// </para>
/// <para>
/// Constraints (enforced at <c>CreateRelationshipAsync</c> time):
/// </para>
/// <list type="bullet">
///   <item><description>Both <see cref="PrimaryTable"/> and <see cref="ForeignTable"/> must already exist as user tables.</description></item>
///   <item><description>Every name in <see cref="PrimaryColumns"/> / <see cref="ForeignColumns"/> must match a column on its table, case-insensitively.</description></item>
///   <item><description><see cref="PrimaryColumns"/> and <see cref="ForeignColumns"/> must have the same length and at least one entry.</description></item>
///   <item><description><see cref="Name"/> must be unique across existing relationships in this database (case-insensitive).</description></item>
///   <item><description>The database must already contain a <c>MSysRelationships</c> table. Databases freshly created by <c>AccessWriter.CreateDatabaseAsync</c> do not include this table; open an Access-authored fixture or copy one before calling <c>CreateRelationshipAsync</c>.</description></item>
/// </list>
/// </remarks>
public sealed record RelationshipDefinition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RelationshipDefinition"/> class
    /// describing a single-column foreign-key relationship.
    /// </summary>
    /// <param name="name">The relationship name (typically <c>"FK_Child_Parent"</c> or similar).</param>
    /// <param name="primaryTable">The name of the parent (PK side) table — written to the <c>szReferencedObject</c> column.</param>
    /// <param name="primaryColumn">The name of the primary-key column on the parent table — written to <c>szReferencedColumn</c>.</param>
    /// <param name="foreignTable">The name of the child (FK side) table — written to the <c>szObject</c> column.</param>
    /// <param name="foreignColumn">The name of the foreign-key column on the child table — written to <c>szColumn</c>.</param>
    public RelationshipDefinition(string name, string primaryTable, string primaryColumn, string foreignTable, string foreignColumn)
        : this(name, primaryTable, [primaryColumn], foreignTable, [foreignColumn])
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RelationshipDefinition"/> class
    /// describing a possibly-composite foreign-key relationship.
    /// </summary>
    /// <param name="name">The relationship name.</param>
    /// <param name="primaryTable">The parent (PK side) table name.</param>
    /// <param name="primaryColumns">The PK column names, in key order. Must have the same length as <paramref name="foreignColumns"/>.</param>
    /// <param name="foreignTable">The child (FK side) table name.</param>
    /// <param name="foreignColumns">The FK column names, in the same order as <paramref name="primaryColumns"/>.</param>
    public RelationshipDefinition(
        string name,
        string primaryTable,
        IReadOnlyList<string> primaryColumns,
        string foreignTable,
        IReadOnlyList<string> foreignColumns)
    {
        Guard.NotNull(primaryColumns, nameof(primaryColumns));
        Guard.NotNull(foreignColumns, nameof(foreignColumns));

        if (primaryColumns.Count == 0)
        {
            throw new ArgumentException("At least one column is required.", nameof(primaryColumns));
        }

        if (primaryColumns.Count != foreignColumns.Count)
        {
            throw new ArgumentException(
                $"primaryColumns ({primaryColumns.Count}) and foreignColumns ({foreignColumns.Count}) must have the same length.",
                nameof(foreignColumns));
        }

        var pkCopy = new string[primaryColumns.Count];
        var fkCopy = new string[foreignColumns.Count];
        for (int i = 0; i < primaryColumns.Count; i++)
        {
            pkCopy[i] = primaryColumns[i];
            fkCopy[i] = foreignColumns[i];
        }

        Name = name;
        PrimaryTable = primaryTable;
        PrimaryColumns = pkCopy;
        ForeignTable = foreignTable;
        ForeignColumns = fkCopy;
    }

    /// <summary>Gets the relationship name (written to the <c>szRelationship</c> column).</summary>
    public string Name { get; }

    /// <summary>Gets the parent (PK side) table name (written to <c>szReferencedObject</c>).</summary>
    public string PrimaryTable { get; }

    /// <summary>Gets the primary-key column names, in key order (written to <c>szReferencedColumn</c>, one row per column).</summary>
    public IReadOnlyList<string> PrimaryColumns { get; }

    /// <summary>Gets the child (FK side) table name (written to <c>szObject</c>).</summary>
    public string ForeignTable { get; }

    /// <summary>Gets the foreign-key column names, in key order (written to <c>szColumn</c>, one row per column).</summary>
    public IReadOnlyList<string> ForeignColumns { get; }

    /// <summary>
    /// Gets a value indicating whether the JET engine should enforce referential integrity
    /// for this relationship. When <see langword="true"/> (the default), the
    /// <c>NO_REFERENTIAL_INTEGRITY</c> flag bit (<c>0x00000002</c>) is left clear on
    /// <c>grbit</c>; when <see langword="false"/>, the bit is set.
    /// </summary>
    /// <remarks>
    /// does not itself enforce referential integrity at insert/update/delete time —
    /// this flag controls only what Microsoft Access does when it opens the file.
    /// </remarks>
    public bool EnforceReferentialIntegrity { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether updates to the parent key should cascade to the
    /// foreign-key column(s). Sets the <c>CASCADE_UPDATES</c> flag bit
    /// (<c>0x00000100</c>) on <c>grbit</c>.
    /// </summary>
    public bool CascadeUpdates { get; init; }

    /// <summary>
    /// Gets a value indicating whether deletes of the parent row should cascade to
    /// matching child rows. Sets the <c>CASCADE_DELETES</c> flag bit
    /// (<c>0x00001000</c>) on <c>grbit</c>.
    /// </summary>
    public bool CascadeDeletes { get; init; }
}
