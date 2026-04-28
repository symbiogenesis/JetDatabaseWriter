namespace JetDatabaseWriter.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Core;

/// <summary>
/// Metadata describing a single JET logical index, parsed directly from the
/// owning table's TDEF page chain. Returned by
/// <see cref="IAccessReader.ListIndexesAsync"/>.
/// </summary>
/// <remarks>
/// The reader does not (yet) traverse the underlying B-tree leaf pages — these
/// are pure schema records describing the index name, key columns, and flags.
/// Multiple logical indexes can share the same physical (real) index; the
/// <see cref="RealIndexNumber"/> field surfaces that mapping.
/// </remarks>
public sealed record IndexMetadata
{
    /// <summary>Gets the logical-index name (e.g. <c>"PrimaryKey"</c>, <c>"CategoryIDFK"</c>).</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Gets the logical-index number (<c>index_num</c>) recorded in the TDEF
    /// logical-index entry. Not necessarily sequential.
    /// </summary>
    public int IndexNumber { get; init; }

    /// <summary>
    /// Gets the index of the physical (real) index that backs this logical index
    /// (<c>index_num2</c>). Multiple logical indexes can share one physical index.
    /// </summary>
    public int RealIndexNumber { get; init; }

    /// <summary>Gets the index classification (normal, primary key, or foreign key).</summary>
    public IndexKind Kind { get; init; }

    /// <summary>Gets a value indicating whether the index enforces uniqueness (<c>flags &amp; 0x01</c>).</summary>
    public bool IsUnique { get; init; }

    /// <summary>Gets a value indicating whether the index ignores rows whose key is null (<c>flags &amp; 0x02</c>).</summary>
    public bool IgnoreNulls { get; init; }

    /// <summary>Gets a value indicating whether the indexed columns are required to be non-null (<c>flags &amp; 0x08</c>).</summary>
    public bool IsRequired { get; init; }

    /// <summary>
    /// Gets a value indicating whether this index participates in a foreign-key
    /// relationship (<c>rel_idx_num</c> != <c>0xFFFFFFFF</c>).
    /// </summary>
    public bool IsForeignKey { get; init; }

    /// <summary>
    /// Gets the TDEF page number of the related table when <see cref="IsForeignKey"/>
    /// is <see langword="true"/>; otherwise <c>0</c>.
    /// </summary>
    public int RelatedTablePage { get; init; }

    /// <summary>
    /// Gets a value indicating whether updates to the parent key cascade to the
    /// child rows (<c>cascade_ups</c> != <c>0</c>). Only meaningful for FK indexes.
    /// </summary>
    public bool CascadeUpdates { get; init; }

    /// <summary>
    /// Gets a value indicating whether deletes of the parent row cascade to the
    /// child rows (<c>cascade_dels</c> != <c>0</c>). Only meaningful for FK indexes.
    /// </summary>
    public bool CascadeDeletes { get; init; }

    /// <summary>Gets the columns that make up the index key, in key order.</summary>
    public IReadOnlyList<IndexColumnReference> Columns { get; init; } = [];
}
