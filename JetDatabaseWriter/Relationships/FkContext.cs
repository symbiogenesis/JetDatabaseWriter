namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Per-mutation-call cache for enforced relationship metadata and FK lookup state.
/// </summary>
/// <param name="all">The complete set of contexts.</param>
internal sealed class FkContext(IReadOnlyList<FkRelationship> all)
{
    public IReadOnlyList<FkRelationship> All { get; } = all;

    /// <summary>
    /// Gets the normalized keys of every row of a relationship's primary
    /// table, by relationship name, read once per call when a foreign key
    /// cannot be checked by an index seek.
    /// </summary>
    public Dictionary<string, HashSet<string>> ParentKeySets { get; }
        = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the primary keys of the rows this call has inserted so far, by
    /// the name of a self-referencing relationship, so a later row of the
    /// same batch can reference an earlier one before the batch's index
    /// maintenance adds it to the index.
    /// </summary>
    public Dictionary<string, HashSet<string>> InsertedParentKeys { get; }
        = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Gets complete post-update parent key sets for tables the statement rewrites.</summary>
    public Dictionary<string, HashSet<string>> PlannedParentKeys { get; }
        = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Gets the tables whose relationship key descriptors passed native policy.</summary>
    public HashSet<string> ValidatedKeyTables { get; } = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, ParentSeekIndex?> SeekIndexes { get; }
        = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, ChildSeekIndex?> ChildSeekIndexes { get; }
        = new(StringComparer.OrdinalIgnoreCase);
}
