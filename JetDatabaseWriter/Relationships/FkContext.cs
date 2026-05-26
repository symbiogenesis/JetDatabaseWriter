namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Per-mutation-call cache for enforced relationship metadata and FK lookup state.
/// </summary>
internal sealed class FkContext(IReadOnlyList<FkRelationship> all)
{
    public IReadOnlyList<FkRelationship> All { get; } = all;

    public Dictionary<string, HashSet<string>> ParentKeySets { get; }
        = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, ParentSeekIndex?> SeekIndexes { get; }
        = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, ChildSeekIndex?> ChildSeekIndexes { get; }
        = new(StringComparer.OrdinalIgnoreCase);
}
