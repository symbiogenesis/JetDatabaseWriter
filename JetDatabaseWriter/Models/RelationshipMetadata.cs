namespace JetDatabaseWriter.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Interfaces;

/// <summary>
/// Metadata describing a single foreign-key relationship declared in the
/// database's <c>MSysRelationships</c> catalog. Returned by
/// <see cref="IAccessReader.ListRelationshipsAsync"/>.
/// </summary>
/// <remarks>
/// A relationship links the <see cref="ForeignTable"/> (the child / FK side) to the
/// <see cref="PrimaryTable"/> (the parent / referenced side). Composite keys list
/// their columns in matching order, so <see cref="ForeignColumns"/>[i] references
/// <see cref="PrimaryColumns"/>[i].
/// </remarks>
public sealed record RelationshipMetadata
{
    /// <summary>Gets the relationship name (<c>szRelationship</c>).</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Gets the parent / referenced table name (<c>szReferencedObject</c>).</summary>
    public string PrimaryTable { get; init; } = string.Empty;

    /// <summary>Gets the parent key columns, in key order (<c>szReferencedColumn</c>).</summary>
    public IReadOnlyList<string> PrimaryColumns { get; init; } = [];

    /// <summary>Gets the child / foreign-key table name (<c>szObject</c>).</summary>
    public string ForeignTable { get; init; } = string.Empty;

    /// <summary>Gets the child foreign-key columns, in key order (<c>szColumn</c>).</summary>
    public IReadOnlyList<string> ForeignColumns { get; init; } = [];

    /// <summary>
    /// Gets a value indicating whether the relationship enforces referential
    /// integrity (the <c>grbit</c> no-integrity bit is clear).
    /// </summary>
    public bool EnforcesReferentialIntegrity { get; init; }

    /// <summary>Gets a value indicating whether updates to the parent key cascade to the child rows.</summary>
    public bool CascadeUpdates { get; init; }

    /// <summary>Gets a value indicating whether deletes of the parent row cascade to the child rows.</summary>
    public bool CascadeDeletes { get; init; }
}
