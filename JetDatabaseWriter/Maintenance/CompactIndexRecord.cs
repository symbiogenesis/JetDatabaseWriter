namespace JetDatabaseWriter.Maintenance;

/// <summary>A raw index observation that never authorizes relocation or reclamation.</summary>
internal sealed record CompactIndexRecord
{
    /// <summary>Gets the observation kind.</summary>
    public string Kind { get; init; } = string.Empty;

    /// <summary>Gets the reference role when this observation contains one.</summary>
    public string? Role { get; init; }

    /// <summary>Gets the page containing this observation.</summary>
    public long PageNumber { get; init; }

    /// <summary>Gets the expected owning table-definition page.</summary>
    public long OwnerTDefPage { get; init; }

    /// <summary>Gets the observed tree root.</summary>
    public long RootPageNumber { get; init; }

    /// <summary>Gets the native page type for page observations.</summary>
    public byte? PageType { get; init; }

    /// <summary>Gets the zero-based entry ordinal, where applicable.</summary>
    public int? EntryIndex { get; init; }

    /// <summary>Gets an independent copy of the stored canonical key bytes, without semantic value interpretation.</summary>
    public byte[]? CanonicalKey { get; init; }

    /// <summary>Gets the referenced bookmark or header page.</summary>
    public long? TargetPageNumber { get; init; }

    /// <summary>Gets the native bookmark row byte.</summary>
    public byte? TargetRowIndex { get; init; }

    /// <summary>Gets the decoded intermediate child page.</summary>
    public long? ChildPageNumber { get; init; }

    /// <summary>Gets a value indicating whether this entry's bookmark names a live same-owner row header and readable stored payload chain.</summary>
    public bool BookmarkTargetValidated { get; init; }

    /// <summary>Gets the resolved stored-payload page when this entry's bookmark target was validated.</summary>
    public long? BookmarkDataPageNumber { get; init; }

    /// <summary>Gets the resolved stored-payload slot when this entry's bookmark target was validated.</summary>
    public int? BookmarkDataRowIndex { get; init; }

    /// <summary>Gets a value indicating whether every observed leaf and intermediate bookmark target was validated; true only on a composed scan's successful terminal.</summary>
    public bool BookmarkTargetsValidated { get; init; }

    /// <summary>Gets the explicit observation coverage on the terminal record.</summary>
    public string? Coverage { get; init; }

    /// <summary>Gets a value indicating whether all observed navigable edges were proven acyclic; true only on a successful terminal record.</summary>
    public bool NavigableGraphAcyclic { get; init; }

    /// <summary>Gets a value indicating whether keys reached solely through entry-child edges satisfy inclusive separator fences; all tail edges are excluded, including non-alias paths.</summary>
    public bool EntryEdgeFencesValidated { get; init; }

    /// <summary>Gets a value indicating whether complete observed leaf-key envelopes satisfy inclusive local separator fences on every entry-child and intermediate-tail edge, including aliases.</summary>
    /// <remarks>False withholds this proof without refusing uncertain native layouts. Bookmark tie breakers, missing pages and stored-key/row correspondence remain unverified.</remarks>
    public bool AllObservedEdgeFencesValidated { get; init; }

    /// <summary>Gets a value indicating whether every observed leaf belongs to one closed, reciprocal sibling chain in nondecreasing key order.</summary>
    /// <remarks>This covers observed leaves only; missing ownership or incoming references remain unverified.</remarks>
    public bool ObservedLeafChainOrdered { get; init; }

    /// <summary>Gets a value indicating whether bookmark tie breakers also satisfy in-page order, inclusive entry-edge-only fences and the complete observed leaf chain's order.</summary>
    /// <remarks>False withholds this proof without treating uncertain native duplicate bookmark ordering as corruption.</remarks>
    public bool BookmarkInclusiveOrderValidated { get; init; }

    /// <summary>Gets a value indicating whether separator/tail descent for each encoded key present in the observed leaves can reach its first matching leaf through one closed, ordered sibling chain.</summary>
    /// <remarks>False means the conservative bounded proof is incomplete. Missing leaves, key/row correspondence and incoming-reference ownership remain unverified.</remarks>
    public bool ObservedKeyRoutingValidated { get; init; }

    /// <summary>Gets a value indicating whether complete tree validity and incoming-reference ownership have been proven.</summary>
    public bool TreeCompletenessProven { get; }

    /// <summary>Gets a value indicating whether the observed storage remains pinned.</summary>
    public bool IsPinned { get; } = true;
}
