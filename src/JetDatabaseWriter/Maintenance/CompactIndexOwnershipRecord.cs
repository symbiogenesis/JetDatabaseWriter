namespace JetDatabaseWriter.Maintenance;

/// <summary>Known cross-root index ownership evidence that never grants reclamation.</summary>
internal sealed record CompactIndexOwnershipRecord
{
    /// <summary>Gets the ownership observation kind.</summary>
    public string Kind { get; init; } = string.Empty;

    /// <summary>Gets the page claimed, shared or targeted by this observation.</summary>
    public long PageNumber { get; init; }

    /// <summary>Gets the current root's expected table-definition owner.</summary>
    public long OwnerTDefPage { get; init; }

    /// <summary>Gets the root responsible for this observation.</summary>
    public long RootPageNumber { get; init; }

    /// <summary>Gets the earlier claimant's expected owner when applicable.</summary>
    public long? RelatedOwnerTDefPage { get; init; }

    /// <summary>Gets the earlier claimant's root when applicable.</summary>
    public long? RelatedRootPageNumber { get; init; }

    /// <summary>Gets the declaring physical descriptor's ordinal when available.</summary>
    public int? DescriptorIndex { get; init; }

    /// <summary>Gets the first declaration's physical descriptor ordinal when a root is shared.</summary>
    public int? RelatedDescriptorIndex { get; init; }

    /// <summary>Gets the exact ownership-map row within its shared physical data page.</summary>
    public int? MapRowIndex { get; init; }

    /// <summary>Gets the number of physical descriptors presented, including zero roots.</summary>
    public long DeclaredDescriptorCount { get; init; }

    /// <summary>Gets the number of distinct declared ownership-map rows.</summary>
    public int DeclaredMapRowCount { get; init; }

    /// <summary>Gets the number of descriptors whose stored root pointer is zero.</summary>
    public long MissingRootCount { get; init; }

    /// <summary>Gets the number of descriptors whose stored ownership-map pointer is zero.</summary>
    public long MissingMapCount { get; init; }

    /// <summary>Gets the page containing an incoming reference, when applicable.</summary>
    public long? SourcePageNumber { get; init; }

    /// <summary>Gets the incoming header or entry reference role.</summary>
    public string? Role { get; init; }

    /// <summary>Gets the incoming entry ordinal when the reference has one.</summary>
    public int? EntryIndex { get; init; }

    /// <summary>Gets the count of observed incoming references to this target, including repeated root scans.</summary>
    public long IncomingReferenceCount { get; init; }

    /// <summary>Gets a value indicating whether every declared descriptor has a nonzero root and each distinct root supplied a successful raw terminal.</summary>
    public bool DeclaredRootsObserved { get; init; }

    /// <summary>Gets a value indicating whether every descriptor has a nonzero independently inspected ownership map.</summary>
    public bool DeclaredMapsInspected { get; init; }

    /// <summary>Gets a value indicating whether every observed index page belonged to every independently inspected descriptor map for its root, with no descriptor map missing.</summary>
    public bool ObservedIndexPagesMapped { get; init; }

    /// <summary>Gets a value indicating whether every independently inspected map member was observed under its declared root, with no descriptor map missing.</summary>
    public bool DeclaredMapMembersObserved { get; init; }

    /// <summary>Gets the successful terminal's explicit coverage boundary.</summary>
    public string? Coverage { get; init; }

    /// <summary>Gets a value indicating whether all possible incoming owners have been established.</summary>
    public bool ExclusiveOwnershipProven { get; }

    /// <summary>Gets a value indicating whether every referenced storage location remains pinned.</summary>
    public bool IsPinned { get; } = true;
}
