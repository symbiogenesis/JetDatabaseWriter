namespace JetDatabaseWriter.Maintenance;

/// <summary>One bounded observation; no record authorizes relocating or reclaiming storage.</summary>
internal sealed record CompactStorageRecord
{
    /// <summary>Gets the observation category.</summary>
    public string Kind { get; init; } = string.Empty;

    /// <summary>Gets the known storage role.</summary>
    public string Role { get; init; } = string.Empty;

    /// <summary>Gets the physical source page.</summary>
    public long PageNumber { get; init; }

    /// <summary>Gets the source offset within its page.</summary>
    public int ByteOffset { get; init; }

    /// <summary>Gets the observed byte count.</summary>
    public int ByteLength { get; init; }

    /// <summary>Gets the stitched TDEF offset, whose span may cross a continuation header.</summary>
    public int? LogicalOffset { get; init; }

    /// <summary>Gets the reference's verified encoding.</summary>
    public string? Encoding { get; init; }

    /// <summary>Gets the referenced page.</summary>
    public long? TargetPageNumber { get; init; }

    /// <summary>Gets the referenced row slot.</summary>
    public int? TargetRowIndex { get; init; }

    /// <summary>Gets the owning table definition, when known.</summary>
    public long? OwnerTDefPage { get; init; }

    /// <summary>Gets the catalog object's name.</summary>
    public string? Name { get; init; }

    /// <summary>Gets the catalog object's type.</summary>
    public int? ObjectType { get; init; }

    /// <summary>Gets the original catalog identity.</summary>
    public long? ObjectId { get; init; }

    /// <summary>Gets the original catalog flags.</summary>
    public long? Flags { get; init; }

    /// <summary>Gets the SHA256 of the complete decrypted physical page.</summary>
    public string? Sha256 { get; init; }

    /// <summary>Gets the scope or uncertainty explanation, without row values.</summary>
    public string? Detail { get; init; }

    /// <summary>Gets a value indicating whether the observed storage must retain its identity.</summary>
    public bool IsPinned { get; } = true;
}
