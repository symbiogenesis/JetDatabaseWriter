namespace JetDatabaseWriter.Maintenance;

/// <summary>Describes unvalidated private allocations; it supplies no replacement TDEF references.</summary>
/// <param name="Tree">The contiguous stored-key tree allocation.</param>
/// <param name="OwnedPages">The immediately following map listing exactly the tree pages, excluding its own storage.</param>
internal sealed record CompactRawIndexAssemblyAllocation(CompactRawIndexBuilderResult Tree, CompactUsageMapBuilderResult OwnedPages);
