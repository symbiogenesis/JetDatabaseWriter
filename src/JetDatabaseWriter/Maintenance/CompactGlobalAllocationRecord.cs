namespace JetDatabaseWriter.Maintenance;

/// <summary>One contiguous allocation with its asserted retained source identity and private storage role.</summary>
/// <param name="Kind">The storage role; pinned source is synthesized only for the complete source prefix.</param>
/// <param name="OwnerPageNumber">The original pinned owner page, or page one for global map storage; zero only for the pinned prefix.</param>
/// <param name="Pages">The positive contiguous allocation.</param>
/// <remarks>Identity is independently checked against caller evidence, not established from geometry or this record alone.</remarks>
internal readonly record struct CompactGlobalAllocationRecord(CompactGlobalAllocationKind Kind, long OwnerPageNumber, CompactPageRun Pages);
