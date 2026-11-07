namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;

/// <summary>
/// Summarizes table-layout optimization (Primary Key row clustering and external <c>LVAL</c> chain coalescing)
/// applied to the staged compacted database.
/// </summary>
/// <param name="ClusteredTableCount">The number of user tables reordered into Primary Key order.</param>
/// <param name="CoalescedLongValueCount">The total number of multi-hop long-value chains coalesced.</param>
/// <param name="ClusteredOwners">The table-definition root pages of clustered tables.</param>
/// <param name="CoalescedByOwner">The count of coalesced long-value chains per table owner.</param>
/// <param name="ReadBytes">The total bytes read during layout optimization.</param>
/// <param name="WrittenBytes">The total bytes written during layout optimization.</param>
internal sealed record CompactTableLayoutOptimizeResult(
    int ClusteredTableCount,
    long CoalescedLongValueCount,
    HashSet<long> ClusteredOwners,
    IReadOnlyDictionary<long, long> CoalescedByOwner,
    long ReadBytes,
    long WrittenBytes);
