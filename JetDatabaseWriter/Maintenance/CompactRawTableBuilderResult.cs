namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;

/// <summary>One private table assembly retaining pinned source and TDEF identities.</summary>
/// <param name="Data">The packed row allocation and complete matching row ledger.</param>
/// <param name="LongValues">The complete appended raw LVAL allocation.</param>
/// <param name="OwnedPages">The packed data-page map, excluding LVAL and map storage.</param>
/// <param name="FreePages">The data pages with capacity for another nonempty row and slot.</param>
/// <param name="LongValueMaps">Independently addressed map pairs in original TDEF trailer order.</param>
/// <param name="PatchedDefinition">The raw logical TDEF with only verified storage pointer words replaced.</param>
/// <param name="AllocationRecords">The complete twelve-byte LVAL allocation ledger record count.</param>
/// <param name="ReadBytes">The cumulative admitted private read requests.</param>
internal sealed record CompactRawTableBuilderResult(
    CompactRawDataBuilderResult Data,
    CompactRawDataLongValueRelocatorResult LongValues,
    CompactUsageMapBuilderResult OwnedPages,
    CompactUsageMapBuilderResult FreePages,
    IReadOnlyList<CompactRawTableLongValueMaps> LongValueMaps,
    byte[] PatchedDefinition,
    long AllocationRecords,
    long ReadBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool IsValidated { get; }

    /// <summary>Gets a value indicating whether global incoming ownership permits reclamation; always false.</summary>
    public bool OwnershipProven { get; }
}
