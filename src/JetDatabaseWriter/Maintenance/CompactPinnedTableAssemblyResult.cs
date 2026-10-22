namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;

/// <summary>Independent reopened private raw table, stored index and pinned definition evidence.</summary>
/// <param name="Table">The complete raw table and exact native maps comparison.</param>
/// <param name="Indexes">The original stored keys, relocated bookmarks and exact map comparisons in physical slot order.</param>
/// <param name="Definition">The independent original-identity physical definition-chain comparison.</param>
/// <param name="ReadBytes">The aggregate admitted private destination and ledger reads.</param>
internal sealed record CompactPinnedTableAssemblyResult(
    CompactRawTableValidatorResult Table,
    IReadOnlyList<CompactRawIndexAssemblyResult> Indexes,
    CompactTDefChainResult Definition,
    long ReadBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether complete incoming ownership permits reclamation; always false.</summary>
    public bool OwnershipProven { get; }
}
