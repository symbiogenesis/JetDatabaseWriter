namespace JetDatabaseWriter.Maintenance;

/// <summary>Returns references only after independent reopened private tree and ownership-map comparison.</summary>
/// <remarks>This proves the supplied original observations and private allocations, not complete source coverage or database validity.</remarks>
/// <param name="References">The verified replacement for one original physical index descriptor.</param>
/// <param name="Tree">The independent stored-key, bookmark and tree-coverage evidence.</param>
/// <param name="OwnedPages">The independent exact tree-membership and map-coverage evidence.</param>
internal sealed record CompactRawIndexAssemblyResult(CompactTDefIndexReferences References, CompactRawIndexValidatorResult Tree, CompactUsageMapValidatorResult OwnedPages);
