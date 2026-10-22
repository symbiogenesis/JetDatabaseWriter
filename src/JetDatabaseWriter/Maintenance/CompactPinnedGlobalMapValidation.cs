namespace JetDatabaseWriter.Maintenance;

/// <summary>Independent inputs required to compare the concrete native global map during prefix preservation.</summary>
/// <param name="AppendPointPages">The independently recorded destination EOF before native global-map installation.</param>
/// <param name="Budget">The native-map limits, additionally constrained by aggregate prefix read and working allowances.</param>
/// <remarks>No installer result, changed page bytes or arbitrary exception range authorizes a prefix mutation.</remarks>
internal sealed record CompactPinnedGlobalMapValidation(long AppendPointPages, CompactGlobalMapBudget Budget);
