namespace JetDatabaseWriter.Maintenance;

/// <summary>The complete private native map allocations for one existing original LVAL trailer record.</summary>
/// <param name="ColumnNumber">The original row-layout column number.</param>
/// <param name="OwnedPages">Every page copied for this column, excluding map storage.</param>
/// <param name="FreePages">Owned LVAL pages with space for another nonempty row and slot.</param>
internal sealed record CompactRawTableLongValueMaps(int ColumnNumber, CompactUsageMapBuilderResult OwnedPages, CompactUsageMapBuilderResult FreePages);
