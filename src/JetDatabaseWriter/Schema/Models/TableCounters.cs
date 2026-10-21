namespace JetDatabaseWriter.Schema.Models;

/// <summary>Mutable on-disk table counters, separated from the structural schema.</summary>
/// <param name="RowCount">The number of live rows.</param>
/// <param name="AutoNumber">The last allocated ordinary AutoNumber.</param>
/// <param name="ComplexAutoNumber">The last allocated complex AutoNumber, or zero for MDB.</param>
internal readonly record struct TableCounters(uint RowCount, uint AutoNumber, uint ComplexAutoNumber);
