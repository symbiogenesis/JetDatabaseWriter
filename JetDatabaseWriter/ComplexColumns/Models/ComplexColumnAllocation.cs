namespace JetDatabaseWriter.ComplexColumns.Models;

using JetDatabaseWriter.ComplexColumns;

/// <summary>
/// Per-column scratch state captured by <see cref="ComplexColumnManager.PrepareComplexColumnAllocationsAsync"/>
/// and consumed by <see cref="ComplexColumnManager.EmitComplexColumnArtifactsAsync"/>.
/// </summary>
/// <param name="ColumnIndex">The column index.</param>
/// <param name="ComplexId">The complex id.</param>
internal readonly record struct ComplexColumnAllocation(int ColumnIndex, int ComplexId);
