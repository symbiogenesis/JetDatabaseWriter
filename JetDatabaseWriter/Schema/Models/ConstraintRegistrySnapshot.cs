namespace JetDatabaseWriter.Schema.Models;

using System.Collections.Generic;

/// <summary>
/// The contents of a <see cref="ConstraintRegistry"/> at one point in time:
/// each registered table's constraint list, and the auto-increment counter
/// each AutoNumber constraint held then. A transaction takes one when it
/// begins and hands it back to <see cref="ConstraintRegistry.Restore"/> if it
/// rolls back.
/// </summary>
/// <param name="Tables">Each registered table's constraint list, keyed by table name.</param>
/// <param name="TableRules">Each cached table validation rule, or known absence.</param>
/// <param name="AutoCounters">Each AutoNumber constraint and its next auto-increment value.</param>
internal sealed record ConstraintRegistrySnapshot(
    IReadOnlyDictionary<string, List<ColumnConstraint>> Tables,
    IReadOnlyList<(ColumnConstraint Constraint, long? NextAutoValue)> AutoCounters,
    IReadOnlyDictionary<string, TableValidationConstraint?> TableRules);
