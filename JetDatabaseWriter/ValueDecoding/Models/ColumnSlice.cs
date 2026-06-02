namespace JetDatabaseWriter.ValueDecoding.Models;

/// <summary>Per-column slice produced by <see cref="AccessBase.ResolveColumnSlice"/>.</summary>
/// <param name="Kind">The table name kind.</param>
/// <param name="DataStart">The data start.</param>
/// <param name="DataLen">The data len.</param>
/// <param name="BoolValue">The bool value.</param>
internal readonly record struct ColumnSlice(
    ColumnSliceKind Kind,
    int DataStart,
    int DataLen,
    bool BoolValue);
