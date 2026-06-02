namespace JetDatabaseWriter.ValueDecoding.Models;

/// <summary>Classification returned by <see cref="AccessBase.ResolveColumnSlice"/>.</summary>
internal enum ColumnSliceKind
{
    /// <summary>Column is missing/empty/out-of-bounds - caller should emit empty/default.</summary>
    Empty = 0,

    /// <summary>Column is null (null-mask bit unset, or column index &gt;= row's numCols).</summary>
    Null = 1,

    /// <summary>Boolean column: <see cref="ColumnSlice.BoolValue"/> holds the null-mask bit.</summary>
    Bool = 2,

    /// <summary>Fixed-width column: <see cref="ColumnSlice.DataStart"/>/<see cref="ColumnSlice.DataLen"/>
    /// are valid (relative to the row start).</summary>
    Fixed = 3,

    /// <summary>Variable-width column: <see cref="ColumnSlice.DataStart"/>/<see cref="ColumnSlice.DataLen"/>
    /// are valid (relative to the row start); <c>DataLen</c> may be 0.</summary>
    Var = 4,
}
