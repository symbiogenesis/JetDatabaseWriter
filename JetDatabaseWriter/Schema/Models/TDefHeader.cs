namespace JetDatabaseWriter.Schema.Models;

/// <summary>The declared counts and counters in a table definition header.</summary>
/// <param name="ColumnCount">The number of column descriptors.</param>
/// <param name="LogicalIndexCount">The declared logical index count.</param>
/// <param name="RealIndexCount">The declared physical index count.</param>
/// <param name="Counters">The current table counters.</param>
internal readonly record struct TDefHeader(int ColumnCount, int LogicalIndexCount, int RealIndexCount, TableCounters Counters)
{
    /// <summary>Gets the declared logical length (excluding the eight-byte root page prefix).</summary>
    internal int LogicalLength { get; init; }

    /// <summary>Gets the Access table-type marker.</summary>
    internal byte TableType { get; init; }

    /// <summary>Gets the highest allocated column number plus one.</summary>
    internal int MaximumColumnCount { get; init; }

    /// <summary>Gets the declared variable-column count.</summary>
    internal int VariableColumnCount { get; init; }

    /// <summary>Gets the packed owned-pages usage-map reference.</summary>
    internal uint OwnedPagesReference { get; init; }

    /// <summary>Gets the packed free-pages usage-map reference.</summary>
    internal uint FreePagesReference { get; init; }
}
