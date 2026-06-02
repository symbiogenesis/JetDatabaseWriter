namespace JetDatabaseWriter.Indexes.Models;

using JetDatabaseWriter.Indexes;

/// <summary>
/// Anchors of the three index-related blocks within a TDEF buffer,
/// plus the populated real-idx / logical-idx slot counts (TDEF header
/// fields), returned by <see cref="IndexLayout.GetIndexSection"/>. Bundles every
/// piece of state a catalog walker needs so callers pass a single value
/// instead of four parallel arguments.
/// </summary>
/// <param name="RealIdxDescStart">The real index desc start.</param>
/// <param name="LogIdxStart">The log index start.</param>
/// <param name="LogIdxNamesStart">The log index names start.</param>
/// <param name="NumRealIdx">The number of real index.</param>
/// <param name="NumIdx">The number of index.</param>
internal readonly record struct IndexSectionAnchors(
    int RealIdxDescStart,
    int LogIdxStart,
    int LogIdxNamesStart,
    int NumRealIdx,
    int NumIdx);
