namespace JetDatabaseWriter.Schema.Models;

/// <summary>The declared structural counts, readable without the usage-map or counter fields.</summary>
/// <param name="ColumnCount">The declared column count.</param>
/// <param name="LogicalIndexCount">The declared logical index count.</param>
/// <param name="RealIndexCount">The declared physical index count.</param>
internal readonly record struct TDefCounts(int ColumnCount, int LogicalIndexCount, int RealIndexCount);
