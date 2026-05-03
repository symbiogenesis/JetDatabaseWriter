namespace JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// Narrow DTO bound against the wide synthetic table (40 columns) — only
/// 4 properties match column names. This is the shape Phase 2's column
/// projection optimization is meant to accelerate; including it in the
/// baseline so the Phase 2 delta is visible.
/// </summary>
public sealed class WideRowNarrowProjection
{
    public int? Id { get; set; }

    public int? N0 { get; set; }

    public int? N5 { get; set; }

    public string? S0 { get; set; }
}
