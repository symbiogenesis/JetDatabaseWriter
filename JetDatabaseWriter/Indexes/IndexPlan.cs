namespace JetDatabaseWriter.Indexes;

using JetDatabaseWriter.Models;

/// <summary>
/// The outcome of <see cref="IndexPlanner.TryPlan"/>: the index chosen to satisfy
/// a row predicate and the seek criteria derived from it.
/// </summary>
/// <param name="index">The index selected to drive the read.</param>
/// <param name="criteria">The seek criteria (exact, key-prefix, or range) for the index.</param>
/// <param name="matchedKeyColumns">The number of leading key columns the predicate constrained.</param>
internal sealed class IndexPlan(IndexMetadata index, IndexQueryCriteria criteria, int matchedKeyColumns)
{
    /// <summary>Gets the index selected to drive the read.</summary>
    public IndexMetadata Index { get; } = index;

    /// <summary>Gets the seek criteria (exact, key-prefix, or range) for the index.</summary>
    public IndexQueryCriteria Criteria { get; } = criteria;

    /// <summary>Gets the number of leading key columns the predicate constrained.</summary>
    public int MatchedKeyColumns { get; } = matchedKeyColumns;
}
