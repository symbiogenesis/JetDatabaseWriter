namespace JetDatabaseWriter.Internal.Models;

using System.Collections.Generic;

/// <summary>
/// A 2D collection of <see cref="IndexEntry"/> lists, each representing the entries for a page after a split operation.
/// Not to be confused with <c>LeafGroup</c> used in incremental index maintenance.
/// </summary>
internal class SplitPages : List<List<IndexEntry>>
{
    public SplitPages()
    {
    }

    public SplitPages(int capacity)
        : base(capacity)
    {
    }

    public SplitPages(List<List<IndexEntry>> pages)
        : base(pages)
    {
    }

    public IndexEntry GetLastEntry(int pageIndex) => this[pageIndex][^1];
}