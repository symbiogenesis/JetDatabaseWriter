namespace JetDatabaseWriter.Indexes.Models;

using System.Collections.Generic;

/// <summary>
/// Result of <c>IndexBTreeBuilder.Build</c>: the rendered pages (in the order they
/// should be appended to the database) and the absolute page number of
/// the root, which the caller writes into the real-index
/// <c>first_dp</c> field on the TDEF.
/// </summary>
/// <param name="pages">The pages.</param>
/// <param name="rootPageNumber">The root page number.</param>
/// <param name="firstPageNumber">The first page number.</param>
internal readonly struct IndexBTreeBuildResult(IReadOnlyList<byte[]> pages, long rootPageNumber, long firstPageNumber)
{
    /// <summary>Gets the rendered pages, indexed [0..N-1]. Page i lives at
    /// absolute database page number <see cref="FirstPageNumber"/> + i.</summary>
    public IReadOnlyList<byte[]> Pages { get; } = pages;

    /// <summary>Gets the absolute page number of the root (leaf for a
    /// single-page tree, otherwise the topmost intermediate).</summary>
    public long RootPageNumber { get; } = rootPageNumber;

    /// <summary>Gets the absolute page number assigned to <c>Pages[0]</c>.</summary>
    public long FirstPageNumber { get; } = firstPageNumber;
}
