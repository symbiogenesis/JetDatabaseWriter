namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Read-only cursor over a JET index B-tree. It performs layout-aware
/// intermediate descent, tail-page fall-through, and leaf-chain walks while
/// delegating page decoding to <see cref="IndexPageCodec"/>.
/// </summary>
internal sealed class IndexCursor
{
    private const int MaxDepth = 32;

    private readonly IndexLeafPageBuilder.LeafPageLayout layout;
    private readonly Func<long, CancellationToken, ValueTask<byte[]>> readPage;
    private readonly int pageSize;

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexCursor"/> class for Jet4 / ACE pages.
    /// </summary>
    /// <param name="readPage">The read page.</param>
    /// <param name="pageSize">The page size.</param>
    public IndexCursor(Func<long, CancellationToken, ValueTask<byte[]>> readPage, int pageSize)
        : this(IndexLeafPageBuilder.LeafPageLayout.Jet4, readPage, pageSize)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexCursor"/> class using the supplied per-format index page layout.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="readPage">The read page.</param>
    /// <param name="pageSize">The page size.</param>
    public IndexCursor(
        IndexLeafPageBuilder.LeafPageLayout layout,
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        int pageSize)
    {
        Guard.NotNull(readPage, nameof(readPage));

        this.layout = layout;
        this.readPage = readPage;
        this.pageSize = pageSize;
    }

    /// <summary>
    /// Returns <see langword="true"/> when the B-tree contains at least one
    /// entry with a canonical key equal to <paramref name="searchKey"/>.
    /// </summary>
    /// <param name="rootPageNumber">The root page number.</param>
    /// <param name="searchKey">The search key.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    public async ValueTask<bool> ContainsKeyAsync(
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(searchKey, nameof(searchKey));

        byte[]? leafPage = await FindCandidateLeafAsync(rootPageNumber, searchKey, cancellationToken).ConfigureAwait(false);
        if (leafPage == null)
        {
            return false;
        }

        return await ContainsInLeafChainAsync(leafPage, searchKey, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns every data-row pointer whose canonical key equals
    /// <paramref name="searchKey"/>.
    /// </summary>
    /// <param name="rootPageNumber">The root page number.</param>
    /// <param name="searchKey">The search key.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    public async ValueTask<List<(long DataPage, int RowIndex)>> FindRowLocationsAsync(
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(searchKey, nameof(searchKey));

        var matches = new List<(long DataPage, int RowIndex)>();
        byte[]? leafPage = await FindCandidateLeafAsync(rootPageNumber, searchKey, cancellationToken).ConfigureAwait(false);
        if (leafPage == null)
        {
            return matches;
        }

        await CollectLeafChainAsync(leafPage, searchKey, matches, cancellationToken).ConfigureAwait(false);
        return matches;
    }

    private async ValueTask<byte[]?> FindCandidateLeafAsync(
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        if (rootPageNumber <= 0 || pageSize <= layout.FirstEntryOffset)
        {
            return null;
        }

        long currentPageNumber = rootPageNumber;
        for (int depth = 0; depth < MaxDepth; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await readPage(currentPageNumber, cancellationToken).ConfigureAwait(false);
            if (IndexPageCodec.IsLeaf(page))
            {
                return page;
            }

            if (!IndexPageCodec.IsIntermediate(page))
            {
                return null;
            }

            long? selectedChildPage = IndexPageCodec.SelectChildPage(layout, page, pageSize, searchKey);
            long nextPageNumber = selectedChildPage ?? IndexPageCodec.ReadTailPage(layout, page);
            if (nextPageNumber <= 0)
            {
                return null;
            }

            currentPageNumber = nextPageNumber;
        }

        return null;
    }

    private async ValueTask<bool> ContainsInLeafChainAsync(
        byte[] leafPage,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        while (page != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (bool found, bool continueToNext) = IndexPageCodec.ContainsKeyInLeafPage(layout, page, pageSize, searchKey);
            if (found)
            {
                return true;
            }

            if (!continueToNext)
            {
                return false;
            }

            long nextPageNumber = IndexPageCodec.ReadNextPage(layout, page);
            if (nextPageNumber <= 0)
            {
                return false;
            }

            page = await readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }

        return false;
    }

    private async ValueTask CollectLeafChainAsync(
        byte[] leafPage,
        byte[] searchKey,
        List<(long DataPage, int RowIndex)> matches,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        while (page != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            bool continueToNext = IndexPageCodec.CollectMatchingLeafEntries(layout, page, pageSize, searchKey, matches);
            if (!continueToNext)
            {
                return;
            }

            long nextPageNumber = IndexPageCodec.ReadNextPage(layout, page);
            if (nextPageNumber <= 0)
            {
                return;
            }

            page = await readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }
    }
}
