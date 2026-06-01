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

    private readonly IndexPageLayout layout;
    private readonly Func<long, CancellationToken, ValueTask<byte[]>> readPage;
    private readonly int pageSize;

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexCursor"/> class for Jet4 / ACE pages.
    /// </summary>
    /// <param name="readPage">The read page.</param>
    /// <param name="pageSize">The page size.</param>
    public IndexCursor(Func<long, CancellationToken, ValueTask<byte[]>> readPage, int pageSize)
        : this(IndexPageLayout.Jet4, readPage, pageSize)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexCursor"/> class using the supplied per-format index page layout.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="readPage">The read page.</param>
    /// <param name="pageSize">The page size.</param>
    public IndexCursor(
        IndexPageLayout layout,
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<bool> ContainsKeyAsync(
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(searchKey, nameof(searchKey));

        byte[]? leafPage = await this.FindCandidateLeafAsync(rootPageNumber, searchKey, cancellationToken).ConfigureAwait(false);
        if (leafPage == null)
        {
            return false;
        }

        return await this.ContainsInLeafChainAsync(leafPage, searchKey, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns every data-row pointer whose canonical key equals
    /// <paramref name="searchKey"/>.
    /// </summary>
    /// <param name="rootPageNumber">The root page number.</param>
    /// <param name="searchKey">The search key.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<List<(long DataPage, int RowIndex)>> FindRowLocationsAsync(
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(searchKey, nameof(searchKey));

        var matches = new List<(long DataPage, int RowIndex)>();
        byte[]? leafPage = await this.FindCandidateLeafAsync(rootPageNumber, searchKey, cancellationToken).ConfigureAwait(false);
        if (leafPage == null)
        {
            return matches;
        }

        await this.CollectLeafChainAsync(leafPage, searchKey, matches, cancellationToken).ConfigureAwait(false);
        return matches;
    }

    /// <summary>
    /// Returns every data-row pointer whose canonical key falls within the
    /// supplied encoded bounds.
    /// </summary>
    /// <param name="rootPageNumber">The root page number.</param>
    /// <param name="lowerKey">The encoded lower key, or <see langword="null"/> for unbounded.</param>
    /// <param name="lowerInclusive">Whether <paramref name="lowerKey"/> is inclusive.</param>
    /// <param name="lowerIsPrefix">Whether <paramref name="lowerKey"/> represents a leading-key prefix.</param>
    /// <param name="upperKey">The encoded upper key, or <see langword="null"/> for unbounded.</param>
    /// <param name="upperInclusive">Whether <paramref name="upperKey"/> is inclusive.</param>
    /// <param name="upperIsPrefix">Whether <paramref name="upperKey"/> represents a leading-key prefix.</param>
    /// <param name="requiredPrefix">An encoded leading-key prefix every match must start with, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<List<(long DataPage, int RowIndex)>> FindRowLocationsInRangeAsync(
        long rootPageNumber,
        byte[]? lowerKey,
        bool lowerInclusive,
        bool lowerIsPrefix,
        byte[]? upperKey,
        bool upperInclusive,
        bool upperIsPrefix,
        byte[]? requiredPrefix,
        CancellationToken cancellationToken)
    {
        var matches = new List<(long DataPage, int RowIndex)>();
        byte[]? startKey = lowerKey ?? requiredPrefix ?? [];
        byte[]? leafPage = await this.FindCandidateLeafAsync(rootPageNumber, startKey, cancellationToken).ConfigureAwait(false);
        if (leafPage == null)
        {
            return matches;
        }

        await this.CollectRangeLeafChainAsync(
            leafPage,
            lowerKey,
            lowerInclusive,
            lowerIsPrefix,
            upperKey,
            upperInclusive,
            upperIsPrefix,
            requiredPrefix,
            matches,
            cancellationToken).ConfigureAwait(false);
        return matches;
    }

    private async ValueTask<byte[]?> FindCandidateLeafAsync(
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        if (rootPageNumber <= 0 || this.pageSize <= this.layout.FirstEntryOffset)
        {
            return null;
        }

        long currentPageNumber = rootPageNumber;
        for (int depth = 0; depth < MaxDepth; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.readPage(currentPageNumber, cancellationToken).ConfigureAwait(false);
            if (IndexPageCodec.IsLeaf(page))
            {
                return page;
            }

            if (!IndexPageCodec.IsIntermediate(page))
            {
                return null;
            }

            long? selectedChildPage = IndexPageCodec.SelectChildPage(this.layout, page, this.pageSize, searchKey);
            long nextPageNumber = selectedChildPage ?? IndexPageCodec.ReadTailPage(this.layout, page);
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

            (bool found, bool continueToNext) = IndexPageCodec.ContainsKeyInLeafPage(this.layout, page, this.pageSize, searchKey);
            if (found)
            {
                return true;
            }

            if (!continueToNext)
            {
                return false;
            }

            long nextPageNumber = IndexPageCodec.ReadNextPage(this.layout, page);
            if (nextPageNumber <= 0)
            {
                return false;
            }

            page = await this.readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
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

            bool continueToNext = IndexPageCodec.CollectMatchingLeafEntries(this.layout, page, this.pageSize, searchKey, matches);
            if (!continueToNext)
            {
                return;
            }

            long nextPageNumber = IndexPageCodec.ReadNextPage(this.layout, page);
            if (nextPageNumber <= 0)
            {
                return;
            }

            page = await this.readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask CollectRangeLeafChainAsync(
        byte[] leafPage,
        byte[]? lowerKey,
        bool lowerInclusive,
        bool lowerIsPrefix,
        byte[]? upperKey,
        bool upperInclusive,
        bool upperIsPrefix,
        byte[]? requiredPrefix,
        List<(long DataPage, int RowIndex)> matches,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        while (page != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            bool continueToNext = IndexPageCodec.CollectRangeLeafEntries(
                this.layout,
                page,
                this.pageSize,
                lowerKey,
                lowerInclusive,
                lowerIsPrefix,
                upperKey,
                upperInclusive,
                upperIsPrefix,
                requiredPrefix,
                matches);
            if (!continueToNext)
            {
                return;
            }

            long nextPageNumber = IndexPageCodec.ReadNextPage(this.layout, page);
            if (nextPageNumber <= 0)
            {
                return;
            }

            page = await this.readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }
    }
}
