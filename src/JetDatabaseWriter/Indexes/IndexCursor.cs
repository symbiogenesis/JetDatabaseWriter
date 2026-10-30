namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>
/// Read-only cursor over a JET index B-tree. It performs layout-aware
/// intermediate descent, tail-page fall-through, and leaf-chain walks while
/// delegating page decoding to <see cref="IndexPageCodec"/>.
/// </summary>
internal sealed class IndexCursor : IDisposable
{
    private const int MaxDepth = 32;
    private const int MaxLeafHops = 1_048_576;

    private readonly IndexPageLayout layout;
    private readonly Func<long, CancellationToken, ValueTask<PageLease>> readPage;
    private readonly int pageSize;
    private PageLease? currentPage;

    /// <summary>
    /// Initializes a new instance of the <see cref="IndexCursor"/> class using the supplied per-format index page layout.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="readPage">The read page.</param>
    /// <param name="pageSize">The page size.</param>
    public IndexCursor(
        IndexPageLayout layout,
        Func<long, CancellationToken, ValueTask<PageLease>> readPage,
        int pageSize)
    {
        Guard.NotNull(readPage, nameof(readPage));

        this.layout = layout;
        this.readPage = readPage;
        this.pageSize = pageSize;
    }

    /// <summary>Releases the currently pinned image.</summary>
    public void Dispose()
    {
        this.currentPage?.Dispose();
        this.currentPage = null;
    }

    private async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        this.Dispose();
        this.currentPage = await this.readPage(pageNumber, cancellationToken).ConfigureAwait(false);
        return this.currentPage.Buffer;
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

        (long PageNumber, byte[] Page)? candidate = await this.FindCandidateLeafWithNumberAsync(rootPageNumber, searchKey, cancellationToken).ConfigureAwait(false);
        if (candidate == null)
        {
            return false;
        }

        return await this.ContainsInLeafChainAsync(candidate.Value.PageNumber, candidate.Value.Page, searchKey, cancellationToken).ConfigureAwait(false);
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
        (long PageNumber, byte[] Page)? candidate = await this.FindCandidateLeafWithNumberAsync(rootPageNumber, searchKey, cancellationToken).ConfigureAwait(false);
        if (candidate == null)
        {
            return matches;
        }

        await this.CollectLeafChainAsync(candidate.Value.PageNumber, candidate.Value.Page, searchKey, matches, cancellationToken).ConfigureAwait(false);
        return matches;
    }

    /// <summary>
    /// Returns every data-row pointer whose canonical key falls within the
    /// supplied encoded bounds.
    /// </summary>
    /// <param name="rootPageNumber">The root page number.</param>
    /// <param name="range">The encoded range.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<List<(long DataPage, int RowIndex)>> FindRowLocationsInRangeAsync(
        long rootPageNumber,
        EncodedIndexRange range,
        CancellationToken cancellationToken)
    {
        var matches = new List<(long DataPage, int RowIndex)>();
        byte[]? startKey = range.Lower.Key ?? range.RequiredPrefix ?? [];
        (long PageNumber, byte[] Page)? candidate = await this.FindCandidateLeafWithNumberAsync(rootPageNumber, startKey, cancellationToken).ConfigureAwait(false);
        if (candidate == null)
        {
            return matches;
        }

        await this.CollectRangeLeafChainAsync(
            candidate.Value.PageNumber,
            candidate.Value.Page,
            range,
            matches,
            cancellationToken).ConfigureAwait(false);
        return matches;
    }

    /// <summary>Streams matching row pointers, retaining only the current leaf's matches.</summary>
    /// <param name="format">The format profile.</param>
    /// <param name="tableName">The owning table.</param>
    /// <param name="index">The selected index.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="criteria">The seek criteria.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetNotSupportedException">The criteria kind is unsupported.</exception>
    public async IAsyncEnumerable<(long DataPage, int RowIndex)> EnumerateRowLocationsForCriteriaAsync(
        JetFormat format,
        string tableName,
        IndexMetadata index,
        TableDef tableDef,
        IndexQueryCriteria criteria,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        EncodedIndexRange range;
        switch (criteria.Kind)
        {
            case IndexQueryKind.All:
                range = new EncodedIndexRange(EncodedIndexBound.None, EncodedIndexBound.None);
                break;
            case IndexQueryKind.Exact:
                byte[] key = IndexKeyEncoder.EncodeIndexSeekKey(format, tableName, index, tableDef, criteria.Values!);
                range = new EncodedIndexRange(new EncodedIndexBound(key, true, false), new EncodedIndexBound(key, true, false));
                break;
            case IndexQueryKind.KeyPrefix:
                byte[] prefix = IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, index, tableDef, criteria.Values!, nameof(criteria));
                range = new EncodedIndexRange(new EncodedIndexBound(prefix, true, false), EncodedIndexBound.None, prefix);
                break;
            case IndexQueryKind.Range:
                EncodedIndexBound lower = criteria.Lower == null ? EncodedIndexBound.None : new EncodedIndexBound(IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, index, tableDef, criteria.Lower.Values, nameof(criteria)), criteria.Lower.IsInclusive, criteria.Lower.Values.Count < index.Columns.Count);
                EncodedIndexBound upper = criteria.Upper == null ? EncodedIndexBound.None : new EncodedIndexBound(IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, index, tableDef, criteria.Upper.Values, nameof(criteria)), criteria.Upper.IsInclusive, criteria.Upper.Values.Count < index.Columns.Count);
                range = new EncodedIndexRange(lower, upper);
                break;
            default:
                throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, $"Index query kind '{criteria.Kind}' is not supported.", new JetErrorInfo { TableName = tableName, IndexName = index.Name, PageNumber = index.FirstDp });
        }

        await foreach ((long DataPage, int RowIndex) location in this.EnumerateRowLocationsInRangeAsync(index.FirstDp, range, cancellationToken).ConfigureAwait(false))
        {
            yield return location;
        }
    }

    /// <summary>Streams an encoded range without reading subsequent leaves until requested.</summary>
    /// <param name="rootPageNumber">The B-tree root.</param>
    /// <param name="range">The encoded range.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async IAsyncEnumerable<(long DataPage, int RowIndex)> EnumerateRowLocationsInRangeAsync(
        long rootPageNumber,
        EncodedIndexRange range,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!range.Lower.IsUnbounded && !range.Upper.IsUnbounded)
        {
            int comparison = IndexHelpers.CompareKeyBytes(range.Lower.Key!, range.Upper.Key!);
            if (comparison > 0 || (comparison == 0 && (!range.Lower.Inclusive || !range.Upper.Inclusive)))
            {
                yield break;
            }
        }

        (long PageNumber, byte[] Page)? candidate = await this.FindCandidateLeafWithNumberAsync(rootPageNumber, range.Lower.Key ?? range.RequiredPrefix ?? [], cancellationToken).ConfigureAwait(false);
        if (candidate == null)
        {
            yield break;
        }

        byte[]? page = candidate.Value.Page;
        var matches = new List<(long DataPage, int RowIndex)>();
        var visited = new HashSet<long> { candidate.Value.PageNumber };
        while (page != null)
        {
            cancellationToken.ThrowIfCancellationRequested();
            matches.Clear();
            bool continueToNext = IndexPageCodec.CollectRangeLeafEntries(this.layout, page, this.pageSize, in range, matches);
            foreach ((long DataPage, int RowIndex) location in matches)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return location;
            }

            long next = IndexPageCodec.ReadNextPage(this.layout, page);
            if (!continueToNext || next <= 0)
            {
                yield break;
            }

            RegisterLeafTraversal(visited, next);
            page = await this.ReadPageAsync(next, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns the last entry of the B-tree rooted at
    /// <paramref name="rootPageNumber"/>, the one with the largest key, or
    /// <see langword="null"/> when the tree has no entries or cannot be walked
    /// (a page that is not an index page, a missing child, a leaf-chain cycle, or more
    /// than <c>MaxDepth</c> levels). It descends through each intermediate
    /// page's last entry (its <c>tail_page</c> when it has none), then follows
    /// the leaf level's <c>next_page</c> chain to its end, because a tail leaf
    /// appended after the rightmost summary is reachable only that way (§4.5),
    /// and steps back through <c>prev_page</c> over empty leaves.
    /// </summary>
    /// <param name="rootPageNumber">The root page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetCorruptDataException">The intermediate branch contains a cycle.</exception>
    public async ValueTask<IndexEntry?> TryReadLastEntryAsync(long rootPageNumber, CancellationToken cancellationToken)
    {
        if (rootPageNumber <= 0 || this.pageSize <= this.layout.FirstEntryOffset)
        {
            return null;
        }

        long pageNumber = rootPageNumber;
        HashSet<long>? descentVisited = null;
        byte[] page = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        for (int depth = 0; !IndexPageCodec.IsLeaf(page); depth++)
        {
            if (depth >= MaxDepth || !IndexPageCodec.IsIntermediate(page))
            {
                return null;
            }

            List<DecodedIntermediateEntry> summaries = IndexPageCodec.DecodeIntermediateEntries(this.layout, page, this.pageSize);
            long nextPageNumber = summaries.Count > 0 ? summaries[^1].ChildPage : IndexPageCodec.ReadTailPage(this.layout, page);
            if (nextPageNumber <= 0)
            {
                return null;
            }

            cancellationToken.ThrowIfCancellationRequested();
            descentVisited ??= [pageNumber];
            RegisterIntermediateTraversal(descentVisited, nextPageNumber);
            pageNumber = nextPageNumber;
            page = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        }

        var visited = new HashSet<long> { pageNumber };
        for (long next = IndexPageCodec.ReadNextPage(this.layout, page); next > 0; next = IndexPageCodec.ReadNextPage(this.layout, page))
        {
            if (!visited.Add(next))
            {
                return null;
            }

            cancellationToken.ThrowIfCancellationRequested();
            page = await this.ReadPageAsync(next, cancellationToken).ConfigureAwait(false);
            if (!IndexPageCodec.IsLeaf(page))
            {
                return null;
            }
        }

        var visitedBack = new HashSet<long>();
        while (true)
        {
            List<IndexEntry> entries = IndexPageCodec.DecodeLeafEntries(this.layout, page, this.pageSize);
            if (entries.Count > 0)
            {
                return entries[^1];
            }

            long previous = IndexPageCodec.ReadPrevPage(this.layout, page);
            if (previous <= 0 || !visitedBack.Add(previous))
            {
                return null;
            }

            cancellationToken.ThrowIfCancellationRequested();
            page = await this.ReadPageAsync(previous, cancellationToken).ConfigureAwait(false);
            if (!IndexPageCodec.IsLeaf(page))
            {
                return null;
            }
        }
    }

    private static void RegisterIntermediateTraversal(HashSet<long> visited, long pageNumber)
    {
        if (!visited.Add(pageNumber))
        {
            throw new JetCorruptDataException(
                JetErrorCode.CorruptIndex,
                "The index intermediate branch contains a cycle.",
                new JetErrorInfo { PageNumber = pageNumber });
        }
    }

    private static void RegisterLeafTraversal(HashSet<long> visited, long pageNumber)
    {
        if (visited.Count >= MaxLeafHops || !visited.Add(pageNumber))
        {
            throw new JetCorruptDataException(
                JetErrorCode.CorruptIndex,
                "The index leaf chain contains a cycle or exceeds the traversal limit.",
                new JetErrorInfo { PageNumber = pageNumber });
        }
    }

    private async ValueTask<(long PageNumber, byte[] Page)?> FindCandidateLeafWithNumberAsync(
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        if (rootPageNumber <= 0 || this.pageSize <= this.layout.FirstEntryOffset)
        {
            return null;
        }

        long currentPageNumber = rootPageNumber;
        HashSet<long>? visited = null;
        for (int depth = 0; depth < MaxDepth; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageAsync(currentPageNumber, cancellationToken).ConfigureAwait(false);
            if (IndexPageCodec.IsLeaf(page))
            {
                return (currentPageNumber, page);
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

            visited ??= [currentPageNumber];
            RegisterIntermediateTraversal(visited, nextPageNumber);

            currentPageNumber = nextPageNumber;
        }

        return null;
    }

    private async ValueTask<bool> ContainsInLeafChainAsync(
        long leafPageNumber,
        byte[] leafPage,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        var visited = new HashSet<long> { leafPageNumber };
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

            RegisterLeafTraversal(visited, nextPageNumber);
            page = await this.ReadPageAsync(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }

        return false;
    }

    private async ValueTask CollectLeafChainAsync(
        long leafPageNumber,
        byte[] leafPage,
        byte[] searchKey,
        List<(long DataPage, int RowIndex)> matches,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        var visited = new HashSet<long> { leafPageNumber };
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

            RegisterLeafTraversal(visited, nextPageNumber);
            page = await this.ReadPageAsync(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask CollectRangeLeafChainAsync(
        long leafPageNumber,
        byte[] leafPage,
        EncodedIndexRange range,
        List<(long DataPage, int RowIndex)> matches,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        var visited = new HashSet<long> { leafPageNumber };
        while (page != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            bool continueToNext = IndexPageCodec.CollectRangeLeafEntries(
                this.layout,
                page,
                this.pageSize,
                in range,
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

            RegisterLeafTraversal(visited, nextPageNumber);
            page = await this.ReadPageAsync(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }
    }
}
