namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

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
    /// <param name="range">The encoded range.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<List<(long DataPage, int RowIndex)>> FindRowLocationsInRangeAsync(
        long rootPageNumber,
        EncodedIndexRange range,
        CancellationToken cancellationToken)
    {
        var matches = new List<(long DataPage, int RowIndex)>();
        byte[]? startKey = range.Lower.Key ?? range.RequiredPrefix ?? [];
        byte[]? leafPage = await this.FindCandidateLeafAsync(rootPageNumber, startKey, cancellationToken).ConfigureAwait(false);
        if (leafPage == null)
        {
            return matches;
        }

        await this.CollectRangeLeafChainAsync(
            leafPage,
            range,
            matches,
            cancellationToken).ConfigureAwait(false);
        return matches;
    }

    /// <summary>
    /// Resolves <paramref name="criteria"/> into data-row pointers by dispatching
    /// to the appropriate seek primitive: an unbounded range for
    /// <see cref="IndexQueryKind.All"/>, an exact-key walk for
    /// <see cref="IndexQueryKind.Exact"/>, and an encoded range for
    /// <see cref="IndexQueryKind.KeyPrefix"/> / <see cref="IndexQueryKind.Range"/>.
    /// Composite seek keys are encoded via <see cref="IndexKeyEncoder"/> using
    /// <paramref name="format"/>'s numeric encoding. Returns an empty list for a
    /// provably empty range.
    /// </summary>
    /// <param name="format">Database format; selects the legacy Jet4 vs. ACE numeric encoding.</param>
    /// <param name="tableName">Owning table name, used only in encoder exception messages.</param>
    /// <param name="index">The index being seeked; supplies the root page and key columns.</param>
    /// <param name="tableDef">Table definition supplying per-column type / scale metadata.</param>
    /// <param name="criteria">The query criteria to resolve.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="NotSupportedException">The criteria kind is not supported.</exception>
    public async ValueTask<List<(long DataPage, int RowIndex)>> FindRowLocationsForCriteriaAsync(
        DatabaseFormat format,
        string tableName,
        IndexMetadata index,
        TableDef tableDef,
        IndexQueryCriteria criteria,
        CancellationToken cancellationToken)
    {
        static bool IsEmptyRange(in EncodedIndexRange range)
        {
            if (range.Lower.IsUnbounded || range.Upper.IsUnbounded)
            {
                return false;
            }

            int comparison = IndexHelpers.CompareKeyBytes(range.Lower.Key!, range.Upper.Key!);
            return comparison > 0 || (comparison == 0 && (!range.Lower.Inclusive || !range.Upper.Inclusive));
        }

        switch (criteria.Kind)
        {
            case IndexQueryKind.All:
                return await this.FindRowLocationsInRangeAsync(
                    index.FirstDp,
                    new EncodedIndexRange(EncodedIndexBound.None, EncodedIndexBound.None),
                    cancellationToken).ConfigureAwait(false);

            case IndexQueryKind.Exact:
                byte[] searchKey = IndexKeyEncoder.EncodeIndexSeekKey(format, tableName, index, tableDef, criteria.Values!);
                return await this.FindRowLocationsAsync(index.FirstDp, searchKey, cancellationToken).ConfigureAwait(false);

            case IndexQueryKind.KeyPrefix:
                byte[] prefixKey = IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, index, tableDef, criteria.Values!, nameof(criteria));
                var prefixRange = new EncodedIndexRange(
                    new EncodedIndexBound(prefixKey, Inclusive: true, IsPrefix: false),
                    EncodedIndexBound.None,
                    prefixKey);
                return await this.FindRowLocationsInRangeAsync(
                    index.FirstDp,
                    prefixRange,
                    cancellationToken).ConfigureAwait(false);

            case IndexQueryKind.Range:
                EncodedIndexBound lowerBound = criteria.Lower is null
                    ? EncodedIndexBound.None
                    : new EncodedIndexBound(
                        IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, index, tableDef, criteria.Lower.Values, nameof(criteria)),
                        criteria.Lower.IsInclusive,
                        criteria.Lower.Values.Count < index.Columns.Count);
                EncodedIndexBound upperBound = criteria.Upper is null
                    ? EncodedIndexBound.None
                    : new EncodedIndexBound(
                        IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, index, tableDef, criteria.Upper.Values, nameof(criteria)),
                        criteria.Upper.IsInclusive,
                        criteria.Upper.Values.Count < index.Columns.Count);
                var range = new EncodedIndexRange(lowerBound, upperBound);

                if (IsEmptyRange(in range))
                {
                    return [];
                }

                return await this.FindRowLocationsInRangeAsync(
                    index.FirstDp,
                    range,
                    cancellationToken).ConfigureAwait(false);

            default:
                throw new NotSupportedException($"Index query kind '{criteria.Kind}' is not supported.");
        }
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
        EncodedIndexRange range,
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

            page = await this.readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
        }
    }
}
