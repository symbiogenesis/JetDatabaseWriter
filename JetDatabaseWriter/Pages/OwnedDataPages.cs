namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Finds the data pages a table owns and visits their live rows. A table's
/// owned pages come from its owned-pages usage map when that map validates
/// against the TDEF's row count, and otherwise from a whole-file pass that
/// maps every data page to its owner. The read-only reader memoizes the
/// whole-file pass and each table's owned pages, whichever of the two gave
/// them, so it reads and validates a table's map once, even a map it rejects;
/// the writer, whose pages change, never caches, and the constructor refuses
/// a caching instance over a <see cref="Pager"/>. Row visits follow overflow
/// pointers to the moved row bytes.
/// </summary>
internal sealed class OwnedDataPages : IDisposable
{
    private readonly IPageSource pages;
    private readonly JetFormat format;
    private readonly bool cacheResults;
    private readonly AsyncLazyInitializer<Dictionary<long, long[]>> ownedDataPageIndex;
#if NET9_0_OR_GREATER
    private readonly Lock ownedDataPagesCacheLock = new();
#else
    private readonly object ownedDataPagesCacheLock = new();
#endif
    private readonly Dictionary<long, long[]> ownedDataPagesByTdef = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="OwnedDataPages"/> class.
    /// </summary>
    /// <param name="pages">The page source the usage maps and data pages are read from.</param>
    /// <param name="format">The file's format profile.</param>
    /// <param name="cacheResults">
    /// <see langword="true"/> to memoize each table's owned data pages and the
    /// whole-file owner index; only safe when nothing writes through
    /// <paramref name="pages"/> (the reader).
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="cacheResults"/> is <see langword="true"/> and <paramref name="pages"/> is the writer's <see cref="Pager"/>.</exception>
    internal OwnedDataPages(IPageSource pages, JetFormat format, bool cacheResults)
    {
        if (cacheResults && pages is Pager)
        {
            throw new ArgumentException(
                "Owned data pages cannot be cached over the writer's file: its pages change.",
                nameof(cacheResults));
        }

        this.pages = pages;
        this.format = format;
        this.cacheResults = cacheResults;
        this.ownedDataPageIndex = new(this.BuildOwnedDataPageIndexAsync);
    }

    /// <summary>Visits one row; returns <see langword="false"/> to stop the walk.</summary>
    /// <param name="row">The row and the page that holds its bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="false"/> to stop.</returns>
    internal delegate ValueTask<bool> TableRowVisitor(TableRow row, CancellationToken cancellationToken);

    /// <summary>Visits one data page; returns <see langword="false"/> to stop the walk.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes, valid only during the call.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="false"/> to stop.</returns>
    internal delegate ValueTask<bool> DataPageVisitor(long pageNumber, byte[] page, CancellationToken cancellationToken);

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (this.ownedDataPagesCacheLock)
        {
            this.ownedDataPagesByTdef.Clear();
        }

        this.ownedDataPageIndex.Dispose();
    }

    /// <summary>
    /// Returns the data pages the table rooted at <paramref name="tdefPage"/>
    /// owns, in ascending order: from its owned-pages usage map when the map
    /// validates, and otherwise from the whole-file owner index. A caching
    /// instance remembers the answer either way.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The owned data pages; empty for a non-positive page number.</returns>
    internal async ValueTask<IReadOnlyList<long>> GetOwnedDataPagesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        if (tdefPage <= 0)
        {
            return [];
        }

        bool canUseCache = this.cacheResults;
        if (canUseCache && this.TryGetCachedOwnedDataPages(tdefPage, out long[] cachedPages))
        {
            return cachedPages;
        }

        long[]? mappedPages = await this.TryGetOwnedDataPagesFromUsageMapAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (mappedPages is not null)
        {
            if (canUseCache)
            {
                this.CacheOwnedDataPages(tdefPage, mappedPages);
            }

            return mappedPages;
        }

        Dictionary<long, long[]> pageIndex = canUseCache
            ? await this.ownedDataPageIndex.GetAsync(cancellationToken).ConfigureAwait(false)
            : await this.BuildOwnedDataPageIndexAsync(cancellationToken).ConfigureAwait(false);
        long[] indexedPages = pageIndex.TryGetValue(tdefPage, out long[]? pageNumbers)
            ? pageNumbers
            : [];
        if (canUseCache)
        {
            // The map was rejected: remember the index's answer too, so the
            // next call does not read and validate the map again.
            this.CacheOwnedDataPages(tdefPage, indexedPages);
        }

        return indexedPages;
    }

    /// <summary>
    /// Visits every live row of the table rooted at <paramref name="tdefPage"/>, in
    /// page and slot order, including overflow rows (see <see cref="ForEachRowOnPageAsync"/>).
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="visitRowAsync">Called for each row; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the walk ends.</returns>
    internal async ValueTask ForEachLiveTableRowAsync(
        long tdefPage,
        TableRowVisitor visitRowAsync,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(visitRowAsync, nameof(visitRowAsync));

        await this.ForEachOwnedDataPageAsync(
            tdefPage,
            (pageNumber, page, token) => this.ForEachRowOnPageAsync(pageNumber, page, visitRowAsync, token),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Visits every live row of the data page <paramref name="pageNumber"/> in slot
    /// order. An overflow row is visited at its header slot: its location keeps the
    /// header as <see cref="RowLocation.PageNumber"/> / <see cref="RowLocation.RowIndex"/>
    /// and names the moved bytes through <see cref="RowLocation.DataPageNumber"/> /
    /// <see cref="RowLocation.DataRowIndex"/>, and <see cref="TableRow.Page"/> holds the
    /// page with those bytes. An overflow pointer that cannot be resolved skips the
    /// row, as an undecodable row is skipped. Visitors must not keep
    /// <see cref="TableRow.Page"/> after they return.
    /// </summary>
    /// <param name="pageNumber">The data page's number.</param>
    /// <param name="page">The data page's bytes.</param>
    /// <param name="visitRowAsync">Called for each row; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="false"/> when the visitor stopped the walk.</returns>
    internal async ValueTask<bool> ForEachRowOnPageAsync(
        long pageNumber,
        byte[] page,
        TableRowVisitor visitRowAsync,
        CancellationToken cancellationToken)
    {
        foreach (RowBound entry in DataPageRows.ComputeRowDirectory(this.format, page))
        {
            if (!entry.IsOverflowPointer)
            {
                var location = new RowLocation(pageNumber, entry.RowIndex, entry.RowStart, entry.RowSize);
                if (!await visitRowAsync(new TableRow(page, location), cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }

                continue;
            }

            OverflowRowTarget? resolved = await this.TryResolveOverflowRowAsync(
                page,
                entry,
                this.pages.ReadPageAsync,
                PageBuffers.Return,
                cancellationToken).ConfigureAwait(false);
            if (resolved is not { } target)
            {
                continue;
            }

            try
            {
                var location = new RowLocation(pageNumber, entry.RowIndex, target.Bound.RowStart, target.Bound.RowSize)
                {
                    DataPageNumber = target.PageNumber,
                    DataRowIndex = target.RowIndex,
                };
                if (!await visitRowAsync(new TableRow(target.Page, location), cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }
            }
            finally
            {
                PageBuffers.Return(target.Page);
            }
        }

        return true;
    }

    /// <summary>
    /// Follows the overflow pointer held in <paramref name="header"/> (a row-directory
    /// entry with <see cref="RowBound.IsOverflowPointer"/> set) to the row data. The
    /// pointer is the target row index (1 byte) followed by the target page number
    /// (3 bytes, little-endian); when the target slot is itself flagged overflow, the
    /// walk continues from it, up to <see cref="Constants.DataPage.MaxOverflowHops"/>
    /// hops (Jackcess <c>TableImpl.positionAtRowData</c>). The target slot's deleted
    /// flag is ignored, because Access always flags the moved bytes deleted. Returns
    /// <see langword="null"/>, without throwing, when the pointer is shorter than four
    /// bytes, names a page outside the file, a page that is not a data page of the
    /// same table, or a slot the page does not have, or when the walk runs out of hops.
    /// </summary>
    /// <param name="headerPage">The data page holding the header slot.</param>
    /// <param name="header">The header slot's directory entry.</param>
    /// <param name="readPage">Reads a page; the walk reads each target page through it.</param>
    /// <param name="returnPage">
    /// Releases a page <paramref name="readPage"/> returned that the walk no longer needs,
    /// or <see langword="null"/> when its pages are not pooled (a page cache). The page of
    /// the returned target is not released: the caller owns it.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The target slot and its page, or <see langword="null"/>.</returns>
    internal async ValueTask<OverflowRowTarget?> TryResolveOverflowRowAsync(
        byte[] headerPage,
        RowBound header,
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        Action<byte[]>? returnPage,
        CancellationToken cancellationToken)
    {
        int owner = Ri32(headerPage, this.format.DataPage.TDefOff);
        long totalPages = this.pages.PageCount;
        byte[] page = headerPage;
        RowBound pointer = header;
        try
        {
            for (int hop = 0; hop < Constants.DataPage.MaxOverflowHops; hop++)
            {
                if (pointer.RowSize < Constants.DataPage.OverflowPointerSize)
                {
                    return null;
                }

                int targetRow = page[pointer.RowStart];
                long targetPageNumber = page[pointer.RowStart + 1]
                    | (page[pointer.RowStart + 2] << 8)
                    | (page[pointer.RowStart + 3] << 16);
                if (targetPageNumber <= 0 || targetPageNumber >= totalPages)
                {
                    return null;
                }

                byte[] target = await readPage(targetPageNumber, cancellationToken).ConfigureAwait(false);
                ReleaseIntermediate(page);
                page = target;

                if (target[0] != Constants.PageTypes.Data
                    || Ri32(target, this.format.DataPage.TDefOff) != owner
                    || !DataPageRows.TryGetSlotBound(this.format, target, targetRow, out RowBound bound))
                {
                    return null;
                }

                int raw = Ru16(target, this.format.DataPage.RowsStart + (targetRow * 2));
                if ((raw & Constants.DataPage.OverflowRowFlag) == 0)
                {
                    page = headerPage;
                    return new OverflowRowTarget(targetPageNumber, targetRow, target, bound);
                }

                pointer = bound;
            }

            return null;
        }
        finally
        {
            ReleaseIntermediate(page);
        }

        void ReleaseIntermediate(byte[] buffer)
        {
            if (!ReferenceEquals(buffer, headerPage))
            {
                returnPage?.Invoke(buffer);
            }
        }
    }

    /// <summary>
    /// Reads each data page the table rooted at <paramref name="tdefPage"/> owns
    /// and passes it to <paramref name="visitPageAsync"/>, skipping a page that
    /// is not a data page of that table.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="visitPageAsync">Called for each data page; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the walk ends.</returns>
    internal async ValueTask ForEachOwnedDataPageAsync(
        long tdefPage,
        DataPageVisitor visitPageAsync,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(visitPageAsync, nameof(visitPageAsync));

        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, this.format.DataPage.TDefOff) != tdefPage)
                {
                    continue;
                }

                if (!await visitPageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false))
                {
                    return;
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }
    }

    private bool TryGetCachedOwnedDataPages(long tdefPage, out long[] pageNumbers)
    {
        lock (this.ownedDataPagesCacheLock)
        {
            bool found = this.ownedDataPagesByTdef.TryGetValue(tdefPage, out long[]? cachedPages);
            pageNumbers = cachedPages ?? [];
            return found;
        }
    }

    private void CacheOwnedDataPages(long tdefPage, long[] pageNumbers)
    {
        lock (this.ownedDataPagesCacheLock)
        {
            this.ownedDataPagesByTdef[tdefPage] = pageNumbers;
        }
    }

    private async ValueTask<long[]?> TryGetOwnedDataPagesFromUsageMapAsync(long tdefPage, CancellationToken cancellationToken)
    {
        // Journal-aware: a table created or grown inside a transaction owns
        // pages appended past the physical end of the file.
        long totalPages = this.pages.PageCount;
        if (tdefPage <= 0 || tdefPage >= totalPages)
        {
            return null;
        }

        byte[] tdef = await this.pages.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (tdef[0] != Constants.PageTypes.TableDefinition
                || !UsageMap.TryReadPointer(tdef, this.format.TDef.UsedPages, out UsageMapPointer pointer)
                || pointer.PageNumber <= 0)
            {
                return null;
            }

            uint declaredRows = Ru32(tdef, this.format.TDef.NumRows);
            return await this.TryReadMappedOwnedDataPagesAsync(
                tdefPage,
                pointer.PageNumber,
                pointer.RowIndex,
                declaredRows,
                totalPages,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(tdef);
        }
    }

    private async ValueTask<long[]?> TryReadMappedOwnedDataPagesAsync(
        long tdefPage,
        int usageMapPageNumber,
        int usageMapRow,
        uint declaredRows,
        long totalPages,
        CancellationToken cancellationToken)
    {
        if (usageMapPageNumber <= 0 || usageMapPageNumber >= totalPages)
        {
            return null;
        }

        byte[] usageMapPage = await this.pages.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (usageMapPage[0] != Constants.PageTypes.Data
                || !UsageMap.TryGetRowBound(usageMapPage, this.format.DataPage, this.format.PageSize, usageMapRow, out RowBound rowBound))
            {
                return null;
            }

            var mappedPages = new List<long>();
            bool recognizedMap = await UsageMap.TryEnumeratePagesAsync(
                usageMapPage,
                rowBound,
                this.format.PageSize,
                totalPages,
                minimumPageNumber: 1,
                strict: true,
                this.pages.ReadPageAsync,
                PageBuffers.Return,
                mappedPages,
                cancellationToken).ConfigureAwait(false);
            if (!recognizedMap)
            {
                return null;
            }

            if (mappedPages.Count == 0)
            {
                return declaredRows == 0 ? [] : null;
            }

            return await this.ValidateOwnedDataPagesAsync(tdefPage, mappedPages, declaredRows, cancellationToken).ConfigureAwait(false)
                ? [.. mappedPages]
                : null;
        }
        finally
        {
            PageBuffers.Return(usageMapPage);
        }
    }

    private async ValueTask<bool> ValidateOwnedDataPagesAsync(
        long tdefPage,
        List<long> pageNumbers,
        uint declaredRows,
        CancellationToken cancellationToken)
    {
        long liveRows = 0;
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, this.format.DataPage.TDefOff) != tdefPage)
                {
                    return false;
                }

                if (declaredRows > 0)
                {
                    // Each overflow header counts once, as Access counts the row in num_rows.
                    liveRows += DataPageRows.ComputeRowDirectory(this.format, page).Length;
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return declaredRows == 0 || liveRows >= declaredRows;
    }

    private async ValueTask<Dictionary<long, long[]>> BuildOwnedDataPageIndexAsync(CancellationToken cancellationToken)
    {
        var pagesByOwner = new Dictionary<long, List<long>>();
        long totalPages = this.pages.PageCount;

        for (long pageNumber = 3; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.pages.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data)
                {
                    continue;
                }

                long owner = Ri32(page, this.format.DataPage.TDefOff);
                if (owner <= 0)
                {
                    continue;
                }

                if (!pagesByOwner.TryGetValue(owner, out List<long>? ownedPages))
                {
                    ownedPages = [];
                    pagesByOwner.Add(owner, ownedPages);
                }

                ownedPages.Add(pageNumber);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        var result = new Dictionary<long, long[]>(pagesByOwner.Count);
        foreach ((long owner, List<long>? ownedPages) in pagesByOwner)
        {
            result.Add(owner, [.. ownedPages]);
        }

        return result;
    }

    /// <summary>One live row: the page that holds its bytes, and its location.</summary>
    /// <param name="Page">The page holding the row's bytes (for an overflow row, the target page).</param>
    /// <param name="Location">The row's location.</param>
    internal readonly record struct TableRow(byte[] Page, RowLocation Location);
}
