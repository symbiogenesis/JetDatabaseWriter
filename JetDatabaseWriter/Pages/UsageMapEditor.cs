namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Edits the rows of a table's usage-map page for the writer: marks a page in
/// the table's owned-pages or free-space row as the table grows, and writes an
/// index's row from the pages of its rebuilt tree. A row stays INLINE, a
/// bitmap over one window of pages from its start page (512 pages in a 69-byte
/// row), while one window holds its pages, and becomes REFERENCE when none
/// does: 4-byte pointers (17 in a 69-byte row), pointer <c>i</c> naming a
/// bitmap page (page type <c>0x05</c>, then <c>01 00 00</c>, then the bitmap)
/// whose bit <c>k</c> is page <c>i * (pageSize - 4) * 8 + k</c>, the layout of
/// the REFERENCE owned-pages maps in binIdxTestV2010.accdb and
/// testEmoticonsV2010.accdb. <see cref="MarkPageAsync"/> reads and writes the
/// usage-map page itself; <see cref="WriteRowAsync"/> changes it in the
/// caller's buffer, which the caller writes. Both allocate and write the
/// bitmap pages themselves.
/// </summary>
/// <param name="format">The file's format profile.</param>
/// <param name="pager">The writer's page file, through which usage-map and bitmap pages are read and written.</param>
/// <param name="pageAllocator">Allocates new bitmap pages.</param>
internal sealed class UsageMapEditor(JetFormat format, Pager pager, PageAllocator pageAllocator)
{
    /// <summary>Clears a page from an existing INLINE or REFERENCE usage-map row without allocating pages.</summary>
    /// <param name="usageMapPageNumber">The usage-map page.</param>
    /// <param name="rowIndex">The row to edit.</param>
    /// <param name="pageNumber">The page to clear.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes after any changed map is written.</returns>
    internal async ValueTask ClearPageAsync(long usageMapPageNumber, int rowIndex, long pageNumber, CancellationToken cancellationToken)
    {
        byte[] map = await pager.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, rowIndex, out RowBound bound))
            {
                return;
            }

            if (map[bound.RowStart] == Constants.UsageMap.InlineMapType)
            {
                if (UsageMap.TrySetInlinePageState(map, bound.RowStart, bound.RowSize, pageNumber, isMarked: false))
                {
                    await pager.WritePageAsync(usageMapPageNumber, map, cancellationToken).ConfigureAwait(false);
                }

                return;
            }

            long window = pageNumber / UsageMap.PagesPerReferenceMapPage(format.PageSize);
            if (map[bound.RowStart] != Constants.UsageMap.ReferenceMapType || pageNumber < 0 || window >= UsageMap.ReferencePointerCount(bound.RowSize))
            {
                return;
            }

            int bitmapNumber = Ri32(map, bound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + checked((int)(window * 4)));
            if (bitmapNumber <= 0 || bitmapNumber >= pager.PageCount)
            {
                return;
            }

            byte[] bitmap = await pager.ReadPageAsync(bitmapNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (bitmap[0] == Constants.PageTypes.UsageMap && UsageMap.TrySetReferencePageState(bitmap, format.PageSize, pageNumber, isMarked: false))
                {
                    await pager.WritePageAsync(bitmapNumber, bitmap, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                PageBuffers.Return(bitmap);
            }
        }
        finally
        {
            PageBuffers.Return(map);
        }
    }

    /// <summary>
    /// Marks <paramref name="pageNumber"/> in each of <paramref name="rowIndexes"/>,
    /// rows of usage-map page <paramref name="usageMapPageNumber"/>, and writes
    /// that page once when a row changed. An INLINE row whose window does not
    /// hold the page moves its window to the page while it lists no page, and
    /// is otherwise promoted to REFERENCE with every page it listed. A missing
    /// row, a row of another type, and a REFERENCE pointer that names no bitmap
    /// page are left as they are.
    /// </summary>
    /// <param name="usageMapPageNumber">The usage-map page.</param>
    /// <param name="rowIndexes">The rows to mark the page in, each once.</param>
    /// <param name="pageNumber">The page to mark.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the usage-map page and any bitmap page the mark changed are written.</returns>
    internal async ValueTask MarkPageAsync(long usageMapPageNumber, IReadOnlyList<int> rowIndexes, long pageNumber, CancellationToken cancellationToken)
    {
        byte[] usageMapPage = await pager.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            bool changed = false;
            for (int i = 0; i < rowIndexes.Count; i++)
            {
                changed |= await this.AddPageAsync(usageMapPage, rowIndexes[i], pageNumber, cancellationToken).ConfigureAwait(false);
            }

            if (changed)
            {
                await pager.WritePageAsync(usageMapPageNumber, usageMapPage, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            PageBuffers.Return(usageMapPage);
        }
    }

    /// <summary>
    /// Writes the row of <paramref name="rowSize"/> bytes at
    /// <paramref name="rowStart"/> so it lists exactly
    /// <paramref name="pageNumbers"/>: INLINE when one window holds them, and
    /// REFERENCE otherwise. With <paramref name="keepReferencePages"/>, a row
    /// that is REFERENCE already stays REFERENCE and keeps its bitmap pages, so
    /// rebuilding the same index again takes no new page.
    /// </summary>
    /// <param name="usageMapPage">The usage-map page's bytes, changed in place; the caller writes them.</param>
    /// <param name="rowStart">The row's offset on the page.</param>
    /// <param name="rowSize">The row's size in bytes.</param>
    /// <param name="pageNumbers">The pages the row lists.</param>
    /// <param name="keepReferencePages">Whether the bytes at <paramref name="rowStart"/> are this row's own, so a REFERENCE row's bitmap pages may be reused.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the row and its bitmap pages are written.</returns>
    internal async ValueTask WriteRowAsync(byte[] usageMapPage, int rowStart, int rowSize, IReadOnlyList<long> pageNumbers, bool keepReferencePages, CancellationToken cancellationToken)
    {
        if (!keepReferencePages || usageMapPage[rowStart] != Constants.UsageMap.ReferenceMapType)
        {
            if (UsageMap.TryWriteInlineRow(usageMapPage, rowStart, rowSize, pageNumbers))
            {
                return;
            }

            Array.Clear(usageMapPage, rowStart, rowSize);
            usageMapPage[rowStart] = Constants.UsageMap.ReferenceMapType;
        }

        await this.WriteBitmapPagesAsync(usageMapPage, rowStart, rowSize, pageNumbers, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Marks <paramref name="pageNumber"/> in row <paramref name="rowIndex"/> of
    /// <paramref name="usageMapPage"/>, as <see cref="MarkPageAsync"/> describes.
    /// </summary>
    /// <param name="usageMapPage">The usage-map page's bytes, changed in place.</param>
    /// <param name="rowIndex">The row's index on the page.</param>
    /// <param name="pageNumber">The page to mark.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when the row changed, so <paramref name="usageMapPage"/> must be written; a bitmap page the mark changed is written already.</returns>
    private async ValueTask<bool> AddPageAsync(byte[] usageMapPage, int rowIndex, long pageNumber, CancellationToken cancellationToken)
    {
        if (!UsageMap.TryGetRowBound(usageMapPage, format.DataPage, format.PageSize, rowIndex, out RowBound rowBound))
        {
            return false;
        }

        int rowStart = rowBound.RowStart;
        int rowSize = rowBound.RowSize;
        switch (usageMapPage[rowStart])
        {
            case Constants.UsageMap.InlineMapType:
                if (UsageMap.TrySetInlinePageState(usageMapPage, rowStart, rowSize, pageNumber, isMarked: true)
                    || (UsageMap.IsInlineBitmapEmpty(usageMapPage, rowStart, rowSize) && UsageMap.TryWriteInlineRow(usageMapPage, rowStart, rowSize, [pageNumber])))
                {
                    return true;
                }

                return await this.PromoteAsync(usageMapPage, rowBound, pageNumber, cancellationToken).ConfigureAwait(false);

            case Constants.UsageMap.ReferenceMapType:
                return await this.AddReferencePageAsync(usageMapPage, rowStart, rowSize, pageNumber, cancellationToken).ConfigureAwait(false);

            default:
                return false;
        }
    }

    /// <summary>
    /// Turns the INLINE row at <paramref name="rowBound"/> into a REFERENCE
    /// row that lists the pages the INLINE row listed, less any past the end
    /// of file, and <paramref name="pageNumber"/>.
    /// </summary>
    /// <param name="usageMapPage">The usage-map page's bytes, changed in place.</param>
    /// <param name="rowBound">The row's bounds on the page.</param>
    /// <param name="pageNumber">The page being marked.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="false"/> when the row is too short to hold a pointer and is left as it is.</returns>
    private async ValueTask<bool> PromoteAsync(byte[] usageMapPage, RowBound rowBound, long pageNumber, CancellationToken cancellationToken)
    {
        if (UsageMap.ReferencePointerCount(rowBound.RowSize) == 0)
        {
            return false;
        }

        var pageNumbers = new List<long>();
        _ = UsageMap.TryEnumerateInlinePages(usageMapPage, rowBound, format.PageSize, pager.PageCount, minimumPageNumber: 1, strict: false, pageNumbers);
        pageNumbers.Add(pageNumber);

        Array.Clear(usageMapPage, rowBound.RowStart, rowBound.RowSize);
        usageMapPage[rowBound.RowStart] = Constants.UsageMap.ReferenceMapType;
        await this.WriteBitmapPagesAsync(usageMapPage, rowBound.RowStart, rowBound.RowSize, pageNumbers, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Marks <paramref name="pageNumber"/> in the bitmap page that the
    /// REFERENCE row's pointer for its window names, allocating that page
    /// when the pointer is 0.
    /// </summary>
    /// <param name="usageMapPage">The usage-map page's bytes, changed in place.</param>
    /// <param name="rowStart">The row's offset on the page.</param>
    /// <param name="rowSize">The row's size in bytes.</param>
    /// <param name="pageNumber">The page to mark.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when the row's pointer changed.</returns>
    private async ValueTask<bool> AddReferencePageAsync(byte[] usageMapPage, int rowStart, int rowSize, long pageNumber, CancellationToken cancellationToken)
    {
        long window = pageNumber / UsageMap.PagesPerReferenceMapPage(format.PageSize);
        if (pageNumber < 0 || window >= UsageMap.ReferencePointerCount(rowSize))
        {
            return false;
        }

        int pointerOffset = rowStart + Constants.UsageMap.ReferenceMapPointerOffset + checked((int)(window * 4));
        int bitmapPageNumber = Ri32(usageMapPage, pointerOffset);
        if (bitmapPageNumber == 0)
        {
            long allocated = await pageAllocator.AllocatePageAsync(this.CreateBitmapPage([pageNumber]), cancellationToken).ConfigureAwait(false);
            Wi32(usageMapPage, pointerOffset, checked((int)allocated));
            return true;
        }

        if (bitmapPageNumber < 0 || bitmapPageNumber >= pager.PageCount)
        {
            return false;
        }

        byte[] bitmapPage = await pager.ReadPageAsync(bitmapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (bitmapPage[0] == Constants.PageTypes.UsageMap
                && UsageMap.TryGetReferencePageState(bitmapPage, format.PageSize, pageNumber, out bool isMarked)
                && !isMarked)
            {
                _ = UsageMap.TrySetReferencePageState(bitmapPage, format.PageSize, pageNumber, isMarked: true);
                await pager.WritePageAsync(bitmapPageNumber, bitmapPage, cancellationToken).ConfigureAwait(false);
            }

            return false;
        }
        finally
        {
            PageBuffers.Return(bitmapPage);
        }
    }

    /// <summary>
    /// Points the REFERENCE row at <paramref name="rowStart"/> at bitmap pages
    /// that list exactly <paramref name="pageNumbers"/>. A bitmap page the row
    /// names already is rewritten when its bytes change, and kept, empty, when
    /// its window holds none of the pages; a window that holds pages and has
    /// no bitmap page gets a new one. A pointer that names anything but a
    /// bitmap page is replaced or cleared.
    /// </summary>
    /// <param name="usageMapPage">The usage-map page's bytes, changed in place.</param>
    /// <param name="rowStart">The row's offset on the page.</param>
    /// <param name="rowSize">The row's size in bytes.</param>
    /// <param name="pageNumbers">The pages the row lists.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the bitmap pages are written.</returns>
    private async ValueTask WriteBitmapPagesAsync(byte[] usageMapPage, int rowStart, int rowSize, IReadOnlyList<long> pageNumbers, CancellationToken cancellationToken)
    {
        // 17 pointers of 32,736 pages each reach page 556,511, past the 2 GB
        // (524,288 pages of 4 KB) an Access file can hold.
        int pagesPerBitmap = UsageMap.PagesPerReferenceMapPage(format.PageSize);
        var windows = new List<long>?[UsageMap.ReferencePointerCount(rowSize)];
        for (int i = 0; i < pageNumbers.Count; i++)
        {
            long window = pageNumbers[i] / pagesPerBitmap;
            if (pageNumbers[i] >= 0 && window < windows.Length)
            {
                (windows[window] ??= []).Add(pageNumbers[i]);
            }
        }

        for (int window = 0; window < windows.Length; window++)
        {
            int pointerOffset = rowStart + Constants.UsageMap.ReferenceMapPointerOffset + (window * 4);
            int bitmapPageNumber = Ri32(usageMapPage, pointerOffset);
            byte[]? current = await this.TryReadBitmapPageAsync(bitmapPageNumber, cancellationToken).ConfigureAwait(false);
            if (current is null)
            {
                int replacement = 0;
                if (windows[window] is { } windowPages)
                {
                    replacement = checked((int)await pageAllocator.AllocatePageAsync(this.CreateBitmapPage(windowPages), cancellationToken).ConfigureAwait(false));
                }

                Wi32(usageMapPage, pointerOffset, replacement);
                continue;
            }

            byte[] bitmapPage = this.CreateBitmapPage(windows[window]);
            if (!bitmapPage.AsSpan().SequenceEqual(current.AsSpan(0, format.PageSize)))
            {
                await pager.WritePageAsync(bitmapPageNumber, bitmapPage, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Returns a copy of page <paramref name="pageNumber"/> when it is a bitmap page in the file, or <see langword="null"/>.</summary>
    /// <param name="pageNumber">The page a REFERENCE pointer names.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    /// <returns>The page copy, or <see langword="null"/> for 0, a page past the end of file, or a page of another type.</returns>
    private async ValueTask<byte[]?> TryReadBitmapPageAsync(int pageNumber, CancellationToken cancellationToken)
    {
        if (pageNumber <= 0 || pageNumber >= pager.PageCount)
        {
            return null;
        }

        byte[] page = await pager.ReadPageCopyAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        return page[0] == Constants.PageTypes.UsageMap ? page : null;
    }

    /// <summary>Returns a new bitmap page that lists <paramref name="pageNumbers"/>, all in one pointer's window.</summary>
    /// <param name="pageNumbers">The pages to mark, or <see langword="null"/> for none.</param>
    /// <returns>The page bytes.</returns>
    private byte[] CreateBitmapPage(List<long>? pageNumbers)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.UsageMap;
        page[1] = 0x01;
        if (pageNumbers is not null)
        {
            foreach (long pageNumber in pageNumbers)
            {
                _ = UsageMap.TrySetReferencePageState(page, format.PageSize, pageNumber, isMarked: true);
            }
        }

        return page;
    }
}
