namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Owns the Access global page-allocation map on page 1 and exposes page
/// reserve, free, scrub, and tail-shrink operations for the writer.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pager">The writer's page file, through which pages are written, appended and truncated.</param>
/// <param name="options">The writer options; supplies the secure-erase policy for freed pages.</param>
internal sealed class PageAllocator(JetFormat format, Pager pager, AccessWriterOptions options)
{
    private const int GlobalUsageMapPageNumber = 1;

    internal async ValueTask<long> AllocatePageAsync(byte[] page, CancellationToken cancellationToken)
    {
        long pageNumber = await this.ReserveContiguousPagesAsync(1, cancellationToken).ConfigureAwait(false);
        try
        {
            await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await this.ReleaseReservedPagesAsync(pageNumber, 1).ConfigureAwait(false);
            throw;
        }

        return pageNumber;
    }

    /// <summary>
    /// Reserves <paramref name="pageCount"/> contiguous pages, reusing the
    /// first free run the global usage map lists or appending blank pages at
    /// the end of the file. If the reservation fails partway, the pages it has
    /// already taken are given back before the exception propagates.
    /// </summary>
    /// <param name="pageCount">The number of pages to reserve.</param>
    /// <param name="cancellationToken">A token used to cancel the reservation.</param>
    /// <returns>The first page of the reserved run.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="pageCount"/> is not positive.</exception>
    /// <exception cref="IOException">Thrown when appended pages do not land contiguously.</exception>
    internal async ValueTask<long> ReserveContiguousPagesAsync(int pageCount, CancellationToken cancellationToken)
    {
        if (pageCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(pageCount), "Page count must be positive.");
        }

        List<long> freePages = await this.EnumerateMappedFreePagesAsync(cancellationToken).ConfigureAwait(false);
        long reusableStart = FindContiguousRun(freePages, pageCount);
        if (reusableStart > 0)
        {
            int marked = 0;
            try
            {
                for (; marked < pageCount; marked++)
                {
                    await this.SetPageFreeStateAsync(reusableStart + marked, free: false, cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                // The page whose update threw may or may not be marked; the
                // run was free a moment ago, so marking it free again is safe.
                await this.RestoreFreeStateAsync(reusableStart, Math.Min(marked + 1, pageCount)).ConfigureAwait(false);
                throw;
            }

            return reusableStart;
        }

        byte[] blankPage = new byte[format.PageSize];
        long firstAppendedPage = -1;
        int appended = 0;
        try
        {
            for (int offset = 0; offset < pageCount; offset++)
            {
                long appendedPage = await pager.AppendPageAsync(blankPage, cancellationToken).ConfigureAwait(false);
                if (offset == 0)
                {
                    firstAppendedPage = appendedPage;
                }
                else if (appendedPage != firstAppendedPage + offset)
                {
                    await this.ReleaseReservedPagesAsync(appendedPage, 1).ConfigureAwait(false);
                    throw new IOException("Contiguous append reservation was interrupted by a non-contiguous page assignment.");
                }

                appended++;
            }
        }
        catch
        {
            if (appended > 0)
            {
                await this.ReleaseReservedPagesAsync(firstAppendedPage, appended).ConfigureAwait(false);
            }

            throw;
        }

        return firstAppendedPage;
    }

    /// <summary>
    /// Gives back pages a caller reserved but never linked into the file, the
    /// way <see cref="DeallocatePageAsync"/> frees one page: each is stamped
    /// freed and marked free in the global usage map. It runs on failure paths,
    /// so it ignores cancellation and never throws: an I/O failure, a disposed
    /// file or an exhausted transaction page budget stops it quietly and leaves
    /// the remaining pages allocated, as they were before.
    /// </summary>
    /// <param name="firstPage">The first page of the run.</param>
    /// <param name="pageCount">The number of pages in the run.</param>
    /// <returns>A task that completes when the run has been released or the release has given up.</returns>
    internal async ValueTask ReleaseReservedPagesAsync(long firstPage, int pageCount)
    {
        try
        {
            for (int offset = 0; offset < pageCount; offset++)
            {
                await this.DeallocatePageAsync(firstPage + offset, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (IsBestEffortReleaseFailure(ex))
        {
            // Best effort: the rest of the run stays allocated.
        }
    }

    internal async ValueTask DeallocatePageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        if (pageNumber <= GlobalUsageMapPageNumber)
        {
            return;
        }

        bool secure = options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages;
        await this.WriteFreedPageAsync(pageNumber, secure, cancellationToken).ConfigureAwait(false);
        await this.SetPageFreeStateAsync(pageNumber, free: true, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<int> ScrubFreePagesAsync(CancellationToken cancellationToken)
    {
        var freePages = new SortedSet<long>(await this.EnumerateMappedFreePagesAsync(cancellationToken).ConfigureAwait(false));
        long totalPages = pager.PageCount;
        for (long pageNumber = 2; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] == Constants.PageTypes.Freed)
                {
                    _ = freePages.Add(pageNumber);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        int scrubbed = 0;
        foreach (long pageNumber in freePages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (pageNumber <= GlobalUsageMapPageNumber || pageNumber >= totalPages)
            {
                continue;
            }

            await this.WriteFreedPageAsync(pageNumber, secure: true, cancellationToken).ConfigureAwait(false);
            await this.SetPageFreeStateAsync(pageNumber, free: true, cancellationToken).ConfigureAwait(false);
            scrubbed++;
        }

        return scrubbed;
    }

    internal async ValueTask<long> ShrinkDatabaseAsync(CancellationToken cancellationToken)
    {
        if (pager.IsJournalActive)
        {
            throw new InvalidOperationException("ShrinkDatabaseAsync cannot run inside an active transaction.");
        }

        bool secure = options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages;
        long totalPages = pager.PageCount;
        long newTotalPages = totalPages;
        while (newTotalPages > 3)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long candidatePage = newTotalPages - 1;
            if (!await this.IsPageFreeAsync(candidatePage, cancellationToken).ConfigureAwait(false))
            {
                break;
            }

            if (secure)
            {
                await this.WriteFreedPageAsync(candidatePage, secure: true, cancellationToken).ConfigureAwait(false);
            }

            newTotalPages--;
        }

        if (newTotalPages == totalPages)
        {
            return 0;
        }

        long newLength = newTotalPages * format.PageSize;
        await pager.SetLengthAsync(newLength, cancellationToken).ConfigureAwait(false);

        return totalPages - newTotalPages;
    }

    internal async ValueTask<bool> IsPageFreeAsync(long pageNumber, CancellationToken cancellationToken)
    {
        if (pageNumber <= GlobalUsageMapPageNumber || pageNumber >= pager.PageCount)
        {
            return false;
        }

        byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return IsPhysicallyReusableFreePage(page)
                && await this.IsPageMarkedFreeAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private static bool IsPhysicallyReusableFreePage(byte[] page)
        => page[0] is Constants.PageTypes.Freed or 0x00;

    private static bool IsBestEffortReleaseFailure(Exception ex)
        => ex is IOException or InvalidDataException or ObjectDisposedException or NotSupportedException or JetLimitationException;

    private static long FindContiguousRun(List<long> freePages, int pageCount)
    {
        if (freePages.Count == 0)
        {
            return -1;
        }

        freePages.Sort();
        long runStart = freePages[0];
        long previousPage = freePages[0];
        int runLength = 1;
        if (pageCount == 1)
        {
            return runStart;
        }

        for (int freePageIndex = 1; freePageIndex < freePages.Count; freePageIndex++)
        {
            long pageNumber = freePages[freePageIndex];
            if (pageNumber == previousPage)
            {
                continue;
            }

            if (pageNumber == previousPage + 1)
            {
                runLength++;
                previousPage = pageNumber;
                if (runLength >= pageCount)
                {
                    return runStart;
                }

                continue;
            }

            runStart = pageNumber;
            previousPage = pageNumber;
            runLength = 1;
        }

        return -1;
    }

    /// <summary>
    /// Marks <paramref name="pageCount"/> pages from <paramref name="firstPage"/>
    /// free again after a reuse reservation failed partway. The pages were
    /// free and untouched, so only the global usage map changes. Best effort,
    /// like <see cref="ReleaseReservedPagesAsync"/>.
    /// </summary>
    /// <param name="firstPage">The first page of the run.</param>
    /// <param name="pageCount">The number of pages to mark free.</param>
    private async ValueTask RestoreFreeStateAsync(long firstPage, int pageCount)
    {
        try
        {
            for (int offset = 0; offset < pageCount; offset++)
            {
                await this.SetPageFreeStateAsync(firstPage + offset, free: true, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (IsBestEffortReleaseFailure(ex))
        {
            // Best effort: the rest of the run stays marked used.
        }
    }

    private async ValueTask<List<long>> EnumerateMappedFreePagesAsync(CancellationToken cancellationToken)
    {
        byte[] globalPage = await this.ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(globalPage, format.DataPage, format.PageSize, out RowBound rowBound))
            {
                return [];
            }

            var mappedFreePages = new List<long>();
            bool recognizedMap = await UsageMap.TryEnumeratePagesAsync(
                globalPage,
                rowBound,
                format.PageSize,
                pager.PageCount,
                minimumPageNumber: GlobalUsageMapPageNumber + 1,
                strict: false,
                pager.ReadPageAsync,
                ReturnPage,
                mappedFreePages,
                cancellationToken).ConfigureAwait(false);
            if (!recognizedMap)
            {
                return [];
            }

            return await this.FilterPhysicallyReusablePagesAsync(mappedFreePages, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(globalPage);
        }
    }

    private async ValueTask<List<long>> FilterPhysicallyReusablePagesAsync(List<long> mappedFreePages, CancellationToken cancellationToken)
    {
        if (mappedFreePages.Count == 0)
        {
            return mappedFreePages;
        }

        var reusablePages = new List<long>(mappedFreePages.Count);
        foreach (long pageNumber in mappedFreePages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (IsPhysicallyReusableFreePage(page))
                {
                    reusablePages.Add(pageNumber);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return reusablePages;
    }

    private async ValueTask<bool> IsPageMarkedFreeAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] globalPage = await this.ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(globalPage, format.DataPage, format.PageSize, out RowBound rowBound))
            {
                return false;
            }

            return globalPage[rowBound.RowStart] switch
            {
                Constants.UsageMap.InlineMapType => UsageMap.TryGetInlinePageState(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, out bool isFree) && isFree,
                Constants.UsageMap.ReferenceMapType => await this.TryGetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, cancellationToken).ConfigureAwait(false),
                _ => false,
            };
        }
        finally
        {
            PageBuffers.Return(globalPage);
        }
    }

    private async ValueTask SetPageFreeStateAsync(long pageNumber, bool free, CancellationToken cancellationToken)
    {
        byte[] globalPage = await this.ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(globalPage, format.DataPage, format.PageSize, out RowBound rowBound))
            {
                this.InitializeGlobalUsageMapPage(globalPage);
                rowBound = new RowBound(0, format.PageSize - Constants.UsageMap.RowSize, Constants.UsageMap.RowSize);
            }

            byte mapType = globalPage[rowBound.RowStart];
            if (mapType == Constants.UsageMap.InlineMapType)
            {
                if (UsageMap.TrySetInlinePageState(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free))
                {
                    await pager.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (!free)
                {
                    return;
                }

                await this.PromoteInlineToReferenceAsync(globalPage, rowBound, cancellationToken).ConfigureAwait(false);
                await this.SetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free: true, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (mapType == Constants.UsageMap.ReferenceMapType)
            {
                await this.SetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            PageBuffers.Return(globalPage);
        }
    }

    private async ValueTask PromoteInlineToReferenceAsync(byte[] globalPage, RowBound rowBound, CancellationToken cancellationToken)
    {
        var existingFreePages = new List<long>();
        _ = UsageMap.TryEnumerateInlinePages(
            globalPage,
            rowBound,
            format.PageSize,
            pager.PageCount,
            minimumPageNumber: GlobalUsageMapPageNumber + 1,
            strict: false,
            existingFreePages);
        Array.Clear(globalPage, rowBound.RowStart, rowBound.RowSize);
        globalPage[rowBound.RowStart] = Constants.UsageMap.ReferenceMapType;
        await pager.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);

        foreach (long freePageNumber in existingFreePages)
        {
            await this.SetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, freePageNumber, free: true, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<bool> TryGetReferenceFreeStateAsync(byte[] globalPage, int rowStart, int rowSize, long pageNumber, CancellationToken cancellationToken)
    {
        int pointerIndex = UsageMap.ReferencePointerIndex(format.PageSize, pageNumber);
        int pointerCount = (rowSize - Constants.UsageMap.ReferenceMapPointerOffset) / 4;
        if (pointerIndex < 0 || pointerIndex >= pointerCount)
        {
            return false;
        }

        int pointerOffset = rowStart + Constants.UsageMap.ReferenceMapPointerOffset + (pointerIndex * 4);
        int mapPageNumber = Ri32(globalPage, pointerOffset);
        if (mapPageNumber <= 0 || mapPageNumber >= pager.PageCount)
        {
            return false;
        }

        byte[] mapPage = await pager.ReadPageAsync(mapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (mapPage[0] != Constants.PageTypes.UsageMap)
            {
                return false;
            }

            return UsageMap.TryGetReferencePageState(mapPage, format.PageSize, pageNumber, out bool isFree) && isFree;
        }
        finally
        {
            PageBuffers.Return(mapPage);
        }
    }

    private async ValueTask SetReferenceFreeStateAsync(byte[] globalPage, int rowStart, int rowSize, long pageNumber, bool free, CancellationToken cancellationToken)
    {
        int pointerIndex = UsageMap.ReferencePointerIndex(format.PageSize, pageNumber);
        int pointerCount = (rowSize - Constants.UsageMap.ReferenceMapPointerOffset) / 4;
        if (pointerIndex < 0 || pointerIndex >= pointerCount)
        {
            return;
        }

        int pointerOffset = rowStart + Constants.UsageMap.ReferenceMapPointerOffset + (pointerIndex * 4);
        int mapPageNumber = Ri32(globalPage, pointerOffset);
        byte[] mapPage;
        bool returnMapPage = false;
        if (mapPageNumber <= 0)
        {
            if (!free)
            {
                return;
            }

            mapPage = new byte[format.PageSize];
            mapPage[0] = Constants.PageTypes.UsageMap;
            mapPageNumber = checked((int)await pager.AppendPageAsync(mapPage, cancellationToken).ConfigureAwait(false));
            Wi32(globalPage, pointerOffset, mapPageNumber);
            await pager.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            mapPage = await pager.ReadPageAsync(mapPageNumber, cancellationToken).ConfigureAwait(false);
            returnMapPage = true;
            if (mapPage[0] != Constants.PageTypes.UsageMap)
            {
                Array.Clear(mapPage, 0, format.PageSize);
                mapPage[0] = Constants.PageTypes.UsageMap;
            }
        }

        try
        {
            if (UsageMap.TrySetReferencePageState(mapPage, format.PageSize, pageNumber, free))
            {
                await pager.WritePageAsync(mapPageNumber, mapPage, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (returnMapPage)
            {
                PageBuffers.Return(mapPage);
            }
        }
    }

    private async ValueTask<byte[]> ReadGlobalUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = await pager.ReadPageAsync(GlobalUsageMapPageNumber, cancellationToken).ConfigureAwait(false);
        if (!this.IsGlobalUsageMapPage(page))
        {
            this.InitializeGlobalUsageMapPage(page);
            await pager.WritePageAsync(GlobalUsageMapPageNumber, page, cancellationToken).ConfigureAwait(false);
        }

        return page;
    }

    private bool IsGlobalUsageMapPage(byte[] page)
    {
        if (page.Length < format.PageSize || page[0] != Constants.PageTypes.Data || page[1] != 0x01)
        {
            return false;
        }

        if (!UsageMap.TryGetFirstRowBound(page, format.DataPage, format.PageSize, out RowBound rowBound))
        {
            return false;
        }

        return rowBound.RowSize >= Constants.UsageMap.InlineMapHeaderSize
            && page[rowBound.RowStart] is Constants.UsageMap.InlineMapType or Constants.UsageMap.ReferenceMapType;
    }

    private void InitializeGlobalUsageMapPage(byte[] page)
    {
        Array.Clear(page, 0, format.PageSize);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = format.PageSize - Constants.UsageMap.RowSize;
        int row1Start = rowStart - Constants.UsageMap.RowSize;
        int slotTableEnd = format.DataPage.RowsStart + 4;
        int freeSpace = row1Start - slotTableEnd;
        Wu16(page, 2, freeSpace);
        Wi32(page, format.DataPage.TDefOff, 1);
        Wu16(page, format.DataPage.NumRows, 2);
        Wu16(page, format.DataPage.RowsStart, rowStart);
        Wu16(page, format.DataPage.RowsStart + 2, row1Start);
        page[rowStart] = Constants.UsageMap.InlineMapType;
        Wi32(page, rowStart + 1, 0);
        page[row1Start] = Constants.UsageMap.InlineMapType;
        Wi32(page, row1Start + 1, 0);
    }

    private async ValueTask WriteFreedPageAsync(long pageNumber, bool secure, CancellationToken cancellationToken)
    {
        byte[] page;
        bool returnPage;
        if (secure)
        {
            page = new byte[format.PageSize];
            returnPage = false;
        }
        else
        {
            page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            returnPage = true;
        }

        try
        {
            page[0] = Constants.PageTypes.Freed;
            page[1] = 0x01;
            Wu16(page, 2, Math.Max(0, format.PageSize - 16));
            await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (returnPage)
            {
                PageBuffers.Return(page);
            }
        }
    }
}
