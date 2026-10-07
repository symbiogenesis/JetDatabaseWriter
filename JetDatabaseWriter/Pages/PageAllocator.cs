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
    private List<long>? cachedFreePages;
    private long freePageGeneration = -1;

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
            await this.PreflightAllocationRangeAsync(reusableStart, pageCount, appending: false, cancellationToken).ConfigureAwait(false);
            int marked = 0;
            try
            {
                for (; marked < pageCount; marked++)
                {
                    await this.SetPageFreeStateAsync(reusableStart + marked, free: false, cancellationToken).ConfigureAwait(false);
                    _ = await pager.ReserveZeroedPageAsync(cancellationToken, reusableStart + marked).ConfigureAwait(false);
                }
            }
            catch
            {
                // The page whose update threw may or may not be marked; the
                // run was free a moment ago, so marking it free again is safe.
                await this.ReleaseReservedPagesAsync(reusableStart, marked).ConfigureAwait(false);
                await this.RestoreFreeStateAsync(reusableStart + marked, 1).ConfigureAwait(false);
                throw;
            }

            return reusableStart;
        }

        await this.PreflightAllocationRangeAsync(pager.PageCount, pageCount, appending: true, cancellationToken).ConfigureAwait(false);
        long firstAppendedPage = -1;
        int appended = 0;
        try
        {
            for (int offset = 0; offset < pageCount; offset++)
            {
                long appendedPage = await pager.ReserveZeroedPageAsync(cancellationToken).ConfigureAwait(false);
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

            // Reserve the complete contiguous run before global-map growth appends bitmap pages.
            for (int offset = 0; offset < appended; offset++)
            {
                await this.SetPageFreeStateAsync(firstAppendedPage + offset, free: false, cancellationToken).ConfigureAwait(false);
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

        await this.PreflightGlobalMapAsync(cancellationToken, pageNumber).ConfigureAwait(false);
        await this.PreflightAllocationRangeAsync(pageNumber, 1, appending: false, cancellationToken).ConfigureAwait(false);
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
        await this.PreflightGlobalMapAsync(cancellationToken).ConfigureAwait(false);
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
        await this.PreflightGlobalMapAsync(cancellationToken).ConfigureAwait(false);
        if (this.cachedFreePages is not null && this.freePageGeneration == pager.InvalidationGeneration)
        {
            return [.. this.cachedFreePages];
        }

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
                PageBuffers.Return,
                mappedFreePages,
                cancellationToken).ConfigureAwait(false);
            if (!recognizedMap)
            {
                return [];
            }

            List<long> reusable = await this.FilterPhysicallyReusablePagesAsync(mappedFreePages, cancellationToken).ConfigureAwait(false);
            this.cachedFreePages = reusable;
            this.freePageGeneration = pager.InvalidationGeneration;
            return [.. reusable];
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
        try
        {
            await this.SetPageFreeStateCoreAsync(pageNumber, free, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            this.cachedFreePages = null;
            throw;
        }

        if (this.cachedFreePages is not null)
        {
            _ = this.cachedFreePages.Remove(pageNumber);
            if (free)
            {
                this.cachedFreePages.Add(pageNumber);
                this.cachedFreePages.Sort();
            }
        }
    }

    private async ValueTask SetPageFreeStateCoreAsync(long pageNumber, bool free, CancellationToken cancellationToken)
    {
        byte[] globalPage = await this.ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(globalPage, format.DataPage, format.PageSize, out RowBound rowBound))
            {
                throw new InvalidDataException("The global usage map has invalid row bounds.");
            }

            byte mapType = globalPage[rowBound.RowStart];
            if (mapType == Constants.UsageMap.InlineMapType)
            {
                if (UsageMap.TrySetInlinePageState(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free))
                {
                    await pager.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);
                    return;
                }

                await this.PromoteInlineToReferenceAsync(globalPage, rowBound, cancellationToken).ConfigureAwait(false);
                await this.SetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free, cancellationToken).ConfigureAwait(false);
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
        if (Ri32(globalPage, rowBound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset) != 0)
        {
            throw new InvalidDataException("The global inline usage map must be anchored at page zero.");
        }

        // Native global maps consider uncovered pages free. Copy every existing bit,
        // then leave only the newly covered range free until its allocations are marked.
        byte[] bitmap = this.CreateGlobalReferenceBitmap();
        Buffer.BlockCopy(
            globalPage,
            rowBound.RowStart + Constants.UsageMap.InlineMapHeaderSize,
            bitmap,
            Constants.UsageMap.ReferenceMapBitmapOffset,
            rowBound.RowSize - Constants.UsageMap.InlineMapHeaderSize);
        long bitmapPage = await pager.AppendPageAsync(bitmap, cancellationToken).ConfigureAwait(false);
        Array.Clear(globalPage, rowBound.RowStart, rowBound.RowSize);
        globalPage[rowBound.RowStart] = Constants.UsageMap.ReferenceMapType;
        Wi32(globalPage, rowBound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset, checked((int)bitmapPage));
        await pager.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);
        await this.SetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, bitmapPage, free: false, cancellationToken).ConfigureAwait(false);
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
            throw new InvalidDataException("The global usage map cannot represent the requested page.");
        }

        int pointerOffset = rowStart + Constants.UsageMap.ReferenceMapPointerOffset + (pointerIndex * 4);
        int mapPageNumber = Ri32(globalPage, pointerOffset);
        if (mapPageNumber < 0 || mapPageNumber >= pager.PageCount)
        {
            throw new InvalidDataException("The global usage-map bitmap pointer is outside the database.");
        }

        byte[] mapPage;
        bool allocatedBitmap = mapPageNumber == 0;
        bool returnMapPage = false;
        if (mapPageNumber == 0)
        {
            mapPage = this.CreateGlobalReferenceBitmap();

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
                PageBuffers.Return(mapPage);
                throw new InvalidDataException("The global usage-map reference does not point to a usage-map bitmap.");
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

        if (allocatedBitmap)
        {
            await this.SetReferenceFreeStateAsync(globalPage, rowStart, rowSize, mapPageNumber, free: false, cancellationToken).ConfigureAwait(false);
        }
    }

    private byte[] CreateGlobalReferenceBitmap()
    {
        byte[] bitmap = new byte[format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[1] = 1;
        bitmap.AsSpan(Constants.UsageMap.ReferenceMapBitmapOffset).Fill(byte.MaxValue);
        return bitmap;
    }

    private async ValueTask<byte[]> ReadGlobalUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = await pager.ReadPageAsync(GlobalUsageMapPageNumber, cancellationToken).ConfigureAwait(false);
        if (!this.IsGlobalUsageMapPage(page))
        {
            PageBuffers.Return(page);
            throw new InvalidDataException("The database global usage map is malformed.");
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

    private async ValueTask PreflightGlobalMapAsync(CancellationToken cancellationToken, long? pageToFree = null)
    {
        byte[] page = await this.ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(page, format.DataPage, format.PageSize, out RowBound row))
            {
                throw new InvalidDataException("The global usage map has invalid row bounds.");
            }

            if (page[row.RowStart] == Constants.UsageMap.InlineMapType)
            {
                if (Ri32(page, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset) != 0)
                {
                    throw new InvalidDataException("The global inline usage map must be anchored at page zero.");
                }

                return;
            }

            var pointers = new HashSet<int>();
            int count = UsageMap.ReferencePointerCount(row.RowSize);
            for (int index = 0; index < count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int pointer = Ri32(page, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (index * 4));
                if (pointer == 0)
                {
                    continue;
                }

                if (pointer <= GlobalUsageMapPageNumber || pointer >= pager.PageCount || !pointers.Add(pointer) || pointer == pageToFree)
                {
                    throw new InvalidDataException("The global usage map has an invalid, repeated or self-referencing bitmap pointer.");
                }

                byte[] bitmap = await pager.ReadPageAsync(pointer, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (bitmap[0] != Constants.PageTypes.UsageMap || bitmap[1] != 1)
                    {
                        throw new InvalidDataException("The global usage-map reference does not point to a usage-map bitmap.");
                    }
                }
                finally
                {
                    PageBuffers.Return(bitmap);
                }
            }

            foreach (int pointer in pointers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int ownerWindow = UsageMap.ReferencePointerIndex(format.PageSize, pointer);
                if (ownerWindow >= count || Ri32(page, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (ownerWindow * 4)) == 0
                    || await this.TryGetReferenceFreeStateAsync(page, row.RowStart, row.RowSize, pointer, cancellationToken).ConfigureAwait(false))
                {
                    throw new InvalidDataException("A global usage-map bitmap is itself marked free or is outside allocation coverage.");
                }
            }
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private async ValueTask PreflightAllocationRangeAsync(long firstPage, int pageCount, bool appending, CancellationToken cancellationToken)
    {
        byte[] page = await this.ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(page, format.DataPage, format.PageSize, out RowBound row))
            {
                throw new InvalidDataException("The global usage map has invalid row bounds.");
            }

            long lastPage = checked(firstPage + pageCount - 1);
            bool inline = page[row.RowStart] == Constants.UsageMap.InlineMapType;
            int inlineCapacity = (row.RowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
            if (inline && lastPage < inlineCapacity)
            {
                return;
            }

            int count = UsageMap.ReferencePointerCount(row.RowSize);
            long capacity = (long)count * UsageMap.PagesPerReferenceMapPage(format.PageSize);
            if (firstPage < 0 || lastPage >= capacity)
            {
                throw new InvalidDataException("The global usage map cannot represent the requested allocation range.");
            }

            var missingWindows = new HashSet<int>();
            if (inline)
            {
                missingWindows.Add(0);
            }

            int firstWindow = UsageMap.ReferencePointerIndex(format.PageSize, firstPage);
            int lastWindow = UsageMap.ReferencePointerIndex(format.PageSize, lastPage);
            for (int window = firstWindow; window <= lastWindow; window++)
            {
                AddMissingWindow(window);
            }

            long firstBitmapPage = checked(pager.PageCount + (appending ? pageCount : 0));
            while (missingWindows.Count != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int previousCount = missingWindows.Count;
                long lastBitmapPage = checked(firstBitmapPage + previousCount - 1);
                if (lastBitmapPage >= capacity)
                {
                    throw new InvalidDataException("The global usage map cannot represent its additional bitmap pages.");
                }

                int firstBitmapWindow = UsageMap.ReferencePointerIndex(format.PageSize, firstBitmapPage);
                int lastBitmapWindow = UsageMap.ReferencePointerIndex(format.PageSize, lastBitmapPage);
                for (int window = firstBitmapWindow; window <= lastBitmapWindow; window++)
                {
                    AddMissingWindow(window);
                }

                if (missingWindows.Count == previousCount)
                {
                    break;
                }
            }

            void AddMissingWindow(int window)
            {
                if (inline || Ri32(page, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (window * 4)) == 0)
                {
                    missingWindows.Add(window);
                }
            }
        }
        finally
        {
            PageBuffers.Return(page);
        }
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
