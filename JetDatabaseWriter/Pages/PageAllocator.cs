namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using static JetDatabaseWriter.AccessBase;
using static JetDatabaseWriter.Schema.JetTypeInfo;

#pragma warning disable SA1204 // Static helpers stay near related instance helpers.

/// <summary>
/// Owns the Access global page-allocation map on page 1 and exposes page
/// reserve, free, scrub, and tail-shrink operations for the writer.
/// </summary>
/// <param name="writer">The writer.</param>
internal sealed class PageAllocator(AccessWriter writer)
{
    private const int GlobalUsageMapPageNumber = 1;

    internal async ValueTask<long> AllocatePageAsync(byte[] page, CancellationToken cancellationToken)
    {
        long pageNumber = await ReserveContiguousPagesAsync(1, cancellationToken).ConfigureAwait(false);
        await writer.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        return pageNumber;
    }

    internal async ValueTask<long> ReserveContiguousPagesAsync(int pageCount, CancellationToken cancellationToken)
    {
        if (pageCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(pageCount), "Page count must be positive.");
        }

        List<long> freePages = await EnumerateMappedFreePagesAsync(cancellationToken).ConfigureAwait(false);
        long reusableStart = FindContiguousRun(freePages, pageCount);
        if (reusableStart > 0)
        {
            for (int offset = 0; offset < pageCount; offset++)
            {
                await SetPageFreeStateAsync(reusableStart + offset, free: false, cancellationToken).ConfigureAwait(false);
            }

            return reusableStart;
        }

        byte[] blankPage = new byte[writer.pgSz];
        long firstAppendedPage = -1;
        for (int offset = 0; offset < pageCount; offset++)
        {
            long appendedPage = await writer.AppendPageAsync(blankPage, cancellationToken).ConfigureAwait(false);
            if (offset == 0)
            {
                firstAppendedPage = appendedPage;
            }
            else if (appendedPage != firstAppendedPage + offset)
            {
                throw new IOException("Contiguous append reservation was interrupted by a non-contiguous page assignment.");
            }
        }

        return firstAppendedPage;
    }

    internal async ValueTask DeallocatePageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        if (pageNumber <= GlobalUsageMapPageNumber)
        {
            return;
        }

        bool secure = writer.Options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages;
        await WriteFreedPageAsync(pageNumber, secure, cancellationToken).ConfigureAwait(false);
        await SetPageFreeStateAsync(pageNumber, free: true, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<int> ScrubFreePagesAsync(CancellationToken cancellationToken)
    {
        var freePages = new SortedSet<long>(await EnumerateMappedFreePagesAsync(cancellationToken).ConfigureAwait(false));
        long totalPages = LogicalPageCount;
        for (long pageNumber = 2; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] == Constants.PageTypes.Freed)
                {
                    _ = freePages.Add(pageNumber);
                }
            }
            finally
            {
                ReturnPage(page);
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

            await WriteFreedPageAsync(pageNumber, secure: true, cancellationToken).ConfigureAwait(false);
            await SetPageFreeStateAsync(pageNumber, free: true, cancellationToken).ConfigureAwait(false);
            scrubbed++;
        }

        return scrubbed;
    }

    internal async ValueTask<long> ShrinkDatabaseAsync(CancellationToken cancellationToken)
    {
        if (writer.ActiveJournal is not null)
        {
            throw new InvalidOperationException("ShrinkDatabaseAsync cannot run inside an active transaction.");
        }

        bool secure = writer.Options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages;
        long totalPages = LogicalPageCount;
        long newTotalPages = totalPages;
        while (newTotalPages > 3)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long candidatePage = newTotalPages - 1;
            if (!await IsPageFreeAsync(candidatePage, cancellationToken).ConfigureAwait(false))
            {
                break;
            }

            if (secure)
            {
                await WriteFreedPageAsync(candidatePage, secure: true, cancellationToken).ConfigureAwait(false);
            }

            newTotalPages--;
        }

        if (newTotalPages == totalPages)
        {
            return 0;
        }

        long newLength = newTotalPages * writer.pgSz;
        await writer.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            writer.stream.SetLength(newLength);
            await writer.stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = writer.IoGate.Release();
        }

        return totalPages - newTotalPages;
    }

    internal async ValueTask<bool> IsPageFreeAsync(long pageNumber, CancellationToken cancellationToken)
    {
        if (pageNumber <= GlobalUsageMapPageNumber || pageNumber >= LogicalPageCount)
        {
            return false;
        }

        byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return IsPhysicallyReusableFreePage(page)
                && await IsPageMarkedFreeAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(page);
        }
    }

    private long LogicalPageCount => writer.ActiveJournal?.NextAppendPageNumber ?? writer.stream.Length / writer.pgSz;

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

    private async ValueTask<List<long>> EnumerateMappedFreePagesAsync(CancellationToken cancellationToken)
    {
        byte[] globalPage = await ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(globalPage, writer.dataPage, writer.pgSz, out RowBound rowBound))
            {
                return [];
            }

            var mappedFreePages = new List<long>();
            bool recognizedMap = await UsageMap.TryEnumeratePagesAsync(
                globalPage,
                rowBound,
                writer.pgSz,
                LogicalPageCount,
                minimumPageNumber: GlobalUsageMapPageNumber + 1,
                strict: false,
                writer.ReadPageAsync,
                ReturnPage,
                mappedFreePages,
                cancellationToken).ConfigureAwait(false);
            if (!recognizedMap)
            {
                return [];
            }

            return await FilterPhysicallyReusablePagesAsync(mappedFreePages, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(globalPage);
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
            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (IsPhysicallyReusableFreePage(page))
                {
                    reusablePages.Add(pageNumber);
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return reusablePages;
    }

    private async ValueTask<bool> IsPageMarkedFreeAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] globalPage = await ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(globalPage, writer.dataPage, writer.pgSz, out RowBound rowBound))
            {
                return false;
            }

            return globalPage[rowBound.RowStart] switch
            {
                Constants.UsageMap.InlineMapType => UsageMap.TryGetInlinePageState(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, out bool isFree) && isFree,
                Constants.UsageMap.ReferenceMapType => await TryGetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, cancellationToken).ConfigureAwait(false),
                _ => false,
            };
        }
        finally
        {
            ReturnPage(globalPage);
        }
    }

    private async ValueTask SetPageFreeStateAsync(long pageNumber, bool free, CancellationToken cancellationToken)
    {
        byte[] globalPage = await ReadGlobalUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(globalPage, writer.dataPage, writer.pgSz, out RowBound rowBound))
            {
                InitializeGlobalUsageMapPage(globalPage);
                rowBound = new AccessBase.RowBound(0, writer.pgSz - Constants.UsageMap.RowSize, Constants.UsageMap.RowSize);
            }

            byte mapType = globalPage[rowBound.RowStart];
            if (mapType == Constants.UsageMap.InlineMapType)
            {
                if (UsageMap.TrySetInlinePageState(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free, initializeBaseForPage: false))
                {
                    await writer.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (!free)
                {
                    return;
                }

                await PromoteInlineToReferenceAsync(globalPage, rowBound.RowStart, rowBound.RowSize, cancellationToken).ConfigureAwait(false);
                await SetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free: true, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (mapType == Constants.UsageMap.ReferenceMapType)
            {
                await SetReferenceFreeStateAsync(globalPage, rowBound.RowStart, rowBound.RowSize, pageNumber, free, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            ReturnPage(globalPage);
        }
    }

    private async ValueTask PromoteInlineToReferenceAsync(byte[] globalPage, int rowStart, int rowSize, CancellationToken cancellationToken)
    {
        var existingFreePages = new List<long>();
        _ = UsageMap.TryEnumerateInlinePages(
            globalPage,
            rowStart,
            rowSize,
            writer.pgSz,
            LogicalPageCount,
            minimumPageNumber: GlobalUsageMapPageNumber + 1,
            strict: false,
            existingFreePages);
        Array.Clear(globalPage, rowStart, rowSize);
        globalPage[rowStart] = Constants.UsageMap.ReferenceMapType;
        await writer.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);

        foreach (long freePageNumber in existingFreePages)
        {
            await SetReferenceFreeStateAsync(globalPage, rowStart, rowSize, freePageNumber, free: true, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<bool> TryGetReferenceFreeStateAsync(byte[] globalPage, int rowStart, int rowSize, long pageNumber, CancellationToken cancellationToken)
    {
        int pointerIndex = UsageMap.ReferencePointerIndex(writer.pgSz, pageNumber);
        int pointerCount = (rowSize - Constants.UsageMap.ReferenceMapPointerOffset) / 4;
        if (pointerIndex < 0 || pointerIndex >= pointerCount)
        {
            return false;
        }

        int pointerOffset = rowStart + Constants.UsageMap.ReferenceMapPointerOffset + (pointerIndex * 4);
        int mapPageNumber = Ri32(globalPage, pointerOffset);
        if (mapPageNumber <= 0 || mapPageNumber >= LogicalPageCount)
        {
            return false;
        }

        byte[] mapPage = await writer.ReadPageAsync(mapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (mapPage[0] != Constants.PageTypes.UsageMap)
            {
                return false;
            }

            return UsageMap.TryGetReferencePageState(mapPage, writer.pgSz, pageNumber, out bool isFree) && isFree;
        }
        finally
        {
            ReturnPage(mapPage);
        }
    }

    private async ValueTask SetReferenceFreeStateAsync(byte[] globalPage, int rowStart, int rowSize, long pageNumber, bool free, CancellationToken cancellationToken)
    {
        int pointerIndex = UsageMap.ReferencePointerIndex(writer.pgSz, pageNumber);
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

            mapPage = new byte[writer.pgSz];
            mapPage[0] = Constants.PageTypes.UsageMap;
            mapPageNumber = checked((int)await writer.AppendPageAsync(mapPage, cancellationToken).ConfigureAwait(false));
            Wi32(globalPage, pointerOffset, mapPageNumber);
            await writer.WritePageAsync(GlobalUsageMapPageNumber, globalPage, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            mapPage = await writer.ReadPageAsync(mapPageNumber, cancellationToken).ConfigureAwait(false);
            returnMapPage = true;
            if (mapPage[0] != Constants.PageTypes.UsageMap)
            {
                Array.Clear(mapPage, 0, writer.pgSz);
                mapPage[0] = Constants.PageTypes.UsageMap;
            }
        }

        try
        {
            if (UsageMap.TrySetReferencePageState(mapPage, writer.pgSz, pageNumber, free))
            {
                await writer.WritePageAsync(mapPageNumber, mapPage, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (returnMapPage)
            {
                ReturnPage(mapPage);
            }
        }
    }

    private async ValueTask<byte[]> ReadGlobalUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = await writer.ReadPageAsync(GlobalUsageMapPageNumber, cancellationToken).ConfigureAwait(false);
        if (!IsGlobalUsageMapPage(page))
        {
            InitializeGlobalUsageMapPage(page);
            await writer.WritePageAsync(GlobalUsageMapPageNumber, page, cancellationToken).ConfigureAwait(false);
        }

        return page;
    }

    private bool IsGlobalUsageMapPage(byte[] page)
    {
        if (page.Length < writer.pgSz || page[0] != Constants.PageTypes.Data || page[1] != 0x01)
        {
            return false;
        }

        if (!UsageMap.TryGetFirstRowBound(page, writer.dataPage, writer.pgSz, out RowBound rowBound))
        {
            return false;
        }

        return rowBound.RowSize >= Constants.UsageMap.InlineMapHeaderSize
            && page[rowBound.RowStart] is Constants.UsageMap.InlineMapType or Constants.UsageMap.ReferenceMapType;
    }

    private void InitializeGlobalUsageMapPage(byte[] page)
    {
        Array.Clear(page, 0, writer.pgSz);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = writer.pgSz - Constants.UsageMap.RowSize;
        int row1Start = rowStart - Constants.UsageMap.RowSize;
        int slotTableEnd = writer.dataPage.RowsStart + 4;
        int freeSpace = row1Start - slotTableEnd;
        Wu16(page, 2, freeSpace);
        Wi32(page, writer.dataPage.TDefOff, 1);
        Wu16(page, writer.dataPage.NumRows, 2);
        Wu16(page, writer.dataPage.RowsStart, rowStart);
        Wu16(page, writer.dataPage.RowsStart + 2, row1Start);
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
            page = new byte[writer.pgSz];
            returnPage = false;
        }
        else
        {
            page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            returnPage = true;
        }

        try
        {
            page[0] = Constants.PageTypes.Freed;
            page[1] = 0x01;
            Wu16(page, 2, Math.Max(0, writer.pgSz - 16));
            await writer.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (returnPage)
            {
                ReturnPage(page);
            }
        }
    }

    private static bool IsPhysicallyReusableFreePage(byte[] page)
        => page[0] is Constants.PageTypes.Freed or 0x00;
}
