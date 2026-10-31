namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Verification;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Shared parser and emitter for JET usage-map rows.
/// </summary>
internal static class UsageMap
{
    internal static int PagesPerReferenceMapPage(int pageSize)
        => (pageSize - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;

    /// <summary>Returns how many bitmap-page pointers a REFERENCE row of <paramref name="rowSize"/> bytes holds: 17 in a 69-byte row.</summary>
    /// <param name="rowSize">The row's size in bytes.</param>
    /// <returns>The pointer count.</returns>
    internal static int ReferencePointerCount(int rowSize)
        => Math.Max(0, (rowSize - Constants.UsageMap.ReferenceMapPointerOffset) / 4);

    internal static int ReferencePointerIndex(int pageSize, long pageNumber)
        => (int)(pageNumber / PagesPerReferenceMapPage(pageSize));

    internal static int AlignInlineBasePage(long pageNumber)
        => checked((int)(pageNumber / 8 * 8));

    internal static bool TryReadPointer(byte[] page, int offset, out UsageMapPointer pointer)
    {
        pointer = default;
        if (!BinaryBuffer.IsRange(page.Length, offset, 4))
        {
            return false;
        }

        pointer = new UsageMapPointer(
            page[offset],
            page[offset + 1] | (page[offset + 2] << 8) | (page[offset + 3] << 16));
        return true;
    }

    internal static int ReadUInt24(byte[] page, int offset)
        => ReadUInt24LittleEndian(page.AsSpan(offset, 3));

    internal static void WritePointer(byte[] page, int offset, int rowIndex, long pageNumber)
    {
        int checkedPageNumber = checked((int)pageNumber);
        page[offset] = checked((byte)rowIndex);
        page[offset + 1] = (byte)(checkedPageNumber & 0xFF);
        page[offset + 2] = (byte)((checkedPageNumber >> 8) & 0xFF);
        page[offset + 3] = (byte)((checkedPageNumber >> 16) & 0xFF);
    }

    internal static bool TryGetFirstRowBound(byte[] page, DataPageLayout layout, int pageSize, out RowBound rowBound)
        => TryGetRowBound(page, layout, pageSize, rowIndex: 0, out rowBound);

    internal static bool TryGetRowBound(ReadOnlySpan<byte> page, DataPageLayout layout, int pageSize, int rowIndex, out RowBound rowBound)
    {
        rowBound = default;
        if (rowIndex < 0)
        {
            return false;
        }

        int rowCount = Ru16(page, layout.NumRows);
        if (rowCount <= rowIndex)
        {
            return false;
        }

        int maxRows = Math.Min(rowCount, (pageSize - layout.RowsStart) / 2);
        if (rowIndex >= maxRows)
        {
            return false;
        }

        int rawRowStart = Ru16(page, layout.RowsStart + (rowIndex * 2));
        if ((rawRowStart & Constants.DataPage.NonLiveRowFlags) != 0)
        {
            return false;
        }

        int rowStart = rawRowStart & Constants.DataPage.RowOffsetMask;
        int slotTableEnd = layout.RowsStart + (maxRows * 2);
        if (rowStart < slotTableEnd || rowStart >= pageSize)
        {
            return false;
        }

        int rowEnd = pageSize;
        for (int candidateIndex = 0; candidateIndex < maxRows; candidateIndex++)
        {
            if (candidateIndex == rowIndex)
            {
                continue;
            }

            int candidateStart = Ru16(page, layout.RowsStart + (candidateIndex * 2)) & Constants.DataPage.RowOffsetMask;
            if (candidateStart > rowStart && candidateStart < rowEnd)
            {
                rowEnd = candidateStart;
            }
        }

        rowBound = new RowBound(rowIndex, rowStart, rowEnd - rowStart);
        return rowBound.RowSize > 0;
    }

    internal static async ValueTask<bool> TryEnumeratePagesAsync(
        byte[] usageMapPage,
        RowBound rowBound,
        int pageSize,
        long totalPages,
        long minimumPageNumber,
        bool strict,
        Func<long, CancellationToken, ValueTask<byte[]>> readPageAsync,
        Action<byte[]> returnPage,
        List<long> pageNumbers,
        CancellationToken cancellationToken)
    {
        if (rowBound.RowSize <= 0
            || rowBound.RowStart < 0
            || rowBound.RowStart + rowBound.RowSize > usageMapPage.Length)
        {
            return false;
        }

        return usageMapPage[rowBound.RowStart] switch
        {
            Constants.UsageMap.InlineMapType => TryEnumerateInlinePages(
                usageMapPage,
                rowBound,
                pageSize,
                totalPages,
                minimumPageNumber,
                strict,
                pageNumbers),
            Constants.UsageMap.ReferenceMapType => await TryEnumerateReferencePagesAsync(
                usageMapPage,
                rowBound,
                pageSize,
                totalPages,
                minimumPageNumber,
                strict,
                readPageAsync,
                returnPage,
                pageNumbers,
                cancellationToken).ConfigureAwait(false),
            _ => false,
        };
    }

    internal static bool TryEnumerateInlinePages(
        byte[] usageMapPage,
        RowBound rowBound,
        int pageSize,
        long totalPages,
        long minimumPageNumber,
        bool strict,
        List<long> pageNumbers)
    {
        if (rowBound.RowSize <= Constants.UsageMap.InlineMapHeaderSize)
        {
            return false;
        }

        int basePage = Ri32(usageMapPage, rowBound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset);
        if (strict && basePage < 0)
        {
            return false;
        }

        int bitmapBytes = Math.Min(
            rowBound.RowSize - Constants.UsageMap.InlineBitmapOffset,
            pageSize - rowBound.RowStart - Constants.UsageMap.InlineBitmapOffset);
        if (bitmapBytes <= 0)
        {
            return false;
        }

        return TryEnumerateBitmapPages(usageMapPage, rowBound.RowStart + Constants.UsageMap.InlineBitmapOffset, bitmapBytes, basePage, totalPages, minimumPageNumber, strict, pageNumbers);
    }

    internal static async ValueTask<bool> TryEnumerateReferencePagesAsync(
        byte[] usageMapPage,
        RowBound rowBound,
        int pageSize,
        long totalPages,
        long minimumPageNumber,
        bool strict,
        Func<long, CancellationToken, ValueTask<byte[]>> readPageAsync,
        Action<byte[]> returnPage,
        List<long> pageNumbers,
        CancellationToken cancellationToken)
    {
        int pointerCount = (rowBound.RowSize - Constants.UsageMap.ReferenceMapPointerOffset) / 4;
        int pagesPerMapPage = PagesPerReferenceMapPage(pageSize);
        for (int pointerIndex = 0; pointerIndex < pointerCount; pointerIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int pointerOffset = rowBound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (pointerIndex * 4);
            int referencePageNumber = Ri32(usageMapPage, pointerOffset);
            if (referencePageNumber == 0)
            {
                continue;
            }

            if (referencePageNumber < 0 || referencePageNumber >= totalPages)
            {
                if (strict)
                {
                    return false;
                }

                continue;
            }

            byte[] referencePage = await readPageAsync(referencePageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (referencePage[0] != Constants.PageTypes.UsageMap)
                {
                    if (strict)
                    {
                        return false;
                    }

                    continue;
                }

                if (!TryEnumerateBitmapPages(referencePage, Constants.UsageMap.ReferenceMapBitmapOffset, pageSize - Constants.UsageMap.ReferenceMapBitmapOffset, (long)pointerIndex * pagesPerMapPage, totalPages, minimumPageNumber, strict, pageNumbers))
                {
                    return false;
                }
            }
            finally
            {
                returnPage(referencePage);
            }
        }

        return true;
    }

    internal static bool TryGetInlinePageState(byte[] usageMapPage, int rowStart, int rowSize, long pageNumber, out bool isMarked)
    {
        byte state = RecoveryKernel.ReadUsageBit(usageMapPage, GetInlineBitPosition(usageMapPage, rowStart, rowSize, pageNumber));
        isMarked = state == 2;
        return state != 0;
    }

    internal static bool TryGetReferencePageState(byte[] referencePage, int pageSize, long pageNumber, out bool isMarked)
    {
        byte state = RecoveryKernel.ReadUsageBit(referencePage, GetReferenceBitPosition(referencePage, pageSize, pageNumber));
        isMarked = state == 2;
        return state != 0;
    }

    internal static bool TrySetReferencePageState(byte[] referencePage, int pageSize, long pageNumber, bool isMarked)
        => RecoveryKernel.WriteUsageBit(referencePage, GetReferenceBitPosition(referencePage, pageSize, pageNumber), isMarked);

    internal static bool TrySetInlinePageState(byte[] usageMapPage, int rowStart, int rowSize, long pageNumber, bool isMarked)
        => RecoveryKernel.WriteUsageBit(usageMapPage, GetInlineBitPosition(usageMapPage, rowStart, rowSize, pageNumber), isMarked);

    /// <summary>
    /// Writes an INLINE row that lists <paramref name="pageNumbers"/> over the
    /// <paramref name="rowSize"/> bytes at <paramref name="rowStart"/>, when one
    /// bitmap window holds them all. The window starts at page 0 when the
    /// lowest page lies in the first window, and otherwise at the lowest page
    /// rounded down to a multiple of 8.
    /// </summary>
    /// <param name="page">The usage-map page.</param>
    /// <param name="rowStart">The row's offset on the page.</param>
    /// <param name="rowSize">The row's size in bytes.</param>
    /// <param name="pageNumbers">The pages to list.</param>
    /// <returns><see langword="false"/>, leaving the row untouched, when the pages span more than one window.</returns>
    internal static bool TryWriteInlineRow(byte[] page, int rowStart, int rowSize, IReadOnlyList<long> pageNumbers)
    {
        int bitCapacity = (rowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        if (bitCapacity <= 0)
        {
            return false;
        }

        long lowest = long.MaxValue;
        long highest = long.MinValue;
        for (int i = 0; i < pageNumbers.Count; i++)
        {
            lowest = Math.Min(lowest, pageNumbers[i]);
            highest = Math.Max(highest, pageNumbers[i]);
        }

        int basePageNumber = pageNumbers.Count == 0 || lowest < bitCapacity ? 0 : AlignInlineBasePage(lowest);
        if (pageNumbers.Count > 0 && (lowest < 0 || highest - basePageNumber >= bitCapacity))
        {
            return false;
        }

        Array.Clear(page, rowStart, rowSize);
        page[rowStart] = Constants.UsageMap.InlineMapType;
        Wi32(page, rowStart + Constants.UsageMap.ReferenceMapPointerOffset, basePageNumber);

        for (int i = 0; i < pageNumbers.Count; i++)
        {
            int bitIndex = checked((int)(pageNumbers[i] - basePageNumber));
            page[rowStart + Constants.UsageMap.InlineBitmapOffset + (bitIndex / 8)] |= (byte)(1 << (bitIndex % 8));
        }

        return true;
    }

    /// <summary>Returns whether the INLINE row of <paramref name="rowSize"/> bytes at <paramref name="rowStart"/> lists no page.</summary>
    /// <param name="page">The usage-map page.</param>
    /// <param name="rowStart">The row's offset on the page.</param>
    /// <param name="rowSize">The row's size in bytes.</param>
    /// <returns><see langword="true"/> when no bitmap bit is set.</returns>
    internal static bool IsInlineBitmapEmpty(byte[] page, int rowStart, int rowSize)
    {
        for (int offset = rowStart + Constants.UsageMap.InlineBitmapOffset; offset < rowStart + rowSize; offset++)
        {
            if (page[offset] != 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Adds to <paramref name="bitmapPages"/> each bitmap page (page type
    /// <c>0x05</c>) that the REFERENCE row at <paramref name="rowBound"/>
    /// points at. Does nothing for a row of another type.
    /// </summary>
    /// <param name="usageMapPage">The usage-map page.</param>
    /// <param name="rowBound">The row's bounds on the page.</param>
    /// <param name="totalPages">The end of file in pages; a pointer at or past it is skipped.</param>
    /// <param name="readPageAsync">Reads a page into a pooled buffer.</param>
    /// <param name="returnPage">Returns a pooled buffer.</param>
    /// <param name="bitmapPages">Receives the bitmap page numbers.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>A task that completes when the row's pointers have been read.</returns>
    internal static async ValueTask CollectReferenceBitmapPagesAsync(
        byte[] usageMapPage,
        RowBound rowBound,
        long totalPages,
        Func<long, CancellationToken, ValueTask<byte[]>> readPageAsync,
        Action<byte[]> returnPage,
        ICollection<long> bitmapPages,
        CancellationToken cancellationToken)
    {
        if (rowBound.RowSize <= 0
            || rowBound.RowStart + rowBound.RowSize > usageMapPage.Length
            || usageMapPage[rowBound.RowStart] != Constants.UsageMap.ReferenceMapType)
        {
            return;
        }

        int pointerCount = ReferencePointerCount(rowBound.RowSize);
        for (int pointerIndex = 0; pointerIndex < pointerCount; pointerIndex++)
        {
            int bitmapPageNumber = Ri32(usageMapPage, rowBound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (pointerIndex * 4));
            if (bitmapPageNumber <= 0 || bitmapPageNumber >= totalPages)
            {
                continue;
            }

            byte[] bitmapPage = await readPageAsync(bitmapPageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (bitmapPage[0] == Constants.PageTypes.UsageMap)
                {
                    bitmapPages.Add(bitmapPageNumber);
                }
            }
            finally
            {
                returnPage(bitmapPage);
            }
        }
    }

    private static bool TryEnumerateBitmapPages(byte[] bitmap, int offset, int byteCount, long basePage, long totalPages, long minimumPageNumber, bool strict, List<long> pageNumbers)
    {
        for (int index = 0; index < byteCount; index++)
        {
            int bits = bitmap[offset + index];
            if (bits == 0)
            {
                continue;
            }

            long pageNumber = basePage + (index * 8);
            do
            {
                if ((bits & 1) != 0)
                {
                    if (pageNumber < minimumPageNumber || pageNumber >= totalPages)
                    {
                        if (strict)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        pageNumbers.Add(pageNumber);
                    }
                }

                bits >>= 1;
                pageNumber++;
            }
            while (bits != 0);
        }

        return true;
    }

    private static ulong GetInlineBitPosition(byte[] usageMapPage, int rowStart, int rowSize, long pageNumber)
        => rowStart < 0 || rowSize < 0 || pageNumber < 0
            ? ulong.MaxValue
            : RecoveryKernel.InlineUsagePosition(usageMapPage, (uint)rowStart, (uint)rowSize, (ulong)pageNumber);

    private static ulong GetReferenceBitPosition(byte[] referencePage, int pageSize, long pageNumber)
        => pageSize < 0 || pageNumber < 0
            ? ulong.MaxValue
            : RecoveryKernel.ReferenceUsagePosition((uint)referencePage.Length, (uint)pageSize, (ulong)pageNumber);
}
