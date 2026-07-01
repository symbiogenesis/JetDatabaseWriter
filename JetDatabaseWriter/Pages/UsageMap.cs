namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Shared parser and emitter for JET usage-map rows.
/// </summary>
internal static class UsageMap
{
    internal static int PagesPerReferenceMapPage(int pageSize)
        => (pageSize - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;

    internal static int ReferencePointerIndex(int pageSize, long pageNumber)
        => (int)(pageNumber / PagesPerReferenceMapPage(pageSize));

    internal static int AlignInlineBasePage(long pageNumber)
        => checked((int)(pageNumber / 8 * 8));

    internal static bool TryReadPointer(byte[] page, int offset, out UsageMapPointer pointer)
    {
        pointer = default;
        if (offset < 0 || offset + 3 >= page.Length)
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

    internal static bool TryGetRowBound(byte[] page, DataPageLayout layout, int pageSize, int rowIndex, out RowBound rowBound)
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

        int bitCapacity = bitmapBytes * 8;
        for (int bitIndex = 0; bitIndex < bitCapacity; bitIndex++)
        {
            int byteOffset = rowBound.RowStart + Constants.UsageMap.InlineBitmapOffset + (bitIndex / 8);
            byte bitMask = (byte)(1 << (bitIndex % 8));
            if ((usageMapPage[byteOffset] & bitMask) == 0)
            {
                continue;
            }

            long pageNumber = (long)basePage + bitIndex;
            if (pageNumber < minimumPageNumber || pageNumber >= totalPages)
            {
                if (strict)
                {
                    return false;
                }

                continue;
            }

            pageNumbers.Add(pageNumber);
        }

        return true;
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

                int bitCapacity = (pageSize - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;
                for (int bitIndex = 0; bitIndex < bitCapacity; bitIndex++)
                {
                    int byteOffset = Constants.UsageMap.ReferenceMapBitmapOffset + (bitIndex / 8);
                    byte bitMask = (byte)(1 << (bitIndex % 8));
                    if ((referencePage[byteOffset] & bitMask) == 0)
                    {
                        continue;
                    }

                    long pageNumber = ((long)pointerIndex * pagesPerMapPage) + bitIndex;
                    if (pageNumber < minimumPageNumber || pageNumber >= totalPages)
                    {
                        if (strict)
                        {
                            return false;
                        }

                        continue;
                    }

                    pageNumbers.Add(pageNumber);
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
        isMarked = false;
        if (!TryGetInlineBitOffset(usageMapPage, rowStart, rowSize, pageNumber, out int byteOffset, out byte bitMask))
        {
            return false;
        }

        isMarked = (usageMapPage[byteOffset] & bitMask) != 0;
        return true;
    }

    internal static bool TryGetReferencePageState(byte[] referencePage, int pageSize, long pageNumber, out bool isMarked)
    {
        isMarked = false;
        if (!TryGetReferenceBitOffset(referencePage, pageSize, pageNumber, out int byteOffset, out byte bitMask))
        {
            return false;
        }

        isMarked = (referencePage[byteOffset] & bitMask) != 0;
        return true;
    }

    internal static bool TrySetReferencePageState(byte[] referencePage, int pageSize, long pageNumber, bool isMarked)
    {
        if (!TryGetReferenceBitOffset(referencePage, pageSize, pageNumber, out int byteOffset, out byte bitMask))
        {
            return false;
        }

        if (isMarked)
        {
            referencePage[byteOffset] |= bitMask;
        }
        else
        {
            referencePage[byteOffset] &= unchecked((byte)~bitMask);
        }

        return true;
    }

    internal static bool TrySetInlinePageState(
        byte[] usageMapPage,
        int rowStart,
        int rowSize,
        long pageNumber,
        bool isMarked,
        bool initializeBaseForPage)
    {
        if (rowSize <= Constants.UsageMap.InlineMapHeaderSize)
        {
            return false;
        }

        int basePage = Ri32(usageMapPage, rowStart + Constants.UsageMap.ReferenceMapPointerOffset);
        if (basePage < 0)
        {
            return false;
        }

        if (initializeBaseForPage && basePage == 0 && pageNumber >= Constants.UsageMap.InlineBitmapBits)
        {
            basePage = AlignInlineBasePage(pageNumber);
            Wi32(usageMapPage, rowStart + Constants.UsageMap.ReferenceMapPointerOffset, basePage);
        }

        long bitIndex = pageNumber - basePage;
        int bitCapacity = (rowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        if (bitIndex < 0 || bitIndex >= bitCapacity)
        {
            return false;
        }

        int byteOffset = rowStart + Constants.UsageMap.InlineBitmapOffset + ((int)bitIndex / 8);
        byte bitMask = (byte)(1 << ((int)bitIndex % 8));
        if (isMarked)
        {
            usageMapPage[byteOffset] |= bitMask;
        }
        else
        {
            usageMapPage[byteOffset] &= unchecked((byte)~bitMask);
        }

        return true;
    }

    internal static void WriteInlineRow(byte[] page, int rowStart, IReadOnlyList<long> pageNumbers)
    {
        Array.Clear(page, rowStart, Constants.UsageMap.RowSize);
        long firstPageNumber = pageNumbers.Count == 0 ? 0 : pageNumbers[0];
        int basePageNumber = firstPageNumber < Constants.UsageMap.InlineBitmapBits
            ? 0
            : AlignInlineBasePage(firstPageNumber);

        page[rowStart] = Constants.UsageMap.InlineMapType;
        Wi32(page, rowStart + Constants.UsageMap.ReferenceMapPointerOffset, basePageNumber);

        for (int i = 0; i < pageNumbers.Count; i++)
        {
            int bitIndex = checked((int)(pageNumbers[i] - basePageNumber));
            if ((uint)bitIndex >= Constants.UsageMap.InlineBitmapBits)
            {
                throw new NotSupportedException(
                    "Index B-tree allocation spans more than one inline usage-map bitmap; " +
                    "REFERENCE usage maps for index pages are not yet supported.");
            }

            page[rowStart + Constants.UsageMap.InlineBitmapOffset + (bitIndex / 8)] |= (byte)(1 << (bitIndex % 8));
        }
    }

    private static bool TryGetInlineBitOffset(byte[] usageMapPage, int rowStart, int rowSize, long pageNumber, out int byteOffset, out byte bitMask)
    {
        byteOffset = 0;
        bitMask = 0;
        if (rowSize <= Constants.UsageMap.InlineMapHeaderSize)
        {
            return false;
        }

        int basePage = Ri32(usageMapPage, rowStart + Constants.UsageMap.ReferenceMapPointerOffset);
        long bitIndex = pageNumber - basePage;
        int bitCapacity = (rowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        if (bitIndex < 0 || bitIndex >= bitCapacity)
        {
            return false;
        }

        byteOffset = rowStart + Constants.UsageMap.InlineBitmapOffset + ((int)bitIndex / 8);
        bitMask = (byte)(1 << ((int)bitIndex % 8));
        return true;
    }

    private static bool TryGetReferenceBitOffset(byte[] referencePage, int pageSize, long pageNumber, out int byteOffset, out byte bitMask)
    {
        byteOffset = 0;
        bitMask = 0;
        if (pageNumber < 0)
        {
            return false;
        }

        int bitIndex = (int)(pageNumber % PagesPerReferenceMapPage(pageSize));
        byteOffset = Constants.UsageMap.ReferenceMapBitmapOffset + (bitIndex / 8);
        if (byteOffset < Constants.UsageMap.ReferenceMapBitmapOffset || byteOffset >= referencePage.Length)
        {
            return false;
        }

        bitMask = (byte)(1 << (bitIndex % 8));
        return true;
    }
}
