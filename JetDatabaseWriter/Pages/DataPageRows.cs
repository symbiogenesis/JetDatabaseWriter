namespace JetDatabaseWriter.Pages;

using System;
using System.Buffers;
using System.Collections.Generic;
using JetDatabaseWriter.Pages.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Parses the row-offset table (the row directory) of a data page. Each row
/// ends at the next distinct row offset on the page, or at the end of the
/// page; deleted slots are left out, and an overflow row's header slot is
/// flagged <see cref="RowBound.IsOverflowPointer"/> where the caller wants it.
/// Pure functions of the page bytes and the format's layout.
/// </summary>
internal static class DataPageRows
{
    /// <summary>
    /// Yields the bounds (row index, start offset, size) of every live (non-deleted, non-overflow)
    /// row on the given data <paramref name="page"/>. Overflow headers are left out, so
    /// this suits pages that never hold overflow rows (usage-map and LVAL pages) and
    /// callers that look for a slot by index; table scans use
    /// <see cref="ComputeRowDirectory"/> or <see cref="OwnedDataPages.ForEachRowOnPageAsync"/>.
    /// </summary>
    /// <param name="format">The file's format profile.</param>
    /// <param name="page">The page bytes.</param>
    /// <returns>The live rows' bounds, in slot order.</returns>
    internal static IEnumerable<RowBound> EnumerateLiveRowBounds(JetFormat format, byte[] page)
    {
        int numRows = Ru16(page, format.DataPage.NumRows);
        if (numRows == 0)
        {
            yield break;
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - format.DataPage.RowsStart) / 2;
        if (numRows > maxPossibleRows)
        {
            numRows = maxPossibleRows;
        }

        if (numRows <= 0)
        {
            yield break;
        }

        int[] rawOffsets = new int[numRows];
        for (int r = 0; r < numRows; r++)
        {
            rawOffsets[r] = Ru16(page, format.DataPage.RowsStart + (r * 2));
        }

        int[] positions = new int[numRows];
        int posCount = 0;
        for (int r = 0; r < numRows; r++)
        {
            int pos = rawOffsets[r] & Constants.DataPage.RowOffsetMask;
            if (pos > 0 && pos < format.PageSize)
            {
                positions[posCount++] = pos;
            }
        }

        Array.Sort(positions, 0, posCount);

        for (int r = 0; r < numRows; r++)
        {
            int raw = rawOffsets[r];
            if ((raw & Constants.DataPage.NonLiveRowFlags) != 0)
            {
                continue;
            }

            int rowStart = raw & Constants.DataPage.RowOffsetMask;
            int rowEnd = FindNextRowStart(positions, posCount, rowStart, format.PageSize);
            yield return new RowBound(r, rowStart, rowEnd - rowStart);
        }
    }

    /// <summary>
    /// Returns the row directory of the data <paramref name="page"/>, in slot order:
    /// every live row, plus every overflow row's header (a slot flagged
    /// <see cref="Constants.DataPage.OverflowRowFlag"/> but not deleted) with
    /// <see cref="RowBound.IsOverflowPointer"/> set and its pointer bytes as the bound.
    /// Deleted slots, including the moved bytes of overflow rows, are left out.
    /// Allocates a single <see cref="RowBound"/>[] (or <see cref="Array.Empty{T}"/>
    /// when the page has no such slot) instead of returning an iterator. Suitable as
    /// a memoization target for <see cref="ReaderPageCache"/>, where the same page may
    /// be visited by multiple streaming consumers.
    /// </summary>
    /// <param name="format">The file's format profile.</param>
    /// <param name="page">The page bytes.</param>
    /// <returns>The row directory.</returns>
    internal static RowBound[] ComputeRowDirectory(JetFormat format, byte[] page)
    {
        int numRows = Ru16(page, format.DataPage.NumRows);
        if (numRows == 0)
        {
            return [];
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - format.DataPage.RowsStart) / 2;
        if (numRows > maxPossibleRows)
        {
            numRows = maxPossibleRows;
        }

        if (numRows <= 0)
        {
            return [];
        }

        // Cold (cache-miss) scan only: warm rescans are served from the
        // row-bounds cache. Rent the two scratch buffers from the shared pool
        // instead of allocating int[numRows] per page; numRows is bounded by the
        // page's row-offset table size. Array.Sort is used because Span<int>.Sort
        // is unavailable on netstandard2.1.
        int[] rawOffsets = ArrayPool<int>.Shared.Rent(numRows);
        int[] positions = ArrayPool<int>.Shared.Rent(numRows);
        try
        {
            int posCount = 0;
            int entryCount = 0;
            for (int r = 0; r < numRows; r++)
            {
                int raw = Ru16(page, format.DataPage.RowsStart + (r * 2));
                rawOffsets[r] = raw;

                int pos = raw & Constants.DataPage.RowOffsetMask;
                if (pos > 0 && pos < format.PageSize)
                {
                    positions[posCount++] = pos;
                }

                if ((raw & Constants.DataPage.DeletedRowFlag) == 0)
                {
                    entryCount++;
                }
            }

            if (entryCount == 0)
            {
                return [];
            }

            Array.Sort(positions, 0, posCount);

            var result = new RowBound[entryCount];
            int idx = 0;
            for (int r = 0; r < numRows; r++)
            {
                int raw = rawOffsets[r];
                if ((raw & Constants.DataPage.DeletedRowFlag) != 0)
                {
                    continue;
                }

                int rowStart = raw & Constants.DataPage.RowOffsetMask;
                int rowEnd = FindNextRowStart(positions, posCount, rowStart, format.PageSize);
                bool isOverflowPointer = (raw & Constants.DataPage.OverflowRowFlag) != 0;
                result[idx++] = new RowBound(r, rowStart, rowEnd - rowStart, isOverflowPointer);
            }

            return result;
        }
        finally
        {
            ArrayPool<int>.Shared.Return(rawOffsets);
            ArrayPool<int>.Shared.Return(positions);
        }
    }

    /// <summary>
    /// Returns the bounds of slot <paramref name="rowIndex"/> on the data
    /// <paramref name="page"/> whatever its flags, for following an overflow pointer
    /// to a slot Access flagged deleted. The slot must exist within the page's
    /// (clamped) row count and start past the row-offset table and inside the page;
    /// it ends at the next greater offset of any slot, as in
    /// <see cref="ComputeRowDirectory"/>.
    /// </summary>
    /// <param name="format">The file's format profile.</param>
    /// <param name="page">The data page bytes.</param>
    /// <param name="rowIndex">The slot's row index.</param>
    /// <param name="bound">Receives the slot's bounds on success.</param>
    /// <returns><see langword="true"/> when the slot exists.</returns>
    internal static bool TryGetSlotBound(JetFormat format, byte[] page, int rowIndex, out RowBound bound)
    {
        bound = default;
        int numRows = Math.Min(Ru16(page, format.DataPage.NumRows), (page.Length - format.DataPage.RowsStart) / 2);
        if (rowIndex < 0 || rowIndex >= numRows)
        {
            return false;
        }

        int rowStart = Ru16(page, format.DataPage.RowsStart + (rowIndex * 2)) & Constants.DataPage.RowOffsetMask;
        if (rowStart < format.DataPage.RowsStart + (numRows * 2) || rowStart >= format.PageSize)
        {
            return false;
        }

        int rowEnd = format.PageSize;
        for (int r = 0; r < numRows; r++)
        {
            int candidate = Ru16(page, format.DataPage.RowsStart + (r * 2)) & Constants.DataPage.RowOffsetMask;
            if (candidate > rowStart && candidate < rowEnd)
            {
                rowEnd = candidate;
            }
        }

        bound = new RowBound(rowIndex, rowStart, rowEnd - rowStart);
        return true;
    }

    /// <summary>
    /// Yields <see cref="RowLocation"/>s (row index + start/size) for every live, non-overflow
    /// row on <paramref name="page"/>, paired with <paramref name="pageNumber"/>. A thin wrapper
    /// over <see cref="EnumerateLiveRowBounds"/> for callers that need to round-trip
    /// the originating page number (update / delete paths).
    /// </summary>
    /// <param name="format">The file's format profile.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    /// <returns>The live rows' locations, in slot order.</returns>
    internal static IEnumerable<RowLocation> EnumerateLiveRowLocations(JetFormat format, long pageNumber, byte[] page)
    {
        foreach (RowBound rb in EnumerateLiveRowBounds(format, page))
        {
            yield return new RowLocation(pageNumber, rb.RowIndex, rb.RowStart, rb.RowSize);
        }
    }

    /// <summary>
    /// Returns the end (exclusive) of the row that starts at <paramref name="rowStart"/>:
    /// the first offset in <paramref name="sortedPositions"/> strictly greater than
    /// <paramref name="rowStart"/>, or <paramref name="pageSize"/> when there is none.
    /// The offsets include deleted slots, and Access leaves deleted slots pointing
    /// at the same offset as a live row, so the search skips every entry equal to
    /// <paramref name="rowStart"/> rather than taking the neighbour of whichever
    /// equal entry a binary search lands on.
    /// </summary>
    /// <param name="sortedPositions">Every slot's masked row offset, sorted ascending.</param>
    /// <param name="count">The number of valid entries in <paramref name="sortedPositions"/>.</param>
    /// <param name="rowStart">The row's start offset.</param>
    /// <param name="pageSize">The page size, which ends the highest row.</param>
    private static int FindNextRowStart(int[] sortedPositions, int count, int rowStart, int pageSize)
    {
        int lo = 0;
        int hi = count;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (sortedPositions[mid] <= rowStart)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo < count ? sortedPositions[lo] : pageSize;
    }
}
