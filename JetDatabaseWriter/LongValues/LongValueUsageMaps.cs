namespace JetDatabaseWriter.LongValues;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Reads the native ten-byte LVAL map records following the logical index names.</summary>
internal static class LongValueUsageMaps
{
    /// <summary>Reads bounded native LVAL records, retaining their original pointer offsets.</summary>
    /// <param name="format">The file format.</param>
    /// <param name="tdef">The stitched logical TDEF bytes.</param>
    /// <param name="maps">The map records in native order.</param>
    /// <param name="sentinelOffset">The sentinel offset, or the logical end when the trailer is absent.</param>
    /// <returns>Whether the complete known trailer can be read without guessing its layout.</returns>
    internal static bool TryRead(JetFormat format, byte[] tdef, out List<LongValueUsageMap> maps, out int sentinelOffset)
    {
        maps = [];
        sentinelOffset = -1;
        if (tdef.Length < format.TDef.BlockEnd)
        {
            return false;
        }

        TDefCounts counts = TDefCodec.ReadCounts(format, tdef);
        if (counts.ColumnCount > Constants.TableDefinition.MaxColumns
            || counts.RealIndexCount is < 0 or > Constants.TableDefinition.MaxIndexes
            || counts.LogicalIndexCount is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            return false;
        }

        long declaredEnd = (long)Ri32(tdef, 8) + 8;
        if (declaredEnd < format.TDef.BlockEnd || declaredEnd > tdef.Length)
        {
            return false;
        }

        int end = (int)declaredEnd;
        int start = TDefCodec.LocateRealIndexDescriptors(format, tdef, counts.ColumnCount, counts.RealIndexCount);
        if (start < 0)
        {
            return false;
        }

        int position = format.Index.GetIndexSection(start, counts.RealIndexCount, counts.LogicalIndexCount).LogIdxNamesStart;
        for (int i = 0; i < counts.LogicalIndexCount; i++)
        {
            if (position >= end || format.ReadColumnName(tdef, ref position, out _) < 0)
            {
                return false;
            }
        }

        if (position > end)
        {
            return false;
        }

        var columns = new HashSet<int>();
        while (position < end)
        {
            if (end - position < 10)
            {
                return false;
            }

            int columnNumber = Ru16(tdef, position);
            if (columnNumber == ushort.MaxValue)
            {
                sentinelOffset = position;
                return true;
            }

            if (maps.Count >= Constants.TableDefinition.MaxColumns || !columns.Add(columnNumber))
            {
                return false;
            }

            maps.Add(new LongValueUsageMap(
                columnNumber,
                position + 2,
                position + 6,
                new UsageMapPointer(tdef[position + 2], UsageMap.ReadUInt24(tdef, position + 3)),
                new UsageMapPointer(tdef[position + 6], UsageMap.ReadUInt24(tdef, position + 7))));
            position += 10;
        }

        sentinelOffset = position;
        return true;
    }

    /// <summary>Allocates independently addressed empty native map rows.</summary>
    /// <param name="format">The native row and page layout.</param>
    /// <param name="allocator">The page allocator.</param>
    /// <param name="count">The number of rows.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The allocated row pointers in request order.</returns>
    internal static async ValueTask<UsageMapPointer[]> CreateRowsAsync(JetFormat format, PageAllocator allocator, int count, CancellationToken cancellationToken)
    {
        var pointers = new UsageMapPointer[count];
        int rowsPerPage = Math.Min(byte.MaxValue, (format.PageSize - format.DataPage.RowsStart) / (format.UsageMapRowSize + 2));
        var reservations = new ReservedPageRuns(allocator);
        try
        {
            for (int first = 0; first < count;)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int rows = Math.Min(rowsPerPage, count - first);
                byte[] page = new byte[format.PageSize];
                page[0] = Constants.PageTypes.Data;
                page[1] = 1;
                int start = format.PageSize;
                for (int row = 0; row < rows; row++)
                {
                    start -= format.UsageMapRowSize;
                    Wu16(page, format.DataPage.RowsStart + (row * 2), start);
                }

                Wu16(page, format.DataPage.NumRows, rows);
                Wu16(page, 2, start - format.DataPage.RowsStart - (rows * 2));
                long pageNumber = await allocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
                reservations.Add(pageNumber, 1);
                for (int row = 0; row < rows; row++)
                {
                    pointers[first + row] = new UsageMapPointer(row, checked((int)pageNumber));
                }

                first += rows;
            }

            // A complete result transfers every allocated row page to its caller.
            reservations.MarkLinked();
            return pointers;
        }
        finally
        {
            await reservations.ReleaseAsync().ConfigureAwait(false);
        }
    }
}
