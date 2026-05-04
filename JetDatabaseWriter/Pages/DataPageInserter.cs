namespace JetDatabaseWriter.Pages;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.AccessBase;

/// <summary>
/// Owns data-page allocation and row insertion mechanics for
/// <see cref="AccessWriter"/>. Handles finding/creating target pages,
/// writing row bytes, and patching usage-map / autonumber TDEF fields.
/// </summary>
internal sealed class DataPageInserter(AccessWriter writer)
{
    internal static void PatchUsageMapPointers(byte[] tdefPage, int usageMapPageNumber)
    {
        tdefPage[0x37] = 0x00;
        WriteUInt24(tdefPage, 0x38, usageMapPageNumber);

        tdefPage[0x3B] = 0x01;
        WriteUInt24(tdefPage, 0x3C, usageMapPageNumber);
    }

    internal static void PatchAutoNumFlag(byte[] tdefPage, TableDef tableDef)
    {
        bool hasAutoNumber = false;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            if ((tableDef.Columns[i].Flags & 0x04) != 0)
            {
                hasAutoNumber = true;
                break;
            }
        }

        tdefPage[0x18] = hasAutoNumber ? (byte)0x01 : (byte)0x00;
    }

    internal async ValueTask<PageInsertTarget> FindInsertTargetAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        if (writer.TryGetCachedInsertPageNumber(tdefPage, out long cachedPageNumber))
        {
            byte[] cached = await writer.ReadPageAsync(cachedPageNumber, cancellationToken).ConfigureAwait(false);
            if (cached[0] == 0x01 && Ri32(cached, writer._dataPage.TDefOff) == tdefPage && CanInsertRow(cached, rowLength))
            {
                return new PageInsertTarget { PageNumber = cachedPageNumber, Page = cached };
            }

            ReturnPage(cached);
        }

        // When the cached page is full, append a new data page directly
        // instead of scanning every page in the file. The previous O(N)
        // scan read + decrypted every page to find one with free space,
        // which dominated insert time for large databases. Appending is
        // O(1) and the marginal file-size cost is negligible — Access
        // itself uses usage-map bitmaps for the same purpose, but we don't
        // yet maintain writable usage maps for existing tables.
        long newPageNumber = await writer.AppendPageAsync(CreateEmptyDataPage(tdefPage), cancellationToken).ConfigureAwait(false);
        writer.SetCachedInsertPageNumber(tdefPage, newPageNumber);
        return new PageInsertTarget
        {
            PageNumber = newPageNumber,
            Page = await writer.ReadPageAsync(newPageNumber, cancellationToken).ConfigureAwait(false),
        };
    }

    internal bool CanInsertRow(byte[] page, int rowLength)
    {
        int numRows = Ru16(page, writer._dataPage.NumRows);
        if (numRows >= Constants.DataPage.MaxRowsPerPage)
        {
            return false;
        }

        int dataStart = GetFirstRowStart(page, numRows);
        int nextOffsetPos = writer._dataPage.RowsStart + ((numRows + 1) * 2);
        return dataStart - nextOffsetPos >= rowLength;
    }

    internal int GetFirstRowStart(byte[] page, int numRows)
    {
        int first = writer._pgSz;
        for (int i = 0; i < numRows; i++)
        {
            int raw = Ru16(page, writer._dataPage.RowsStart + (i * 2));
            int start = raw & 0x1FFF;
            if (start > 0 && start < first)
            {
                first = start;
            }
        }

        return first;
    }

    internal byte[] CreateEmptyDataPage(long tdefPage)
    {
        byte[] page = new byte[writer._pgSz];
        page[0] = 0x01;
        page[1] = 0x01;
        Wu16(page, 2, writer._pgSz - writer._dataPage.RowsStart);
        Wi32(page, writer._dataPage.TDefOff, (int)tdefPage);
        Wu16(page, writer._dataPage.NumRows, 0);
        return page;
    }

    internal async ValueTask<long> AppendUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = new byte[writer._pgSz];
        page[0] = 0x01;
        page[1] = 0x01;

        const int rowSize = 69;
        int row0Off = writer._pgSz - rowSize;
        int row1Off = row0Off - rowSize;

        Wi32(page, writer._dataPage.TDefOff, 0);
        Wu16(page, writer._dataPage.NumRows, 2);
        Wu16(page, writer._dataPage.RowsStart, row0Off);
        Wu16(page, writer._dataPage.RowsStart + 2, row1Off);

        int freeSpace = row1Off - (writer._dataPage.RowsStart + 4);
        Wu16(page, 2, freeSpace);

        return await writer.AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
    }

    internal void WriteRowToPage(long pageNumber, byte[] page, byte[] rowBytes)
    {
        int numRows = Ru16(page, writer._dataPage.NumRows);
        int firstRowStart = GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = writer._dataPage.RowsStart + (numRows * 2);

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, writer._dataPage.NumRows, numRows + 1);

        int freeSpace = rowStart - (writer._dataPage.RowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Wu16(page, 2, freeSpace);
        writer.WritePage(pageNumber, page);
    }

    internal async ValueTask WriteRowToPageAsync(long pageNumber, byte[] page, byte[] rowBytes, CancellationToken cancellationToken)
    {
        int numRows = Ru16(page, writer._dataPage.NumRows);
        int firstRowStart = GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = writer._dataPage.RowsStart + (numRows * 2);

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, writer._dataPage.NumRows, numRows + 1);

        int freeSpace = rowStart - (writer._dataPage.RowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Wu16(page, 2, freeSpace);
        await writer.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
    }
}
