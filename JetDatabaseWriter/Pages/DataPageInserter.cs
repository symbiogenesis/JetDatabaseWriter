namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using static JetDatabaseWriter.AccessBase;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Owns data-page allocation and row insertion mechanics for
/// <see cref="AccessWriter"/>. Handles finding/creating target pages,
/// writing row bytes, and patching usage-map / autonumber TDEF fields.
/// </summary>
internal sealed class DataPageInserter(AccessWriter writer, PageAllocator pageAllocator)
{
    internal static void PatchUsageMapPointers(byte[] tdefPage, int usageMapPageNumber)
    {
        tdefPage[Constants.TableDefinition.OwnedPagesRowOffset] = 0x00;
        WriteUInt24(tdefPage, Constants.TableDefinition.OwnedPagesPageOffset, usageMapPageNumber);

        tdefPage[Constants.TableDefinition.FreePagesRowOffset] = 0x01;
        WriteUInt24(tdefPage, Constants.TableDefinition.FreePagesPageOffset, usageMapPageNumber);
    }

    internal static void PatchAutoNumFlag(byte[] tdefPage, TableDef tableDef)
    {
        // Stamp TDEF byte 0x18 unconditionally to 0x01. Per Jackcess
        // (`TableImpl.writeDefinition`, "this makes autonumbering work in
        // access") and verified empirically in WriterTDefAutoNumFlagTests:
        // every user table in the DAO-authored NorthwindTraders.accdb has
        // byte 0x18 == 0x01, including the 4 tables (Catalog_TableOfContents,
        // States, TaxStatus, Titles) that carry no autonumber column. The
        // earlier conditional implementation wrote 0x00 for no-autonum tables
        // and disagreed with DAO ground truth.
        _ = tableDef;
        tdefPage[0x18] = 0x01;
    }

    internal async ValueTask<PageInsertTarget> FindInsertTargetAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        if (writer.TryGetCachedInsertPageNumber(tdefPage, out long cachedPageNumber))
        {
            byte[] cached = await writer.ReadPageAsync(cachedPageNumber, cancellationToken).ConfigureAwait(false);
            if (cached[0] == Constants.PageTypes.Data && Ri32(cached, writer._dataPage.TDefOff) == tdefPage && CanInsertRow(cached, rowLength))
            {
                return new PageInsertTarget { PageNumber = cachedPageNumber, Page = cached };
            }

            ReturnPage(cached);
        }

        if (tdefPage <= 1024)
        {
            PageInsertTarget? existingTarget = await TryFindExistingSystemTablePageAsync(tdefPage, rowLength, cancellationToken).ConfigureAwait(false);
            if (existingTarget is not null)
            {
                return existingTarget;
            }
        }

        // When the cached page is full, append a new data page directly
        // instead of scanning every page in the file. The previous O(N)
        // scan read + decrypted every page to find one with free space,
        // which dominated insert time for large databases. Appending is
        // O(1) and the marginal file-size cost is negligible — Access
        // itself uses usage-map bitmaps for the same purpose, but we don't
        // yet maintain writable usage maps for existing tables.
        long newPageNumber = await pageAllocator.AllocatePageAsync(CreateEmptyDataPage(tdefPage), cancellationToken).ConfigureAwait(false);
        writer.SetCachedInsertPageNumber(tdefPage, newPageNumber);

        // Mark the newly-appended data page in the per-table owned-pages
        // usage map. Without this, DAO's sequential / snapshot recordset
        // scans (which walk the usage map rather than the PK index) see
        // the table as empty, even though the row bytes are on disk and
        // the data page's parent_tdef back-pointer is correct.
        // Skip the small set of pre-existing system-table TDEFs whose
        // usage maps are already populated and managed by DAO; modifying
        // them surfaces "Invalid argument" from DAO.OpenDatabase. Freshly
        // created databases have low page numbers too, so the writer records
        // the TDEFs whose owned-page maps it created and can safely maintain.
        if (await writer.CanMaintainOwnedMapAsync(tdefPage, cancellationToken).ConfigureAwait(false))
        {
            await MarkPageInOwnedMapAsync(tdefPage, newPageNumber, cancellationToken).ConfigureAwait(false);
        }

        return new PageInsertTarget
        {
            PageNumber = newPageNumber,
            Page = await writer.ReadPageAsync(newPageNumber, cancellationToken).ConfigureAwait(false),
        };
    }

    private async ValueTask<PageInsertTarget?> TryFindExistingSystemTablePageAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        List<long>? mappedPages = await TryReadMappedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (mappedPages is not null)
        {
            foreach (long pageNumber in mappedPages)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                if (page[0] == Constants.PageTypes.Data && Ri32(page, writer._dataPage.TDefOff) == tdefPage && CanInsertRow(page, rowLength))
                {
                    writer.SetCachedInsertPageNumber(tdefPage, pageNumber);
                    return new PageInsertTarget { PageNumber = pageNumber, Page = page };
                }

                ReturnPage(page);
            }

            return null;
        }

        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 1; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] == Constants.PageTypes.Data && Ri32(page, writer._dataPage.TDefOff) == tdefPage && CanInsertRow(page, rowLength))
            {
                writer.SetCachedInsertPageNumber(tdefPage, pageNumber);
                return new PageInsertTarget { PageNumber = pageNumber, Page = page };
            }

            ReturnPage(page);
        }

        return null;
    }

    private async ValueTask<List<long>?> TryReadMappedDataPagesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        long totalPages = writer._stream.Length / writer._pgSz;
        if (tdefPage <= 0 || tdefPage >= totalPages)
        {
            return null;
        }

        int usageMapRow;
        int usageMapPageNumber;
        byte[] tdef = await writer.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            usageMapRow = tdef[Constants.TableDefinition.OwnedPagesRowOffset];
            usageMapPageNumber = tdef[Constants.TableDefinition.OwnedPagesPageOffset]
                | (tdef[Constants.TableDefinition.OwnedPagesPageOffset + 1] << 8)
                | (tdef[Constants.TableDefinition.OwnedPagesPageOffset + 2] << 16);
            if (usageMapPageNumber <= 0 || usageMapPageNumber >= totalPages)
            {
                return null;
            }
        }
        finally
        {
            ReturnPage(tdef);
        }

        byte[] usageMapPage = await writer.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (usageMapPage[0] != Constants.PageTypes.Data)
            {
                return null;
            }

            foreach (RowBound rowBound in writer.EnumerateLiveRowBounds(usageMapPage))
            {
                if (rowBound.RowIndex != usageMapRow || rowBound.RowSize <= 0)
                {
                    continue;
                }

                var mappedPages = new List<long>();
                return usageMapPage[rowBound.RowStart] switch
                {
                    0x00 => TryReadInlineMappedPages(usageMapPage, rowBound, totalPages, mappedPages) ? mappedPages : null,
                    0x01 => await TryReadReferenceMappedPagesAsync(usageMapPage, rowBound, totalPages, mappedPages, cancellationToken).ConfigureAwait(false) ? mappedPages : null,
                    _ => null,
                };
            }

            return null;
        }
        finally
        {
            ReturnPage(usageMapPage);
        }
    }

    private bool TryReadInlineMappedPages(byte[] usageMapPage, RowBound rowBound, long totalPages, List<long> mappedPages)
    {
        if (rowBound.RowSize <= Constants.UsageMap.InlineBitmapOffset)
        {
            return false;
        }

        int startPage = Ri32(usageMapPage, rowBound.RowStart + 1);
        if (startPage < 0)
        {
            return false;
        }

        int bitmapBytes = Math.Min(rowBound.RowSize - Constants.UsageMap.InlineBitmapOffset, writer._pgSz - rowBound.RowStart - Constants.UsageMap.InlineBitmapOffset);
        for (int bitIndex = 0; bitIndex < bitmapBytes * 8; bitIndex++)
        {
            int byteOffset = rowBound.RowStart + Constants.UsageMap.InlineBitmapOffset + (bitIndex / 8);
            byte bitMask = (byte)(1 << (bitIndex % 8));
            if ((usageMapPage[byteOffset] & bitMask) == 0)
            {
                continue;
            }

            long pageNumber = (long)startPage + bitIndex;
            if (pageNumber <= 0 || pageNumber >= totalPages)
            {
                return false;
            }

            mappedPages.Add(pageNumber);
        }

        return true;
    }

    private async ValueTask<bool> TryReadReferenceMappedPagesAsync(
        byte[] usageMapPage,
        RowBound rowBound,
        long totalPages,
        List<long> mappedPages,
        CancellationToken cancellationToken)
    {
        int pointerCount = (rowBound.RowSize - Constants.UsageMap.ReferenceMapPointerOffset) / 4;
        int pagesPerMapPage = (writer._pgSz - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;
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
                return false;
            }

            byte[] referencePage = await writer.ReadPageAsync(referencePageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (referencePage[0] != Constants.PageTypes.UsageMap)
                {
                    return false;
                }

                int bitCapacity = (writer._pgSz - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;
                for (int bitIndex = 0; bitIndex < bitCapacity; bitIndex++)
                {
                    int byteOffset = Constants.UsageMap.ReferenceMapBitmapOffset + (bitIndex / 8);
                    byte bitMask = (byte)(1 << (bitIndex % 8));
                    if ((referencePage[byteOffset] & bitMask) == 0)
                    {
                        continue;
                    }

                    long pageNumber = ((long)pointerIndex * pagesPerMapPage) + bitIndex;
                    if (pageNumber <= 0 || pageNumber >= totalPages)
                    {
                        return false;
                    }

                    mappedPages.Add(pageNumber);
                }
            }
            finally
            {
                ReturnPage(referencePage);
            }
        }

        return true;
    }

    /// <summary>
    /// Sets the owned-pages usage-map bit for <paramref name="dataPageNumber"/> in the
    /// per-table usage map referenced by the TDEF at offset 0x37 (1 byte row + 3 byte page).
    /// The map row is the INLINE form (type byte 0x00): startPage at bytes 1..4 (int32 LE),
    /// then a 64-byte bitmap covering 512 consecutive pages from startPage. On first use
    /// the startPage remains zero for low page numbers and is otherwise initialized to
    /// <c>(dataPageNumber / 8) * 8</c> so the bit fits in the bitmap. If the page is already
    /// outside the existing INLINE window, the row is left untouched (REFERENCE-form maps
    /// are not yet implemented; this is acceptable because the writer always appends pages
    /// monotonically in a single session).
    /// </summary>
    internal async ValueTask MarkPageInOwnedMapAsync(long tdefPageNumber, long dataPageNumber, CancellationToken cancellationToken)
    {
        byte[] tdef = await writer.ReadPageAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            int ownedRow = tdef[Constants.TableDefinition.OwnedPagesRowOffset];
            int ownedPage = tdef[Constants.TableDefinition.OwnedPagesPageOffset]
                | (tdef[Constants.TableDefinition.OwnedPagesPageOffset + 1] << 8)
                | (tdef[Constants.TableDefinition.OwnedPagesPageOffset + 2] << 16);
            int freeRow = tdef[Constants.TableDefinition.FreePagesRowOffset];
            int freePage = tdef[Constants.TableDefinition.FreePagesPageOffset]
                | (tdef[Constants.TableDefinition.FreePagesPageOffset + 1] << 8)
                | (tdef[Constants.TableDefinition.FreePagesPageOffset + 2] << 16);
            if (ownedPage == 0)
            {
                return;
            }

            byte[] umPage = await writer.ReadPageAsync(ownedPage, cancellationToken).ConfigureAwait(false);
            try
            {
                bool changed = TrySetUsageMapBit(umPage, ownedRow, dataPageNumber);
                if (freePage == ownedPage && freeRow != ownedRow)
                {
                    changed |= TrySetUsageMapBit(umPage, freeRow, dataPageNumber);
                }

                if (!changed)
                {
                    return;
                }

                await writer.WritePageAsync(ownedPage, umPage, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ReturnPage(umPage);
            }

            if (freePage != ownedPage && freePage != 0)
            {
                byte[] freeUmPage = await writer.ReadPageAsync(freePage, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (TrySetUsageMapBit(freeUmPage, freeRow, dataPageNumber))
                    {
                        await writer.WritePageAsync(freePage, freeUmPage, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    ReturnPage(freeUmPage);
                }
            }
        }
        finally
        {
            ReturnPage(tdef);
        }
    }

    private bool TrySetUsageMapBit(byte[] umPage, int rowIndex, long pageNumber)
    {
        int rowOffPos = writer._dataPage.RowsStart + (rowIndex * 2);
        int rowOff = Ru16(umPage, rowOffPos) & Constants.DataPage.RowOffsetMask;
        if (rowOff == 0)
        {
            return false;
        }

        byte type = umPage[rowOff];
        if (type != 0x00)
        {
            // REFERENCE-form (type 0x01) usage map: not yet implemented.
            return false;
        }

        int startPage = Ri32(umPage, rowOff + 1);
        if (startPage == 0 && pageNumber >= Constants.UsageMap.InlineBitmapBits)
        {
            startPage = checked((int)((pageNumber / 8) * 8));
            Wi32(umPage, rowOff + 1, startPage);
        }

        long bitIdx = pageNumber - startPage;
        if (bitIdx < 0 || bitIdx >= Constants.UsageMap.InlineBitmapBits)
        {
            return false;
        }

        umPage[rowOff + Constants.UsageMap.InlineBitmapOffset + (int)(bitIdx / 8)] |= (byte)(1 << (int)(bitIdx % 8));
        return true;
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
            int start = raw & Constants.DataPage.RowOffsetMask;
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
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        Wu16(page, 2, writer._pgSz - writer._dataPage.RowsStart);
        Wi32(page, writer._dataPage.TDefOff, (int)tdefPage);
        Wu16(page, writer._dataPage.NumRows, 0);
        return page;
    }

    internal async ValueTask<long> AppendUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = new byte[writer._pgSz];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;

        int row0Off = writer._pgSz - Constants.UsageMap.RowSize;
        int row1Off = row0Off - Constants.UsageMap.RowSize;

        Wi32(page, writer._dataPage.TDefOff, 0);
        Wu16(page, writer._dataPage.NumRows, 2);
        Wu16(page, writer._dataPage.RowsStart, row0Off);
        Wu16(page, writer._dataPage.RowsStart + 2, row1Off);

        int freeSpace = row1Off - (writer._dataPage.RowsStart + 4);
        Wu16(page, 2, freeSpace);

        return await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
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
