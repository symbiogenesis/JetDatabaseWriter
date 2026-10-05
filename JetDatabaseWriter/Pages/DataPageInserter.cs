namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Owns data-page allocation and row insertion mechanics for
/// <see cref="AccessWriter"/>. Handles finding/creating target pages,
/// writing row bytes, and patching usage-map / autonumber TDEF fields. Also
/// owns the per-writer insert-page hint (restored when a transaction rolls
/// back); the owned-map policy it is given decides whose owned-page usage
/// maps an appended data page is marked in.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="ownedPages">The database's owned-page discovery and row walks.</param>
/// <param name="pager">The writer's page file, through which data and usage-map pages are written.</param>
/// <param name="pageAllocator">The page allocator.</param>
/// <param name="ownedMaps">Decides whether a table's owned-page usage map may be extended.</param>
/// <param name="usageMaps">Edits and promotes table and index usage-map rows.</param>
internal sealed class DataPageInserter(JetFormat format, OwnedDataPages ownedPages, Pager pager, PageAllocator pageAllocator, IOwnedMapPolicy ownedMaps, UsageMapEditor usageMaps)
{
#if NET9_0_OR_GREATER
    private readonly Lock insertPageHintLock = new();
#else
    private readonly object insertPageHintLock = new();
#endif
    private long cachedInsertTDefPage = -1;
    private long cachedInsertPageNumber = -1;

    internal static void PatchUsageMapPointers(byte[] tdefPage, TDefHeaderLayout layout, int usageMapPageNumber)
    {
        UsageMap.WritePointer(tdefPage, layout.UsedPages, rowIndex: 0, usageMapPageNumber);
        UsageMap.WritePointer(tdefPage, layout.FreePages, rowIndex: 1, usageMapPageNumber);
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

    internal static long[][] ToSinglePageGroups(long[] pageNumbers)
    {
        long[][] pageGroups = new long[pageNumbers.Length][];
        for (int i = 0; i < pageNumbers.Length; i++)
        {
            pageGroups[i] = [pageNumbers[i]];
        }

        return pageGroups;
    }

    internal async ValueTask<PageInsertTarget> FindInsertTargetAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        if (this.TryGetCachedInsertPageNumber(tdefPage, out long cachedPageNumber))
        {
            byte[] cached = await pager.ReadPageAsync(cachedPageNumber, cancellationToken).ConfigureAwait(false);
            if (cached[0] == Constants.PageTypes.Data && Ri32(cached, format.DataPage.TDefOff) == tdefPage && this.CanInsertRow(cached, rowLength))
            {
                return new PageInsertTarget { PageNumber = cachedPageNumber, Page = cached };
            }

            PageBuffers.Return(cached);
        }

        if (tdefPage <= 1024)
        {
            PageInsertTarget? existingTarget = await this.TryFindExistingSystemTablePageAsync(tdefPage, rowLength, cancellationToken).ConfigureAwait(false);
            if (existingTarget is not null)
            {
                return existingTarget;
            }
        }

        // When the cached page is full, append a new data page directly
        // instead of scanning every page in the file. The previous O(N)
        // scan read + decrypted every page to find one with free space,
        // which dominated insert time for large databases. Appending is
        // O(1), and the owned-map update below keeps DAO sequential scans
        // aware of the new page for tables whose maps this writer owns.
        long newPageNumber = await pageAllocator.AllocatePageAsync(this.CreateEmptyDataPage(tdefPage), cancellationToken).ConfigureAwait(false);
        this.SetCachedInsertPageNumber(tdefPage, newPageNumber);

        // Mark the newly-appended data page in the per-table owned-pages
        // usage map. Without this, DAO's sequential / snapshot recordset
        // scans (which walk the usage map rather than the PK index) see
        // the table as empty, even though the row bytes are on disk and
        // the data page's parent_tdef back-pointer is correct. The owned-map
        // policy skips the system tables whose maps Access manages.
        if (await ownedMaps.CanMaintainAsync(tdefPage, cancellationToken).ConfigureAwait(false))
        {
            await this.MarkPageInOwnedMapAsync(tdefPage, newPageNumber, cancellationToken).ConfigureAwait(false);
        }

        return new PageInsertTarget
        {
            PageNumber = newPageNumber,
            Page = await pager.ReadPageAsync(newPageNumber, cancellationToken).ConfigureAwait(false),
        };
    }

    internal bool TryGetCachedInsertPageNumber(long tdefPage, out long pageNumber)
    {
        lock (this.insertPageHintLock)
        {
            if (this.cachedInsertTDefPage == tdefPage && this.cachedInsertPageNumber >= 3)
            {
                pageNumber = this.cachedInsertPageNumber;
                return true;
            }

            pageNumber = -1;
            return false;
        }
    }

    internal void SetCachedInsertPageNumber(long tdefPage, long pageNumber)
    {
        lock (this.insertPageHintLock)
        {
            this.cachedInsertTDefPage = tdefPage;
            this.cachedInsertPageNumber = pageNumber;
        }
    }

    /// <summary>
    /// Captures the insert-page hint. A transaction takes this when it begins:
    /// inside it, the hint can come to name a page the transaction appended,
    /// which rollback discards.
    /// </summary>
    /// <returns>The state to pass to <see cref="RestoreState"/>.</returns>
    internal DataPageInserterState CaptureState()
    {
        lock (this.insertPageHintLock)
        {
            return new DataPageInserterState(this.cachedInsertTDefPage, this.cachedInsertPageNumber);
        }
    }

    /// <summary>Puts the insert-page hint back to <paramref name="state"/>.</summary>
    /// <param name="state">A state from <see cref="CaptureState"/>.</param>
    internal void RestoreState(DataPageInserterState state)
    {
        lock (this.insertPageHintLock)
        {
            this.cachedInsertTDefPage = state.HintTDefPage;
            this.cachedInsertPageNumber = state.HintPageNumber;
        }
    }

    private async ValueTask<PageInsertTarget?> TryFindExistingSystemTablePageAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        IReadOnlyList<long> pageNumbers = await ownedPages.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] == Constants.PageTypes.Data && Ri32(page, format.DataPage.TDefOff) == tdefPage && this.CanInsertRow(page, rowLength))
            {
                this.SetCachedInsertPageNumber(tdefPage, pageNumber);
                return new PageInsertTarget { PageNumber = pageNumber, Page = page };
            }

            PageBuffers.Return(page);
        }

        return null;
    }

    /// <summary>
    /// Marks <paramref name="dataPageNumber"/> in the table's owned-pages usage
    /// map, the row the TDEF's <see cref="TDefHeaderLayout.UsedPages"/> pointer
    /// names (1-byte row + 3-byte page), and in its free-space map
    /// (<see cref="TDefHeaderLayout.FreePages"/>), through
    /// <see cref="UsageMapEditor.MarkPageAsync"/>, which reads and writes the
    /// usage-map pages. An INLINE row whose 512-page window does not hold the
    /// page moves the window while it lists no page, and is otherwise promoted
    /// to REFERENCE, so both maps keep every data page the writer appends
    /// however far apart the pages lie.
    /// </summary>
    /// <param name="tdefPageNumber">The TDEF page number.</param>
    /// <param name="dataPageNumber">The data page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask MarkPageInOwnedMapAsync(long tdefPageNumber, long dataPageNumber, CancellationToken cancellationToken)
    {
        byte[] tdef = await pager.ReadPageAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            int ownedRow = tdef[format.TDef.UsedPages];
            int ownedPage = UsageMap.ReadUInt24(tdef, format.TDef.UsedPagesPage);
            int freeRow = tdef[format.TDef.FreePages];
            int freePage = UsageMap.ReadUInt24(tdef, format.TDef.FreePagesPage);
            if (ownedPage == 0)
            {
                return;
            }

            // Both rows on one page, as the writer puts them, take one read and
            // one write of it.
            int[] ownedPageRows = freePage == ownedPage && freeRow != ownedRow ? [ownedRow, freeRow] : [ownedRow];
            await usageMaps.MarkPageAsync(ownedPage, ownedPageRows, dataPageNumber, cancellationToken).ConfigureAwait(false);
            if (freePage != ownedPage && freePage != 0)
            {
                await usageMaps.MarkPageAsync(freePage, [freeRow], dataPageNumber, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            PageBuffers.Return(tdef);
        }
    }

    internal bool CanInsertRow(byte[] page, int rowLength)
    {
        int numRows = Ru16(page, format.DataPage.NumRows);
        if (numRows >= Constants.DataPage.MaxRowsPerPage)
        {
            return false;
        }

        int dataStart = this.GetFirstRowStart(page, numRows);
        int nextOffsetPos = format.DataPage.RowsStart + ((numRows + 1) * 2);
        return dataStart - nextOffsetPos >= rowLength;
    }

    internal int GetFirstRowStart(byte[] page, int numRows)
    {
        int first = format.PageSize;
        for (int i = 0; i < numRows; i++)
        {
            int raw = Ru16(page, format.DataPage.RowsStart + (i * 2));
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
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        Wu16(page, 2, format.PageSize - format.DataPage.RowsStart);
        Wi32(page, format.DataPage.TDefOff, (int)tdefPage);
        Wu16(page, format.DataPage.NumRows, 0);
        return page;
    }

    internal async ValueTask<long> AppendUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;

        int row0Off = format.PageSize - Constants.UsageMap.RowSize;
        int row1Off = row0Off - Constants.UsageMap.RowSize;

        Wi32(page, format.DataPage.TDefOff, 0);
        Wu16(page, format.DataPage.NumRows, 2);
        Wu16(page, format.DataPage.RowsStart, row0Off);
        Wu16(page, format.DataPage.RowsStart + 2, row1Off);

        int freeSpace = row1Off - (format.DataPage.RowsStart + 4);
        Wu16(page, 2, freeSpace);

        return await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask WriteRowToPageAsync(long pageNumber, byte[] page, byte[] rowBytes, CancellationToken cancellationToken)
    {
        int numRows = Ru16(page, format.DataPage.NumRows);
        int firstRowStart = this.GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = format.DataPage.RowsStart + (numRows * 2);

        // Check before touching the page, so a row too long for it never
        // overwrites the row-offset table.
        int freeSpace = rowStart - (format.DataPage.RowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, format.DataPage.NumRows, numRows + 1);
        Wu16(page, 2, freeSpace);
        await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Matches the DAO-observed shape for a real-index usage-map page: table rows
    /// 0/1 first, then one 69-byte usage-map row per real index, INLINE when one
    /// window holds the index's pages and REFERENCE otherwise
    /// (<see cref="UsageMapEditor.WriteRowAsync"/>).
    /// </summary>
    /// <param name="leafPageNumbers">An array of leaf page numbers, one per real index on the table. Each page number is stored in a separate usage-map row at index i+2.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The page number of the newly allocated usage-map page.</returns>
    internal ValueTask<long> AppendIndexUsageMapPageAsync(long[] leafPageNumbers, CancellationToken cancellationToken)
        => this.AppendIndexUsageMapPageAsync(ToSinglePageGroups(leafPageNumbers), cancellationToken);

    internal async ValueTask<long> AppendIndexUsageMapPageAsync(IReadOnlyList<long[]> indexPageGroups, CancellationToken cancellationToken)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;

        int rowCount = indexPageGroups.Count + 2;
        int rowStart = format.PageSize;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            rowStart -= Constants.UsageMap.RowSize;
            Wu16(page, format.DataPage.RowsStart + (rowIndex * 2), rowStart);

            if (rowIndex < 2)
            {
                continue;
            }

            await usageMaps.WriteRowAsync(page, rowStart, Constants.UsageMap.RowSize, indexPageGroups[rowIndex - 2], keepReferencePages: false, cancellationToken).ConfigureAwait(false);
        }

        Wi32(page, format.DataPage.TDefOff, 0);
        Wu16(page, format.DataPage.NumRows, rowCount);
        int freeSpace = rowStart - (format.DataPage.RowsStart + (rowCount * 2));
        Wu16(page, 2, freeSpace);

        return await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<UsageMapPointer[]> UpdateTableIndexUsageMapRowsAsync(
        long fallbackUsageMapPageNumber,
        IReadOnlyList<UsageMapPointer> indexPointers,
        IReadOnlyList<long[]> indexPageGroups,
        CancellationToken cancellationToken)
    {
        var result = new UsageMapPointer[indexPointers.Count];
        var groupsByPage = new Dictionary<long, List<int>>();
        for (int index = 0; index < indexPointers.Count; index++)
        {
            result[index] = indexPointers[index];
            if (indexPageGroups[index].Length == 0)
            {
                continue;
            }

            long pageNumber = indexPointers[index].PageNumber > 0 ? indexPointers[index].PageNumber : fallbackUsageMapPageNumber;
            if (!groupsByPage.TryGetValue(pageNumber, out List<int>? groups))
            {
                groups = [];
                groupsByPage.Add(pageNumber, groups);
            }

            groups.Add(index);
        }

        foreach ((long pageNumber, List<int> groups) in groupsByPage)
        {
            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                int existingRowCount = Ru16(page, format.DataPage.NumRows);
                int rowCount = existingRowCount;
                int rowStart = this.GetFirstRowStart(page, existingRowCount);
                var rows = new RowBound[groups.Count];
                for (int group = 0; group < groups.Count; group++)
                {
                    UsageMapPointer pointer = indexPointers[groups[group]];
                    if (pointer.PageNumber > 0)
                    {
                        if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out rows[group]))
                        {
                            throw new InvalidDataException("The index usage-map row has invalid bounds.");
                        }
                    }
                    else
                    {
                        rowStart -= Constants.UsageMap.RowSize;
                        rows[group] = new RowBound(rowCount++, rowStart, Constants.UsageMap.RowSize);
                    }
                }

                int slotTableEnd = format.DataPage.RowsStart + (rowCount * 2);
                if (page[0] != Constants.PageTypes.Data || rowCount > byte.MaxValue + 1 || rowStart < slotTableEnd)
                {
                    throw new InvalidDataException("The usage-map page has no room for the index rows.");
                }

                // Access-authored rows need not be 69 bytes or sit at index+2.
                // Other maps can share this page. Keep their slots and bytes,
                // and reuse each existing index row's own REFERENCE bitmaps.
                for (int group = 0; group < groups.Count; group++)
                {
                    int index = groups[group];
                    RowBound row = rows[group];
                    bool existingRow = indexPointers[index].PageNumber > 0;
                    if (!existingRow)
                    {
                        Wu16(page, format.DataPage.RowsStart + (row.RowIndex * 2), row.RowStart);
                    }

                    await usageMaps.WriteRowAsync(page, row.RowStart, row.RowSize, indexPageGroups[index], existingRow, cancellationToken).ConfigureAwait(false);
                    result[index] = new UsageMapPointer(row.RowIndex, checked((int)pageNumber));
                }

                if (rowCount != existingRowCount)
                {
                    Wu16(page, format.DataPage.NumRows, rowCount);
                    Wu16(page, 2, rowStart - slotTableEnd);
                }

                await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return result;
    }
}
