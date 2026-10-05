namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Models;
using static JetDatabaseWriter.DatabaseFile;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Owns data-page allocation and row insertion mechanics for
/// <see cref="AccessWriter"/>. Handles finding/creating target pages,
/// writing row bytes, and patching usage-map / autonumber TDEF fields. Also
/// owns the per-writer insert-page hint and the set of TDEFs whose owned-page
/// usage maps this writer may extend (both restored when a transaction rolls back).
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="pageAllocator">The page allocator.</param>
/// <param name="catalogRows">Reads <c>MSysObjects</c> rows to decide whether an existing table's owned-page map is writable.</param>
/// <param name="usageMaps">Marks pages in usage-map rows and writes index rows, promoting a row to REFERENCE when one INLINE window no longer holds its pages.</param>
internal sealed class DataPageInserter(DatabaseFile db, PageAllocator pageAllocator, CatalogRowReader catalogRows, UsageMapEditor usageMaps)
{
#if NET9_0_OR_GREATER
    private readonly Lock insertPageHintLock = new();
#else
    private readonly object insertPageHintLock = new();
#endif
    private readonly HashSet<long> ownedMapWritableTdefs = [];
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
            byte[] cached = await db.ReadPageAsync(cachedPageNumber, cancellationToken).ConfigureAwait(false);
            if (cached[0] == Constants.PageTypes.Data && Ri32(cached, db.DataPage.TDefOff) == tdefPage && this.CanInsertRow(cached, rowLength))
            {
                return new PageInsertTarget { PageNumber = cachedPageNumber, Page = cached };
            }

            ReturnPage(cached);
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
        // the data page's parent_tdef back-pointer is correct.
        // Skip the small set of pre-existing system-table TDEFs whose
        // usage maps are already populated and managed by DAO; modifying
        // them surfaces "Invalid argument" from DAO.OpenDatabase. Freshly
        // created databases have low page numbers too, so the writer records
        // the TDEFs whose owned-page maps it created and can safely maintain.
        if (await this.CanMaintainOwnedMapAsync(tdefPage, cancellationToken).ConfigureAwait(false))
        {
            await this.MarkPageInOwnedMapAsync(tdefPage, newPageNumber, cancellationToken).ConfigureAwait(false);
        }

        return new PageInsertTarget
        {
            PageNumber = newPageNumber,
            Page = await db.ReadPageAsync(newPageNumber, cancellationToken).ConfigureAwait(false),
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
    /// Captures the insert-page hint and the writable owned-map set. A
    /// transaction takes this when it begins: inside it, both can come to name
    /// pages the transaction appended, which rollback discards.
    /// </summary>
    /// <returns>The state to pass to <see cref="RestoreState"/>.</returns>
    internal DataPageInserterState CaptureState()
    {
        lock (this.insertPageHintLock)
        {
            return new DataPageInserterState(this.cachedInsertTDefPage, this.cachedInsertPageNumber, [.. this.ownedMapWritableTdefs]);
        }
    }

    /// <summary>Puts the insert-page hint and the writable owned-map set back to <paramref name="state"/>.</summary>
    /// <param name="state">A state from <see cref="CaptureState"/>.</param>
    internal void RestoreState(DataPageInserterState state)
    {
        lock (this.insertPageHintLock)
        {
            this.cachedInsertTDefPage = state.HintTDefPage;
            this.cachedInsertPageNumber = state.HintPageNumber;
        }

        this.ownedMapWritableTdefs.Clear();
        this.ownedMapWritableTdefs.UnionWith(state.OwnedMapWritableTdefs);
    }

    /// <summary>
    /// Records that this writer created <paramref name="tdefPageNumber"/>'s
    /// owned-page usage map, so appended data pages may be marked in it.
    /// </summary>
    /// <param name="tdefPageNumber">The TDEF page number.</param>
    internal void RegisterOwnedMapWritableTdef(long tdefPageNumber) => this.ownedMapWritableTdefs.Add(tdefPageNumber);

    internal async ValueTask<bool> CanMaintainOwnedMapAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        if (db.Format == DatabaseFormat.Jet3Mdb || tdefPageNumber <= 0)
        {
            return false;
        }

        if (this.ownedMapWritableTdefs.Contains(tdefPageNumber))
        {
            return true;
        }

        if (tdefPageNumber == 2)
        {
            return false;
        }

        TableDef? msys = await db.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys is null)
        {
            return false;
        }

        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage != tdefPageNumber || row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if (string.IsNullOrEmpty(row.Name) || row.Name.StartsWith("MSys", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            this.RegisterOwnedMapWritableTdef(tdefPageNumber);
            return true;
        }

        return false;
    }

    private async ValueTask<PageInsertTarget?> TryFindExistingSystemTablePageAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await db.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] == Constants.PageTypes.Data && Ri32(page, db.DataPage.TDefOff) == tdefPage && this.CanInsertRow(page, rowLength))
            {
                this.SetCachedInsertPageNumber(tdefPage, pageNumber);
                return new PageInsertTarget { PageNumber = pageNumber, Page = page };
            }

            ReturnPage(page);
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
        byte[] tdef = await db.ReadPageAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            int ownedRow = tdef[db.TDef.UsedPages];
            int ownedPage = UsageMap.ReadUInt24(tdef, db.TDef.UsedPagesPage);
            int freeRow = tdef[db.TDef.FreePages];
            int freePage = UsageMap.ReadUInt24(tdef, db.TDef.FreePagesPage);
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
            ReturnPage(tdef);
        }
    }

    internal bool CanInsertRow(byte[] page, int rowLength)
    {
        int numRows = Ru16(page, db.DataPage.NumRows);
        if (numRows >= Constants.DataPage.MaxRowsPerPage)
        {
            return false;
        }

        int dataStart = this.GetFirstRowStart(page, numRows);
        int nextOffsetPos = db.DataPage.RowsStart + ((numRows + 1) * 2);
        return dataStart - nextOffsetPos >= rowLength;
    }

    internal int GetFirstRowStart(byte[] page, int numRows)
    {
        int first = db.PageSizeBytes;
        for (int i = 0; i < numRows; i++)
        {
            int raw = Ru16(page, db.DataPage.RowsStart + (i * 2));
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
        byte[] page = new byte[db.PageSizeBytes];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        Wu16(page, 2, db.PageSizeBytes - db.DataPage.RowsStart);
        Wi32(page, db.DataPage.TDefOff, (int)tdefPage);
        Wu16(page, db.DataPage.NumRows, 0);
        return page;
    }

    internal async ValueTask<long> AppendUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = new byte[db.PageSizeBytes];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;

        int row0Off = db.PageSizeBytes - Constants.UsageMap.RowSize;
        int row1Off = row0Off - Constants.UsageMap.RowSize;

        Wi32(page, db.DataPage.TDefOff, 0);
        Wu16(page, db.DataPage.NumRows, 2);
        Wu16(page, db.DataPage.RowsStart, row0Off);
        Wu16(page, db.DataPage.RowsStart + 2, row1Off);

        int freeSpace = row1Off - (db.DataPage.RowsStart + 4);
        Wu16(page, 2, freeSpace);

        return await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask WriteRowToPageAsync(long pageNumber, byte[] page, byte[] rowBytes, CancellationToken cancellationToken)
    {
        int numRows = Ru16(page, db.DataPage.NumRows);
        int firstRowStart = this.GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = db.DataPage.RowsStart + (numRows * 2);

        // Check before touching the page, so a row too long for it never
        // overwrites the row-offset table.
        int freeSpace = rowStart - (db.DataPage.RowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, db.DataPage.NumRows, numRows + 1);
        Wu16(page, 2, freeSpace);
        await db.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
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
        byte[] page = new byte[db.PageSizeBytes];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;

        int rowCount = indexPageGroups.Count + 2;
        int rowStart = db.PageSizeBytes;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            rowStart -= Constants.UsageMap.RowSize;
            Wu16(page, db.DataPage.RowsStart + (rowIndex * 2), rowStart);

            if (rowIndex < 2)
            {
                continue;
            }

            await usageMaps.WriteRowAsync(page, rowStart, Constants.UsageMap.RowSize, indexPageGroups[rowIndex - 2], keepReferencePages: false, cancellationToken).ConfigureAwait(false);
        }

        Wi32(page, db.DataPage.TDefOff, 0);
        Wu16(page, db.DataPage.NumRows, rowCount);
        int freeSpace = rowStart - (db.DataPage.RowsStart + (rowCount * 2));
        Wu16(page, 2, freeSpace);

        return await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask UpdateTableIndexUsageMapRowsAsync(long usageMapPageNumber, IReadOnlyList<long[]> indexPageGroups, CancellationToken cancellationToken)
    {
        byte[] page = await db.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);

        int existingRowCount = Ru16(page, db.DataPage.NumRows);
        int rowCount = Math.Max(existingRowCount, indexPageGroups.Count + 2);
        int rowStart = db.PageSizeBytes;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            rowStart -= Constants.UsageMap.RowSize;
            int slotOffset = db.DataPage.RowsStart + (rowIndex * 2);

            // Only a row that already sat at this offset is the index's own, so
            // only its REFERENCE bitmap pages may be reused.
            bool existingRow = rowIndex < existingRowCount && Ru16(page, slotOffset) == rowStart;
            Wu16(page, slotOffset, rowStart);

            if (rowIndex < 2)
            {
                continue;
            }

            int groupIndex = rowIndex - 2;
            if (groupIndex >= indexPageGroups.Count || indexPageGroups[groupIndex].Length == 0)
            {
                continue;
            }

            await usageMaps.WriteRowAsync(page, rowStart, Constants.UsageMap.RowSize, indexPageGroups[groupIndex], existingRow, cancellationToken).ConfigureAwait(false);
        }

        Wi32(page, db.DataPage.TDefOff, 0);
        Wu16(page, db.DataPage.NumRows, rowCount);
        int freeSpace = rowStart - (db.DataPage.RowsStart + (rowCount * 2));
        Wu16(page, 2, freeSpace);
        await db.WritePageAsync(usageMapPageNumber, page, cancellationToken).ConfigureAwait(false);
    }
}
