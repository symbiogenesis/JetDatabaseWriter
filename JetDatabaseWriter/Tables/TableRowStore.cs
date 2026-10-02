namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.ValueEncoding;
using static JetDatabaseWriter.AccessBase;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Row-level storage primitives shared by every writer workflow: writes a
/// row's bytes (after pushing oversized long values to LVAL pages), marks a
/// row deleted (optionally scrubbing its payload), and keeps the TDEF row
/// count in step. Index maintenance and referential integrity are the
/// caller's responsibility.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="options">The writer options; supplies the secure-erase policy for deleted rows.</param>
/// <param name="longValueEncoder">Pre-encodes and deallocates LVAL chains.</param>
/// <param name="rowEncoder">Serializes row values into on-disk row bytes.</param>
/// <param name="dataPages">Finds or allocates the data page that receives a row.</param>
/// <param name="tdefPageBuilder">Owns the TDEF row-count byte layout.</param>
internal sealed class TableRowStore(
    AccessBase db,
    AccessWriterOptions options,
    LongValueEncoder longValueEncoder,
    RowEncoder rowEncoder,
    DataPageInserter dataPages,
    TDefPageBuilder tdefPageBuilder)
{
    internal async ValueTask InsertRowDataAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default) => _ = await this.InsertRowDataLocAsync(tdefPage, tableDef, values, updateTDefRowCount, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Inserts a row and returns its (page, row-index) location so the caller
    /// can mark it deleted if a subsequent step (e.g. unique-index rebuild)
    /// fails. Mirrors <see cref="InsertRowDataAsync"/> but exposes the
    /// <see cref="RowLocation"/> of the freshly written row.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values.</param>
    /// <param name="updateTDefRowCount">Whether to update the table row count in the TDEF.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="values"/> does not contain one value per column in <paramref name="tableDef"/>.</exception>
    internal async ValueTask<RowLocation> InsertRowDataLocAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default)
    {
        if (values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException(
                $"Expected {tableDef.Columns.Count} values for table row but received {values.Length}.",
                nameof(values));
        }

        // Push any oversized MEMO / OLE / Attachment payload to LVAL pages
        // before serializing the row. The pre-encode pass appends LVAL pages to
        // the file and rewrites the matching slot in `values` with a
        // PreEncodedLongValue sentinel carrying the finished 12-byte header.
        values = await longValueEncoder.PreEncodeLongValuesAsync(tdefPage, tableDef, values, cancellationToken).ConfigureAwait(false);

        byte[] rowBytes = rowEncoder.SerializeRow(tableDef, values);
        PageInsertTarget target = await dataPages.FindInsertTargetAsync(tdefPage, rowBytes.Length, cancellationToken).ConfigureAwait(false);
        int rowIndex;
        int rowStart;
        try
        {
            rowIndex = Ru16(target.Page, db.DataPage.NumRows);
            rowStart = dataPages.GetFirstRowStart(target.Page, rowIndex) - rowBytes.Length;
            await dataPages.WriteRowToPageAsync(target.PageNumber, target.Page, rowBytes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(target.Page);
        }

        if (updateTDefRowCount)
        {
            await this.AdjustTDefRowCountAsync(tdefPage, 1, cancellationToken).ConfigureAwait(false);
        }

        return new RowLocation(target.PageNumber, rowIndex, rowStart, rowBytes.Length);
    }

    /// <summary>
    /// Adjusts the persisted row count of the table at <paramref name="tdefPage"/>
    /// by <paramref name="delta"/>. Delegates to <see cref="TDefPageBuilder"/>,
    /// which owns TDEF row-count and <c>num_idx_rows</c> byte layout.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="delta">The signed row-count delta.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask AdjustTDefRowCountAsync(long tdefPage, long delta, CancellationToken cancellationToken)
        => tdefPageBuilder.AdjustTDefRowCountAsync(tdefPage, delta, cancellationToken);

    internal ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, CancellationToken cancellationToken)
        => this.MarkRowDeletedAsync(pageNumber, rowIndex, tableDef: null, DeletedRowDataMode.Default, cancellationToken);

    internal ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, DeletedRowDataMode dataMode, CancellationToken cancellationToken)
        => this.MarkRowDeletedAsync(pageNumber, rowIndex, tableDef: null, dataMode, cancellationToken);

    internal async ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, TableDef? tableDef, CancellationToken cancellationToken)
        => await this.MarkRowDeletedAsync(pageNumber, rowIndex, tableDef, DeletedRowDataMode.Default, cancellationToken).ConfigureAwait(false);

    private async ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, TableDef? tableDef, DeletedRowDataMode dataMode, CancellationToken cancellationToken)
    {
        byte[] page = await db.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        List<LongValueDescriptor>? longValueRoots = null;
        int offsetPos = db.DataPage.RowsStart + (rowIndex * 2);
        int raw = Ru16(page, offsetPos);
        if ((raw & Constants.DataPage.NonLiveRowFlags) != 0)
        {
            ReturnPage(page);
            return;
        }

        if (dataMode == DeletedRowDataMode.Clear || options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages)
        {
            foreach (RowBound rowBound in db.EnumerateLiveRowBounds(page))
            {
                if (rowBound.RowIndex != rowIndex)
                {
                    continue;
                }

                if (tableDef is not null && options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages)
                {
                    longValueRoots = longValueEncoder.CollectLongValueRoots(page, rowBound, tableDef);
                }

                Array.Clear(page, rowBound.RowStart, rowBound.RowSize);
                break;
            }
        }

        Wu16(page, offsetPos, raw | Constants.DataPage.DeletedRowFlag);
        await db.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        ReturnPage(page);

        if (longValueRoots is null)
        {
            return;
        }

        foreach (LongValueDescriptor root in longValueRoots)
        {
            await longValueEncoder.DeallocateLongValueAsync(root, cancellationToken).ConfigureAwait(false);
        }
    }
}
