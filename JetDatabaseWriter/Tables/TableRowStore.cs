namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.DatabaseFile;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Row-level storage primitives shared by every writer workflow: encodes and
/// measures a row without writing it, writes a row's bytes (after pushing
/// oversized long values to LVAL pages), marks a row deleted (optionally
/// scrubbing its payload), and keeps the TDEF row count in step. Index
/// maintenance and referential integrity are the caller's responsibility.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="options">The writer options; supplies the secure-erase policy for deleted rows.</param>
/// <param name="longValueEncoder">Moves oversized long values to LVAL chains and deallocates them.</param>
/// <param name="rowEncoder">Serializes row values into on-disk row bytes.</param>
/// <param name="dataPages">Finds or allocates the data page that receives a row.</param>
/// <param name="tdefPageBuilder">Owns the TDEF row-count byte layout.</param>
internal sealed class TableRowStore(
    DatabaseFile db,
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

        // Callers that rewrite snapshot rows check before deleting the old row;
        // this guard keeps a placeholder from ever reaching the page.
        UnreadableLongValue.ThrowIfAny(values, tableName: null);

        // A MEMO or OLE value over its inline cap goes to LVAL pages, and so
        // do the largest of the others while the row is too long for a page.
        // They are written only after the row has been encoded and measured
        // with a placeholder header for each such value, so a value the
        // encoder rejects, or a row too long for a page, throws before
        // anything is written. The real headers have the same size, so the
        // row serialized again with them has the measured length.
        (object[] prepared, byte[] rowBytes) = this.EncodeRow(tableDef, values);
        if (await longValueEncoder.WriteLongValuesAsync(prepared, cancellationToken).ConfigureAwait(false))
        {
            rowBytes = rowEncoder.SerializeRow(tableDef, prepared);
        }

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
    /// Encodes a row as <see cref="InsertRowDataLocAsync"/> writes it, with no
    /// I/O and no change to any state: each MEMO or OLE value over its inline
    /// cap becomes a pending long value with a zeroed 12-byte placeholder
    /// header (<see cref="LongValueEncoder.PrepareLongValues"/>), and the row
    /// is serialized with those placeholders (<see cref="RowEncoder.TrySerializeRow"/>).
    /// While the row is longer than one data page, the largest MEMO or
    /// byte-array OLE value left in it, the first of equal ones in column
    /// order, becomes a pending long value too (Microsoft Access also moves
    /// long values out of a full row; which ones it picks is unchecked). An
    /// OLE value given as a string stays in the row. A value the encoder
    /// refuses, or a row still too long once no such value is left in it,
    /// throws here with the exception the insert would throw, so a caller
    /// that rewrites a row by deleting it and inserting the new version calls
    /// this for every new version before its first delete.
    /// </summary>
    /// <param name="tableDef">The row's table.</param>
    /// <param name="values">The row, one value per column; it is not changed.</param>
    /// <returns>
    /// The values with the pending long values (<paramref name="values"/>
    /// itself when no value leaves the row) and the serialized row, whose
    /// length is final.
    /// </returns>
    /// <exception cref="JetLimitationException">
    /// The row is longer than one data page with every MEMO and byte-array
    /// OLE value it holds moved out, or a value is past what its column
    /// stores, such as a Decimal value past its precision or a long value
    /// past the LVAL length limit.
    /// </exception>
    /// <exception cref="OverflowException">A value does not fit its column's type, such as a Currency value past ±922,337,203,685,477.5807.</exception>
    internal (object[] Values, byte[] RowBytes) EncodeRow(TableDef tableDef, object[] values)
    {
        object[] prepared = longValueEncoder.PrepareLongValues(tableDef, values);
        if (rowEncoder.TrySerializeRow(tableDef, prepared, out int rowLength) is { } rowBytes)
        {
            return (prepared, rowBytes);
        }

        // Moving a value leaves its 12-byte header in place of its inline
        // form, so the row shrinks by the payload's length (a Jet3 jump table
        // only shrinks with the row). Values are moved until their payloads
        // cover the excess and the row is measured again; each pass moves at
        // least one value, so the loop ends.
        List<(int Column, byte[] Payload)> inline = longValueEncoder.CollectInlineLongValues(tableDef, prepared);
        int next = 0;
        while (next < inline.Count)
        {
            if (ReferenceEquals(prepared, values))
            {
                prepared = (object[])values.Clone();
            }

            int excess = rowLength - rowEncoder.MaxRowLength;
            while (excess > 0 && next < inline.Count)
            {
                (int column, byte[] payload) = inline[next++];
                prepared[column] = new PreEncodedLongValue(new byte[Constants.LongValue.HeaderSize], payload);
                excess -= payload.Length;
            }

            if (rowEncoder.TrySerializeRow(tableDef, prepared, out rowLength) is { } fitted)
            {
                return (prepared, fitted);
            }
        }

        throw rowEncoder.CreateRowTooLongException(rowLength);
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

    /// <summary>
    /// Flags the row at (<paramref name="pageNumber"/>, <paramref name="rowIndex"/>)
    /// deleted. For an overflow row that slot is the header: it is flagged deleted
    /// as well as overflow (<c>0xC000</c>, as Jackcess <c>TableImpl.deleteRow</c>
    /// does), and the moved bytes, whose slot Access already flags deleted, are
    /// scrubbed with the header's pointer when <paramref name="dataMode"/> or the
    /// secure-erase policy asks for it. A slot that is already deleted is left alone.
    /// </summary>
    /// <param name="pageNumber">The page holding the row's slot.</param>
    /// <param name="rowIndex">The row's slot index.</param>
    /// <param name="tableDef">The row's table, needed to free its long values under secure erase.</param>
    /// <param name="dataMode">Whether the row's bytes are cleared.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, TableDef? tableDef, DeletedRowDataMode dataMode, CancellationToken cancellationToken)
    {
        byte[] page = await db.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        List<LongValueDescriptor>? longValueRoots = null;
        int offsetPos = db.DataPage.RowsStart + (rowIndex * 2);
        int raw = Ru16(page, offsetPos);
        if ((raw & Constants.DataPage.NonLiveRowFlags) == Constants.DataPage.OverflowRowFlag)
        {
            await this.MarkOverflowRowDeletedAsync(pageNumber, page, rowIndex, tableDef, dataMode, cancellationToken).ConfigureAwait(false);
            return;
        }

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

    /// <summary>
    /// Deletes an overflow row through its header slot on <paramref name="page"/>,
    /// which this method writes and returns to the pool.
    /// </summary>
    /// <param name="pageNumber">The page holding the header.</param>
    /// <param name="page">The header page's bytes, owned by this method.</param>
    /// <param name="rowIndex">The header's slot index.</param>
    /// <param name="tableDef">The row's table, needed to free its long values under secure erase.</param>
    /// <param name="dataMode">Whether the row's bytes are cleared.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask MarkOverflowRowDeletedAsync(long pageNumber, byte[] page, int rowIndex, TableDef? tableDef, DeletedRowDataMode dataMode, CancellationToken cancellationToken)
    {
        List<LongValueDescriptor>? longValueRoots = null;
        try
        {
            bool secureErase = options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages;
            if ((dataMode == DeletedRowDataMode.Clear || secureErase) && db.TryGetSlotBound(page, rowIndex, out RowBound header))
            {
                if (await db.TryResolveOverflowRowAsync(page, header, db.ReadPageAsync, ReturnPage, cancellationToken).ConfigureAwait(false) is { } target)
                {
                    try
                    {
                        if (tableDef is not null && secureErase)
                        {
                            longValueRoots = longValueEncoder.CollectLongValueRoots(target.Page, target.Bound, tableDef);
                        }

                        // A target on the header's own page is cleared in the
                        // header page's buffer, which is the one written below.
                        byte[] dataPage = target.PageNumber == pageNumber ? page : target.Page;
                        Array.Clear(dataPage, target.Bound.RowStart, target.Bound.RowSize);
                        if (target.PageNumber != pageNumber)
                        {
                            await db.WritePageAsync(target.PageNumber, target.Page, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        ReturnPage(target.Page);
                    }
                }

                Array.Clear(page, header.RowStart, header.RowSize);
            }

            int offsetPos = db.DataPage.RowsStart + (rowIndex * 2);
            Wu16(page, offsetPos, Ru16(page, offsetPos) | Constants.DataPage.DeletedRowFlag);
            await db.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(page);
        }

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
