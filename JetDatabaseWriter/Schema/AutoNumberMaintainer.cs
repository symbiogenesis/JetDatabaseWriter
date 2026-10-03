namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Maintains the per-table AutoNumber high-water value stored in a table's
/// TDEF page: the last value handed out, which Access and DAO continue from.
/// After rows are inserted, the counter is advanced past the largest identity
/// value the batch wrote, and it is never lowered (a schema rewrite carries it
/// over to the rebuilt TDEF), so deleting the top rows does not make their
/// values available again. <see cref="ConstraintRegistry"/> seeds a writer
/// session's next AutoNumber from it and the column's largest value, which
/// <see cref="ReadUsedHighWaterAsync"/> reads from an index on the column
/// when there is one. The counter is one unsigned 32-bit value
/// shared by every AutoNumber column of the table. ACE tables keep a second
/// counter the same way, the complex AutoNumber at
/// <see cref="Pages.TDefHeaderLayout.ComplexAutoNumber"/>: the last per-row
/// complex reference handed out to the table's complex columns. Owned by
/// <see cref="AccessWriter"/>.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
internal sealed class AutoNumberMaintainer(DatabaseFile db)
{
    /// <summary>
    /// Returns the largest value <paramref name="row"/> holds in any AutoNumber
    /// column of <paramref name="tableDef"/>, or 0 when it holds none. Complex
    /// columns carry the AutoNumber flag bit too, but their values are per-row
    /// complex references, counted by <see cref="MaxComplexReference"/>.
    /// </summary>
    /// <param name="tableDef">The table definition describing the columns.</param>
    /// <param name="row">The row values, positionally aligned with the columns.</param>
    internal static long MaxAutoNumberValue(TableDef tableDef, object[] row)
    {
        long highWater = 0;
        for (int colIndex = 0; colIndex < tableDef.Columns.Count && colIndex < row.Length; colIndex++)
        {
            ColumnInfo column = tableDef.Columns[colIndex];
            if ((column.Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0
                || column.Type is ColumnType.ComplexType or ColumnType.AttachmentType
                || row[colIndex] is null or DBNull)
            {
                continue;
            }

            if (TryGetAutoNumberCandidate(row[colIndex], out long value) && value > highWater)
            {
                highWater = value;
            }
        }

        return highWater;
    }

    /// <summary>
    /// Returns the largest per-row complex reference <paramref name="row"/>
    /// holds in any complex column of <paramref name="tableDef"/>, or 0 when
    /// it holds none.
    /// </summary>
    /// <param name="tableDef">The table definition describing the columns.</param>
    /// <param name="row">The row values, positionally aligned with the columns.</param>
    internal static long MaxComplexReference(TableDef tableDef, object[] row)
    {
        long highWater = 0;
        for (int colIndex = 0; colIndex < tableDef.Columns.Count && colIndex < row.Length; colIndex++)
        {
            if (tableDef.Columns[colIndex].Type is not (ColumnType.ComplexType or ColumnType.AttachmentType))
            {
                continue;
            }

            long value = row[colIndex] switch
            {
                ComplexIdRef reference => reference.Id,
                null or DBNull => 0,
                _ => TryGetAutoNumberCandidate(row[colIndex], out long candidate) ? candidate : 0,
            };

            highWater = Math.Max(highWater, value);
        }

        return highWater;
    }

    /// <summary>
    /// Reads the AutoNumber high-water value from the TDEF at
    /// <paramref name="tdefPage"/> through <see cref="DatabaseFile"/>, so an
    /// active transaction's pending writes are visible. Returns 0 when the
    /// page is not a TDEF.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> ReadHighWaterAsync(long tdefPage, CancellationToken cancellationToken)
    {
        if (tdefPage <= 0)
        {
            return 0;
        }

        byte[] page = await db.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            return page[0] == Constants.PageTypes.TableDefinition ? Ru32(page, db.TDef.AutoNumber) : 0;
        }
        finally
        {
            DatabaseFile.ReturnPage(page);
        }
    }

    /// <summary>
    /// Returns the largest AutoNumber value the table at
    /// <paramref name="tdefPage"/> has used in the column at
    /// <paramref name="columnIndex"/>: the larger of the TDEF counter and the
    /// largest value the column holds. A writer session's first AutoNumber
    /// follows it. The column's largest value comes from the rightmost key of
    /// the first ascending index the column leads, which takes a few page
    /// reads. With no such index, a key that does not decode, or an index
    /// that is empty while the TDEF declares rows, it comes from a scan of
    /// that column alone: data pages only, with no long-value reads. Every
    /// read goes through <see cref="DatabaseFile"/>, so an active
    /// transaction's pending writes are visible. Returns 0 when the page is
    /// not a TDEF.
    /// </summary>
    /// <remarks>
    /// The counter alone is not enough: releases before it was maintained left
    /// it at 0, and another tool can store explicit values without raising it.
    /// The index key is read without the row it points at, so a row a reader
    /// cannot decode still counts.
    /// </remarks>
    /// <param name="tdefPage">The table's TDEF page number.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="columnIndex">The AutoNumber column's index in <paramref name="tableDef"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> ReadUsedHighWaterAsync(long tdefPage, TableDef tableDef, int columnIndex, CancellationToken cancellationToken)
    {
        if (tdefPage <= 0)
        {
            return 0;
        }

        long counter;
        uint declaredRows;
        byte[] page = await db.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.TableDefinition)
            {
                return 0;
            }

            counter = Ru32(page, db.TDef.AutoNumber);
            declaredRows = Ru32(page, db.TDef.NumRows);
        }
        finally
        {
            DatabaseFile.ReturnPage(page);
        }

        if (columnIndex < 0 || columnIndex >= tableDef.Columns.Count)
        {
            return counter;
        }

        long observed = await this.TryReadIndexedMaxAsync(tdefPage, tableDef, tableDef.Columns[columnIndex], declaredRows, cancellationToken).ConfigureAwait(false)
            ?? await this.ScanColumnMaxAsync(tdefPage, tableDef, columnIndex, cancellationToken).ConfigureAwait(false);
        return Math.Max(counter, observed);
    }

    /// <summary>
    /// Scans <paramref name="rows"/> for the largest value written to any
    /// AutoNumber column and, when it exceeds the TDEF's cached counter, rewrites
    /// the high-water value at the format's <see cref="Pages.TDefHeaderLayout.AutoNumber"/> offset.
    /// No-ops when the batch is empty, declares no AutoNumber column, or wrote no
    /// value larger than the current counter.
    /// </summary>
    /// <param name="tdefPage">The owning table's TDEF page number.</param>
    /// <param name="tableDef">The table definition describing the columns.</param>
    /// <param name="rows">The rows that were just inserted.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask UpdateHighWaterAsync(long tdefPage, TableDef tableDef, List<object[]> rows, CancellationToken cancellationToken)
    {
        long highWater = 0;
        foreach (object[] row in rows)
        {
            highWater = Math.Max(highWater, MaxAutoNumberValue(tableDef, row));
        }

        await this.RaiseHighWaterAsync(tdefPage, highWater, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Raises the TDEF counter at <paramref name="tdefPage"/> to
    /// <paramref name="highWater"/> (saturating at <see cref="uint.MaxValue"/>).
    /// Leaves it unchanged when it already holds that value or more.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page number.</param>
    /// <param name="highWater">The value the counter must be at least.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask RaiseHighWaterAsync(long tdefPage, long highWater, CancellationToken cancellationToken)
        => this.RaiseCounterAsync(tdefPage, db.TDef.AutoNumber, highWater, cancellationToken);

    /// <summary>
    /// Reads the complex AutoNumber (the last per-row complex reference handed
    /// out) from the TDEF at <paramref name="tdefPage"/> through
    /// <see cref="DatabaseFile"/>, so an active transaction's pending writes
    /// are visible. Returns 0 on Jet3 and Jet4, which have no such counter,
    /// and when the page is not a TDEF.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> ReadComplexHighWaterAsync(long tdefPage, CancellationToken cancellationToken)
    {
        int offset = db.TDef.ComplexAutoNumber;
        if (offset < 0 || tdefPage <= 0)
        {
            return 0;
        }

        byte[] page = await db.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            return page[0] == Constants.PageTypes.TableDefinition ? Ru32(page, offset) : 0;
        }
        finally
        {
            DatabaseFile.ReturnPage(page);
        }
    }

    /// <summary>
    /// Raises the TDEF complex AutoNumber at <paramref name="tdefPage"/> to
    /// <paramref name="highWater"/>. Leaves it unchanged when it already holds
    /// that value or more, and does nothing on Jet3 and Jet4.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page number.</param>
    /// <param name="highWater">The value the counter must be at least.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask RaiseComplexHighWaterAsync(long tdefPage, long highWater, CancellationToken cancellationToken)
        => db.TDef.ComplexAutoNumber < 0
            ? default
            : this.RaiseCounterAsync(tdefPage, db.TDef.ComplexAutoNumber, highWater, cancellationToken);

    /// <summary>
    /// Raises the TDEF complex AutoNumber at <paramref name="tdefPage"/> to the
    /// largest per-row complex reference <paramref name="rows"/> wrote.
    /// Returns without reading any page when the table has no complex column
    /// or the batch wrote no reference.
    /// </summary>
    /// <param name="tdefPage">The owning table's TDEF page number.</param>
    /// <param name="tableDef">The table definition describing the columns.</param>
    /// <param name="rows">The rows that were just written.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask UpdateComplexHighWaterAsync(long tdefPage, TableDef tableDef, List<object[]> rows, CancellationToken cancellationToken)
    {
        if (!tableDef.HasComplexColumns)
        {
            return default;
        }

        long highWater = 0;
        foreach (object[] row in rows)
        {
            highWater = Math.Max(highWater, MaxComplexReference(tableDef, row));
        }

        return highWater > 0 ? this.RaiseComplexHighWaterAsync(tdefPage, highWater, cancellationToken) : default;
    }

    private static bool TryGetAutoNumberCandidate(object boxed, out long value)
    {
        // AutoNumber columns are always Long Integer identity values, but the boxed
        // row payload may carry any integer-family CLR type (or a numeric string)
        // depending on how the caller supplied the row. Resolve the candidate
        // without throwing so an unexpected non-integer value is skipped
        // deterministically rather than masked by an empty catch around Convert.ToInt64.
        switch (boxed)
        {
            case int i:
                value = i;
                return true;
            case long l:
                value = l;
                return true;
            case short s:
                value = s;
                return true;
            case byte b:
                value = b;
                return true;
            case sbyte sb:
                value = sb;
                return true;
            case ushort us:
                value = us;
                return true;
            case uint ui:
                value = ui;
                return true;
            case ulong ul when ul <= long.MaxValue:
                value = (long)ul;
                return true;
            case string text when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed):
                value = parsed;
                return true;
            default:
                value = 0;
                return false;
        }
    }

    /// <summary>
    /// Returns the largest value in <paramref name="column"/> from the
    /// rightmost key of the first ascending index the column leads, 0 when
    /// that index is empty and the TDEF declares no rows, or
    /// <see langword="null"/> when the column must be scanned instead: it is
    /// not an <c>Integer</c> or <c>Long Integer</c>, no such index exists, the
    /// index section does not parse, the key does not decode, or the index is
    /// empty while the TDEF declares rows.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page number.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="column">The AutoNumber column.</param>
    /// <param name="declaredRows">The TDEF's row count.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<long?> TryReadIndexedMaxAsync(long tdefPage, TableDef tableDef, ColumnInfo column, uint declaredRows, CancellationToken cancellationToken)
    {
        if (column.Type is not (ColumnType.IntegerType or ColumnType.LongIntegerType))
        {
            return null;
        }

        byte[]? tdefBytes = await db.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (tdefBytes is null || tdefBytes.Length < db.TDef.BlockEnd)
        {
            return null;
        }

        List<IndexMetadata> indexes;
        try
        {
            indexes = IndexCatalogReader.ReadMetadata(db, tdefBytes, tableDef.Columns);
        }
        catch (Exception ex) when (ex is InvalidDataException or ArgumentException)
        {
            return null;
        }

        IndexMetadata? index = indexes.Find(i => i.FirstDp > 0
            && i.Columns.Count > 0
            && i.Columns[0].IsAscending
            && i.Columns[0].ColumnNumber == column.ColNum);
        if (index is null)
        {
            return null;
        }

        var cursor = new IndexCursor(IndexPageLayout.ForFormat(db.Format), db.ReadPageCopyAsync, db.PageSizeBytes);
        IndexEntry? last = await cursor.TryReadLastEntryAsync(index.FirstDp, cancellationToken).ConfigureAwait(false);
        if (last is null)
        {
            return declaredRows == 0 ? 0 : null;
        }

        return IndexKeyEncoder.TryDecodeIntegralKey(column.Type, last.Value.Key, out long value) ? value : null;
    }

    /// <summary>
    /// Returns the largest value in the column at <paramref name="columnIndex"/>
    /// over every live row, decoding that column alone from the data pages, or
    /// 0 when no row holds one.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page number.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="columnIndex">The column's index in <paramref name="tableDef"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<long> ScanColumnMaxAsync(long tdefPage, TableDef tableDef, int columnIndex, CancellationToken cancellationToken)
    {
        var plan = RowDecodePlan.CreatePartial(tableDef, [columnIndex]);
        object?[] cell = new object?[1];
        long max = 0;
        await db.ForEachLiveTableRowAsync(
            tdefPage,
            (row, _) =>
            {
                if (row.Location.RowSize >= db.RowFields.NumCols
                    && plan.TryDecodePartialColumns(db, row.Page, row.Location.RowStart, row.Location.RowSize, cell)
                    && cell[0] is { } boxed
                    && TryGetAutoNumberCandidate(boxed, out long value)
                    && value > max)
                {
                    max = value;
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return max;
    }

    private async ValueTask RaiseCounterAsync(long tdefPage, int offset, long highWater, CancellationToken cancellationToken)
    {
        if (highWater <= 0)
        {
            return;
        }

        byte[] page = await db.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            uint current = Ru32(page, offset);
            uint next = highWater >= uint.MaxValue ? uint.MaxValue : (uint)highWater;
            if (next <= current)
            {
                return;
            }

            Wi32(page, offset, unchecked((int)next));
            await db.WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            DatabaseFile.ReturnPage(page);
        }
    }
}
