namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Maintains the per-table AutoNumber high-water value stored in a table's
/// TDEF page: the last value handed out, which Access and DAO continue from.
/// After rows are inserted, the counter is advanced past the largest identity
/// value the batch wrote, and it is never lowered (a schema rewrite carries it
/// over to the rebuilt TDEF), so deleting the top rows does not make their
/// values available again. <see cref="ConstraintRegistry"/> seeds a writer
/// session's next AutoNumber from it. The counter is one unsigned 32-bit value
/// shared by every AutoNumber column of the table. Owned by
/// <see cref="AccessWriter"/>.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
internal sealed class AutoNumberMaintainer(DatabaseFile db)
{
    /// <summary>
    /// Returns the largest value <paramref name="row"/> holds in any AutoNumber
    /// column of <paramref name="tableDef"/>, or 0 when it holds none.
    /// </summary>
    /// <param name="tableDef">The table definition describing the columns.</param>
    /// <param name="row">The row values, positionally aligned with the columns.</param>
    internal static long MaxAutoNumberValue(TableDef tableDef, object[] row)
    {
        long highWater = 0;
        for (int colIndex = 0; colIndex < tableDef.Columns.Count && colIndex < row.Length; colIndex++)
        {
            if ((tableDef.Columns[colIndex].Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0
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
    internal async ValueTask RaiseHighWaterAsync(long tdefPage, long highWater, CancellationToken cancellationToken)
    {
        if (highWater <= 0)
        {
            return;
        }

        byte[] page = await db.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            uint current = Ru32(page, db.TDef.AutoNumber);
            uint next = highWater >= uint.MaxValue ? uint.MaxValue : (uint)highWater;
            if (next <= current)
            {
                return;
            }

            Wi32(page, db.TDef.AutoNumber, unchecked((int)next));
            await db.WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            DatabaseFile.ReturnPage(page);
        }
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
}
