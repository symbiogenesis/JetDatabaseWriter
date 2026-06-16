namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Maintains the per-table AutoNumber high-water value stored in a table's
/// TDEF page. After rows are inserted, the cached "next AutoNumber" counter is
/// advanced past the largest identity value the batch wrote so Access never
/// reissues a value that already exists on disk. Owned by
/// <see cref="AccessWriter"/>.
/// </summary>
/// <param name="writer">The writer.</param>
internal sealed class AutoNumberMaintainer(AccessWriter writer)
{
    /// <summary>
    /// Scans <paramref name="rows"/> for the largest value written to any
    /// AutoNumber column and, when it exceeds the TDEF's cached counter, rewrites
    /// the high-water value at <see cref="Constants.TableDefinition.AutoNumberOffset"/>.
    /// No-ops when the batch is empty, declares no AutoNumber column, or wrote no
    /// value larger than the current counter.
    /// </summary>
    /// <param name="tdefPage">The owning table's TDEF page number.</param>
    /// <param name="tableDef">The table definition describing the columns.</param>
    /// <param name="rows">The rows that were just inserted.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask UpdateHighWaterAsync(long tdefPage, TableDef tableDef, List<object[]> rows, CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        long highWater = 0;
        for (int colIndex = 0; colIndex < tableDef.Columns.Count; colIndex++)
        {
            ColumnInfo column = tableDef.Columns[colIndex];
            if ((column.Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0)
            {
                continue;
            }

            foreach (object[] row in rows)
            {
                if (colIndex >= row.Length || row[colIndex] is null || row[colIndex] is DBNull)
                {
                    continue;
                }

                if (TryGetAutoNumberCandidate(row[colIndex], out long value) && value > highWater)
                {
                    highWater = value;
                }
            }
        }

        if (highWater <= 0)
        {
            return;
        }

        byte[] page = await writer.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            uint current = Ru32(page, Constants.TableDefinition.AutoNumberOffset);
            uint next = highWater >= uint.MaxValue ? uint.MaxValue : (uint)highWater;
            if (next <= current)
            {
                return;
            }

            Wi32(page, Constants.TableDefinition.AutoNumberOffset, unchecked((int)next));
            await writer.WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            AccessBase.ReturnPage(page);
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
