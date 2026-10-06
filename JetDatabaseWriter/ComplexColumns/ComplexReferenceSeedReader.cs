namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Reads the per-row complex references a table already uses, so a new one
/// can be allocated above all of them. A parent row's complex slot (the
/// 4-byte value of an Attachment / multi-value / version-history column)
/// joins it to its rows in each complex column's hidden flat table. Access
/// gives every row one reference, shared by all its complex columns, from the
/// TDEF complex AutoNumber at <see cref="Pages.TDefHeaderLayout.ComplexAutoNumber"/>.
/// The seed also covers the references a file actually holds, because files
/// written by earlier builds of this library leave that counter at 0. Every
/// read uses the writer's page source, so an active transaction's
/// pending writes are visible.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="ownedPages">The database's owned-page discovery and row walks.</param>
/// <param name="catalogRows">Locates <c>MSysComplexColumns</c>.</param>
/// <param name="autoNumbers">Reads the TDEF complex AutoNumber.</param>
internal sealed class ComplexReferenceSeedReader(JetFormat format, TableDefReader tableDefs, OwnedDataPages ownedPages, CatalogRowReader catalogRows, AutoNumberMaintainer autoNumbers)
{
    /// <summary>
    /// Reads the reference held in <paramref name="column"/>'s slot of the row
    /// at <paramref name="rowStart"/>. Returns <see langword="false"/> when the
    /// slot is null (its null-mask bit is clear), holds 0 or a negative value,
    /// or lies outside the row.
    /// </summary>
    /// <param name="format">The database's immutable format profile.</param>
    /// <param name="page">The data page holding the row.</param>
    /// <param name="rowStart">The row's offset on the page.</param>
    /// <param name="rowSize">The row's size.</param>
    /// <param name="column">The complex column.</param>
    /// <param name="reference">The reference, when one is set.</param>
    internal static bool TryReadSlot(JetFormat format, byte[] page, int rowStart, int rowSize, ColumnInfo column, out int reference)
    {
        reference = 0;
        int nullMaskSize = GetNullMaskSizeBytes(format.ReadRowColumnCount(page, rowStart));
        int slotOffset = rowStart + format.RowFields.NumCols + column.FixedOff;
        if (nullMaskSize > rowSize
            || !IsNullMaskBitSet(page.AsSpan(rowStart + rowSize - nullMaskSize, nullMaskSize), column.ColNum)
            || slotOffset + 4 > rowStart + rowSize)
        {
            return false;
        }

        reference = Ri32(page, slotOffset);
        return reference > 0;
    }

    /// <summary>
    /// Stores <paramref name="reference"/> in <paramref name="column"/>'s slot
    /// of the row at <paramref name="rowStart"/> and marks the slot non-null.
    /// The caller writes the page back.
    /// </summary>
    /// <param name="format">The database's immutable format profile.</param>
    /// <param name="page">The data page holding the row.</param>
    /// <param name="rowStart">The row's offset on the page.</param>
    /// <param name="rowSize">The row's size.</param>
    /// <param name="column">The complex column.</param>
    /// <param name="reference">The reference to store.</param>
    /// <exception cref="InvalidDataException">The slot lies outside the row.</exception>
    internal static void WriteSlot(JetFormat format, byte[] page, int rowStart, int rowSize, ColumnInfo column, int reference)
    {
        int nullMaskSize = GetNullMaskSizeBytes(format.ReadRowColumnCount(page, rowStart));
        int slotOffset = rowStart + format.RowFields.NumCols + column.FixedOff;
        if (slotOffset + 4 > rowStart + rowSize)
        {
            throw new InvalidDataException("Complex column slot is out of row bounds.");
        }

        Wi32(page, slotOffset, reference);
        SetNullMaskBit(page.AsSpan(rowStart + rowSize - nullMaskSize, nullMaskSize), column.ColNum, true);
    }

    /// <summary>
    /// Returns the largest per-row complex reference the table at
    /// <paramref name="parentTdefPage"/> has used: the larger of its TDEF
    /// complex AutoNumber, every live row's complex slots, and the foreign keys
    /// in each complex column's flat table. The next reference is one more.
    /// </summary>
    /// <param name="parentTdefPage">The table's TDEF page.</param>
    /// <param name="parentDef">The table definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> ReadSeedAsync(long parentTdefPage, TableDef parentDef, CancellationToken cancellationToken)
    {
        long seed = await autoNumbers.ReadComplexHighWaterAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
        List<ColumnInfo> complexColumns = parentDef.Columns.Where(c => c.Type is AttachmentType or ComplexType).ToList();
        if (complexColumns.Count == 0)
        {
            return seed;
        }

        await ownedPages.ForEachLiveTableRowAsync(
            parentTdefPage,
            (row, _) =>
            {
                foreach (ColumnInfo column in complexColumns)
                {
                    if (TryReadSlot(format, row.Page, row.Location.RowStart, row.Location.RowSize, column, out int reference))
                    {
                        seed = Math.Max(seed, reference);
                    }
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        foreach (ColumnInfo column in complexColumns)
        {
            long flatTdefPage = await this.ResolveFlatTableTdefPageAsync(column.Name, column.Misc, cancellationToken).ConfigureAwait(false);
            TableDef? flatDef = flatTdefPage > 0
                ? await tableDefs.ReadTableDefAsync(flatTdefPage, cancellationToken).ConfigureAwait(false)
                : null;
            ColumnInfo? foreignKey = flatDef?.FindColumn("_" + column.Name)
                ?? flatDef?.Columns.FirstOrDefault(c => c.Type == LongIntegerType && c.Name.StartsWith('_'));
            if (foreignKey is null)
            {
                continue;
            }

            await ownedPages.ForEachLiveTableRowAsync(
                flatTdefPage,
                (row, _) =>
                {
                    string text = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, foreignKey);
                    if (CatalogValueReader.TryParseInt32(text, out int reference))
                    {
                        seed = Math.Max(seed, reference);
                    }

                    return new ValueTask<bool>(true);
                },
                cancellationToken).ConfigureAwait(false);
        }

        return seed;
    }

    /// <summary>
    /// Looks up <c>MSysComplexColumns</c> for a row matching both
    /// <paramref name="columnName"/> and <paramref name="complexId"/> and returns
    /// the lower-24-bit TDEF page number of the hidden flat child table, or 0
    /// when there is none.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <param name="complexId">The complex id, or 0 to match on the name alone.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> ResolveFlatTableTdefPageAsync(string columnName, int complexId, CancellationToken cancellationToken)
    {
        long msysPg = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysPg == 0)
        {
            return 0;
        }

        TableDef msys = await tableDefs.ReadRequiredTableDefAsync(msysPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msys.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msys.FindColumn("FlatTableID");
        ColumnInfo? complexIdCol = msys.FindColumn("ComplexID");
        if (nameCol == null || flatIdCol == null || complexIdCol == null)
        {
            return 0;
        }

        long flatTdefPage = 0;
        await ownedPages.ForEachLiveTableRowAsync(
            msysPg,
            (row, _) =>
            {
                string rowName = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, nameCol);
                if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return new ValueTask<bool>(true);
                }

                string idText = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, complexIdCol);
                if (complexId != 0 && (!CatalogValueReader.TryParseInt32(idText, out int rid) || rid != complexId))
                {
                    return new ValueTask<bool>(true);
                }

                string flatText = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, flatIdCol);
                if (!CatalogValueReader.TryParseInt64(flatText, out long flatId))
                {
                    return new ValueTask<bool>(true);
                }

                flatTdefPage = CatalogValueReader.TdefPageFromId(flatId);
                return new ValueTask<bool>(false);
            },
            cancellationToken).ConfigureAwait(false);

        return flatTdefPage;
    }
}
