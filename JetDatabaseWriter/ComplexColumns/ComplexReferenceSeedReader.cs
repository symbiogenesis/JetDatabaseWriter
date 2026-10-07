namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Reads and validates the persisted counter used to allocate per-row
/// complex references. A parent row's complex slot (the
/// 4-byte value of an Attachment / multi-value / version-history column)
/// joins it to its rows in each complex column's hidden flat table. Access
/// gives every row one reference, shared by all its complex columns, from the
/// TDEF complex AutoNumber at <see cref="Pages.TDefHeaderLayout.ComplexAutoNumber"/>.
/// Existing parent slots and flat-table foreign keys must not exceed that
/// counter; an inconsistent file is refused without repairing its identities.
/// Every read uses the writer's page source, so an active transaction's
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
        if (rowStart < 0 || rowStart > page.Length
            || rowSize < format.RowFields.NumCols || rowSize > page.Length - rowStart
            || column.FixedOff < 0 || column.ColNum < 0)
        {
            return false;
        }

        int columnCount = format.ReadRowColumnCount(page, rowStart);
        int nullMaskSize = GetNullMaskSizeBytes(columnCount);
        int fixedAreaSize = rowSize - format.RowFields.NumCols - nullMaskSize;
        if (column.ColNum >= columnCount
            || column.FixedOff > fixedAreaSize - 4
            || !IsNullMaskBitSet(page.AsSpan(rowStart + rowSize - nullMaskSize, nullMaskSize), column.ColNum))
        {
            return false;
        }

        int slotOffset = rowStart + format.RowFields.NumCols + column.FixedOff;
        reference = Ri32(page, slotOffset);
        return reference > 0;
    }

    /// <summary>
    /// Returns the TDEF complex AutoNumber for the table at
    /// <paramref name="parentTdefPage"/>, after checking that every live
    /// parent slot and flat-table foreign key is at or below it. The next
    /// reference is one more; an inconsistent counter is never repaired.
    /// </summary>
    /// <param name="parentTdefPage">The table's TDEF page.</param>
    /// <param name="parentDef">The table definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetCorruptDataException">An existing complex reference is invalid, repeated in a parent column, or exceeds the persisted counter, or its flat-table metadata cannot be read.</exception>
    internal async ValueTask<long> ReadSeedAsync(long parentTdefPage, TableDef parentDef, CancellationToken cancellationToken)
    {
        long seed = await autoNumbers.ReadComplexHighWaterAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
        var complexColumns = parentDef.Columns.Where(c => c.Type is ComplexType).ToList();
        if (complexColumns.Count == 0)
        {
            return seed;
        }

        var usedReferences = new HashSet<int>[complexColumns.Count];
        for (int columnIndex = 0; columnIndex < usedReferences.Length; columnIndex++)
        {
            usedReferences[columnIndex] = [];
        }
        await ownedPages.ForEachLiveTableRowAsync(
            parentTdefPage,
            (row, _) =>
            {
                for (int columnIndex = 0; columnIndex < complexColumns.Count; columnIndex++)
                {
                    ColumnInfo column = complexColumns[columnIndex];
                    if (!TryReadSlot(format, row.Page, row.Location.RowStart, row.Location.RowSize, column, out int reference))
                    {
                        throw new JetCorruptDataException(
                            JetErrorCode.CorruptComplexColumn,
                            $"Parent row has no valid complex reference for '{column.Name}'.",
                            new JetErrorInfo { ColumnName = column.Name, PageNumber = row.Location.DataPageNumber });
                    }

                    if (reference > seed)
                    {
                        throw new JetCorruptDataException(
                            JetErrorCode.CorruptComplexColumn,
                            $"Complex reference {reference} for '{column.Name}' exceeds the parent TDEF complex AutoNumber counter {seed}.",
                            new JetErrorInfo { ColumnName = column.Name, PageNumber = parentTdefPage });
                    }

                    if (!usedReferences[columnIndex].Add(reference))
                    {
                        throw new JetCorruptDataException(
                            JetErrorCode.CorruptComplexColumn,
                            $"Parent rows reuse complex reference {reference} for '{column.Name}'.",
                            new JetErrorInfo { ColumnName = column.Name, PageNumber = row.Location.DataPageNumber });
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
                ?? flatDef?.Columns.FirstOrDefault(c => c.Type == LongIntegerType && !c.IsAutoNumber && c.Name.StartsWith('_'));
            if (foreignKey is null || foreignKey.Type != LongIntegerType || foreignKey.IsAutoNumber)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptComplexColumn,
                    $"Complex column '{column.Name}' has no readable flat table with a Long Integer foreign-key column.",
                    new JetErrorInfo { ColumnName = column.Name, PageNumber = parentTdefPage });
            }

            await ownedPages.ForEachLiveTableRowAsync(
                flatTdefPage,
                (row, _) =>
                {
                    string text = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, foreignKey);
                    if (!CatalogValueReader.TryParseInt32(text, out int reference) || reference <= 0)
                    {
                        throw new JetCorruptDataException(
                            JetErrorCode.CorruptComplexColumn,
                            $"Flat table for '{column.Name}' contains an invalid complex foreign-key reference.",
                            new JetErrorInfo { ColumnName = column.Name, PageNumber = row.Location.DataPageNumber });
                    }

                    if (reference > seed)
                    {
                        throw new JetCorruptDataException(
                            JetErrorCode.CorruptComplexColumn,
                            $"Flat-table reference {reference} for '{column.Name}' exceeds the parent TDEF complex AutoNumber counter {seed}.",
                            new JetErrorInfo { ColumnName = column.Name, PageNumber = parentTdefPage });
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
