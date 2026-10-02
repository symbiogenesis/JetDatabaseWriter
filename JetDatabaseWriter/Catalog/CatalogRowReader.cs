namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Read-only <c>MSysObjects</c> scans for the writer-side services. Depends
/// only on page I/O, so every catalog writer, the data-page inserter, and the
/// feature managers can share it without depending on each other.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
internal sealed class CatalogRowReader(AccessBase db)
{
    /// <summary>
    /// Scans all data pages belonging to <c>MSysObjects</c> (TDEF page 2) and
    /// returns a decoded row for each live catalog entry.
    /// </summary>
    /// <param name="msys">The <c>MSysObjects</c> table definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<List<CatalogRow>> GetCatalogRowsAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo? idColumn = msys.FindColumn("Id");
        ColumnInfo? parentIdColumn = msys.FindColumn("ParentId");
        ColumnInfo? nameColumn = msys.FindColumn("Name");
        ColumnInfo? typeColumn = msys.FindColumn("Type");
        ColumnInfo? flagsColumn = msys.FindColumn("Flags");
        if (nameColumn == null || typeColumn == null)
        {
            return [];
        }

        var result = new List<CatalogRow>();
        await db.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                byte[] page = row.Page;
                RowLocation location = row.Location;
                long id = idColumn is null
                    ? 0
                    : CatalogValueReader.ParseInt64OrZero(db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, idColumn));
                long parentId = parentIdColumn is null
                    ? 0
                    : CatalogValueReader.ParseInt64OrZero(db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, parentIdColumn));

                result.Add(new CatalogRow(
                    PageNumber: location.PageNumber,
                    RowIndex: location.RowIndex,
                    Name: db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, nameColumn),
                    ObjectType: CatalogValueReader.ParseInt32OrZero(db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, typeColumn)),
                    Flags: CatalogValueReader.ParseInt64OrZero(db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, flagsColumn!)),
                    TDefPage: CatalogValueReader.TdefPageFromId(id),
                    Id: id,
                    ParentId: parentId));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return result;
    }

    /// <summary>
    /// Locates a system or user table's TDEF page number by name (case-insensitive)
    /// by scanning every <c>MSysObjects</c> row. Returns <c>0</c> when not found.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> FindSystemTableTdefPageAsync(string tableName, CancellationToken cancellationToken)
    {
        TableDef? msys = await db.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        List<CatalogRow> rows = await this.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == Constants.SystemObjects.UserTableType
                && row.TDefPage > 0
                && string.Equals(row.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return row.TDefPage;
            }
        }

        return 0;
    }
}
