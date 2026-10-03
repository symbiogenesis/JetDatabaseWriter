namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Read-only <c>MSysObjects</c> scans, and the one system-table lookup by name
/// that the reader's <see cref="CatalogReader"/> and the writer-side services
/// share. Depends only on page I/O, so every catalog writer, the data-page
/// inserter, and the feature managers can share it without depending on each other.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
internal sealed class CatalogRowReader(DatabaseFile db)
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
                    ParentId: parentId,
                    IsDecoded: this.CanDecodeRow(page, location)));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return result;
    }

    /// <summary>
    /// Locates a local system or user table's TDEF page number by name
    /// (case-insensitive). Returns <c>0</c> when not found.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<long> FindSystemTableTdefPageAsync(string tableName, CancellationToken cancellationToken)
        => this.FindSystemTableTdefPageAsync(tableName, includeLinkedOdbc: false, cancellationToken);

    /// <summary>
    /// Locates a system or user table's TDEF page number by name (case-insensitive)
    /// through <see cref="FindTableTdefPageAsync"/>. <c>MSysObjects</c> itself is
    /// always TDEF page 2 (Jackcess <c>PAGE_SYSTEM_CATALOG</c>), so it resolves there
    /// when no catalog row names it, as in the Jet3, Jet4 and slim-catalog ACCDB files
    /// the writer creates. Returns <c>0</c> when not found.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="includeLinkedOdbc">
    /// Whether a linked ODBC table (<c>MSysObjects</c> type 4), which carries a local
    /// TDEF with the remote table's columns, also matches.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> FindSystemTableTdefPageAsync(string tableName, bool includeLinkedOdbc, CancellationToken cancellationToken)
    {
        long tdefPage = await this.FindTableTdefPageAsync(
            name => string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase),
            includeLinkedOdbc,
            cancellationToken).ConfigureAwait(false);
        return tdefPage == 0 && string.Equals(tableName, Constants.SystemTableNames.Objects, StringComparison.OrdinalIgnoreCase)
            ? 2
            : tdefPage;
    }

    /// <summary>
    /// Returns the TDEF page of the first <c>MSysObjects</c> row whose name satisfies
    /// <paramref name="nameMatches"/> and that is a local table (type 1) or, with
    /// <paramref name="includeLinkedOdbc"/>, a linked ODBC table (type 4). Rows the
    /// scan cannot decode are skipped. Returns <c>0</c> when none matches.
    /// </summary>
    /// <param name="nameMatches">The name test.</param>
    /// <param name="includeLinkedOdbc">Whether linked ODBC tables, which carry a local TDEF, also match.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> FindTableTdefPageAsync(Predicate<string> nameMatches, bool includeLinkedOdbc, CancellationToken cancellationToken)
    {
        TableDef? msys = await db.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        List<CatalogRow> rows = await this.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            bool isTable = row.ObjectType == Constants.SystemObjects.UserTableType
                || (includeLinkedOdbc && row.ObjectType == Constants.SystemObjects.LinkedOdbcType);
            if (isTable && row.IsDecoded && row.TDefPage > 0 && nameMatches(row.Name))
            {
                return row.TDefPage;
            }
        }

        return 0;
    }

    /// <summary>
    /// Applies the table reader's skip rules: a row shorter than the column-count
    /// field, with a zero column count, or whose null mask and variable-column
    /// offsets do not fit the row cannot be decoded.
    /// </summary>
    /// <param name="page">The data page holding the row.</param>
    /// <param name="location">The row's location on <paramref name="page"/>.</param>
    private bool CanDecodeRow(byte[] page, RowLocation location)
        => location.RowSize >= db.RowColumnCountFieldSize
            && db.ReadRowColumnCount(page, location.RowStart) != 0
            && db.TryParseRowLayout(page, location.RowStart, location.RowSize, hasVarColumns: true, out _);
}
