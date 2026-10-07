namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;

/// <summary>
/// Read-only <c>MSysObjects</c> scans, and the one system-table lookup by name
/// that the reader's <see cref="CatalogReader"/> and the writer-side services
/// share. Depends only on the format profile, the TDEF reader and the owned-page
/// walk, so every catalog writer, the data-page inserter, and the feature
/// managers can share it without depending on each other.
/// </summary>
/// <param name="format">The file's format profile, which decodes the catalog columns.</param>
/// <param name="tableDefs">Reads the <c>MSysObjects</c> table definition.</param>
/// <param name="ownedPages">Walks the rows of <c>MSysObjects</c>.</param>
internal sealed class CatalogRowReader(JetFormat format, TableDefReader tableDefs, OwnedDataPages ownedPages)
{
    /// <summary>
    /// Scans all data pages belonging to <c>MSysObjects</c> (TDEF page 2) and
    /// returns a decoded row for each live catalog entry.
    /// </summary>
    /// <param name="msys">The <c>MSysObjects</c> table definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetCorruptDataException">The catalog is missing a required field or a live row has malformed required values.</exception>
    internal async ValueTask<List<CatalogRow>> GetCatalogRowsAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo? idColumn = msys.FindColumn("Id");
        ColumnInfo? parentIdColumn = msys.FindColumn("ParentId");
        ColumnInfo? nameColumn = msys.FindColumn("Name");
        ColumnInfo? typeColumn = msys.FindColumn("Type");
        ColumnInfo? flagsColumn = msys.FindColumn("Flags");
        if (idColumn == null || nameColumn == null || typeColumn == null || flagsColumn == null)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects is missing a required Id, Name, Type or Flags column.");
        }

        var result = new List<CatalogRow>();
        await ownedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                byte[] page = row.Page;
                RowLocation location = row.Location;
                if (!this.CanDecodeRow(page, location))
                {
                    throw CorruptRow(location, "The catalog row layout cannot be decoded.");
                }

                string name = ScalarColumnReader.DecodeSimpleColumnValue(format, page, location.RowStart, location.RowSize, nameColumn);
                if (string.IsNullOrEmpty(name)
                    || !CatalogValueReader.TryParseInt64(ScalarColumnReader.DecodeSimpleColumnValue(format, page, location.RowStart, location.RowSize, idColumn), out long id)
                    || !CatalogValueReader.TryParseInt32(ScalarColumnReader.DecodeSimpleColumnValue(format, page, location.RowStart, location.RowSize, typeColumn), out int objectType)
                    || !CatalogValueReader.TryParseInt64(ScalarColumnReader.DecodeSimpleColumnValue(format, page, location.RowStart, location.RowSize, flagsColumn), out long flags))
                {
                    throw CorruptRow(location, "A required catalog row value is missing or malformed.");
                }

                long parentId = parentIdColumn is null
                    ? 0
                    : CatalogValueReader.ParseInt64OrZero(ScalarColumnReader.DecodeSimpleColumnValue(format, page, location.RowStart, location.RowSize, parentIdColumn));
                result.Add(new CatalogRow(
                    PageNumber: location.PageNumber,
                    RowIndex: location.RowIndex,
                    Name: name,
                    ObjectType: objectType,
                    Flags: flags,
                    TDefPage: CatalogValueReader.TdefPageFromId(id),
                    Id: id,
                    ParentId: parentId,
                    IsDecoded: true));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return result;
    }

    /// <summary>Validates all local table references, including hidden system tables and ODBC definitions.</summary>
    /// <param name="rows">The decoded catalog rows.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask ValidateTableReferencesAsync(List<CatalogRow> rows, CancellationToken cancellationToken)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var roots = new HashSet<long>();
        foreach (CatalogRow row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (row.ObjectType != Constants.SystemObjects.UserTableType
                && row.ObjectType != Constants.SystemObjects.LinkedOdbcType)
            {
                continue;
            }

            bool isCatalog = string.Equals(row.Name, Constants.SystemTableNames.Objects, StringComparison.OrdinalIgnoreCase);
            if (isCatalog && row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptCatalog,
                    "The reserved MSysObjects name must identify the local catalog table.",
                    new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, PageNumber = row.PageNumber });
            }

            if (!names.Add(row.Name))
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptCatalog,
                    "A catalog table name is ambiguous.",
                    new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, PageNumber = row.PageNumber });
            }

            // Negative ODBC IDs identify catalog-only links whose schema lives in LvProp.
            // Their low bits are object identities, not physical table-definition pointers.
            if (row.ObjectType == Constants.SystemObjects.LinkedOdbcType && row.Id < 0)
            {
                continue;
            }

            if (row.TDefPage < 2 || row.TDefPage >= tableDefs.PageCount
                || (isCatalog ? row.TDefPage != 2 : row.TDefPage == 2)
                || !roots.Add(row.TDefPage)
                || await tableDefs.ReadTableDefAsync(row.TDefPage, cancellationToken).ConfigureAwait(false) is null)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptCatalog,
                    "A catalog table reference is invalid or ambiguous.",
                    new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, PageNumber = row.PageNumber });
            }
        }
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
    /// when no catalog row names it. Returns <c>0</c> when not found.
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
    /// Returns the TDEF page of the unique <c>MSysObjects</c> row whose name satisfies
    /// <paramref name="nameMatches"/> and that is a local table (type 1) or, with
    /// <paramref name="includeLinkedOdbc"/>, a linked ODBC table (type 4). Malformed required row values are refused.
    /// Returns <c>0</c> when none matches.
    /// </summary>
    /// <param name="nameMatches">The name test.</param>
    /// <param name="includeLinkedOdbc">Whether linked ODBC tables, which carry a local TDEF, also match.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetCorruptDataException">The catalog definition is unreadable.</exception>
    internal async ValueTask<long> FindTableTdefPageAsync(Predicate<string> nameMatches, bool includeLinkedOdbc, CancellationToken cancellationToken)
    {
        TableDef msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false)
            ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The MSysObjects catalog table definition could not be read.");

        List<CatalogRow> rows = await this.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        await this.ValidateTableReferencesAsync(rows, cancellationToken).ConfigureAwait(false);
        long match = 0;
        foreach (CatalogRow row in rows)
        {
            bool isTable = row.ObjectType == Constants.SystemObjects.UserTableType
                || (includeLinkedOdbc && row.ObjectType == Constants.SystemObjects.LinkedOdbcType && row.Id >= 0);
            if (isTable && row.IsDecoded && row.TDefPage > 0 && nameMatches(row.Name))
            {
                if (match != 0)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The catalog table lookup is ambiguous.");
                }

                match = row.TDefPage;
            }
        }

        return match;
    }

    private static JetCorruptDataException CorruptRow(RowLocation location, string reason)
        => new(JetErrorCode.CorruptCatalog, reason, new JetErrorInfo
        {
            TableName = Constants.SystemTableNames.Objects,
            PageNumber = location.PageNumber,
            Reason = reason,
        });

    /// <summary>
    /// Applies the table reader's skip rules: a row shorter than the column-count
    /// field, with a zero column count, or whose null mask and variable-column
    /// offsets do not fit the row cannot be decoded.
    /// </summary>
    /// <param name="page">The data page holding the row.</param>
    /// <param name="location">The row's location on <paramref name="page"/>.</param>
    private bool CanDecodeRow(byte[] page, RowLocation location)
        => location.RowSize >= format.RowFields.NumCols
            && format.ReadRowColumnCount(page, location.RowStart) != 0
            && RowDecodePlan.TryParseRowLayout(format.RowFields, page, location.RowStart, location.RowSize, hasVarColumns: true, out _);
}
