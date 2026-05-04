namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

#pragma warning disable CA1822 // Mark members as static

/// <summary>
/// Catalog (MSysObjects) write operations for <see cref="AccessWriter"/>.
/// Owns insertion of catalog entries, ACE rows, table renames, and
/// catalog row scanning. The writer exposes thin instance forwarders.
/// </summary>
internal sealed class CatalogWriter(AccessWriter writer)
{
    /// <summary>
    /// Inserts a new row into <c>MSysObjects</c> with default flags.
    /// </summary>
    internal ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, CancellationToken cancellationToken = default)
        => InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, catalogFlags: 0, cancellationToken);

    /// <summary>
    /// Inserts a new row into <c>MSysObjects</c> with the specified flags.
    /// </summary>
    internal async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, uint catalogFlags, CancellationToken cancellationToken = default)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", (int)tdefPageNumber);
        msys.SetValueByName(values, "ParentId", Constants.SystemObjects.TablesParentId);
        msys.SetValueByName(values, "Name", tableName);
        msys.SetValueByName(values, "Type", (short)Constants.SystemObjects.UserTableType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", unchecked((int)catalogFlags));
        msys.SetValueByName(values, "Owner", Constants.SystemObjects.DefaultOwnerBlob);
        msys.SetValueByName(values, "LvProp", lvProp ?? Constants.SystemObjects.DefaultLvPropPlaceholder);

        RowLocation loc = await writer.InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
        _ = await writer.TrySpliceCatalogIndexEntryAsync(2, msys, loc, values, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts 3 ACE rows into <c>MSysACEs</c> for a newly-created user table.
    /// </summary>
    internal async ValueTask InsertAceRowsForTableAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        long acesTdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesTdefPage <= 0)
        {
            return;
        }

        TableDef acesDef = await writer.ReadRequiredTableDefAsync(acesTdefPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        byte[]? adminsSid = await HarvestAdminsSidAsync(acesTdefPage, acesDef, cancellationToken).ConfigureAwait(false);

        byte[][] sids = adminsSid != null
            ? [Constants.Aces.OwnerSid, adminsSid, Constants.Aces.UsersSid]
            : [Constants.Aces.OwnerSid, Constants.Aces.UsersSid];

        foreach (byte[] sid in sids)
        {
            object[] row = acesDef.CreateNullValueRow();
            acesDef.SetValueByName(row, "ObjectId", (int)tdefPageNumber);
            acesDef.SetValueByName(row, "ACM", Constants.Aces.DefaultAcm);
            acesDef.SetValueByName(row, "FInheritable", true);
            acesDef.SetValueByName(row, "SID", sid);
            await writer.InsertSystemRowAndMaintainAsync(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, row, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads an existing ACE row from <c>MSysACEs</c> and extracts the
    /// Admins-group SID blob.
    /// </summary>
    private async ValueTask<byte[]?> HarvestAdminsSidAsync(long acesTdefPage, TableDef acesDef, CancellationToken cancellationToken)
    {
        ColumnInfo? sidCol = acesDef.FindColumn("SID");
        if (sidCol == null)
        {
            return null;
        }

        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01 || AccessBase.Ri32(page, writer._dataPage.TDefOff) != acesTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string hex = writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, sidCol);

                    if (hex.Length > 4)
                    {
                        return ParseHexBytes(hex);
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return null;
    }

    /// <summary>
    /// Renames a table in the catalog by deleting the old row and inserting a
    /// new one with the updated name and LvProp.
    /// </summary>
    internal async ValueTask RenameTableInCatalogAsync(string oldName, string newName, byte[]? lvProp, CancellationToken cancellationToken)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);

        long? tdefPage = null;
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if (!string.Equals(row.Name, oldName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            tdefPage = row.TDefPage;
            await writer.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            break;
        }

        if (tdefPage == null)
        {
            throw new InvalidOperationException($"Catalog row for '{oldName}' was not found during rename.");
        }

        await InsertCatalogEntryAsync(newName, tdefPage.Value, lvProp, cancellationToken).ConfigureAwait(false);
        writer.Constraints.Rename(oldName, newName);
        writer.InvalidateCatalogCache();
    }

    /// <summary>
    /// Scans all data pages belonging to <c>MSysObjects</c> (TDEF page 2) and
    /// returns a decoded row for each live catalog entry.
    /// </summary>
    internal async ValueTask<List<CatalogRow>> GetCatalogRowsAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo? idColumn = msys.FindColumn("Id");
        ColumnInfo? nameColumn = msys.FindColumn("Name");
        ColumnInfo? typeColumn = msys.FindColumn("Type");
        ColumnInfo? flagsColumn = msys.FindColumn("Flags");
        if (nameColumn == null || typeColumn == null)
        {
            return [];
        }

        var result = new List<CatalogRow>();
        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01)
            {
                AccessBase.ReturnPage(page);
                continue;
            }

            if (AccessBase.Ri32(page, writer._dataPage.TDefOff) != 2)
            {
                AccessBase.ReturnPage(page);
                continue;
            }

            foreach (RowLocation row in writer.EnumerateLiveRowLocations(pageNumber, page))
            {
                result.Add(new CatalogRow(
                    PageNumber: row.PageNumber,
                    RowIndex: row.RowIndex,
                    Name: writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameColumn),
                    ObjectType: writer.ParseInt32(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, typeColumn)),
                    Flags: ParseInt64(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flagsColumn!)),
                    TDefPage: ParseInt64(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, idColumn!)) & 0x00FFFFFFL));
            }

            AccessBase.ReturnPage(page);
        }

        return result;
    }

    private static long ParseInt64(string value)
    {
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) ? parsed : 0L;
    }

    private static byte[] ParseHexBytes(string hex)
    {
#if NET5_0_OR_GREATER
        return Convert.FromHexString(hex);
#else
        byte[] bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = byte.Parse(hex.AsSpan(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return bytes;
#endif
    }
}
