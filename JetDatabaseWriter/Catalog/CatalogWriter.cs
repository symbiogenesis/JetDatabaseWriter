namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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
        await RequireCatalogIndexSpliceAsync(msys, loc, values, tableName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts a caller-shaped row into <c>MSysObjects</c> for Access bootstrap
    /// containers whose <c>Id</c> is not a physical TDEF page number.
    /// </summary>
    internal async ValueTask InsertCatalogObjectAsync(
        int objectId,
        int parentId,
        string objectName,
        short objectType,
        uint catalogFlags,
        byte[]? owner,
        byte[]? lvProp,
        CancellationToken cancellationToken = default)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", objectId);
        msys.SetValueByName(values, "ParentId", parentId);
        msys.SetValueByName(values, "Name", objectName);
        msys.SetValueByName(values, "Type", objectType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", unchecked((int)catalogFlags));

        if (owner is not null && msys.FindColumn("Owner") is not null)
        {
            msys.SetValueByName(values, "Owner", owner);
        }

        if (lvProp is not null && msys.FindColumn("LvProp") is not null)
        {
            msys.SetValueByName(values, "LvProp", lvProp);
        }

        RowLocation loc = await writer.InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
        await RequireCatalogIndexSpliceAsync(msys, loc, values, objectName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts the Type=8 <c>MSysObjects</c> row DAO creates for a relationship.
    /// </summary>
    internal async ValueTask<int> InsertRelationshipCatalogEntryAsync(string relationshipName, CancellationToken cancellationToken = default)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        int objectId = await AllocateNonTableObjectIdAsync(msys, cancellationToken).ConfigureAwait(false);

        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", objectId);
        msys.SetValueByName(values, "ParentId", Constants.SystemObjects.RelationshipsParentId);
        msys.SetValueByName(values, "Name", relationshipName);
        msys.SetValueByName(values, "Type", (short)Constants.SystemObjects.RelationshipType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", 0);
        msys.SetValueByName(values, "Owner", Constants.SystemObjects.DefaultOwnerBlob);

        RowLocation loc = await writer.InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
        await RequireCatalogIndexSpliceAsync(msys, loc, values, relationshipName, cancellationToken).ConfigureAwait(false);

        return objectId;
    }

    /// <summary>
    /// Inserts a Type=4/6 linked-table row into <c>MSysObjects</c> using a
    /// catalog-only object id and the MSysObjects splice path.
    /// </summary>
    internal async ValueTask<int> InsertLinkedTableCatalogEntryAsync(
        string linkedTableName,
        string? sourceDatabasePath,
        string foreignName,
        string? connectString,
        short objectType,
        CancellationToken cancellationToken = default)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        await EnsureTablesContainerNameAvailableAsync(msys, linkedTableName, cancellationToken).ConfigureAwait(false);

        int objectId = await AllocateNonTableObjectIdAsync(msys, cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", objectId);
        msys.SetValueByName(values, "ParentId", Constants.SystemObjects.TablesParentId);
        msys.SetValueByName(values, "Name", linkedTableName);
        msys.SetValueByName(values, "Type", objectType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", GetLinkedTableFlags(objectType));
        msys.SetValueByName(values, "Owner", Constants.SystemObjects.DefaultOwnerBlob);
        if (msys.FindColumn("LvProp") is not null)
        {
            msys.SetValueByName(values, "LvProp", Constants.SystemObjects.DefaultLvPropPlaceholder);
        }

        msys.SetValueByName(values, "ForeignName", foreignName);

        if (!string.IsNullOrEmpty(sourceDatabasePath))
        {
            msys.SetValueByName(values, "Database", sourceDatabasePath);
        }

        if (!string.IsNullOrEmpty(connectString))
        {
            msys.SetValueByName(values, "Connect", connectString);
        }

        RowLocation loc = await writer.InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
        await RequireCatalogIndexSpliceAsync(msys, loc, values, linkedTableName, cancellationToken).ConfigureAwait(false);
        await InsertAceRowsForCatalogObjectAsync(objectId, useRelationshipAcm: false, cancellationToken).ConfigureAwait(false);
        writer.InvalidateCatalogCache();

        return objectId;
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
            acesDef.SetValueByName(row, "FInheritable", false);
            acesDef.SetValueByName(row, "SID", sid);
            await writer.InsertSystemRowAndMaintainAsync(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, row, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Inserts DAO-shaped ACE rows for a Type=8 relationship object.
    /// </summary>
    internal async ValueTask InsertAceRowsForRelationshipAsync(int objectId, CancellationToken cancellationToken)
        => await InsertAceRowsForCatalogObjectAsync(objectId, useRelationshipAcm: true, cancellationToken).ConfigureAwait(false);

    private static long ParseInt64(string value)
    {
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) ? parsed : 0L;
    }

    private static int ParseInt32(string value)
    {
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed) ? parsed : 0;
    }

    private static int GetLinkedTableFlags(short objectType) =>
        objectType == Constants.SystemObjects.LinkedOdbcType ? Constants.SystemObjects.LinkedOdbcFlags : Constants.SystemObjects.LinkedTableFlags;

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

    private async ValueTask InsertAceRowsForCatalogObjectAsync(int objectId, bool useRelationshipAcm, CancellationToken cancellationToken)
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

        for (int i = 0; i < sids.Length; i++)
        {
            object[] row = acesDef.CreateNullValueRow();
            acesDef.SetValueByName(row, "ObjectId", objectId);
            int acm = useRelationshipAcm
                ? (i == 0 ? Constants.Aces.RelationshipOwnerAcm : Constants.Aces.RelationshipGroupAcm)
                : Constants.Aces.DefaultAcm;
            acesDef.SetValueByName(row, "ACM", acm);
            acesDef.SetValueByName(row, "FInheritable", false);
            acesDef.SetValueByName(row, "SID", sids[i]);
            await writer.InsertSystemRowAndMaintainAsync(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, row, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask RequireCatalogIndexSpliceAsync(
        TableDef msys,
        RowLocation loc,
        object[] values,
        string objectName,
        CancellationToken cancellationToken)
    {
        bool spliced = await writer.TrySpliceCatalogIndexEntryAsync(2, msys, loc, values, cancellationToken).ConfigureAwait(false);
        if (!spliced && writer.DatabaseFormat != Enums.DatabaseFormat.Jet3Mdb)
        {
            throw new InvalidOperationException($"Could not maintain MSysObjects catalog indexes for '{objectName}'.");
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
        uint catalogFlags = 0;
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
            catalogFlags = unchecked((uint)row.Flags);
            object[] deletedIndexRow = msys.CreateNullValueRow();
            msys.SetValueByName(deletedIndexRow, "Id", checked((int)row.TDefPage));
            msys.SetValueByName(deletedIndexRow, "ParentId", Constants.SystemObjects.TablesParentId);
            msys.SetValueByName(deletedIndexRow, "Name", row.Name);
            await writer.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, clearRowData: true, cancellationToken).ConfigureAwait(false);
            _ = await writer.TryMaintainIndexesIncrementalAsync(
                2,
                msys,
                null,
                [(new RowLocation(row.PageNumber, row.RowIndex, 0, 0), deletedIndexRow)],
                cancellationToken).ConfigureAwait(false);
            break;
        }

        if (tdefPage == null)
        {
            throw new InvalidOperationException($"Catalog row for '{oldName}' was not found during rename.");
        }

        await InsertCatalogEntryAsync(newName, tdefPage.Value, lvProp, catalogFlags, cancellationToken).ConfigureAwait(false);
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
        ColumnInfo? parentIdColumn = msys.FindColumn("ParentId");
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
                long id = idColumn is null
                    ? 0
                    : ParseInt64(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, idColumn));
                long parentId = parentIdColumn is null
                    ? 0
                    : ParseInt64(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, parentIdColumn));

                result.Add(new CatalogRow(
                    PageNumber: row.PageNumber,
                    RowIndex: row.RowIndex,
                    Name: writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameColumn),
                    ObjectType: writer.ParseInt32(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, typeColumn)),
                    Flags: ParseInt64(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flagsColumn!)),
                    TDefPage: id & 0x00FFFFFFL,
                    Id: id,
                    ParentId: parentId));
            }

            AccessBase.ReturnPage(page);
        }

        return result;
    }

    private async ValueTask EnsureTablesContainerNameAvailableAsync(TableDef msys, string objectName, CancellationToken cancellationToken)
    {
        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if ((row.ParentId == Constants.SystemObjects.TablesParentId || row.ParentId == 0)
                && string.Equals(row.Name, objectName, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"An object named '{objectName}' already exists.");
            }
        }
    }

    private async ValueTask<int> AllocateNonTableObjectIdAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo? idColumn = msys.FindColumn("Id");
        if (idColumn == null)
        {
            throw new InvalidDataException("MSysObjects does not expose an 'Id' column.");
        }

        var usedIds = new HashSet<int>();
        int maxLow24 = 0;
        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01 || AccessBase.Ri32(page, writer._dataPage.TDefOff) != 2)
                {
                    continue;
                }

                foreach (RowLocation row in writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    int id = ParseInt32(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, idColumn));
                    _ = usedIds.Add(id);
                    if (id != 0)
                    {
                        maxLow24 = Math.Max(maxLow24, id & 0x00FFFFFF);
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        int low24 = Math.Max(1, maxLow24 + 1);
        for (int attempt = 0; attempt < 0x00FFFFFE; attempt++)
        {
            if (low24 > 0x00FFFFFF)
            {
                low24 = 1;
            }

            int candidate = unchecked((int)(0x80000000u | (uint)low24));
            if (!usedIds.Contains(candidate))
            {
                return candidate;
            }

            low24++;
        }

        throw new InvalidOperationException("No free negative MSysObjects catalog object id is available.");
    }
}
