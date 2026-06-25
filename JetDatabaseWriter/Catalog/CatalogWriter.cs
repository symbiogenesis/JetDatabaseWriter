namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
#if !NET5_0_OR_GREATER
using System.Globalization;
#endif
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;

/// <summary>
/// Catalog (MSysObjects) write operations for <see cref="AccessWriter"/>.
/// Owns insertion of catalog entries, ACE rows, table renames, and
/// catalog row scanning.
/// </summary>
/// <param name="writer">The writer.</param>
/// <param name="indexes">The indexes.</param>
/// <param name="longValueEncoder">The long value encoder.</param>
internal sealed class CatalogWriter(AccessWriter writer, IndexMaintainer indexes, LongValueEncoder longValueEncoder)
{
    /// <summary>
    /// Inserts a new row into <c>MSysObjects</c> with the specified flags.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tdefPageNumber">The TDEF page number.</param>
    /// <param name="lvProp">The LvProp payload.</param>
    /// <param name="catalogFlags">The catalog flags.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, uint catalogFlags, CancellationToken cancellationToken = default)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        await this.EnsureCatalogContainerNameAvailableAsync(msys, Constants.SystemObjects.TablesParentId, tableName, cancellationToken).ConfigureAwait(false);

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
        await this.RequireCatalogIndexSpliceAsync(msys, loc, values, tableName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts a caller-shaped row into <c>MSysObjects</c> and applies any
    /// declarative object-id, linked-field, rollback, or ACE policy carried by
    /// the artifact.
    /// </summary>
    /// <param name="artifact">The catalog object artifact.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The inserted <c>MSysObjects.Id</c> value.</returns>
    internal async ValueTask<int> InsertCatalogObjectAsync(CatalogObjectArtifact artifact, CancellationToken cancellationToken = default)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        await this.EnsureCatalogContainerNameAvailableAsync(msys, artifact.ParentId, artifact.ObjectName, cancellationToken).ConfigureAwait(false);

        int objectId = artifact.ObjectIdPolicy == CatalogObjectIdPolicy.AllocateNonTable
            ? await this.AllocateNonTableObjectIdAsync(msys, cancellationToken).ConfigureAwait(false)
            : artifact.ObjectId;

        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", objectId);
        msys.SetValueByName(values, "ParentId", artifact.ParentId);
        msys.SetValueByName(values, "Name", artifact.ObjectName);
        msys.SetValueByName(values, "Type", artifact.ObjectType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", unchecked((int)artifact.CatalogFlags));

        if (artifact.Owner is not null && msys.FindColumn("Owner") is not null)
        {
            msys.SetValueByName(values, "Owner", artifact.Owner);
        }

        if (artifact.LvProp is not null && msys.FindColumn("LvProp") is not null)
        {
            msys.SetValueByName(values, "LvProp", artifact.LvProp);
        }

        if (artifact.ForeignName is not null)
        {
            msys.SetValueByName(values, "ForeignName", artifact.EncodeForeignNameForTextLink ? EncodeTextForeignName(artifact.ForeignName) : artifact.ForeignName);
        }

        if (!string.IsNullOrEmpty(artifact.Database))
        {
            object databaseValue = artifact.EncodeDatabaseAsMemoLval
                ? await this.EncodeLinkedMemoFieldAsync(artifact.Database, cancellationToken).ConfigureAwait(false)
                : artifact.Database;
            msys.SetValueByName(values, "Database", databaseValue);
        }

        if (!string.IsNullOrEmpty(artifact.Connect))
        {
            msys.SetValueByName(values, "Connect", artifact.Connect);
        }

        RowLocation loc = await writer.InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
        try
        {
            await this.RequireCatalogIndexSpliceAsync(msys, loc, values, artifact.ObjectName, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex) when (artifact.RollbackCatalogRowOnIndexFailure && IsCatalogSpliceFailure(ex))
        {
            await this.RemoveUnindexedCatalogRowAsync(loc, cancellationToken).ConfigureAwait(false);
            throw;
        }

        if (artifact.AcePolicy != CatalogObjectAcePolicy.None)
        {
            await this.InsertAceRowsForCatalogObjectAsync(
                objectId,
                useRestrictedOwnerAcm: true,
                useRelationshipGroupAcm: artifact.AcePolicy == CatalogObjectAcePolicy.RelationshipObject,
                cancellationToken).ConfigureAwait(false);
            writer.InvalidateCatalogCache();
        }

        return objectId;
    }

    internal ValueTask InsertAceRowsForTableAsync(long tdefPageNumber, CancellationToken cancellationToken)
        => this.InsertAceRowsForCatalogObjectAsync(checked((int)tdefPageNumber), useRestrictedOwnerAcm: false, useRelationshipGroupAcm: false, cancellationToken);

    /// <summary>
    /// Deletes every <c>MSysACEs</c> row whose <c>ObjectId</c> matches one of
    /// the supplied object identifiers and refreshes the table's indexes so the
    /// removals are visible to external readers.
    /// </summary>
    /// <param name="objectIds">The object identifiers whose ACE rows are removed.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask DeleteAceRowsForObjectIdsAsync(IReadOnlyList<long> objectIds, CancellationToken cancellationToken)
    {
        if (objectIds.Count == 0)
        {
            return;
        }

        long acesTdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesTdefPage <= 0)
        {
            return;
        }

        TableDef acesDef = await writer.ReadRequiredTableDefAsync(acesTdefPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        ColumnInfo? objectIdColumn = acesDef.FindColumn("ObjectId");
        if (objectIdColumn is null)
        {
            return;
        }

        var ids = new HashSet<int>();
        foreach (long id in objectIds)
        {
            ids.Add(checked((int)id));
        }

        var deletedRows = new List<(RowLocation Loc, object[] Row)>();
        await writer.ForEachLiveTableRowAsync(
            acesTdefPage,
            (row, _) =>
            {
                string objectIdText = writer.DecodeSimpleColumnValue(row.Page, row.Location.RowStart, row.Location.RowSize, objectIdColumn);
                if (CatalogValueReader.TryParseInt32(objectIdText, out int objectId)
                    && ids.Contains(objectId))
                {
                    object[] deletedIndexRow = acesDef.CreateNullValueRow();
                    acesDef.SetValueByName(deletedIndexRow, "ObjectId", objectId);
                    deletedRows.Add((row.Location, deletedIndexRow));
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        foreach ((RowLocation row, _) in deletedRows)
        {
            await writer.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        if (deletedRows.Count > 0)
        {
            await writer.AdjustTDefRowCountAsync(acesTdefPage, -deletedRows.Count, cancellationToken).ConfigureAwait(false);
            await indexes.MaintainSystemTableIndexesIncrementallyAsync(
                acesTdefPage,
                acesDef,
                Constants.SystemTableNames.Aces,
                insertedRows: null,
                deletedRows: deletedRows,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    private static string EncodeTextForeignName(string foreignName) =>
        foreignName.Replace('.', '#');

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

    private static bool IsCatalogSpliceFailure(InvalidOperationException exception)
        => exception.Message.StartsWith("Could not maintain MSysObjects catalog indexes", StringComparison.Ordinal);

    private async ValueTask<object> EncodeLinkedMemoFieldAsync(string value, CancellationToken cancellationToken)
    {
        object? encoded = await longValueEncoder.ForceEncodeMemoAsLvalAsync(value, compress: false, cancellationToken).ConfigureAwait(false);
        return encoded ?? value;
    }

    private async ValueTask InsertAceRowsForCatalogObjectAsync(
        int objectId,
        bool useRestrictedOwnerAcm,
        bool useRelationshipGroupAcm,
        CancellationToken cancellationToken)
    {
        long acesTdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesTdefPage <= 0)
        {
            return;
        }

        TableDef acesDef = await writer.ReadRequiredTableDefAsync(acesTdefPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        byte[]? adminsSid = await this.HarvestAdminsSidAsync(acesTdefPage, acesDef, cancellationToken).ConfigureAwait(false);

        byte[][] sids = adminsSid != null
            ? [Constants.Aces.OwnerSid, adminsSid, Constants.Aces.UsersSid]
            : [Constants.Aces.OwnerSid, Constants.Aces.UsersSid];

        for (int i = 0; i < sids.Length; i++)
        {
            object[] row = acesDef.CreateNullValueRow();
            acesDef.SetValueByName(row, "ObjectId", objectId);
            int acm;
            if (i == 0 && useRestrictedOwnerAcm)
            {
                acm = Constants.Aces.RelationshipOwnerAcm;
            }
            else if (useRelationshipGroupAcm)
            {
                acm = Constants.Aces.RelationshipGroupAcm;
            }
            else
            {
                acm = Constants.Aces.DefaultAcm;
            }

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
        bool spliced = await indexes.TrySpliceCatalogIndexEntryAsync(2, msys, loc, values, cancellationToken).ConfigureAwait(false);
        if (!spliced)
        {
            throw new InvalidOperationException($"Could not maintain MSysObjects catalog indexes for '{objectName}'.");
        }
    }

    private async ValueTask RemoveUnindexedCatalogRowAsync(RowLocation loc, CancellationToken cancellationToken)
    {
        await writer.MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        await writer.AdjustTDefRowCountAsync(2, -1, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads an existing ACE row from <c>MSysACEs</c> and extracts the
    /// Admins-group SID blob.
    /// </summary>
    /// <param name="acesTdefPage">The aces TDEF page.</param>
    /// <param name="acesDef">The aces def.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<byte[]?> HarvestAdminsSidAsync(long acesTdefPage, TableDef acesDef, CancellationToken cancellationToken)
    {
        ColumnInfo? sidCol = acesDef.FindColumn("SID");
        if (sidCol == null)
        {
            return null;
        }

        byte[]? sid = null;
        await writer.ForEachLiveTableRowAsync(
            acesTdefPage,
            (row, _) =>
            {
                string hex = writer.DecodeSimpleColumnValue(row.Page, row.Location.RowStart, row.Location.RowSize, sidCol);
                if (hex.Length <= 4)
                {
                    return new ValueTask<bool>(true);
                }

                sid = ParseHexBytes(hex);
                return new ValueTask<bool>(false);
            },
            cancellationToken).ConfigureAwait(false);

        return sid;
    }

    /// <summary>
    /// Renames a table in the catalog by deleting the old row and inserting a
    /// new one with the updated name and LvProp.
    /// </summary>
    /// <param name="oldName">The old name.</param>
    /// <param name="newName">The new name.</param>
    /// <param name="lvProp">The LvProp payload.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when no catalog row exists for <paramref name="oldName"/>.</exception>
    internal async ValueTask RenameTableInCatalogAsync(string oldName, string newName, byte[]? lvProp, CancellationToken cancellationToken)
    {
        _ = await this.ReplaceUserTableCatalogEntryAsync(
            oldName,
            newName,
            tdefPage: null,
            lvProp,
            includeSystemTables: true,
            operation: $"renaming catalog row '{oldName}' to '{newName}'",
            missingMessage: $"Catalog row for '{oldName}' was not found during rename.",
            cancellationToken).ConfigureAwait(false);
        writer.Constraints.Rename(oldName, newName);
        writer.InvalidateCatalogCache();
    }

    internal async ValueTask<long> ReplaceUserTableCatalogEntryAsync(
        string existingName,
        string replacementName,
        long? tdefPage,
        byte[]? lvProp,
        bool includeSystemTables,
        string operation,
        string? missingMessage,
        CancellationToken cancellationToken)
    {
        UserTableCatalogDeletionResult deleted = await this.DeleteUserTableCatalogRowsAsync(
            existingName,
            tdefPage,
            includeSystemTables,
            throwIfNotFound: true,
            operation,
            missingMessage,
            cancellationToken).ConfigureAwait(false);

        long replacementTdefPage = tdefPage ?? deleted.FirstTDefPage
            ?? throw new InvalidOperationException(missingMessage ?? $"Catalog row for '{existingName}' was not found.");

        await this.InsertCatalogEntryAsync(
            replacementName,
            replacementTdefPage,
            lvProp,
            deleted.FirstCatalogFlags,
            cancellationToken).ConfigureAwait(false);
        writer.InvalidateCatalogCache();
        return replacementTdefPage;
    }

    internal async ValueTask<UserTableCatalogDeletionResult> DeleteUserTableCatalogRowsAsync(
        string tableName,
        long? tdefPage,
        bool includeSystemTables,
        bool throwIfNotFound,
        string operation,
        string? missingMessage,
        CancellationToken cancellationToken)
    {
        TableDef msys = await writer.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> rows = await this.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        var droppedTdefPages = new List<long>();
        var deletedCatalogRows = new List<(RowLocation Loc, object[] Row)>();
        long? firstTdefPage = null;
        uint firstCatalogFlags = 0;

        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if (!includeSystemTables && (unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) != 0)
            {
                continue;
            }

            if (tdefPage is long requiredTdefPage && row.TDefPage != requiredTdefPage)
            {
                continue;
            }

            if (!string.Equals(row.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (row.TDefPage > 0)
            {
                droppedTdefPages.Add(row.TDefPage);
            }

            firstTdefPage ??= row.TDefPage;
            if (deletedCatalogRows.Count == 0)
            {
                firstCatalogFlags = unchecked((uint)row.Flags);
            }

            object[] indexRow = CreateMsysObjectsIndexRow(msys, row);
            deletedCatalogRows.Add((new RowLocation(row.PageNumber, row.RowIndex, 0, 0), indexRow));

            await writer.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        if (deletedCatalogRows.Count == 0)
        {
            if (throwIfNotFound)
            {
                throw new InvalidOperationException(missingMessage ?? $"Catalog row for '{tableName}' was not found.");
            }

            return new UserTableCatalogDeletionResult(0, [], null, 0);
        }

        await writer.AdjustTDefRowCountAsync(2, -deletedCatalogRows.Count, cancellationToken).ConfigureAwait(false);
        await writer.RequireMsysObjectsIndexMaintenanceAsync(
            msys,
            insertedRows: null,
            deletedRows: deletedCatalogRows,
            operation,
            cancellationToken).ConfigureAwait(false);

        writer.InvalidateCatalogCache();
        return new UserTableCatalogDeletionResult(deletedCatalogRows.Count, droppedTdefPages, firstTdefPage, firstCatalogFlags);
    }

    private static object[] CreateMsysObjectsIndexRow(TableDef msys, CatalogRow row)
    {
        object[] indexRow = msys.CreateNullValueRow();
        msys.SetValueByName(indexRow, "Id", checked((int)row.TDefPage));
        msys.SetValueByName(indexRow, "ParentId", Constants.SystemObjects.TablesParentId);
        msys.SetValueByName(indexRow, "Name", row.Name);
        return indexRow;
    }

    /// <summary>
    /// Scans all data pages belonging to <c>MSysObjects</c> (TDEF page 2) and
    /// returns a decoded row for each live catalog entry.
    /// </summary>
    /// <param name="msys">The system-table data.</param>
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
        await writer.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                byte[] page = row.Page;
                RowLocation location = row.Location;
                long id = idColumn is null
                    ? 0
                    : CatalogValueReader.ParseInt64OrZero(writer.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, idColumn));
                long parentId = parentIdColumn is null
                    ? 0
                    : CatalogValueReader.ParseInt64OrZero(writer.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, parentIdColumn));

                result.Add(new CatalogRow(
                    PageNumber: location.PageNumber,
                    RowIndex: location.RowIndex,
                    Name: writer.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, nameColumn),
                    ObjectType: CatalogValueReader.ParseInt32OrZero(writer.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, typeColumn)),
                    Flags: CatalogValueReader.ParseInt64OrZero(writer.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, flagsColumn!)),
                    TDefPage: id & 0x00FFFFFFL,
                    Id: id,
                    ParentId: parentId));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return result;
    }

    private async ValueTask EnsureCatalogContainerNameAvailableAsync(TableDef msys, int parentId, string objectName, CancellationToken cancellationToken)
    {
        List<CatalogRow> rows = await this.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            bool sameParent = row.ParentId == parentId
                || (parentId == Constants.SystemObjects.TablesParentId && row.ParentId == 0);
            if (sameParent && string.Equals(row.Name, objectName, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"An object named '{objectName}' already exists.");
            }
        }
    }

    private async ValueTask<int> AllocateNonTableObjectIdAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo? idColumn = msys.FindColumn("Id")
            ?? throw new InvalidDataException("MSysObjects does not expose an 'Id' column.");

        var usedIds = new HashSet<int>();
        int maxLow24 = 0;
        await writer.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                int id = CatalogValueReader.ParseInt32OrZero(
                    writer.DecodeSimpleColumnValue(row.Page, row.Location.RowStart, row.Location.RowSize, idColumn));
                usedIds.Add(id);
                if (id != 0)
                {
                    maxLow24 = Math.Max(maxLow24, id & 0x00FFFFFF);
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

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
