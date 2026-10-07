namespace JetDatabaseWriter.Catalog;

using System;
using System.Buffers;
using System.Collections.Generic;
#if !NET5_0_OR_GREATER
using System.Globalization;
#endif
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueEncoding;

/// <summary>
/// Catalog (MSysObjects) write operations for <see cref="AccessWriter"/>.
/// Owns insertion of catalog entries, ACE rows, table renames, and catalog
/// row deletion.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="ownedPages">The database's owned-page discovery and row walks.</param>
/// <param name="catalog">The cached user-table catalog, invalidated after every catalog mutation.</param>
/// <param name="tableRows">Writes and tombstones catalog rows.</param>
/// <param name="indexes">Keeps the catalog and <c>MSysACEs</c> indexes current.</param>
/// <param name="longValueEncoder">Encodes linked-table memo fields as LVAL chains.</param>
/// <param name="constraints">Follows table renames in the client-side constraint registry.</param>
/// <param name="catalogRows">Scans <c>MSysObjects</c> rows and locates system tables.</param>
/// <param name="pager">The writer page file.</param>
internal sealed class CatalogWriter(
    JetFormat format,
    TableDefReader tableDefs,
    OwnedDataPages ownedPages,
    TableCatalog catalog,
    TableRowStore tableRows,
    IndexMaintainer indexes,
    LongValueEncoder longValueEncoder,
    ConstraintRegistry constraints,
    CatalogRowReader catalogRows,
    Pager pager)
{
    private int checkedCatalogGeneration = -1;

    /// <summary>Checks the catalog's indexes once for each catalog generation before DDL writes.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask ThrowIfCatalogIndexesUnmaintainableAsync(CancellationToken cancellationToken)
    {
        int generation = catalog.Generation;
        if (this.checkedCatalogGeneration == generation)
        {
            return;
        }

        TableDef definition = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        await indexes.ThrowIfIndexesUnmaintainableAsync(2, definition, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        this.checkedCatalogGeneration = generation;
    }

    /// <summary>
    /// Inserts a new row into <c>MSysObjects</c> with the specified flags.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tdefPageNumber">The TDEF page number.</param>
    /// <param name="lvProp">The LvProp payload.</param>
    /// <param name="catalogFlags">The catalog flags.</param>
    /// <param name="owner">An explicit native bootstrap system owner.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, uint catalogFlags, byte[]? owner = null, CancellationToken cancellationToken = default)
    {
        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        TableDef msys = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
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
        msys.SetValueByName(values, "Owner", owner ?? (format.UsesHeaderMaskedSecuritySids ? await this.ReadDatabaseOwnerAsync(cancellationToken).ConfigureAwait(false) : Constants.SystemObjects.DefaultOwnerBlob));
        if (lvProp is not null)
        {
            msys.SetValueByName(values, "LvProp", lvProp);
        }

        RowLocation loc = await tableRows.InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
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
        if (artifact.AcePolicy != CatalogObjectAcePolicy.None)
        {
            await this.ThrowIfNativeSecurityUnmaintainableAsync(artifact.AcePolicy == CatalogObjectAcePolicy.RelationshipObject, cancellationToken).ConfigureAwait(false);
        }

        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        TableDef msys = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
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
            byte[] owner = format.UsesHeaderMaskedSecuritySids && artifact.AcePolicy != CatalogObjectAcePolicy.None
                ? await this.ReadDatabaseOwnerAsync(cancellationToken).ConfigureAwait(false)
                : artifact.Owner;
            msys.SetValueByName(values, "Owner", owner);
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

        RowLocation loc = await tableRows.InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
        await this.RequireCatalogIndexSpliceAsync(msys, loc, values, artifact.ObjectName, cancellationToken).ConfigureAwait(false);

        if (artifact.AcePolicy != CatalogObjectAcePolicy.None)
        {
            await this.InsertAceRowsForCatalogObjectAsync(
                objectId,
                useRestrictedOwnerAcm: true,
                useRelationshipGroupAcm: artifact.AcePolicy == CatalogObjectAcePolicy.RelationshipObject,
                cancellationToken).ConfigureAwait(false);
            catalog.Invalidate();
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

        long acesTdefPage = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesTdefPage <= 0)
        {
            return;
        }

        TableDef acesDef = await tableDefs.ReadRequiredTableDefAsync(acesTdefPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
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
        await ownedPages.ForEachLiveTableRowAsync(
            acesTdefPage,
            (row, _) =>
            {
                string objectIdText = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, objectIdColumn);
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
            await tableRows.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, acesDef, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        if (deletedRows.Count > 0)
        {
            await tableRows.AdjustTDefRowCountAsync(acesTdefPage, -deletedRows.Count, cancellationToken).ConfigureAwait(false);
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

    private async ValueTask<object> EncodeLinkedMemoFieldAsync(string value, CancellationToken cancellationToken)
    {
        object? encoded = await longValueEncoder.ForceEncodeMemoAsLvalAsync(value, compress: false, cancellationToken).ConfigureAwait(false);
        return encoded ?? value;
    }

    /// <summary>Reads the unique native database owner without synthesizing security metadata.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The encoded database owner.</returns>
    /// <exception cref="JetCorruptDataException">The native database security owner is missing or malformed.</exception>
    private ValueTask<byte[]> ReadDatabaseOwnerAsync(CancellationToken cancellationToken) => this.ReadCatalogSecurityObjectAsync("MSysDb", Constants.SystemObjects.DatabaseObjectId, 2, cancellationToken);

    /// <summary>Validates one uniquely named native catalog security object.</summary>
    /// <param name="objectName">The required object name.</param>
    /// <param name="objectId">The required object identifier.</param>
    /// <param name="objectType">The required catalog type.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The encoded object owner.</returns>
    /// <exception cref="JetCorruptDataException">The object identity or owner is absent, duplicated, or malformed.</exception>
    private async ValueTask<byte[]> ReadCatalogSecurityObjectAsync(string objectName, int objectId, int objectType, CancellationToken cancellationToken)
    {
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameColumn = objects.FindColumn("Name");
        ColumnInfo? ownerColumn = objects.FindColumn("Owner");
        if (nameColumn is null || ownerColumn is null)
        {
            throw new JetCorruptDataException("MSysObjects has no database security owner columns.");
        }

        ColumnInfo idColumn = objects.FindColumn("Id") ?? throw new JetCorruptDataException("MSysObjects has no security object identifier column.");
        ColumnInfo typeColumn = objects.FindColumn("Type") ?? throw new JetCorruptDataException("MSysObjects has no security object type column.");
        byte[]? owner = null;
        int matches = 0;
        await ownedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                RowLocation location = row.Location;
                string name = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, nameColumn);
                if (!string.Equals(name, objectName, StringComparison.OrdinalIgnoreCase))
                {
                    return new ValueTask<bool>(true);
                }

                if (++matches != 1)
                {
                    throw new JetCorruptDataException($"MSysObjects contains duplicate {objectName} security objects.");
                }

                if (!CatalogValueReader.TryParseInt32(ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, idColumn), out int id)
                    || id != objectId
                    || !CatalogValueReader.TryParseInt32(ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, typeColumn), out int type) || type != objectType)
                {
                    throw new JetCorruptDataException($"{objectName} has an invalid security object identity.");
                }

                string value = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, ownerColumn);
                owner = value.Length == 0 ? null : ParseHexBytes(value);
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);
        return owner is { Length: > 0 }
            ? owner
            : throw new JetCorruptDataException($"MSysObjects has no valid {objectName} security owner.");
    }

    /// <summary>Inherits native Jet4 container permissions and resolves the masked owner placeholder.</summary>
    /// <param name="relationships">Whether to inherit relationship-container permissions.</param>
    /// <param name="acesTdefPage">The permissions table page.</param>
    /// <param name="acesDef">The permissions table definition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The validated and remapped native permissions.</returns>
    /// <exception cref="JetCorruptDataException">Required security metadata is malformed.</exception>
    /// <exception cref="NotSupportedException">The container has no inheritable permissions.</exception>
    private async ValueTask<(byte[] Sid, int Acm)[]> ReadInheritedJet4PermissionsAsync(
        bool relationships,
        long acesTdefPage,
        TableDef acesDef,
        CancellationToken cancellationToken)
    {
        byte[] owner = await this.ReadDatabaseOwnerAsync(cancellationToken).ConfigureAwait(false);

        ColumnInfo objectColumn = acesDef.FindColumn("ObjectId") ?? throw new JetCorruptDataException("MSysACEs has no ObjectId column.");
        ColumnInfo sidColumn = acesDef.FindColumn("SID") ?? throw new JetCorruptDataException("MSysACEs has no SID column.");
        ColumnInfo acmColumn = acesDef.FindColumn("ACM") ?? throw new JetCorruptDataException("MSysACEs has no ACM column.");
        ColumnInfo inheritColumn = acesDef.FindColumn("FInheritable") ?? throw new JetCorruptDataException("MSysACEs has no FInheritable column.");
        int parentId = relationships ? Constants.SystemObjects.RelationshipsParentId : Constants.SystemObjects.TablesParentId;
        _ = await this.ReadCatalogSecurityObjectAsync(relationships ? "Relationships" : "Tables", parentId, 3, cancellationToken).ConfigureAwait(false);
        var inherited = new List<(byte[] Sid, int Acm)>();
        var storedIdentities = new HashSet<string>(StringComparer.Ordinal);
        await ownedPages.ForEachLiveTableRowAsync(
            acesTdefPage,
            (row, _) =>
            {
                RowLocation location = row.Location;
                string Decode(ColumnInfo column) => ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, column);

                if (!CatalogValueReader.TryParseInt32(Decode(objectColumn), out int id))
                {
                    throw new JetCorruptDataException("MSysACEs contains an invalid object identifier.");
                }

                if (id == parentId)
                {
                    string inherit = Decode(inheritColumn);
                    if (inherit is not ("True" or "False"))
                    {
                        throw new JetCorruptDataException("A container MSysACEs entry has an invalid inheritance flag.");
                    }

                    if (!CatalogValueReader.TryParseInt32(Decode(acmColumn), out int acm))
                    {
                        throw new JetCorruptDataException("A container MSysACEs entry has an invalid permission mask.");
                    }

                    byte[] sid = ParseHexBytes(Decode(sidColumn));
                    if (sid.Length == 0)
                    {
                        throw new JetCorruptDataException("A container MSysACEs entry has no security identity.");
                    }

                    if (inherit == "True")
                    {
                        if (!storedIdentities.Add(BitConverter.ToString(sid)))
                        {
                            throw new JetCorruptDataException("Native inherited container permissions contain duplicate stored security identities.");
                        }

                        inherited.Add((sid, acm));
                    }
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);
        if (inherited.Count == 0)
        {
            throw new NotSupportedException("The Jet4 database has no inheritable catalog permissions for the new object.");
        }

        byte[] header = await pager.ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
        byte[] placeholder;
        try
        {
            placeholder = Jet4SecuritySid.GetOwnerPlaceholder(format, header);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
        }

        // Native Access uses the explicit inheritable owner entry when present;
        // the owner placeholder supplies permissions only when no such entry exists.
        bool explicitOwner = inherited.Exists(entry => entry.Sid.AsSpan().SequenceEqual(owner));
        var resolved = new List<(byte[] Sid, int Acm)>(inherited.Count);
        foreach ((byte[] sid, int acm) in inherited)
        {
            if (sid.AsSpan().SequenceEqual(placeholder))
            {
                if (!explicitOwner)
                {
                    resolved.Add((owner, acm));
                }
            }
            else
            {
                resolved.Add((sid, acm));
            }
        }

        return resolved.ToArray();
    }

    /// <summary>Validates native security metadata before allocating or changing catalog artifacts.</summary>
    /// <param name="relationships">Whether to use the relationships container.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetCorruptDataException">The native permissions table is absent or malformed.</exception>
    internal async ValueTask ThrowIfNativeSecurityUnmaintainableAsync(bool relationships, CancellationToken cancellationToken)
    {
        if (!format.UsesHeaderMaskedSecuritySids)
        {
            return;
        }

        long page = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (page <= 0)
        {
            throw new JetCorruptDataException("The native Jet4 permissions table is absent.");
        }

        _ = await this.ReadCatalogSecurityObjectAsync(Constants.SystemTableNames.Aces, checked((int)page), 1, cancellationToken).ConfigureAwait(false);
        TableDef definition = await tableDefs.ReadRequiredTableDefAsync(page, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        _ = await this.ReadInheritedJet4PermissionsAsync(relationships, page, definition, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes permissions for a catalog object using its native container policy.</summary>
    /// <param name="objectId">The object identifier.</param>
    /// <param name="useRestrictedOwnerAcm">Whether the non-Jet4 owner permissions are restricted.</param>
    /// <param name="useRelationshipGroupAcm">Whether to use relationship-container permissions.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetCorruptDataException">The native permissions table is absent.</exception>
    private async ValueTask InsertAceRowsForCatalogObjectAsync(
        int objectId,
        bool useRestrictedOwnerAcm,
        bool useRelationshipGroupAcm,
        CancellationToken cancellationToken)
    {
        long acesTdefPage = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesTdefPage <= 0)
        {
            if (format.UsesHeaderMaskedSecuritySids)
            {
                throw new JetCorruptDataException("The native Jet4 permissions table is absent.");
            }

            return;
        }

        TableDef acesDef = await tableDefs.ReadRequiredTableDefAsync(acesTdefPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (format.UsesHeaderMaskedSecuritySids)
        {
            (byte[] Sid, int Acm)[] inherited = await this.ReadInheritedJet4PermissionsAsync(useRelationshipGroupAcm, acesTdefPage, acesDef, cancellationToken).ConfigureAwait(false);
            foreach ((byte[] sid, int acm) in inherited)
            {
                object[] values = acesDef.CreateNullValueRow();
                acesDef.SetValueByName(values, "ObjectId", objectId);
                acesDef.SetValueByName(values, "SID", sid);
                acesDef.SetValueByName(values, "ACM", acm);
                acesDef.SetValueByName(values, "FInheritable", false);
                await indexes.InsertSystemRowAndMaintainAsync(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, values, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            return;
        }

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
            await indexes.InsertSystemRowAndMaintainAsync(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, row, cancellationToken: cancellationToken).ConfigureAwait(false);
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
        await ownedPages.ForEachLiveTableRowAsync(
            acesTdefPage,
            (row, _) =>
            {
                string hex = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, sidCol);
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
        constraints.Rename(oldName, newName);
        catalog.Invalidate();
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
        await this.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);

        UserTableCatalogDeletionResult deleted = await this.DeleteUserTableCatalogRowsAsync(
            existingName,
            tdefPage,
            includeSystemTables,
            throwIfNotFound: true,
            operation,
            missingMessage,
            cancellationToken).ConfigureAwait(false);

        long replacementTdefPage = tdefPage ?? deleted.FirstTDefPage
            ?? throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, missingMessage ?? $"Catalog row for '{existingName}' was not found.", new JetErrorInfo { ObjectName = existingName });

        await this.InsertCatalogEntryAsync(
            replacementName,
            replacementTdefPage,
            lvProp,
            deleted.FirstCatalogFlags,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        catalog.Invalidate();
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
        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        TableDef msys = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
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

            await tableRows.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, msys, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        if (deletedCatalogRows.Count == 0)
        {
            if (throwIfNotFound)
            {
                throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, missingMessage ?? $"Catalog row for '{tableName}' was not found.", errorInfo: new JetErrorInfo { TableName = tableName });
            }

            return new UserTableCatalogDeletionResult(0, [], null, 0);
        }

        await tableRows.AdjustTDefRowCountAsync(2, -deletedCatalogRows.Count, cancellationToken).ConfigureAwait(false);
        await this.RequireMsysObjectsIndexMaintenanceAsync(
            msys,
            insertedRows: null,
            deletedRows: deletedCatalogRows,
            operation,
            cancellationToken).ConfigureAwait(false);

        catalog.Invalidate();
        return new UserTableCatalogDeletionResult(deletedCatalogRows.Count, droppedTdefPages, firstTdefPage, firstCatalogFlags);
    }

    /// <summary>Deletes catalog rows for hidden tables, keeping catalog indexes and counts current.</summary>
    /// <param name="tdefPages">The table definition pages.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask DeleteCatalogRowsByTdefPagesAsync(IReadOnlyCollection<long> tdefPages, CancellationToken cancellationToken)
    {
        TableDef msys = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        var pages = new HashSet<long>(tdefPages);
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == Constants.SystemObjects.UserTableType && pages.Remove(row.TDefPage))
            {
                _ = await this.DeleteUserTableCatalogRowsAsync(row.Name, row.TDefPage, includeSystemTables: true, throwIfNotFound: false, operation: "DropComplexTable", missingMessage: null, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static object[] CreateMsysObjectsIndexRow(TableDef msys, CatalogRow row)
    {
        object[] indexRow = msys.CreateNullValueRow();
        msys.SetValueByName(indexRow, "Id", checked((int)row.TDefPage));
        msys.SetValueByName(indexRow, "ParentId", Constants.SystemObjects.TablesParentId);
        msys.SetValueByName(indexRow, "Name", row.Name);
        return indexRow;
    }

    private async ValueTask RequireMsysObjectsIndexMaintenanceAsync(
        TableDef msys,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        string operation,
        CancellationToken cancellationToken)
    {
        bool incremental = await indexes.TryMaintainIndexesIncrementalAsync(
            2,
            msys,
            insertedRows,
            deletedRows,
            cancellationToken).ConfigureAwait(false);
        if (incremental)
        {
            return;
        }

        if (!format.IsJet3)
        {
            throw new InvalidOperationException($"Could not maintain MSysObjects catalog indexes while {operation}.");
        }

        await indexes.MaintainIndexesAsync(2, msys, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EnsureCatalogContainerNameAvailableAsync(TableDef msys, int parentId, string objectName, CancellationToken cancellationToken)
    {
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            bool sameParent = row.ParentId == parentId
                || (parentId == Constants.SystemObjects.TablesParentId && row.ParentId == 0);
            if (sameParent && string.Equals(row.Name, objectName, StringComparison.OrdinalIgnoreCase))
            {
                throw new JetObjectExistsException(JetErrorCode.ObjectExists, $"An object named '{objectName}' already exists.", errorInfo: new JetErrorInfo { ObjectName = objectName });
            }
        }
    }

    private async ValueTask<int> AllocateNonTableObjectIdAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo? idColumn = msys.FindColumn("Id")
            ?? throw new InvalidDataException("MSysObjects does not expose an 'Id' column.");

        var usedIds = new HashSet<int>();
        int maxLow24 = 0;
        await ownedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                int id = CatalogValueReader.ParseInt32OrZero(
                    ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, row.Location.RowStart, row.Location.RowSize, idColumn));
                usedIds.Add(id);
                if (id != 0)
                {
                    maxLow24 = Math.Max(maxLow24, CatalogValueReader.TdefPageFromId(id));
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
