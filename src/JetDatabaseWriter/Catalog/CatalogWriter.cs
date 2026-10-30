namespace JetDatabaseWriter.Catalog;

using System;
using System.Buffers;
using System.Collections.Generic;
#if !NET5_0_OR_GREATER
using System.Globalization;
#endif
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
using JetDatabaseWriter.ValueDecoding.Models;
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
/// <param name="indexes">Validates native catalog index support.</param>
/// <param name="mutations">Applies prepared catalog and security row changes.</param>
/// <param name="longValueEncoder">Encodes linked-table memo fields as LVAL chains.</param>
/// <param name="catalogRows">Scans <c>MSysObjects</c> rows and locates system tables.</param>
/// <param name="pager">The writer page file.</param>
/// <param name="snapshots">Reads existing catalog values for preservation.</param>
internal sealed class CatalogWriter(
    JetFormat format,
    TableDefReader tableDefs,
    OwnedDataPages ownedPages,
    TableCatalog catalog,
    IndexMaintainer indexes,
    MutationExecutor mutations,
    LongValueEncoder longValueEncoder,
    CatalogRowReader catalogRows,
    Pager pager,
    TableSnapshotReader snapshots)
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
        await this.EnsureCatalogContainerNameAvailableAsync(msys, Constants.SystemObjects.TablesParentId, tableName, cancellationToken: cancellationToken).ConfigureAwait(false);

        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", (int)tdefPageNumber);
        msys.SetValueByName(values, "ParentId", Constants.SystemObjects.TablesParentId);
        msys.SetValueByName(values, "Name", tableName);
        msys.SetValueByName(values, "Type", (short)Constants.SystemObjects.UserTableType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", unchecked((int)catalogFlags));
        msys.SetValueByName(values, "Owner", owner ?? await this.ReadDatabaseOwnerAsync(cancellationToken).ConfigureAwait(false));
        if (lvProp is not null)
        {
            msys.SetValueByName(values, "LvProp", this.PrepareCatalogProperties(lvProp));
        }

        var plan = new MutationPlan();
        plan.ForTable(2, msys, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogInsert, tableName).Insert(values);
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
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
        await this.EnsureCatalogContainerNameAvailableAsync(msys, artifact.ParentId, artifact.ObjectName, cancellationToken: cancellationToken).ConfigureAwait(false);

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

        if (artifact.AcePolicy != CatalogObjectAcePolicy.None || artifact.Owner is not null)
        {
            byte[] owner = artifact.AcePolicy != CatalogObjectAcePolicy.None
                ? await this.ReadDatabaseOwnerAsync(cancellationToken).ConfigureAwait(false)
                : artifact.Owner!;
            msys.SetValueByName(values, "Owner", owner);
        }

        if (artifact.LvProp is not null && msys.FindColumn("LvProp") is not null)
        {
            msys.SetValueByName(values, "LvProp", this.PrepareCatalogProperties(artifact.LvProp));
        }

        if (artifact.ForeignName is not null)
        {
            msys.SetValueByName(values, "ForeignName", artifact.EncodeForeignNameForTextLink ? EncodeTextForeignName(artifact.ForeignName) : artifact.ForeignName);
        }

        if (!string.IsNullOrEmpty(artifact.Database))
        {
            object databaseValue = artifact.EncodeDatabaseAsMemoLval
                ? await this.EncodeLinkedMemoFieldAsync(artifact.Database, msys.FindColumn("Database")!.ColNum, cancellationToken).ConfigureAwait(false)
                : artifact.Database;
            msys.SetValueByName(values, "Database", databaseValue);
        }

        if (!string.IsNullOrEmpty(artifact.Connect))
        {
            msys.SetValueByName(values, "Connect", artifact.Connect);
        }

        var plan = new MutationPlan();
        plan.ForTable(2, msys, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogInsert, artifact.ObjectName).Insert(values);
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);

        if (artifact.AcePolicy != CatalogObjectAcePolicy.None)
        {
            await this.InsertAceRowsForCatalogObjectAsync(
                objectId,
                useRelationshipGroupAcm: artifact.AcePolicy == CatalogObjectAcePolicy.RelationshipObject,
                cancellationToken).ConfigureAwait(false);
            catalog.Invalidate();
        }

        return objectId;
    }

    /// <summary>Resolves a relationship's exclusive catalog identity before any relationship pages change.</summary>
    /// <param name="relationshipName">The case-insensitive relationship name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The native relationship object, or null when the file stores only relationship rows.</returns>
    /// <exception cref="JetCorruptDataException">The relationship identity is ambiguous or shared with another object.</exception>
    internal async ValueTask<CatalogRow?> FindRelationshipCatalogObjectAsync(string relationshipName, CancellationToken cancellationToken)
    {
        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(objects, cancellationToken).ConfigureAwait(false);
        CatalogRow? match = null;
        foreach (CatalogRow row in rows)
        {
            if (row.ParentId != Constants.SystemObjects.RelationshipsParentId
                || !string.Equals(row.Name, relationshipName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (match is not null || row.ObjectType != Constants.SystemObjects.RelationshipType || row.Id == 0)
            {
                throw AmbiguousRelationshipIdentity(relationshipName);
            }

            match = row;
        }

        if (match is null)
        {
            return null;
        }

        foreach (CatalogRow row in rows)
        {
            if (row.Id == match.Id && (row.PageNumber != match.PageNumber || row.RowIndex != match.RowIndex))
            {
                throw AmbiguousRelationshipIdentity(relationshipName);
            }
        }

        long acesPage = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesPage > 0)
        {
            TableDef aces = await tableDefs.ReadRequiredTableDefAsync(acesPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
            await indexes.ThrowIfIndexesUnmaintainableAsync(acesPage, aces, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        }

        return match;
    }

    /// <summary>Prepares complete catalog and permissions rows before a relationship drop changes any page.</summary>
    /// <param name="relationshipName">The relationship name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The prepared deletion, if a native identity exists.</returns>
    /// <exception cref="JetCorruptDataException">The permissions table has a malformed object identifier.</exception>
    internal async ValueTask<RelationshipCatalogDeletionArtifact?> PlanRelationshipCatalogDeletionAsync(string relationshipName, CancellationToken cancellationToken)
    {
        CatalogRow? identity = await this.FindRelationshipCatalogObjectAsync(relationshipName, cancellationToken).ConfigureAwait(false);
        if (identity is null)
        {
            return null;
        }

        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        LocatedRow original = await this.ReadCatalogObjectRowAsync(identity, cancellationToken).ConfigureAwait(false);
        UnreadableLongValue.ThrowIfAny(original.Values, Constants.SystemTableNames.Objects);
        long acesPage = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        TableDef? aces = null;
        var aceRows = new List<LocatedRow>();
        if (acesPage > 0)
        {
            aces = await tableDefs.ReadRequiredTableDefAsync(acesPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
            int objectIdIndex = aces.FindColumnIndex("ObjectId");
            if (objectIdIndex < 0)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs has no ObjectId column.", new JetErrorInfo { TableName = Constants.SystemTableNames.Aces, RelationshipName = relationshipName });
            }

            foreach (LocatedRow row in await snapshots.ReadRowsAsync(acesPage, cancellationToken).ConfigureAwait(false))
            {
                if (row.Values[objectIdIndex] is not int objectId)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs contains an invalid object identifier.", new JetErrorInfo { TableName = Constants.SystemTableNames.Aces, RelationshipName = relationshipName });
                }

                if (objectId == identity.Id)
                {
                    UnreadableLongValue.ThrowIfAny(row.Values, Constants.SystemTableNames.Aces);
                    aceRows.Add(row);
                }
            }
        }

        return new RelationshipCatalogDeletionArtifact(identity, objects, original, acesPage, aces, aceRows);
    }

    /// <summary>Deletes a prevalidated relationship object and only its associated permissions.</summary>
    /// <param name="deletion">The catalog/security rows resolved before mutation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask DeleteRelationshipCatalogObjectAsync(RelationshipCatalogDeletionArtifact? deletion, CancellationToken cancellationToken)
    {
        if (deletion is null)
        {
            return;
        }

        var plan = new MutationPlan();
        plan.ForTable(2, deletion.Objects, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogChange, $"dropping relationship '{deletion.Identity.Name}'")
            .Delete(deletion.Original.Location, deletion.Original.Values, DeletedRowDataMode.Clear);
        if (deletion.Aces is TableDef aces && deletion.AceRows.Count > 0)
        {
            MutationPlan.TableChanges changes = plan.ForTable(deletion.AcesPage, aces, Constants.SystemTableNames.Aces, MutationPlan.IndexPolicy.System);
            foreach (LocatedRow row in deletion.AceRows)
            {
                changes.Delete(row.Location, row.Values, DeletedRowDataMode.Clear);
            }
        }

        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
        catalog.Invalidate();
    }

    /// <summary>Plans a relationship object rename without changing its identity, permissions or other values.</summary>
    /// <param name="oldName">The existing relationship name.</param>
    /// <param name="newName">The replacement relationship name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The prevalidated rename, or null when no catalog identity exists.</returns>
    /// <exception cref="JetCorruptDataException">The relationship identity cannot be located unambiguously.</exception>
    internal async ValueTask<RelationshipCatalogRenameArtifact?> PlanRelationshipCatalogRenameAsync(string oldName, string newName, CancellationToken cancellationToken)
    {
        CatalogRow? identity = await this.FindRelationshipCatalogObjectAsync(oldName, cancellationToken).ConfigureAwait(false);
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        await this.EnsureCatalogContainerNameAvailableAsync(objects, Constants.SystemObjects.RelationshipsParentId, newName, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (identity is null)
        {
            return null;
        }

        LocatedRow original = await this.ReadCatalogObjectRowAsync(identity, cancellationToken).ConfigureAwait(false);
        int nameIndex = objects.FindColumnIndex("Name");
        object[] replacement = (object[])original.Values.Clone();
        replacement[nameIndex] = newName;
        UnreadableLongValue.ThrowIfAny(replacement, Constants.SystemTableNames.Objects);
        IReadOnlyDictionary<int, byte[]>? fixedTextBytes = original.GetPreservedFixedText(replacement, [nameIndex]);

        return new RelationshipCatalogRenameArtifact(objects, original, replacement, fixedTextBytes);
    }

    /// <summary>Applies a prevalidated catalog rename while preserving its security identity.</summary>
    /// <param name="rename">The prepared rename.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask RenameRelationshipCatalogObjectAsync(RelationshipCatalogRenameArtifact? rename, CancellationToken cancellationToken)
    {
        if (rename is null)
        {
            return;
        }

        var plan = new MutationPlan();
        plan.ForTable(2, rename.Definition, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogChange, "renaming relationship catalog identity")
            .Update(rename.Original.Location, rename.Original.Values, rename.Replacement, rename.FixedTextBytes, DeletedRowDataMode.Clear);
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
        catalog.Invalidate();
    }

    private static JetCorruptDataException AmbiguousRelationshipIdentity(string relationshipName)
        => new(JetErrorCode.CorruptCatalog, $"Relationship '{relationshipName}' has an ambiguous catalog identity.", new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, RelationshipName = relationshipName });

    internal ValueTask InsertAceRowsForTableAsync(long tdefPageNumber, CancellationToken cancellationToken)
        => this.InsertAceRowsForCatalogObjectAsync(checked((int)tdefPageNumber), useRelationshipGroupAcm: false, cancellationToken);

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

        if (deletedRows.Count > 0)
        {
            var plan = new MutationPlan();
            MutationPlan.TableChanges changes = plan.ForTable(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, MutationPlan.IndexPolicy.System);
            foreach ((RowLocation row, object[] values) in deletedRows)
            {
                changes.Delete(row, values, DeletedRowDataMode.Clear);
            }

            await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
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

    /// <summary>Reads the full original values needed by every supported catalog index.</summary>
    /// <param name="identity">The exclusive object identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The original object row.</returns>
    /// <exception cref="JetCorruptDataException">The object row cannot be resolved unambiguously.</exception>
    private async ValueTask<LocatedRow> ReadCatalogObjectRowAsync(CatalogRow identity, CancellationToken cancellationToken)
    {
        List<LocatedRow> matches = (await snapshots.ReadRowsAsync(2, cancellationToken).ConfigureAwait(false)).FindAll(
            row => row.Location.PageNumber == identity.PageNumber && row.Location.RowIndex == identity.RowIndex);
        if (matches.Count != 1)
        {
            throw AmbiguousRelationshipIdentity(identity.Name);
        }

        return matches[0];
    }

    private async ValueTask<object> EncodeLinkedMemoFieldAsync(string value, int columnNumber, CancellationToken cancellationToken)
    {
        object? encoded = await longValueEncoder.ForceEncodeMemoAsLvalAsync(value, compress: false, tableTdefPage: 2, columnNumber, cancellationToken).ConfigureAwait(false);
        return encoded ?? value;
    }

    /// <summary>Defers Jet3 catalog property storage to a native external LVAL even for a small payload.</summary>
    /// <param name="properties">The unchanged property-block bytes.</param>
    /// <returns>The value prepared for row validation and later long-value allocation.</returns>
    private object PrepareCatalogProperties(byte[] properties)
        => format.IsJet3
            ? LongValueEncoder.PrepareExternalValue(properties)
            : properties;

    /// <summary>Reads the unique native database owner without synthesizing security metadata.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The encoded database owner.</returns>
    /// <exception cref="JetCorruptDataException">The native database security owner is missing or malformed.</exception>
    private ValueTask<byte[]> ReadDatabaseOwnerAsync(CancellationToken cancellationToken) => this.ReadCatalogSecurityObjectAsync("MSysDb", Constants.SystemObjects.DatabaseObjectId, 2, cancellationToken);

    /// <summary>Validates a native catalog security object unique by name and identifier.</summary>
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
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects has no database security owner columns.");
        }

        ColumnInfo idColumn = objects.FindColumn("Id") ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects has no security object identifier column.");
        ColumnInfo typeColumn = objects.FindColumn("Type") ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects has no security object type column.");
        byte[]? owner = null;
        int matches = 0;
        await ownedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                RowLocation location = row.Location;
                string name = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, nameColumn);
                bool validId = CatalogValueReader.TryParseInt32(ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, idColumn), out int id);
                if (!string.Equals(name, objectName, StringComparison.OrdinalIgnoreCase))
                {
                    if (validId && id == objectId)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"MSysObjects contains another object sharing the {objectName} security identifier.", new JetErrorInfo { ObjectName = objectName });
                    }

                    return new ValueTask<bool>(true);
                }

                if (++matches != 1)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"MSysObjects contains duplicate {objectName} security objects.", new JetErrorInfo { ObjectName = objectName });
                }

                if (!validId || id != objectId
                    || !CatalogValueReader.TryParseInt32(ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, typeColumn), out int type) || type != objectType)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"{objectName} has an invalid security object identity.", new JetErrorInfo { ObjectName = objectName });
                }

                string value = ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, ownerColumn);
                owner = value.Length == 0 ? null : ParseHexBytes(value);
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);
        return owner is { Length: > 0 }
            ? owner
            : throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"MSysObjects has no valid {objectName} security owner.", new JetErrorInfo { ObjectName = objectName });
    }

    /// <summary>Inherits native container permissions and resolves the format-specific owner placeholder.</summary>
    /// <param name="relationships">Whether to inherit relationship-container permissions.</param>
    /// <param name="acesTdefPage">The permissions table page.</param>
    /// <param name="acesDef">The permissions table definition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The validated and remapped native permissions.</returns>
    /// <exception cref="JetCorruptDataException">Required security metadata is malformed.</exception>
    /// <exception cref="JetNotSupportedException">The container has no inheritable permissions.</exception>
    private async ValueTask<(byte[] Sid, int Acm)[]> ReadInheritedPermissionsAsync(
        bool relationships,
        long acesTdefPage,
        TableDef acesDef,
        CancellationToken cancellationToken)
    {
        byte[] owner = await this.ReadDatabaseOwnerAsync(cancellationToken).ConfigureAwait(false);

        ColumnInfo objectColumn = acesDef.FindColumn("ObjectId") ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs has no ObjectId column.");
        ColumnInfo sidColumn = acesDef.FindColumn("SID") ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs has no SID column.");
        ColumnInfo acmColumn = acesDef.FindColumn("ACM") ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs has no ACM column.");
        ColumnInfo inheritColumn = acesDef.FindColumn("FInheritable") ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs has no FInheritable column.");
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
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs contains an invalid object identifier.");
                }

                if (id == parentId)
                {
                    string inherit = Decode(inheritColumn);
                    if (inherit is not ("True" or "False"))
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "A container MSysACEs entry has an invalid inheritance flag.");
                    }

                    if (!CatalogValueReader.TryParseInt32(Decode(acmColumn), out int acm))
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "A container MSysACEs entry has an invalid permission mask.");
                    }

                    byte[] sid = ParseHexBytes(Decode(sidColumn));
                    if (sid.Length == 0)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "A container MSysACEs entry has no security identity.");
                    }

                    if (inherit == "True")
                    {
                        if (!storedIdentities.Add(BitConverter.ToString(sid)))
                        {
                            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "Native inherited container permissions contain duplicate stored security identities.");
                        }

                        inherited.Add((sid, acm));
                    }
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);
        if (inherited.Count == 0)
        {
            throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, "The database has no inheritable catalog permissions for the new object.");
        }

        byte[] placeholder;
        if (format.IsJet3)
        {
            placeholder = [0x02, 0x04];
        }
        else
        {
            byte[] header = await pager.ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
            try
            {
                placeholder = Jet4SecuritySid.GetOwnerPlaceholder(format, header);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
            }
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
        long page = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (page <= 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The native permissions table is absent.");
        }

        _ = await this.ReadCatalogSecurityObjectAsync(Constants.SystemTableNames.Aces, checked((int)page), 1, cancellationToken).ConfigureAwait(false);
        TableDef definition = await tableDefs.ReadRequiredTableDefAsync(page, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        _ = await this.ReadInheritedPermissionsAsync(relationships, page, definition, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes permissions for a catalog object using its native container policy.</summary>
    /// <param name="objectId">The object identifier.</param>
    /// <param name="useRelationshipGroupAcm">Whether to use relationship-container permissions.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetCorruptDataException">The native permissions table is absent.</exception>
    private async ValueTask InsertAceRowsForCatalogObjectAsync(
        int objectId,
        bool useRelationshipGroupAcm,
        CancellationToken cancellationToken)
    {
        long acesTdefPage = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesTdefPage <= 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The native permissions table is absent.");
        }

        TableDef acesDef = await tableDefs.ReadRequiredTableDefAsync(acesTdefPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        (byte[] Sid, int Acm)[] inherited = await this.ReadInheritedPermissionsAsync(useRelationshipGroupAcm, acesTdefPage, acesDef, cancellationToken).ConfigureAwait(false);
        var plan = new MutationPlan();
        MutationPlan.TableChanges changes = plan.ForTable(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, MutationPlan.IndexPolicy.System);
        foreach ((byte[] sid, int acm) in inherited)
        {
            object[] row = acesDef.CreateNullValueRow();
            acesDef.SetValueByName(row, "ObjectId", objectId);
            acesDef.SetValueByName(row, "ACM", acm);
            acesDef.SetValueByName(row, "FInheritable", false);
            acesDef.SetValueByName(row, "SID", sid);
            changes.Insert(row);
        }

        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask UpdateTablePropertiesAsync(
        string tableName,
        long tdefPage,
        byte[]? properties,
        bool? hasComplexColumns = null,
        CancellationToken cancellationToken = default)
    {
        await this.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);

        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        int nameIndex = objects.FindColumnIndex("Name");
        int idIndex = objects.FindColumnIndex("Id");
        int typeIndex = objects.FindColumnIndex("Type");
        int flagsIndex = objects.FindColumnIndex("Flags");
        int propertyIndex = objects.FindColumnIndex("LvProp");
        if (nameIndex < 0 || idIndex < 0 || typeIndex < 0 || flagsIndex < 0 || propertyIndex < 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects lacks required table identity or property columns.");
        }

        List<LocatedRow> matches = (await snapshots.ReadRowsAsync(2, cancellationToken).ConfigureAwait(false)).FindAll(row =>
            string.Equals(row.Values[nameIndex] as string, tableName, StringComparison.OrdinalIgnoreCase)
            && Convert.ToInt32(row.Values[typeIndex], System.Globalization.CultureInfo.InvariantCulture) == Constants.SystemObjects.UserTableType
            && CatalogValueReader.TdefPageFromId(Convert.ToInt64(row.Values[idIndex], System.Globalization.CultureInfo.InvariantCulture)) == tdefPage);
        if (matches.Count != 1)
        {
            throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, $"Table '{tableName}' has {matches.Count} matching catalog rows.", new JetErrorInfo { ObjectName = tableName });
        }

        LocatedRow original = matches[0];
        object[] replacement = (object[])original.Values.Clone();
        replacement[propertyIndex] = properties is null ? DBNull.Value : this.PrepareCatalogProperties(properties);
        if (hasComplexColumns is bool complex)
        {
            uint flags = unchecked((uint)Convert.ToInt32(original.Values[flagsIndex], System.Globalization.CultureInfo.InvariantCulture));
            replacement[flagsIndex] = unchecked((int)((flags & ~0x00040000U) | (complex ? 0x00040000U : 0U)));
        }

        UnreadableLongValue.ThrowIfAny(replacement, Constants.SystemTableNames.Objects);
        IReadOnlyDictionary<int, byte[]>? fixedTextBytes = original.GetPreservedFixedText(replacement, [propertyIndex, flagsIndex]);

        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        var plan = new MutationPlan();
        plan.ForTable(2, objects, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogRelocation, tableName)
            .Update(original.Location, original.Values, replacement, fixedTextBytes, DeletedRowDataMode.Clear);
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);

        catalog.Invalidate();
    }

    internal async ValueTask<IReadOnlyList<long>> DeleteUserTableCatalogRowsAsync(
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
        var plan = new MutationPlan();
        MutationPlan.TableChanges changes = plan.ForTable(2, msys, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogChange, operation);

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

            object[] indexRow = CreateMsysObjectsIndexRow(msys, row);
            changes.Delete(new RowLocation(row.PageNumber, row.RowIndex, 0, 0), indexRow, DeletedRowDataMode.Clear);
        }

        if (changes.Rows.Count == 0)
        {
            if (throwIfNotFound)
            {
                throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, missingMessage ?? $"Catalog row for '{tableName}' was not found.", errorInfo: new JetErrorInfo { TableName = tableName });
            }

            return [];
        }

        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);

        catalog.Invalidate();
        return droppedTdefPages;
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

    private async ValueTask EnsureCatalogContainerNameAvailableAsync(TableDef msys, int parentId, string objectName, long? excludedTdefPage = null, CancellationToken cancellationToken = default)
    {
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            bool sameParent = row.ParentId == parentId
                || (parentId == Constants.SystemObjects.TablesParentId && row.ParentId == 0);
            if (sameParent && row.TDefPage != excludedTdefPage && string.Equals(row.Name, objectName, StringComparison.OrdinalIgnoreCase))
            {
                throw new JetObjectExistsException(JetErrorCode.ObjectExists, $"An object named '{objectName}' already exists.", errorInfo: new JetErrorInfo { ObjectName = objectName });
            }
        }
    }

    private async ValueTask<int> AllocateNonTableObjectIdAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo? idColumn = msys.FindColumn("Id")
            ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects does not expose an 'Id' column.");

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

        throw new JetLimitationException(JetErrorCode.ValueTooLarge, "No free negative MSysObjects catalog object id is available.");
    }
}
