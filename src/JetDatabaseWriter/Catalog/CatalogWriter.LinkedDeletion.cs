namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.ValueDecoding.Models;

internal sealed partial class CatalogWriter
{
    /// <summary>Resolves a linked table's exclusive catalog identity before dependent metadata changes.</summary>
    /// <param name="tableName">The local linked-table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether the name identifies a non-system linked table.</returns>
    /// <exception cref="JetCorruptDataException">The linked-table identity is ambiguous or malformed.</exception>
    internal async ValueTask<bool> ContainsLinkedTableAsync(string tableName, CancellationToken cancellationToken)
    {
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> entries = await catalogRows.GetCatalogRowsAsync(objects, cancellationToken).ConfigureAwait(false);
        CatalogRow? identity = FindLinkedIdentity(tableName, entries);
        if (identity is null)
        {
            return false;
        }

        LocatedRow original = await this.ReadCatalogObjectRowAsync(identity, cancellationToken).ConfigureAwait(false);
        UnreadableLongValue.ThrowIfAny(original.Values, Constants.SystemTableNames.Objects);
        return true;
    }

    /// <summary>Deletes a linked table's catalog identity and permissions without acquiring its source.</summary>
    /// <param name="tableName">The local linked-table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether a non-system linked table was removed.</returns>
    /// <exception cref="JetCorruptDataException">The catalog identity or permissions are ambiguous or malformed.</exception>
    internal async ValueTask<bool> TryDeleteLinkedTableAsync(string tableName, CancellationToken cancellationToken)
    {
        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> entries = await catalogRows.GetCatalogRowsAsync(objects, cancellationToken).ConfigureAwait(false);
        CatalogRow? identity = FindLinkedIdentity(tableName, entries);
        if (identity is null)
        {
            return false;
        }

        LocatedRow original = await this.ReadCatalogObjectRowAsync(identity, cancellationToken).ConfigureAwait(false);
        UnreadableLongValue.ThrowIfAny(original.Values, Constants.SystemTableNames.Objects);
        IReadOnlyDictionary<int, byte[]> longValues = await this.ReadUnassignedLongValuesAsync(objects, original, original.Values, cancellationToken).ConfigureAwait(false);
        var plan = new MutationPlan();
        plan.ForTable(2, objects, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogChange, $"dropping linked table '{tableName}'")
            .Delete(original.Location, original.Values, DeletedRowDataMode.Clear, longValues);

        long acesPage = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesPage > 0)
        {
            TableDef aces = await tableDefs.ReadRequiredTableDefAsync(acesPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
            await indexes.ThrowIfIndexesUnmaintainableAsync(acesPage, aces, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
            int objectIdIndex = aces.FindColumnIndex("ObjectId");
            if (objectIdIndex < 0)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs has no ObjectId column.", new JetErrorInfo { TableName = Constants.SystemTableNames.Aces, ObjectName = tableName });
            }

            MutationPlan.TableChanges permissions = plan.ForTable(acesPage, aces, Constants.SystemTableNames.Aces, MutationPlan.IndexPolicy.System);
            foreach (LocatedRow row in await snapshots.ReadRowsAsync(acesPage, cancellationToken).ConfigureAwait(false))
            {
                if (row.Values[objectIdIndex] is not int objectId)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysACEs contains an invalid object identifier.", new JetErrorInfo { TableName = Constants.SystemTableNames.Aces, ObjectName = tableName });
                }

                if (objectId == identity.Id)
                {
                    UnreadableLongValue.ThrowIfAny(row.Values, Constants.SystemTableNames.Aces);
                    IReadOnlyDictionary<int, byte[]> retained = await this.ReadUnassignedLongValuesAsync(aces, row, row.Values, cancellationToken).ConfigureAwait(false);
                    permissions.Delete(row.Location, row.Values, DeletedRowDataMode.Clear, retained);
                }
            }
        }

        // A link's Id need not identify a physical table definition. Retain its
        // cached native definition and external metadata until complete incoming
        // ownership permits reclaiming them; another opaque object can share it.
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
        catalog.Invalidate();
        return true;
    }

    private static CatalogRow? FindLinkedIdentity(string tableName, IReadOnlyList<CatalogRow> entries)
    {
        CatalogRow? identity = null;
        foreach (CatalogRow entry in entries)
        {
            if (entry.ObjectType is Constants.SystemObjects.LinkedTableType or Constants.SystemObjects.LinkedOdbcType
                && (unchecked((uint)entry.Flags) & Constants.SystemObjects.SystemTableMask) == 0
                && string.Equals(entry.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                if (identity is not null || entry.Id == 0
                    || entry.ParentId is not 0 and not Constants.SystemObjects.TablesParentId)
                {
                    throw AmbiguousLinkedIdentity(tableName);
                }

                identity = entry;
            }
        }

        if (identity is null)
        {
            return null;
        }

        foreach (CatalogRow entry in entries)
        {
            if ((entry.PageNumber != identity.PageNumber || entry.RowIndex != identity.RowIndex)
                && (entry.Id == identity.Id
                    || (entry.ParentId is 0 or Constants.SystemObjects.TablesParentId
                        && string.Equals(entry.Name, tableName, StringComparison.OrdinalIgnoreCase))))
            {
                throw AmbiguousLinkedIdentity(tableName);
            }
        }

        return identity;
    }

    private static JetCorruptDataException AmbiguousLinkedIdentity(string tableName)
        => new(JetErrorCode.CorruptCatalog, $"Linked table '{tableName}' has an ambiguous catalog identity.", new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, ObjectName = tableName });
}
