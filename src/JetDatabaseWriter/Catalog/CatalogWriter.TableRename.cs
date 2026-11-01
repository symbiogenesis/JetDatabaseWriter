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

/// <content>Identity-preserving local and linked table renames.</content>
internal sealed partial class CatalogWriter
{
    /// <summary>Renames one visible table and its relationship references in one prepared mutation.</summary>
    /// <param name="oldName">The existing table name.</param>
    /// <param name="newName">The validated replacement name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetOperationException">The named table does not exist.</exception>
    /// <exception cref="JetObjectExistsException">Another catalog object has the replacement name.</exception>
    /// <exception cref="JetCorruptDataException">The table's catalog identity or required relationship columns are ambiguous.</exception>
    internal async ValueTask RenameTableAsync(string oldName, string newName, CancellationToken cancellationToken)
    {
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> identities = await catalogRows.GetCatalogRowsAsync(objects, cancellationToken).ConfigureAwait(false);
        if (identities.Exists(row => !row.IsDecoded))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The catalog contains an unreadable object identity.");
        }

        List<CatalogRow> matches = identities.FindAll(row =>
            row.ObjectType is Constants.SystemObjects.UserTableType or Constants.SystemObjects.LinkedTableType or Constants.SystemObjects.LinkedOdbcType
            && (unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) == 0
            && string.Equals(row.Name, oldName, StringComparison.OrdinalIgnoreCase));
        if (matches.Count == 0)
        {
            throw new JetOperationException(JetErrorCode.TableNotFound, $"Table '{oldName}' was not found.", new JetErrorInfo { TableName = oldName });
        }

        CatalogRow identity = matches[0];
        if (matches.Count != 1 || identities.FindAll(row => row.Id == identity.Id).Count != 1)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"Table '{oldName}' has an ambiguous catalog identity.", new JetErrorInfo { TableName = oldName });
        }

        if (string.Equals(identity.Name, newName, StringComparison.Ordinal))
        {
            return;
        }

        foreach (CatalogRow row in identities)
        {
            if (row.PageNumber == identity.PageNumber && row.RowIndex == identity.RowIndex)
            {
                continue;
            }

            bool sameContainer = row.ParentId == identity.ParentId
                || ((row.ParentId == 0 || row.ParentId == Constants.SystemObjects.TablesParentId)
                    && (identity.ParentId == 0 || identity.ParentId == Constants.SystemObjects.TablesParentId));
            if (sameContainer && string.Equals(row.Name, newName, StringComparison.OrdinalIgnoreCase))
            {
                throw new JetObjectExistsException(JetErrorCode.TableExists, $"An object named '{newName}' already exists.", new JetErrorInfo { TableName = newName });
            }
        }

        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        LocatedRow original = await this.ReadCatalogObjectRowAsync(identity, cancellationToken).ConfigureAwait(false);
        int nameIndex = objects.FindColumnIndex("Name");
        object[] replacement = (object[])original.Values.Clone();
        replacement[nameIndex] = newName;
        UnreadableLongValue.ThrowIfAny(replacement, Constants.SystemTableNames.Objects);
        IReadOnlyDictionary<int, byte[]>? fixedText = original.GetPreservedText(replacement, [nameIndex]);
        IReadOnlyDictionary<int, byte[]> longValues = await this.ReadUnassignedLongValuesAsync(objects, original, replacement, cancellationToken).ConfigureAwait(false);
        var plan = new MutationPlan();
        plan.ForTable(2, objects, Constants.SystemTableNames.Objects, MutationPlan.IndexPolicy.CatalogChange, $"renaming table '{oldName}'")
            .Update(original.Location, original.Values, replacement, fixedText, DeletedRowDataMode.Clear, longValues);
        await this.PlanRelationshipTableRenameAsync(plan, identity.Name, newName, cancellationToken).ConfigureAwait(false);
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
        catalog.Invalidate();
    }

    private async ValueTask PlanRelationshipTableRenameAsync(MutationPlan plan, string oldName, string newName, CancellationToken cancellationToken)
    {
        long root = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (root <= 0)
        {
            return;
        }

        TableDef definition = await tableDefs.ReadRequiredTableDefAsync(root, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        int objectIndex = definition.FindColumnIndex("szObject");
        int referencedIndex = definition.FindColumnIndex("szReferencedObject");
        if (objectIndex < 0 || referencedIndex < 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysRelationships lacks required table-name columns.");
        }

        foreach (LocatedRow row in await snapshots.ReadRowsAsync(root, cancellationToken).ConfigureAwait(false))
        {
            bool isForeign = string.Equals(row.Values[objectIndex] as string, oldName, StringComparison.OrdinalIgnoreCase);
            bool isPrimary = string.Equals(row.Values[referencedIndex] as string, oldName, StringComparison.OrdinalIgnoreCase);
            if (!isForeign && !isPrimary)
            {
                continue;
            }

            object[] values = (object[])row.Values.Clone();
            if (isForeign)
            {
                values[objectIndex] = newName;
            }

            if (isPrimary)
            {
                values[referencedIndex] = newName;
            }

            UnreadableLongValue.ThrowIfAny(values, Constants.SystemTableNames.Relationships);
            IReadOnlyDictionary<int, byte[]>? fixedText = row.GetPreservedText(values, [objectIndex, referencedIndex]);
            IReadOnlyDictionary<int, byte[]> longValues = await this.ReadUnassignedLongValuesAsync(definition, row, values, cancellationToken).ConfigureAwait(false);
            plan.ForTable(root, definition, Constants.SystemTableNames.Relationships, MutationPlan.IndexPolicy.System)
                .Update(row.Location, row.Values, values, fixedText, DeletedRowDataMode.Clear, longValues);
        }
    }
}
