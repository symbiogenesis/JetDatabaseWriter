namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.ValueDecoding.Models;

/// <summary>
/// Runtime foreign-key enforcement for insert, update, and delete, including
/// cascade-update and cascade-delete of dependent rows.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="tableCatalog">Resolves child tables by name.</param>
/// <param name="tableRows">Deletes and rewrites cascaded child rows.</param>
/// <param name="indexes">Rebuilds child-table indexes after cascades.</param>
/// <param name="catalog">Loads the enforced relationships from <c>MSysRelationships</c>.</param>
/// <param name="complexColumns">Cascades deletes into complex-column child rows.</param>
/// <param name="snapshots">Reads decoded parent and child rows when no seekable index exists.</param>
internal sealed class RelationshipEnforcer(
    DatabaseFile db,
    TableCatalog tableCatalog,
    TableRowStore tableRows,
    IndexMaintainer indexes,
    RelationshipCatalogStore catalog,
    ComplexColumnManager complexColumns,
    TableSnapshotReader snapshots)
{
    private readonly RelationshipSeekPlanner seekPlanner = new(db, tableCatalog);
    private readonly RelationshipChildRowLocator childRowLocator = new(db);

    public static void AugmentParentSetsAfterInsert(string primaryTable, TableDef tableDef, object[] insertedValues, FkContext ctx)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? set))
            {
                continue;
            }

            int[] primaryColumnIndexes = new int[rel.PrimaryColumns.Count];
            bool ok = true;
            for (int index = 0; index < rel.PrimaryColumns.Count; index++)
            {
                primaryColumnIndexes[index] = tableDef.FindColumnIndex(rel.PrimaryColumns[index]);
                if (primaryColumnIndexes[index] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            string? key = RelationshipKeyBuilder.Build(insertedValues, primaryColumnIndexes);
            if (key != null)
            {
                _ = set.Add(key);
            }
        }
    }

    public ValueTask<IReadOnlyList<FkRelationship>> GetEnforcedRelationshipsAsync(CancellationToken cancellationToken)
        => catalog.GetEnforcedRelationshipsAsync(cancellationToken);

    public async ValueTask<HashSet<string>> GetParentKeySetAsync(FkRelationship rel, FkContext ctx, CancellationToken cancellationToken)
    {
        if (ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? cached))
        {
            return cached;
        }

        var set = new HashSet<string>(StringComparer.Ordinal);

        DataTable parent;
        try
        {
            parent = await snapshots.ReadTableSnapshotAsync(rel.PrimaryTable, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            ctx.ParentKeySets[rel.Name] = set;
            return set;
        }

        try
        {
            int[] indexesByColumn = new int[rel.PrimaryColumns.Count];
            bool ok = true;
            for (int index = 0; index < rel.PrimaryColumns.Count; index++)
            {
                indexesByColumn[index] = parent.Columns.IndexOf(rel.PrimaryColumns[index]);
                if (indexesByColumn[index] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (ok)
            {
                foreach (DataRow row in parent.Rows)
                {
                    string? key = RelationshipKeyBuilder.Build(row.ItemArray, indexesByColumn);
                    if (key != null)
                    {
                        _ = set.Add(key);
                    }
                }
            }
        }
        finally
        {
            parent.Dispose();
        }

        ctx.ParentKeySets[rel.Name] = set;
        return set;
    }

    /// <summary>
    /// Checks that every non-null foreign key of a row about to be inserted
    /// into <paramref name="foreignTable"/> names an existing parent row.
    /// </summary>
    /// <param name="foreignTable">The table the row is inserted into.</param>
    /// <param name="foreignDef">The table's definition.</param>
    /// <param name="values">The row, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">A foreign key has no matching parent row.</exception>
    public async ValueTask EnforceFkOnInsertAsync(
        string foreignTable,
        TableDef foreignDef,
        object[] values,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.ForeignTable, foreignTable, StringComparison.OrdinalIgnoreCase)
                || !TryMapColumns(rel.ForeignColumns, foreignDef, out int[] foreignColumnIndexes))
            {
                continue;
            }

            string? key = RelationshipKeyBuilder.Build(values, foreignColumnIndexes);
            if (key == null)
            {
                continue;
            }

            await this.RequireParentKeyAsync(rel, foreignTable, values, key, ctx, FkCheckKind.Insert, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Checks the foreign keys an update of <paramref name="foreignTable"/>
    /// changes. As in Access, a relationship is checked only on the rows whose
    /// foreign key the update changes to a non-null value: a relationship none
    /// of whose foreign-key columns the update assigns is skipped, and so is a
    /// row whose key keeps its value, even when that key has no parent row.
    /// </summary>
    /// <param name="foreignTable">The table being updated.</param>
    /// <param name="foreignDef">The table's definition.</param>
    /// <param name="assignedColumns">The ordinals of the columns the update assigns.</param>
    /// <param name="rows">Each matching row before and after the update, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">A changed foreign key has no matching parent row.</exception>
    public async ValueTask EnforceFkOnForeignUpdateAsync(
        string foreignTable,
        TableDef foreignDef,
        ICollection<int> assignedColumns,
        IReadOnlyList<(object[] OldRow, object[] NewRow)> rows,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.ForeignTable, foreignTable, StringComparison.OrdinalIgnoreCase)
                || !TryMapColumns(rel.ForeignColumns, foreignDef, out int[] foreignColumnIndexes)
                || !Array.Exists(foreignColumnIndexes, assignedColumns.Contains))
            {
                continue;
            }

            foreach ((object[] oldRow, object[] newRow) in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                string? newKey = RelationshipKeyBuilder.Build(newRow, foreignColumnIndexes);
                if (newKey == null
                    || string.Equals(newKey, RelationshipKeyBuilder.Build(oldRow, foreignColumnIndexes), StringComparison.Ordinal))
                {
                    continue;
                }

                await this.RequireParentKeyAsync(rel, foreignTable, newRow, newKey, ctx, FkCheckKind.Update, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async ValueTask EnforceFkOnPrimaryDeleteAsync(
        string primaryTable,
        TableDef primaryDef,
        List<object?[]> deletedParentRows,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        RelationshipCascadePolicy.ThrowIfDepthExceeded(depth);

        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            ResolvedTable childTable = await tableCatalog.ResolveRequiredTableAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            CatalogEntry childEntry = childTable.Entry;
            TableDef childDef = childTable.Definition;

            if (!TryMapFkPairOrdinals(rel, primaryDef, childDef, out int[] primaryPkIdx, out int[] fkIdx))
            {
                continue;
            }

            List<object?[]> parentPkRows = RelationshipKeyBuilder.ProjectNonNullKeys(deletedParentRows, primaryPkIdx);
            if (parentPkRows.Count == 0)
            {
                continue;
            }

            ChildSeekIndex? childSeek = await this.seekPlanner.ResolveChildSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (childSeek != null)
            {
                bool seekOk = await this.TryProcessCascadeDeleteWithSeekAsync(
                    rel,
                    childEntry,
                    childDef,
                    childSeek,
                    parentPkRows,
                    ctx,
                    depth,
                    cancellationToken).ConfigureAwait(false);
                if (seekOk)
                {
                    continue;
                }
            }

            List<LocatedRow> childRows = await snapshots.ReadRowsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            HashSet<string> deletedSet = RelationshipKeyBuilder.BuildSetFromProjectedKeys(parentPkRows);

            var matchingRows = new List<LocatedRow>();
            foreach (LocatedRow childRow in childRows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = RelationshipKeyBuilder.Build(childRow.Values, fkIdx);
                if (childKey != null && deletedSet.Contains(childKey))
                {
                    matchingRows.Add(childRow);
                }
            }

            if (matchingRows.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeDeletes)
            {
                throw new InvalidOperationException(
                    $"DELETE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{matchingRows.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
            }

            var childDeletedRows = new List<object?[]>(matchingRows.Count);
            var cascadeLocations = new List<RowLocation>(matchingRows.Count);
            foreach ((RowLocation location, object[] values) in matchingRows)
            {
                childDeletedRows.Add(values);
                cascadeLocations.Add(location);
            }

            await this.EnforceFkOnPrimaryDeleteAsync(
                rel.ForeignTable,
                childDef,
                childDeletedRows,
                ctx,
                depth + 1,
                cancellationToken).ConfigureAwait(false);

            await complexColumns.CascadeDeleteComplexChildrenAsync(childDef, cascadeLocations, cancellationToken).ConfigureAwait(false);

            int deleted = 0;
            foreach (RowLocation location in cascadeLocations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await tableRows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
                deleted++;
            }

            if (deleted > 0)
            {
                await tableRows.AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
                await indexes.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Cascades or refuses the primary-key changes an update of
    /// <paramref name="primaryTable"/> makes. Each relationship is checked
    /// against its own primary columns: one none of whose primary columns the
    /// update assigns is skipped, and only the rows whose key in those columns
    /// changes from a non-null value to another non-null value move their
    /// dependent rows.
    /// </summary>
    /// <param name="primaryTable">The table being updated.</param>
    /// <param name="primaryDef">The table's definition.</param>
    /// <param name="assignedColumns">The ordinals of the columns the update assigns.</param>
    /// <param name="rows">Each matching row before and after the update, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="depth">The cascade depth.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">A changed key has dependent rows and the relationship does not cascade updates.</exception>
    public async ValueTask EnforceFkOnPrimaryUpdateAsync(
        string primaryTable,
        TableDef primaryDef,
        ICollection<int> assignedColumns,
        IReadOnlyList<(object[] OldRow, object[] NewRow)> rows,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        RelationshipCascadePolicy.ThrowIfDepthExceeded(depth);

        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase)
                || !TryMapColumns(rel.PrimaryColumns, primaryDef, out int[] primaryPkIdx)
                || !Array.Exists(primaryPkIdx, assignedColumns.Contains))
            {
                continue;
            }

            var movingChanges = new Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)>(StringComparer.Ordinal);
            foreach ((object[] oldRow, object[] newRow) in rows)
            {
                string? oldKey = RelationshipKeyBuilder.Build(oldRow, primaryPkIdx);
                string? newKey = RelationshipKeyBuilder.Build(newRow, primaryPkIdx);
                if (oldKey == null || newKey == null || string.Equals(newKey, oldKey, StringComparison.Ordinal))
                {
                    continue;
                }

                object[] newPkSubset = new object[rel.PrimaryColumns.Count];
                object?[] oldPkSubset = new object?[rel.PrimaryColumns.Count];
                for (int index = 0; index < rel.PrimaryColumns.Count; index++)
                {
                    newPkSubset[index] = newRow[primaryPkIdx[index]];
                    oldPkSubset[index] = oldRow[primaryPkIdx[index]];
                }

                movingChanges[oldKey] = (oldPkSubset, newPkSubset);
            }

            if (movingChanges.Count == 0)
            {
                continue;
            }

            ResolvedTable childTable = await tableCatalog.ResolveRequiredTableAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            CatalogEntry childEntry = childTable.Entry;
            TableDef childDef = childTable.Definition;
            if (!TryMapColumns(rel.ForeignColumns, childDef, out int[] fkIdx))
            {
                continue;
            }

            ChildSeekIndex? childSeek = await this.seekPlanner.ResolveChildSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (childSeek != null)
            {
                bool seekOk = await this.TryProcessCascadeUpdateWithSeekAsync(
                    rel,
                    childEntry,
                    childDef,
                    childSeek,
                    movingChanges,
                    fkIdx,
                    cancellationToken).ConfigureAwait(false);
                if (seekOk)
                {
                    continue;
                }
            }

            List<LocatedRow> childRows = await snapshots.ReadRowsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            var affectedRows = new List<(LocatedRow Row, string OldKey)>();
            foreach (LocatedRow childRow in childRows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = RelationshipKeyBuilder.Build(childRow.Values, fkIdx);
                if (childKey != null && movingChanges.ContainsKey(childKey))
                {
                    affectedRows.Add((childRow, childKey));
                }
            }

            if (affectedRows.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeUpdates)
            {
                throw new InvalidOperationException(
                    $"UPDATE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{affectedRows.Count} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
            }

            // Build every rewritten child row first so an unreadable MEMO / OLE
            // value refuses the cascade before any child row is deleted.
            object[][] rewrittenRows = new object[affectedRows.Count][];
            for (int affectedIndex = 0; affectedIndex < affectedRows.Count; affectedIndex++)
            {
                (LocatedRow childRow, string oldKey) = affectedRows[affectedIndex];
                object[] newPkSubset = movingChanges[oldKey].NewPkSubset;
                object[] rowValues = (object[])childRow.Values.Clone();

                for (int column = 0; column < rel.ForeignColumns.Count; column++)
                {
                    rowValues[fkIdx[column]] = newPkSubset[column] ?? DBNull.Value;
                }

                UnreadableLongValue.ThrowIfAny(rowValues, rel.ForeignTable);
                rewrittenRows[affectedIndex] = rowValues;
            }

            for (int affectedIndex = 0; affectedIndex < affectedRows.Count; affectedIndex++)
            {
                RowLocation location = affectedRows[affectedIndex].Row.Location;
                await tableRows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
                await tableRows.InsertRowDataAsync(childEntry.TDefPage, childDef, rewrittenRows[affectedIndex], updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            await indexes.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool TryMapFkPairOrdinals(
        FkRelationship rel,
        TableDef primaryDef,
        TableDef childDef,
        out int[] primaryPkIdx,
        out int[] fkIdx)
    {
        int count = rel.PrimaryColumns.Count;
        primaryPkIdx = new int[count];
        fkIdx = new int[count];
        for (int index = 0; index < count; index++)
        {
            primaryPkIdx[index] = primaryDef.FindColumnIndex(rel.PrimaryColumns[index]);
            fkIdx[index] = childDef.FindColumnIndex(rel.ForeignColumns[index]);
            if (primaryPkIdx[index] < 0 || fkIdx[index] < 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Maps column names to their ordinals in <paramref name="definition"/>.</summary>
    /// <param name="names">The column names.</param>
    /// <param name="definition">The table definition.</param>
    /// <param name="ordinals">The ordinals, in the order of <paramref name="names"/>.</param>
    /// <returns><see langword="false"/> when a column is not in the table.</returns>
    private static bool TryMapColumns(IReadOnlyList<string> names, TableDef definition, out int[] ordinals)
    {
        ordinals = new int[names.Count];
        for (int index = 0; index < names.Count; index++)
        {
            ordinals[index] = definition.FindColumnIndex(names[index]);
            if (ordinals[index] < 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Builds the error for a foreign key with no matching parent row.</summary>
    /// <param name="rel">The relationship.</param>
    /// <param name="foreignTable">The table being written.</param>
    /// <param name="kind">Whether an insert or an update supplied the key.</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException ForeignKeyViolation(FkRelationship rel, string foreignTable, FkCheckKind kind)
    {
        string columns = string.Join(", ", rel.ForeignColumns);
        return kind == FkCheckKind.Update
            ? new InvalidOperationException(
                $"UPDATE of '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                $"no matching row in '{rel.PrimaryTable}' for the new {columns} value(s).")
            : new InvalidOperationException(
                $"INSERT into '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                $"no matching row in '{rel.PrimaryTable}' for the supplied {columns} value(s).");
    }

    /// <summary>
    /// Checks that the non-null foreign key <paramref name="key"/> of
    /// <paramref name="values"/> names an existing row of the relationship's
    /// primary table: by a seek on the parent's index when one covers the
    /// key, otherwise against the parent's key set.
    /// </summary>
    /// <param name="rel">The relationship.</param>
    /// <param name="foreignTable">The table being written.</param>
    /// <param name="values">The row being written, in table-column order.</param>
    /// <param name="key">The row's normalized foreign key.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="kind">Whether an insert or an update supplied the key.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">No parent row has the key.</exception>
    private async ValueTask RequireParentKeyAsync(
        FkRelationship rel,
        string foreignTable,
        object[] values,
        string key,
        FkContext ctx,
        FkCheckKind kind,
        CancellationToken cancellationToken)
    {
        ParentSeekIndex? seekIndex = await this.seekPlanner.ResolveParentSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
        if (seekIndex != null)
        {
            if (!ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? pendingSet))
            {
                pendingSet = new HashSet<string>(StringComparer.Ordinal);
                ctx.ParentKeySets[rel.Name] = pendingSet;
            }

            if (pendingSet.Contains(key))
            {
                return;
            }

            byte[]? encodedKey = IndexHelpers.TryEncodeSeekKey(seekIndex, values);
            if (encodedKey != null)
            {
                var cursor = new IndexCursor(
                    (page, token) => RelationshipPageReader.ReadOwnedAsync(db, page, token),
                    db.PageSizeBytes);
                bool found = await cursor.ContainsKeyAsync(
                    seekIndex.RootPage,
                    encodedKey,
                    cancellationToken).ConfigureAwait(false);

                if (!found)
                {
                    throw ForeignKeyViolation(rel, foreignTable, kind);
                }

                return;
            }
        }

        HashSet<string> parentKeys = await this.GetParentKeySetAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
        if (!parentKeys.Contains(key))
        {
            throw ForeignKeyViolation(rel, foreignTable, kind);
        }
    }

    private async ValueTask<List<object?[]>?> TryReadAllRowsTypedAsync(
        TableDef def,
        List<RowLocation> locations,
        CancellationToken cancellationToken)
    {
        int[] allColumnOrdinals = new int[def.Columns.Count];
        for (int index = 0; index < allColumnOrdinals.Length; index++)
        {
            allColumnOrdinals[index] = index;
        }

        var rows = new List<object?[]>(locations.Count);
        foreach (RowLocation location in locations)
        {
            object?[]? values = await db.TryReadColumnValuesTypedAsync(location, def, allColumnOrdinals, cancellationToken).ConfigureAwait(false);
            if (values == null)
            {
                return null;
            }

            for (int index = 0; index < values.Length; index++)
            {
                if (values[index] == null)
                {
                    values[index] = DBNull.Value;
                }
            }

            rows.Add(values);
        }

        return rows;
    }

    private async ValueTask<bool> TryProcessCascadeDeleteWithSeekAsync(
        FkRelationship rel,
        CatalogEntry childEntry,
        TableDef childDef,
        ChildSeekIndex childSeek,
        List<object?[]> parentPkRows,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        var requests = new List<(object?[] OldPk, byte Payload)>(parentPkRows.Count);
        foreach (object?[] primaryKey in parentPkRows)
        {
            requests.Add((primaryKey, 0));
        }

        List<(RowLocation Loc, byte Payload)>? hits = await this.childRowLocator.TrySeekChildLocationsAsync(
            childEntry,
            childSeek,
            requests,
            cancellationToken).ConfigureAwait(false);
        if (hits == null)
        {
            return false;
        }

        if (hits.Count == 0)
        {
            return true;
        }

        if (!rel.CascadeDeletes)
        {
            throw new InvalidOperationException(
                $"DELETE on '{rel.PrimaryTable}' violates foreign-key constraint '{rel.Name}': " +
                $"{hits.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
        }

        var fullLocations = new List<RowLocation>(hits.Count);
        foreach ((RowLocation location, _) in hits)
        {
            fullLocations.Add(location);
        }

        List<object?[]>? childDeletedRows = await this.TryReadAllRowsTypedAsync(childDef, fullLocations, cancellationToken).ConfigureAwait(false);
        if (childDeletedRows == null)
        {
            return false;
        }

        await this.EnforceFkOnPrimaryDeleteAsync(
            rel.ForeignTable,
            childDef,
            childDeletedRows,
            ctx,
            depth + 1,
            cancellationToken).ConfigureAwait(false);

        await complexColumns.CascadeDeleteComplexChildrenAsync(childDef, fullLocations, cancellationToken).ConfigureAwait(false);

        int deleted = 0;
        foreach (RowLocation location in fullLocations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await tableRows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted > 0)
        {
            await tableRows.AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
            await indexes.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    private async ValueTask<bool> TryProcessCascadeUpdateWithSeekAsync(
        FkRelationship rel,
        CatalogEntry childEntry,
        TableDef childDef,
        ChildSeekIndex childSeek,
        Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)> movingChanges,
        int[] fkIdx,
        CancellationToken cancellationToken)
    {
        var requests = new List<(object?[] OldPk, object[] Payload)>(movingChanges.Count);
        foreach (KeyValuePair<string, (object?[] OldPkSubset, object[] NewPkSubset)> change in movingChanges)
        {
            requests.Add((change.Value.OldPkSubset, change.Value.NewPkSubset));
        }

        List<(RowLocation Loc, object[] NewPkSubset)>? rowMeta = await this.childRowLocator.TrySeekChildLocationsAsync(
            childEntry,
            childSeek,
            requests,
            cancellationToken).ConfigureAwait(false);
        if (rowMeta == null)
        {
            return false;
        }

        if (rowMeta.Count == 0)
        {
            return true;
        }

        if (!rel.CascadeUpdates)
        {
            throw new InvalidOperationException(
                $"UPDATE on '{rel.PrimaryTable}' violates foreign-key constraint '{rel.Name}': " +
                $"{rowMeta.Count} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
        }

        var locations = new List<RowLocation>(rowMeta.Count);
        foreach ((RowLocation location, _) in rowMeta)
        {
            locations.Add(location);
        }

        List<object?[]>? rows = await this.TryReadAllRowsTypedAsync(childDef, locations, cancellationToken).ConfigureAwait(false);
        if (rows == null)
        {
            return false;
        }

        for (int rowIndex = 0; rowIndex < rowMeta.Count; rowIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (RowLocation location, object[] newPkSubset) = rowMeta[rowIndex];
            object?[] values = rows[rowIndex];

            object[] rowValues = new object[values.Length];
            for (int column = 0; column < values.Length; column++)
            {
                rowValues[column] = values[column] ?? DBNull.Value;
            }

            for (int column = 0; column < fkIdx.Length; column++)
            {
                rowValues[fkIdx[column]] = newPkSubset[column] ?? DBNull.Value;
            }

            await tableRows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
            await tableRows.InsertRowDataAsync(childEntry.TDefPage, childDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        await indexes.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        return true;
    }
}
