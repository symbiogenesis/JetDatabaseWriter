namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Models;

internal sealed class RelationshipEnforcer(AccessWriter writer, IndexMaintainer indexes, RelationshipCatalogStore catalog)
{
    private readonly RelationshipSeekPlanner seekPlanner = new(writer);
    private readonly RelationshipChildRowLocator childRowLocator = new(writer);

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
            parent = await writer.ReadTableSnapshotAsync(rel.PrimaryTable, cancellationToken).ConfigureAwait(false);
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

    public async ValueTask EnforceFkOnInsertAsync(
        string foreignTable,
        TableDef foreignDef,
        object[] values,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.ForeignTable, foreignTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            int[] foreignColumnIndexes = new int[rel.ForeignColumns.Count];
            bool ok = true;
            for (int index = 0; index < rel.ForeignColumns.Count; index++)
            {
                foreignColumnIndexes[index] = foreignDef.FindColumnIndex(rel.ForeignColumns[index]);
                if (foreignColumnIndexes[index] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            string? key = RelationshipKeyBuilder.Build(values, foreignColumnIndexes);
            if (key == null)
            {
                continue;
            }

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
                    continue;
                }

                byte[]? encodedKey = IndexHelpers.TryEncodeSeekKey(seekIndex, values);
                if (encodedKey != null)
                {
                    var cursor = new IndexCursor(
                        (page, token) => RelationshipPageReader.ReadOwnedAsync(writer, page, token),
                        writer.PageSizeBytes);
                    bool found = await cursor.ContainsKeyAsync(
                        seekIndex.RootPage,
                        encodedKey,
                        cancellationToken).ConfigureAwait(false);

                    if (found)
                    {
                        continue;
                    }

                    throw new InvalidOperationException(
                        $"INSERT into '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                        $"no matching row in '{rel.PrimaryTable}' for the supplied {string.Join(", ", rel.ForeignColumns)} value(s).");
                }
            }

            HashSet<string> parentKeys = await this.GetParentKeySetAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (!parentKeys.Contains(key))
            {
                throw new InvalidOperationException(
                    $"INSERT into '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"no matching row in '{rel.PrimaryTable}' for the supplied {string.Join(", ", rel.ForeignColumns)} value(s).");
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

            ResolvedTable childTable = await writer.ResolveRequiredTableAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
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

            using DataTable childSnap = await writer.ReadTableSnapshotAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            List<RowLocation> locations = await writer.GetLiveRowLocationsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            int total = Math.Min(childSnap.Rows.Count, locations.Count);
            HashSet<string> deletedSet = RelationshipKeyBuilder.BuildSetFromProjectedKeys(parentPkRows);

            var matchingRowIndices = new List<int>();
            for (int index = 0; index < total; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = RelationshipKeyBuilder.Build(childSnap.Rows[index].ItemArray, fkIdx);
                if (childKey != null && deletedSet.Contains(childKey))
                {
                    matchingRowIndices.Add(index);
                }
            }

            if (matchingRowIndices.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeDeletes)
            {
                throw new InvalidOperationException(
                    $"DELETE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{matchingRowIndices.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
            }

            var childDeletedRows = new List<object?[]>(matchingRowIndices.Count);
            foreach (int rowIndex in matchingRowIndices)
            {
                childDeletedRows.Add(childSnap.Rows[rowIndex].ItemArray);
            }

            await this.EnforceFkOnPrimaryDeleteAsync(
                rel.ForeignTable,
                childDef,
                childDeletedRows,
                ctx,
                depth + 1,
                cancellationToken).ConfigureAwait(false);

            var cascadeLocations = new List<RowLocation>(matchingRowIndices.Count);
            foreach (int rowIndex in matchingRowIndices)
            {
                cascadeLocations.Add(locations[rowIndex]);
            }

            await writer.ComplexColumns.CascadeDeleteComplexChildrenAsync(childDef, cascadeLocations, cancellationToken).ConfigureAwait(false);

            int deleted = 0;
            foreach (int rowIndex in matchingRowIndices)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.MarkRowDeletedAsync(locations[rowIndex].PageNumber, locations[rowIndex].RowIndex, cancellationToken).ConfigureAwait(false);
                deleted++;
            }

            if (deleted > 0)
            {
                await writer.AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
                await indexes.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async ValueTask EnforceFkOnPrimaryUpdateAsync(
        string primaryTable,
        TableDef primaryDef,
        IReadOnlyList<(string? OldKey, object?[] OldFullRow, object[] NewPkValues)> changes,
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

            ResolvedTable childTable = await writer.ResolveRequiredTableAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            CatalogEntry childEntry = childTable.Entry;
            TableDef childDef = childTable.Definition;
            if (!TryMapFkPairOrdinals(rel, primaryDef, childDef, out int[] primaryPkIdx, out int[] fkIdx))
            {
                continue;
            }

            var movingChanges = new Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)>(StringComparer.Ordinal);
            foreach ((string? oldKey, object?[] oldFullRow, object[] newPkValues) in changes)
            {
                if (oldKey == null)
                {
                    continue;
                }

                string? newKey = RelationshipKeyBuilder.Build(newPkValues, primaryPkIdx);
                if (newKey == null || string.Equals(newKey, oldKey, StringComparison.Ordinal))
                {
                    continue;
                }

                object[] newPkSubset = new object[rel.PrimaryColumns.Count];
                object?[] oldPkSubset = new object?[rel.PrimaryColumns.Count];
                for (int index = 0; index < rel.PrimaryColumns.Count; index++)
                {
                    newPkSubset[index] = newPkValues[primaryPkIdx[index]];
                    oldPkSubset[index] = oldFullRow[primaryPkIdx[index]];
                }

                movingChanges[oldKey] = (oldPkSubset, newPkSubset);
            }

            if (movingChanges.Count == 0)
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

            using DataTable childSnap = await writer.ReadTableSnapshotAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            List<RowLocation> locations = await writer.GetLiveRowLocationsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            int total = Math.Min(childSnap.Rows.Count, locations.Count);
            var affectedIndices = new List<int>();
            var affectedOldKeys = new List<string>();
            for (int index = 0; index < total; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = RelationshipKeyBuilder.Build(childSnap.Rows[index].ItemArray, fkIdx);
                if (childKey != null && movingChanges.ContainsKey(childKey))
                {
                    affectedIndices.Add(index);
                    affectedOldKeys.Add(childKey);
                }
            }

            if (affectedIndices.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeUpdates)
            {
                throw new InvalidOperationException(
                    $"UPDATE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{affectedIndices.Count} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
            }

            for (int affectedIndex = 0; affectedIndex < affectedIndices.Count; affectedIndex++)
            {
                int rowIndex = affectedIndices[affectedIndex];
                object[] newPkSubset = movingChanges[affectedOldKeys[affectedIndex]].NewPkSubset;
                object[] rowValues = AccessWriter.GetDbNullNormalizedItemArray(childSnap.Rows[rowIndex]);

                for (int column = 0; column < rel.ForeignColumns.Count; column++)
                {
                    rowValues[fkIdx[column]] = newPkSubset[column] ?? DBNull.Value;
                }

                await writer.MarkRowDeletedAsync(locations[rowIndex].PageNumber, locations[rowIndex].RowIndex, cancellationToken).ConfigureAwait(false);
                await writer.InsertRowDataAsync(childEntry.TDefPage, childDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
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
            object?[]? values = await writer.TryReadColumnValuesTypedAsync(location, def, allColumnOrdinals, cancellationToken).ConfigureAwait(false);
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

        await writer.ComplexColumns.CascadeDeleteComplexChildrenAsync(childDef, fullLocations, cancellationToken).ConfigureAwait(false);

        int deleted = 0;
        foreach (RowLocation location in fullLocations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await writer.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted > 0)
        {
            await writer.AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
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

            await writer.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
            await writer.InsertRowDataAsync(childEntry.TDefPage, childDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        await indexes.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        return true;
    }
}
