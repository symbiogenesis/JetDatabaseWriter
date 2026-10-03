namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
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

    /// <summary>
    /// Records the key of a row just inserted into <paramref name="tableName"/>
    /// in <see cref="FkContext.InsertedParentKeys"/> for every relationship that
    /// relates the table to itself, so a later row of the same batch can
    /// reference it. The table's indexes take the batch's rows only after the
    /// last one is written, so an index seek cannot see them yet.
    /// </summary>
    /// <param name="tableName">The table the row was inserted into.</param>
    /// <param name="tableDef">The table's definition.</param>
    /// <param name="insertedValues">The inserted row, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    public static void AugmentParentSetsAfterInsert(string tableName, TableDef tableDef, object[] insertedValues, FkContext ctx)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, tableName, StringComparison.OrdinalIgnoreCase)
                || !string.Equals(rel.ForeignTable, tableName, StringComparison.OrdinalIgnoreCase)
                || !TryMapColumns(rel.PrimaryColumns, tableDef, out int[] primaryColumnIndexes))
            {
                continue;
            }

            string? key = RelationshipKeyBuilder.Build(insertedValues, primaryColumnIndexes);
            if (key == null)
            {
                continue;
            }

            if (!ctx.InsertedParentKeys.TryGetValue(rel.Name, out HashSet<string>? inserted))
            {
                inserted = new HashSet<string>(StringComparer.Ordinal);
                ctx.InsertedParentKeys[rel.Name] = inserted;
            }

            _ = inserted.Add(key);
        }
    }

    public ValueTask<IReadOnlyList<FkRelationship>> GetEnforcedRelationshipsAsync(CancellationToken cancellationToken)
        => catalog.GetEnforcedRelationshipsAsync(cancellationToken);

    /// <summary>
    /// Checks that every non-null foreign key of a row about to be inserted
    /// into <paramref name="foreignTable"/> names an existing parent row.
    /// </summary>
    /// <param name="foreignTable">The table the row is inserted into.</param>
    /// <param name="foreignDef">The table's definition.</param>
    /// <param name="values">The row, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// A foreign key has no matching parent row, or a relationship that
    /// constrains the row names a table or column that cannot be found.
    /// </exception>
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

            int[] foreignColumnIndexes = RequireColumns(rel, foreignTable, rel.ForeignColumns, foreignDef);
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
    /// <exception cref="InvalidOperationException">
    /// A changed foreign key has no matching parent row, or its relationship
    /// names a primary table or column that cannot be found.
    /// </exception>
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
            // An update cannot assign a foreign-key column the table does not
            // have, so it never changes the key of such a relationship.
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

    /// <summary>
    /// Cascades or refuses the delete of <paramref name="deletedParentRows"/>
    /// from <paramref name="primaryTable"/>, for every relationship whose
    /// primary table it is and, in turn, for every relationship of each table
    /// the delete cascades into: dependent rows are deleted when the
    /// relationship cascades deletes, and the delete is refused otherwise.
    /// Every dependent row is found and every relationship checked before
    /// any row is deleted, so a refused delete changes nothing.
    /// </summary>
    /// <param name="primaryTable">The table rows are deleted from.</param>
    /// <param name="primaryDef">The table's definition.</param>
    /// <param name="deletedParentRows">The deleted rows, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// A deleted key has dependent rows and the relationship does not cascade
    /// deletes, a relationship with a non-null deleted key names a table or
    /// column that cannot be found, or the cascades nest deeper than
    /// <see cref="RelationshipCascadePolicy.MaxDepth"/>.
    /// </exception>
    public async ValueTask EnforceFkOnPrimaryDeleteAsync(
        string primaryTable,
        TableDef primaryDef,
        List<object?[]> deletedParentRows,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        var cascades = new List<CascadeDelete>();
        HashSet<(long PageNumber, int RowIndex)> cascaded = [];
        await this.PlanCascadeDeletesAsync(primaryTable, primaryDef, deletedParentRows, ctx, depth: 0, cascades, cascaded, cancellationToken).ConfigureAwait(false);

        foreach ((string tableName, ResolvedTable table, List<RowLocation> locations) in cascades)
        {
            await complexColumns.CascadeDeleteComplexChildrenAsync(table.Definition, locations, cancellationToken).ConfigureAwait(false);

            foreach (RowLocation location in locations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await tableRows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
            }

            await tableRows.AdjustTDefRowCountAsync(table.Entry.TDefPage, -locations.Count, cancellationToken).ConfigureAwait(false);
            await indexes.MaintainIndexesAsync(table.Entry.TDefPage, table.Definition, tableName, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Cascades or refuses the primary-key changes an update of
    /// <paramref name="primaryTable"/> makes. Each relationship is checked
    /// against its own primary columns: one none of whose primary columns the
    /// update assigns is skipped, and only the rows whose key in those columns
    /// changes from a non-null value to another non-null value move their
    /// dependent rows. Every relationship whose key changes is resolved
    /// before any dependent row is rewritten, so one that names a missing
    /// table or column refuses the update before another one cascades.
    /// </summary>
    /// <param name="primaryTable">The table being updated.</param>
    /// <param name="primaryDef">The table's definition.</param>
    /// <param name="assignedColumns">The ordinals of the columns the update assigns.</param>
    /// <param name="rows">Each matching row before and after the update, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// A changed key has dependent rows and the relationship does not cascade
    /// updates, or a relationship whose key changes names a foreign table or
    /// column that cannot be found.
    /// </exception>
    public async ValueTask EnforceFkOnPrimaryUpdateAsync(
        string primaryTable,
        TableDef primaryDef,
        ICollection<int> assignedColumns,
        IReadOnlyList<(object[] OldRow, object[] NewRow)> rows,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        var keyChanges = new List<ReferencedKeyChange>();
        foreach (FkRelationship rel in ctx.All)
        {
            // An update cannot assign a primary column the table does not
            // have, so it never changes the key of such a relationship.
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

            ResolvedTable childTable = await this.ResolveRelationshipTableAsync(rel, "foreign table", rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            int[] foreignColumnIndexes = RequireColumns(rel, rel.ForeignTable, rel.ForeignColumns, childTable.Definition);
            keyChanges.Add(new ReferencedKeyChange(rel, childTable, foreignColumnIndexes, movingChanges));
        }

        foreach (ReferencedKeyChange change in keyChanges)
        {
            FkRelationship rel = change.Relationship;
            CatalogEntry childEntry = change.ChildTable.Entry;
            TableDef childDef = change.ChildTable.Definition;
            int[] fkIdx = change.ForeignColumnIndexes;
            Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)> movingChanges = change.Changes;
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

    /// <summary>
    /// Maps a relationship's key columns to their ordinals in
    /// <paramref name="definition"/>, or throws when one is not in the table.
    /// </summary>
    /// <param name="rel">The relationship.</param>
    /// <param name="table">The table's name, for the message.</param>
    /// <param name="names">The key columns on that side of the relationship.</param>
    /// <param name="definition">The table definition.</param>
    /// <returns>The ordinals, in the order of <paramref name="names"/>.</returns>
    /// <exception cref="InvalidOperationException">A key column is not in the table.</exception>
    private static int[] RequireColumns(FkRelationship rel, string table, IReadOnlyList<string> names, TableDef definition)
    {
        int[] ordinals = new int[names.Count];
        for (int index = 0; index < names.Count; index++)
        {
            ordinals[index] = definition.FindColumnIndex(names[index]);
            if (ordinals[index] < 0)
            {
                throw RelationshipCannotBeEnforced(rel, $"table '{table}' has no column '{names[index]}'");
            }
        }

        return ordinals;
    }

    /// <summary>
    /// Builds the error for an enforced relationship whose table or key column
    /// cannot be found. The writer refuses the write rather than skip the
    /// check, because it cannot tell a missing object from one it failed to
    /// find.
    /// </summary>
    /// <param name="rel">The relationship.</param>
    /// <param name="problem">What is missing.</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException RelationshipCannotBeEnforced(FkRelationship rel, string problem)
        => new($"Foreign-key constraint '{rel.Name}' cannot be enforced: {problem}. DropRelationshipAsync removes the relationship.");

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
    /// primary table: a row this call has already inserted, otherwise by a
    /// seek on the parent's index when one covers the key and can encode it,
    /// otherwise against the parent's key set.
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
        if (ctx.InsertedParentKeys.TryGetValue(rel.Name, out HashSet<string>? inserted) && inserted.Contains(key))
        {
            return;
        }

        ParentSeekIndex? seekIndex = await this.seekPlanner.ResolveParentSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
        if (seekIndex != null)
        {
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

    /// <summary>
    /// Returns the normalized keys of every row of the relationship's primary
    /// table, read once per call and kept in <see cref="FkContext.ParentKeySets"/>.
    /// </summary>
    /// <param name="rel">The relationship.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The parent keys.</returns>
    /// <exception cref="InvalidOperationException">The primary table or a primary column cannot be found.</exception>
    private async ValueTask<HashSet<string>> GetParentKeySetAsync(FkRelationship rel, FkContext ctx, CancellationToken cancellationToken)
    {
        if (ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? cached))
        {
            return cached;
        }

        ResolvedTable parent = await this.ResolveRelationshipTableAsync(rel, "primary table", rel.PrimaryTable, cancellationToken).ConfigureAwait(false);
        int[] primaryColumnIndexes = RequireColumns(rel, rel.PrimaryTable, rel.PrimaryColumns, parent.Definition);

        var set = new HashSet<string>(StringComparer.Ordinal);
        foreach (LocatedRow row in await snapshots.ReadRowsAsync(parent.Entry.TDefPage, cancellationToken).ConfigureAwait(false))
        {
            string? key = RelationshipKeyBuilder.Build(row.Values, primaryColumnIndexes);
            if (key != null)
            {
                _ = set.Add(key);
            }
        }

        ctx.ParentKeySets[rel.Name] = set;
        return set;
    }

    /// <summary>
    /// Resolves one of a relationship's tables, or throws when it cannot be
    /// found. A user table resolves through the writer's table catalog, which
    /// keeps calculated columns' result types; any other name falls back to
    /// the system tables.
    /// </summary>
    /// <param name="rel">The relationship.</param>
    /// <param name="role">The table's role in the relationship, for the message.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The table's catalog entry and definition.</returns>
    /// <exception cref="InvalidOperationException">No table has that name.</exception>
    private async ValueTask<ResolvedTable> ResolveRelationshipTableAsync(FkRelationship rel, string role, string tableName, CancellationToken cancellationToken)
    {
        ResolvedTable? resolved = await tableCatalog.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) is not null
            ? await tableCatalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false)
            : await snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        return resolved ?? throw RelationshipCannotBeEnforced(rel, $"its {role} '{tableName}' was not found");
    }

    /// <summary>
    /// Finds, without writing anything, the rows a delete of
    /// <paramref name="deletedRows"/> from <paramref name="primaryTable"/>
    /// cascades to, and in turn the rows their deletes cascade to. Each
    /// relationship's dependent rows are appended to
    /// <paramref name="cascades"/> after the cascades they cause, which is the
    /// order they are deleted in. A row an earlier cascade of the call
    /// deletes, which <paramref name="cascaded"/> holds, is no longer a
    /// dependent row, so it neither refuses the delete nor is deleted twice.
    /// </summary>
    /// <param name="primaryTable">The table rows are deleted from.</param>
    /// <param name="primaryDef">The table's definition.</param>
    /// <param name="deletedRows">The deleted rows, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="depth">How many cascades lead to this delete.</param>
    /// <param name="cascades">The cascading deletes found so far, in delete order.</param>
    /// <param name="cascaded">The rows <paramref name="cascades"/> deletes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// A deleted key has dependent rows and the relationship does not cascade
    /// deletes, a relationship with a non-null deleted key names a table or
    /// column that cannot be found, or <paramref name="depth"/> exceeds
    /// <see cref="RelationshipCascadePolicy.MaxDepth"/>.
    /// </exception>
    private async ValueTask PlanCascadeDeletesAsync(
        string primaryTable,
        TableDef primaryDef,
        List<object?[]> deletedRows,
        FkContext ctx,
        int depth,
        List<CascadeDelete> cascades,
        HashSet<(long PageNumber, int RowIndex)> cascaded,
        CancellationToken cancellationToken)
    {
        RelationshipCascadePolicy.ThrowIfDepthExceeded(depth);

        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            int[] primaryPkIdx = RequireColumns(rel, primaryTable, rel.PrimaryColumns, primaryDef);
            List<object?[]> parentPkRows = RelationshipKeyBuilder.ProjectNonNullKeys(deletedRows, primaryPkIdx);
            if (parentPkRows.Count == 0)
            {
                continue;
            }

            ResolvedTable childTable = await this.ResolveRelationshipTableAsync(rel, "foreign table", rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            int[] fkIdx = RequireColumns(rel, rel.ForeignTable, rel.ForeignColumns, childTable.Definition);

            List<LocatedRow> dependents = await this.FindDependentRowsAsync(rel, childTable, fkIdx, parentPkRows, ctx, cancellationToken).ConfigureAwait(false);
            _ = dependents.RemoveAll(row => cascaded.Contains((row.Location.PageNumber, row.Location.RowIndex)));
            if (dependents.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeDeletes)
            {
                throw new InvalidOperationException(
                    $"DELETE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{dependents.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
            }

            var dependentValues = new List<object?[]>(dependents.Count);
            foreach (LocatedRow row in dependents)
            {
                dependentValues.Add(row.Values);
            }

            await this.PlanCascadeDeletesAsync(rel.ForeignTable, childTable.Definition, dependentValues, ctx, depth + 1, cascades, cascaded, cancellationToken).ConfigureAwait(false);

            // A dependent row that a cascade nested in this one already
            // deletes, such as the child of another dependent row through a
            // self-relationship, is not deleted or counted again.
            var locations = new List<RowLocation>(dependents.Count);
            foreach (LocatedRow row in dependents)
            {
                if (cascaded.Add((row.Location.PageNumber, row.Location.RowIndex)))
                {
                    locations.Add(row.Location);
                }
            }

            if (locations.Count > 0)
            {
                cascades.Add(new CascadeDelete(rel.ForeignTable, childTable, locations));
            }
        }
    }

    /// <summary>
    /// Finds the rows of the relationship's foreign table whose key is one of
    /// <paramref name="parentKeys"/>: by a seek on the table's foreign-key
    /// index when one covers the key and every row found can be read, and
    /// otherwise by reading the table.
    /// </summary>
    /// <param name="rel">The relationship.</param>
    /// <param name="childTable">The relationship's foreign table.</param>
    /// <param name="fkIdx">The ordinals of the foreign-key columns in <paramref name="childTable"/>.</param>
    /// <param name="parentKeys">The referenced keys, in the relationship's primary-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The dependent rows.</returns>
    private async ValueTask<List<LocatedRow>> FindDependentRowsAsync(
        FkRelationship rel,
        ResolvedTable childTable,
        int[] fkIdx,
        List<object?[]> parentKeys,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        ChildSeekIndex? childSeek = await this.seekPlanner.ResolveChildSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
        if (childSeek != null)
        {
            var requests = new List<(object?[] OldPk, byte Payload)>(parentKeys.Count);
            foreach (object?[] parentKey in parentKeys)
            {
                requests.Add((parentKey, 0));
            }

            List<(RowLocation Loc, byte Payload)>? hits = await this.childRowLocator.TrySeekChildLocationsAsync(
                childTable.Entry,
                childSeek,
                requests,
                cancellationToken).ConfigureAwait(false);
            if (hits != null)
            {
                var locations = new List<RowLocation>(hits.Count);
                foreach ((RowLocation location, _) in hits)
                {
                    locations.Add(location);
                }

                List<LocatedRow>? found = await this.TryReadAllRowsTypedAsync(childTable.Definition, locations, cancellationToken).ConfigureAwait(false);
                if (found != null)
                {
                    return found;
                }
            }
        }

        HashSet<string> keySet = RelationshipKeyBuilder.BuildSetFromProjectedKeys(parentKeys);
        var dependents = new List<LocatedRow>();
        foreach (LocatedRow childRow in await snapshots.ReadRowsAsync(childTable.Entry.TDefPage, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? childKey = RelationshipKeyBuilder.Build(childRow.Values, fkIdx);
            if (childKey != null && keySet.Contains(childKey))
            {
                dependents.Add(childRow);
            }
        }

        return dependents;
    }

    /// <summary>
    /// Reads every column of the rows at <paramref name="locations"/>, or
    /// returns <see langword="null"/> when a row holds a value the single-row
    /// reader cannot decode (a MEMO, OLE or complex column), so the caller
    /// reads the table instead.
    /// </summary>
    /// <param name="def">The table's definition.</param>
    /// <param name="locations">The rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The rows, with <see cref="DBNull.Value"/> for nulls, or <see langword="null"/>.</returns>
    private async ValueTask<List<LocatedRow>?> TryReadAllRowsTypedAsync(
        TableDef def,
        List<RowLocation> locations,
        CancellationToken cancellationToken)
    {
        int[] allColumnOrdinals = new int[def.Columns.Count];
        for (int index = 0; index < allColumnOrdinals.Length; index++)
        {
            allColumnOrdinals[index] = index;
        }

        var rows = new List<LocatedRow>(locations.Count);
        foreach (RowLocation location in locations)
        {
            object?[]? values = await db.TryReadColumnValuesTypedAsync(location, def, allColumnOrdinals, cancellationToken).ConfigureAwait(false);
            if (values == null)
            {
                return null;
            }

            object[] row = new object[values.Length];
            for (int index = 0; index < values.Length; index++)
            {
                row[index] = values[index] ?? DBNull.Value;
            }

            rows.Add(new LocatedRow(location, row));
        }

        return rows;
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

        List<LocatedRow>? rows = await this.TryReadAllRowsTypedAsync(childDef, locations, cancellationToken).ConfigureAwait(false);
        if (rows == null)
        {
            return false;
        }

        for (int rowIndex = 0; rowIndex < rowMeta.Count; rowIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (RowLocation location, object[] newPkSubset) = rowMeta[rowIndex];
            object[] rowValues = rows[rowIndex].Values;
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
