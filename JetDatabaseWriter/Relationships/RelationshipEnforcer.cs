namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Exceptions;
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
/// <param name="tableRows">Encodes, deletes and rewrites cascaded child rows.</param>
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
    /// <param name="rows">Each matching row's location, and the row before and after the update, in table-column order.</param>
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
        IReadOnlyList<(RowLocation Location, object[] OldRow, object[] NewRow)> rows,
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

            foreach ((_, object[] oldRow, object[] newRow) in rows)
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
    /// Plans, without writing anything, the cascades of the primary-key
    /// changes an update of <paramref name="primaryTable"/> makes, and makes
    /// every refusal they call for. Each relationship is checked against its
    /// own primary columns: one none of whose primary columns the update
    /// assigns is skipped, and only the rows whose key in those columns
    /// changes from a non-null value to another non-null value move their
    /// dependent rows. Every relationship whose key changes is resolved first,
    /// so one that names a missing table or column refuses the update; then a
    /// relationship that does not cascade updates refuses when it has any
    /// dependent rows, which its child index seek counts without reading them
    /// where it can, and each cascading relationship's dependent rows are read
    /// and every rewritten child row is built and checked for unreadable MEMO
    /// and OLE values. A child row that several relationships reach is
    /// rewritten once, with every key change. Through a self-relationship, a
    /// row of
    /// <paramref name="rows"/> is a dependent row when its new foreign key
    /// still names a key the update moves; its cascaded key is written into
    /// its new row, which the caller rewrites, rather than planned as a
    /// second rewrite. Last, every rewritten child row, and every row of
    /// <paramref name="rows"/> that took a cascaded key, is encoded and
    /// measured as its re-insert will encode it
    /// (<see cref="TableRowStore.EncodeRow"/>). A refusal from any
    /// relationship therefore comes before any row is written.
    /// </summary>
    /// <param name="primaryTable">The table being updated.</param>
    /// <param name="primaryDef">The table's definition.</param>
    /// <param name="assignedColumns">The ordinals of the columns the update assigns.</param>
    /// <param name="rows">
    /// Each matching row's location, and the row before and after the update,
    /// in table-column order. A self-relationship's cascaded key is written
    /// into the new row.
    /// </param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>
    /// The child rows to rewrite, by table, in the order the relationships
    /// first reach the tables, for <see cref="ApplyCascadeUpdatesAsync"/>.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// A changed key has dependent rows and the relationship does not cascade
    /// updates, or a relationship whose key changes names a foreign table or
    /// column that cannot be found.
    /// </exception>
    /// <exception cref="System.IO.InvalidDataException">
    /// A dependent row holds a MEMO or OLE value whose stored data cannot be
    /// read, which rewriting the row would lose.
    /// </exception>
    /// <exception cref="JetLimitationException">
    /// A rewritten row is longer than one data page, or holds a value past
    /// what its column stores.
    /// </exception>
    /// <exception cref="OverflowException">A rewritten row holds a value that does not fit its column's type.</exception>
    public async ValueTask<List<CascadeUpdate>> PlanCascadeUpdatesAsync(
        string primaryTable,
        TableDef primaryDef,
        ICollection<int> assignedColumns,
        IReadOnlyList<(RowLocation Location, object[] OldRow, object[] NewRow)> rows,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        List<ReferencedKeyChange> keyChanges = await this.ResolveReferencedKeyChangesAsync(primaryTable, primaryDef, assignedColumns, rows, ctx, cancellationToken).ConfigureAwait(false);

        HashSet<(long PageNumber, int RowIndex)> ownRows = [];
        if (keyChanges.Exists(change => IsSelfRelationship(change.Relationship)))
        {
            foreach ((RowLocation location, _, _) in rows)
            {
                _ = ownRows.Add((location.PageNumber, location.RowIndex));
            }
        }

        var cascades = new List<CascadeUpdate>();
        var plannedTables = new Dictionary<long, (CascadeUpdate Cascade, Dictionary<(long PageNumber, int RowIndex), int> Positions)>();
        var ownRowChanges = new List<(object[] NewRow, int[] ForeignColumnIndexes, object[] NewPkSubset)>();
        foreach (ReferencedKeyChange change in keyChanges)
        {
            FkRelationship rel = change.Relationship;
            int[] fkIdx = change.ForeignColumnIndexes;

            // Through a self-relationship the update's own rows are dependent
            // rows by their new foreign key, which the update may assign, and
            // not by the stored rows the child table holds for them; their
            // cascaded key goes into the caller's rewrite: a second rewrite of
            // the stale row would leave two live rows.
            bool self = IsSelfRelationship(rel);
            var ownDependents = new List<(object[] NewRow, object[] NewPkSubset)>();
            if (self)
            {
                foreach ((_, _, object[] newRow) in rows)
                {
                    string? newKey = RelationshipKeyBuilder.Build(newRow, fkIdx);
                    if (newKey != null && change.Changes.TryGetValue(newKey, out (object?[] OldPkSubset, object[] NewPkSubset) moved))
                    {
                        ownDependents.Add((newRow, moved.NewPkSubset));
                    }
                }
            }

            if (!rel.CascadeUpdates)
            {
                // A refusal needs only the number of dependent rows, which the
                // child index seek gives without reading them.
                int dependentCount = ownDependents.Count
                    + await this.CountDependentRowsOfKeyChangeAsync(change, self ? ownRows : null, ctx, cancellationToken).ConfigureAwait(false);
                if (dependentCount > 0)
                {
                    throw new InvalidOperationException(
                        $"UPDATE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                        $"{dependentCount} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
                }

                continue;
            }

            List<(LocatedRow Row, object[] NewPkSubset)> dependents = await this.FindDependentRowsOfKeyChangeAsync(change, ctx, cancellationToken).ConfigureAwait(false);
            if (self)
            {
                _ = dependents.RemoveAll(dependent => ownRows.Contains((dependent.Row.Location.PageNumber, dependent.Row.Location.RowIndex)));
            }

            foreach ((object[] newRow, object[] newPkSubset) in ownDependents)
            {
                ownRowChanges.Add((newRow, fkIdx, newPkSubset));
            }

            if (dependents.Count == 0)
            {
                continue;
            }

            long childTdefPage = change.ChildTable.Entry.TDefPage;
            if (!plannedTables.TryGetValue(childTdefPage, out (CascadeUpdate Cascade, Dictionary<(long PageNumber, int RowIndex), int> Positions) planned))
            {
                planned = (new CascadeUpdate(rel.ForeignTable, change.ChildTable, []), []);
                plannedTables[childTdefPage] = planned;
                cascades.Add(planned.Cascade);
            }

            List<(RowLocation Location, object[] NewRow)> rewrites = planned.Cascade.Rows;
            foreach ((LocatedRow row, object[] newPkSubset) in dependents)
            {
                // A row an earlier relationship already rewrites takes this
                // relationship's key change in the same rewrite.
                (long PageNumber, int RowIndex) slot = (row.Location.PageNumber, row.Location.RowIndex);
                object[] newRow;
                if (planned.Positions.TryGetValue(slot, out int position))
                {
                    newRow = rewrites[position].NewRow;
                }
                else
                {
                    newRow = (object[])row.Values.Clone();
                    planned.Positions[slot] = rewrites.Count;
                    rewrites.Add((row.Location, newRow));
                }

                for (int column = 0; column < fkIdx.Length; column++)
                {
                    newRow[fkIdx[column]] = newPkSubset[column] ?? DBNull.Value;
                }

                UnreadableLongValue.ThrowIfAny(newRow, rel.ForeignTable);
            }
        }

        // Every relationship has passed, so the update's own rows take their
        // cascaded keys; each decision above read the rows as the caller
        // built them. The set holds each new row once, by reference, however
        // many self-relationships reach it.
        HashSet<object[]> foldedRows = [];
        foreach ((object[] newRow, int[] fkIdx, object[] newPkSubset) in ownRowChanges)
        {
            for (int column = 0; column < fkIdx.Length; column++)
            {
                newRow[fkIdx[column]] = newPkSubset[column] ?? DBNull.Value;
            }

            _ = foldedRows.Add(newRow);
        }

        // Every rewritten row is final now, so each is encoded and measured as
        // its re-insert will encode it: a value the encoder refuses, or a row
        // longer than a data page, refuses the update before any row is
        // deleted. The caller encoded its own rows before the plan; one that
        // took a cascaded key is encoded again.
        foreach ((_, ResolvedTable table, List<(RowLocation Location, object[] NewRow)> rewrites) in cascades)
        {
            foreach ((_, object[] newRow) in rewrites)
            {
                _ = tableRows.EncodeRow(table.Definition, newRow);
            }
        }

        foreach (object[] newRow in foldedRows)
        {
            _ = tableRows.EncodeRow(primaryDef, newRow);
        }

        return cascades;
    }

    /// <summary>
    /// Rewrites the dependent rows <see cref="PlanCascadeUpdatesAsync"/>
    /// planned, each with the values it built, and then maintains each child
    /// table's indexes once. Every write of a cascading key update happens
    /// here or in the caller's rewrite of its own rows, after every check the
    /// plan and the caller make. Only a child table's index rebuild checks
    /// that table's unique indexes and that the writer can maintain its
    /// indexes, so a failure of either throws after that table's rows are
    /// rewritten.
    /// </summary>
    /// <param name="cascades">The planned rewrites, by table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public async ValueTask ApplyCascadeUpdatesAsync(IReadOnlyList<CascadeUpdate> cascades, CancellationToken cancellationToken)
    {
        foreach ((string tableName, ResolvedTable table, List<(RowLocation Location, object[] NewRow)> rewrites) in cascades)
        {
            foreach ((RowLocation location, object[] newRow) in rewrites)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await tableRows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, cancellationToken).ConfigureAwait(false);
                await tableRows.InsertRowDataAsync(table.Entry.TDefPage, table.Definition, newRow, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            await indexes.MaintainIndexesAsync(table.Entry.TDefPage, table.Definition, tableName, cancellationToken).ConfigureAwait(false);
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

    /// <summary>Returns whether <paramref name="rel"/> relates a table to itself.</summary>
    /// <param name="rel">The relationship.</param>
    /// <returns><see langword="true"/> when the primary and foreign tables are the same table.</returns>
    private static bool IsSelfRelationship(FkRelationship rel)
        => string.Equals(rel.PrimaryTable, rel.ForeignTable, StringComparison.OrdinalIgnoreCase);

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

    /// <summary>
    /// Resolves every relationship whose referenced key an update of
    /// <paramref name="primaryTable"/> moves: one whose primary columns the
    /// update assigns and whose key in those columns changes, on some row,
    /// from a non-null value to another non-null value.
    /// </summary>
    /// <param name="primaryTable">The table being updated.</param>
    /// <param name="primaryDef">The table's definition.</param>
    /// <param name="assignedColumns">The ordinals of the columns the update assigns.</param>
    /// <param name="rows">Each matching row's location, and the row before and after the update, in table-column order.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The relationships and their key changes, in <see cref="FkContext.All"/> order.</returns>
    /// <exception cref="InvalidOperationException">A relationship whose key changes names a foreign table or column that cannot be found.</exception>
    private async ValueTask<List<ReferencedKeyChange>> ResolveReferencedKeyChangesAsync(
        string primaryTable,
        TableDef primaryDef,
        ICollection<int> assignedColumns,
        IReadOnlyList<(RowLocation Location, object[] OldRow, object[] NewRow)> rows,
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
            foreach ((_, object[] oldRow, object[] newRow) in rows)
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

        return keyChanges;
    }

    /// <summary>
    /// Finds, without writing anything, the rows of a relationship's foreign
    /// table that reference a key the update moves, each paired with the new
    /// key it takes: by a seek on the table's foreign-key index when one
    /// covers the key, every old key can be encoded and every row found can
    /// be read, and otherwise by reading the table.
    /// </summary>
    /// <param name="change">The relationship, its foreign table and its key changes.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The dependent rows, as stored, with the new key of each in the relationship's primary-column order.</returns>
    private async ValueTask<List<(LocatedRow Row, object[] NewPkSubset)>> FindDependentRowsOfKeyChangeAsync(
        ReferencedKeyChange change,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        List<(RowLocation Loc, object[] NewPkSubset)>? hits = await this.TrySeekDependentRowsOfKeyChangeAsync(change, ctx, cancellationToken).ConfigureAwait(false);
        if (hits != null)
        {
            var locations = new List<RowLocation>(hits.Count);
            foreach ((RowLocation location, _) in hits)
            {
                locations.Add(location);
            }

            List<LocatedRow>? found = await this.TryReadAllRowsTypedAsync(change.ChildTable.Definition, locations, cancellationToken).ConfigureAwait(false);
            if (found != null)
            {
                var seekDependents = new List<(LocatedRow Row, object[] NewPkSubset)>(found.Count);
                for (int index = 0; index < found.Count; index++)
                {
                    seekDependents.Add((found[index], hits[index].NewPkSubset));
                }

                return seekDependents;
            }
        }

        return await this.ScanDependentRowsOfKeyChangeAsync(change, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Counts, without writing anything, the rows of a relationship's foreign
    /// table that reference a key the update moves, leaving out the rows at
    /// <paramref name="excludedRows"/>: from the row locations a seek on the
    /// table's foreign-key index finds, without reading the rows, when one
    /// covers the key and every old key can be encoded, and otherwise by
    /// reading the table.
    /// </summary>
    /// <param name="change">The relationship, its foreign table and its key changes.</param>
    /// <param name="excludedRows">The rows not to count, or <see langword="null"/> to count every dependent row.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of dependent rows.</returns>
    private async ValueTask<int> CountDependentRowsOfKeyChangeAsync(
        ReferencedKeyChange change,
        HashSet<(long PageNumber, int RowIndex)>? excludedRows,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        int count = 0;
        List<(RowLocation Loc, object[] NewPkSubset)>? hits = await this.TrySeekDependentRowsOfKeyChangeAsync(change, ctx, cancellationToken).ConfigureAwait(false);
        if (hits != null)
        {
            foreach ((RowLocation location, _) in hits)
            {
                if (excludedRows?.Contains((location.PageNumber, location.RowIndex)) != true)
                {
                    count++;
                }
            }

            return count;
        }

        foreach ((LocatedRow row, _) in await this.ScanDependentRowsOfKeyChangeAsync(change, cancellationToken).ConfigureAwait(false))
        {
            if (excludedRows?.Contains((row.Location.PageNumber, row.Location.RowIndex)) != true)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Finds, without reading them, the rows of a relationship's foreign
    /// table that reference a key the update moves, each paired with the new
    /// key it takes, by a seek on the table's foreign-key index.
    /// </summary>
    /// <param name="change">The relationship, its foreign table and its key changes.</param>
    /// <param name="ctx">The call's relationship state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>
    /// The dependent rows' locations with the new key of each, in the
    /// relationship's primary-column order, or <see langword="null"/> when no
    /// index covers the key, an old key cannot be encoded, or a row the index
    /// names cannot be found on the table's data pages, so the caller reads
    /// the table instead.
    /// </returns>
    private async ValueTask<List<(RowLocation Loc, object[] NewPkSubset)>?> TrySeekDependentRowsOfKeyChangeAsync(
        ReferencedKeyChange change,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        ChildSeekIndex? childSeek = await this.seekPlanner.ResolveChildSeekIndexAsync(change.Relationship, ctx, cancellationToken).ConfigureAwait(false);
        if (childSeek == null)
        {
            return null;
        }

        var requests = new List<(object?[] OldPk, object[] Payload)>(change.Changes.Count);
        foreach ((object?[] oldPkSubset, object[] newPkSubset) in change.Changes.Values)
        {
            requests.Add((oldPkSubset, newPkSubset));
        }

        return await this.childRowLocator.TrySeekChildLocationsAsync(
            change.ChildTable.Entry,
            childSeek,
            requests,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a relationship's foreign table and returns the rows that
    /// reference a key the update moves, each paired with the new key it
    /// takes.
    /// </summary>
    /// <param name="change">The relationship, its foreign table and its key changes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The dependent rows, as stored, with the new key of each in the relationship's primary-column order.</returns>
    private async ValueTask<List<(LocatedRow Row, object[] NewPkSubset)>> ScanDependentRowsOfKeyChangeAsync(
        ReferencedKeyChange change,
        CancellationToken cancellationToken)
    {
        var dependents = new List<(LocatedRow Row, object[] NewPkSubset)>();
        foreach (LocatedRow childRow in await snapshots.ReadRowsAsync(change.ChildTable.Entry.TDefPage, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? childKey = RelationshipKeyBuilder.Build(childRow.Values, change.ForeignColumnIndexes);
            if (childKey != null && change.Changes.TryGetValue(childKey, out (object?[] OldPkSubset, object[] NewPkSubset) moved))
            {
                dependents.Add((childRow, moved.NewPkSubset));
            }
        }

        return dependents;
    }
}
