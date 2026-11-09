namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;

/// <summary>
/// Row DML workflows behind <see cref="Interfaces.IAccessWriter"/>: insert, update, and
/// delete. Each workflow stages its rows, applies client-side constraints,
/// checks that every index of the table can be maintained
/// (<see cref="IndexMaintainer.ThrowIfIndexesUnmaintainableAsync"/>), and runs
/// foreign-key and unique-index checks before any page is mutated, then
/// composes a shared mutation plan. The executor checks storage encoding
/// before applying rows and maintains each affected table once. Cancellation
/// before publication restores the statement through its transaction scope;
/// physical write-back completes under the transaction lifecycle's fence.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pageSource">The database's page source.</param>
/// <param name="catalog">Resolves the target table by name.</param>
/// <param name="indexes">Validates target index support before planning.</param>
/// <param name="uniqueIndexes">Runs pre-write unique-index checks.</param>
/// <param name="mutations">Applies composed row changes and table bookkeeping.</param>
/// <param name="constraints">Applies defaults, AutoNumber, required, validation, and calculated-column rules.</param>
/// <param name="enforcer">Enforces and cascades foreign-key constraints.</param>
/// <param name="complexColumns">Cascades deletes into complex-column child rows.</param>
/// <param name="history">Appends native Memo version snapshots.</param>
/// <param name="snapshots">Reads the decoded rows that update and delete predicates are evaluated against.</param>
internal sealed class TableDataWriter(
    JetFormat format,
    IPageSource pageSource,
    TableCatalog catalog,
    IndexMaintainer indexes,
    UniqueIndexChecker uniqueIndexes,
    MutationExecutor mutations,
    ConstraintRegistry constraints,
    RelationshipEnforcer enforcer,
    ComplexColumnManager complexColumns,
    AppendOnlyHistoryWriter history,
    TableSnapshotReader snapshots)
{
    private static object[] NormalizePublicRow(object?[] values, string paramName)
    {
        Guard.NotNull(values, paramName);

        object[] normalized = new object[values.Length];
        Array.Copy(values, normalized, values.Length);
        NormalizeRowInPlace(normalized);
        return normalized;
    }

    /// <summary>
    /// Projects a named-column <see cref="RowValues"/> onto a positional
    /// <c>object[]</c> in table-column order. Columns not named in the row are
    /// set to <see cref="DbDefault.Value"/>, so AutoNumber columns generate, column
    /// defaults apply, and any other omitted column stores database null. A
    /// named column whose value is <see langword="null"/> becomes
    /// <see cref="DBNull.Value"/> and stores database null even when it has a
    /// default. Unknown column names throw.
    /// </summary>
    /// <param name="tableDef">The target table definition.</param>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="row">The named-column values.</param>
    /// <param name="paramName">The public parameter name, for <see cref="ArgumentException"/>.</param>
    /// <returns>The positional row values.</returns>
    /// <exception cref="JetObjectNotFoundException">Thrown when <paramref name="row"/> names a column not in the table.</exception>
    private static object[] ResolveNamedRow(TableDef tableDef, string tableName, RowValues row, string paramName)
    {
        Guard.NotNull(row, paramName);

        object[] values = new object[tableDef.Columns.Count];
        Array.Fill(values, DbDefault.Value);

        foreach (KeyValuePair<string, object?> pair in row)
        {
            int columnIndex = tableDef.FindColumnIndex(pair.Key);
            if (columnIndex < 0)
            {
                throw new JetObjectNotFoundException(
                    JetErrorCode.ColumnNotFound,
                    $"Column '{pair.Key}' was not found in table '{tableName}'.",
                    paramName,
                    new JetErrorInfo { TableName = tableName, ColumnName = pair.Key });
            }

            values[columnIndex] = pair.Value ?? DBNull.Value;
        }

        return values;
    }

    private static void NormalizeRowInPlace(object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            values[i] ??= DBNull.Value;
        }
    }

    private static IEnumerable<TItem> SingleItem<TItem>(TItem item)
        where TItem : class
    {
        yield return item;
    }

    private static bool AssignsAutoNumberColumn(TableDef tableDef, IEnumerable<int> assignedColumns)
    {
        foreach (int columnIndex in assignedColumns)
        {
            ColumnInfo column = tableDef.Columns[columnIndex];
            if (column.IsAutoNumber)
            {
                return true;
            }
        }

        return false;
    }

    internal async ValueTask InsertRowAsync(string tableName, object?[] values, CancellationToken cancellationToken)
    {
        object[] normalized = NormalizePublicRow(values, nameof(values));
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        _ = await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            SingleItem(normalized),
            static (_, row) => row,
            nameof(values),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object?[]> rows, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(rows, nameof(rows));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            rows,
            static (_, row) => NormalizePublicRow(row, nameof(rows)),
            nameof(rows),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask InsertItemAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, T item, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(item, nameof(item));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        _ = await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            SingleItem(item),
            static (tableDef, row) => RowMapper<T>.ToRow(tableDef, row),
            nameof(item),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<int> InsertItemsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(items, nameof(items));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            items,
            static (tableDef, item) => RowMapper<T>.ToRow(tableDef, item),
            nameof(items),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask InsertNamedRowAsync(string tableName, RowValues row, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(row, nameof(row));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        _ = await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            SingleItem(row),
            (tableDef, named) => ResolveNamedRow(tableDef, tableName, named, nameof(row)),
            nameof(row),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<int> InsertNamedRowsAsync(string tableName, IEnumerable<RowValues> rows, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(rows, nameof(rows));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            rows,
            (tableDef, named) => ResolveNamedRow(tableDef, tableName, named, nameof(rows)),
            nameof(rows),
            cancellationToken).ConfigureAwait(false);
    }

    internal ValueTask<int> UpdateRowsAsync(string tableName, RowCriteria criteria, RowValues updatedValues, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        if (updatedValues.Count == 0)
        {
            return new ValueTask<int>(0);
        }

        return this.UpdateRowsCoreAsync(tableName, [new RowUpdate(criteria, updatedValues)], nameof(criteria), nameof(updatedValues), cancellationToken);
    }

    internal ValueTask<int> UpdateRowsAsync(string tableName, IEnumerable<RowUpdate> updates, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(updates, nameof(updates));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        var updateList = new List<RowUpdate>();
        foreach (RowUpdate? item in updates)
        {
            if (item is null)
            {
                throw new ArgumentException("RowUpdate collection cannot contain null elements.", nameof(updates));
            }

            if (item.UpdatedValues.Count > 0)
            {
                updateList.Add(item);
            }
        }

        if (updateList.Count == 0)
        {
            return new ValueTask<int>(0);
        }

        return this.UpdateRowsCoreAsync(tableName, updateList, nameof(updates), nameof(updates), cancellationToken);
    }

    private async ValueTask<int> UpdateRowsCoreAsync(
        string tableName,
        List<RowUpdate> updates,
        string criteriaParamName,
        string valuesParamName,
        CancellationToken cancellationToken)
    {
        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = table.Entry;
        TableDef tableDef = table.Definition;

        var compiledUpdates = new List<(RowCriteria Criteria, RowCriteriaEvaluator Predicate, Dictionary<int, object> Assignments)>(updates.Count);
        var unionAssignedColumns = new HashSet<int>();
        foreach (RowUpdate item in updates)
        {
            var predicate = RowCriteriaEvaluator.Compile(item.Criteria, tableDef, tableName, criteriaParamName);
            var updateIndexes = new Dictionary<int, object>(item.UpdatedValues.Count);
            foreach (KeyValuePair<string, object?> kvp in item.UpdatedValues)
            {
                int columnIndex = tableDef.FindColumnIndex(kvp.Key);
                if (columnIndex < 0)
                {
                    throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{kvp.Key}' was not found in table '{tableName}'.", valuesParamName, new JetErrorInfo { TableName = tableName, ColumnName = kvp.Key });
                }

                if (kvp.Value is DbDefault)
                {
                    throw new ArgumentException(
                        $"Column '{kvp.Key}': DbDefault.Value is accepted only by inserts; an update never applies a default.",
                        valuesParamName);
                }

                // A complex column's slot holds the row's per-row complex
                // reference, which joins it to its items in every complex column.
                // An update cannot assign it: clearing or changing it would orphan
                // the row's items or join the row to another row's.
                ColumnInfo column = tableDef.Columns[columnIndex];
                if (column.Type is ColumnType.ComplexType)
                {
                    throw new ArgumentException(
                        $"Column '{column.Name}' on table '{tableName}' is an Attachment or multi-value column, which an update cannot assign; the row keeps its items. Add items with AddAttachmentAsync or AddMultiValueItemAsync.",
                        valuesParamName);
                }

                updateIndexes[columnIndex] = kvp.Value ?? DBNull.Value;
                _ = unionAssignedColumns.Add(columnIndex);
            }

            compiledUpdates.Add((item.Criteria, predicate, updateIndexes));
        }

        IReadOnlyList<FkRelationship> rels = await enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        bool canSeek = await indexes.CanSeekUpdateTargetsAsync(entry.TDefPage, tableDef, tableName, unionAssignedColumns, cancellationToken).ConfigureAwait(false);
        foreach (FkRelationship relationship in rels)
        {
            if (string.Equals(relationship.PrimaryTable, tableName, StringComparison.OrdinalIgnoreCase))
            {
                foreach (string column in relationship.PrimaryColumns)
                {
                    int colIdx = tableDef.FindColumnIndex(column);
                    canSeek &= !unionAssignedColumns.Contains(colIdx) && (colIdx < 0 || !tableDef.Columns[colIdx].IsCalculated);
                }
            }

            if (string.Equals(relationship.ForeignTable, tableName, StringComparison.OrdinalIgnoreCase))
            {
                foreach (string column in relationship.ForeignColumns)
                {
                    int colIdx = tableDef.FindColumnIndex(column);
                    canSeek &= !unionAssignedColumns.Contains(colIdx) && (colIdx < 0 || !tableDef.Columns[colIdx].IsCalculated);
                }
            }
        }

        List<LocatedRow>? selected = null;
        if (compiledUpdates.Count == 1)
        {
            selected = await snapshots.TryReadIndexedRowsAsync(table, compiledUpdates[0].Criteria, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var merged = new List<LocatedRow>();
            var seenLocations = new HashSet<(long PageNumber, int RowIndex)>();
            bool allSought = true;
            foreach ((RowCriteria itemCriteria, _, _) in compiledUpdates)
            {
                List<LocatedRow>? itemRows = await snapshots.TryReadIndexedRowsAsync(table, itemCriteria, cancellationToken).ConfigureAwait(false);
                if (itemRows is null)
                {
                    allSought = false;
                    break;
                }

                foreach (LocatedRow located in itemRows)
                {
                    if (seenLocations.Add((located.Location.PageNumber, located.Location.RowIndex)))
                    {
                        merged.Add(located);
                    }
                }
            }

            if (allSought)
            {
                selected = merged;
            }
        }

        bool singlePredicateEvaluated = compiledUpdates.Count == 1 && selected is null;
        List<LocatedRow> rows;
        if (selected is not null)
        {
            rows = selected;
        }
        else if (compiledUpdates.Count == 1)
        {
            rows = await snapshots.ReadMatchingRowsAsync(table, compiledUpdates[0].Predicate, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var batchPredicates = new List<RowCriteriaEvaluator>(compiledUpdates.Count);
            foreach ((_, RowCriteriaEvaluator itemPredicate, _) in compiledUpdates)
            {
                batchPredicates.Add(itemPredicate);
            }

            rows = await snapshots.ReadMatchingRowsAsync(table, batchPredicates, cancellationToken).ConfigureAwait(false);
        }

        // Stage every matching row so FK / unique-index checks complete before
        // any disk page is mutated.
        var pendingUpdates = new List<(int Index, object[] OldRow, object[] NewRow)>();
        var pendingAssignedByIndex = new Dictionary<int, ICollection<int>>();
        for (int i = 0; i < rows.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object[] oldRow = rows[i].Values;
            Dictionary<int, object>? rowAssignments = null;
            if (compiledUpdates.Count == 1)
            {
                if (!singlePredicateEvaluated && !compiledUpdates[0].Predicate.Matches(oldRow))
                {
                    continue;
                }

                rowAssignments = compiledUpdates[0].Assignments;
            }
            else
            {
                foreach ((_, RowCriteriaEvaluator itemPredicate, Dictionary<int, object> itemAssignments) in compiledUpdates)
                {
                    if (!itemPredicate.Matches(oldRow))
                    {
                        continue;
                    }

                    rowAssignments ??= [];
                    foreach (KeyValuePair<int, object> kvp in itemAssignments)
                    {
                        rowAssignments[kvp.Key] = kvp.Value;
                    }
                }

                if (rowAssignments is null)
                {
                    continue;
                }
            }

            object[] newRow = (object[])oldRow.Clone();
            foreach (KeyValuePair<int, object> update in rowAssignments)
            {
                newRow[update.Key] = update.Value;
            }

            // Recalculation must not replace unreadable cached values that the
            // caller did not assign; their loss needs an explicit repair decision.
            UnreadableLongValue.ThrowIfAny(newRow, tableName);
            await constraints.ApplyUpdateAsync(tableName, tableDef, newRow, rowAssignments.Keys, cancellationToken).ConfigureAwait(false);

            // Unsupported in-place layouts retain the replacement fallback,
            // so every carried MEMO / OLE value must have been read;
            // refuse before any page is touched, the cascades below included.
            // The new version is encoded and measured once it is final, after
            // the cascade plan.
            UnreadableLongValue.ThrowIfAny(newRow, tableName);
            this.ThrowIfTextNotStorable(tableName, tableDef, newRow, rows[i].GetPreservedText(newRow, rowAssignments.Keys));

            pendingUpdates.Add((i, oldRow, newRow));
            pendingAssignedByIndex[i] = rowAssignments.Keys;
        }

        // An index the writer cannot maintain would fail the index rebuild
        // after the rows below had been rewritten; refuse the update first.
        if (pendingUpdates.Count == 0)
        {
            await indexes.ThrowIfIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await indexes.ThrowIfRowIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        }

        if (pendingUpdates.Count == 0)
        {
            return 0;
        }

        List<CascadeUpdate> cascades = [];
        if (rels.Count > 0)
        {
            var fkCtx = new FkContext(rels);

            // FK-side: as in Access, only a foreign key this update changes
            // must name a parent row; keys it leaves alone are not re-checked.
            var rowChanges = new List<(RowLocation Location, object[] OldRow, object[] NewRow)>(pendingUpdates.Count);
            foreach ((int i, object[] oldRow, object[] newRow) in pendingUpdates)
            {
                rowChanges.Add((rows[i].Location, oldRow, newRow));
            }

            // PK-side: plan the cascade, or make the refusal, of every
            // relationship whose referenced key this update changes. Nothing
            // is written yet. Through a self-relationship, a matching row's
            // own cascaded key goes into its new row in pendingUpdates.
            cascades = await enforcer.PlanCascadeUpdatesAsync(tableName, tableDef, unionAssignedColumns, rowChanges, fkCtx, cancellationToken).ConfigureAwait(false);

            // Changed foreign keys see every composed parent replacement, so
            // stable cycles can establish their parents within this statement.
            await enforcer.EnforceFkOnPlannedUpdateAsync(tableName, tableDef, rowChanges, cascades, fkCtx, cancellationToken).ConfigureAwait(false);
        }

        // Pre-write unique-index enforcement: after FK checks succeed,
        // validate that the post-update key set contains no duplicates for
        // any unique index. The check sees the table's rows with
        // pendingUpdates substituted at their original positions.
        // A self-cascade plan already checked this table with both its own
        // rewrites and the caller's rows substituted in one final row set.
        if (!cascades.Exists(cascade => cascade.Table.Entry.TDefPage == entry.TDefPage))
        {
            if (canSeek)
            {
                await uniqueIndexes.CheckUniqueIndexesPreUpdateAsync(entry.TDefPage, tableDef, tableName, rows, pendingUpdates, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var pendingUpdatesByLocation = new List<(RowLocation Location, object[] OldRow, object[] NewRow)>(pendingUpdates.Count);
                foreach ((int i, object[] oldRow, object[] newRow) in pendingUpdates)
                {
                    pendingUpdatesByLocation.Add((rows[i].Location, oldRow, newRow));
                }

                await uniqueIndexes.CheckUniqueIndexesPreUpdateByLocationAsync(entry.TDefPage, tableDef, tableName, pendingUpdatesByLocation, cancellationToken).ConfigureAwait(false);
            }
        }

        var historyBatches = new List<AppendOnlyHistoryWriter.HistoryBatch>();
        if (compiledUpdates.Count == 1)
        {
            var historyRows = new List<object[]>(pendingUpdates.Count);
            foreach ((_, _, object[] newRow) in pendingUpdates)
            {
                historyRows.Add(newRow);
            }

            historyBatches.AddRange(await history.PrepareAsync(tableName, tableDef, historyRows, compiledUpdates[0].Assignments.Keys, insertAssignments: null, cancellationToken).ConfigureAwait(false));
        }
        else
        {
            foreach ((int i, _, object[] newRow) in pendingUpdates)
            {
                historyBatches.AddRange(await history.PrepareAsync(tableName, tableDef, [newRow], pendingAssignedByIndex[i], insertAssignments: null, cancellationToken).ConfigureAwait(false));
            }
        }

        foreach (CascadeUpdate cascade in cascades)
        {
            foreach ((_, object[] oldRow, object[] newRow) in cascade.Rows)
            {
                var assigned = new List<int>();
                for (int index = 0; index < newRow.Length; index++)
                {
                    if (!Equals(oldRow[index], newRow[index]))
                    {
                        assigned.Add(index);
                    }
                }

                historyBatches.AddRange(await history.PrepareAsync(cascade.TableName, cascade.Table.Definition, [newRow], assigned, insertAssignments: null, cancellationToken).ConfigureAwait(false));
            }
        }

        await history.ValidateAsync(historyBatches, cancellationToken).ConfigureAwait(false);

        var plan = new MutationPlan();
        RelationshipEnforcer.AddCascadeUpdates(plan, cascades);
        MutationPlan.TableChanges changes = plan.ForTable(entry.TDefPage, tableDef, tableName);
        foreach ((int i, object[] oldRow, object[] newRow) in pendingUpdates)
        {
            ICollection<int> rowAssigned = pendingAssignedByIndex[i];
            changes.Update(rows[i].Location, oldRow, newRow, rows[i].GetPreservedText(newRow, rowAssigned), longValues: rows[i].GetPreservedLongValues(newRow, rowAssigned), assignedColumns: (IReadOnlyCollection<int>)rowAssigned);
        }

        bool raiseAutoNumbers = AssignsAutoNumberColumn(tableDef, unionAssignedColumns);
        if (!raiseAutoNumbers)
        {
            for (int columnIndex = 0; columnIndex < tableDef.Columns.Count; columnIndex++)
            {
                if (tableDef.Columns[columnIndex].IsAutoNumber)
                {
                    foreach ((_, object[] oldRow, object[] newRow) in pendingUpdates)
                    {
                        if (!Equals(oldRow[columnIndex], newRow[columnIndex]))
                        {
                            raiseAutoNumbers = true;
                            break;
                        }
                    }
                }
            }
        }

        changes.RaiseAutoNumbers = raiseAutoNumbers;
        AppendOnlyHistoryWriter.AddToPlan(plan, historyBatches);
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
        return pendingUpdates.Count;
    }

    internal ValueTask<int> DeleteRowsAsync(string tableName, RowCriteria criteria, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(criteria, nameof(criteria));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        return this.DeleteRowsCoreAsync(tableName, [criteria], nameof(criteria), cancellationToken);
    }

    internal ValueTask<int> DeleteRowsAsync(string tableName, IReadOnlyList<RowCriteria> criteriaList, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(criteriaList, nameof(criteriaList));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        if (criteriaList.Count == 0)
        {
            return new ValueTask<int>(0);
        }

        return this.DeleteRowsCoreAsync(tableName, criteriaList, nameof(criteriaList), cancellationToken);
    }

    private async ValueTask<int> DeleteRowsCoreAsync(
        string tableName,
        IReadOnlyList<RowCriteria> criteriaList,
        string criteriaParamName,
        CancellationToken cancellationToken)
    {
        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = table.Entry;
        TableDef tableDef = table.Definition;

        List<LocatedRow> matchingRows;
        if (criteriaList.Count == 1)
        {
            RowCriteria criteria = criteriaList[0];
            var predicate = RowCriteriaEvaluator.Compile(criteria, tableDef, tableName, criteriaParamName);
            List<LocatedRow>? selected = await snapshots.TryReadIndexedRowsAsync(table, criteria, cancellationToken).ConfigureAwait(false);
            matchingRows = selected is null
                ? await snapshots.ReadMatchingRowsAsync(table, predicate, cancellationToken).ConfigureAwait(false)
                : [];
            if (selected is not null)
            {
                foreach (LocatedRow row in selected)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (predicate.Matches(row.Values))
                    {
                        matchingRows.Add(row);
                    }
                }
            }
        }
        else
        {
            var predicates = new List<RowCriteriaEvaluator>(criteriaList.Count);
            foreach (RowCriteria itemCriteria in criteriaList)
            {
                if (itemCriteria is null)
                {
                    throw new ArgumentException("RowCriteria collection cannot contain null elements.", criteriaParamName);
                }

                predicates.Add(RowCriteriaEvaluator.Compile(itemCriteria, tableDef, tableName, criteriaParamName));
            }

            var merged = new List<LocatedRow>();
            var seenLocations = new HashSet<(long PageNumber, int RowIndex)>();
            bool allSought = true;
            for (int i = 0; i < criteriaList.Count; i++)
            {
                List<LocatedRow>? itemRows = await snapshots.TryReadIndexedRowsAsync(table, criteriaList[i], cancellationToken).ConfigureAwait(false);
                if (itemRows is null)
                {
                    allSought = false;
                    break;
                }

                RowCriteriaEvaluator itemPredicate = predicates[i];
                foreach (LocatedRow row in itemRows)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (itemPredicate.Matches(row.Values)
                        && seenLocations.Add((row.Location.PageNumber, row.Location.RowIndex)))
                    {
                        merged.Add(row);
                    }
                }
            }

            matchingRows = allSought
                ? merged
                : await snapshots.ReadMatchingRowsAsync(table, predicates, cancellationToken).ConfigureAwait(false);
        }

        // An index the writer cannot maintain would fail the index rebuild
        // after the rows, and any cascaded rows, had been deleted; refuse the
        // delete first.
        if (matchingRows.Count == 0)
        {
            await indexes.ThrowIfIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await indexes.ThrowIfRowIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        }

        IReadOnlyList<FkRelationship> rels = await enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        if (matchingRows.Count == 0)
        {
            return 0;
        }

        // The hidden flat-table rows of every Attachment / MultiValue column
        // the delete reaches: each cascade's go into a group of its own, and
        // the matching rows' into this one. They are found by the rows'
        // per-row complex-reference slots, so all of them are read before any
        // row is marked deleted.
        var complexChildren = new ComplexChildDeletes();
        List<CascadeDelete> cascades = [];
        if (rels.Count > 0)
        {
            var fkCtx = new FkContext(rels);

            // The typed full row of every parent we are about to delete, in
            // primary-table column order. PlanCascadeDeletesAsync consumes
            // this once per relationship (slicing the relationship's
            // PrimaryColumns out for the FK seek / snapshot scan).
            var deletedParentRows = new List<object?[]>(matchingRows.Count);
            foreach (LocatedRow row in matchingRows)
            {
                deletedParentRows.Add(row.Values);
            }

            cascades = await enforcer.PlanCascadeDeletesAsync(
                tableName,
                tableDef,
                deletedParentRows,
                matchingRows,
                fkCtx,
                complexChildren,
                cancellationToken).ConfigureAwait(false);
        }

        var parentLocs = new List<RowLocation>(matchingRows.Count);
        foreach (LocatedRow row in matchingRows)
        {
            parentLocs.Add(row.Location);
        }

        await complexColumns.PlanComplexChildDeletesAsync(tableDef, parentLocs, complexChildren, cancellationToken).ConfigureAwait(false);

        var plan = new MutationPlan();
        await enforcer.AddCascadeDeletesAsync(plan, cascades, cancellationToken).ConfigureAwait(false);
        await complexColumns.AddComplexChildDeletesAsync(plan, complexChildren, cancellationToken).ConfigureAwait(false);
        MutationPlan.TableChanges changes = plan.ForTable(entry.TDefPage, tableDef, tableName);
        foreach ((RowLocation location, object[] oldRow) in matchingRows)
        {
            changes.Delete(location, oldRow);
        }

        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
        return matchingRows.Count;
    }

    private async ValueTask<int> InsertMappedRowsAfterValidationAsync<TItem>(
        string tableName,
        IEnumerable<TItem> items,
        Func<TableDef, TItem, object[]> mapRow,
        string itemParamName,
        CancellationToken cancellationToken)
        where TItem : class
    {
        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = table.Entry;
        TableDef tableDef = table.Definition;

        // Before any AutoNumber value is taken and any row written: an index
        // the writer cannot maintain would fail the index maintenance after
        // the rows were written, and the incremental path would already have
        // added them to the indexes before it.
        await indexes.ThrowIfRowIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);

        IReadOnlyList<FkRelationship> relationships = await enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkContext = relationships.Count > 0 ? new FkContext(relationships) : null;

        (List<object[]> pendingRows, List<ICollection<int>> insertAssignments) =
            await this.PrepareInsertBatchAsync(
                tableName,
                tableDef,
                items,
                mapRow,
                itemParamName,
                cancellationToken).ConfigureAwait(false);

        return await this.InsertPreparedBatchAsync(
            tableName,
            entry.TDefPage,
            tableDef,
            pendingRows,
            insertAssignments,
            fkContext,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<(List<object[]> PendingRows, List<ICollection<int>> InsertAssignments)> PrepareInsertBatchAsync<TItem>(
        string tableName,
        TableDef tableDef,
        IEnumerable<TItem> items,
        Func<TableDef, TItem, object[]> mapRow,
        string itemParamName,
        CancellationToken cancellationToken)
        where TItem : class
    {
        var pendingRows = new List<object[]>();
        var insertAssignments = new List<ICollection<int>>();
        foreach (TItem item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guard.NotNull(item, itemParamName);

            object[] row = mapRow(tableDef, item);
            var assigned = new HashSet<int>();
            for (int index = 0; index < Math.Min(row.Length, tableDef.Columns.Count); index++)
            {
                if (tableDef.Columns[index].Type == ColumnType.MemoType && row[index] is not DbDefault)
                {
                    assigned.Add(index);
                }
            }

            _ = await constraints.ApplyAsync(tableName, tableDef, row, cancellationToken).ConfigureAwait(false);
            this.ThrowIfTextNotStorable(tableName, tableDef, row);
            pendingRows.Add(row);
            insertAssignments.Add(assigned);
        }

        return (pendingRows, insertAssignments);
    }

    private async ValueTask<int> InsertPreparedBatchAsync(
        string tableName,
        long tdefPage,
        TableDef tableDef,
        List<object[]> pendingRows,
        List<ICollection<int>> insertAssignments,
        FkContext? fkContext,
        CancellationToken cancellationToken)
    {
        await uniqueIndexes.CheckUniqueIndexesPreInsertAsync(tdefPage, tableDef, tableName, pendingRows, cancellationToken).ConfigureAwait(false);
        List<AppendOnlyHistoryWriter.HistoryBatch> historyBatches = await history.PrepareAsync(tableName, tableDef, pendingRows, assignedColumns: null, insertAssignments, cancellationToken).ConfigureAwait(false);
        await history.ValidateAsync(historyBatches, cancellationToken).ConfigureAwait(false);
        var plan = new MutationPlan();
        MutationPlan.TableChanges changes = plan.ForTable(tdefPage, tableDef, tableName);
        foreach (object[] row in pendingRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (fkContext != null)
            {
                await enforcer.EnforceFkOnInsertAsync(tableName, tableDef, row, fkContext, cancellationToken).ConfigureAwait(false);
                RelationshipEnforcer.AugmentPlannedParentKeys(tableName, tableDef, row, fkContext);
            }

            changes.Insert(row);
        }

        changes.RaiseAutoNumbers = true;
        changes.RaiseComplexReferences = true;
        AppendOnlyHistoryWriter.AddToPlan(plan, historyBatches);
        await mutations.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
        return pendingRows.Count;
    }

    /// <summary>
    /// Refuses, before any page is written, a row whose Text or Memo value
    /// holds a character the database cannot store. Jet3 stores text in its
    /// code page, where .NET would write a best-fit match or <c>?</c>, and an
    /// index entry built from the caller's text would then not match the row
    /// (<see cref="JetFormat.DescribeUnstorableCharacter"/>). Checking the
    /// whole row also keeps an update from deleting the old row and then
    /// failing to encode the new one.
    /// </summary>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="row">The finished row, after defaults, in table-column order.</param>
    /// <param name="textBytes">Unassigned Text slots copied without re-encoding.</param>
    /// <exception cref="JetLimitationException">A Text or Memo value has no faithful supported storage encoding.</exception>
    private void ThrowIfTextNotStorable(string tableName, TableDef tableDef, object[] row, IReadOnlyDictionary<int, byte[]>? textBytes = null)
    {
        for (int i = 0; i < tableDef.Columns.Count && i < row.Length; i++)
        {
            ColumnInfo column = tableDef.Columns[i];
            if (textBytes?.ContainsKey(i) == true || row[i] is null or DBNull
                || JetTypeInfo.ResolveValueType(column) is not (ColumnType.TextType or ColumnType.MemoType))
            {
                continue;
            }

            if (!format.IsJet3 && (column.IsFixed || JetTypeInfo.ResolveValueType(column) != ColumnType.TextType || (!column.IsCalculated && !column.IsCompressedUnicode)))
            {
                continue;
            }

            if (Convert.ToString(row[i], CultureInfo.InvariantCulture) is not { } text)
            {
                continue;
            }

            if (!format.IsJet3 && text.Length > 0 && text[0] == '\uFEFF')
            {
                if (column.IsCalculated)
                {
                    continue;
                }

                JetFormat.ValidateEscapedVariableText(text, column.Size, new JetErrorInfo { TableName = tableName, ColumnName = column.Name });
            }

            if (format.DescribeUnstorableCharacter(text) is { } character)
            {
                throw new JetLimitationException(JetErrorCode.FeatureNotSupported, format.UnstorableTextMessage($"The value for column '{column.Name}' of table '{tableName}'", character), new JetErrorInfo { TableName = tableName, ColumnName = column.Name, Reason = "Unrepresentable character in database code page" });
            }
        }
    }
}
