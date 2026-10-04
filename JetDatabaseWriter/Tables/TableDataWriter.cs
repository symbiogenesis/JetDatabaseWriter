namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
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
/// writes the rows and maintains indexes. The public facade owns the
/// auto-commit scope around each call.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="catalog">Resolves the target table by name.</param>
/// <param name="tableRows">Writes and tombstones the affected rows.</param>
/// <param name="indexes">Maintains index B-trees after each batch.</param>
/// <param name="uniqueIndexes">Runs pre-write unique-index checks.</param>
/// <param name="autoNumbers">Advances AutoNumber high-water values after inserts.</param>
/// <param name="constraints">Applies defaults, AutoNumber, required, validation, and calculated-column rules.</param>
/// <param name="enforcer">Enforces and cascades foreign-key constraints.</param>
/// <param name="complexColumns">Cascades deletes into complex-column child rows.</param>
/// <param name="snapshots">Reads the decoded rows that update and delete predicates are evaluated against.</param>
internal sealed class TableDataWriter(
    DatabaseFile db,
    TableCatalog catalog,
    TableRowStore tableRows,
    IndexMaintainer indexes,
    UniqueIndexChecker uniqueIndexes,
    AutoNumberMaintainer autoNumbers,
    ConstraintRegistry constraints,
    RelationshipEnforcer enforcer,
    ComplexColumnManager complexColumns,
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
    /// <exception cref="ArgumentException">Thrown when <paramref name="row"/> names a column not in the table.</exception>
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
                throw new ArgumentException(
                    $"Column '{pair.Key}' was not found in table '{tableName}'.",
                    paramName);
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
            // Complex columns carry the 0x07 marker in the flag byte, not real flag bits.
            ColumnInfo column = tableDef.Columns[columnIndex];
            if (column.Type is not ColumnType.AttachmentType and not ColumnType.ComplexType
                && (column.Flags & Constants.ColumnDescriptorFlags.AutoNumber) != 0)
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
        db.ThrowIfDisposedOrCancelled(cancellationToken);

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
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            rows,
            static (_, row) => NormalizePublicRow(row, nameof(rows)),
            nameof(rows),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask InsertItemAsync<T>(string tableName, T item, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(item, nameof(item));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        _ = await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            SingleItem(item),
            static (tableDef, row) => RowMapper<T>.ToRow(tableDef, row),
            nameof(item),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<int> InsertItemsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(items, nameof(items));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

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
        db.ThrowIfDisposedOrCancelled(cancellationToken);

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
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            rows,
            (tableDef, named) => ResolveNamedRow(tableDef, tableName, named, nameof(rows)),
            nameof(rows),
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<int> UpdateRowsAsync(string tableName, RowCriteria criteria, RowValues updatedValues, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        if (updatedValues.Count == 0)
        {
            return 0;
        }

        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = table.Entry;
        TableDef tableDef = table.Definition;
        var predicate = RowCriteriaEvaluator.Compile(criteria, tableDef, tableName, nameof(criteria));

        var updateIndexes = new Dictionary<int, object>(updatedValues.Count);
        foreach (KeyValuePair<string, object?> kvp in updatedValues)
        {
            int columnIndex = tableDef.FindColumnIndex(kvp.Key);
            if (columnIndex < 0)
            {
                throw new ArgumentException($"Column '{kvp.Key}' was not found in table '{tableName}'.", nameof(updatedValues));
            }

            if (kvp.Value is DbDefault)
            {
                throw new ArgumentException(
                    $"Column '{kvp.Key}': DbDefault.Value is accepted only by inserts; an update never applies a default.",
                    nameof(updatedValues));
            }

            // A complex column's slot holds the row's per-row complex
            // reference, which joins it to its items in every complex column.
            // An update cannot assign it: clearing or changing it would orphan
            // the row's items or join the row to another row's.
            ColumnInfo column = tableDef.Columns[columnIndex];
            if (column.Type is ColumnType.AttachmentType or ColumnType.ComplexType)
            {
                throw new ArgumentException(
                    $"Column '{column.Name}' on table '{tableName}' is an Attachment or multi-value column, which an update cannot assign; the row keeps its items. Add items with AddAttachmentAsync or AddMultiValueItemAsync.",
                    nameof(updatedValues));
            }

            updateIndexes[columnIndex] = kvp.Value ?? DBNull.Value;
        }

        List<LocatedRow> rows = await snapshots.ReadRowsAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

        // Stage every matching row so FK / unique-index checks complete before
        // any disk page is mutated.
        var pendingUpdates = new List<(int Index, object[] OldRow, object[] NewRow)>();
        for (int i = 0; i < rows.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object[] oldRow = rows[i].Values;
            if (!predicate.Matches(oldRow))
            {
                continue;
            }

            object[] newRow = (object[])oldRow.Clone();
            foreach (KeyValuePair<int, object> update in updateIndexes)
            {
                newRow[update.Key] = update.Value;
            }

            await constraints.ApplyUpdateAsync(tableName, tableDef, newRow, updateIndexes.Keys, cancellationToken).ConfigureAwait(false);

            // The row is deleted and re-inserted, so every carried MEMO / OLE
            // value must have been read, and every text value must encode;
            // refuse before any page is touched.
            UnreadableLongValue.ThrowIfAny(newRow, tableName);
            this.ThrowIfTextNotStorable(tableName, tableDef, newRow);

            pendingUpdates.Add((i, oldRow, newRow));
        }

        // An index the writer cannot maintain would fail the index rebuild
        // after the rows below had been rewritten; refuse the update first.
        await indexes.ThrowIfIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);

        if (pendingUpdates.Count == 0)
        {
            return 0;
        }

        IReadOnlyList<FkRelationship> rels = await enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        if (rels.Count > 0)
        {
            var fkCtx = new FkContext(rels);

            // FK-side: as in Access, only a foreign key this update changes
            // must name a parent row; keys it leaves alone are not re-checked.
            var rowChanges = new List<(object[] OldRow, object[] NewRow)>(pendingUpdates.Count);
            foreach ((_, object[] oldRow, object[] newRow) in pendingUpdates)
            {
                rowChanges.Add((oldRow, newRow));
            }

            await enforcer.EnforceFkOnForeignUpdateAsync(tableName, tableDef, updateIndexes.Keys, rowChanges, fkCtx, cancellationToken).ConfigureAwait(false);

            // PK-side: cascade or reject only the relationships whose
            // referenced key this update changes.
            await enforcer.EnforceFkOnPrimaryUpdateAsync(tableName, tableDef, updateIndexes.Keys, rowChanges, fkCtx, cancellationToken).ConfigureAwait(false);
        }

        // Pre-write unique-index enforcement: after FK checks succeed,
        // validate that the post-update key set contains no duplicates for
        // any unique index. The check sees the table's rows with
        // pendingUpdates substituted at their original positions.
        await uniqueIndexes.CheckUniqueIndexesPreUpdateAsync(entry.TDefPage, tableDef, tableName, rows, pendingUpdates, cancellationToken).ConfigureAwait(false);

        var updateInsertedHints = new List<(RowLocation Loc, object[] Row)>(pendingUpdates.Count);
        var updateDeletedHints = new List<(RowLocation Loc, object[] Row)>(pendingUpdates.Count);
        foreach ((int i, object[] oldRow, object[] newRow) in pendingUpdates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RowLocation oldLoc = rows[i].Location;
            await tableRows.MarkRowDeletedAsync(oldLoc.PageNumber, oldLoc.RowIndex, tableDef, cancellationToken).ConfigureAwait(false);
            updateDeletedHints.Add((oldLoc, oldRow));
            RowLocation newLoc = await tableRows.InsertRowDataLocAsync(entry.TDefPage, tableDef, newRow, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            updateInsertedHints.Add((newLoc, newRow));
        }

        bool incremental = await indexes.TryMaintainIndexesIncrementalAsync(
            entry.TDefPage,
            tableDef,
            updateInsertedHints,
            updateDeletedHints,
            cancellationToken).ConfigureAwait(false);
        if (!incremental)
        {
            await indexes.MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        }

        // An explicit AutoNumber value raises the TDEF high-water exactly as it
        // does on insert, so Access never issues that number again.
        if (AssignsAutoNumberColumn(tableDef, updateIndexes.Keys))
        {
            var newRows = new List<object[]>(pendingUpdates.Count);
            foreach ((_, _, object[] newRow) in pendingUpdates)
            {
                newRows.Add(newRow);
            }

            await autoNumbers.UpdateHighWaterAsync(entry.TDefPage, tableDef, newRows, cancellationToken).ConfigureAwait(false);
        }

        return pendingUpdates.Count;
    }

    internal async ValueTask<int> DeleteRowsAsync(string tableName, RowCriteria criteria, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(criteria, nameof(criteria));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = table.Entry;
        TableDef tableDef = table.Definition;
        var predicate = RowCriteriaEvaluator.Compile(criteria, tableDef, tableName, nameof(criteria));

        List<LocatedRow> rows = await snapshots.ReadRowsAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

        // FK enforcement: identify the rows we are about to delete; if any
        // FK relationship names this table as the primary side, capture the
        // deleted PK tuples and let EnforceFkOnPrimaryDeleteAsync
        // cascade-delete dependent child rows (or throw when cascade is
        // disabled). It finds and checks every dependent row, at every
        // cascade level, before it deletes any, so a refused delete leaves
        // the database unchanged.
        var matchingRows = new List<LocatedRow>();
        foreach (LocatedRow row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (predicate.Matches(row.Values))
            {
                matchingRows.Add(row);
            }
        }

        // An index the writer cannot maintain would fail the index rebuild
        // after the rows, and any cascaded rows, had been deleted; refuse the
        // delete first.
        await indexes.ThrowIfIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);

        IReadOnlyList<FkRelationship> rels = await enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        if (rels.Count > 0 && matchingRows.Count > 0)
        {
            var fkCtx = new FkContext(rels);

            // The typed full row of every parent we are about to delete, in
            // primary-table column order. EnforceFkOnPrimaryDeleteAsync
            // consumes this once per relationship (slicing the relationship's
            // PrimaryColumns out for the FK seek / snapshot scan).
            var deletedParentRows = new List<object?[]>(matchingRows.Count);
            foreach (LocatedRow row in matchingRows)
            {
                deletedParentRows.Add(row.Values);
            }

            await enforcer.EnforceFkOnPrimaryDeleteAsync(
                tableName,
                tableDef,
                deletedParentRows,
                fkCtx,
                cancellationToken).ConfigureAwait(false);
        }

        // Cascade flat-child rows for any complex columns on the parent
        // BEFORE we mark the parent rows deleted (we need to read the
        // parent's per-row complex-reference slots while the rows are still live).
        if (matchingRows.Count > 0)
        {
            var parentLocs = new List<RowLocation>(matchingRows.Count);
            foreach (LocatedRow row in matchingRows)
            {
                parentLocs.Add(row.Location);
            }

            await complexColumns.CascadeDeleteComplexChildrenAsync(tableDef, parentLocs, cancellationToken).ConfigureAwait(false);
        }

        int deleted = 0;
        var deleteHints = new List<(RowLocation Loc, object[] Row)>(matchingRows.Count);
        foreach ((RowLocation location, object[] oldRow) in matchingRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await tableRows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, tableDef, cancellationToken).ConfigureAwait(false);
            deleteHints.Add((location, oldRow));
            deleted++;
        }

        if (deleted > 0)
        {
            await tableRows.AdjustTDefRowCountAsync(entry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
            bool incremental = await indexes.TryMaintainIndexesIncrementalAsync(
                entry.TDefPage,
                tableDef,
                insertedRows: null,
                deleteHints,
                cancellationToken).ConfigureAwait(false);
            if (!incremental)
            {
                await indexes.MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
            }
        }

        return deleted;
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
        await indexes.ThrowIfIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);

        IReadOnlyList<FkRelationship> relationships = await enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkContext = relationships.Count > 0 ? new FkContext(relationships) : null;

        (List<object[]> pendingRows, List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints) =
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
            autoCheckpoints,
            fkContext,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<(List<object[]> PendingRows, List<(ColumnConstraint Constraint, long? PreviousValue)>? AutoCheckpoints)> PrepareInsertBatchAsync<TItem>(
        string tableName,
        TableDef tableDef,
        IEnumerable<TItem> items,
        Func<TableDef, TItem, object[]> mapRow,
        string itemParamName,
        CancellationToken cancellationToken)
        where TItem : class
    {
        var pendingRows = new List<object[]>();
        List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints = null;

        try
        {
            foreach (TItem item in items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Guard.NotNull(item, itemParamName);

                object[] row = mapRow(tableDef, item);
                List<(ColumnConstraint Constraint, long? PreviousValue)>? rowCheckpoints =
                    await constraints.ApplyAsync(tableName, tableDef, row, cancellationToken).ConfigureAwait(false);
                if (rowCheckpoints != null)
                {
                    (autoCheckpoints ??= []).AddRange(rowCheckpoints);
                }

                this.ThrowIfTextNotStorable(tableName, tableDef, row);
                pendingRows.Add(row);
            }
        }
        catch
        {
            ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
            throw;
        }

        return (pendingRows, autoCheckpoints);
    }

    private async ValueTask<int> InsertPreparedBatchAsync(
        string tableName,
        long tdefPage,
        TableDef tableDef,
        List<object[]> pendingRows,
        List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints,
        FkContext? fkContext,
        CancellationToken cancellationToken)
    {
        var batchLocations = new List<RowLocation>();
        var batchHintRows = new List<(RowLocation Loc, object[] Row)>();
        int inserted = 0;

        try
        {
            // AutoNumber values are already assigned, so the unique check sees
            // the exact keys the row writer and index maintainer will encode.
            await uniqueIndexes.CheckUniqueIndexesPreInsertAsync(
                tdefPage,
                tableDef,
                tableName,
                pendingRows,
                cancellationToken).ConfigureAwait(false);

            // Cancellation is honoured only between rows. A row write that is
            // interrupted partway leaves a live row the rollback below cannot
            // find, and abandoned index maintenance leaves the indexes out of
            // step with the rows, so both run to completion once started.
            foreach (object[] row in pendingRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (fkContext != null)
                {
                    await enforcer.EnforceFkOnInsertAsync(tableName, tableDef, row, fkContext, cancellationToken).ConfigureAwait(false);
                }

                RowLocation location = await tableRows.InsertRowDataLocAsync(tdefPage, tableDef, row, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                batchLocations.Add(location);
                batchHintRows.Add((location, row));

                if (fkContext != null)
                {
                    RelationshipEnforcer.AugmentParentSetsAfterInsert(tableName, tableDef, row, fkContext);
                }

                inserted++;
            }

            if (inserted > 0)
            {
                bool incremental = await indexes.TryMaintainIndexesIncrementalAsync(
                    tdefPage,
                    tableDef,
                    batchHintRows,
                    deletedRows: null,
                    CancellationToken.None).ConfigureAwait(false);
                if (!incremental)
                {
                    await indexes.MaintainIndexesAsync(tdefPage, tableDef, tableName, CancellationToken.None).ConfigureAwait(false);
                }

                await autoNumbers.UpdateHighWaterAsync(tdefPage, tableDef, pendingRows, CancellationToken.None).ConfigureAwait(false);
                await autoNumbers.UpdateComplexHighWaterAsync(tdefPage, tableDef, pendingRows, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch
        {
            await this.RollbackInsertedRowsAsync(tdefPage, batchLocations).ConfigureAwait(false);
            ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
            throw;
        }

        return inserted;
    }

    /// <summary>
    /// Refuses, before any page is written, a row whose Text or Memo value
    /// holds a character the database cannot store. Jet3 stores text in its
    /// code page, where .NET would write a best-fit match or <c>?</c>, and an
    /// index entry built from the caller's text would then not match the row
    /// (<see cref="DatabaseFile.DescribeUnstorableCharacter"/>). Checking the
    /// whole row also keeps an update from deleting the old row and then
    /// failing to encode the new one.
    /// </summary>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="row">The finished row, after defaults, in table-column order.</param>
    /// <exception cref="JetLimitationException">A Text or Memo value holds a character the database's code page does not have.</exception>
    private void ThrowIfTextNotStorable(string tableName, TableDef tableDef, object[] row)
    {
        if (db.Format != DatabaseFormat.Jet3Mdb)
        {
            return;
        }

        for (int i = 0; i < tableDef.Columns.Count && i < row.Length; i++)
        {
            ColumnInfo column = tableDef.Columns[i];
            if (row[i] is null or DBNull
                || JetTypeInfo.ResolveValueType(column) is not (ColumnType.TextType or ColumnType.MemoType))
            {
                continue;
            }

            if (Convert.ToString(row[i], CultureInfo.InvariantCulture) is { } text
                && db.DescribeUnstorableCharacter(text) is { } character)
            {
                throw new JetLimitationException(db.UnstorableTextMessage($"The value for column '{column.Name}' of table '{tableName}'", character));
            }
        }
    }

    /// <summary>
    /// Marks every row in <paramref name="locations"/> as deleted on its data
    /// page and rewinds the owning TDEF's row count by the matching amount.
    /// Takes no cancellation token, because the batch it undoes may have
    /// failed through cancellation. Best-effort: an I/O failure during
    /// rollback is swallowed so the original failure surfaces to the caller
    /// intact.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="locations">The locations.</param>
    private async ValueTask RollbackInsertedRowsAsync(long tdefPage, List<RowLocation> locations)
    {
        if (locations.Count == 0)
        {
            return;
        }

        try
        {
            foreach (RowLocation loc in locations)
            {
                await tableRows.MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, CancellationToken.None).ConfigureAwait(false);
            }

            await tableRows.AdjustTDefRowCountAsync(tdefPage, -locations.Count, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            // Best-effort rollback; surface the original failure.
        }
    }
}
