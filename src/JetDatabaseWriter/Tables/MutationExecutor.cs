namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding.Models;

/// <summary>Applies prepared row changes; the transaction lifecycle owns rollback and publication.</summary>
/// <param name="format">The database format.</param>
/// <param name="pager">The writer's transaction-aware page file.</param>
/// <param name="ownedPages">Finds existing pages for native system-table rewrites.</param>
/// <param name="dataPages">Allocates and resets row storage.</param>
/// <param name="rows">The row storage primitives.</param>
/// <param name="indexes">The native index-maintenance algorithms.</param>
/// <param name="autoNumbers">Persists prepared scalar and complex counters.</param>
internal sealed class MutationExecutor(JetFormat format, Pager pager, OwnedDataPages ownedPages, DataPageInserter dataPages, TableRowStore rows, IndexMaintainer indexes, AutoNumberMaintainer autoNumbers)
{
    /// <summary>Gets the maintenance path of the last system-table insert.</summary>
    internal SystemTableIndexMaintenancePath LastSystemTableIndexMaintenancePath { get; private set; }

    /// <summary>Checks every replacement before applying any row, then finalizes each affected table once.</summary>
    /// <param name="plan">The statement's composed prepared changes.</param>
    /// <param name="cancellationToken">The cancellation token; the owning statement restores cancelled writes.</param>
    internal async ValueTask ApplyAsync(MutationPlan plan, CancellationToken cancellationToken)
    {
        foreach (MutationPlan.TableChanges table in plan.Tables)
        {
            foreach (MutationPlan.RowChange change in table.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (change.Replacement is object[] replacement)
                {
                    _ = rows.EncodeRow(table.Definition, StorageValues(replacement, change.LongValues), change.TextBytes);
                }
            }
        }

        foreach (MutationPlan.TableChanges table in plan.Tables)
        {
            if (table.Rows.Count == 0 && table.FinalRowCount is null)
            {
                continue;
            }

            foreach (long page in table.ResetPages)
            {
                await pager.WritePageAsync(page, dataPages.CreateEmptyDataPage(table.Page), cancellationToken).ConfigureAwait(false);
            }

            if (table.ResetPages.Count > 0)
            {
                dataPages.SetCachedInsertPageNumber(table.Page, table.ResetPages[0]);
            }

            var inserted = new List<(RowLocation Loc, object[] Row)>();
            var deleted = new List<(RowLocation Loc, object[] Row)>();
            var newRows = new List<object[]>();
            int countDelta = 0;
            bool completeHints = true;
            foreach (MutationPlan.RowChange change in table.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (table.Policy == MutationPlan.IndexPolicy.Standard && change.Location is RowLocation retained
                    && change.Original is object[] before && change.Replacement is object[] after
                    && await rows.TryUpdateRowInPlaceAsync(table.Page, table.Definition, retained, before, after, StorageValues(after, change.LongValues), change.TextBytes, cancellationToken).ConfigureAwait(false))
                {
                    if (!await indexes.HasUnchangedKeysAsync(table.Page, table.Definition, table.Name, before, after, cancellationToken).ConfigureAwait(false))
                    {
                        deleted.Add((retained, before));
                        inserted.Add((retained, after));
                    }

                    newRows.Add(after);
                    continue;
                }

                if (change.Location is RowLocation location)
                {
                    bool retainLongValues = false;
                    if (change.LongValues is { Count: > 0 } longValues)
                    {
                        foreach (byte[] bytes in longValues.Values)
                        {
                            if (LongValueDescriptor.TryRead(bytes, out LongValueDescriptor descriptor) && descriptor.IsExternal)
                            {
                                retainLongValues = true;
                                break;
                            }
                        }
                    }

                    await rows.MarkRowDeletedAsync(location.PageNumber, location.RowIndex, table.Definition, change.DataMode, retainLongValues, cancellationToken).ConfigureAwait(false);
                    if (change.Original is object[] original)
                    {
                        deleted.Add((location, original));
                    }
                    else
                    {
                        completeHints = false;
                    }
                }

                if (change.Replacement is object[] replacement)
                {
                    RowLocation added = await rows.InsertRowDataLocAsync(table.Page, table.Definition, StorageValues(replacement, change.LongValues), updateTDefRowCount: false, textBytes: change.TextBytes, cancellationToken: cancellationToken).ConfigureAwait(false);
                    inserted.Add((added, replacement));
                    newRows.Add(replacement);
                }

                countDelta += change.CountDelta;
            }

            if (table.FinalRowCount is uint rowCount)
            {
                await rows.SetTDefRowCountAsync(table.Page, rowCount, cancellationToken).ConfigureAwait(false);
            }
            else if (countDelta != 0)
            {
                await rows.AdjustTDefRowCountAsync(table.Page, countDelta, cancellationToken).ConfigureAwait(false);
            }

            if (inserted.Count > 0 || deleted.Count > 0 || !completeHints || table.Policy != MutationPlan.IndexPolicy.Standard)
            {
                await this.MaintainIndexesAsync(table, inserted, deleted, completeHints, cancellationToken).ConfigureAwait(false);
            }

            if (table.RaiseAutoNumbers)
            {
                await autoNumbers.UpdateHighWaterAsync(table.Page, table.Definition, newRows, cancellationToken).ConfigureAwait(false);
            }

            if (table.RaiseComplexReferences)
            {
                await autoNumbers.UpdateComplexHighWaterAsync(table.Page, table.Definition, newRows, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static object[] StorageValues(object[] values, IReadOnlyDictionary<int, byte[]>? longValues)
    {
        if (longValues is not { Count: > 0 })
        {
            return values;
        }

        object[] stored = (object[])values.Clone();
        foreach ((int column, byte[] bytes) in longValues)
        {
            stored[column] = new PreEncodedLongValue(bytes);
        }

        return stored;
    }

    /// <summary>Stages one internal system row with its required native index policy.</summary>
    /// <param name="tdefPage">The table-definition page.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="tableName">The system table name.</param>
    /// <param name="values">The prepared row.</param>
    /// <param name="updateTDefRowCount">Whether the row raises the table count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask InsertSystemRowAndMaintainAsync(long tdefPage, TableDef tableDef, string tableName, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default)
    {
        this.LastSystemTableIndexMaintenancePath = SystemTableIndexMaintenancePath.None;
        var plan = new MutationPlan();
        plan.ForTable(tdefPage, tableDef, tableName, MutationPlan.IndexPolicy.System).Insert(values, updateTDefRowCount);
        await this.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Replaces small system-table data pages when native readers cannot accept tombstones.</summary>
    /// <param name="tdefPage">The table-definition page.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="tableName">The system table name.</param>
    /// <param name="values">The complete prepared live rows.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetCorruptDataException">The system table has live rows but no data pages.</exception>
    internal async ValueTask RewriteSystemTableRowsAsync(long tdefPage, TableDef tableDef, string tableName, IReadOnlyList<object[]> values, CancellationToken cancellationToken)
    {
        var plan = new MutationPlan();
        MutationPlan.TableChanges table = plan.ForTable(tdefPage, tableDef, tableName, MutationPlan.IndexPolicy.Rebuild);
        await ownedPages.ForEachOwnedDataPageAsync(
            tdefPage,
            (page, _, _) =>
            {
                table.ResetPages.Add(page);
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);
        if (table.ResetPages.Count == 0 && values.Count > 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptTableDefinition, $"System table '{tableName}' has no data pages to rewrite.", new JetErrorInfo { TableName = tableName, PageNumber = tdefPage });
        }

        foreach (object[] value in values)
        {
            UnreadableLongValue.ThrowIfAny(value, tableName);
            table.Insert((object[])value.Clone(), updateRowCount: false);
        }

        table.FinalRowCount = checked((uint)values.Count);
        await this.ApplyAsync(plan, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask MaintainIndexesAsync(MutationPlan.TableChanges table, List<(RowLocation Loc, object[] Row)> inserted, List<(RowLocation Loc, object[] Row)> deleted, bool completeHints, CancellationToken cancellationToken)
    {
        switch (table.Policy)
        {
            case MutationPlan.IndexPolicy.System:
                this.LastSystemTableIndexMaintenancePath = await indexes.MaintainSystemTableIndexesIncrementallyAsync(table.Page, table.Definition, table.Name, inserted, deleted, cancellationToken).ConfigureAwait(false);
                return;
            case MutationPlan.IndexPolicy.CatalogInsert:
                foreach ((RowLocation location, object[] values) in inserted)
                {
                    if (!await indexes.TrySpliceCatalogIndexEntryAsync(table.Page, table.Definition, location, values, cancellationToken).ConfigureAwait(false))
                    {
                        throw new JetOperationException(JetErrorCode.CatalogIndexMaintenanceFailed, $"Could not maintain MSysObjects catalog indexes for '{table.Operation}'.");
                    }
                }

                return;
            case MutationPlan.IndexPolicy.CatalogRelocation:
                for (int index = 0; index < inserted.Count; index++)
                {
                    var original = new LocatedRow(deleted[index].Loc, deleted[index].Row);
                    var replacement = new LocatedRow(inserted[index].Loc, inserted[index].Row);
                    if (!await indexes.TryRelocateCatalogRowAsync(table.Definition, original, replacement, cancellationToken).ConfigureAwait(false))
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"The native catalog indexes for '{table.Operation}' could not retain their stored keys.", new JetErrorInfo { TableName = table.Operation });
                    }
                }

                return;
            case MutationPlan.IndexPolicy.Rebuild:
                break;
            case MutationPlan.IndexPolicy.Standard:
            case MutationPlan.IndexPolicy.CatalogChange:
                if (completeHints && await indexes.TryMaintainIndexesIncrementalAsync(table.Page, table.Definition, inserted, deleted, cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                if (table.Policy == MutationPlan.IndexPolicy.CatalogChange && !format.IsJet3)
                {
                    throw new JetOperationException(JetErrorCode.CatalogIndexMaintenanceFailed, $"Could not maintain MSysObjects catalog indexes while {table.Operation}.");
                }

                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(table));
        }

        await indexes.MaintainIndexesAsync(table.Page, table.Definition, table.Name, cancellationToken).ConfigureAwait(false);
    }
}
