namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Owns replacement row copies, table storage transfers and page reclamation.</summary>
/// <param name="format">The format service.</param>
/// <param name="tableDefs">The tableDefs service.</param>
/// <param name="ownedPages">The ownedPages service.</param>
/// <param name="pager">The pager service.</param>
/// <param name="catalog">The catalog service.</param>
/// <param name="tableRows">The tableRows service.</param>
/// <param name="indexMaintainer">The indexMaintainer service.</param>
/// <param name="pageAllocator">The pageAllocator service.</param>
/// <param name="longValueEncoder">The longValueEncoder service.</param>
/// <param name="catalogWriter">The catalogWriter service.</param>
/// <param name="catalogArtifacts">The catalogArtifacts service.</param>
/// <param name="complexColumns">The complexColumns service.</param>
/// <param name="constraints">The constraints service.</param>
/// <param name="relationships">The relationships service.</param>
/// <param name="autoNumbers">The autoNumbers service.</param>
internal sealed class TableStorageEditor(
    JetFormat format,
    TableDefReader tableDefs,
    OwnedDataPages ownedPages,
    Pager pager,
    TableCatalog catalog,
    TableRowStore tableRows,
    IndexMaintainer indexMaintainer,
    PageAllocator pageAllocator,
    LongValueEncoder longValueEncoder,
    CatalogWriter catalogWriter,
    CatalogArtifactWriter catalogArtifacts,
    ComplexColumnManager complexColumns,
    ConstraintRegistry constraints,
    RelationshipManager relationships,
    AutoNumberMaintainer autoNumbers)
{
    /// <summary>Copies prepared rows and transfers storage and metadata ownership to the replacement.</summary>
    /// <param name="plan">The validated rewrite projection.</param>
    /// <param name="tempName">The replacement table already created by the schema editor.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask ApplyAsync(TableRewritePlan plan, string tempName, CancellationToken cancellationToken)
    {
        TableRewriteCopy copy = await this.CopyRowsAndIndexesAsync(plan, tempName, cancellationToken).ConfigureAwait(false);
        await this.ValidateCopyAsync(plan, copy, cancellationToken).ConfigureAwait(false);
        await this.ReplaceOriginalAsync(plan, copy, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<TableRewriteCopy> CopyRowsAndIndexesAsync(TableRewritePlan plan, string tempName, CancellationToken cancellationToken)
    {
        CatalogEntry entry = plan.Entry;
        List<IndexDefinition> projectedIndexes = plan.Indexes;
        List<object[]> projectedRows = plan.Rows;
        RelationshipRewriteState relationshipState = plan.Relationships;
        Func<string, string?> mapColumnName = plan.MapColumnName;
        bool transplant = plan.Transplant;
        ResolvedTable tempTable = await catalog.ResolveRequiredTableAsync(tempName, cancellationToken).ConfigureAwait(false);
        CatalogEntry tempEntry = tempTable.Entry;
        TableDef tempDef = tempTable.Definition;

        // Re-emit the foreign-key index entries on the copy before the row copy,
        // so the single index rebuild below also fills their leaves. A
        // self-referencing entry points at the page the table ends up on: the
        // original TDEF page when the copy is transplanted, else the copy's.
        long finalTdefPage = transplant ? entry.TDefPage : tempEntry.TDefPage;
        IReadOnlyDictionary<int, int> fkIndexNumbers = await relationships.EmitFkEntriesForRewriteAsync(
            relationshipState,
            tempEntry.TDefPage,
            tempDef,
            finalTdefPage,
            mapColumnName,
            cancellationToken).ConfigureAwait(false);
        if (fkIndexNumbers.Count > 0)
        {
            tempDef = await tableDefs.ReadRequiredTableDefAsync(tempEntry.TDefPage, tempName, cancellationToken).ConfigureAwait(false);
        }

        // The temp TDEF starts with its AutoNumber and complex AutoNumber
        // counters at 0. Carry the original's high-water values (and anything
        // larger the copied rows hold) over to it, or values and per-row
        // complex references freed by deleting the top rows would be handed
        // out again once the temp table takes the original's place.
        long autoNumberHighWater = plan.AutoNumberHighWater;
        long complexHighWater = plan.ComplexHighWater;
        var writtenRows = new List<LocatedRow>(projectedRows.Count);
        foreach (object[] projected in projectedRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RowLocation location = await tableRows.InsertRowDataLocAsync(tempEntry.TDefPage, tempDef, projected, cancellationToken: cancellationToken).ConfigureAwait(false);
            writtenRows.Add(new LocatedRow(location, projected));
            autoNumberHighWater = Math.Max(autoNumberHighWater, AutoNumberMaintainer.MaxAutoNumberValue(tempDef, projected));
            complexHighWater = Math.Max(complexHighWater, AutoNumberMaintainer.MaxComplexReference(tempDef, projected));
        }

        await autoNumbers.RaiseHighWaterAsync(tempEntry.TDefPage, autoNumberHighWater, cancellationToken).ConfigureAwait(false);
        await autoNumbers.RaiseComplexHighWaterAsync(tempEntry.TDefPage, complexHighWater, cancellationToken).ConfigureAwait(false);

        // Rebuild forwarded indexes once after the bulk row copy completes,
        // so we don't pay the rebuild cost per row. The rebuild keys the rows
        // just written at the locations the inserts returned, rather than
        // re-reading the copy. Re-emitted FK entries need the rebuild even on
        // an empty table, so it records their leaves in the index usage map.
        if (fkIndexNumbers.Count > 0 || (projectedIndexes.Count > 0 && writtenRows.Count > 0))
        {
            await indexMaintainer.RebuildIndexesAsync(tempEntry.TDefPage, tempDef, tempName, writtenRows, cancellationToken).ConfigureAwait(false);
        }

        return new TableRewriteCopy(tempName, tempEntry, tempDef, finalTdefPage, fkIndexNumbers);
    }

    private async ValueTask ValidateCopyAsync(TableRewritePlan plan, TableRewriteCopy copy, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!plan.Transplant)
        {
            return;
        }

        byte[] tdef = await pager.ReadPageAsync(copy.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (tdef[0] != Constants.PageTypes.TableDefinition || Ri32(tdef, 4) != 0)
            {
                throw new NotSupportedException("Complex table schema rewrite currently requires a single-page rebuilt TDEF.");
            }
        }
        finally
        {
            PageBuffers.Return(tdef);
        }
    }

    private async ValueTask ReplaceOriginalAsync(TableRewritePlan plan, TableRewriteCopy copy, CancellationToken cancellationToken)
    {
        string tableName = plan.TableName;
        CatalogEntry entry = plan.Entry;
        TableDef tableDef = plan.Definition;
        byte[]? persistedLvProp = plan.PropertyBytes;
        RelationshipRewriteState relationshipState = plan.Relationships;
        Func<string, string?> mapColumnName = plan.MapColumnName;
        Dictionary<int, ColumnDefinition> newComplexById = plan.ComplexColumns;
        List<(string Name, int ComplexId)> droppedComplex = plan.DroppedComplexColumns;
        List<(string OldName, string NewName, int ComplexId)> renamedComplex = plan.RenamedComplexColumns;
        bool transplant = plan.Transplant;
        string tempName = copy.Name;
        CatalogEntry tempEntry = copy.Entry;
        TableDef tempDef = copy.Definition;
        long finalTdefPage = copy.FinalTDefPage;
        IReadOnlyDictionary<int, int> fkIndexNumbers = copy.FkIndexNumbers;
        // Drop the original table, then rename the temp catalog entry to take its place.
        // Either way the table's catalog row carries the projected persisted properties.
        if (transplant)
        {
            await this.TransplantTempTableToOriginalAsync(
                tableName,
                entry.TDefPage,
                tableDef,
                tempName,
                tempEntry.TDefPage,
                persistedLvProp,
                cancellationToken).ConfigureAwait(false);

            // New complex columns were emitted under the temporary parent.
            // Their catalog ownership must follow the transplanted TDEF too.
            foreach (ColumnInfo column in tempDef.Columns)
            {
                if ((column.Type is ComplexType) && column.Misc > 0)
                {
                    await complexColumns.UpdateComplexColumnParentTableIdAsync(
                        column.Misc,
                        checked((int)entry.TDefPage),
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
        else
        {
            await this.DropTableCoreAsync(tableName, rewriting: true, cancellationToken).ConfigureAwait(false);
            await catalogWriter.RenameTableInCatalogAsync(tempName, tableName, persistedLvProp, cancellationToken).ConfigureAwait(false);

            foreach (ColumnDefinition survivor in newComplexById.Values)
            {
                await complexColumns.UpdateComplexColumnParentTableIdAsync(
                    survivor.ComplexId,
                    checked((int)tempEntry.TDefPage),
                    cancellationToken).ConfigureAwait(false);
            }

            foreach ((string colName, int complexId) in droppedComplex)
            {
                await complexColumns.DropSingleComplexChildAsync(entry.TDefPage, colName, complexId, (page, definition, token) => this.ReclaimTableStoragePagesAsync(page, definition, includeTDefRoot: true, token), cancellationToken).ConfigureAwait(false);
            }

            foreach ((string oldColName, string newColName, int complexId) in renamedComplex)
            {
                await complexColumns.RenameComplexColumnArtifactsAsync(oldColName, newColName, complexId, cancellationToken).ConfigureAwait(false);
            }
        }

        // Point every partner table's FK entry at the table's final TDEF page
        // and renumbered entries, and rename key columns in MSysRelationships.
        await relationships.CompleteRewriteAsync(relationshipState, finalTdefPage, fkIndexNumbers, mapColumnName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Moves the rebuilt copy onto the original TDEF page, so the complex columns'
    /// <c>MSysComplexColumns</c> rows keep naming the table, and frees the original's
    /// storage.
    /// </summary>
    /// <param name="tableName">The table being rewritten.</param>
    /// <param name="originalTdefPage">The original's TDEF page, which the copy takes over.</param>
    /// <param name="originalDef">The original's definition, with its calculated result types.</param>
    /// <param name="tempName">The rebuilt copy's name.</param>
    /// <param name="tempTdefPage">The rebuilt copy's TDEF page.</param>
    /// <param name="lvProp">The persisted properties for the table's catalog row.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the copy has replaced the original.</returns>
    /// <exception cref="NotSupportedException">The rebuilt copy's TDEF spans more than one page.</exception>
    private async ValueTask TransplantTempTableToOriginalAsync(
        string tableName,
        long originalTdefPage,
        TableDef originalDef,
        string tempName,
        long tempTdefPage,
        byte[]? lvProp,
        CancellationToken cancellationToken)
    {
        byte[] tempTdef = await pager.ReadPageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            await this.ReclaimTableStoragePagesAsync(originalTdefPage, originalDef, includeTDefRoot: false, cancellationToken).ConfigureAwait(false);
            await this.PatchTablePageOwnersAsync(tempTdefPage, originalTdefPage, cancellationToken).ConfigureAwait(false);
            await pager.WritePageAsync(originalTdefPage, tempTdef, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(tempTdef);
        }

        await catalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan([], [])
            {
                CatalogReplacements =
                [
                    new UserTableCatalogReplacementArtifact(
                        tableName,
                        tableName,
                        originalTdefPage,
                        lvProp,
                        Operation: $"replacing catalog row for '{tableName}'",
                        MissingMessage: $"Catalog row for '{tableName}' was not found during schema rewrite."),
                ],
                CatalogDeletions =
                [
                    new UserTableCatalogDeletionArtifact(
                        tempName,
                        tempTdefPage,
                        Operation: $"deleting catalog row for '{tempName}'"),
                ],
            },
            cancellationToken).ConfigureAwait(false);
        await catalogWriter.DeleteAceRowsForObjectIdsAsync([tempTdefPage], cancellationToken).ConfigureAwait(false);
        await pageAllocator.DeallocatePageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);

        // The rebuilt schema's constraints were registered under the temp name.
        // Move them onto the table, as the drop-and-rename path does, so the
        // stale pre-rewrite list (with its old column count and in-session
        // AutoNumber seed) is not applied to the next insert.
        constraints.Unregister(tableName);
        constraints.Rename(tempName, tableName);
    }

    private async ValueTask PatchTablePageOwnersAsync(long fromTdefPage, long toTdefPage, CancellationToken cancellationToken)
    {
        long totalPages = pager.PageCount;
        for (long pageNumber = 3; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                bool patchDataPage = page[0] == Constants.PageTypes.Data && Ri32(page, format.DataPage.TDefOff) == fromTdefPage;
                bool patchIndexPage = page[0] is Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf && Ri32(page, 4) == fromTdefPage;
                if (!patchDataPage && !patchIndexPage)
                {
                    continue;
                }

                int ownerOffset = patchDataPage ? format.DataPage.TDefOff : 4;
                Wi32(page, ownerOffset, checked((int)toTdefPage));
                await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }
    }

    /// <summary>
    /// Shared implementation backing <see cref="TableSchemaEditor.DropTableAsync"/> and the
    /// <c>RewriteTableAsync</c> path. The rewrite path sets
    /// <paramref name="rewriting"/>, so the hidden flat child tables and
    /// matching <c>MSysComplexColumns</c> rows for surviving complex columns
    /// stay attached to the rebuilt parent, and the partner tables' FK entries
    /// are left for <see cref="RelationshipManager.CompleteRewriteAsync"/> to
    /// re-link. A real drop removes both.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rewriting">Whether a schema rewrite is replacing the table with its rebuilt copy.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <c>MSysObjects</c> is missing or no matching user table exists.</exception>
    internal async ValueTask DropTableCoreAsync(string tableName, bool rewriting, CancellationToken cancellationToken)
    {
        Dictionary<long, TableDef> definitions = await this.ReadDefinitionsBeforeDropAsync(tableName, cancellationToken).ConfigureAwait(false);
        UserTableCatalogDeletionResult deleted = await catalogWriter.DeleteUserTableCatalogRowsAsync(
            tableName,
            tdefPage: null,
            includeSystemTables: false,
            throwIfNotFound: true,
            operation: $"dropping table '{tableName}'",
            missingMessage: $"Table '{tableName}' does not exist.",
            cancellationToken).ConfigureAwait(false);

        if (!rewriting)
        {
            // No MSysRelationships row names the table (DropTableAsync removed or refused
            // otherwise), but its TDEF can still hold FK entries that earlier
            // builds left behind. Remove the partner entries that name it
            // while its TDEF is intact, so none is left naming a freed page
            // or, later, the unrelated table that reuses it.
            foreach (long tdefPage in deleted.TDefPages)
            {
                await relationships.RemovePartnerLinksAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            }

            foreach (long parentTdefPage in deleted.TDefPages)
            {
                await complexColumns.DropComplexChildrenForTableAsync(parentTdefPage, (page, definition, token) => this.ReclaimTableStoragePagesAsync(page, definition, includeTDefRoot: true, token), cancellationToken).ConfigureAwait(false);
            }
        }

        await catalogWriter.DeleteAceRowsForObjectIdsAsync(deleted.TDefPages, cancellationToken).ConfigureAwait(false);

        foreach (long tdefPage in deleted.TDefPages)
        {
            await this.ReclaimTableStoragePagesAsync(tdefPage, definitions.GetValueOrDefault(tdefPage), includeTDefRoot: true, cancellationToken).ConfigureAwait(false);
        }

        constraints.Unregister(tableName);
        catalog.Invalidate();
    }

    /// <summary>
    /// Reads the definition of each user table named <paramref name="tableName"/>,
    /// by TDEF page, while its <c>MSysObjects</c> row still holds the calculated
    /// columns' <c>ResultType</c>. A calculated column whose result is Memo or OLE
    /// can have another descriptor type in an Access-authored table, and the
    /// reclaim needs the result type to find the LVAL rows its values point to.
    /// </summary>
    /// <param name="tableName">The table about to be dropped.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The definitions, by TDEF page.</returns>
    private async ValueTask<Dictionary<long, TableDef>> ReadDefinitionsBeforeDropAsync(string tableName, CancellationToken cancellationToken)
    {
        var definitions = new Dictionary<long, TableDef>();
        foreach (CatalogEntry entry in await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(entry.Name, tableName, StringComparison.OrdinalIgnoreCase)
                && await catalog.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false) is TableDef tableDef)
            {
                definitions[entry.TDefPage] = tableDef;
            }
        }

        return definitions;
    }

    /// <summary>
    /// Frees a table's storage: its data pages, the LVAL rows its rows' long values
    /// point to, the pages its usage maps list after the first two (index pages, and
    /// in an Access-authored table each long-value column's LVAL pages), its usage-map
    /// page with the bitmap pages of its REFERENCE rows, and its TDEF pages.
    /// </summary>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="tableDef">
    /// The table's definition, read with its calculated result types before its
    /// catalog row was deleted; <see langword="null"/> reads it from the TDEF, by
    /// descriptor type alone.
    /// </param>
    /// <param name="includeTDefRoot">Whether to free the first TDEF page too; a transplant keeps it.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the pages are freed.</returns>
    private async ValueTask ReclaimTableStoragePagesAsync(long tdefPage, TableDef? tableDef, bool includeTDefRoot, CancellationToken cancellationToken)
    {
        var pagesToFree = new SortedSet<long>();
        var longValueRoots = new List<LongValueDescriptor>();

        tableDef ??= await tableDefs.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long totalPages = pager.PageCount;
        if (tableDef is not null)
        {
            // Overflow rows are followed to their moved bytes, so their long
            // values are freed too.
            await ownedPages.ForEachOwnedDataPageAsync(
                tdefPage,
                (pageNumber, page, token) =>
                {
                    pagesToFree.Add(pageNumber);
                    return ownedPages.ForEachRowOnPageAsync(
                        pageNumber,
                        page,
                        (row, _) =>
                        {
                            var rowBound = new RowBound(row.Location.DataRowIndex, row.Location.RowStart, row.Location.RowSize);
                            longValueRoots.AddRange(longValueEncoder.CollectLongValueRoots(row.Page, rowBound, tableDef));
                            return new ValueTask<bool>(true);
                        },
                        token);
                },
                cancellationToken).ConfigureAwait(false);
        }

        byte[]? firstTdefPage = null;
        var seenTdefPages = new HashSet<long>();
        long currentTdefPage = tdefPage;
        while (currentTdefPage > 0 && currentTdefPage < totalPages && seenTdefPages.Add(currentTdefPage))
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await pager.ReadPageAsync(currentTdefPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.TableDefinition)
                {
                    break;
                }

                if (includeTDefRoot || currentTdefPage != tdefPage)
                {
                    _ = pagesToFree.Add(currentTdefPage);
                }

                firstTdefPage ??= (byte[])page.Clone();
                currentTdefPage = Ri32(page, 4);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        if (firstTdefPage is not null && !format.IsJet3)
        {
            int usageMapPage = UsageMap.ReadUInt24(firstTdefPage, format.TDef.UsedPagesPage);
            if (usageMapPage > 0)
            {
                _ = pagesToFree.Add(usageMapPage);
                await this.CollectIndexPagesFromUsageMapAsync(usageMapPage, pagesToFree, cancellationToken).ConfigureAwait(false);
            }
        }

        foreach (LongValueDescriptor root in longValueRoots)
        {
            await longValueEncoder.DeallocateLongValueAsync(root, cancellationToken).ConfigureAwait(false);
        }

        foreach (long pageNumber in pagesToFree)
        {
            if (pageNumber > 2)
            {
                await pageAllocator.DeallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask CollectIndexPagesFromUsageMapAsync(long usageMapPageNumber, SortedSet<long> pagesToFree, CancellationToken cancellationToken)
    {
        long totalPages = pager.PageCount;
        if (usageMapPageNumber <= 0 || usageMapPageNumber >= totalPages)
        {
            return;
        }

        byte[] page = await pager.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.Data)
            {
                return;
            }

            foreach (RowBound rowBound in DataPageRows.EnumerateLiveRowBounds(format, page))
            {
                // A REFERENCE row's bitmap pages go with the table, the owned
                // and free-space rows' included.
                await UsageMap.CollectReferenceBitmapPagesAsync(page, rowBound, totalPages, pager.ReadPageAsync, PageBuffers.Return, pagesToFree, cancellationToken).ConfigureAwait(false);
                if (rowBound.RowIndex < 2)
                {
                    continue;
                }

                var indexPages = new List<long>();
                if (!await UsageMap.TryEnumeratePagesAsync(
                    page,
                    rowBound,
                    format.PageSize,
                    totalPages,
                    minimumPageNumber: 3,
                    strict: false,
                    pager.ReadPageAsync,
                    PageBuffers.Return,
                    indexPages,
                    cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                foreach (long pageNumber in indexPages)
                {
                    _ = pagesToFree.Add(pageNumber);
                }
            }
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }
}
