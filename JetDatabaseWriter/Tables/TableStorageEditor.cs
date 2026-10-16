namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.LongValues;
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

/// <summary>Builds isolated replacement storage, publishes it on the original table identity and reclaims pages.</summary>
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
/// <param name="complexColumns">The complexColumns service.</param>
/// <param name="constraints">The constraints service.</param>
/// <param name="relationships">The relationships service.</param>
/// <param name="autoNumbers">The autoNumbers service.</param>
/// <param name="catalogRows">The catalog used to prove surviving map ownership.</param>
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
    ComplexColumnManager complexColumns,
    ConstraintRegistry constraints,
    RelationshipManager relationships,
    AutoNumberMaintainer autoNumbers,
    CatalogRowReader catalogRows)
{
    /// <summary>Copies prepared rows and transfers storage and metadata ownership to the replacement.</summary>
    /// <param name="plan">The validated rewrite projection.</param>
    /// <param name="tempName">The replacement table already created by the schema editor.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask ApplyAsync(TableRewritePlan plan, string tempName, CancellationToken cancellationToken)
    {
        TableRewriteCopy copy = await this.CopyRowsAndIndexesAsync(plan, tempName, cancellationToken).ConfigureAwait(false);
        await this.PreserveJet3IndexMetadataAsync(plan, copy, cancellationToken).ConfigureAwait(false);
        await this.ValidateCopyAsync(copy, cancellationToken).ConfigureAwait(false);
        await this.ReplaceOriginalAsync(plan, copy, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<TableRewriteCopy> CopyRowsAndIndexesAsync(TableRewritePlan plan, string tempName, CancellationToken cancellationToken)
    {
        List<IndexDefinition> projectedIndexes = plan.Indexes;
        List<object[]> projectedRows = plan.Rows;
        RelationshipRewriteState relationshipState = plan.Relationships;
        Func<string, string?> mapColumnName = plan.MapColumnName;
        ResolvedTable tempTable = await catalog.ResolveRequiredTableAsync(tempName, cancellationToken).ConfigureAwait(false);
        CatalogEntry tempEntry = tempTable.Entry;
        TableDef tempDef = tempTable.Definition;

        // Build FK leaves privately; self references name the original identity
        // that the replacement will occupy once publication completes.
        IReadOnlyDictionary<int, int> fkIndexNumbers = await relationships.EmitFkEntriesForRewriteAsync(
            relationshipState,
            tempEntry.TDefPage,
            tempDef,
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
        var storagePages = new SortedSet<long>();
        for (int rowIndex = 0; rowIndex < projectedRows.Count; rowIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object[] projected = projectedRows[rowIndex];
            RowLocation location = await tableRows.InsertRowDataLocAsync(tempEntry.TDefPage, tempDef, projected, fixedTextBytes: plan.FixedTextBytes[rowIndex], cancellationToken: cancellationToken).ConfigureAwait(false);
            writtenRows.Add(new LocatedRow(location, projected));
            storagePages.Add(location.PageNumber);
            storagePages.Add(location.DataPageNumber);
            autoNumberHighWater = Math.Max(autoNumberHighWater, AutoNumberMaintainer.MaxAutoNumberValue(tempDef, projected));
            complexHighWater = Math.Max(complexHighWater, AutoNumberMaintainer.MaxComplexReference(tempDef, projected));
        }

        await autoNumbers.RaiseHighWaterAsync(tempEntry.TDefPage, autoNumberHighWater, cancellationToken).ConfigureAwait(false);
        await autoNumbers.RaiseComplexHighWaterAsync(tempEntry.TDefPage, complexHighWater, cancellationToken).ConfigureAwait(false);

        long[] initialIndexPages = await indexMaintainer.CollectRewriteIndexPagesAsync(tempEntry.TDefPage, cancellationToken).ConfigureAwait(false);

        // Rebuild forwarded indexes once after the bulk row copy completes,
        // so we don't pay the rebuild cost per row. The rebuild keys the rows
        // just written at the locations the inserts returned, rather than
        // re-reading the copy. Re-emitted FK entries need the rebuild even on
        // an empty table, so it records their leaves in the index usage map.
        if (fkIndexNumbers.Count > 0 || (projectedIndexes.Count > 0 && writtenRows.Count > 0))
        {
            await indexMaintainer.RebuildIndexesAsync(tempEntry.TDefPage, tempDef, tempName, writtenRows, cancellationToken).ConfigureAwait(false);
        }

        storagePages.UnionWith(await indexMaintainer.CollectRewriteIndexPagesAsync(tempEntry.TDefPage, cancellationToken).ConfigureAwait(false));

        // Some formats retain initial leaves when long values or native maps
        // prevent reclamation. They are private too, even outside the new tree.
        foreach (long pageNumber in initialIndexPages)
        {
            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Freed)
                {
                    _ = this.ReplacementOwnerOffset(page, pageNumber, tempEntry.TDefPage);
                    storagePages.Add(pageNumber);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return new TableRewriteCopy(tempName, tempEntry, tempDef, [.. storagePages], fkIndexNumbers);
    }

    private async ValueTask PreserveJet3IndexMetadataAsync(TableRewritePlan plan, TableRewriteCopy copy, CancellationToken cancellationToken)
    {
        if (!format.IsJet3 || plan.SourceImage.RealIndexes.Count == 0)
        {
            return;
        }

        byte[] original = plan.SourceImage.CopyBytes();
        var originalIndexes = new Dictionary<string, IndexMetadata>(StringComparer.OrdinalIgnoreCase);
        foreach (IndexMetadata index in IndexCatalogReader.ReadMetadata(format, original, plan.Definition.Columns))
        {
            originalIndexes.Add(index.Name, index);
        }

        LogicalTDefChain replacement = await tableDefs.ReadTDefChainAsync(copy.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        TDefCounts counts = TDefCodec.ReadCounts(format, replacement.Bytes);
        int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(format, replacement.Bytes, counts.ColumnCount, counts.RealIndexCount);
        foreach (IndexMetadata index in IndexCatalogReader.ReadMetadata(format, replacement.Bytes, copy.Definition.Columns))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!originalIndexes.TryGetValue(index.Name, out IndexMetadata? source))
            {
                continue;
            }

            int sourceSlot = format.TDef.BlockEnd + (source.RealIndexNumber * format.TDef.RealIdxEntrySz);
            int targetSlot = format.TDef.BlockEnd + (index.RealIndexNumber * format.TDef.RealIdxEntrySz);
            original.AsSpan(sourceSlot, format.TDef.RealIdxEntrySz).CopyTo(replacement.Bytes.AsSpan(targetSlot));
            int descriptor = format.Index.RealIdxPhysOffset(descriptorStart, index.RealIndexNumber);
            replacement.Bytes[format.Index.FlagsAbsoluteOffset(descriptor)] = plan.SourceImage.RealIndexes[source.RealIndexNumber].Slot.Flags;
        }

        await replacement.WriteInPlaceAsync(pager.ReadPageAsync, PageBuffers.Return, pager.WritePageAsync, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ValidateCopyAsync(TableRewriteCopy copy, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _ = await LogicalTDefChain.ReadRequiredAsync(
            copy.Entry.TDefPage,
            format.PageSize,
            pager.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: false,
            cancellationToken).ConfigureAwait(false);

        foreach (long pageNumber in copy.StoragePages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = this.ReplacementOwnerOffset(page, pageNumber, copy.Entry.TDefPage);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }
    }

    private async ValueTask ReplaceOriginalAsync(TableRewritePlan plan, TableRewriteCopy copy, CancellationToken cancellationToken)
    {
        await this.PublishReplacementAsync(plan, copy, cancellationToken).ConfigureAwait(false);

        // Only new complex descriptors were emitted under the private parent.
        // Existing children already name the unchanged original table identity.
        var originalComplexIds = new HashSet<int>();
        foreach (ColumnInfo column in plan.Definition.Columns)
        {
            if (column.Type is ComplexType)
            {
                originalComplexIds.Add(column.Misc);
            }
        }

        foreach (ColumnInfo column in copy.Definition.Columns)
        {
            if (column.Type is ComplexType && column.Misc > 0 && !originalComplexIds.Contains(column.Misc))
            {
                await complexColumns.UpdateComplexColumnParentTableIdAsync(
                    column.Misc,
                    checked((int)plan.Entry.TDefPage),
                    cancellationToken).ConfigureAwait(false);
            }
        }

        foreach ((string name, int complexId) in plan.DroppedComplexColumns)
        {
            await complexColumns.DropSingleComplexChildAsync(plan.Entry.TDefPage, name, complexId, (page, definition, token) => this.ReclaimTableStoragePagesAsync(page, definition, includeTDefRoot: true, token), cancellationToken).ConfigureAwait(false);
        }

        foreach ((string oldName, string newName, int complexId) in plan.RenamedComplexColumns)
        {
            await complexColumns.RenameComplexColumnArtifactsAsync(oldName, newName, complexId, cancellationToken).ConfigureAwait(false);
        }

        // Table identities stay fixed; logical FK slots can still be renumbered.
        await relationships.CompleteRewriteAsync(plan.Relationships, copy.FkIndexNumbers, plan.MapColumnName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Publishes private replacement storage on the original root and retains its catalog identity.</summary>
    /// <param name="plan">The original identity and prepared schema.</param>
    /// <param name="copy">The validated private replacement.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async ValueTask PublishReplacementAsync(TableRewritePlan plan, TableRewriteCopy copy, CancellationToken cancellationToken)
    {
        long originalTdefPage = plan.Entry.TDefPage;
        long tempTdefPage = copy.Entry.TDefPage;
        byte[] tempTdef = await pager.ReadPageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            await this.ReclaimTableStoragePagesAsync(originalTdefPage, plan.Definition, includeTDefRoot: false, cancellationToken).ConfigureAwait(false);
            await this.PatchReplacementPageOwnersAsync(copy, originalTdefPage, cancellationToken).ConfigureAwait(false);
            await pager.WritePageAsync(originalTdefPage, tempTdef, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(tempTdef);
        }

        await catalogWriter.UpdateTablePropertiesAsync(
            plan.TableName,
            originalTdefPage,
            plan.PropertyBytes,
            copy.Definition.HasComplexColumns,
            cancellationToken).ConfigureAwait(false);
        _ = await catalogWriter.DeleteUserTableCatalogRowsAsync(
            copy.Name,
            tempTdefPage,
            includeSystemTables: true,
            throwIfNotFound: false,
            operation: $"deleting catalog row for '{copy.Name}'",
            missingMessage: null,
            cancellationToken).ConfigureAwait(false);
        await catalogWriter.DeleteAceRowsForObjectIdsAsync([tempTdefPage], cancellationToken).ConfigureAwait(false);
        await pageAllocator.DeallocatePageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);

        constraints.Unregister(plan.TableName);
        constraints.Rename(copy.Name, plan.TableName);
        constraints.RebindPhysicalTable(plan.TableName, originalTdefPage);
    }

    private async ValueTask PatchReplacementPageOwnersAsync(TableRewriteCopy copy, long originalTdefPage, CancellationToken cancellationToken)
    {
        foreach (long pageNumber in copy.StoragePages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                int ownerOffset = this.ReplacementOwnerOffset(page, pageNumber, copy.Entry.TDefPage);
                Wi32(page, ownerOffset, checked((int)originalTdefPage));
                await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }
    }

    private int ReplacementOwnerOffset(byte[] page, long pageNumber, long tempTdefPage)
    {
        int ownerOffset = page[0] == Constants.PageTypes.Data ? format.DataPage.TDefOff : 4;
        if (page[0] is not (Constants.PageTypes.Data or Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf)
            || Ri32(page, ownerOffset) != tempTdefPage)
        {
            throw new JetCorruptDataException($"Replacement storage page {pageNumber} does not belong to temporary table {tempTdefPage}.");
        }

        return ownerOffset;
    }

    /// <summary>Drops a table, its relationship links and its complex children.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <c>MSysObjects</c> is missing or no matching user table exists.</exception>
    internal async ValueTask DropTableCoreAsync(string tableName, CancellationToken cancellationToken)
    {
        Dictionary<long, TableDef> definitions = await this.ReadDefinitionsBeforeDropAsync(tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<long> deletedPages = await catalogWriter.DeleteUserTableCatalogRowsAsync(
            tableName,
            tdefPage: null,
            includeSystemTables: false,
            throwIfNotFound: true,
            operation: $"dropping table '{tableName}'",
            missingMessage: $"Table '{tableName}' does not exist.",
            cancellationToken).ConfigureAwait(false);

        // No MSysRelationships row names the table (DropTableAsync removed or refused
        // otherwise), but its TDEF can still hold FK entries that earlier
        // builds left behind. Remove the partner entries that name it
        // while its TDEF is intact, so none is left naming a freed page
        // or, later, the unrelated table that reuses it.
        foreach (long tdefPage in deletedPages)
        {
            await relationships.RemovePartnerLinksAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        }

        foreach (long parentTdefPage in deletedPages)
        {
            await complexColumns.DropComplexChildrenForTableAsync(parentTdefPage, (page, definition, token) => this.ReclaimTableStoragePagesAsync(page, definition, includeTDefRoot: true, token), cancellationToken).ConfigureAwait(false);
        }

        await catalogWriter.DeleteAceRowsForObjectIdsAsync(deletedPages, cancellationToken).ConfigureAwait(false);

        foreach (long tdefPage in deletedPages)
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

        await longValueEncoder.DeallocateLongValuesAsync(longValueRoots, cancellationToken, tdefPage).ConfigureAwait(false);

        if (firstTdefPage is not null)
        {
            await this.CollectUnreferencedMapStorageAsync(tdefPage, pagesToFree, cancellationToken).ConfigureAwait(false);
        }

        foreach (long pageNumber in pagesToFree)
        {
            if (pageNumber > 2)
            {
                await pageAllocator.DeallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask CollectUnreferencedMapStorageAsync(long tdefPage, SortedSet<long> pagesToFree, CancellationToken cancellationToken)
    {
        byte[] definition = await tableDefs.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false)
            ?? throw new JetCorruptDataException("The removed table definition is unreadable.");
        TDefImage image = TDefCodec.Parse(format, definition)
            ?? throw new JetCorruptDataException("The removed table definition is malformed.");
        var pointers = new HashSet<UsageMapPointer>();
        _ = UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer owned);
        _ = UsageMap.TryReadPointer(definition, format.TDef.FreePages, out UsageMapPointer free);
        pointers.Add(owned);
        pointers.Add(free);
        foreach (TDefIndexDescriptor index in image.RealIndexes)
        {
            if (UsageMap.TryReadPointer(definition, index.Slot.FirstDpOffset - 4, out UsageMapPointer pointer))
            {
                pointers.Add(pointer);
            }

            // A map can share storage or members with another object. Only
            // its independently owned index pages are candidates for release.
            try
            {
                UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, tableDefs, catalogRows, pager, tdefPage, index.Slot.FirstDpOffset - 4, cancellationToken).ConfigureAwait(false);
                foreach (long number in proof.Pages)
                {
                    if (proof.OtherMappedPages.Contains(number))
                    {
                        continue;
                    }

                    byte[] page = await pager.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (page[0] is Constants.PageTypes.IndexLeaf or Constants.PageTypes.IndexIntermediate && Ri32(page, 4) == tdefPage)
                        {
                            pagesToFree.Add(number);
                        }
                    }
                    finally
                    {
                        PageBuffers.Return(page);
                    }
                }
            }
            catch (JetCorruptDataException)
            {
                // Retain index storage when unrelated descriptors prevent a proof.
            }
        }

        if (LongValueUsageMaps.TryRead(format, definition, out List<LongValueUsageMap> longValues, out _))
        {
            foreach (LongValueUsageMap maps in longValues)
            {
                pointers.Add(maps.Owned);
                pointers.Add(maps.Free);
            }
        }

        var storage = new SortedSet<long>();
        foreach (UsageMapPointer pointer in pointers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (pointer.PageNumber <= 2 || pointer.PageNumber >= pager.PageCount)
            {
                continue;
            }

            byte[] page = await pager.ReadPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] == Constants.PageTypes.Data && Ri32(page, format.DataPage.TDefOff) == 0
                    && UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound bound))
                {
                    storage.Add(pointer.PageNumber);
                    await UsageMap.CollectReferenceBitmapPagesAsync(page, bound, pager.PageCount, pager.ReadPageAsync, PageBuffers.Return, storage, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        pagesToFree.UnionWith(await UsageMapOwnership.FindUnreferencedStoragePagesAsync(format, tableDefs, catalogRows, pager, storage, tdefPage, cancellationToken).ConfigureAwait(false));
    }
}
