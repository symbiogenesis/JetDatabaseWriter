namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Discovers all local tables, hydrates calculated columns, audits stored indexes,
/// regenerates damaged user-table indexes from verified active rows, evaluates table counter repairs,
/// detects unassigned physical rows, and classifies source pages into a <see cref="CompactReclamationPlan"/>.
/// </summary>
internal static class CompactDatabasePlanner
{
    /// <summary>
    /// Plans a complete database compaction and page-reclamation pass over an immutable decrypted page source.
    /// </summary>
    /// <param name="source">The caller-owned immutable decrypted page source.</param>
    /// <param name="format">The verified native format of <paramref name="source"/>.</param>
    /// <param name="budget">The discovery and traversal budget.</param>
    /// <param name="activeTableBudget">The active-table row corroboration budget.</param>
    /// <param name="cancellationToken">Cancels discovery, index auditing, and page classification.</param>
    /// <param name="salvage">Optional pre-scan data-page salvage and quarantine evidence.</param>
    /// <param name="rebuildDamagedIndexes">Whether damaged user-table real indexes may be regenerated from verified active rows.</param>
    /// <param name="repairTableCounters">Whether out-of-sync user-table <c>TDef</c> counters are audited and repaired.</param>
    /// <returns>The complete table build plan, reclamation plan, counts, per-table summaries, and bounded diagnostics.</returns>
    /// <exception cref="JetCorruptDataException">The source database geometry, catalog, or unrecoverable index topology is corrupt.</exception>
    /// <exception cref="JetLimitationException">A discovery, index, or working-storage budget is exceeded.</exception>
    internal static async ValueTask<CompactDatabasePlannerResult> PlanAsync(
        IPageSource source,
        JetFormat format,
        CompactDiscoveryBudget budget,
        CompactActiveTableBudget activeTableBudget,
        CancellationToken cancellationToken,
        CompactDataPageSalvageResult? salvage = null,
        bool rebuildDamagedIndexes = true,
        bool repairTableCounters = true)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(activeTableBudget, nameof(activeTableBudget));
        source.ThrowIfDisposedOrCancelled(cancellationToken);
        if (source.PageSize != format.PageSize || source.PageCount < 3)
        {
            throw Corrupt(0, "The source database geometry does not match its verified native format.");
        }

        var session = new CompactDiscoverySession(source, format, budget);
        var diagnostics = new List<AccessCompactDiagnostic>();
        var quarantineRecords = new List<AccessCompactQuarantineRecord>();
        var salvageQuarantinedPageNumbers = new HashSet<long>();
        var tableNames = new Dictionary<long, string>
        {
            [2] = Constants.SystemTableNames.Objects,
        };
        if (salvage is not null)
        {
            foreach (KeyValuePair<long, string> pair in salvage.TableNames)
            {
                tableNames[pair.Key] = pair.Value;
            }

            foreach (AccessCompactDiagnostic diag in salvage.Diagnostics)
            {
                AddDiagnostic(diagnostics, budget.MaxDiagnostics, diag);
            }

            foreach (AccessCompactQuarantineRecord qRec in salvage.QuarantineRecords)
            {
                quarantineRecords.Add(qRec);
                _ = salvageQuarantinedPageNumbers.Add(qRec.PageNumber);
            }
        }

        var localDefinitions = new Dictionary<long, TDefImage>();
        var recoveredCatalogRows = new List<CompactCatalogRecovery.RecoveredRow>();
        var tableOwnedPages = new Dictionary<long, HashSet<long>>();
        var globalFreePages = new HashSet<long>();
        var opaquePages = new HashSet<long>();
        var unresolvedPages = new HashSet<long>();
        int catalogObjectCount = 0;

        int schemaBytes = checked((int)Math.Min(int.MaxValue, budget.MaxSchemaBytes));
        using var tableDefs = new TableDefReader(source, format, cacheResults: false, budget.MaxDefinitionBytes, schemaBytes);
        await CompactStorageInventory.ScanAsync(
            source,
            format,
            tableDefs,
            new()
            {
                Accounting = session.Accounting,
                MaxPages = budget.MaxPages,
                MaxObjects = checked((int)Math.Min(int.MaxValue, budget.MaxCatalogObjects)),
                MaxReferences = budget.MaxReferences,
                MaxDiagnostics = checked((int)Math.Min(int.MaxValue, budget.MaxDiagnostics)),
                MaxSchemaBytes = schemaBytes,
                MaxPageReads = budget.MaxPageReads,
                MaxMapSteps = budget.MaxMapSteps,
                MaxRecords = budget.MaxRecords,
                MaxWorkingBytes = budget.MaxWorkingBytes,
            },
            (record, token) =>
            {
                token.ThrowIfCancellationRequested();
                if (record.Kind == "page" && record.Role == "opaque-page" && record.PageNumber is long opaquePage)
                {
                    _ = opaquePages.Add(opaquePage);
                }
                else if (record.Kind == "diagnostic")
                {
                    if (record.PageNumber is long diagPage
                        && salvageQuarantinedPageNumbers.Contains(diagPage)
                        && record.Detail == "An unrecognized page type remains opaque and pinned.")
                    {
                        return default;
                    }

                    AccessCompactDiagnosticCategory category = record.Detail == "An unrecognized page type remains opaque and pinned."
                        ? AccessCompactDiagnosticCategory.OpaquePage
                        : AccessCompactDiagnosticCategory.CatalogUncertainty;
                    AddDiagnostic(
                        diagnostics,
                        budget.MaxDiagnostics,
                        new(
                            "inventory-diagnostic",
                            record.PageNumber,
                            record.OwnerTDefPage,
                            record.TargetRowIndex,
                            record.Detail ?? "Storage inventory diagnostic.",
                            category));
                }
                else if (record.Kind == "unresolved-reference")
                {
                    if (record.PageNumber is long srcPage && srcPage >= 0 && srcPage < source.PageCount)
                    {
                        _ = unresolvedPages.Add(srcPage);
                    }

                    if (record.TargetPageNumber is long tgtPage && tgtPage >= 0 && tgtPage < source.PageCount)
                    {
                        _ = unresolvedPages.Add(tgtPage);
                    }

                    AddDiagnostic(
                        diagnostics,
                        budget.MaxDiagnostics,
                        new(
                            "unresolved-reference",
                            record.PageNumber,
                            record.OwnerTDefPage,
                            record.TargetRowIndex,
                            $"Unresolved {record.Role} reference to page {record.TargetPageNumber}.",
                            AccessCompactDiagnosticCategory.UnassignedStorage));
                }
                else if (record.Kind == "reference")
                {
                    if (record.Role == "global-free-member" && record.TargetPageNumber is long freePage)
                    {
                        _ = globalFreePages.Add(freePage);
                    }
                    else if (record.Role == "table-owned-member" && record.OwnerTDefPage is long tableOwner && record.TargetPageNumber is long ownedPage)
                    {
                        if (!tableOwnedPages.TryGetValue(tableOwner, out HashSet<long>? ownedSet))
                        {
                            ownedSet = [];
                            tableOwnedPages[tableOwner] = ownedSet;
                        }

                        _ = ownedSet.Add(ownedPage);
                    }
                }
                else if (record.Kind == "catalog-object")
                {
                    catalogObjectCount++;
                    if (record.Role == "local-table" && record.OwnerTDefPage is long localOwner && !string.IsNullOrEmpty(record.Name))
                    {
                        tableNames[localOwner] = record.Name;
                    }
                }

                return default;
            },
            cancellationToken,
            (owner, image, token) =>
            {
                token.ThrowIfCancellationRequested();
                localDefinitions[owner] = image;
                return default;
            },
            (recovered, token) =>
            {
                token.ThrowIfCancellationRequested();
                recoveredCatalogRows.Add(recovered);
                AddDiagnostic(
                    diagnostics,
                    budget.MaxDiagnostics,
                    new(
                        "catalog-recovered-row",
                        recovered.Identity.PageNumber,
                        2,
                        recovered.Identity.RowIndex,
                        $"Recovered catalog row Id={recovered.Identity.Id} ('{recovered.Identity.Name}') from {recovered.IdentityKey} index.",
                        AccessCompactDiagnosticCategory.CatalogUncertainty));
                return default;
            }).ConfigureAwait(false);

        if (!localDefinitions.TryGetValue(2, out TDefImage? catalogDefinition))
        {
            throw Corrupt(2, "The source database inventory did not discover the MSysObjects catalog definition.");
        }

        var orderedOwners = new List<long>(localDefinitions.Count) { 2 };
        foreach (long owner in localDefinitions.Keys)
        {
            if (owner != 2)
            {
                orderedOwners.Add(owner);
            }
        }

        orderedOwners.Sort(1, orderedOwners.Count - 1, Comparer<long>.Default);

        var pinnedTDefPages = new HashSet<long> { 2 };
        foreach (long owner in orderedOwners)
        {
            LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(owner, cancellationToken).ConfigureAwait(false);
            foreach (long pageNumber in chain.PageNumbers)
            {
                _ = pinnedTDefPages.Add(pageNumber);
            }
        }

        var buildTables = new List<CompactPinnedDatabaseBuildTable>(orderedOwners.Count);
        var tableSummaries = new List<AccessCompactTableSummary>(orderedOwners.Count);
        var counterEvaluations = new List<CompactTableCounterEvaluation>(orderedOwners.Count);
        long totalActiveRows = 0;
        long totalUnassignedRows = 0;
        int totalRegeneratedIndexes = 0;
        int repairedCounterTableCount = 0;

        await session.WithCatalogRecoveryAsync(
            async token =>
            {
                using var hydrator = new CompactCalculatedColumnHydrator(
                    format,
                    budget,
                    session.Accounting,
                    session.ScanRowsAsync,
                    session.AuditLongValueAsync);
                hydrator.SetCatalog(catalogDefinition);

                long indexCoordinates = Math.Min(int.MaxValue, budget.MaxIndexPages);
                long mapCoordinates = Math.Min(int.MaxValue, budget.MaxObjects);
                using var indexOwnership = new CompactIndexOwnershipAudit(
                    source.PageCount,
                    new()
                    {
                        Accounting = session.Accounting,
                        MaxTrackedPages = checked((int)Math.Min(int.MaxValue, indexCoordinates + mapCoordinates)),
                        MaxMapRows = checked((int)Math.Min(int.MaxValue, budget.MaxObjects)),
                        MaxRootDescriptors = budget.MaxObjects,
                        MaxReferences = budget.MaxReferences,
                        MaxPageReads = budget.MaxPageReads,
                        MaxMapSteps = budget.MaxMapSteps,
                        MaxMemberships = budget.MaxReferences,
                        MaxGraphSteps = budget.MaxGraphSteps,
                        MaxRecords = budget.MaxRecords,
                        MaxWorkingBytes = budget.MaxWorkingBytes,
                    });

                var auditedRoots = new Dictionary<(long Owner, long Root), long>();

                foreach (long owner in orderedOwners)
                {
                    token.ThrowIfCancellationRequested();
                    TDefImage definition = localDefinitions[owner];
                    string tableName = tableNames.GetValueOrDefault(owner, $"Table_{owner}");
                    IReadOnlyList<ColumnInfo>? hydratedColumns = null;
                    await hydrator.WithColumnsAsync(
                        owner,
                        definition,
                        (cols, _) =>
                        {
                            hydratedColumns = cols is null ? null : [.. cols];
                            return default;
                        },
                        token).ConfigureAwait(false);

                    int? activeIndexOrdinal = null;
                    long tableActiveRows = 0;
                    long tableUnassignedRows = 0;
                    List<(RowLocation Location, byte[] Payload)>? retainedRowsForTable = null;
                    HashSet<(long Page, int Row)>? activeBookmarks = null;

                    if (owner == 2)
                    {
                        long catalogRows = 0;
                        await session.ScanRowsAsync(
                            2,
                            (_, _, _) =>
                            {
                                catalogRows++;
                                return default;
                            },
                            token).ConfigureAwait(false);
                        tableActiveRows = catalogRows;
                        totalActiveRows = checked(totalActiveRows + catalogRows);
                    }
                    else
                    {
                        tableOwnedPages.TryGetValue(owner, out HashSet<long>? mappedPages);
                        var mappedRowList = new List<(RowLocation Location, byte[] Payload)>();
                        var unmappedRowList = new List<(RowLocation Location, byte[] Payload)>();
                        await session.ScanRowsAsync(
                            owner,
                            (location, payload, _) =>
                            {
                                bool isMapped = mappedPages?.Contains(location.PageNumber) == true
                                    && mappedPages.Contains(location.DataPageNumber);
                                if (isMapped)
                                {
                                    mappedRowList.Add((location, payload.ToArray()));
                                }
                                else
                                {
                                    unmappedRowList.Add((location, payload.ToArray()));
                                }

                                return default;
                            },
                            token).ConfigureAwait(false);

                        long mappedRows = mappedRowList.Count;
                        long unmappedRows = unmappedRowList.Count;
                        if (unmappedRows > 0)
                        {
                            activeIndexOrdinal = await SelectActiveCoveringIndexAsync(
                                source,
                                format,
                                owner,
                                tableName,
                                definition,
                                activeTableBudget with { Accounting = session.Accounting },
                                diagnostics,
                                budget.MaxDiagnostics,
                                token).ConfigureAwait(false);
                            if (activeIndexOrdinal is not null)
                            {
                                tableActiveRows = mappedRows;
                                totalActiveRows = checked(totalActiveRows + mappedRows);
                                retainedRowsForTable = mappedRowList;
                                foreach ((RowLocation loc, byte[] rawRow) in unmappedRowList)
                                {
                                    quarantineRecords.Add(
                                        new AccessCompactQuarantineRecord(
                                            "quarantined-unassigned-row",
                                            loc.PageNumber,
                                            owner,
                                            tableName,
                                            loc.RowIndex,
                                            "Physical row lies outside the corroborated active table usage map and was excluded by covering-index verification.",
                                            CompactDataPageSalvager.ComputeSha256Hex(rawRow),
                                            Convert.ToBase64String(rawRow)));
                                }
                            }
                            else
                            {
                                foreach ((RowLocation location, byte[] rawRow) in unmappedRowList)
                                {
                                    AddDiagnostic(
                                        diagnostics,
                                        budget.MaxDiagnostics,
                                        new(
                                            "unassigned-table-row",
                                            location.PageNumber,
                                            owner,
                                            location.RowIndex,
                                            $"Physical row {location.PageNumber}:{location.RowIndex} on table owner {owner} lies outside the table's owned usage map and has no supported covering index to corroborate exclusion; all rows and source pages remain pinned.",
                                            AccessCompactDiagnosticCategory.UnassignedTableRow,
                                            tableName));
                                    quarantineRecords.Add(
                                        new AccessCompactQuarantineRecord(
                                            "quarantined-unassigned-row",
                                            location.PageNumber,
                                            owner,
                                            tableName,
                                            location.RowIndex,
                                            "Unmapped physical row has no covering index to corroborate exclusion.",
                                            CompactDataPageSalvager.ComputeSha256Hex(rawRow),
                                            Convert.ToBase64String(rawRow)));
                                }

                                tableActiveRows = checked(mappedRows + unmappedRows);
                                totalActiveRows = checked(totalActiveRows + tableActiveRows);
                                mappedRowList.AddRange(unmappedRowList);
                                retainedRowsForTable = mappedRowList;
                            }

                            tableUnassignedRows = unmappedRows;
                            totalUnassignedRows = checked(totalUnassignedRows + unmappedRows);
                        }
                        else
                        {
                            tableActiveRows = mappedRows;
                            totalActiveRows = checked(totalActiveRows + mappedRows);
                            retainedRowsForTable = mappedRowList;
                        }

                        activeBookmarks = [];
                        foreach ((RowLocation loc, byte[] _) in retainedRowsForTable)
                        {
                            _ = activeBookmarks.Add((loc.PageNumber, loc.RowIndex));
                        }
                    }

                    byte[] definitionBytes = definition.CopyBytes();
                    var indexes = new CompactPinnedTableIndexSource[definition.RealIndexes.Count];
                    int tableRegeneratedIndexes = 0;
                    bool forceRegenerateTable = salvage?.ForceRegenerateOwners.Contains(owner) == true;

                    for (int slot = 0; slot < definition.RealIndexes.Count; slot++)
                    {
                        token.ThrowIfCancellationRequested();
                        TDefIndexDescriptor descriptor = definition.RealIndexes[slot];
                        if (!UsageMap.TryReadPointer(definitionBytes, descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer ownedPages))
                        {
                            throw Corrupt(owner, "A physical index descriptor has a truncated ownership-map pointer.");
                        }

                        if (descriptor.RootPage == 0 || ownedPages == default)
                        {
                            throw Corrupt(owner, "A physical index descriptor is missing its root page or ownership-map pointer.");
                        }

                        if (owner == 2)
                        {
                            await indexOwnership.DeclareDescriptorAsync(owner, descriptor.RootPage, ownedPages, slot, IgnoreOwnershipAsync, token).ConfigureAwait(false);
                            await indexOwnership.InspectMapAsync(source, format, owner, descriptor.RootPage, ownedPages, IgnoreOwnershipAsync, token).ConfigureAwait(false);

                            if (!auditedRoots.TryGetValue((owner, descriptor.RootPage), out long firstLeafPage))
                            {
                                firstLeafPage = await AuditIndexRootAndFindFirstLeafAsync(
                                    source,
                                    format,
                                    owner,
                                    descriptor.RootPage,
                                    budget,
                                    session.Accounting,
                                    indexOwnership,
                                    token).ConfigureAwait(false);
                                auditedRoots.Add((owner, descriptor.RootPage), firstLeafPage);
                            }

                            indexes[slot] = new(
                                descriptor.RootPage,
                                ownedPages,
                                StreamLeafEntriesAsync(
                                    source,
                                    format,
                                    owner,
                                    firstLeafPage,
                                    checked((int)Math.Min(int.MaxValue, budget.MaxIndexPages)),
                                    budget.MaxIndexEntries,
                                    token));
                            continue;
                        }

                        bool ignoreNulls = (descriptor.Slot.Flags & Constants.TableDefinition.IgnoreNullsIndexFlag) != 0;
                        bool enforceBookmarkCount = !ignoreNulls && !CompactDataPageSalvager.IsSystemTableName(tableName);
                        Exception? indexError = null;
                        long userFirstLeafPage = 0;

                        if (!forceRegenerateTable)
                        {
                            try
                            {
                                userFirstLeafPage = await PreAuditUserIndexAsync(
                                    source,
                                    format,
                                    owner,
                                    descriptor.RootPage,
                                    ownedPages,
                                    slot,
                                    budget,
                                    session.Accounting,
                                    activeBookmarks,
                                    enforceBookmarkCount,
                                    token).ConfigureAwait(false);
                            }
                            catch (JetCorruptDataException ex)
                            {
                                indexError = ex;
                            }
                        }

                        if (!forceRegenerateTable && indexError is null)
                        {
                            await indexOwnership.DeclareDescriptorAsync(owner, descriptor.RootPage, ownedPages, slot, IgnoreOwnershipAsync, token).ConfigureAwait(false);
                            await indexOwnership.InspectMapAsync(source, format, owner, descriptor.RootPage, ownedPages, IgnoreOwnershipAsync, token).ConfigureAwait(false);
                            if (!auditedRoots.TryGetValue((owner, descriptor.RootPage), out long firstLeaf))
                            {
                                firstLeaf = await AuditIndexRootAndFindFirstLeafAsync(
                                    source,
                                    format,
                                    owner,
                                    descriptor.RootPage,
                                    budget,
                                    session.Accounting,
                                    indexOwnership,
                                    token).ConfigureAwait(false);
                                auditedRoots.Add((owner, descriptor.RootPage), firstLeaf);
                            }

                            _ = userFirstLeafPage;
                            indexes[slot] = new(
                                descriptor.RootPage,
                                ownedPages,
                                StreamLeafEntriesAsync(
                                    source,
                                    format,
                                    owner,
                                    firstLeaf,
                                    checked((int)Math.Min(int.MaxValue, budget.MaxIndexPages)),
                                    budget.MaxIndexEntries,
                                    token));
                        }
                        else
                        {
                            if (!rebuildDamagedIndexes || !CompactIndexRowAudit.SupportsDescriptor(definition, slot) || retainedRowsForTable is null)
                            {
                                throw indexError ?? Corrupt(descriptor.RootPage, $"Damaged index slot {slot} on table '{tableName}' (owner {owner}) cannot be regenerated.");
                            }

                            IAsyncEnumerable<IndexEntry> regenerated = await CompactIndexKeyRegenerator.RegenerateAsync(
                                format,
                                owner,
                                definition,
                                slot,
                                retainedRowsForTable,
                                activeTableBudget.MaxKeyBytes,
                                session.Accounting,
                                CompactIndexRowAudit.HasMemoColumn(definition, slot) ? session : null,
                                budget,
                                token).ConfigureAwait(false);
                            indexes[slot] = new(descriptor.RootPage, ownedPages, regenerated);
                            tableRegeneratedIndexes++;
                            totalRegeneratedIndexes++;
                            string reason = indexError?.Message ?? "Quarantined data page required index key regeneration.";
                            AddDiagnostic(
                                diagnostics,
                                budget.MaxDiagnostics,
                                new(
                                    "regenerated-index",
                                    descriptor.RootPage,
                                    owner,
                                    null,
                                    $"Regenerated damaged physical index slot {slot} (root page {descriptor.RootPage}) on table '{tableName}' (owner {owner}) from {retainedRowsForTable.Count} verified active row(s): {reason}",
                                    AccessCompactDiagnosticCategory.RegeneratedIndex,
                                    tableName));
                        }
                    }

                    CompactTableCounterEvaluation counterEval = await CompactTableCounterRepairer.EvaluateAsync(
                        format,
                        owner,
                        tableName,
                        definition,
                        tableActiveRows,
                        retainedRowsForTable,
                        repairTableCounters,
                        token).ConfigureAwait(false);
                    counterEvaluations.Add(counterEval);
                    if (counterEval.CountersRepaired)
                    {
                        repairedCounterTableCount++;
                    }

                    if (counterEval.Diagnostic is not null)
                    {
                        AddDiagnostic(diagnostics, budget.MaxDiagnostics, counterEval.Diagnostic);
                    }

                    long tableSalvagedRows = salvage?.SalvagedRowsByOwner.GetValueOrDefault(owner, 0) ?? 0;
                    int tableQuarantinedPages = salvage?.QuarantinedPagesByOwner.GetValueOrDefault(owner, 0) ?? 0;
                    tableSummaries.Add(
                        new AccessCompactTableSummary(
                            owner,
                            tableName,
                            tableActiveRows,
                            tableUnassignedRows,
                            definition.RealIndexes.Count,
                            tableRegeneratedIndexes,
                            tableSalvagedRows,
                            tableQuarantinedPages,
                            counterEval.DeclaredRowCount,
                            counterEval.RepairedRowCount,
                            counterEval.DeclaredAutoNumber,
                            counterEval.RepairedAutoNumber,
                            counterEval.CountersRepaired));

                    buildTables.Add(new(owner, definition, indexes, hydratedColumns, activeIndexOrdinal));
                }

                await indexOwnership.CompleteAsync(IgnoreOwnershipAsync, token).ConfigureAwait(false);
                return true;
            },
            cancellationToken).ConfigureAwait(false);

        var pinnedPages = new HashSet<long> { 0, 1, 2 };
        foreach (long pageNumber in pinnedTDefPages)
        {
            _ = pinnedPages.Add(pageNumber);
        }

        foreach (long pageNumber in opaquePages)
        {
            _ = pinnedPages.Add(pageNumber);
        }

        foreach (long pageNumber in unresolvedPages)
        {
            _ = pinnedPages.Add(pageNumber);
        }

        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (pinnedPages.Contains(pageNumber))
            {
                continue;
            }

            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                byte pageType = page[0];
                uint owner = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(4, 4));
                bool isGlobalFree = globalFreePages.Contains(pageNumber);
                if (pageType == Constants.PageTypes.TableDefinition)
                {
                    if (!isGlobalFree)
                    {
                        _ = pinnedPages.Add(pageNumber);
                        AddDiagnostic(
                            diagnostics,
                            budget.MaxDiagnostics,
                            new(
                                "unassigned-tdef-page",
                                pageNumber,
                                pageNumber,
                                null,
                                $"Table definition page {pageNumber} is marked in use in the global usage map but is not referenced by any active catalog table.",
                                AccessCompactDiagnosticCategory.UnassignedStorage));
                    }
                }
                else if (pageType is Constants.PageTypes.Data or Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf)
                {
                    if (!isGlobalFree && owner != 0 && owner != 0x4C41564C && !localDefinitions.ContainsKey(owner))
                    {
                        _ = pinnedPages.Add(pageNumber);
                        AddDiagnostic(
                            diagnostics,
                            budget.MaxDiagnostics,
                            new(
                                "unassigned-owner-page",
                                pageNumber,
                                owner,
                                null,
                                $"Page {pageNumber} (type 0x{pageType:X2}) is marked in use in the global usage map for unknown owner {owner}.",
                                AccessCompactDiagnosticCategory.UnassignedStorage));
                    }
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        bool hasBlockingDiagnostics = false;
        foreach (AccessCompactDiagnostic diag in diagnostics)
        {
            if (diag.IsBlocking)
            {
                hasBlockingDiagnostics = true;
                break;
            }
        }

        bool isComplete = !hasBlockingDiagnostics && recoveredCatalogRows.Count == 0 && totalUnassignedRows == 0;
        CompactReclamationPlan reclamation;
        if (!isComplete)
        {
            reclamation = new CompactReclamationPlan(
                source.PageCount,
                source.PageCount,
                new HashSet<long>(),
                isComplete: false,
                reclamationAuthorized: false);
        }
        else
        {
            long maxPinned = 2;
            foreach (long pageNumber in pinnedPages)
            {
                if (pageNumber > maxPinned)
                {
                    maxPinned = pageNumber;
                }
            }

            long retainedPrefixPages = maxPinned + 1;
            var scrubbedPrefixPages = new HashSet<long>();
            for (long pageNumber = 3; pageNumber < retainedPrefixPages; pageNumber++)
            {
                if (!pinnedPages.Contains(pageNumber))
                {
                    _ = scrubbedPrefixPages.Add(pageNumber);
                }
            }

            reclamation = new CompactReclamationPlan(
                source.PageCount,
                retainedPrefixPages,
                scrubbedPrefixPages,
                isComplete: true,
                reclamationAuthorized: true);
        }

        return new(
            buildTables,
            reclamation,
            catalogObjectCount,
            totalActiveRows,
            recoveredCatalogRows.Count,
            totalUnassignedRows,
            diagnostics,
            tableSummaries,
            counterEvaluations,
            quarantineRecords,
            totalRegeneratedIndexes,
            salvage?.TotalSalvagedRows ?? 0,
            repairedCounterTableCount);
    }

    private static async ValueTask<long> PreAuditUserIndexAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        long rootPage,
        UsageMapPointer ownedPages,
        int slot,
        CompactDiscoveryBudget budget,
        CompactDiscoveryAccounting accounting,
        HashSet<(long Page, int Row)>? activeBookmarks,
        bool enforceBookmarkCount,
        CancellationToken cancellationToken)
    {
        long indexCoordinates = Math.Min(int.MaxValue, budget.MaxIndexPages);
        long mapCoordinates = Math.Min(int.MaxValue, budget.MaxObjects);
        using var singleOwnership = new CompactIndexOwnershipAudit(
            source.PageCount,
            new()
            {
                Accounting = accounting,
                MaxTrackedPages = checked((int)Math.Min(int.MaxValue, indexCoordinates + mapCoordinates)),
                MaxMapRows = checked((int)Math.Min(int.MaxValue, budget.MaxObjects)),
                MaxRootDescriptors = budget.MaxObjects,
                MaxReferences = budget.MaxReferences,
                MaxPageReads = budget.MaxPageReads,
                MaxMapSteps = budget.MaxMapSteps,
                MaxMemberships = budget.MaxReferences,
                MaxGraphSteps = budget.MaxGraphSteps,
                MaxRecords = budget.MaxRecords,
                MaxWorkingBytes = budget.MaxWorkingBytes,
            });

        await singleOwnership.DeclareDescriptorAsync(owner, rootPage, ownedPages, slot, IgnoreOwnershipAsync, cancellationToken).ConfigureAwait(false);
        await singleOwnership.InspectMapAsync(source, format, owner, rootPage, ownedPages, IgnoreOwnershipAsync, cancellationToken).ConfigureAwait(false);

        var leafPages = new HashSet<long>();
        var pagesWithPrevious = new HashSet<long>();
        HashSet<(long Page, int Row)>? observedBookmarks = activeBookmarks is not null ? [] : null;
        long leafEntryCount = 0;
        bool leafChainOrdered = false;

        await CompactIndexAudit.ScanAsync(
            source,
            format,
            owner,
            rootPage,
            new()
            {
                Accounting = accounting,
                MaxTreePages = checked((int)Math.Min(int.MaxValue, budget.MaxIndexPages)),
                MaxEntries = budget.MaxIndexEntries,
                MaxCanonicalBytes = budget.MaxIndexCanonicalBytes,
                MaxReferences = budget.MaxReferences,
                MaxGraphSteps = budget.MaxGraphSteps,
                MaxRecords = budget.MaxRecords,
                MaxWorkingBytes = budget.MaxWorkingBytes,
            },
            async (record, token) =>
            {
                await singleOwnership.ObserveAsync(record, IgnoreOwnershipAsync, token).ConfigureAwait(false);
                if (record.Kind == "index-page" && record.PageType == Constants.PageTypes.IndexLeaf)
                {
                    _ = leafPages.Add(record.PageNumber);
                }
                else if (record.Kind == "index-reference" && record.Role == "previous")
                {
                    _ = pagesWithPrevious.Add(record.PageNumber);
                }
                else if (record.Kind == "leaf-entry")
                {
                    leafEntryCount++;
                    if (activeBookmarks is not null
                        && record.TargetPageNumber is long tgtPage
                        && record.TargetRowIndex is byte tgtRow)
                    {
                        if (!activeBookmarks.Contains((tgtPage, tgtRow)) || !observedBookmarks!.Add((tgtPage, tgtRow)))
                        {
                            throw Corrupt(record.PageNumber, $"Stored index leaf entry on page {record.PageNumber} references non-active or duplicate row bookmark {tgtPage}:{tgtRow}.");
                        }
                    }
                }
                else if (record.Kind == "index-terminal")
                {
                    leafChainOrdered = record.NavigableGraphAcyclic
                        && record.EntryEdgeFencesValidated
                        && record.ObservedLeafChainOrdered
                        && record.ObservedKeyRoutingValidated
                        && record.ObservedChildMaximumSeparatorsValidated;
                }
            },
            cancellationToken).ConfigureAwait(false);

        if (!leafChainOrdered)
        {
            throw Corrupt(rootPage, "A stored index B-tree failed structural, key-routing, or leaf-chain ordering verification.");
        }

        if (enforceBookmarkCount && activeBookmarks is not null && leafEntryCount != activeBookmarks.Count)
        {
            throw Corrupt(rootPage, $"Stored index leaf entry count ({leafEntryCount}) disagrees with active row count ({activeBookmarks.Count}).");
        }

        await singleOwnership.CompleteAsync(IgnoreOwnershipAsync, cancellationToken).ConfigureAwait(false);

        long firstLeafPage = 0;
        foreach (long leafPage in leafPages)
        {
            if (!pagesWithPrevious.Contains(leafPage))
            {
                if (firstLeafPage != 0)
                {
                    throw Corrupt(rootPage, "The stored index B-tree has multiple head leaf pages.");
                }

                firstLeafPage = leafPage;
            }
        }

        if (firstLeafPage == 0)
        {
            throw Corrupt(rootPage, "The stored index B-tree has no head leaf page.");
        }

        return firstLeafPage;
    }

    private static async ValueTask<long> AuditIndexRootAndFindFirstLeafAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        long rootPage,
        CompactDiscoveryBudget budget,
        CompactDiscoveryAccounting accounting,
        CompactIndexOwnershipAudit indexOwnership,
        CancellationToken cancellationToken)
    {
        var leafPages = new HashSet<long>();
        var pagesWithPrevious = new HashSet<long>();
        bool leafChainOrdered = false;
        await CompactIndexAudit.ScanAsync(
            source,
            format,
            owner,
            rootPage,
            new()
            {
                Accounting = accounting,
                MaxTreePages = checked((int)Math.Min(int.MaxValue, budget.MaxIndexPages)),
                MaxEntries = budget.MaxIndexEntries,
                MaxCanonicalBytes = budget.MaxIndexCanonicalBytes,
                MaxReferences = budget.MaxReferences,
                MaxGraphSteps = budget.MaxGraphSteps,
                MaxRecords = budget.MaxRecords,
                MaxWorkingBytes = budget.MaxWorkingBytes,
            },
            async (record, token) =>
            {
                await indexOwnership.ObserveAsync(record, IgnoreOwnershipAsync, token).ConfigureAwait(false);
                if (record.Kind == "index-page" && record.PageType == Constants.PageTypes.IndexLeaf)
                {
                    _ = leafPages.Add(record.PageNumber);
                }
                else if (record.Kind == "index-reference" && record.Role == "previous")
                {
                    _ = pagesWithPrevious.Add(record.PageNumber);
                }
                else if (record.Kind == "index-terminal")
                {
                    leafChainOrdered = record.NavigableGraphAcyclic
                        && record.EntryEdgeFencesValidated
                        && record.ObservedLeafChainOrdered
                        && record.ObservedKeyRoutingValidated
                        && record.ObservedChildMaximumSeparatorsValidated;
                }
            },
            cancellationToken).ConfigureAwait(false);

        if (!leafChainOrdered)
        {
            throw Corrupt(rootPage, "A stored index B-tree failed structural, key-routing, or leaf-chain ordering verification.");
        }

        long firstLeafPage = 0;
        foreach (long leafPage in leafPages)
        {
            if (!pagesWithPrevious.Contains(leafPage))
            {
                if (firstLeafPage != 0)
                {
                    throw Corrupt(rootPage, "The stored index B-tree has multiple head leaf pages.");
                }

                firstLeafPage = leafPage;
            }
        }

        if (firstLeafPage == 0)
        {
            throw Corrupt(rootPage, "The stored index B-tree has no head leaf page.");
        }

        return firstLeafPage;
    }

    private static async ValueTask<int?> SelectActiveCoveringIndexAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        string tableName,
        TDefImage definition,
        CompactActiveTableBudget activeTableBudget,
        List<AccessCompactDiagnostic> diagnostics,
        long maxDiagnostics,
        CancellationToken cancellationToken)
    {
        Exception? lastError = null;
        for (int ordinal = 0; ordinal < definition.RealIndexes.Count; ordinal++)
        {
            if (!CompactIndexRowAudit.SupportsDescriptor(definition, ordinal))
            {
                continue;
            }

            var candidateDiagnostics = new List<AccessCompactDiagnostic>();
            try
            {
                using CompactActiveTableRows active = await CompactActiveTableRows.ValidateAsync(
                    source,
                    format,
                    owner,
                    definition,
                    ordinal,
                    activeTableBudget,
                    (record, token) =>
                    {
                        token.ThrowIfCancellationRequested();
                        if (record.Kind == "unassigned-row" && record.Location is RowLocation location)
                        {
                            candidateDiagnostics.Add(
                                new(
                                    "unassigned-table-row",
                                    location.PageNumber,
                                    owner,
                                    location.RowIndex,
                                    $"Physical row {location.PageNumber}:{location.RowIndex} on table owner {owner} lies outside the corroborated active table storage and remains pinned.",
                                    AccessCompactDiagnosticCategory.UnassignedTableRow,
                                    tableName));
                        }

                        return default;
                    },
                    cancellationToken).ConfigureAwait(false);
                _ = await active.ValidateIndexesAsync(cancellationToken).ConfigureAwait(false);
                foreach (AccessCompactDiagnostic diagnostic in candidateDiagnostics)
                {
                    AddDiagnostic(diagnostics, maxDiagnostics, diagnostic);
                }

                return ordinal;
            }
            catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException)
            {
                lastError = ex;
            }
        }

        if (lastError is not null)
        {
            throw lastError;
        }

        return null;
    }

    private static async IAsyncEnumerable<IndexEntry> StreamLeafEntriesAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        long firstLeafPage,
        int maxLeafPages,
        long maxEntries,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long current = firstLeafPage;
        long previous = 0;
        int visitedPages = 0;
        long emittedEntries = 0;
        while (current != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (++visitedPages > maxLeafPages)
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Stored index leaf chain exceeds MaxIndexPages.");
            }

            List<IndexEntry> pageEntries;
            long next;
            byte[] page = await source.ReadUncachedPageAsync(current, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page.Length < format.PageSize
                    || !IndexPageCodec.IsLeaf(page)
                    || BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(4, 4)) != owner
                    || IndexPageCodec.ReadPrevPage(format.IndexPage, page) != previous
                    || !IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out pageEntries))
                {
                    throw Corrupt(current, "A stored index leaf page changed or could not be decoded during streaming.");
                }

                next = IndexPageCodec.ReadNextPage(format.IndexPage, page);
            }
            finally
            {
                PageBuffers.Return(page);
            }

            foreach (IndexEntry entry in pageEntries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (++emittedEntries > maxEntries)
                {
                    throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Stored index leaf entries exceed MaxIndexEntries.");
                }

                yield return entry;
            }

            previous = current;
            current = next;
        }
    }

    private static void AddDiagnostic(List<AccessCompactDiagnostic> diagnostics, long maxDiagnostics, AccessCompactDiagnostic diagnostic)
    {
        if (diagnostics.Count < maxDiagnostics)
        {
            diagnostics.Add(diagnostic);
        }
    }

    private static ValueTask IgnoreOwnershipAsync(CompactIndexOwnershipRecord record, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return default;
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptPage, reason, new JetErrorInfo { PageNumber = page, Reason = reason });
}
