namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Inspects user-table data pages before compact planning when partial recovery is enabled,
/// repairing verified data pages whose page-type header byte or owner TDEF pointer was damaged,
/// salvaging healthy rows from partially corrupt data pages at individual row-slot granularity,
/// isolating unrecoverable data pages or corrupt row slots into quarantine records, and optionally
/// quarantining orphaned complex flat-table child rows and orphaned enforced foreign-key child rows.
/// </summary>
internal static class CompactDataPageSalvager
{
    internal static async ValueTask<CompactDataPageSalvageResult> InspectAndCreateOverlayAsync(
        IPageSource source,
        JetFormat format,
        CompactDiscoveryBudget budget,
        bool allowDataSalvage,
        CancellationToken cancellationToken,
        bool quarantineOrphanedChildRows = true)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();

        var tableNames = new Dictionary<long, string>
        {
            [2] = Constants.SystemTableNames.Objects,
        };
        var definitions = new Dictionary<long, TDefImage>();
        int maxSchemaBytes = checked((int)Math.Min(int.MaxValue, budget.MaxSchemaBytes));
        using var tableDefs = new TableDefReader(source, format, cacheResults: false, budget.MaxDefinitionBytes, maxSchemaBytes);
        using var ownedData = new OwnedDataPages(source, format, cacheResults: false);
        var catalogReader = new CatalogRowReader(format, tableDefs, ownedData);

        TDefImage? catalogDef;
        try
        {
            catalogDef = await tableDefs.ReadImageAsync(2, maxSchemaBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException)
        {
            return EmptyResult(source, tableNames);
        }

        if (catalogDef is null || catalogDef.Issues != TDefParseIssues.None)
        {
            return EmptyResult(source, tableNames);
        }

        definitions[2] = catalogDef;
        try
        {
            var traversalBudget = new CompactTraversalBudget
            {
                MaxPages = budget.MaxPages,
                MaxObjects = checked((int)Math.Min(int.MaxValue, budget.MaxCatalogObjects)),
                MaxReferences = budget.MaxReferences,
                MaxDiagnostics = checked((int)Math.Min(int.MaxValue, budget.MaxDiagnostics)),
                MaxSchemaBytes = maxSchemaBytes,
                MaxPageReads = budget.MaxPageReads,
                MaxMapSteps = budget.MaxMapSteps,
                MaxRecords = budget.MaxRecords,
                MaxWorkingBytes = budget.MaxWorkingBytes,
            };
            CompactCatalogRecovery.Result physical = await CompactCatalogRecovery.ScanPhysicalAsync(
                source,
                format,
                catalogDef,
                catalogReader,
                traversalBudget,
                budget.MaxReferences,
                budget.MaxMapSteps,
                cancellationToken).ConfigureAwait(false);
            foreach (CompactCatalogRecovery.RecoveredRow recovered in physical.Rows)
            {
                CatalogRow row = recovered.Identity;
                bool isLocalTable = row.ObjectType == Constants.SystemObjects.UserTableType
                    || (row.ObjectType == Constants.SystemObjects.LinkedOdbcType && row.Id > 0);
                if (isLocalTable && row.TDefPage >= 2 && row.TDefPage < source.PageCount)
                {
                    tableNames[row.TDefPage] = row.Name;
                }
            }
        }
        catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException)
        {
            if (!allowDataSalvage)
            {
                return EmptyResult(source, tableNames);
            }
        }

        if (!allowDataSalvage)
        {
            return EmptyResult(source, tableNames);
        }

        var knownNonDataTablePages = new HashSet<long> { 0, 1, 2 };
        var tableOwnedPages = new Dictionary<long, HashSet<long>>();
        var tableOwnedMapPointers = new Dictionary<long, UsageMapPointer>();
        var tableFreeMapPointers = new Dictionary<long, UsageMapPointer>();

        // Read Page 1 global usage map so we know which pages are marked globally free.
        HashSet<long> globalFreePages = await ReadUsageMapMembersSafeAsync(
            source,
            format,
            new UsageMapPointer(0, 1),
            knownNonDataTablePages,
            cancellationToken).ConfigureAwait(false);

        foreach (KeyValuePair<long, string> pair in tableNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long owner = pair.Key;
            if (owner < 3 || owner >= source.PageCount)
            {
                continue;
            }

            try
            {
                LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(owner, cancellationToken).ConfigureAwait(false);
                foreach (long tdefPage in chain.PageNumbers)
                {
                    _ = knownNonDataTablePages.Add(tdefPage);
                }

                TDefImage? image = await tableDefs.ReadImageAsync(owner, maxSchemaBytes, cancellationToken).ConfigureAwait(false);
                if (image is null || image.Issues != TDefParseIssues.None)
                {
                    continue;
                }

                definitions[owner] = image;
                if (UsageMap.TryReadPointer(chain.Bytes, format.TDef.UsedPages, out UsageMapPointer ownedPtr) && ownedPtr.PageNumber > 0)
                {
                    tableOwnedMapPointers[owner] = ownedPtr;
                    _ = knownNonDataTablePages.Add(ownedPtr.PageNumber);
                    tableOwnedPages[owner] = await ReadUsageMapMembersSafeAsync(source, format, ownedPtr, knownNonDataTablePages, cancellationToken).ConfigureAwait(false);
                }

                if (UsageMap.TryReadPointer(chain.Bytes, format.TDef.FreePages, out UsageMapPointer freePtr) && freePtr.PageNumber > 0)
                {
                    tableFreeMapPointers[owner] = freePtr;
                    _ = knownNonDataTablePages.Add(freePtr.PageNumber);
                    _ = await ReadUsageMapMembersSafeAsync(source, format, freePtr, knownNonDataTablePages, cancellationToken).ConfigureAwait(false);
                }

                foreach (TDefIndexDescriptor index in image.RealIndexes)
                {
                    if (index.RootPage > 0)
                    {
                        _ = knownNonDataTablePages.Add(index.RootPage);
                    }

                    if (UsageMap.TryReadPointer(chain.Bytes, index.Slot.FirstDpOffset - 4, out UsageMapPointer idxPtr) && idxPtr.PageNumber > 0)
                    {
                        _ = knownNonDataTablePages.Add(idxPtr.PageNumber);
                        HashSet<long> idxMembers = await ReadUsageMapMembersSafeAsync(source, format, idxPtr, knownNonDataTablePages, cancellationToken).ConfigureAwait(false);
                        foreach (long idxPage in idxMembers)
                        {
                            _ = knownNonDataTablePages.Add(idxPage);
                        }
                    }
                }

                if (LongValueUsageMaps.TryRead(format, chain.Bytes, out List<LongValueUsageMap> lvalMaps, out _))
                {
                    foreach (LongValueUsageMap lvalMap in lvalMaps)
                    {
                        if (UsageMap.TryReadPointer(chain.Bytes, lvalMap.OwnedOffset, out UsageMapPointer lvOwned) && lvOwned.PageNumber > 0)
                        {
                            _ = knownNonDataTablePages.Add(lvOwned.PageNumber);
                            HashSet<long> lvPages = await ReadUsageMapMembersSafeAsync(source, format, lvOwned, knownNonDataTablePages, cancellationToken).ConfigureAwait(false);
                            foreach (long lvPage in lvPages)
                            {
                                _ = knownNonDataTablePages.Add(lvPage);
                            }
                        }

                        if (UsageMap.TryReadPointer(chain.Bytes, lvalMap.FreeOffset, out UsageMapPointer lvFree) && lvFree.PageNumber > 0)
                        {
                            _ = knownNonDataTablePages.Add(lvFree.PageNumber);
                            _ = await ReadUsageMapMembersSafeAsync(source, format, lvFree, knownNonDataTablePages, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException)
            {
                // Leave unreadable TDEF chains to standard inventory diagnostics.
            }
        }

        // Harvest surviving index leaf bookmarks (DataPage -> Set of owners) and identify LVAL/system pages.
        var indexBookmarkOwners = new Dictionary<long, HashSet<long>>();
        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                byte pageType = page[0];
                uint rawOwner = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4));
                if (rawOwner == 0x4C41564CU
                    || rawOwner == 2
                    || (definitions.ContainsKey(rawOwner) && IsSystemTableName(tableNames.GetValueOrDefault(rawOwner))))
                {
                    _ = knownNonDataTablePages.Add(pageNumber);
                    continue;
                }

                if (pageType == Constants.PageTypes.IndexLeaf)
                {
                    uint idxOwner = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4));
                    if (idxOwner >= 3
                        && definitions.ContainsKey(idxOwner)
                        && !IsSystemTableName(tableNames.GetValueOrDefault(idxOwner))
                        && IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out List<IndexEntry> leafEntries))
                    {
                        foreach (IndexEntry entry in leafEntries)
                        {
                            if (entry.DataPage >= 3 && entry.DataPage < source.PageCount)
                            {
                                if (!indexBookmarkOwners.TryGetValue(entry.DataPage, out HashSet<long>? owners))
                                {
                                    owners = [];
                                    indexBookmarkOwners[entry.DataPage] = owners;
                                }

                                _ = owners.Add(idxOwner);
                            }
                        }
                    }
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        var overlayPages = new Dictionary<long, byte[]>();
        var diagnostics = new List<AccessCompactDiagnostic>();
        var quarantineRecords = new List<AccessCompactQuarantineRecord>();
        var salvagedRowsByOwner = new Dictionary<long, long>();
        var quarantinedPagesByOwner = new Dictionary<long, int>();
        var quarantinedRowsByOwner = new Dictionary<long, long>();
        var forceRegenerateOwners = new HashSet<long>();
        long totalSalvagedRows = 0;
        long totalQuarantinedRows = 0;

        // Pass 1: Detect damaged-header DATA pages (e.g. 0x7F -> 0x01) AND damaged-owner DATA pages (corrupted TDef pointer at offset 4..7).
        // Also mask the owner header on damaged known non-DATA table pages (such as corrupted index pages) so raw-row scanning does not abort before index regeneration runs.
        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                byte pageType = page[0];
                uint rawOwner = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4));

                // Subcase 1A: Known user-table owner with unrecognized page-type byte.
                if (!IsRecognizedPageType(pageType)
                    && rawOwner >= 3
                    && rawOwner != 0x4C41564CU
                    && definitions.TryGetValue(rawOwner, out TDefImage? knownDef))
                {
                    string tableName = tableNames.GetValueOrDefault(rawOwner, $"Table_{rawOwner}");
                    if (IsSystemTableName(tableName))
                    {
                        continue;
                    }

                    if (knownNonDataTablePages.Contains(pageNumber))
                    {
                        byte[] maskedNonData = new byte[format.PageSize];
                        Buffer.BlockCopy(page, 0, maskedNonData, 0, format.PageSize);
                        BinaryPrimitives.WriteUInt32LittleEndian(maskedNonData.AsSpan(format.DataPage.TDefOff, 4), 0U);
                        overlayPages[pageNumber] = maskedNonData;
                        _ = forceRegenerateOwners.Add(rawOwner);
                        continue;
                    }

                    byte[] candidate = new byte[format.PageSize];
                    Buffer.BlockCopy(page, 0, candidate, 0, format.PageSize);
                    candidate[0] = Constants.PageTypes.Data;
                    candidate[1] = 1;
                    if (await TryValidateCandidateDataPageAsync(source, format, rawOwner, knownDef, pageNumber, candidate, requirePositiveLiveRows: true, cancellationToken).ConfigureAwait(false) is { Valid: true, LiveRowCount: > 0 } check)
                    {
                        overlayPages[pageNumber] = candidate;
                        salvagedRowsByOwner.TryGetValue(rawOwner, out long existingRows);
                        salvagedRowsByOwner[rawOwner] = existingRows + check.LiveRowCount;
                        totalSalvagedRows += check.LiveRowCount;
                        if (diagnostics.Count < budget.MaxDiagnostics)
                        {
                            diagnostics.Add(
                                new AccessCompactDiagnostic(
                                    "salvaged-data-page-header",
                                    pageNumber,
                                    rawOwner,
                                    null,
                                    $"Repaired damaged page-type header byte (0x{pageType:X2} -> 0x01) on DATA page {pageNumber} for table '{tableName}' (owner {rawOwner}) after verifying its row directory and {check.LiveRowCount} live row(s).",
                                    AccessCompactDiagnosticCategory.SalvagedDataPage,
                                    tableName));
                        }
                    }

                    continue;
                }

                // Subcase 1B: Damaged/corrupted 4-byte TDef owner header on a DATA page (with either pageType == 0x01 or unrecognized pageType).
                if (knownNonDataTablePages.Contains(pageNumber))
                {
                    continue;
                }

                bool isUnknownOwner = rawOwner < 3 || (rawOwner != 0x4C41564CU && !definitions.ContainsKey(rawOwner));
                if (!isUnknownOwner || (pageType != Constants.PageTypes.Data && IsRecognizedPageType(pageType)))
                {
                    continue;
                }

                (long? reconstructedOwner, string? evidenceSource, byte[]? repairedOwnerPage, int reconstructedLiveRows) = await TryReconstructDataPageOwnerAsync(
                    source,
                    format,
                    pageNumber,
                    page,
                    definitions,
                    tableNames,
                    tableOwnedPages,
                    indexBookmarkOwners,
                    globalFreePages,
                    cancellationToken).ConfigureAwait(false);
                if (reconstructedOwner is long resolvedOwner && repairedOwnerPage is not null && reconstructedLiveRows > 0)
                {
                    string tableName = tableNames.GetValueOrDefault(resolvedOwner, $"Table_{resolvedOwner}");
                    overlayPages[pageNumber] = repairedOwnerPage;
                    salvagedRowsByOwner.TryGetValue(resolvedOwner, out long existingRows);
                    salvagedRowsByOwner[resolvedOwner] = existingRows + reconstructedLiveRows;
                    totalSalvagedRows += reconstructedLiveRows;

                    if (!tableOwnedPages.TryGetValue(resolvedOwner, out HashSet<long>? ownedSet))
                    {
                        ownedSet = [];
                        tableOwnedPages[resolvedOwner] = ownedSet;
                    }

                    if (ownedSet.Add(pageNumber) && tableOwnedMapPointers.TryGetValue(resolvedOwner, out UsageMapPointer ownedPtr))
                    {
                        await SetUsageMapBitInOverlayAsync(source, format, ownedPtr, pageNumber, isMarked: true, overlayPages, cancellationToken).ConfigureAwait(false);
                    }

                    if (diagnostics.Count < budget.MaxDiagnostics)
                    {
                        diagnostics.Add(
                            new AccessCompactDiagnostic(
                                "salvaged-data-page-owner",
                                pageNumber,
                                resolvedOwner,
                                null,
                                $"Reconstructed damaged TDEF owner header (0x{rawOwner:X8} -> {resolvedOwner}) on DATA page {pageNumber} for table '{tableName}' using {evidenceSource} evidence and verified {reconstructedLiveRows} live row(s).",
                                AccessCompactDiagnosticCategory.SalvagedDataPage,
                                tableName));
                    }
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        // Pass 2: Inspect user-table DATA pages for row-level corruption; salvage surviving rows on partially corrupt pages,
        // or quarantine genuinely unrecoverable DATA pages when zero rows can be rescued.
        IPageSource pass1Overlay = overlayPages.Count > 0 ? new OverlayPageSource(source, overlayPages) : source;
        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (knownNonDataTablePages.Contains(pageNumber))
            {
                continue;
            }

            byte[] page = await pass1Overlay.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                byte pageType = page[0];
                uint rawOwner = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4));
                long? matchedOwner = null;
                if ((pageType == Constants.PageTypes.Data || !IsRecognizedPageType(pageType))
                    && rawOwner >= 3
                    && rawOwner != 0x4C41564CU
                    && definitions.ContainsKey(rawOwner))
                {
                    matchedOwner = rawOwner;
                }
                else
                {
                    foreach (KeyValuePair<long, HashSet<long>> pair in tableOwnedPages)
                    {
                        if (pair.Value.Contains(pageNumber) && definitions.ContainsKey(pair.Key))
                        {
                            matchedOwner = pair.Key;
                            break;
                        }
                    }
                }

                if (matchedOwner is not long owner || !definitions.TryGetValue(owner, out TDefImage? def))
                {
                    continue;
                }

                string tableName = tableNames.GetValueOrDefault(owner, $"Table_{owner}");
                if (IsSystemTableName(tableName))
                {
                    continue;
                }

                (bool valid, int _, string? error) = await TryValidateCandidateDataPageAsync(
                    pass1Overlay,
                    format,
                    owner,
                    def,
                    pageNumber,
                    page,
                    requirePositiveLiveRows: false,
                    cancellationToken).ConfigureAwait(false);
                if (valid)
                {
                    continue;
                }

                // Attempt individual row-slot salvage before quarantining the whole page.
                PartialPageSalvageAttempt partial = await TrySalvagePartialDataPageAsync(
                    pass1Overlay,
                    format,
                    owner,
                    def,
                    pageNumber,
                    page,
                    cancellationToken).ConfigureAwait(false);
                if (partial.Salvaged && partial.RepairedPage is not null && partial.SurvivingLiveRows > 0)
                {
                    overlayPages[pageNumber] = partial.RepairedPage;
                    salvagedRowsByOwner.TryGetValue(owner, out long existingSalvaged);
                    salvagedRowsByOwner[owner] = existingSalvaged + partial.SurvivingLiveRows;
                    totalSalvagedRows += partial.SurvivingLiveRows;

                    quarantinedRowsByOwner.TryGetValue(owner, out long existingQuarantinedRows);
                    quarantinedRowsByOwner[owner] = existingQuarantinedRows + partial.CorruptSlots.Count;
                    totalQuarantinedRows += partial.CorruptSlots.Count;
                    _ = forceRegenerateOwners.Add(owner);

                    if (!tableOwnedPages.TryGetValue(owner, out HashSet<long>? ownedSet))
                    {
                        ownedSet = [];
                        tableOwnedPages[owner] = ownedSet;
                    }

                    if (ownedSet.Add(pageNumber) && tableOwnedMapPointers.TryGetValue(owner, out UsageMapPointer ownedPtr))
                    {
                        await SetUsageMapBitInOverlayAsync(source, format, ownedPtr, pageNumber, isMarked: true, overlayPages, cancellationToken).ConfigureAwait(false);
                    }

                    foreach (CorruptSlotInfo corruptSlot in partial.CorruptSlots)
                    {
                        quarantineRecords.Add(
                            new AccessCompactQuarantineRecord(
                                "quarantined-corrupt-row",
                                pageNumber,
                                owner,
                                tableName,
                                corruptSlot.SlotIndex,
                                corruptSlot.Reason,
                                ComputeSha256Hex(corruptSlot.RawBytes),
                                Convert.ToBase64String(corruptSlot.RawBytes)));
                        if (diagnostics.Count < budget.MaxDiagnostics)
                        {
                            diagnostics.Add(
                                new AccessCompactDiagnostic(
                                    "quarantined-corrupt-row",
                                    pageNumber,
                                    owner,
                                    corruptSlot.SlotIndex,
                                    $"Isolated and quarantined corrupt row slot {corruptSlot.SlotIndex} on DATA page {pageNumber} for table '{tableName}' (owner {owner}): {corruptSlot.Reason}",
                                    AccessCompactDiagnosticCategory.QuarantinedRow,
                                    tableName));
                        }
                    }

                    if (diagnostics.Count < budget.MaxDiagnostics)
                    {
                        diagnostics.Add(
                            new AccessCompactDiagnostic(
                                "salvaged-partial-data-page",
                                pageNumber,
                                owner,
                                null,
                                $"Salvaged {partial.SurvivingLiveRows} verified live row(s) on partially damaged DATA page {pageNumber} for table '{tableName}' (owner {owner}) after isolating {partial.CorruptSlots.Count} corrupt row slot(s).",
                                AccessCompactDiagnosticCategory.SalvagedDataPage,
                                tableName));
                    }

                    continue;
                }

                string reason = error ?? "The data page row directory or field payload is corrupt.";
                string sha256 = ComputeSha256Hex(page.AsSpan(0, format.PageSize));
                string rawBase64 = Convert.ToBase64String(page, 0, format.PageSize);
                quarantineRecords.Add(
                    new AccessCompactQuarantineRecord(
                        "quarantined-data-page",
                        pageNumber,
                        owner,
                        tableName,
                        null,
                        reason,
                        sha256,
                        rawBase64));
                if (diagnostics.Count < budget.MaxDiagnostics)
                {
                    diagnostics.Add(
                        new AccessCompactDiagnostic(
                            "quarantined-data-page",
                            pageNumber,
                            owner,
                            null,
                            $"Isolated and quarantined unrecoverable DATA page {pageNumber} for table '{tableName}' (owner {owner}): {reason}",
                            AccessCompactDiagnosticCategory.QuarantinedDataPage,
                            tableName));
                }

                quarantinedPagesByOwner.TryGetValue(owner, out int qCount);
                quarantinedPagesByOwner[owner] = qCount + 1;
                _ = forceRegenerateOwners.Add(owner);

                // Mask page as an opaque pinned page with owner 0 so inventory pins it and table discovery skips it.
                byte[] masked = new byte[format.PageSize];
                Buffer.BlockCopy(page, 0, masked, 0, format.PageSize);
                masked[0] = 0x7F;
                BinaryPrimitives.WriteUInt32LittleEndian(masked.AsSpan(format.DataPage.TDefOff, 4), 0);
                overlayPages[pageNumber] = masked;

                if (tableOwnedMapPointers.TryGetValue(owner, out UsageMapPointer ownedMapPtr))
                {
                    await SetUsageMapBitInOverlayAsync(source, format, ownedMapPtr, pageNumber, isMarked: false, overlayPages, cancellationToken).ConfigureAwait(false);
                }

                if (tableFreeMapPointers.TryGetValue(owner, out UsageMapPointer freeMapPtr))
                {
                    await SetUsageMapBitInOverlayAsync(source, format, freeMapPtr, pageNumber, isMarked: false, overlayPages, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        // Pass 3: Quarantine orphaned complex flat-table child rows (MSysComplexColumns) and orphaned enforced foreign-key child rows.
        if (quarantineOrphanedChildRows)
        {
            long newlyQuarantinedOrphans = await QuarantineOrphanedComplexAndForeignKeyRowsAsync(
                source,
                format,
                budget,
                definitions,
                tableNames,
                overlayPages,
                diagnostics,
                quarantineRecords,
                quarantinedRowsByOwner,
                forceRegenerateOwners,
                cancellationToken).ConfigureAwait(false);
            totalQuarantinedRows += newlyQuarantinedOrphans;
        }

        IPageSource effectiveSource = overlayPages.Count > 0
            ? new OverlayPageSource(source, overlayPages)
            : source;

        return new CompactDataPageSalvageResult(
            effectiveSource,
            tableNames,
            diagnostics,
            quarantineRecords,
            salvagedRowsByOwner,
            quarantinedPagesByOwner,
            forceRegenerateOwners,
            totalSalvagedRows,
            quarantinedRowsByOwner,
            totalQuarantinedRows);
    }

    internal static bool IsSystemTableName(string? tableName)
        => !string.IsNullOrEmpty(tableName)
            && (tableName.StartsWith("MSys", StringComparison.OrdinalIgnoreCase)
                || tableName.StartsWith("USys", StringComparison.OrdinalIgnoreCase));

    internal static string ComputeSha256Hex(ReadOnlySpan<byte> data)
    {
#if NET8_0_OR_GREATER
        Span<byte> hash = stackalloc byte[32];
        _ = SHA256.HashData(data, hash);
        return Convert.ToHexString(hash);
#else
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hasher.AppendData(data);
        byte[] hash = hasher.GetHashAndReset();
        char[] chars = new char[hash.Length * 2];
        const string hex = "0123456789ABCDEF";
        for (int i = 0; i < hash.Length; i++)
        {
            chars[i * 2] = hex[hash[i] >> 4];
            chars[(i * 2) + 1] = hex[hash[i] & 0x0F];
        }

        return new string(chars);
#endif
    }

    private static CompactDataPageSalvageResult EmptyResult(IPageSource source, Dictionary<long, string> tableNames)
        => new(
            source,
            tableNames,
            [],
            [],
            new Dictionary<long, long>(),
            new Dictionary<long, int>(),
            [],
            0,
            new Dictionary<long, long>(),
            0);

    private static bool IsRecognizedPageType(byte pageType)
        => pageType is Constants.PageTypes.Data
            or Constants.PageTypes.TableDefinition
            or Constants.PageTypes.IndexIntermediate
            or Constants.PageTypes.IndexLeaf
            or Constants.PageTypes.UsageMap
            or Constants.PageTypes.Freed;

    private static async ValueTask<(long? Owner, string? Evidence, byte[]? RepairedPage, int LiveRows)> TryReconstructDataPageOwnerAsync(
        IPageSource source,
        JetFormat format,
        long pageNumber,
        byte[] page,
        Dictionary<long, TDefImage> definitions,
        Dictionary<long, string> tableNames,
        Dictionary<long, HashSet<long>> tableOwnedPages,
        Dictionary<long, HashSet<long>> indexBookmarkOwners,
        HashSet<long> globalFreePages,
        CancellationToken cancellationToken)
    {
        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
        int maxRows = (format.PageSize - format.DataPage.RowsStart) / 2;
        if (numRows <= 0 || numRows > maxRows || numRows > Constants.DataPage.MaxRowsPerPage)
        {
            return (null, null, null, 0);
        }

        // Tier 1: Unique table usage-map membership.
        long? mapCandidate = null;
        int mapMatches = 0;
        foreach (KeyValuePair<long, HashSet<long>> pair in tableOwnedPages)
        {
            if (pair.Key >= 3
                && definitions.ContainsKey(pair.Key)
                && !IsSystemTableName(tableNames.GetValueOrDefault(pair.Key))
                && pair.Value.Contains(pageNumber))
            {
                mapCandidate = pair.Key;
                mapMatches++;
            }
        }

        if (mapMatches == 1 && mapCandidate is long mapOwner && definitions.TryGetValue(mapOwner, out TDefImage? mapDef))
        {
            (bool ok, byte[]? repaired, int liveRows) = await TryBuildOwnerCandidatePageAsync(source, format, mapOwner, mapDef, pageNumber, page, requireSchemaSignature: false, cancellationToken).ConfigureAwait(false);
            if (ok && repaired is not null && liveRows > 0)
            {
                return (mapOwner, "usage-map", repaired, liveRows);
            }
        }

        // Tier 2: Unique surviving index leaf bookmark reference.
        if (indexBookmarkOwners.TryGetValue(pageNumber, out HashSet<long>? bookmarkOwners) && bookmarkOwners.Count == 1)
        {
            long bookmarkOwner = 0;
            foreach (long item in bookmarkOwners)
            {
                bookmarkOwner = item;
            }

            if (definitions.TryGetValue(bookmarkOwner, out TDefImage? bmDef))
            {
                (bool ok, byte[]? repaired, int liveRows) = await TryBuildOwnerCandidatePageAsync(source, format, bookmarkOwner, bmDef, pageNumber, page, requireSchemaSignature: false, cancellationToken).ConfigureAwait(false);
                if (ok && repaired is not null && liveRows > 0)
                {
                    return (bookmarkOwner, "index-bookmarks", repaired, liveRows);
                }
            }
        }

        // Tier 3: Unambiguous schema-signature match across user tables (for pages not marked globally free).
        if (globalFreePages.Contains(pageNumber))
        {
            return (null, null, null, 0);
        }

        long? schemaOwner = null;
        byte[]? schemaRepaired = null;
        int schemaLiveRows = 0;
        int schemaMatches = 0;
        foreach (KeyValuePair<long, TDefImage> pair in definitions)
        {
            if (pair.Key < 3 || IsSystemTableName(tableNames.GetValueOrDefault(pair.Key)))
            {
                continue;
            }

            (bool ok, byte[]? repaired, int liveRows) = await TryBuildOwnerCandidatePageAsync(source, format, pair.Key, pair.Value, pageNumber, page, requireSchemaSignature: true, cancellationToken).ConfigureAwait(false);
            if (ok && repaired is not null && liveRows > 0)
            {
                schemaOwner = pair.Key;
                schemaRepaired = repaired;
                schemaLiveRows = liveRows;
                schemaMatches++;
                if (schemaMatches > 1)
                {
                    return (null, null, null, 0);
                }
            }
        }

        return schemaMatches == 1 && schemaOwner is not null
            ? (schemaOwner, "schema-signature", schemaRepaired, schemaLiveRows)
            : (null, null, null, 0);
    }

    private static async ValueTask<(bool Valid, byte[]? RepairedPage, int LiveRows)> TryBuildOwnerCandidatePageAsync(
        IPageSource source,
        JetFormat format,
        long candidateOwner,
        TDefImage definition,
        long pageNumber,
        byte[] page,
        bool requireSchemaSignature,
        CancellationToken cancellationToken)
    {
        byte[] candidate = new byte[format.PageSize];
        Buffer.BlockCopy(page, 0, candidate, 0, format.PageSize);
        candidate[0] = Constants.PageTypes.Data;
        candidate[1] = 1;
        BinaryPrimitives.WriteUInt32LittleEndian(candidate.AsSpan(format.DataPage.TDefOff, 4), checked((uint)candidateOwner));

        (bool valid, int liveRowCount, string? _) = await TryValidateCandidateDataPageAsync(
            source,
            format,
            candidateOwner,
            definition,
            pageNumber,
            candidate,
            requirePositiveLiveRows: true,
            cancellationToken,
            requireStrictSchemaSignature: requireSchemaSignature).ConfigureAwait(false);
        return valid && liveRowCount > 0 ? (true, candidate, liveRowCount) : (false, null, 0);
    }

    private static async ValueTask<(bool Valid, int LiveRowCount, string? Error)> TryValidateCandidateDataPageAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        TDefImage definition,
        long pageNumber,
        byte[] candidatePage,
        bool requirePositiveLiveRows,
        CancellationToken cancellationToken,
        bool requireStrictSchemaSignature = false)
    {
        if (candidatePage.Length < format.PageSize
            || candidatePage[0] != Constants.PageTypes.Data
            || candidatePage[1] != 1
            || BinaryPrimitives.ReadUInt32LittleEndian(candidatePage.AsSpan(format.DataPage.TDefOff, 4)) != owner)
        {
            return (false, 0, "The page header or owner TDEF pointer is invalid.");
        }

        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(candidatePage.AsSpan(format.DataPage.NumRows, 2));
        int maxRows = (format.PageSize - format.DataPage.RowsStart) / 2;
        if (numRows < 0 || numRows > maxRows || numRows > Constants.DataPage.MaxRowsPerPage)
        {
            return (false, 0, $"The row directory count ({numRows}) exceeds physical page capacity.");
        }

        if (requirePositiveLiveRows && numRows == 0)
        {
            return (false, 0, "The candidate page has zero directory slots.");
        }

        int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(candidatePage.AsSpan(2, 2));
        int directoryEnd = format.DataPage.RowsStart + (numRows * 2);
        if (freeSpace > format.PageSize - format.DataPage.RowsStart)
        {
            return (false, 0, "The declared free space exceeds the physical payload capacity.");
        }

        int liveRowCount = 0;
        try
        {
            int[] rawSlots = new int[numRows];
            int[] sorted = new int[numRows];
            for (int slot = 0; slot < numRows; slot++)
            {
                int raw = BinaryPrimitives.ReadUInt16LittleEndian(candidatePage.AsSpan(format.DataPage.RowsStart + (slot * 2), 2));
                if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0)
                {
                    return (false, 0, $"Row slot {slot} has an unsupported directory flag.");
                }

                int start = raw & Constants.DataPage.RowOffsetMask;
                bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
                if ((!deleted && (start < directoryEnd || start >= format.PageSize))
                    || (deleted && start != 0 && (start < directoryEnd || start > format.PageSize)))
                {
                    return (false, 0, $"Row slot {slot} has an invalid stored offset {start}.");
                }

                rawSlots[slot] = raw;
                sorted[slot] = (start << 16) | slot;
            }

            Array.Sort(sorted, 0, numRows);
            int previousLiveStart = -1;
            for (int index = 0; index < numRows; index++)
            {
                int row = sorted[index] & 0xFFFF;
                if ((rawSlots[row] & Constants.DataPage.DeletedRowFlag) == 0)
                {
                    int start = sorted[index] >> 16;
                    if (start == previousLiveStart)
                    {
                        return (false, 0, $"Row slot {row} aliases another live row's stored payload.");
                    }

                    previousLiveStart = start;
                }
            }

            for (int slot = 0; slot < numRows; slot++)
            {
                int raw = rawSlots[slot];
                if ((raw & Constants.DataPage.DeletedRowFlag) != 0)
                {
                    continue;
                }

                int start = raw & Constants.DataPage.RowOffsetMask;
                int low = 0;
                int high = numRows;
                while (low < high)
                {
                    int middle = low + ((high - low) / 2);
                    if ((sorted[middle] >> 16) <= start)
                    {
                        low = middle + 1;
                    }
                    else
                    {
                        high = middle;
                    }
                }

                int end = low < numRows ? sorted[low] >> 16 : format.PageSize;
                if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
                {
                    if (end - start != 4)
                    {
                        return (false, 0, $"Overflow pointer slot {slot} has length {end - start} instead of 4.");
                    }

                    (bool overflowValid, string? overflowErr) = await TryValidateOverflowPointerAsync(
                        source,
                        format,
                        owner,
                        definition,
                        pageNumber,
                        slot,
                        candidatePage.AsMemory(start, 4),
                        cancellationToken).ConfigureAwait(false);
                    if (!overflowValid)
                    {
                        return (false, 0, overflowErr ?? $"Overflow pointer slot {slot} targets an invalid or corrupt payload.");
                    }

                    liveRowCount++;
                    continue;
                }

                liveRowCount++;
                ReadOnlyMemory<byte> rowBytes = candidatePage.AsMemory(start, end - start);
                var location = new RowLocation(pageNumber, slot, start, end - start);
                (bool rowValid, string? rowError) = await TryValidateInlineRowAsync(
                    source,
                    format,
                    definition,
                    location,
                    rowBytes,
                    requireStrictSchemaSignature,
                    cancellationToken).ConfigureAwait(false);
                if (!rowValid)
                {
                    return (false, 0, rowError);
                }
            }

            if (requirePositiveLiveRows && liveRowCount == 0)
            {
                return (false, 0, "The candidate page contains no live rows.");
            }

            return (true, liveRowCount, null);
        }
        catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException or ArgumentException)
        {
            return (false, 0, ex.Message);
        }
    }

    private static async ValueTask<(bool Valid, string? Error)> TryValidateInlineRowAsync(
        IPageSource source,
        JetFormat format,
        TDefImage definition,
        RowLocation location,
        ReadOnlyMemory<byte> rowBytes,
        bool requireStrictSchemaSignature,
        CancellationToken cancellationToken)
    {
        try
        {
            if (requireStrictSchemaSignature)
            {
                int rowNumCols = format.IsJet3 ? rowBytes.Span[0] : BinaryPrimitives.ReadUInt16LittleEndian(rowBytes.Span);
                if (rowNumCols <= 0 || rowNumCols > definition.Header.ColumnCount)
                {
                    return (false, "Row column count does not match the candidate table definition.");
                }

                int expectedFixedBytes = 0;
                foreach (ColumnInfo col in definition.Columns)
                {
                    if (col.IsFixed && col.Type != ColumnType.BooleanType && col.ColNum < rowNumCols)
                    {
                        int colEnd = col.FixedOff + col.Size;
                        if (colEnd > expectedFixedBytes)
                        {
                            expectedFixedBytes = colEnd;
                        }
                    }
                }

                if (definition.Header.VariableColumnCount == 0)
                {
                    int expectedRowLength = format.RowFields.NumCols + expectedFixedBytes + JetFormat.GetNullMaskSizeBytes(rowNumCols);
                    if (rowBytes.Length != expectedRowLength)
                    {
                        return (false, "Fixed-only row length does not match the candidate table definition.");
                    }
                }
            }

            string? fieldError = null;
            int? firstVarOffset = null;
            await CompactRawFieldReader.ScanAsync(
                format,
                definition,
                location,
                rowBytes,
                async (field, token) =>
                {
                    if (!field.Column.IsFixed && field.Column.VarIdx == 0 && field.Kind is CompactRawFieldKind.Stored or CompactRawFieldKind.Null)
                    {
                        firstVarOffset = field.RowOffset;
                    }

                    if (field.Kind != CompactRawFieldKind.Stored)
                    {
                        return;
                    }

                    if (field.IsLongValue)
                    {
                        if (!LongValueDescriptor.TryRead(field.RawBytes.Span, out LongValueDescriptor desc))
                        {
                            fieldError = $"Row slot {location.RowIndex} has a truncated long-value descriptor on column '{field.Column.Name}'.";
                            return;
                        }

                        if (desc.IsInline)
                        {
                            if (desc.Length < 0 || field.RawBytes.Length - Constants.LongValue.HeaderSize < desc.Length)
                            {
                                fieldError = $"Row slot {location.RowIndex} has an inline long-value length ({desc.Length}) exceeding its slot size.";
                            }
                        }
                        else if (desc.FirstDp != 0)
                        {
                            bool lvalOk = await TryValidateExternalLongValueAsync(source, format, desc, token).ConfigureAwait(false);
                            if (!lvalOk)
                            {
                                fieldError = $"Row slot {location.RowIndex} references a corrupt external long-value chain at bookmark 0x{desc.FirstDp:X8}.";
                            }
                        }
                    }
                    else if (!field.Column.IsFixed && !field.Column.IsCalculated && field.Column.Size > 0 && field.RawBytes.Length > field.Column.Size)
                    {
                        fieldError = $"Row slot {location.RowIndex} variable column '{field.Column.Name}' length ({field.RawBytes.Length}) exceeds declared size ({field.Column.Size}).";
                    }
                },
                cancellationToken).ConfigureAwait(false);

            if (fieldError is not null)
            {
                return (false, fieldError);
            }

            if (requireStrictSchemaSignature && firstVarOffset is int varStart)
            {
                int rowNumCols = format.IsJet3 ? rowBytes.Span[0] : BinaryPrimitives.ReadUInt16LittleEndian(rowBytes.Span);
                int expectedFixedBytes = 0;
                foreach (ColumnInfo col in definition.Columns)
                {
                    if (col.IsFixed && col.Type != ColumnType.BooleanType && col.ColNum < rowNumCols)
                    {
                        int colEnd = col.FixedOff + col.Size;
                        if (colEnd > expectedFixedBytes)
                        {
                            expectedFixedBytes = colEnd;
                        }
                    }
                }

                if (varStart != format.RowFields.NumCols + expectedFixedBytes)
                {
                    return (false, "First variable column offset does not match the candidate table's fixed-area size.");
                }
            }

            return (true, null);
        }
        catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException or ArgumentException)
        {
            return (false, ex.Message);
        }
    }

    private static async ValueTask<bool> TryValidateExternalLongValueAsync(
        IPageSource source,
        JetFormat format,
        LongValueDescriptor desc,
        CancellationToken cancellationToken)
    {
        if (desc.Length < 0)
        {
            return false;
        }

        long currentPage = desc.FirstDp >> 8;
        int currentRow = (int)(desc.FirstDp & 0xFFU);
        int remaining = desc.Length;
        int hops = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (++hops > 4096 || currentPage < 3 || currentPage >= source.PageCount)
            {
                return false;
            }

            byte[] lvalPage = await source.ReadUncachedPageAsync(currentPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (lvalPage[0] != Constants.PageTypes.Data
                    || BinaryPrimitives.ReadUInt32LittleEndian(lvalPage.AsSpan(4, 4)) != 0x4C41564CU
                    || !UsageMap.TryGetRowBound(lvalPage, format.DataPage, format.PageSize, currentRow, out RowBound bound)
                    || bound.RowSize <= 0)
                {
                    return false;
                }

                if (desc.IsSinglePage)
                {
                    return bound.RowSize >= remaining;
                }

                if (bound.RowSize < 4)
                {
                    return false;
                }

                int chunkPayload = bound.RowSize - 4;
                remaining = Math.Max(0, remaining - chunkPayload);
                uint nextBookmark = BinaryPrimitives.ReadUInt32LittleEndian(lvalPage.AsSpan(bound.RowStart, 4));
                if (nextBookmark == 0)
                {
                    return remaining == 0;
                }

                currentPage = nextBookmark >> 8;
                currentRow = (int)(nextBookmark & 0xFFU);
            }
            finally
            {
                PageBuffers.Return(lvalPage);
            }
        }
    }

    private static async ValueTask<(bool Valid, string? Error)> TryValidateOverflowPointerAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        TDefImage definition,
        long headerPageNumber,
        int headerSlot,
        ReadOnlyMemory<byte> pointerBytes,
        CancellationToken cancellationToken)
    {
        if (pointerBytes.Length != 4)
        {
            return (false, $"Overflow slot {headerSlot} is {pointerBytes.Length} bytes instead of 4.");
        }

        long currentPage = headerPageNumber;
        int currentRow = headerSlot;
        ReadOnlySpan<byte> initSpan = pointerBytes.Span;
        int nextRow = initSpan[0];
        long nextPage = initSpan[1] | (initSpan[2] << 8) | (initSpan[3] << 16);
        var seen = new HashSet<(long Page, int Row)> { (currentPage, currentRow) };

        for (int hop = 0; hop < Constants.DataPage.MaxOverflowHops; hop++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (nextPage < 3 || nextPage >= source.PageCount || !seen.Add((nextPage, nextRow)))
            {
                return (false, $"Overflow slot {headerSlot} targets invalid or cyclic coordinate {nextPage}:{nextRow}.");
            }

            byte[] targetPage = await source.ReadUncachedPageAsync(nextPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (targetPage[0] != Constants.PageTypes.Data
                    || BinaryPrimitives.ReadUInt32LittleEndian(targetPage.AsSpan(format.DataPage.TDefOff, 4)) != owner)
                {
                    return (false, $"Overflow target page {nextPage} is not a DATA page owned by {owner}.");
                }

                int targetNumRows = BinaryPrimitives.ReadUInt16LittleEndian(targetPage.AsSpan(format.DataPage.NumRows, 2));
                int maxRows = (format.PageSize - format.DataPage.RowsStart) / 2;
                if (targetNumRows <= 0 || targetNumRows > maxRows || targetNumRows > Constants.DataPage.MaxRowsPerPage || nextRow >= targetNumRows)
                {
                    return (false, $"Overflow target slot {nextPage}:{nextRow} lies outside directory count {targetNumRows}.");
                }

                int raw = BinaryPrimitives.ReadUInt16LittleEndian(targetPage.AsSpan(format.DataPage.RowsStart + (nextRow * 2), 2));
                if ((raw & Constants.DataPage.DeletedRowFlag) == 0)
                {
                    return (false, $"Overflow target slot {nextPage}:{nextRow} is not marked with DeletedRowFlag.");
                }

                if (!UsageMap.TryGetRowBound(targetPage, format.DataPage, format.PageSize, nextRow, out RowBound bound)
                    || bound.RowSize <= 0)
                {
                    return (false, $"Overflow target slot {nextPage}:{nextRow} has invalid slice bounds.");
                }

                if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
                {
                    if (bound.RowSize < 4)
                    {
                        return (false, $"Intermediate overflow slot {nextPage}:{nextRow} is shorter than 4 bytes.");
                    }

                    nextRow = targetPage[bound.RowStart];
                    nextPage = targetPage[bound.RowStart + 1] | (targetPage[bound.RowStart + 2] << 8) | (targetPage[bound.RowStart + 3] << 16);
                    continue;
                }

                var location = new RowLocation(headerPageNumber, headerSlot, bound.RowStart, bound.RowSize)
                {
                    DataPageNumber = nextPage,
                    DataRowIndex = nextRow,
                };
                return await TryValidateInlineRowAsync(
                    source,
                    format,
                    definition,
                    location,
                    targetPage.AsMemory(bound.RowStart, bound.RowSize),
                    requireStrictSchemaSignature: false,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                PageBuffers.Return(targetPage);
            }
        }

        return (false, $"Overflow slot {headerSlot} exceeds maximum overflow hops.");
    }

    private static async ValueTask<PartialPageSalvageAttempt> TrySalvagePartialDataPageAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        TDefImage definition,
        long pageNumber,
        byte[] page,
        CancellationToken cancellationToken)
    {
        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
        int maxRows = (format.PageSize - format.DataPage.RowsStart) / 2;
        if (numRows <= 0 || numRows > maxRows || numRows > Constants.DataPage.MaxRowsPerPage)
        {
            return new PartialPageSalvageAttempt(false, null, 0, []);
        }

        int directoryEnd = format.DataPage.RowsStart + (numRows * 2);
        byte[] repaired = new byte[format.PageSize];
        Buffer.BlockCopy(page, 0, repaired, 0, format.PageSize);
        repaired[0] = Constants.PageTypes.Data;
        repaired[1] = 1;
        BinaryPrimitives.WriteUInt32LittleEndian(repaired.AsSpan(format.DataPage.TDefOff, 4), checked((uint)owner));

        var corruptSlots = new List<CorruptSlotInfo>();
        int[] rawSlots = new int[numRows];
        bool[] slotQuarantined = new bool[numRows];

        // Step 1: Identify slots with invalid flags or out-of-bounds start offsets.
        for (int slot = 0; slot < numRows; slot++)
        {
            int raw = BinaryPrimitives.ReadUInt16LittleEndian(repaired.AsSpan(format.DataPage.RowsStart + (slot * 2), 2));
            bool hasBadFlags = (raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0;
            int start = raw & Constants.DataPage.RowOffsetMask;
            bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
            bool badOffset = (!deleted && (start < directoryEnd || start >= format.PageSize))
                || (deleted && start != 0 && (start < directoryEnd || start > format.PageSize));

            if (hasBadFlags || badOffset)
            {
                slotQuarantined[slot] = true;
                rawSlots[slot] = Constants.DataPage.DeletedRowFlag;
                BinaryPrimitives.WriteUInt16LittleEndian(repaired.AsSpan(format.DataPage.RowsStart + (slot * 2), 2), Constants.DataPage.DeletedRowFlag);
                if (!deleted || hasBadFlags)
                {
                    byte[] rawEntryBytes = new byte[2];
                    BinaryPrimitives.WriteUInt16LittleEndian(rawEntryBytes, checked((ushort)raw));
                    corruptSlots.Add(new CorruptSlotInfo(slot, $"Row slot {slot} has an invalid directory entry (0x{raw:X4}, offset {start}).", rawEntryBytes));
                }
            }
            else
            {
                rawSlots[slot] = raw;
            }
        }

        // Step 2: Sort valid offsets and detect live slots that alias an earlier live slot's start offset.
        int[] sorted = new int[numRows];
        for (int slot = 0; slot < numRows; slot++)
        {
            int start = rawSlots[slot] & Constants.DataPage.RowOffsetMask;
            sorted[slot] = (start << 16) | slot;
        }

        Array.Sort(sorted, 0, numRows);
        int previousLiveStart = -1;
        for (int index = 0; index < numRows; index++)
        {
            int slot = sorted[index] & 0xFFFF;
            if ((rawSlots[slot] & Constants.DataPage.DeletedRowFlag) == 0)
            {
                int start = sorted[index] >> 16;
                if (start == previousLiveStart)
                {
                    slotQuarantined[slot] = true;
                    rawSlots[slot] = Constants.DataPage.DeletedRowFlag;
                    BinaryPrimitives.WriteUInt16LittleEndian(repaired.AsSpan(format.DataPage.RowsStart + (slot * 2), 2), Constants.DataPage.DeletedRowFlag);
                    byte[] rawEntryBytes = new byte[2];
                    BinaryPrimitives.WriteUInt16LittleEndian(rawEntryBytes, checked((ushort)(start | (rawSlots[slot] & Constants.DataPage.NonLiveRowFlags))));
                    corruptSlots.Add(new CorruptSlotInfo(slot, $"Row slot {slot} aliases another live row's stored offset {start}.", rawEntryBytes));
                }
                else
                {
                    previousLiveStart = start;
                }
            }
        }

        // Re-sort after removing aliasing slots so remaining live slots have well-defined non-overlapping [start, end) slices.
        for (int slot = 0; slot < numRows; slot++)
        {
            int start = rawSlots[slot] & Constants.DataPage.RowOffsetMask;
            sorted[slot] = (start << 16) | slot;
        }

        Array.Sort(sorted, 0, numRows);

        // Step 3: Validate each remaining live slot's payload (or overflow pointer) individually.
        int survivingLiveRows = 0;
        for (int slot = 0; slot < numRows; slot++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int raw = rawSlots[slot];
            if ((raw & Constants.DataPage.DeletedRowFlag) != 0 || slotQuarantined[slot])
            {
                continue;
            }

            int start = raw & Constants.DataPage.RowOffsetMask;
            int low = 0;
            int high = numRows;
            while (low < high)
            {
                int middle = low + ((high - low) / 2);
                if ((sorted[middle] >> 16) <= start)
                {
                    low = middle + 1;
                }
                else
                {
                    high = middle;
                }
            }

            int end = low < numRows ? sorted[low] >> 16 : format.PageSize;
            int length = end - start;
            if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
            {
                (bool overflowOk, string? overflowErr) = length == 4
                    ? await TryValidateOverflowPointerAsync(source, format, owner, definition, pageNumber, slot, repaired.AsMemory(start, 4), cancellationToken).ConfigureAwait(false)
                    : (false, $"Overflow pointer slot {slot} has length {length} instead of 4.");
                if (!overflowOk)
                {
                    byte[] sliceCopy = repaired.AsSpan(start, Math.Max(0, length)).ToArray();
                    corruptSlots.Add(new CorruptSlotInfo(slot, overflowErr ?? $"Overflow pointer slot {slot} is corrupt.", sliceCopy));
                    ushort deletedEntry = checked((ushort)(Constants.DataPage.DeletedRowFlag | start));
                    rawSlots[slot] = deletedEntry;
                    BinaryPrimitives.WriteUInt16LittleEndian(repaired.AsSpan(format.DataPage.RowsStart + (slot * 2), 2), deletedEntry);
                    continue;
                }

                survivingLiveRows++;
                continue;
            }

            var location = new RowLocation(pageNumber, slot, start, length);
            (bool inlineOk, string? inlineErr) = await TryValidateInlineRowAsync(
                source,
                format,
                definition,
                location,
                repaired.AsMemory(start, length),
                requireStrictSchemaSignature: false,
                cancellationToken).ConfigureAwait(false);
            if (!inlineOk)
            {
                byte[] sliceCopy = repaired.AsSpan(start, length).ToArray();
                corruptSlots.Add(new CorruptSlotInfo(slot, inlineErr ?? $"Row slot {slot} field layout is corrupt.", sliceCopy));
                ushort deletedEntry = checked((ushort)(Constants.DataPage.DeletedRowFlag | start));
                rawSlots[slot] = deletedEntry;
                BinaryPrimitives.WriteUInt16LittleEndian(repaired.AsSpan(format.DataPage.RowsStart + (slot * 2), 2), deletedEntry);
                repaired.AsSpan(start, length).Clear();
                continue;
            }

            survivingLiveRows++;
        }

        if (survivingLiveRows <= 0)
        {
            return new PartialPageSalvageAttempt(false, null, 0, []);
        }

        // Repair freeSpace header at offset 2..3 if needed.
        int minStart = format.PageSize;
        for (int slot = 0; slot < numRows; slot++)
        {
            int start = rawSlots[slot] & Constants.DataPage.RowOffsetMask;
            if (start >= directoryEnd && start < minStart)
            {
                minStart = start;
            }
        }

        ushort computedFreeSpace = checked((ushort)Math.Max(0, minStart - directoryEnd));
        if (BinaryPrimitives.ReadUInt16LittleEndian(repaired.AsSpan(2, 2)) > format.PageSize - format.DataPage.RowsStart)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(repaired.AsSpan(2, 2), computedFreeSpace);
        }

        (bool finalValid, int verifiedLiveRows, string? _) = await TryValidateCandidateDataPageAsync(
            source,
            format,
            owner,
            definition,
            pageNumber,
            repaired,
            requirePositiveLiveRows: true,
            cancellationToken).ConfigureAwait(false);
        return finalValid && verifiedLiveRows > 0
            ? new PartialPageSalvageAttempt(true, repaired, verifiedLiveRows, corruptSlots)
            : new PartialPageSalvageAttempt(false, null, 0, []);
    }

    private static async ValueTask<long> QuarantineOrphanedComplexAndForeignKeyRowsAsync(
        IPageSource baseSource,
        JetFormat format,
        CompactDiscoveryBudget budget,
        Dictionary<long, TDefImage> definitions,
        Dictionary<long, string> tableNames,
        Dictionary<long, byte[]> overlayPages,
        List<AccessCompactDiagnostic> diagnostics,
        List<AccessCompactQuarantineRecord> quarantineRecords,
        Dictionary<long, long> quarantinedRowsByOwner,
        HashSet<long> forceRegenerateOwners,
        CancellationToken cancellationToken)
    {
        long totalOrphansQuarantined = 0;

        // 1. Orphaned Complex Flat-Table Child Rows (MSysComplexColumns)
        if (format.SupportsComplexColumns)
        {
            long complexCatalogOwner = 0;
            foreach (KeyValuePair<long, string> pair in tableNames)
            {
                if (string.Equals(pair.Value, Constants.SystemTableNames.ComplexColumns, StringComparison.OrdinalIgnoreCase))
                {
                    complexCatalogOwner = pair.Key;
                    break;
                }
            }

            if (complexCatalogOwner >= 3 && definitions.TryGetValue(complexCatalogOwner, out TDefImage? complexCatalogDef))
            {
                IPageSource currentOverlay = overlayPages.Count > 0 ? new OverlayPageSource(baseSource, overlayPages) : baseSource;
                Dictionary<(string ColumnName, int ComplexId), long> flatTablesByColumn = await ReadComplexFlatTableMappingsAsync(
                    currentOverlay,
                    format,
                    complexCatalogOwner,
                    complexCatalogDef,
                    cancellationToken).ConfigureAwait(false);

                foreach (KeyValuePair<long, TDefImage> pair in definitions)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    long parentOwner = pair.Key;
                    TDefImage parentDef = pair.Value;
                    string parentTableName = tableNames.GetValueOrDefault(parentOwner, $"Table_{parentOwner}");
                    if (parentOwner < 3 || IsSystemTableName(parentTableName) || parentTableName.StartsWith("f_", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    foreach (ColumnInfo col in parentDef.Columns)
                    {
                        if (col.Type != ColumnType.ComplexType)
                        {
                            continue;
                        }

                        long flatOwner = ResolveFlatTableOwner(flatTablesByColumn, col.Name, col.Misc);
                        if (flatOwner < 3 || !definitions.TryGetValue(flatOwner, out TDefImage? flatDef))
                        {
                            continue;
                        }

                        ColumnInfo? linkCol = FindComplexFlatLinkColumn(flatDef, col.Name);
                        if (linkCol is null)
                        {
                            continue;
                        }

                        IPageSource parentScanSource = overlayPages.Count > 0 ? new OverlayPageSource(baseSource, overlayPages) : baseSource;
                        HashSet<int> activeComplexRefs = await CollectActiveInt32ColumnValuesAsync(
                            parentScanSource,
                            format,
                            parentOwner,
                            parentDef,
                            col.ColNum,
                            cancellationToken).ConfigureAwait(false);

                        string flatTableName = tableNames.GetValueOrDefault(flatOwner, $"Table_{flatOwner}");
                        long orphans = await QuarantineFlatTableOrphansAsync(
                            baseSource,
                            format,
                            budget,
                            flatOwner,
                            flatTableName,
                            flatDef,
                            linkCol.ColNum,
                            parentTableName,
                            col.Name,
                            activeComplexRefs,
                            overlayPages,
                            diagnostics,
                            quarantineRecords,
                            quarantinedRowsByOwner,
                            forceRegenerateOwners,
                            cancellationToken).ConfigureAwait(false);
                        totalOrphansQuarantined += orphans;
                    }
                }
            }
        }

        // 2. Orphaned Enforced Foreign-Key Child Rows (IndexKind.ForeignKey)
        foreach (KeyValuePair<long, TDefImage> pair in definitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long childOwner = pair.Key;
            TDefImage childDef = pair.Value;
            string childTableName = tableNames.GetValueOrDefault(childOwner, $"Table_{childOwner}");
            if (childOwner < 3 || IsSystemTableName(childTableName))
            {
                continue;
            }

            byte[] childTDefBytes = childDef.CopyBytes();
            foreach (TDefLogicalIndexDescriptor logical in childDef.LogicalIndexes)
            {
                int relTypePos = logical.Entry.FieldsOffset + Constants.TableDefinition.Jet3.LogicalIdx.RelTblTypeOffset;
                if (logical.Entry.IndexType != IndexKind.ForeignKey
                    || logical.Entry.RelTblPage < 3
                    || logical.Entry.RelIdxNum < 0
                    || relTypePos < 0
                    || relTypePos >= childTDefBytes.Length
                    || childTDefBytes[relTypePos] != Constants.TableDefinition.ChildRelationshipTableType)
                {
                    continue;
                }

                long parentOwner = logical.Entry.RelTblPage;
                int childRealIdx = logical.Entry.IndexNum2;
                if (!definitions.TryGetValue(parentOwner, out TDefImage? parentDef))
                {
                    continue;
                }

                int parentRealIdx = -1;
                foreach (TDefLogicalIndexDescriptor parentLogical in parentDef.LogicalIndexes)
                {
                    if (parentLogical.Entry.IndexNum == logical.Entry.RelIdxNum)
                    {
                        parentRealIdx = parentLogical.Entry.IndexNum2;
                        break;
                    }
                }

                if (parentRealIdx < 0
                    || !CompactIndexRowAudit.SupportsDescriptor(parentDef, parentRealIdx)
                    || !CompactIndexRowAudit.SupportsDescriptor(childDef, childRealIdx)
                    || CompactIndexRowAudit.HasMemoColumn(parentDef, parentRealIdx)
                    || CompactIndexRowAudit.HasMemoColumn(childDef, childRealIdx))
                {
                    continue;
                }

                string parentTableName = tableNames.GetValueOrDefault(parentOwner, $"Table_{parentOwner}");
                IPageSource currentOverlay = overlayPages.Count > 0 ? new OverlayPageSource(baseSource, overlayPages) : baseSource;
                HashSet<byte[]> parentKeys = await CollectParentIndexKeysAsync(
                    currentOverlay,
                    format,
                    budget,
                    parentOwner,
                    parentDef,
                    parentRealIdx,
                    cancellationToken).ConfigureAwait(false);

                long fkOrphans = await QuarantineForeignKeyOrphansAsync(
                    baseSource,
                    format,
                    budget,
                    childOwner,
                    childTableName,
                    childDef,
                    childRealIdx,
                    parentTableName,
                    logical.Name,
                    parentKeys,
                    overlayPages,
                    diagnostics,
                    quarantineRecords,
                    quarantinedRowsByOwner,
                    forceRegenerateOwners,
                    cancellationToken).ConfigureAwait(false);
                totalOrphansQuarantined += fkOrphans;
            }
        }

        return totalOrphansQuarantined;
    }

    private static async ValueTask<Dictionary<(string ColumnName, int ComplexId), long>> ReadComplexFlatTableMappingsAsync(
        IPageSource source,
        JetFormat format,
        long complexCatalogOwner,
        TDefImage complexCatalogDef,
        CancellationToken cancellationToken)
    {
        var mappings = new Dictionary<(string ColumnName, int ComplexId), long>();
        ColumnInfo? colNameInfo = null;
        ColumnInfo? complexIdInfo = null;
        ColumnInfo? flatIdInfo = null;
        foreach (ColumnInfo col in complexCatalogDef.Columns)
        {
            if (string.Equals(col.Name, "ColumnName", StringComparison.OrdinalIgnoreCase))
            {
                colNameInfo = col;
            }
            else if (string.Equals(col.Name, "ComplexID", StringComparison.OrdinalIgnoreCase))
            {
                complexIdInfo = col;
            }
            else if (string.Equals(col.Name, "FlatTableID", StringComparison.OrdinalIgnoreCase))
            {
                flatIdInfo = col;
            }
        }

        if (colNameInfo is null || complexIdInfo is null || flatIdInfo is null)
        {
            return mappings;
        }

        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data
                    || BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4)) != complexCatalogOwner)
                {
                    continue;
                }

                int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
                for (int slot = 0; slot < numRows; slot++)
                {
                    if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, slot, out RowBound bound) || bound.RowSize <= 0)
                    {
                        continue;
                    }

                    string? colName = null;
                    int? complexId = null;
                    int? flatTableId = null;
                    var location = new RowLocation(pageNumber, slot, bound.RowStart, bound.RowSize);
                    await CompactRawFieldReader.ScanAsync(
                        format,
                        complexCatalogDef,
                        location,
                        page.AsMemory(bound.RowStart, bound.RowSize),
                        (field, _) =>
                        {
                            if (field.Kind != CompactRawFieldKind.Stored)
                            {
                                return default;
                            }

                            if (field.Column.ColNum == colNameInfo.ColNum)
                            {
                                colName = format.DecodeText(field.RawBytes.ToArray(), 0, field.RawBytes.Length);
                            }
                            else if (field.Column.ColNum == complexIdInfo.ColNum && field.RawBytes.Length == 4)
                            {
                                complexId = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                            }
                            else if (field.Column.ColNum == flatIdInfo.ColNum && field.RawBytes.Length == 4)
                            {
                                flatTableId = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                            }

                            return default;
                        },
                        cancellationToken).ConfigureAwait(false);

                    if (!string.IsNullOrEmpty(colName) && complexId is int cid && flatTableId is int fid)
                    {
                        long flatPage = fid & 0x00FFFFFFL;
                        if (flatPage >= 3)
                        {
                            mappings[(colName.ToUpperInvariant(), cid)] = flatPage;
                            mappings[(colName.ToUpperInvariant(), 0)] = flatPage;
                        }
                    }
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return mappings;
    }

    private static long ResolveFlatTableOwner(
        Dictionary<(string ColumnName, int ComplexId), long> mappings,
        string columnName,
        int complexId)
    {
        string upper = columnName.ToUpperInvariant();
        if (mappings.TryGetValue((upper, complexId), out long owner))
        {
            return owner;
        }

        return mappings.TryGetValue((upper, 0), out long fallback) ? fallback : 0;
    }

    private static ColumnInfo? FindComplexFlatLinkColumn(TDefImage flatDef, string parentColumnName)
    {
        string expected = "_" + parentColumnName;
        foreach (ColumnInfo col in flatDef.Columns)
        {
            if (col.Type == ColumnType.LongIntegerType
                && (col.Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0
                && string.Equals(col.Name, expected, StringComparison.OrdinalIgnoreCase))
            {
                return col;
            }
        }

        foreach (ColumnInfo col in flatDef.Columns)
        {
            if (col.Type == ColumnType.LongIntegerType
                && (col.Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0
                && !string.IsNullOrEmpty(col.Name)
                && col.Name[0] == '_')
            {
                return col;
            }
        }

        return null;
    }

    private static async ValueTask<HashSet<int>> CollectActiveInt32ColumnValuesAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        TDefImage definition,
        int colNum,
        CancellationToken cancellationToken)
    {
        var values = new HashSet<int>();
        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data
                    || BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4)) != owner)
                {
                    continue;
                }

                int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
                for (int slot = 0; slot < numRows; slot++)
                {
                    if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, slot, out RowBound bound) || bound.RowSize <= 0)
                    {
                        continue;
                    }

                    int raw = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (slot * 2), 2));
                    if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
                    {
                        continue;
                    }

                    var location = new RowLocation(pageNumber, slot, bound.RowStart, bound.RowSize);
                    await CompactRawFieldReader.ScanAsync(
                        format,
                        definition,
                        location,
                        page.AsMemory(bound.RowStart, bound.RowSize),
                        (field, _) =>
                        {
                            if (field.Column.ColNum == colNum && field.Kind == CompactRawFieldKind.Stored && field.RawBytes.Length == 4)
                            {
                                int val = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                                if (val > 0)
                                {
                                    values.Add(val);
                                }
                            }

                            return default;
                        },
                        cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return values;
    }

    private static async ValueTask<long> QuarantineFlatTableOrphansAsync(
        IPageSource baseSource,
        JetFormat format,
        CompactDiscoveryBudget budget,
        long flatOwner,
        string flatTableName,
        TDefImage flatDef,
        int linkColNum,
        string parentTableName,
        string parentColumnName,
        HashSet<int> activeParentRefs,
        Dictionary<long, byte[]> overlayPages,
        List<AccessCompactDiagnostic> diagnostics,
        List<AccessCompactQuarantineRecord> quarantineRecords,
        Dictionary<long, long> quarantinedRowsByOwner,
        HashSet<long> forceRegenerateOwners,
        CancellationToken cancellationToken)
    {
        long quarantinedCount = 0;
        IPageSource currentOverlay = overlayPages.Count > 0 ? new OverlayPageSource(baseSource, overlayPages) : baseSource;
        for (long pageNumber = 3; pageNumber < currentOverlay.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await currentOverlay.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data
                    || BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4)) != flatOwner)
                {
                    continue;
                }

                int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
                byte[]? modifiedPage = null;
                for (int slot = 0; slot < numRows; slot++)
                {
                    if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, slot, out RowBound bound) || bound.RowSize <= 0)
                    {
                        continue;
                    }

                    int raw = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (slot * 2), 2));
                    if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
                    {
                        continue;
                    }

                    int? linkVal = null;
                    var location = new RowLocation(pageNumber, slot, bound.RowStart, bound.RowSize);
                    await CompactRawFieldReader.ScanAsync(
                        format,
                        flatDef,
                        location,
                        page.AsMemory(bound.RowStart, bound.RowSize),
                        (field, _) =>
                        {
                            if (field.Column.ColNum == linkColNum && field.Kind == CompactRawFieldKind.Stored && field.RawBytes.Length == 4)
                            {
                                linkVal = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                            }

                            return default;
                        },
                        cancellationToken).ConfigureAwait(false);

                    if (linkVal is int refId && refId > 0 && activeParentRefs.Contains(refId))
                    {
                        continue;
                    }

                    if (modifiedPage is null)
                    {
                        modifiedPage = new byte[format.PageSize];
                        Buffer.BlockCopy(page, 0, modifiedPage, 0, format.PageSize);
                    }

                    byte[] rawRowBytes = page.AsSpan(bound.RowStart, bound.RowSize).ToArray();
                    ushort deletedEntry = checked((ushort)(Constants.DataPage.DeletedRowFlag | bound.RowStart));
                    BinaryPrimitives.WriteUInt16LittleEndian(modifiedPage.AsSpan(format.DataPage.RowsStart + (slot * 2), 2), deletedEntry);

                    string reason = $"Complex flat-table row {pageNumber}:{slot} on '{flatTableName}' has complex reference {linkVal?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "null"} not present in any surviving row of '{parentTableName}.{parentColumnName}'.";
                    quarantineRecords.Add(
                        new AccessCompactQuarantineRecord(
                            "quarantined-orphan-complex-row",
                            pageNumber,
                            flatOwner,
                            flatTableName,
                            slot,
                            reason,
                            ComputeSha256Hex(rawRowBytes),
                            Convert.ToBase64String(rawRowBytes)));
                    if (diagnostics.Count < budget.MaxDiagnostics)
                    {
                        diagnostics.Add(
                            new AccessCompactDiagnostic(
                                "quarantined-orphan-complex-row",
                                pageNumber,
                                flatOwner,
                                slot,
                                reason,
                                AccessCompactDiagnosticCategory.QuarantinedRow,
                                flatTableName));
                    }

                    quarantinedRowsByOwner.TryGetValue(flatOwner, out long existing);
                    quarantinedRowsByOwner[flatOwner] = existing + 1;
                    quarantinedCount++;
                    _ = forceRegenerateOwners.Add(flatOwner);
                }

                if (modifiedPage is not null)
                {
                    overlayPages[pageNumber] = modifiedPage;
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return quarantinedCount;
    }

    private static async ValueTask<HashSet<byte[]>> CollectParentIndexKeysAsync(
        IPageSource source,
        JetFormat format,
        CompactDiscoveryBudget budget,
        long parentOwner,
        TDefImage parentDef,
        int parentRealIdx,
        CancellationToken cancellationToken)
    {
        var keys = new HashSet<byte[]>(ByteSequenceComparer.Instance);
        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data
                    || BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4)) != parentOwner)
                {
                    continue;
                }

                int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
                for (int slot = 0; slot < numRows; slot++)
                {
                    if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, slot, out RowBound bound) || bound.RowSize <= 0)
                    {
                        continue;
                    }

                    int raw = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (slot * 2), 2));
                    if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
                    {
                        continue;
                    }

                    var location = new RowLocation(pageNumber, slot, bound.RowStart, bound.RowSize);
                    (byte[] key, bool allNull) = await CompactIndexRowAudit.ReadKeyAsync(
                        format,
                        parentDef,
                        parentRealIdx,
                        location,
                        page.AsMemory(bound.RowStart, bound.RowSize),
                        4096,
                        null,
                        null,
                        budget,
                        cancellationToken).ConfigureAwait(false);
                    if (!allNull)
                    {
                        _ = keys.Add(key);
                    }
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return keys;
    }

    private static async ValueTask<long> QuarantineForeignKeyOrphansAsync(
        IPageSource baseSource,
        JetFormat format,
        CompactDiscoveryBudget budget,
        long childOwner,
        string childTableName,
        TDefImage childDef,
        int childRealIdx,
        string parentTableName,
        string relationshipName,
        HashSet<byte[]> parentKeys,
        Dictionary<long, byte[]> overlayPages,
        List<AccessCompactDiagnostic> diagnostics,
        List<AccessCompactQuarantineRecord> quarantineRecords,
        Dictionary<long, long> quarantinedRowsByOwner,
        HashSet<long> forceRegenerateOwners,
        CancellationToken cancellationToken)
    {
        long quarantinedCount = 0;
        TDefIndexDescriptor descriptor = childDef.RealIndexes[childRealIdx];
        var fkColNums = new HashSet<int>(descriptor.Columns.Count);
        foreach (KeyColumn kc in descriptor.Columns)
        {
            _ = fkColNums.Add(kc.ColNum);
        }

        IPageSource currentOverlay = overlayPages.Count > 0 ? new OverlayPageSource(baseSource, overlayPages) : baseSource;
        for (long pageNumber = 3; pageNumber < currentOverlay.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await currentOverlay.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data
                    || BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4)) != childOwner)
                {
                    continue;
                }

                int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
                byte[]? modifiedPage = null;
                for (int slot = 0; slot < numRows; slot++)
                {
                    if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, slot, out RowBound bound) || bound.RowSize <= 0)
                    {
                        continue;
                    }

                    int raw = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (slot * 2), 2));
                    if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
                    {
                        continue;
                    }

                    var location = new RowLocation(pageNumber, slot, bound.RowStart, bound.RowSize);
                    ReadOnlyMemory<byte> rowMemory = page.AsMemory(bound.RowStart, bound.RowSize);

                    bool hasNullFkComponent = false;
                    await CompactRawFieldReader.ScanAsync(
                        format,
                        childDef,
                        location,
                        rowMemory,
                        (field, _) =>
                        {
                            if (fkColNums.Contains(field.Column.ColNum)
                                && field.Column.Type != ColumnType.BooleanType
                                && field.Kind is CompactRawFieldKind.Null or CompactRawFieldKind.Absent)
                            {
                                hasNullFkComponent = true;
                            }

                            return default;
                        },
                        cancellationToken).ConfigureAwait(false);

                    if (hasNullFkComponent)
                    {
                        continue;
                    }

                    (byte[] childKey, bool allNull) = await CompactIndexRowAudit.ReadKeyAsync(
                        format,
                        childDef,
                        childRealIdx,
                        location,
                        rowMemory,
                        4096,
                        null,
                        null,
                        budget,
                        cancellationToken).ConfigureAwait(false);
                    if (allNull || parentKeys.Contains(childKey))
                    {
                        continue;
                    }

                    if (modifiedPage is null)
                    {
                        modifiedPage = new byte[format.PageSize];
                        Buffer.BlockCopy(page, 0, modifiedPage, 0, format.PageSize);
                    }

                    byte[] rawRowBytes = rowMemory.ToArray();
                    ushort deletedEntry = checked((ushort)(Constants.DataPage.DeletedRowFlag | bound.RowStart));
                    BinaryPrimitives.WriteUInt16LittleEndian(modifiedPage.AsSpan(format.DataPage.RowsStart + (slot * 2), 2), deletedEntry);

                    string reason = $"Child row {pageNumber}:{slot} on table '{childTableName}' violates enforced foreign-key relationship '{relationshipName}' because its referenced parent row in '{parentTableName}' is missing.";
                    quarantineRecords.Add(
                        new AccessCompactQuarantineRecord(
                            "quarantined-orphan-fk-row",
                            pageNumber,
                            childOwner,
                            childTableName,
                            slot,
                            reason,
                            ComputeSha256Hex(rawRowBytes),
                            Convert.ToBase64String(rawRowBytes)));
                    if (diagnostics.Count < budget.MaxDiagnostics)
                    {
                        diagnostics.Add(
                            new AccessCompactDiagnostic(
                                "quarantined-orphan-fk-row",
                                pageNumber,
                                childOwner,
                                slot,
                                reason,
                                AccessCompactDiagnosticCategory.QuarantinedRow,
                                childTableName));
                    }

                    quarantinedRowsByOwner.TryGetValue(childOwner, out long existing);
                    quarantinedRowsByOwner[childOwner] = existing + 1;
                    quarantinedCount++;
                    _ = forceRegenerateOwners.Add(childOwner);
                }

                if (modifiedPage is not null)
                {
                    overlayPages[pageNumber] = modifiedPage;
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return quarantinedCount;
    }

    private static async ValueTask<HashSet<long>> ReadUsageMapMembersSafeAsync(
        IPageSource source,
        JetFormat format,
        UsageMapPointer pointer,
        HashSet<long> knownMapPages,
        CancellationToken cancellationToken)
    {
        var members = new HashSet<long>();
        if (pointer.PageNumber <= 0 || pointer.PageNumber >= source.PageCount)
        {
            return members;
        }

        byte[] page = await source.ReadUncachedPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.Data
                || !UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row)
                || row.RowSize < Constants.UsageMap.InlineMapHeaderSize)
            {
                return members;
            }

            if (page[row.RowStart] == Constants.UsageMap.InlineMapType)
            {
                int first = Ri32(page, row.RowStart + 1);
                if (first < 0)
                {
                    return members;
                }

                CollectBits(page, row.RowStart + Constants.UsageMap.InlineBitmapOffset, row.RowStart + row.RowSize, first, members);
            }
            else if (page[row.RowStart] == Constants.UsageMap.ReferenceMapType)
            {
                int count = UsageMap.ReferencePointerCount(row.RowSize);
                int windowSize = UsageMap.PagesPerReferenceMapPage(format.PageSize);
                for (int index = 0; index < count; index++)
                {
                    long bitmapNumber = Ru32(page, row.RowStart + 1 + (index * 4));
                    if (bitmapNumber <= 1 || bitmapNumber >= source.PageCount)
                    {
                        continue;
                    }

                    _ = knownMapPages.Add(bitmapNumber);
                    byte[] bitmap = await source.ReadUncachedPageAsync(bitmapNumber, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (bitmap[0] == Constants.PageTypes.UsageMap && bitmap[1] == 1)
                        {
                            CollectBits(bitmap, Constants.UsageMap.ReferenceMapBitmapOffset, format.PageSize, (long)index * windowSize, members);
                        }
                    }
                    finally
                    {
                        PageBuffers.Return(bitmap);
                    }
                }
            }
        }
        finally
        {
            PageBuffers.Return(page);
        }

        return members;
    }

    private static void CollectBits(byte[] page, int firstOffset, int endOffset, long firstPage, HashSet<long> members)
    {
        for (int offset = firstOffset; offset < endOffset; offset++)
        {
            byte b = page[offset];
            if (b == 0)
            {
                continue;
            }

            for (int bit = 0; bit < 8; bit++)
            {
                if ((b & (1 << bit)) != 0)
                {
                    _ = members.Add(firstPage + ((offset - firstOffset) * 8L) + bit);
                }
            }
        }
    }

    private static async ValueTask SetUsageMapBitInOverlayAsync(
        IPageSource source,
        JetFormat format,
        UsageMapPointer pointer,
        long targetPage,
        bool isMarked,
        Dictionary<long, byte[]> overlayPages,
        CancellationToken cancellationToken)
    {
        if (pointer.PageNumber <= 0 || pointer.PageNumber >= source.PageCount)
        {
            return;
        }

        if (!overlayPages.TryGetValue(pointer.PageNumber, out byte[]? mapPage))
        {
            byte[] raw = await source.ReadUncachedPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                mapPage = new byte[format.PageSize];
                Buffer.BlockCopy(raw, 0, mapPage, 0, format.PageSize);
            }
            finally
            {
                PageBuffers.Return(raw);
            }
        }

        if (mapPage[0] != Constants.PageTypes.Data
            || !UsageMap.TryGetRowBound(mapPage, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row)
            || row.RowSize < Constants.UsageMap.InlineMapHeaderSize)
        {
            return;
        }

        if (mapPage[row.RowStart] == Constants.UsageMap.InlineMapType)
        {
            if (UsageMap.TrySetInlinePageState(mapPage, row.RowStart, row.RowSize, targetPage, isMarked))
            {
                overlayPages[pointer.PageNumber] = mapPage;
            }
        }
        else if (mapPage[row.RowStart] == Constants.UsageMap.ReferenceMapType)
        {
            int windowSize = UsageMap.PagesPerReferenceMapPage(format.PageSize);
            int windowIndex = checked((int)(targetPage / windowSize));
            int pointerCount = UsageMap.ReferencePointerCount(row.RowSize);
            if (windowIndex < 0 || windowIndex >= pointerCount)
            {
                return;
            }

            long bitmapPageNumber = Ru32(mapPage, row.RowStart + 1 + (windowIndex * 4));
            if (bitmapPageNumber <= 1 || bitmapPageNumber >= source.PageCount)
            {
                return;
            }

            if (!overlayPages.TryGetValue(bitmapPageNumber, out byte[]? bitmapPage))
            {
                byte[] rawBitmap = await source.ReadUncachedPageAsync(bitmapPageNumber, cancellationToken).ConfigureAwait(false);
                try
                {
                    bitmapPage = new byte[format.PageSize];
                    Buffer.BlockCopy(rawBitmap, 0, bitmapPage, 0, format.PageSize);
                }
                finally
                {
                    PageBuffers.Return(rawBitmap);
                }
            }

            long bitWithinWindow = targetPage - ((long)windowIndex * windowSize);
            int byteOffset = Constants.UsageMap.ReferenceMapBitmapOffset + checked((int)(bitWithinWindow / 8));
            int bitOffset = checked((int)(bitWithinWindow % 8));
            if (byteOffset >= Constants.UsageMap.ReferenceMapBitmapOffset && byteOffset < format.PageSize)
            {
                bitmapPage[byteOffset] = isMarked
                    ? (byte)(bitmapPage[byteOffset] | (1 << bitOffset))
                    : (byte)(bitmapPage[byteOffset] & ~(1 << bitOffset));
                overlayPages[bitmapPageNumber] = bitmapPage;
            }
        }
    }

    private readonly record struct CorruptSlotInfo(int SlotIndex, string Reason, byte[] RawBytes);

    private readonly record struct PartialPageSalvageAttempt(
        bool Salvaged,
        byte[]? RepairedPage,
        int SurvivingLiveRows,
        List<CorruptSlotInfo> CorruptSlots);

    private sealed class ByteSequenceComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteSequenceComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y)
            => ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        public int GetHashCode(byte[] obj)
        {
            unchecked
            {
                uint hash = 2166136261U;
                foreach (byte b in obj)
                {
                    hash = (hash ^ b) * 16777619U;
                }

                return (int)hash;
            }
        }
    }

    private sealed class OverlayPageSource(IPageSource inner, Dictionary<long, byte[]> overrides) : IPageSource
    {
        public int PageSize => inner.PageSize;

        public long PageCount => inner.PageCount;

        public bool IsDisposed => inner.IsDisposed;

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
            => inner.ThrowIfDisposedOrCancelled(cancellationToken);

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken)
            => this.ReadCoreAsync(pageNumber, uncached: false, cancellationToken);

        public ValueTask<byte[]> ReadUncachedPageAsync(long pageNumber, CancellationToken cancellationToken)
            => this.ReadCoreAsync(pageNumber, uncached: true, cancellationToken);

        private ValueTask<byte[]> ReadCoreAsync(long pageNumber, bool uncached, CancellationToken cancellationToken)
        {
            inner.ThrowIfDisposedOrCancelled(cancellationToken);
            if (overrides.TryGetValue(pageNumber, out byte[]? replacement))
            {
                byte[] buffer = ArrayPool<byte>.Shared.Rent(inner.PageSize);
                Buffer.BlockCopy(replacement, 0, buffer, 0, inner.PageSize);
                return new ValueTask<byte[]>(buffer);
            }

            return uncached
                ? inner.ReadUncachedPageAsync(pageNumber, cancellationToken)
                : inner.ReadPageAsync(pageNumber, cancellationToken);
        }
    }
}
