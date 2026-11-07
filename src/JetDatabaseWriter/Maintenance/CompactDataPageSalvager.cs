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
/// repairing verified data pages whose page-type header byte was damaged and isolating unrecoverable
/// data pages into quarantine records so surviving table pages can still be compacted.
/// </summary>
internal static class CompactDataPageSalvager
{
    internal static async ValueTask<CompactDataPageSalvageResult> InspectAndCreateOverlayAsync(
        IPageSource source,
        JetFormat format,
        CompactDiscoveryBudget budget,
        bool allowDataSalvage,
        CancellationToken cancellationToken)
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

        var overlayPages = new Dictionary<long, byte[]>();
        var diagnostics = new List<AccessCompactDiagnostic>();
        var quarantineRecords = new List<AccessCompactQuarantineRecord>();
        var salvagedRowsByOwner = new Dictionary<long, long>();
        var quarantinedPagesByOwner = new Dictionary<long, int>();
        var forceRegenerateOwners = new HashSet<long>();
        long totalSalvagedRows = 0;

        // Pass 1: Detect damaged-header DATA pages (e.g. 0x7F -> 0x01) whose owner, directory, and fields are valid.
        // Also mask the owner header on damaged known non-DATA table pages (such as corrupted index pages) so raw-row scanning does not abort before index regeneration runs.
        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                byte pageType = page[0];
                if (IsRecognizedPageType(pageType))
                {
                    continue;
                }

                uint owner = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4));
                if (owner < 3 || owner == 0x4C41564CU || !definitions.TryGetValue(owner, out TDefImage? def))
                {
                    continue;
                }

                string tableName = tableNames.GetValueOrDefault(owner, $"Table_{owner}");
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
                    _ = forceRegenerateOwners.Add(owner);
                    continue;
                }

                byte[] candidate = new byte[format.PageSize];
                Buffer.BlockCopy(page, 0, candidate, 0, format.PageSize);
                candidate[0] = Constants.PageTypes.Data;
                if (await TryValidateCandidateDataPageAsync(source, format, owner, def, pageNumber, candidate, requirePositiveLiveRows: true, cancellationToken).ConfigureAwait(false) is { Valid: true, LiveRowCount: > 0 } check)
                {
                    overlayPages[pageNumber] = candidate;
                    salvagedRowsByOwner.TryGetValue(owner, out long existingRows);
                    salvagedRowsByOwner[owner] = existingRows + check.LiveRowCount;
                    totalSalvagedRows += check.LiveRowCount;
                    if (diagnostics.Count < budget.MaxDiagnostics)
                    {
                        diagnostics.Add(
                            new AccessCompactDiagnostic(
                                "salvaged-data-page-header",
                                pageNumber,
                                owner,
                                null,
                                $"Repaired damaged page-type header byte (0x{pageType:X2} -> 0x01) on DATA page {pageNumber} for table '{tableName}' (owner {owner}) after verifying its row directory and {check.LiveRowCount} live row(s).",
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

        // Pass 2: Detect unrecoverable DATA pages on user tables and quarantine them so surviving pages can compact.
        IPageSource pass1Overlay = overlayPages.Count > 0 ? new OverlayPageSource(source, overlayPages) : source;
        for (long pageNumber = 3; pageNumber < source.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (knownNonDataTablePages.Contains(pageNumber) || overlayPages.ContainsKey(pageNumber))
            {
                continue;
            }

            byte[] page = await source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                byte pageType = page[0];
                uint rawOwner = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4));
                long? matchedOwner = null;
                if (pageType == Constants.PageTypes.Data && rawOwner >= 3 && rawOwner != 0x4C41564CU && definitions.ContainsKey(rawOwner))
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

                if (tableOwnedMapPointers.TryGetValue(owner, out UsageMapPointer ownedPtr))
                {
                    await ClearUsageMapBitInOverlayAsync(source, format, ownedPtr, pageNumber, overlayPages, cancellationToken).ConfigureAwait(false);
                }

                if (tableFreeMapPointers.TryGetValue(owner, out UsageMapPointer freePtr))
                {
                    await ClearUsageMapBitInOverlayAsync(source, format, freePtr, pageNumber, overlayPages, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
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
            totalSalvagedRows);
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
            0);

    private static bool IsRecognizedPageType(byte pageType)
        => pageType is Constants.PageTypes.Data
            or Constants.PageTypes.TableDefinition
            or Constants.PageTypes.IndexIntermediate
            or Constants.PageTypes.IndexLeaf
            or Constants.PageTypes.UsageMap
            or Constants.PageTypes.Freed;

    private static async ValueTask<(bool Valid, int LiveRowCount, string? Error)> TryValidateCandidateDataPageAsync(
        IPageSource source,
        JetFormat format,
        long owner,
        TDefImage definition,
        long pageNumber,
        byte[] candidatePage,
        bool requirePositiveLiveRows,
        CancellationToken cancellationToken)
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

        // Verify directory offsets using the exact rules of CompactRawRowReader and decode every live row situated on this page.
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

                    continue;
                }

                liveRowCount++;
                ReadOnlyMemory<byte> rowBytes = candidatePage.AsMemory(start, end - start);
                var location = new RowLocation(pageNumber, slot, start, end - start);
                await CompactRawFieldReader.ScanAsync(
                    format,
                    definition,
                    location,
                    rowBytes,
                    static (_, _) => default,
                    cancellationToken).ConfigureAwait(false);
            }

            if (requirePositiveLiveRows && liveRowCount == 0)
            {
                return (false, 0, "The candidate page contains no live inline rows.");
            }

            _ = source;
            return (true, liveRowCount, null);
        }
        catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException or ArgumentException)
        {
            return (false, 0, ex.Message);
        }
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

    private static async ValueTask ClearUsageMapBitInOverlayAsync(
        IPageSource source,
        JetFormat format,
        UsageMapPointer pointer,
        long targetPage,
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
            if (UsageMap.TrySetInlinePageState(mapPage, row.RowStart, row.RowSize, targetPage, isMarked: false))
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
                bitmapPage[byteOffset] = (byte)(bitmapPage[byteOffset] & ~(1 << bitOffset));
                overlayPages[bitmapPageNumber] = bitmapPage;
            }
        }
    }

    private sealed class OverlayPageSource(IPageSource inner, IReadOnlyDictionary<long, byte[]> overrides) : IPageSource
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
