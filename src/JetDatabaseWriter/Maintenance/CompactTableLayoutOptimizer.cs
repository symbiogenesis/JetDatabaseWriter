namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Optimizes the physical storage layout of staged tables after initial monotone build validation:
/// clusters user-table <c>DATA</c> rows into canonical Primary Key order (<c>ClusterByPrimaryKey</c>)
/// and coalesces fragmented multi-hop external <c>LVAL</c> chains into single-page or maximally dense
/// full-page <c>LVAL</c> chunks (<c>CoalesceLongValueChains</c>), then independently validates the result.
/// </summary>
internal static class CompactTableLayoutOptimizer
{
    internal static async ValueTask<CompactTableLayoutOptimizeResult> OptimizeAndValidateAsync(
        JetFormat format,
        long retainedPrefixPages,
        IReadOnlyList<CompactPinnedDatabaseTable> tables,
        IReadOnlyDictionary<long, string> tableNames,
        IReadOnlyDictionary<long, IReadOnlyList<ColumnInfo>?> hydratedColumnsByOwner,
        string destinationPath,
        bool clusterByPrimaryKey,
        bool coalesceLongValueChains,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(tables, nameof(tables));
        Guard.NotNull(tableNames, nameof(tableNames));
        Guard.NotNull(hydratedColumnsByOwner, nameof(hydratedColumnsByOwner));
        Guard.NotNullOrEmpty(destinationPath, nameof(destinationPath));
        cancellationToken.ThrowIfCancellationRequested();

        if ((!clusterByPrimaryKey && !coalesceLongValueChains) || tables.Count == 0)
        {
            return new CompactTableLayoutOptimizeResult(
                ClusteredTableCount: 0,
                CoalescedLongValueCount: 0,
                ClusteredOwners: [],
                CoalescedByOwner: new Dictionary<long, long>(),
                ReadBytes: 0,
                WrittenBytes: 0);
        }

        long tableAppendEnd = retainedPrefixPages;
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            tableAppendEnd = checked(tableAppendEnd + table.Allocation.PageCount);
        }

        long appendedCountLong = tableAppendEnd - retainedPrefixPages;
        if (appendedCountLong is <= 0 or > int.MaxValue)
        {
            return new CompactTableLayoutOptimizeResult(
                ClusteredTableCount: 0,
                CoalescedLongValueCount: 0,
                ClusteredOwners: [],
                CoalescedByOwner: new Dictionary<long, long>(),
                ReadBytes: 0,
                WrittenBytes: 0);
        }

        int appendedCount = (int)appendedCountLong;
        byte[][] appendedPages = new byte[appendedCount][];
        byte[] GetAppendedPage(long pageNumber)
        {
            long idx = pageNumber - retainedPrefixPages;
            if (idx < 0 || idx >= appendedCount)
            {
                throw Corrupt($"Page {pageNumber} lies outside staged table range [{retainedPrefixPages}, {tableAppendEnd}).");
            }

            return appendedPages[idx];
        }

        long readBytes = 0;
        long writtenBytes = 0;
        var clusteredOwners = new HashSet<long>();
        var coalescedByOwner = new Dictionary<long, long>();
        long totalCoalescedLvals = 0;
        var modifiedPageNumbers = new HashSet<long>();

        await using (FileStream stream = FileStreamFactory.Open(
            destinationPath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.None,
            FileOptions.Asynchronous | FileOptions.RandomAccess))
        {
            stream.Position = retainedPrefixPages * format.PageSize;
            for (int i = 0; i < appendedCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                byte[] buf = new byte[format.PageSize];
                await ReadExactAsync(stream, buf, cancellationToken).ConfigureAwait(false);
                appendedPages[i] = buf;
                readBytes = checked(readBytes + format.PageSize);
            }

            foreach (CompactPinnedDatabaseTable table in tables)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (table.Owner < 2)
                {
                    continue;
                }

                string tableName = tableNames.GetValueOrDefault(table.Owner, $"Table_{table.Owner}");
                bool isUserTable = table.Owner >= 3 && !CompactDataPageSalvager.IsSystemTableName(tableName);
                hydratedColumnsByOwner.TryGetValue(table.Owner, out IReadOnlyList<ColumnInfo>? hydratedColumns);

                if (isUserTable && clusterByPrimaryKey && TryClusterTableByPrimaryKey(format, table, GetAppendedPage, modifiedPageNumbers))
                {
                    _ = clusteredOwners.Add(table.Owner);
                }

                if (coalesceLongValueChains)
                {
                    long coalesced = await TryCoalesceTableLongValuesAsync(
                        format,
                        table,
                        hydratedColumns,
                        GetAppendedPage,
                        modifiedPageNumbers,
                        cancellationToken).ConfigureAwait(false);
                    if (coalesced > 0)
                    {
                        coalescedByOwner[table.Owner] = coalesced;
                        totalCoalescedLvals = checked(totalCoalescedLvals + coalesced);
                    }
                }
            }

            if (modifiedPageNumbers.Count > 0)
            {
                var sortedModified = new List<long>(modifiedPageNumbers);
                sortedModified.Sort();
                foreach (long pageNumber in sortedModified)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    stream.Position = pageNumber * format.PageSize;
                    await stream.WriteAsync(GetAppendedPage(pageNumber).AsMemory(0, format.PageSize), cancellationToken).ConfigureAwait(false);
                    writtenBytes = checked(writtenBytes + format.PageSize);
                }

                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        if (modifiedPageNumbers.Count > 0)
        {
            await using FileStream reopened = FileStreamFactory.Open(
                destinationPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            byte[] verifyBuf = new byte[format.PageSize];
            foreach (long pageNumber in modifiedPageNumbers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                reopened.Position = pageNumber * format.PageSize;
                await ReadExactAsync(reopened, verifyBuf, cancellationToken).ConfigureAwait(false);
                readBytes = checked(readBytes + format.PageSize);
                if (!verifyBuf.AsSpan(0, format.PageSize).SequenceEqual(GetAppendedPage(pageNumber).AsSpan(0, format.PageSize)))
                {
                    throw Corrupt($"Reopened verification of layout-optimized page {pageNumber} disagreed with written bytes.");
                }
            }
        }

        return new CompactTableLayoutOptimizeResult(
            ClusteredTableCount: clusteredOwners.Count,
            CoalescedLongValueCount: totalCoalescedLvals,
            ClusteredOwners: clusteredOwners,
            CoalescedByOwner: coalescedByOwner,
            ReadBytes: readBytes,
            WrittenBytes: writtenBytes);
    }

    private static bool TryClusterTableByPrimaryKey(
        JetFormat format,
        CompactPinnedDatabaseTable table,
        Func<long, byte[]> getAppendedPage,
        HashSet<long> modifiedPageNumbers)
    {
        CompactRawDataBuilderResult dataAlloc = table.Allocation.Table.Data;
        if (dataAlloc.RowCount <= 1 || dataAlloc.PageCount <= 0)
        {
            return false;
        }

        int pkSlot = -1;
        foreach (TDefLogicalIndexDescriptor logical in table.Definition.LogicalIndexes)
        {
            if (logical.Entry.IndexType == IndexKind.PrimaryKey)
            {
                pkSlot = logical.Entry.IndexNum2;
                break;
            }
        }

        if (pkSlot < 0 || pkSlot >= table.Allocation.Indexes.Count)
        {
            return false;
        }

        // Walk the Primary Key leaf chain to obtain the canonical PK bookmark order.
        CompactRawIndexBuilderResult pkTree = table.Allocation.Indexes[pkSlot].Tree;
        if (!TryReadOrderedLeafChainEntries(format, pkTree, getAppendedPage, out List<IndexEntry> pkEntries)
            || pkEntries.Count != dataAlloc.RowCount)
        {
            return false;
        }

        // Check whether the rows are already in strictly increasing (DataPage, DataRow) order.
        bool alreadyOrdered = true;
        for (int i = 1; i < pkEntries.Count; i++)
        {
            long prevPage = pkEntries[i - 1].DataPage;
            int prevRow = pkEntries[i - 1].DataRow;
            long currPage = pkEntries[i].DataPage;
            int currRow = pkEntries[i].DataRow;
            if (currPage < prevPage || (currPage == prevPage && currRow <= prevRow))
            {
                alreadyOrdered = false;
                break;
            }
        }

        if (alreadyOrdered)
        {
            return false;
        }

        // Extract all current rows from dataAlloc pages.
        var rowsByOldBookmark = new Dictionary<(long Page, int Row), byte[]>((int)dataAlloc.RowCount);
        for (long p = dataAlloc.FirstPageNumber; p < dataAlloc.FirstPageNumber + dataAlloc.PageCount; p++)
        {
            byte[] page = getAppendedPage(p);
            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
            for (int slot = 0; slot < numRows; slot++)
            {
                if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, slot, out RowBound bound) || bound.RowSize <= 0)
                {
                    return false;
                }

                rowsByOldBookmark[(p, slot)] = page.AsSpan(bound.RowStart, bound.RowSize).ToArray();
            }
        }

        if (rowsByOldBookmark.Count != pkEntries.Count)
        {
            return false;
        }

        // Pack rows in PK order across the exact dataAlloc.PageCount pages, ensuring every page receives at least 1 row.
        int targetPageCount = checked((int)dataAlloc.PageCount);
        byte[][] newDataPages = new byte[targetPageCount][];
        for (int i = 0; i < targetPageCount; i++)
        {
            byte[] dp = new byte[format.PageSize];
            dp[0] = Constants.PageTypes.Data;
            dp[1] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(dp.AsSpan(format.DataPage.TDefOff, 4), checked((int)table.Owner));
            newDataPages[i] = dp;
        }

        var bookmarkMap = new Dictionary<(long Page, int Row), (long Page, byte Row)>(pkEntries.Count);
        int currentPageIdx = 0;
        int currentRowCount = 0;
        int currentRowStart = format.PageSize;

        for (int i = 0; i < pkEntries.Count; i++)
        {
            IndexEntry pkEntry = pkEntries[i];
            if (!rowsByOldBookmark.TryGetValue((pkEntry.DataPage, pkEntry.DataRow), out byte[]? rowBytes))
            {
                return false;
            }

            int remainingRowsAfterThis = pkEntries.Count - 1 - i;
            int remainingPagesAfterCurrent = targetPageCount - 1 - currentPageIdx;
            bool forceAdvanceToKeepAllPagesNonEmpty = currentRowCount > 0 && remainingRowsAfterThis < remainingPagesAfterCurrent;
            bool pageFull = currentRowCount >= Constants.DataPage.MaxRowsPerPage
                || currentRowStart - rowBytes.Length < format.DataPage.RowsStart + ((currentRowCount + 1) * 2);

            if (forceAdvanceToKeepAllPagesNonEmpty || pageFull)
            {
                currentPageIdx++;
                if (currentPageIdx >= targetPageCount)
                {
                    return false;
                }

                currentRowCount = 0;
                currentRowStart = format.PageSize;
            }

            byte[] targetPage = newDataPages[currentPageIdx];
            currentRowStart -= rowBytes.Length;
            if (currentRowStart < format.DataPage.RowsStart + ((currentRowCount + 1) * 2))
            {
                return false;
            }

            rowBytes.CopyTo(targetPage.AsSpan(currentRowStart));
            BinaryPrimitives.WriteUInt16LittleEndian(
                targetPage.AsSpan(format.DataPage.RowsStart + (currentRowCount * 2), 2),
                checked((ushort)currentRowStart));
            int slotIndex = currentRowCount;
            currentRowCount++;
            BinaryPrimitives.WriteUInt16LittleEndian(targetPage.AsSpan(format.DataPage.NumRows, 2), checked((ushort)currentRowCount));
            BinaryPrimitives.WriteUInt16LittleEndian(
                targetPage.AsSpan(2, 2),
                checked((ushort)(currentRowStart - format.DataPage.RowsStart - (currentRowCount * 2))));

            long newPageNumber = dataAlloc.FirstPageNumber + currentPageIdx;
            bookmarkMap[(pkEntry.DataPage, pkEntry.DataRow)] = (newPageNumber, checked((byte)slotIndex));
        }

        if (currentPageIdx != targetPageCount - 1 || bookmarkMap.Count != pkEntries.Count)
        {
            return false;
        }

        // Rebuild all physical index trees with the remapped bookmarks before committing any page change.
        var stagedIndexReplacements = new Dictionary<long, byte[]>();
        var processedTrees = new HashSet<long>();
        foreach (CompactRawIndexAssemblyAllocation indexAlloc in table.Allocation.Indexes)
        {
            if (!processedTrees.Add(indexAlloc.Tree.FirstPageNumber))
            {
                continue;
            }

            if (indexAlloc.Tree.PageCount == 1)
            {
                long rootPageNum = indexAlloc.Tree.RootPageNumber;
                byte[] oldLeaf = getAppendedPage(rootPageNum);
                if (!IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, oldLeaf, format.PageSize, out List<IndexEntry> entries))
                {
                    return false;
                }

                for (int idx = 0; idx < entries.Count; idx++)
                {
                    IndexEntry oldEntry = entries[idx];
                    if (!bookmarkMap.TryGetValue((oldEntry.DataPage, oldEntry.DataRow), out (long NewPage, byte NewRow) mapped))
                    {
                        return false;
                    }

                    entries[idx] = new IndexEntry(oldEntry.Key, checked((uint)mapped.NewPage), mapped.NewRow);
                }

                entries.Sort(CompareIndexEntries);
                try
                {
                    stagedIndexReplacements[rootPageNum] = IndexPageCodec.BuildLeafPage(
                        format.IndexPage,
                        format.PageSize,
                        table.Owner,
                        entries,
                        0,
                        0,
                        0,
                        enablePrefixCompression: true);
                }
                catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException or ArgumentException or InvalidOperationException)
                {
                    return false;
                }
            }
            else
            {
                var visitedLeaves = new HashSet<long>();
                var leafChain = new List<(long PageNum, long Prev, long Next, int EntryCount)>();
                var allLeafEntries = new List<IndexEntry>();
                long firstTreePage = indexAlloc.Tree.FirstPageNumber;
                long endTreePage = firstTreePage + indexAlloc.Tree.PageCount;
                long curLeaf = firstTreePage;

                while (curLeaf != 0)
                {
                    if (curLeaf < firstTreePage || curLeaf >= endTreePage || !visitedLeaves.Add(curLeaf))
                    {
                        return false;
                    }

                    byte[] leafPage = getAppendedPage(curLeaf);
                    if (!IndexPageCodec.IsLeaf(leafPage))
                    {
                        return false;
                    }

                    long prev = IndexPageCodec.ReadPrevPage(format.IndexPage, leafPage);
                    long next = IndexPageCodec.ReadNextPage(format.IndexPage, leafPage);
                    if (!IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, leafPage, format.PageSize, out List<IndexEntry> entries)
                        || entries.Count == 0)
                    {
                        return false;
                    }

                    for (int idx = 0; idx < entries.Count; idx++)
                    {
                        IndexEntry oldEntry = entries[idx];
                        if (!bookmarkMap.TryGetValue((oldEntry.DataPage, oldEntry.DataRow), out (long NewPage, byte NewRow) mapped))
                        {
                            return false;
                        }

                        allLeafEntries.Add(new IndexEntry(oldEntry.Key, checked((uint)mapped.NewPage), mapped.NewRow));
                    }

                    leafChain.Add((curLeaf, prev, next, entries.Count));
                    curLeaf = next;
                }

                if (leafChain.Count == 0)
                {
                    return false;
                }

                allLeafEntries.Sort(CompareIndexEntries);
                for (int idx = 1; idx < allLeafEntries.Count; idx++)
                {
                    if (CompareIndexEntries(allLeafEntries[idx - 1], allLeafEntries[idx]) >= 0)
                    {
                        return false;
                    }
                }

                var maxEntryByPage = new Dictionary<long, IndexEntry>();
                int entryCursor = 0;
                foreach ((long leafPageNum, long prev, long next, int count) in leafChain)
                {
                    List<IndexEntry> leafEntries = allLeafEntries.GetRange(entryCursor, count);
                    entryCursor += count;
                    try
                    {
                        stagedIndexReplacements[leafPageNum] = IndexPageCodec.BuildLeafPage(
                            format.IndexPage,
                            format.PageSize,
                            table.Owner,
                            leafEntries,
                            prev,
                            next,
                            0,
                            enablePrefixCompression: true);
                        maxEntryByPage[leafPageNum] = leafEntries[^1];
                    }
                    catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException or ArgumentException or InvalidOperationException)
                    {
                        return false;
                    }
                }

                for (long p = firstTreePage; p < endTreePage; p++)
                {
                    if (visitedLeaves.Contains(p))
                    {
                        continue;
                    }

                    byte[] indexPage = getAppendedPage(p);
                    if (!IndexPageCodec.IsIntermediate(indexPage))
                    {
                        return false;
                    }

                    long prev = IndexPageCodec.ReadPrevPage(format.IndexPage, indexPage);
                    long next = IndexPageCodec.ReadNextPage(format.IndexPage, indexPage);
                    long tail = IndexPageCodec.ReadTailPage(format.IndexPage, indexPage);
                    if (!IndexPageCodec.TryDecodeIntermediateEntries(format.IndexPage, indexPage, format.PageSize, out List<DecodedIntermediateEntry> entries)
                        || entries.Count == 0)
                    {
                        return false;
                    }

                    for (int idx = 0; idx < entries.Count; idx++)
                    {
                        DecodedIntermediateEntry oldEntry = entries[idx];
                        if (maxEntryByPage.TryGetValue(oldEntry.ChildPage, out IndexEntry childMax))
                        {
                            entries[idx] = new DecodedIntermediateEntry(childMax, oldEntry.ChildPage);
                        }
                        else if (bookmarkMap.TryGetValue((oldEntry.Entry.DataPage, oldEntry.Entry.DataRow), out (long NewPage, byte NewRow) mapped))
                        {
                            entries[idx] = new DecodedIntermediateEntry(
                                new IndexEntry(oldEntry.Entry.Key, checked((uint)mapped.NewPage), mapped.NewRow),
                                oldEntry.ChildPage);
                        }
                        else
                        {
                            return false;
                        }
                    }

                    try
                    {
                        stagedIndexReplacements[p] = IndexPageCodec.BuildIntermediatePage(
                            format.IndexPage,
                            format.PageSize,
                            table.Owner,
                            entries,
                            prev,
                            next,
                            tail);
                        maxEntryByPage[p] = entries[^1].Entry;
                    }
                    catch (Exception ex) when (ex is JetCorruptDataException or JetLimitationException or ArgumentException or InvalidOperationException)
                    {
                        return false;
                    }
                }
            }
        }

        // Commit the PK-clustered DATA pages and updated index pages into the staged page buffers.
        for (int i = 0; i < targetPageCount; i++)
        {
            long pageNum = dataAlloc.FirstPageNumber + i;
            Buffer.BlockCopy(newDataPages[i], 0, getAppendedPage(pageNum), 0, format.PageSize);
            _ = modifiedPageNumbers.Add(pageNum);
        }

        foreach (KeyValuePair<long, byte[]> pair in stagedIndexReplacements)
        {
            Buffer.BlockCopy(pair.Value, 0, getAppendedPage(pair.Key), 0, format.PageSize);
            _ = modifiedPageNumbers.Add(pair.Key);
        }

        return true;
    }

    private static async ValueTask<long> TryCoalesceTableLongValuesAsync(
        JetFormat format,
        CompactPinnedDatabaseTable table,
        IReadOnlyList<ColumnInfo>? hydratedColumns,
        Func<long, byte[]> getAppendedPage,
        HashSet<long> modifiedPageNumbers,
        CancellationToken cancellationToken)
    {
        CompactRawDataLongValueRelocatorResult lvalAlloc = table.Allocation.Table.LongValues;
        CompactRawDataBuilderResult dataAlloc = table.Allocation.Table.Data;
        if (lvalAlloc.PageCount <= 0 || dataAlloc.PageCount <= 0)
        {
            return 0;
        }

        int maxSingleChunkPayload = format.PageSize - format.DataPage.RowsStart - 2;
        int maxChainedChunkPayload = maxSingleChunkPayload - 4;
        var descriptorSites = new List<LongValueSite>();
        long coalescableCount = 0;

        for (long p = dataAlloc.FirstPageNumber; p < dataAlloc.FirstPageNumber + dataAlloc.PageCount; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] dataPage = getAppendedPage(p);
            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(dataPage.AsSpan(format.DataPage.NumRows, 2));
            for (int slot = 0; slot < numRows; slot++)
            {
                if (!UsageMap.TryGetRowBound(dataPage, format.DataPage, format.PageSize, slot, out RowBound bound) || bound.RowSize <= 0)
                {
                    return 0;
                }

                var location = new RowLocation(p, slot, bound.RowStart, bound.RowSize);
                ReadOnlyMemory<byte> rowMemory = dataPage.AsMemory(bound.RowStart, bound.RowSize);
                bool failed = false;
                await CompactRawFieldReader.ScanAsync(
                    format,
                    table.Definition,
                    location,
                    rowMemory,
                    (field, _) =>
                    {
                        if (field.Kind == CompactRawFieldKind.Stored && field.IsLongValue)
                        {
                            if (!LongValueDescriptor.TryRead(field.RawBytes.Span, out LongValueDescriptor desc))
                            {
                                failed = true;
                                return default;
                            }

                            if (desc.IsExternal && desc.FirstDp != 0)
                            {
                                if (!TryExtractExternalLongValue(
                                    format,
                                    lvalAlloc,
                                    desc,
                                    getAppendedPage,
                                    out byte[] payload,
                                    out uint pageToken,
                                    out int chunkCount))
                                {
                                    failed = true;
                                    return default;
                                }

                                bool canPromoteToSingle = desc.UsesChainedPages && desc.Length <= maxSingleChunkPayload;
                                int optimalChunks = desc.Length <= maxSingleChunkPayload
                                    ? 1
                                    : (desc.Length + maxChainedChunkPayload - 1) / maxChainedChunkPayload;
                                bool canCoalesce = canPromoteToSingle || chunkCount > optimalChunks;
                                if (canCoalesce)
                                {
                                    coalescableCount++;
                                }

                                descriptorSites.Add(
                                    new LongValueSite(
                                        p,
                                        bound.RowStart + field.RowOffset,
                                        desc,
                                        payload,
                                        pageToken,
                                        canCoalesce));
                            }
                        }

                        return default;
                    },
                    cancellationToken,
                    hydratedColumns).ConfigureAwait(false);

                if (failed)
                {
                    return 0;
                }
            }
        }

        if (coalescableCount == 0 || descriptorSites.Count == 0)
        {
            return 0;
        }

        // Build the coalesced LVAL rows for each site.
        var lvalUnits = new List<(int SiteIndex, byte NewStorageMode, uint PageToken, List<byte[]> Chunks)>(descriptorSites.Count);
        int totalLvalRows = 0;
        for (int i = 0; i < descriptorSites.Count; i++)
        {
            LongValueSite site = descriptorSites[i];
            if (site.Payload.Length <= maxSingleChunkPayload)
            {
                lvalUnits.Add((i, Constants.LongValue.SinglePageStorageMode, site.PageToken, [site.Payload]));
                totalLvalRows++;
            }
            else
            {
                var chunks = new List<byte[]>();
                int offset = 0;
                while (offset < site.Payload.Length)
                {
                    int take = Math.Min(maxChainedChunkPayload, site.Payload.Length - offset);
                    byte[] chunkWithPrefix = new byte[4 + take];
                    Buffer.BlockCopy(site.Payload, offset, chunkWithPrefix, 4, take);
                    chunks.Add(chunkWithPrefix);
                    offset += take;
                    totalLvalRows++;
                }

                lvalUnits.Add((i, Constants.LongValue.ChainedStorageMode, site.PageToken, chunks));
            }
        }

        int targetLvalPageCount = checked((int)lvalAlloc.PageCount);
        uint defaultPageToken = descriptorSites[0].PageToken;
        byte[][] newLvalPages = new byte[targetLvalPageCount][];
        for (int i = 0; i < targetLvalPageCount; i++)
        {
            byte[] emptyLval = new byte[format.PageSize];
            emptyLval[0] = Constants.PageTypes.Data;
            emptyLval[1] = 1;
            BinaryPrimitives.WriteUInt16LittleEndian(
                emptyLval.AsSpan(2, 2),
                checked((ushort)(format.PageSize - format.DataPage.RowsStart)));
            "LVAL"u8.CopyTo(emptyLval.AsSpan(4, 4));
            if (!format.IsJet3)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(emptyLval.AsSpan(8, 4), defaultPageToken);
            }

            newLvalPages[i] = emptyLval;
        }

        int currentPageIdx = 0;
        int currentRowCount = 0;
        int currentRowStart = format.PageSize;
        uint currentPageToken = 0;
        int remainingTotalRows = totalLvalRows;
        uint[] newFirstDps = new uint[descriptorSites.Count];
        byte[] newModes = new byte[descriptorSites.Count];

        for (int unitIdx = 0; unitIdx < lvalUnits.Count; unitIdx++)
        {
            (int siteIndex, byte mode, uint pageToken, List<byte[]> chunks) = lvalUnits[unitIdx];
            newModes[siteIndex] = mode;
            int prevChunkPageIdx = -1;
            int prevChunkRowStart = -1;

            for (int c = 0; c < chunks.Count; c++)
            {
                byte[] chunkBytes = chunks[c];
                remainingTotalRows--;
                int remainingPagesAfterCurrent = targetLvalPageCount - 1 - currentPageIdx;
                bool forceAdvance = currentRowCount > 0 && remainingTotalRows < remainingPagesAfterCurrent;
                bool tokenMismatch = !format.IsJet3 && currentRowCount > 0 && pageToken != currentPageToken;
                bool pageFull = currentRowCount >= Constants.DataPage.MaxRowsPerPage
                    || currentRowStart - chunkBytes.Length < format.DataPage.RowsStart + ((currentRowCount + 1) * 2);

                if (forceAdvance || tokenMismatch || pageFull)
                {
                    currentPageIdx++;
                    if (currentPageIdx >= targetLvalPageCount)
                    {
                        return 0;
                    }

                    currentRowCount = 0;
                    currentRowStart = format.PageSize;
                }

                byte[] lvalPage = newLvalPages[currentPageIdx];
                if (currentRowCount == 0)
                {
                    lvalPage[0] = Constants.PageTypes.Data;
                    lvalPage[1] = 1;
                    "LVAL"u8.CopyTo(lvalPage.AsSpan(4, 4));
                    if (!format.IsJet3)
                    {
                        BinaryPrimitives.WriteUInt32LittleEndian(lvalPage.AsSpan(8, 4), pageToken);
                    }

                    currentPageToken = pageToken;
                }

                currentRowStart -= chunkBytes.Length;
                if (currentRowStart < format.DataPage.RowsStart + ((currentRowCount + 1) * 2))
                {
                    return 0;
                }

                chunkBytes.CopyTo(lvalPage.AsSpan(currentRowStart));
                int rowSlot = currentRowCount;
                BinaryPrimitives.WriteUInt16LittleEndian(
                    lvalPage.AsSpan(format.DataPage.RowsStart + (rowSlot * 2), 2),
                    checked((ushort)currentRowStart));
                currentRowCount++;
                BinaryPrimitives.WriteUInt16LittleEndian(lvalPage.AsSpan(format.DataPage.NumRows, 2), checked((ushort)currentRowCount));
                BinaryPrimitives.WriteUInt16LittleEndian(
                    lvalPage.AsSpan(2, 2),
                    checked((ushort)(currentRowStart - format.DataPage.RowsStart - (currentRowCount * 2))));

                long absPageNumber = lvalAlloc.FirstPageNumber + currentPageIdx;
                uint bookmark = checked(((uint)absPageNumber << 8) | (uint)rowSlot);
                if (c == 0)
                {
                    newFirstDps[siteIndex] = bookmark;
                }
                else if (prevChunkPageIdx >= 0 && prevChunkRowStart >= 0)
                {
                    BinaryPrimitives.WriteUInt32LittleEndian(
                        newLvalPages[prevChunkPageIdx].AsSpan(prevChunkRowStart, 4),
                        bookmark);
                }

                prevChunkPageIdx = currentPageIdx;
                prevChunkRowStart = currentRowStart;
            }
        }

        // Commit coalesced LVAL pages and updated 12-byte descriptors on DATA pages.
        for (int i = 0; i < targetLvalPageCount; i++)
        {
            long pageNum = lvalAlloc.FirstPageNumber + i;
            Buffer.BlockCopy(newLvalPages[i], 0, getAppendedPage(pageNum), 0, format.PageSize);
            _ = modifiedPageNumbers.Add(pageNum);
        }

        for (int i = 0; i < descriptorSites.Count; i++)
        {
            LongValueSite site = descriptorSites[i];
            byte[] dataPage = getAppendedPage(site.DataPageNumber);
            var updatedDesc = new LongValueDescriptor(site.OriginalDescriptor.Length, newModes[i], newFirstDps[i], site.OriginalDescriptor.Token);
            updatedDesc.WriteTo(dataPage.AsSpan(site.DescriptorOffsetOnPage, Constants.LongValue.HeaderSize));
            _ = modifiedPageNumbers.Add(site.DataPageNumber);
        }

        return coalescableCount;
    }

    private static bool TryExtractExternalLongValue(
        JetFormat format,
        CompactRawDataLongValueRelocatorResult lvalAlloc,
        LongValueDescriptor desc,
        Func<long, byte[]> getAppendedPage,
        out byte[] payload,
        out uint pageToken,
        out int chunkCount)
    {
        payload = [];
        pageToken = 0;
        chunkCount = 0;
        if (desc.Length < 0)
        {
            return false;
        }

        long currentPage = desc.FirstDp >> 8;
        int currentRow = (int)(desc.FirstDp & 0xFFU);
        byte[] result = new byte[desc.Length];
        int written = 0;

        while (true)
        {
            if (++chunkCount > 65_536
                || currentPage < lvalAlloc.FirstPageNumber
                || currentPage >= lvalAlloc.FirstPageNumber + lvalAlloc.PageCount)
            {
                return false;
            }

            byte[] lvalPage = getAppendedPage(currentPage);
            if (lvalPage[0] != Constants.PageTypes.Data
                || BinaryPrimitives.ReadUInt32LittleEndian(lvalPage.AsSpan(4, 4)) != 0x4C41564CU
                || !UsageMap.TryGetRowBound(lvalPage, format.DataPage, format.PageSize, currentRow, out RowBound bound)
                || bound.RowSize <= 0)
            {
                return false;
            }

            if (chunkCount == 1 && !format.IsJet3)
            {
                pageToken = BinaryPrimitives.ReadUInt32LittleEndian(lvalPage.AsSpan(8, 4));
            }

            if (desc.IsSinglePage)
            {
                if (bound.RowSize < desc.Length)
                {
                    return false;
                }

                Buffer.BlockCopy(lvalPage, bound.RowStart, result, 0, desc.Length);
                payload = result;
                return true;
            }

            if (bound.RowSize < 4)
            {
                return false;
            }

            int take = Math.Min(desc.Length - written, bound.RowSize - 4);
            if (take > 0)
            {
                Buffer.BlockCopy(lvalPage, bound.RowStart + 4, result, written, take);
                written += take;
            }

            uint nextBookmark = BinaryPrimitives.ReadUInt32LittleEndian(lvalPage.AsSpan(bound.RowStart, 4));
            if (nextBookmark == 0)
            {
                if (written != desc.Length)
                {
                    return false;
                }

                payload = result;
                return true;
            }

            currentPage = nextBookmark >> 8;
            currentRow = (int)(nextBookmark & 0xFFU);
        }
    }

    private static bool TryReadOrderedLeafChainEntries(
        JetFormat format,
        CompactRawIndexBuilderResult tree,
        Func<long, byte[]> getAppendedPage,
        out List<IndexEntry> allEntries)
    {
        allEntries = [];
        long current = tree.RootPageNumber;
        int hops = 0;
        while (true)
        {
            if (++hops > tree.PageCount + 4
                || current < tree.FirstPageNumber
                || current >= tree.FirstPageNumber + tree.PageCount)
            {
                return false;
            }

            byte[] page = getAppendedPage(current);
            if (IndexPageCodec.IsLeaf(page))
            {
                break;
            }

            if (!IndexPageCodec.IsIntermediate(page)
                || !IndexPageCodec.TryDecodeIntermediateEntries(format.IndexPage, page, format.PageSize, out List<DecodedIntermediateEntry> interm)
                || interm.Count == 0)
            {
                return false;
            }

            current = interm[0].ChildPage;
        }

        var visitedLeaves = new HashSet<long>();
        while (current != 0)
        {
            if (!visitedLeaves.Add(current)
                || current < tree.FirstPageNumber
                || current >= tree.FirstPageNumber + tree.PageCount)
            {
                return false;
            }

            byte[] leafPage = getAppendedPage(current);
            if (!IndexPageCodec.IsLeaf(leafPage)
                || !IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, leafPage, format.PageSize, out List<IndexEntry> leafEntries))
            {
                return false;
            }

            allEntries.AddRange(leafEntries);
            current = IndexPageCodec.ReadNextPage(format.IndexPage, leafPage);
        }

        return true;
    }

    private static int CompareIndexEntries(IndexEntry left, IndexEntry right)
    {
        int key = left.Key.AsSpan().SequenceCompareTo(right.Key);
        if (key != 0)
        {
            return key;
        }

        int page = left.DataPage.CompareTo(right.DataPage);
        return page != 0 ? page : left.DataRow.CompareTo(right.DataRow);
    }

    private static async ValueTask ReadExactAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), cancellationToken).ConfigureAwait(false);
            if (read <= 0)
            {
                throw Corrupt("Unexpected EOF while reading staged destination during table layout optimization.");
            }

            offset += read;
        }
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptPage, reason, new JetErrorInfo { Reason = reason });

    private readonly record struct LongValueSite(
        long DataPageNumber,
        int DescriptorOffsetOnPage,
        LongValueDescriptor OriginalDescriptor,
        byte[] Payload,
        uint PageToken,
        bool CanCoalesce);
}
