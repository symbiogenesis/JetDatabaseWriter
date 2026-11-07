namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Relocates rebuilt table allocations into scrubbed interior prefix holes (<c>3 .. RetainedPrefixPages - 1</c>)
/// using a strictly order-preserving page bijection before leaving pages appended at the end of the compacted file.
/// </summary>
internal static class CompactPrefixHolePacker
{
    internal static async ValueTask<CompactPrefixHolePackResult> PackAndValidateAsync(
        IPageSource source,
        JetFormat format,
        CompactReclamationPlan reclamation,
        IReadOnlyList<CompactPinnedDatabaseTable> tables,
        IReadOnlyDictionary<long, IReadOnlyList<ColumnInfo>?> hydratedColumnsByOwner,
        string destinationPath,
        CompactPinnedDatabaseBuilderBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(reclamation, nameof(reclamation));
        Guard.NotNull(tables, nameof(tables));
        Guard.NotNull(hydratedColumnsByOwner, nameof(hydratedColumnsByOwner));
        Guard.NotNullOrEmpty(destinationPath, nameof(destinationPath));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();

        long retainedPrefixPages = reclamation.RetainedPrefixPages;
        long tableAppendEnd = retainedPrefixPages;
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            tableAppendEnd = checked(tableAppendEnd + table.Allocation.PageCount);
        }

        if (!reclamation.ReclamationAuthorized || reclamation.ScrubbedPrefixPageCount == 0 || tables.Count == 0 || tableAppendEnd <= retainedPrefixPages)
        {
            return new CompactPrefixHolePackResult(
                PackedPages: 0,
                RemainingScrubbedPages: reclamation.ScrubbedPrefixPageCount,
                DestinationPages: tableAppendEnd,
                ReadBytes: 0,
                WrittenBytes: 0);
        }

        var holes = new List<long>(reclamation.ScrubbedPrefixPages);
        holes.Sort();
        long appendedCountLong = tableAppendEnd - retainedPrefixPages;
        if (appendedCountLong > int.MaxValue || appendedCountLong * format.PageSize > budget.MaxWorkingBytes / 2)
        {
            return new CompactPrefixHolePackResult(
                PackedPages: 0,
                RemainingScrubbedPages: reclamation.ScrubbedPrefixPageCount,
                DestinationPages: tableAppendEnd,
                ReadBytes: 0,
                WrittenBytes: 0);
        }

        int appendedCount = (int)appendedCountLong;
        int packedCount = Math.Min(holes.Count, appendedCount);
        while (packedCount > 0 && !AllUsageMapsFit(packedCount, holes, retainedPrefixPages, tableAppendEnd, tables, format))
        {
            packedCount--;
        }

        if (packedCount <= 0)
        {
            return new CompactPrefixHolePackResult(
                PackedPages: 0,
                RemainingScrubbedPages: reclamation.ScrubbedPrefixPageCount,
                DestinationPages: tableAppendEnd,
                ReadBytes: 0,
                WrittenBytes: 0);
        }

        long Relocate(long pageNumber)
            => RelocatePage(pageNumber, packedCount, holes, retainedPrefixPages, tableAppendEnd);

        long readBytes = 0;
        long writtenBytes = 0;
        byte[][] appendedPages = new byte[appendedCount][];
        byte[] GetAppendedPage(long oldPageNumber)
        {
            long index = oldPageNumber - retainedPrefixPages;
            if (index < 0 || index >= appendedCount)
            {
                throw Corrupt($"Expected appended table page {oldPageNumber} within [{retainedPrefixPages}, {tableAppendEnd}).");
            }

            return appendedPages[index];
        }

        var tdefInstallations = new List<CompactTDefChainInstallation>(tables.Count);
        var patchedTDefPages = new Dictionary<long, byte[]>();
        var chainBudget = new CompactTDefChainBudget
        {
            MaxPages = budget.MaxPages,
            MaxReadBytes = budget.MaxReadBytes,
            MaxWriteBytes = budget.MaxWriteBytes,
            MaxWorkingBytes = budget.MaxWorkingBytes,
        };
        long newAppendPoint = tableAppendEnd - packedCount;
        var remainingScrubbed = new HashSet<long>();
        for (int i = packedCount; i < holes.Count; i++)
        {
            _ = remainingScrubbed.Add(holes[i]);
        }

        var packedReclamation = new CompactReclamationPlan(
            source.PageCount,
            retainedPrefixPages,
            remainingScrubbed,
            isComplete: reclamation.IsComplete,
            reclamationAuthorized: reclamation.ReclamationAuthorized);

        await using (FileStream destination = FileStreamFactory.Open(
            destinationPath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.None,
            FileOptions.Asynchronous | FileOptions.RandomAccess))
        {
            if (destination.Length < tableAppendEnd * format.PageSize)
            {
                throw Corrupt("Staged destination is shorter than the expected table append end.");
            }

            destination.Position = retainedPrefixPages * format.PageSize;
            for (int i = 0; i < appendedCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                byte[] buf = new byte[format.PageSize];
                await ReadExactAsync(destination, buf, cancellationToken).ConfigureAwait(false);
                appendedPages[i] = buf;
                readBytes = checked(readBytes + format.PageSize);
            }

            foreach (CompactPinnedDatabaseTable table in tables)
            {
                cancellationToken.ThrowIfCancellationRequested();
                hydratedColumnsByOwner.TryGetValue(table.Owner, out IReadOnlyList<ColumnInfo>? hydratedColumns);

                // 1. Relocate external LVAL descriptors on DATA pages and multi-chunk LVAL chains.
                CompactRawDataBuilderResult dataAlloc = table.Allocation.Table.Data;
                for (long p = dataAlloc.FirstPageNumber; p < dataAlloc.FirstPageNumber + dataAlloc.PageCount; p++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    byte[] dataPage = GetAppendedPage(p);
                    int rowCount = BinaryPrimitives.ReadUInt16LittleEndian(dataPage.AsSpan(format.DataPage.NumRows, 2));
                    for (int slot = 0; slot < rowCount; slot++)
                    {
                        (int rowStart, int rowEnd) = LocatePackedRowBounds(dataPage, format, rowCount, slot);
                        var location = new RowLocation(p, slot, rowStart, rowEnd - rowStart);
                        ReadOnlyMemory<byte> rowMemory = dataPage.AsMemory(rowStart, rowEnd - rowStart);
                        await CompactRawFieldReader.ScanAsync(
                            format,
                            table.Definition,
                            location,
                            rowMemory,
                            (field, _) =>
                            {
                                if (field.Kind == CompactRawFieldKind.Stored
                                    && field.IsLongValue
                                    && LongValueDescriptor.TryRead(field.RawBytes.Span, out LongValueDescriptor desc)
                                    && desc.IsExternal
                                    && desc.FirstDp != 0)
                                {
                                    long lvalPage = desc.FirstDp >> 8;
                                    int lvalRow = (int)(desc.FirstDp & 0xFFU);
                                    if (desc.UsesChainedPages)
                                    {
                                        RelocateLongValueChain(lvalPage, lvalRow, GetAppendedPage, format, Relocate);
                                    }

                                    long relocatedLvalPage = Relocate(lvalPage);
                                    uint newFirstDp = (checked((uint)relocatedLvalPage) << 8) | (uint)lvalRow;
                                    BinaryPrimitives.WriteUInt32LittleEndian(
                                        dataPage.AsSpan(rowStart + field.RowOffset + sizeof(uint), sizeof(uint)),
                                        newFirstDp);
                                }

                                return default;
                            },
                            cancellationToken,
                            hydratedColumns).ConfigureAwait(false);
                    }
                }

                // 2. Relocate Index B-tree pages (leaf and intermediate).
                var processedIndexTrees = new HashSet<long>();
                foreach (CompactRawIndexAssemblyAllocation indexAlloc in table.Allocation.Indexes)
                {
                    if (!processedIndexTrees.Add(indexAlloc.Tree.FirstPageNumber))
                    {
                        continue;
                    }

                    for (long p = indexAlloc.Tree.FirstPageNumber; p < indexAlloc.Tree.FirstPageNumber + indexAlloc.Tree.PageCount; p++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        byte[] indexPage = GetAppendedPage(p);
                        if (IndexPageCodec.IsLeaf(indexPage))
                        {
                            long prev = IndexPageCodec.ReadPrevPage(format.IndexPage, indexPage);
                            long next = IndexPageCodec.ReadNextPage(format.IndexPage, indexPage);
                            if (!IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, indexPage, format.PageSize, out List<IndexEntry> entries))
                            {
                                throw Corrupt($"Failed to decode staged leaf index page {p} during interior hole packing.");
                            }

                            for (int idx = 0; idx < entries.Count; idx++)
                            {
                                entries[idx] = entries[idx] with
                                {
                                    DataPage = checked((uint)Relocate(entries[idx].DataPage)),
                                };
                            }

                            byte[] rebuilt = IndexPageCodec.BuildLeafPage(
                                format.IndexPage,
                                format.PageSize,
                                table.Owner,
                                entries,
                                prev == 0 ? 0 : Relocate(prev),
                                next == 0 ? 0 : Relocate(next),
                                0,
                                enablePrefixCompression: true);
                            Buffer.BlockCopy(rebuilt, 0, indexPage, 0, format.PageSize);
                        }
                        else if (IndexPageCodec.IsIntermediate(indexPage))
                        {
                            long prev = IndexPageCodec.ReadPrevPage(format.IndexPage, indexPage);
                            long next = IndexPageCodec.ReadNextPage(format.IndexPage, indexPage);
                            long tail = IndexPageCodec.ReadTailPage(format.IndexPage, indexPage);
                            if (!IndexPageCodec.TryDecodeIntermediateEntries(format.IndexPage, indexPage, format.PageSize, out List<DecodedIntermediateEntry> entries))
                            {
                                throw Corrupt($"Failed to decode staged intermediate index page {p} during interior hole packing.");
                            }

                            for (int idx = 0; idx < entries.Count; idx++)
                            {
                                DecodedIntermediateEntry entry = entries[idx];
                                entries[idx] = new DecodedIntermediateEntry(
                                    entry.Entry with { DataPage = checked((uint)Relocate(entry.Entry.DataPage)) },
                                    Relocate(entry.ChildPage));
                            }

                            byte[] rebuilt = IndexPageCodec.BuildIntermediatePage(
                                format.IndexPage,
                                format.PageSize,
                                table.Owner,
                                entries,
                                prev == 0 ? 0 : Relocate(prev),
                                next == 0 ? 0 : Relocate(next),
                                tail == 0 ? 0 : Relocate(tail));
                            Buffer.BlockCopy(rebuilt, 0, indexPage, 0, format.PageSize);
                        }
                        else
                        {
                            throw Corrupt($"Unexpected page type 0x{indexPage[0]:X2} in staged index allocation at page {p}.");
                        }
                    }
                }

                // 3. Relocate UsageMap allocations (both Inline 0x00 and Reference 0x01 + 0x05) for table Owned/Free, LVAL Owned/Free, and Index Owned.
                RelocateUsageMapAllocation(table.Allocation.Table.OwnedPages, GetAppendedPage, format, Relocate);
                RelocateUsageMapAllocation(table.Allocation.Table.FreePages, GetAppendedPage, format, Relocate);

                var relocatedLvalRefs = new CompactTDefLongValueReferences[table.Allocation.Table.LongValueMaps.Count];
                for (int i = 0; i < relocatedLvalRefs.Length; i++)
                {
                    CompactRawTableLongValueMaps lvMap = table.Allocation.Table.LongValueMaps[i];
                    RelocateUsageMapAllocation(lvMap.OwnedPages, GetAppendedPage, format, Relocate);
                    RelocateUsageMapAllocation(lvMap.FreePages, GetAppendedPage, format, Relocate);
                    relocatedLvalRefs[i] = new CompactTDefLongValueReferences(
                        lvMap.ColumnNumber,
                        new UsageMapPointer(lvMap.OwnedPages.Pointer.RowIndex, checked((int)Relocate(lvMap.OwnedPages.Pointer.PageNumber))),
                        new UsageMapPointer(lvMap.FreePages.Pointer.RowIndex, checked((int)Relocate(lvMap.FreePages.Pointer.PageNumber))));
                }

                var processedIndexMaps = new HashSet<long>();
                var relocatedIndexRefs = new CompactTDefIndexReferences[table.Allocation.Indexes.Count];
                for (int i = 0; i < relocatedIndexRefs.Length; i++)
                {
                    CompactRawIndexAssemblyAllocation idxAlloc = table.Allocation.Indexes[i];
                    if (processedIndexMaps.Add(idxAlloc.OwnedPages.FirstPageNumber))
                    {
                        RelocateUsageMapAllocation(idxAlloc.OwnedPages, GetAppendedPage, format, Relocate);
                    }

                    relocatedIndexRefs[i] = new CompactTDefIndexReferences(
                        checked((uint)Relocate(idxAlloc.Tree.RootPageNumber)),
                        new UsageMapPointer(idxAlloc.OwnedPages.Pointer.RowIndex, checked((int)Relocate(idxAlloc.OwnedPages.Pointer.PageNumber))));
                }

                var relocatedTableOwned = new UsageMapPointer(
                    table.Allocation.Table.OwnedPages.Pointer.RowIndex,
                    checked((int)Relocate(table.Allocation.Table.OwnedPages.Pointer.PageNumber)));
                var relocatedTableFree = new UsageMapPointer(
                    table.Allocation.Table.FreePages.Pointer.RowIndex,
                    checked((int)Relocate(table.Allocation.Table.FreePages.Pointer.PageNumber)));

                byte[] patchedTDef = CompactTDefReferencePatcher.Patch(
                    format,
                    table.Definition,
                    relocatedTableOwned,
                    relocatedTableFree,
                    relocatedIndexRefs,
                    relocatedLvalRefs,
                    budget.MaxWorkingBytes,
                    cancellationToken);
                tdefInstallations.Add(new CompactTDefChainInstallation(table.Owner, table.Definition, patchedTDef));
            }

            // 4. Write all relocated pages to their new coordinates in destination.
            for (int i = 0; i < appendedCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                long oldPage = retainedPrefixPages + i;
                long newPage = Relocate(oldPage);
                destination.Position = newPage * format.PageSize;
                await destination.WriteAsync(appendedPages[i].AsMemory(0, format.PageSize), cancellationToken).ConfigureAwait(false);
                writtenBytes = checked(writtenBytes + format.PageSize);
            }

            // 5. Restore original source TDEF chain pages onto destination so CompactTDefChainInstaller preflights against immutable source pages, then reinstall relocated TDEF pointer patches.
            foreach (CompactTDefChainInstallation installation in tdefInstallations)
            {
                long number = installation.RootPage;
                int logical = 0;
                while (logical < installation.Original.LogicalByteLength)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int start = logical == 0 ? 0 : 8;
                    int length = format.PageSize - start;
                    byte[] sourceTDefPage = await source.ReadUncachedPageAsync(number, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        readBytes = checked(readBytes + format.PageSize);
                        destination.Position = number * format.PageSize;
                        await destination.WriteAsync(sourceTDefPage.AsMemory(0, format.PageSize), cancellationToken).ConfigureAwait(false);
                        writtenBytes = checked(writtenBytes + format.PageSize);
                        number = BinaryPrimitives.ReadUInt32LittleEndian(sourceTDefPage.AsSpan(4, 4));
                        logical += length;
                    }
                    finally
                    {
                        PageBuffers.Return(sourceTDefPage);
                    }
                }
            }

            CompactTDefChainResult chainInstall = await CompactTDefChainInstaller.InstallAsync(
                source,
                format,
                tdefInstallations,
                destination,
                chainBudget,
                cancellationToken).ConfigureAwait(false);
            readBytes = checked(readBytes + chainInstall.ReadBytes);
            writtenBytes = checked(writtenBytes + chainInstall.WrittenBytes);

            // 6. Truncate destination to newAppendPoint, restore original Page 1 from source, and reinstall Page 1's global usage map.
            destination.SetLength(newAppendPoint * format.PageSize);
            byte[] originalPageOne = await source.ReadUncachedPageAsync(1, cancellationToken).ConfigureAwait(false);
            try
            {
                readBytes = checked(readBytes + format.PageSize);
                destination.Position = format.PageSize;
                await destination.WriteAsync(originalPageOne.AsMemory(0, format.PageSize), cancellationToken).ConfigureAwait(false);
                writtenBytes = checked(writtenBytes + format.PageSize);
            }
            finally
            {
                PageBuffers.Return(originalPageOne);
            }

            CompactGlobalMapResult globalMapInstall = await CompactGlobalMapInstaller.InstallAsync(
                source,
                format,
                newAppendPoint,
                destination,
                budget.GlobalMap,
                cancellationToken,
                packedReclamation).ConfigureAwait(false);
            readBytes = checked(readBytes + globalMapInstall.ReadBytes);
            writtenBytes = checked(writtenBytes + globalMapInstall.WrittenBytes);
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // 7. Reopen destination read-only and independently validate global map, TDEF chains, remaining scrubbed holes, and storage inventory.
        await using (FileStream reopened = FileStreamFactory.Open(
            destinationPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            FileOptions.Asynchronous | FileOptions.SequentialScan))
        {
            CompactGlobalMapResult globalMapValid = await CompactGlobalMapValidator.ValidateAsync(
                source,
                format,
                newAppendPoint,
                reopened,
                budget.GlobalMap,
                cancellationToken,
                packedReclamation).ConfigureAwait(false);
            readBytes = checked(readBytes + globalMapValid.ReadBytes);

            CompactTDefChainResult chainValid = await CompactTDefChainValidator.ValidateAsync(
                source,
                format,
                tdefInstallations,
                reopened,
                chainBudget,
                cancellationToken).ConfigureAwait(false);
            readBytes = checked(readBytes + chainValid.ReadBytes);

            byte[] checkBuffer = new byte[format.PageSize];
            foreach (long remainingHole in remainingScrubbed)
            {
                cancellationToken.ThrowIfCancellationRequested();
                reopened.Position = remainingHole * format.PageSize;
                await ReadExactAsync(reopened, checkBuffer, cancellationToken).ConfigureAwait(false);
                readBytes = checked(readBytes + format.PageSize);
                if (!CompactReclamationPlan.IsScrubbedFreePage(checkBuffer, format.PageSize))
                {
                    throw Corrupt($"Remaining interior hole page {remainingHole} is not a canonical scrubbed freed page after hole packing.");
                }
            }

            var reopenedPages = new PrivateStreamPageSource(reopened, format.PageSize);
            int maxSchemaBytes = checked((int)Math.Min(int.MaxValue, budget.Tables.Discovery.MaxSchemaBytes));
            using var tableDefs = new TableDefReader(reopenedPages, format, cacheResults: false, budget.Tables.Discovery.MaxDefinitionBytes, maxSchemaBytes);
            int discoveredDefinitions = 0;
            await CompactStorageInventory.ScanAsync(
                reopenedPages,
                format,
                tableDefs,
                new CompactTraversalBudget
                {
                    MaxPages = budget.MaxPages,
                    MaxObjects = checked((int)Math.Min(int.MaxValue, budget.Tables.Discovery.MaxCatalogObjects)),
                    MaxReferences = budget.Tables.Discovery.MaxReferences,
                    MaxDiagnostics = checked((int)Math.Min(int.MaxValue, budget.Tables.Discovery.MaxDiagnostics)),
                    MaxSchemaBytes = maxSchemaBytes,
                    MaxPageReads = budget.Tables.Discovery.MaxPageReads,
                    MaxMapSteps = budget.Tables.Discovery.MaxMapSteps,
                    MaxRecords = budget.Tables.Discovery.MaxRecords,
                    MaxWorkingBytes = budget.MaxWorkingBytes,
                },
                (record, _) =>
                {
                    if (record.Kind == "diagnostic")
                    {
                        throw Corrupt($"Reopened inventory after interior hole packing reported an unexpected diagnostic on page {record.PageNumber}: {record.Detail}");
                    }

                    return default;
                },
                cancellationToken,
                (_, _, _) =>
                {
                    discoveredDefinitions++;
                    return default;
                }).ConfigureAwait(false);

            if (discoveredDefinitions != tables.Count)
            {
                throw Corrupt($"Reopened inventory after interior hole packing discovered {discoveredDefinitions} definitions instead of {tables.Count}.");
            }

            _ = patchedTDefPages;
            return new CompactPrefixHolePackResult(
                PackedPages: packedCount,
                RemainingScrubbedPages: remainingScrubbed.Count,
                DestinationPages: globalMapValid.DestinationPages,
                ReadBytes: readBytes,
                WrittenBytes: writtenBytes);
        }
    }

    private static long RelocatePage(
        long pageNumber,
        int packedCount,
        List<long> holes,
        long retainedPrefixPages,
        long tableAppendEnd)
    {
        if (pageNumber < retainedPrefixPages || pageNumber >= tableAppendEnd)
        {
            return pageNumber;
        }

        long offset = pageNumber - retainedPrefixPages;
        if (offset < packedCount)
        {
            return holes[(int)offset];
        }

        return retainedPrefixPages + (offset - packedCount);
    }

    private static bool AllUsageMapsFit(
        int packedCount,
        List<long> holes,
        long retainedPrefixPages,
        long tableAppendEnd,
        IReadOnlyList<CompactPinnedDatabaseTable> tables,
        JetFormat format)
    {
        int inlineCapacity = (format.UsageMapRowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            if (table.Allocation.Table.FreePages.PageCount != 1)
            {
                return false;
            }

            if (!ExtentFitsUsageMapAllocation(
                table.Allocation.Table.Data.FirstPageNumber,
                table.Allocation.Table.Data.PageCount,
                table.Allocation.Table.OwnedPages.PageCount,
                packedCount,
                holes,
                retainedPrefixPages,
                tableAppendEnd,
                inlineCapacity,
                format))
            {
                return false;
            }

            foreach (CompactRawTableLongValueMaps lvMap in table.Allocation.Table.LongValueMaps)
            {
                if (lvMap.FreePages.PageCount != 1)
                {
                    return false;
                }

                if (!ExtentFitsUsageMapAllocation(
                    table.Allocation.Table.LongValues.FirstPageNumber,
                    table.Allocation.Table.LongValues.PageCount,
                    lvMap.OwnedPages.PageCount,
                    packedCount,
                    holes,
                    retainedPrefixPages,
                    tableAppendEnd,
                    inlineCapacity,
                    format))
                {
                    return false;
                }
            }

            foreach (CompactRawIndexAssemblyAllocation idxAlloc in table.Allocation.Indexes)
            {
                if (!ExtentFitsUsageMapAllocation(
                    idxAlloc.Tree.FirstPageNumber,
                    idxAlloc.Tree.PageCount,
                    idxAlloc.OwnedPages.PageCount,
                    packedCount,
                    holes,
                    retainedPrefixPages,
                    tableAppendEnd,
                    inlineCapacity,
                    format))
                {
                    return false;
                }
            }
        }

        return true;
    }

    private static bool ExtentFitsUsageMapAllocation(
        long firstPage,
        long pageCount,
        long mapAllocationPageCount,
        int packedCount,
        List<long> holes,
        long retainedPrefixPages,
        long tableAppendEnd,
        int inlineCapacity,
        JetFormat format)
    {
        if (mapAllocationPageCount <= 0)
        {
            return false;
        }

        if (mapAllocationPageCount == 1)
        {
            return ExtentFitsInlineWindow(
                firstPage,
                pageCount,
                packedCount,
                holes,
                retainedPrefixPages,
                tableAppendEnd,
                inlineCapacity);
        }

        if (pageCount <= 0)
        {
            return false;
        }

        long bitmapPageCapacity = mapAllocationPageCount - 1;
        int maxPointers = UsageMap.ReferencePointerCount(format.UsageMapRowSize);
        int lastWindow = -1;
        long distinctWindows = 0;
        for (long p = firstPage; p < firstPage + pageCount; p++)
        {
            long relocated = RelocatePage(p, packedCount, holes, retainedPrefixPages, tableAppendEnd);
            int window = UsageMap.ReferencePointerIndex(format.PageSize, relocated);
            if (window < 0 || window >= maxPointers)
            {
                return false;
            }

            if (window != lastWindow)
            {
                distinctWindows++;
                if (distinctWindows > bitmapPageCapacity)
                {
                    return false;
                }

                lastWindow = window;
            }
        }

        return distinctWindows == bitmapPageCapacity;
    }

    private static bool ExtentFitsInlineWindow(
        long firstPage,
        long pageCount,
        int packedCount,
        List<long> holes,
        long retainedPrefixPages,
        long tableAppendEnd,
        int inlineCapacity)
    {
        if (pageCount <= 0)
        {
            return true;
        }

        long minPage = RelocatePage(firstPage, packedCount, holes, retainedPrefixPages, tableAppendEnd);
        long maxPage = RelocatePage(firstPage + pageCount - 1, packedCount, holes, retainedPrefixPages, tableAppendEnd);
        int basePage = minPage < inlineCapacity ? 0 : UsageMap.AlignInlineBasePage(minPage);
        return maxPage < (long)basePage + inlineCapacity;
    }

    private static (int Start, int End) LocatePackedRowBounds(byte[] dataPage, JetFormat format, int rowCount, int slot)
    {
        int start = BinaryPrimitives.ReadUInt16LittleEndian(dataPage.AsSpan(format.DataPage.RowsStart + (slot * 2), 2));
        int end = format.PageSize;
        for (int i = 0; i < rowCount; i++)
        {
            int other = BinaryPrimitives.ReadUInt16LittleEndian(dataPage.AsSpan(format.DataPage.RowsStart + (i * 2), 2));
            if (other > start && other < end)
            {
                end = other;
            }
        }

        return (start, end);
    }

    private static void RelocateLongValueChain(
        long firstPage,
        int firstRow,
        Func<long, byte[]> getAppendedPage,
        JetFormat format,
        Func<long, long> relocate)
    {
        long currentPage = firstPage;
        int currentRow = firstRow;
        int hops = 0;
        while (true)
        {
            if (++hops > 1_048_576)
            {
                throw Corrupt("Cycle detected while relocating staged LVAL chain.");
            }

            byte[] lvalPage = getAppendedPage(currentPage);
            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(lvalPage.AsSpan(format.DataPage.NumRows, 2));
            if (currentRow < 0 || currentRow >= numRows)
            {
                throw Corrupt($"Staged LVAL row {currentPage}:{currentRow} exceeds page row count {numRows}.");
            }

            int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(
                lvalPage.AsSpan(format.DataPage.RowsStart + (currentRow * 2), 2)) & Constants.DataPage.RowOffsetMask;
            uint nextBookmark = BinaryPrimitives.ReadUInt32LittleEndian(lvalPage.AsSpan(rowStart, 4));
            if (nextBookmark == 0)
            {
                break;
            }

            long nextPage = nextBookmark >> 8;
            uint nextRow = nextBookmark & 0xFFU;
            long relocatedNext = relocate(nextPage);
            BinaryPrimitives.WriteUInt32LittleEndian(
                lvalPage.AsSpan(rowStart, 4),
                (checked((uint)relocatedNext) << 8) | nextRow);
            currentPage = nextPage;
            currentRow = (int)nextRow;
        }
    }

    private static void RelocateUsageMapAllocation(
        CompactUsageMapBuilderResult mapAlloc,
        Func<long, byte[]> getAppendedPage,
        JetFormat format,
        Func<long, long> relocate)
    {
        if (mapAlloc.PageCount == 1)
        {
            RelocateInlineUsageMapPage(getAppendedPage(mapAlloc.FirstPageNumber), format, relocate);
            return;
        }

        RelocateReferenceUsageMapAllocation(mapAlloc, getAppendedPage, format, relocate);
    }

    private static void RelocateInlineUsageMapPage(
        byte[] mapPage,
        JetFormat format,
        Func<long, long> relocate)
    {
        int rowStart = format.PageSize - format.UsageMapRowSize;
        if (mapPage[0] != Constants.PageTypes.Data || mapPage[rowStart] != Constants.UsageMap.InlineMapType)
        {
            throw Corrupt("Expected an inline usage-map page during interior hole packing.");
        }

        int oldBasePage = BinaryPrimitives.ReadInt32LittleEndian(mapPage.AsSpan(rowStart + 1, 4));
        int bitmapBytes = format.UsageMapRowSize - Constants.UsageMap.InlineMapHeaderSize;
        var relocatedMembers = new List<long>();
        for (int byteIndex = 0; byteIndex < bitmapBytes; byteIndex++)
        {
            byte b = mapPage[rowStart + Constants.UsageMap.InlineBitmapOffset + byteIndex];
            if (b == 0)
            {
                continue;
            }

            for (int bit = 0; bit < 8; bit++)
            {
                if ((b & (1 << bit)) != 0)
                {
                    long oldMember = oldBasePage + (byteIndex * 8L) + bit;
                    relocatedMembers.Add(relocate(oldMember));
                }
            }
        }

        mapPage.AsSpan(0, format.PageSize).Clear();
        mapPage[0] = Constants.PageTypes.Data;
        mapPage[1] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(mapPage.AsSpan(2, 2), checked((ushort)(rowStart - format.DataPage.RowsStart - 2)));
        BinaryPrimitives.WriteUInt16LittleEndian(mapPage.AsSpan(format.DataPage.NumRows, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(mapPage.AsSpan(format.DataPage.RowsStart, 2), checked((ushort)rowStart));
        mapPage[rowStart] = Constants.UsageMap.InlineMapType;

        if (relocatedMembers.Count == 0)
        {
            return;
        }

        relocatedMembers.Sort();
        int inlineCapacity = bitmapBytes * 8;
        int newBasePage = relocatedMembers[0] < inlineCapacity ? 0 : UsageMap.AlignInlineBasePage(relocatedMembers[0]);
        BinaryPrimitives.WriteInt32LittleEndian(mapPage.AsSpan(rowStart + 1, 4), newBasePage);
        foreach (long member in relocatedMembers)
        {
            if (!UsageMap.TrySetInlinePageState(mapPage, rowStart, format.UsageMapRowSize, member, isMarked: true))
            {
                throw Corrupt($"Relocated page {member} did not fit in inline usage map anchored at {newBasePage}.");
            }
        }
    }

    private static void RelocateReferenceUsageMapAllocation(
        CompactUsageMapBuilderResult mapAlloc,
        Func<long, byte[]> getAppendedPage,
        JetFormat format,
        Func<long, long> relocate)
    {
        byte[] rowPage = getAppendedPage(mapAlloc.FirstPageNumber);
        int rowStart = format.PageSize - format.UsageMapRowSize;
        if (rowPage[0] != Constants.PageTypes.Data || rowPage[rowStart] != Constants.UsageMap.ReferenceMapType)
        {
            throw Corrupt("Expected a reference usage-map row page during interior hole packing.");
        }

        int pointerCount = UsageMap.ReferencePointerCount(format.UsageMapRowSize);
        int pagesPerMapPage = UsageMap.PagesPerReferenceMapPage(format.PageSize);
        int bitmapBytes = format.PageSize - Constants.UsageMap.ReferenceMapBitmapOffset;
        var relocatedMembers = new List<long>(checked((int)Math.Min(int.MaxValue, mapAlloc.ListedPages)));
        for (int pointerIndex = 0; pointerIndex < pointerCount; pointerIndex++)
        {
            int bitmapPageNumber = BinaryPrimitives.ReadInt32LittleEndian(rowPage.AsSpan(rowStart + 1 + (pointerIndex * 4), 4));
            if (bitmapPageNumber == 0)
            {
                continue;
            }

            byte[] oldBitmapPage = getAppendedPage(bitmapPageNumber);
            if (oldBitmapPage[0] != Constants.PageTypes.UsageMap)
            {
                throw Corrupt($"Expected usage-map bitmap page at {bitmapPageNumber} during interior hole packing.");
            }

            long windowBase = (long)pointerIndex * pagesPerMapPage;
            for (int byteIndex = 0; byteIndex < bitmapBytes; byteIndex++)
            {
                byte b = oldBitmapPage[Constants.UsageMap.ReferenceMapBitmapOffset + byteIndex];
                if (b == 0)
                {
                    continue;
                }

                for (int bit = 0; bit < 8; bit++)
                {
                    if ((b & (1 << bit)) != 0)
                    {
                        long oldMember = windowBase + (byteIndex * 8L) + bit;
                        relocatedMembers.Add(relocate(oldMember));
                    }
                }
            }
        }

        rowPage.AsSpan(0, format.PageSize).Clear();
        rowPage[0] = Constants.PageTypes.Data;
        rowPage[1] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(rowPage.AsSpan(2, 2), checked((ushort)(rowStart - format.DataPage.RowsStart - 2)));
        BinaryPrimitives.WriteUInt16LittleEndian(rowPage.AsSpan(format.DataPage.NumRows, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(rowPage.AsSpan(format.DataPage.RowsStart, 2), checked((ushort)rowStart));
        rowPage[rowStart] = Constants.UsageMap.ReferenceMapType;

        relocatedMembers.Sort();
        int currentWindow = -1;
        long nextBitmapOldPage = mapAlloc.FirstPageNumber + 1;
        long bitmapEndOldPage = mapAlloc.FirstPageNumber + mapAlloc.PageCount;
        byte[]? currentBitmapPage = null;

        foreach (long member in relocatedMembers)
        {
            int window = UsageMap.ReferencePointerIndex(format.PageSize, member);
            if (window < 0 || window >= pointerCount)
            {
                throw Corrupt($"Relocated page {member} exceeds reference usage-map capacity.");
            }

            if (window != currentWindow)
            {
                if (nextBitmapOldPage >= bitmapEndOldPage)
                {
                    throw Corrupt("Relocated reference usage-map members exceeded allocated bitmap page count.");
                }

                currentBitmapPage = getAppendedPage(nextBitmapOldPage);
                currentBitmapPage.AsSpan(0, format.PageSize).Clear();
                currentBitmapPage[0] = Constants.PageTypes.UsageMap;
                currentBitmapPage[1] = 1;
                long relocatedBitmapPage = relocate(nextBitmapOldPage);
                BinaryPrimitives.WriteInt32LittleEndian(
                    rowPage.AsSpan(rowStart + 1 + (window * 4), 4),
                    checked((int)relocatedBitmapPage));
                nextBitmapOldPage++;
                currentWindow = window;
            }

            if (!UsageMap.TrySetReferencePageState(currentBitmapPage!, format.PageSize, member, isMarked: true))
            {
                throw Corrupt($"Relocated page {member} could not be marked in reference bitmap window {window}.");
            }
        }

        if (nextBitmapOldPage != bitmapEndOldPage)
        {
            throw Corrupt("Relocated reference usage-map did not consume all allocated bitmap pages.");
        }
    }

    private static async ValueTask ReadExactAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), cancellationToken).ConfigureAwait(false);
            if (read <= 0)
            {
                throw Corrupt("Unexpected EOF while reading staged destination during interior hole packing.");
            }

            offset += read;
        }
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptPage, reason, new JetErrorInfo { Reason = reason });

    private sealed class PrivateStreamPageSource(Stream stream, int pageSize) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount => stream.Length / pageSize;

        public bool IsDisposed => false;

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
            => cancellationToken.ThrowIfCancellationRequested();

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken)
            => this.ReadUncachedPageAsync(pageNumber, cancellationToken);

        public async ValueTask<byte[]> ReadUncachedPageAsync(long pageNumber, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (pageNumber < 0 || pageNumber >= this.PageCount)
            {
                throw Corrupt($"Page {pageNumber} lies outside reopened destination [0, {this.PageCount}).");
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(pageSize);
            try
            {
                stream.Position = pageNumber * pageSize;
                int offset = 0;
                while (offset < pageSize)
                {
                    int read = await stream.ReadAsync(buffer.AsMemory(offset, pageSize - offset), cancellationToken).ConfigureAwait(false);
                    if (read <= 0)
                    {
                        throw Corrupt($"Unexpected EOF reading page {pageNumber} from reopened destination.");
                    }

                    offset += read;
                }

                return buffer;
            }
            catch
            {
                PageBuffers.Return(buffer);
                throw;
            }
        }
    }
}
