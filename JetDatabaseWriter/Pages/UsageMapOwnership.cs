namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Proves usage-map row and bitmap ownership before editing or releasing native storage.</summary>
internal static class UsageMapOwnership
{
    /// <summary>Inspects map ownership for mutation without collecting unrelated member pages.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="tableDefs">The definition reader.</param>
    /// <param name="catalogRows">The local catalog reader.</param>
    /// <param name="pages">The current page source.</param>
    /// <param name="ownerTdefPage">The definition containing the map pointer.</param>
    /// <param name="ownerOffset">The map pointer's logical offset.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The target map's members and its conservative storage ownership proof.</returns>
    internal static ValueTask<Proof> InspectForMutationAsync(JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows, IPageSource pages, long ownerTdefPage, int ownerOffset, CancellationToken cancellationToken)
        => InspectCoreAsync(format, tableDefs, catalogRows, pages, ownerTdefPage, ownerOffset, collectOtherMembers: false, cancellationToken);

    /// <summary>Inspects every local definition, including opaque descriptors, for references to a map's storage.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="tableDefs">The definition reader.</param>
    /// <param name="catalogRows">The local catalog reader.</param>
    /// <param name="pages">The current page source.</param>
    /// <param name="ownerTdefPage">The definition containing the map pointer.</param>
    /// <param name="ownerOffset">The map pointer's logical offset.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The map's members and conservative storage ownership proof.</returns>
    /// <exception cref="JetCorruptDataException">A definition or usage map is invalid or exceeds the ownership budget.</exception>
    /// <exception cref="JetLimitationException">The ownership reference budget is exceeded.</exception>
    internal static ValueTask<Proof> InspectAsync(JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows, IPageSource pages, long ownerTdefPage, int ownerOffset, CancellationToken cancellationToken)
        => InspectCoreAsync(format, tableDefs, catalogRows, pages, ownerTdefPage, ownerOffset, collectOtherMembers: true, cancellationToken);

    private static async ValueTask<Proof> InspectCoreAsync(JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows, IPageSource pages, long ownerTdefPage, int ownerOffset, bool collectOtherMembers, CancellationToken cancellationToken)
    {
        byte[] target = await tableDefs.ReadTDefBytesAsync(ownerTdefPage, cancellationToken).ConfigureAwait(false)
            ?? throw InvalidMap(ownerTdefPage, "The map's table definition is unreadable.");
        if (!UsageMap.TryReadPointer(target, ownerOffset, out UsageMapPointer pointer)
            || pointer.PageNumber <= 1 || pointer.PageNumber >= pages.PageCount)
        {
            throw InvalidMap(ownerTdefPage, "The usage-map pointer is outside the database.");
        }

        byte[] map = await pages.ReadPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
        Proof proof;
        try
        {
            if (map[0] != Constants.PageTypes.Data
                || Ri32(map, format.DataPage.TDefOff) != 0
                || !UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound bound)
                || bound.RowSize < 6)
            {
                throw InvalidMap(ownerTdefPage, "The usage-map row is unreadable.");
            }

            var members = new List<long>();
            if (!await UsageMap.TryEnumeratePagesAsync(map, bound, format.PageSize, pages.PageCount, 1, true, pages.ReadPageAsync, PageBuffers.Return, members, cancellationToken).ConfigureAwait(false))
            {
                throw InvalidMap(ownerTdefPage, "The usage-map bitmap is invalid.");
            }

            var bitmaps = new HashSet<long>();
            CollectBitmaps(map, bound, bitmaps);
            bool exclusive = true;
            int rowCount = Math.Min(Ru16(map, format.DataPage.NumRows), (format.PageSize - format.DataPage.RowsStart) / 2);
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int raw = Ru16(map, format.DataPage.RowsStart + (rowIndex * 2));
                if (rowIndex != pointer.RowIndex && (raw & Constants.DataPage.RowOffsetMask) == bound.RowStart)
                {
                    exclusive = false;
                }
            }

            proof = new Proof(pointer, bound.RowSize, members, [.. bitmaps], exclusive);
            if (map[bound.RowStart] == Constants.UsageMap.ReferenceMapType)
            {
                int bitmapCount = 0;
                for (int index = 0; index < UsageMap.ReferencePointerCount(bound.RowSize); index++)
                {
                    if (Ri32(map, bound.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (index * 4)) != 0)
                    {
                        bitmapCount++;
                    }
                }

                proof.HasExclusiveBitmapPages = bitmapCount == bitmaps.Count;
            }
        }
        finally
        {
            PageBuffers.Return(map);
        }

        HashSet<long> globalBitmaps = await ReadGlobalBitmapPagesAsync(format, pages, cancellationToken).ConfigureAwait(false)
            ?? throw InvalidMap(1, "The global usage map is unreadable.");

        if (globalBitmaps.Overlaps(proof.BitmapPages))
        {
            proof.HasExclusiveBitmapPages = false;
        }

        TableDef msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false)
            ?? throw InvalidMap(ownerTdefPage, "The local catalog definition is unreadable.");
        int budget = tableDefs.MaxLogicalBytes;
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken, Math.Max(1, budget / format.TDef.BlockEnd)).ConfigureAwait(false);
        await catalogRows.ValidateTableReferencesAsync(rows, cancellationToken).ConfigureAwait(false);
        var roots = new HashSet<long> { 2, ownerTdefPage };
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == Constants.SystemObjects.UserTableType
                || (row.ObjectType == Constants.SystemObjects.LinkedOdbcType && row.Id > 0))
            {
                _ = roots.Add(row.TDefPage);
            }
        }

        int maxCandidates = Math.Max(1, tableDefs.MaxLogicalBytes / 64);
        var candidateRows = new HashSet<UsageMapPointer> { pointer };
        uint targetReference = checked(((uint)pointer.PageNumber << 8) | (uint)pointer.RowIndex);
        foreach (long root in roots)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] definition = root == ownerTdefPage ? target : await tableDefs.ReadTDefBytesAsync(root, cancellationToken).ConfigureAwait(false)
                ?? throw InvalidMap(root, "A local table definition is unreadable.");
            if (definition.Length > budget)
            {
                throw InvalidMap(root, "The aggregate usage-map ownership definition budget is exceeded.");
            }

            budget -= definition.Length;
            TDefImage? image = TDefCodec.Parse(format, definition);
            if (image is null || image.Issues != TDefParseIssues.None)
            {
                throw InvalidMap(root, "A local definition has uncertain map references.");
            }

            // Native LVAL and unknown descriptors also carry map pointers. An
            // incidental byte match retains storage rather than risking an alias.
            for (int offset = 0; offset <= definition.Length - 4; offset++)
            {
                if ((offset & 4095) == 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }

                uint reference = Ru32(definition, offset);
                long pageNumber = reference >> 8;
                bool owner = root == ownerTdefPage && offset == ownerOffset;
                if (!owner && reference == targetReference)
                {
                    proof.IsExclusiveRow = false;
                }

                if (!owner && pageNumber == pointer.PageNumber)
                {
                    proof.IsDedicatedPage = false;
                }

                if (pageNumber > 1 && pageNumber < pages.PageCount)
                {
                    _ = candidateRows.Add(new UsageMapPointer(checked((int)(reference & 0xFF)), checked((int)pageNumber)));
                    if (candidateRows.Count > maxCandidates)
                    {
                        throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The usage-map ownership reference budget is exceeded.");
                    }
                }

                if (proof.BitmapPages.Contains(reference))
                {
                    proof.HasExclusiveBitmapPages = false;
                }
            }
        }

        foreach (long pageNumber in candidateRows.Select(candidate => (long)candidate.PageNumber).Distinct())
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] candidate = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (candidate[0] != Constants.PageTypes.Data || Ri32(candidate, format.DataPage.TDefOff) != 0)
                {
                    continue;
                }

                int rowCount = Ru16(candidate, format.DataPage.NumRows);
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    if (!UsageMap.TryGetRowBound(candidate, format.DataPage, format.PageSize, rowIndex, out RowBound row)
                        || (pageNumber == pointer.PageNumber && rowIndex == pointer.RowIndex))
                    {
                        continue;
                    }

                    if (candidateRows.Contains(new UsageMapPointer(rowIndex, checked((int)pageNumber)))
                        && candidate[row.RowStart] is Constants.UsageMap.InlineMapType or Constants.UsageMap.ReferenceMapType)
                    {
                        if (collectOtherMembers)
                        {
                            var otherMembers = new List<long>();
                            if (!await UsageMap.TryEnumeratePagesAsync(candidate, row, format.PageSize, pages.PageCount, 1, true, pages.ReadPageAsync, PageBuffers.Return, otherMembers, cancellationToken).ConfigureAwait(false))
                            {
                                throw InvalidMap(pageNumber, "Another usage-map bitmap is invalid.");
                            }

                            proof.OtherMappedPages.UnionWith(otherMembers);
                        }
                        else if (!await ValidateOtherMapAsync(format, pages, candidate, row, cancellationToken).ConfigureAwait(false))
                        {
                            throw InvalidMap(pageNumber, "Another usage-map bitmap is invalid.");
                        }
                    }

                    if (!candidateRows.Contains(new UsageMapPointer(rowIndex, checked((int)pageNumber)))
                        || candidate[row.RowStart] != Constants.UsageMap.ReferenceMapType)
                    {
                        continue;
                    }

                    for (int index = 0; index < UsageMap.ReferencePointerCount(row.RowSize); index++)
                    {
                        long bitmap = Ri32(candidate, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (index * 4));
                        if (proof.BitmapPages.Contains(bitmap))
                        {
                            proof.HasExclusiveBitmapPages = false;
                        }
                    }
                }
            }
            finally
            {
                PageBuffers.Return(candidate);
            }
        }

        if (collectOtherMembers && !proof.IsExclusiveRow)
        {
            proof.OtherMappedPages.UnionWith(proof.Pages);
        }

        proof.HasExclusiveBitmapPages &= proof.IsExclusiveRow;
        return proof;
    }

    private static async ValueTask<bool> ValidateOtherMapAsync(JetFormat format, IPageSource pages, byte[] map, RowBound row, CancellationToken cancellationToken)
    {
        if (map[row.RowStart] == Constants.UsageMap.InlineMapType)
        {
            if (row.RowSize <= Constants.UsageMap.InlineMapHeaderSize)
            {
                return false;
            }

            int basePage = Ri32(map, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset);
            int bitmapBytes = Math.Min(row.RowSize - Constants.UsageMap.InlineBitmapOffset, format.PageSize - row.RowStart - Constants.UsageMap.InlineBitmapOffset);
            return basePage >= 0 && bitmapBytes > 0
                && HasValidMemberBounds(map, row.RowStart + Constants.UsageMap.InlineBitmapOffset, bitmapBytes, basePage, pages.PageCount);
        }

        for (int index = 0; index < UsageMap.ReferencePointerCount(row.RowSize); index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int bitmapPage = Ri32(map, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (index * 4));
            if (bitmapPage == 0)
            {
                continue;
            }

            if (bitmapPage < 0 || bitmapPage >= pages.PageCount)
            {
                return false;
            }

            byte[] bitmap = await pages.ReadPageAsync(bitmapPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (bitmap[0] != Constants.PageTypes.UsageMap
                    || !HasValidMemberBounds(bitmap, Constants.UsageMap.ReferenceMapBitmapOffset, format.PageSize - Constants.UsageMap.ReferenceMapBitmapOffset, (long)index * UsageMap.PagesPerReferenceMapPage(format.PageSize), pages.PageCount))
                {
                    return false;
                }
            }
            finally
            {
                PageBuffers.Return(bitmap);
            }
        }

        return true;
    }

    private static bool HasValidMemberBounds(byte[] bitmap, int offset, int byteCount, long basePage, long totalPages)
    {
        // Mutation needs certainty about the map's storage, not a second copy
        // of its members. Only bits outside [1, PageCount) can invalidate it.
        int bitCapacity = byteCount * 8;
        int firstValid = checked((int)Math.Min(bitCapacity, Math.Max(0L, 1 - basePage)));
        int afterLastValid = checked((int)Math.Min(bitCapacity, Math.Max(0L, totalPages - basePage)));
        for (int index = 0; index < firstValid / 8; index++)
        {
            if (bitmap[offset + index] != 0)
            {
                return false;
            }
        }

        int firstBit = firstValid % 8;
        if (firstBit != 0 && (bitmap[offset + (firstValid / 8)] & ((1 << firstBit) - 1)) != 0)
        {
            return false;
        }

        int lastBit = afterLastValid % 8;
        if (lastBit != 0 && (bitmap[offset + (afterLastValid / 8)] & (0xFF << lastBit)) != 0)
        {
            return false;
        }

        for (int index = (afterLastValid + 7) / 8; index < byteCount; index++)
        {
            if (bitmap[offset + index] != 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Finds candidate map storage that no surviving definition or map row references.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="tableDefs">The definition reader.</param>
    /// <param name="catalogRows">The local catalog reader.</param>
    /// <param name="pages">The current page source.</param>
    /// <param name="candidatePages">Previously collected map-row and bitmap storage pages.</param>
    /// <param name="ignoredTdefPage">An old definition being replaced or dropped, or zero to inspect all definitions.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Only physical map storage with no surviving reference. Uncertain definitions retain all candidates.</returns>
    /// <exception cref="JetCorruptDataException">The surviving catalog cannot be traversed safely.</exception>
    internal static async ValueTask<HashSet<long>> FindUnreferencedStoragePagesAsync(JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows, IPageSource pages, IReadOnlyCollection<long> candidatePages, long ignoredTdefPage, CancellationToken cancellationToken)
    {
        var unreferenced = new HashSet<long>();
        if (ignoredTdefPage == 2 || candidatePages.Count == 0)
        {
            return unreferenced;
        }

        foreach (long candidate in candidatePages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (candidate <= 1 || candidate >= pages.PageCount)
            {
                continue;
            }

            byte[] page = await pages.ReadPageAsync(candidate, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] == Constants.PageTypes.UsageMap
                    || (page[0] == Constants.PageTypes.Data && Ri32(page, format.DataPage.TDefOff) == 0))
                {
                    _ = unreferenced.Add(candidate);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        HashSet<long>? globalBitmaps = await ReadGlobalBitmapPagesAsync(format, pages, cancellationToken).ConfigureAwait(false);
        if (globalBitmaps is null)
        {
            return [];
        }

        unreferenced.ExceptWith(globalBitmaps);

        TableDef msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false)
            ?? throw InvalidMap(2, "The local catalog definition is unreadable.");
        int budget = tableDefs.MaxLogicalBytes;
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken, Math.Max(1, budget / format.TDef.BlockEnd)).ConfigureAwait(false);
        if (ignoredTdefPage != 0)
        {
            _ = rows.RemoveAll(row => row.TDefPage == ignoredTdefPage);
        }

        await catalogRows.ValidateTableReferencesAsync(rows, cancellationToken).ConfigureAwait(false);
        var roots = new HashSet<long> { 2 };
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == Constants.SystemObjects.UserTableType
                || (row.ObjectType == Constants.SystemObjects.LinkedOdbcType && row.Id > 0))
            {
                _ = roots.Add(row.TDefPage);
            }
        }

        int maxReferences = Math.Max(1, tableDefs.MaxLogicalBytes / 64);
        var references = new HashSet<UsageMapPointer>();
        foreach (long root in roots)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[]? definition = await tableDefs.ReadTDefBytesAsync(root, cancellationToken).ConfigureAwait(false);
            if (definition is null || definition.Length > budget)
            {
                return [];
            }

            budget -= definition.Length;
            TDefImage? image = TDefCodec.Parse(format, definition);
            if (image is null || image.Issues != TDefParseIssues.None)
            {
                return [];
            }

            for (int offset = 0; offset <= definition.Length - 4; offset++)
            {
                if ((offset & 4095) == 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }

                uint value = Ru32(definition, offset);
                long pageNumber = value >> 8;
                _ = unreferenced.Remove(value);
                _ = unreferenced.Remove(pageNumber);
                if (pageNumber > 1 && pageNumber < pages.PageCount)
                {
                    _ = references.Add(new UsageMapPointer(checked((int)(value & 0xFF)), checked((int)pageNumber)));
                    if (references.Count > maxReferences)
                    {
                        return [];
                    }
                }
            }
        }

        foreach (IGrouping<int, UsageMapPointer> group in references.GroupBy(reference => reference.PageNumber))
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await pages.ReadPageAsync(group.Key, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, format.DataPage.TDefOff) != 0)
                {
                    continue;
                }

                foreach (UsageMapPointer reference in group)
                {
                    if (!UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, reference.RowIndex, out RowBound row)
                        || page[row.RowStart] != Constants.UsageMap.ReferenceMapType)
                    {
                        continue;
                    }

                    for (int index = 0; index < UsageMap.ReferencePointerCount(row.RowSize); index++)
                    {
                        _ = unreferenced.Remove(Ri32(page, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (index * 4)));
                    }
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return unreferenced;
    }

    /// <summary>Copies a readable map into private native-width row and bitmap storage.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="pager">The writer's pager.</param>
    /// <param name="allocator">The global page allocator.</param>
    /// <param name="proof">The inspected source map.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The new map pointer; its owner must be retargeted before old storage is released.</returns>
    /// <exception cref="JetCorruptDataException">The source row cannot fit in a page.</exception>
    internal static async ValueTask<UsageMapPointer> CreatePrivateCopyAsync(JetFormat format, Pager pager, PageAllocator allocator, Proof proof, CancellationToken cancellationToken)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        page[1] = 1;
        int rowSize = Math.Max(format.UsageMapRowSize, proof.RowSize);
        int rowStart = format.PageSize - rowSize;
        if (rowStart < format.DataPage.RowsStart + 2)
        {
            throw InvalidMap(proof.Pointer.PageNumber, "The usage-map row cannot fit in private storage.");
        }

        Wu16(page, format.DataPage.NumRows, 1);
        Wu16(page, format.DataPage.RowsStart, rowStart);
        Wu16(page, 2, rowStart - format.DataPage.RowsStart - 2);
        var editor = new UsageMapEditor(format, pager, allocator);
        try
        {
            await editor.WriteRowAsync(page, rowStart, rowSize, proof.Pages, keepReferencePages: false, cancellationToken).ConfigureAwait(false);
            long pageNumber = await allocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
            return new UsageMapPointer(0, checked((int)pageNumber));
        }
        catch
        {
            var allocated = new HashSet<long>();
            CollectBitmaps(page, new RowBound(0, rowStart, rowSize), allocated);
            foreach (long bitmap in allocated)
            {
                await allocator.ReleaseReservedPagesAsync(bitmap, 1).ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <summary>Releases an unlinked private copy after preparing a later copy fails.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="pager">The writer's pager.</param>
    /// <param name="allocator">The global page allocator.</param>
    /// <param name="pointer">A private copy that has never been published in a definition.</param>
    /// <returns>A task completing after the storage is released.</returns>
    internal static async ValueTask ReleasePrivateCopyAsync(JetFormat format, Pager pager, PageAllocator allocator, UsageMapPointer pointer)
    {
        byte[] page = await pager.ReadPageAsync(pointer.PageNumber, CancellationToken.None).ConfigureAwait(false);
        var bitmaps = new HashSet<long>();
        try
        {
            if (UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row))
            {
                CollectBitmaps(page, row, bitmaps);
            }
        }
        finally
        {
            PageBuffers.Return(page);
        }

        foreach (long bitmap in bitmaps)
        {
            await allocator.ReleaseReservedPagesAsync(bitmap, 1).ConfigureAwait(false);
        }

        await allocator.ReleaseReservedPagesAsync(pointer.PageNumber, 1).ConfigureAwait(false);
    }

    private static async ValueTask<HashSet<long>?> ReadGlobalBitmapPagesAsync(JetFormat format, IPageSource pages, CancellationToken cancellationToken)
    {
        byte[] page = await pages.ReadPageAsync(1, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.Data
                || !UsageMap.TryGetFirstRowBound(page, format.DataPage, format.PageSize, out RowBound row)
                || row.RowSize < 6
                || page[row.RowStart] is not Constants.UsageMap.InlineMapType and not Constants.UsageMap.ReferenceMapType)
            {
                return null;
            }

            var bitmaps = new HashSet<long>();
            CollectBitmaps(page, row, bitmaps);
            return bitmaps;
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private static JetCorruptDataException InvalidMap(long page, string reason)
        => new(JetErrorCode.CorruptTableDefinition, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static void CollectBitmaps(byte[] page, RowBound row, HashSet<long> bitmaps)
    {
        if (page[row.RowStart] == Constants.UsageMap.ReferenceMapType)
        {
            for (int index = 0; index < UsageMap.ReferencePointerCount(row.RowSize); index++)
            {
                int bitmap = Ri32(page, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset + (index * 4));
                if (bitmap > 0)
                {
                    _ = bitmaps.Add(bitmap);
                }
            }
        }
    }

    /// <summary>Current row contents and independently proven storage ownership.</summary>
    /// <param name="pointer">The original map pointer.</param>
    /// <param name="rowSize">The original row width.</param>
    /// <param name="pages">The map's member pages.</param>
    /// <param name="bitmapPages">The map's bitmap storage pages.</param>
    /// <param name="exclusiveRow">Whether the row directory is unambiguous.</param>
    internal sealed class Proof(UsageMapPointer pointer, int rowSize, IReadOnlyList<long> pages, IReadOnlyList<long> bitmapPages, bool exclusiveRow)
    {
        internal UsageMapPointer Pointer { get; } = pointer;

        internal int RowSize { get; } = rowSize;

        internal IReadOnlyList<long> Pages { get; } = pages;

        internal IReadOnlyList<long> BitmapPages { get; } = bitmapPages;

        internal HashSet<long> OtherMappedPages { get; } = [];

        internal bool IsExclusiveRow { get; set; } = exclusiveRow;

        internal bool HasExclusiveBitmapPages { get; set; } = true;

        internal bool IsDedicatedPage { get; set; } = true;
    }
}
