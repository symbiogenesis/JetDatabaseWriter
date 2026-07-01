namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Plans and applies in-place JET index B-tree mutations for <see cref="IndexMaintainer"/>.
/// </summary>
/// <param name="writer">The writer.</param>
/// <param name="pageAllocator">The page allocator.</param>
internal sealed class IndexBTreeEditor(AccessWriter writer, PageAllocator pageAllocator)
{
    internal async ValueTask<bool> TryRebuildCatalogIndexTreeAsync(
        IndexPageLayout layout,
        long tdefPage,
        long firstDp,
        int firstDpOffset,
        List<IndexEntry> addEntries,
        CancellationToken cancellationToken)
    {
        long leftmostLeaf = await this.DescendToLeftmostLeafAsync(layout, firstDp, cancellationToken).ConfigureAwait(false);
        if (leftmostLeaf <= 0)
        {
            return false;
        }

        var allExisting = new List<IndexEntry>();
        long walkPage = leftmostLeaf;
        int safetyBudget = 1_000_000;
        while (walkPage > 0)
        {
            if (--safetyBudget <= 0)
            {
                return false;
            }

            cancellationToken.ThrowIfCancellationRequested();
            byte[] leaf = await this.ReadAndClonePageAsync(walkPage, cancellationToken).ConfigureAwait(false);
            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                return false;
            }

            allExisting.AddRange(IndexPageCodec.DecodeLeafEntries(layout, leaf, writer.PageSizeBytes));
            walkPage = IndexPageCodec.ReadNextPage(layout, leaf);
        }

        List<IndexEntry>? spliced = IndexEntrySplicer.Splice(allExisting, addEntries, []);
        if (spliced is null)
        {
            return false;
        }

        IndexBTreeBuildResult build;
        try
        {
            long provisionalFirstPage = writer.PhysicalPageCount;
            build = IndexBTreeBuilder.Build(layout, writer.PageSizeBytes, tdefPage, spliced, provisionalFirstPage);
            long firstNewPage = await pageAllocator.ReserveContiguousPagesAsync(build.Pages.Count, cancellationToken).ConfigureAwait(false);
            if (firstNewPage != provisionalFirstPage)
            {
                build = IndexBTreeBuilder.Build(layout, writer.PageSizeBytes, tdefPage, spliced, firstNewPage);
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }

        long expectedPage = build.FirstPageNumber;
        foreach (byte[] page in build.Pages)
        {
            await writer.WritePageAsync(expectedPage, page, cancellationToken).ConfigureAwait(false);
            expectedPage++;
        }

        byte[] currentTdef = await this.ReadAndClonePageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        Wi32(currentTdef, firstDpOffset, checked((int)build.RootPageNumber));
        await writer.WritePageAsync(tdefPage, currentTdef, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Reads the 4-byte big-endian child-page pointer at the END of the LAST
    /// entry on an intermediate (<c>0x03</c>) page. Each intermediate entry
    /// trails with <c>[3 B BE data page][1 B data row][4 B BE child page]</c>;
    /// the bitmask-driven entry layout means the last entry ends exactly at
    /// <c>payloadEnd</c>, so the child pointer occupies
    /// <c>[payloadEnd-4, payloadEnd)</c>.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="layout">The layout.</param>
    private static long ReadLastChildPointer(byte[] page, int pageSize, IndexPageLayout layout)
    {
        if (page == null || page.Length < pageSize)
        {
            return 0;
        }

        int freeSpace = Ru16(page, 2);
        int payloadEnd = pageSize - freeSpace;
        if (payloadEnd < layout.FirstEntryOffset + 8)
        {
            return 0;
        }

        return IndexPageCodec.DecodeIntermediateChildPointer(page, payloadEnd - 4);
    }

    /// <summary>
    /// Reads a page through the writer cache and returns a caller-owned clone.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<byte[]> ReadAndClonePageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] pageBytes = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])pageBytes.Clone();
        }
        finally
        {
            AccessBase.ReturnPage(pageBytes);
        }
    }

    /// <summary>
    /// Appends <paramref name="pages"/> to the end of the file in order,
    /// verifying each lands at the next sequential page number. Returns
    /// <see langword="false"/> if the stream was extended concurrently (a
    /// partial append leaves only orphans, so the caller bails safely).
    /// </summary>
    /// <param name="pages">The pages to append, in order.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> TryAppendContiguousAsync(IReadOnlyList<byte[]> pages, CancellationToken cancellationToken)
    {
        long expected = writer.PhysicalPageCount;
        for (int i = 0; i < pages.Count; i++)
        {
            long appended = await writer.AppendPageAsync(pages[i], cancellationToken).ConfigureAwait(false);
            if (appended != expected)
            {
                return false;
            }

            expected++;
        }

        return true;
    }

    /// <summary>
    /// Reads <paramref name="page"/>, patches its <c>prev_page</c> sibling
    /// pointer to <paramref name="prevPage"/>, and writes it back.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page to patch.</param>
    /// <param name="prevPage">The new prev_page value.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask PatchPrevPointerAsync(IndexPageLayout layout, long page, long prevPage, CancellationToken cancellationToken)
    {
        byte[] bytes = await this.ReadAndClonePageAsync(page, cancellationToken).ConfigureAwait(false);
        IndexPageCodec.WritePrevPage(layout, bytes, prevPage);
        await writer.WritePageAsync(page, bytes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads <paramref name="page"/>, patches its <c>next_page</c> sibling
    /// pointer to <paramref name="nextPage"/>, and writes it back.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page to patch.</param>
    /// <param name="nextPage">The new next_page value.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask PatchNextPointerAsync(IndexPageLayout layout, long page, long nextPage, CancellationToken cancellationToken)
    {
        byte[] bytes = await this.ReadAndClonePageAsync(page, cancellationToken).ConfigureAwait(false);
        IndexPageCodec.WriteNextPage(layout, bytes, nextPage);
        await writer.WritePageAsync(page, bytes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Allocates the page-number array for an N-way leaf split. The first
    /// page reuses <paramref name="originalPage"/>; pages 1..N-1 are
    /// consecutive starting at <paramref name="firstNewPage"/>. Used by
    /// both surgical split paths so the (file-end / staging-counter)
    /// allocation source is the only thing the caller varies.
    /// </summary>
    /// <param name="originalPage">The original page.</param>
    /// <param name="count">The count.</param>
    /// <param name="firstNewPage">The first new page.</param>
    internal static long[] AllocateSplitPageNumbers(long originalPage, int count, long firstNewPage)
    {
        long[] pageNumbers = new long[count];
        pageNumbers[0] = originalPage;
        for (int p = 1; p < count; p++)
        {
            pageNumbers[p] = firstNewPage + (p - 1);
        }

        return pageNumbers;
    }

    /// <summary>
    /// Builds every page of an N-way leaf split into a fresh
    /// <c>byte[][]</c>. Each page's prev/next sibling pointers stitch
    /// the new pages into the existing chain (page 0's prev =
    /// <paramref name="leafPrev"/>, page N-1's next =
    /// <paramref name="leafNext"/>; interior pages point at their
    /// neighbours via <paramref name="pageNumbers"/>). Returns
    /// <see langword="null"/> on any single-entry overflow
    /// (<see cref="ArgumentOutOfRangeException"/> from the page builder).
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="splitPages">The split pages.</param>
    /// <param name="pageNumbers">The page numbers.</param>
    /// <param name="leafPrev">The leaf prev.</param>
    /// <param name="leafNext">The leaf next.</param>
    /// <param name="maxPrefixLength">The max prefix length.</param>
    internal byte[][]? TryBuildSplitLeafPages(
        IndexPageLayout layout,
        long tdefPage,
        SplitPages splitPages,
        long[] pageNumbers,
        long leafPrev,
        long leafNext,
        int maxPrefixLength)
    {
        int splitCount = splitPages.Count;
        byte[][] pageBytesAll = new byte[splitCount][];
        try
        {
            for (int p = 0; p < splitCount; p++)
            {
                long thisPrev = p == 0 ? leafPrev : pageNumbers[p - 1];
                long thisNext = p == splitCount - 1 ? leafNext : pageNumbers[p + 1];
                pageBytesAll[p] = IndexPageCodec.BuildLeafPage(
                    layout,
                    writer.PageSizeBytes,
                    tdefPage,
                    splitPages[p],
                    prevPage: thisPrev,
                    nextPage: thisNext,
                    tailPage: 0,
                    enablePrefixCompression: true,
                    maxPrefixLength: maxPrefixLength);
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }

        return pageBytesAll;
    }

    /// <summary>
    /// Builds every page of an N-way intermediate split, stitching prev/next
    /// sibling pointers across the new page numbers (page 0's prev =
    /// <paramref name="firstPrev"/>, page N-1's next =
    /// <paramref name="lastNext"/>) and stamping each page's recomputed
    /// <paramref name="tails"/> value. Returns <see langword="null"/> on any
    /// single-page overflow.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="splitInts">The per-page intermediate entry lists.</param>
    /// <param name="pageNumbers">Page numbers parallel to <paramref name="splitInts"/>.</param>
    /// <param name="firstPrev">The prev_page for the first split page.</param>
    /// <param name="lastNext">The next_page for the last split page.</param>
    /// <param name="tails">Per-page tail_page values.</param>
    private byte[][]? TryBuildSplitIntermediatePages(
        IndexPageLayout layout,
        long tdefPage,
        List<List<DecodedIntermediateEntry>> splitInts,
        long[] pageNumbers,
        long firstPrev,
        long lastNext,
        long[] tails)
    {
        int n = splitInts.Count;
        byte[][] pages = new byte[n][];
        try
        {
            for (int p = 0; p < n; p++)
            {
                long prev = p == 0 ? firstPrev : pageNumbers[p - 1];
                long next = p == n - 1 ? lastNext : pageNumbers[p + 1];
                byte[]? built = IndexBTreeBuilder.TryBuildIntermediatePage(
                    layout, writer.PageSizeBytes, tdefPage, splitInts[p], prev, next, tails[p]);
                if (built is null)
                {
                    return null;
                }

                pages[p] = built;
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }

        return pages;
    }

    internal SplitPages? TryBalancedTwoWayLeafSplit(
        IndexPageLayout layout,
        List<IndexEntry> entries,
        int maxPrefixLength)
    {
        if (entries.Count < 2)
        {
            return null;
        }

        SplitPages? best = null;
        int bestFreeSpaceDifference = int.MaxValue;
        int bestMinimumFreeSpace = -1;
        for (int splitIndex = 1; splitIndex < entries.Count; splitIndex++)
        {
            List<IndexEntry> left = entries.GetRange(0, splitIndex);
            List<IndexEntry> right = entries.GetRange(splitIndex, entries.Count - splitIndex);
            if (!this.TryMeasureLeafFreeSpace(layout, left, maxPrefixLength, out int leftFree)
                || !this.TryMeasureLeafFreeSpace(layout, right, maxPrefixLength, out int rightFree))
            {
                continue;
            }

            int freeSpaceDifference = Math.Abs(leftFree - rightFree);
            int minimumFreeSpace = Math.Min(leftFree, rightFree);
            if (freeSpaceDifference < bestFreeSpaceDifference
                || (freeSpaceDifference == bestFreeSpaceDifference && minimumFreeSpace > bestMinimumFreeSpace))
            {
                best = new SplitPages([left, right]);
                bestFreeSpaceDifference = freeSpaceDifference;
                bestMinimumFreeSpace = minimumFreeSpace;
            }
        }

        return best;
    }

    /// <summary>
    /// Adds a parent-intermediate op for a split leaf/intermediate.
    /// </summary>
    /// <param name="parentOps">The parent ops.</param>
    /// <param name="parentPageNumber">The parent page number.</param>
    /// <param name="originalIndex">The original index.</param>
    /// <param name="type">The JET column type or operation type.</param>
    /// <param name="newEntry">The new entry.</param>
    private static void AddParentOp(
        Dictionary<long, List<IntermediateOp>> parentOps,
        long parentPageNumber,
        int originalIndex,
        IntermediateOpType type,
        DecodedIntermediateEntry newEntry) => IndexHelpers.AddIntermediateOp(parentOps, parentPageNumber, new IntermediateOp(
            OriginalIndex: originalIndex,
            Type: type,
            NewEntry: newEntry));

    private bool TryMeasureLeafFreeSpace(
        IndexPageLayout layout,
        List<IndexEntry> entries,
        int maxPrefixLength,
        out int freeSpace)
    {
        freeSpace = 0;
        try
        {
            byte[] page = IndexPageCodec.BuildLeafPage(
                layout,
                writer.PageSizeBytes,
                parentTdefPage: 0,
                entries,
                enablePrefixCompression: true,
                maxPrefixLength: maxPrefixLength);
            freeSpace = Ru16(page, 2);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
    }

    /// <summary>
    /// Builds one summary entry (max key + child-page pointer) per page of a
    /// split. Summary <c>[0]</c> is the left-most page (which reuses the
    /// original page number); the rest are the new right pages in
    /// left-to-right order. Shared by every surgical leaf/intermediate split
    /// parent-update path.
    /// </summary>
    /// <param name="splitPages">The split pages.</param>
    /// <param name="pageNumbers">Page numbers parallel to <paramref name="splitPages"/>.</param>
    /// <exception cref="ArgumentException">Thrown when the inputs differ in length or are empty.</exception>
    internal static DecodedIntermediateEntry[] BuildSplitSummaries(SplitPages splitPages, long[] pageNumbers)
    {
        if (splitPages.Count != pageNumbers.Length || splitPages.Count == 0)
        {
            throw new ArgumentException("splitPages and pageNumbers must have the same nonzero length");
        }

        var summaries = new DecodedIntermediateEntry[splitPages.Count];
        for (int p = 0; p < splitPages.Count; p++)
        {
            summaries[p] = new DecodedIntermediateEntry(splitPages[p][^1], pageNumbers[p]);
        }

        return summaries;
    }

    private static void AddParentOpsForSplitPages(
        Dictionary<long, List<IntermediateOp>> parentOps,
        long parentPageNumber,
        int takenIndex,
        SplitPages splitPages,
        long[] pageNumbers)
    {
        DecodedIntermediateEntry[] summaries = BuildSplitSummaries(splitPages, pageNumbers);
        AddParentOp(parentOps, parentPageNumber, takenIndex, IntermediateOpType.Replace, summaries[0]);
        for (int p = 1; p < summaries.Length; p++)
        {
            AddParentOp(parentOps, parentPageNumber, takenIndex, IntermediateOpType.InsertAfter, summaries[p]);
        }
    }

    /// <summary>
    /// Descends an index B-tree from <paramref name="rootPage"/> through intermediate (<c>0x03</c>) levels by following the first child pointer of each.
    /// - Returns the page number of the leftmost leaf (<c>0x04</c>).
    /// - Returns 0 if the chain is malformed (unknown page type, missing child pointer, or excessive depth),
    ///   so the caller can fall back to the bulk-rebuild path.
    /// </summary>
    /// <param name="layout">Page layout descriptor (Jet3: offsets <c>0xF8</c>/<c>0x16</c>; Jet4: <c>0x1E0</c>/<c>0x1B</c>).</param>
    /// <param name="rootPage">Root page number of the index B-tree.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async ValueTask<long> DescendToLeftmostLeafAsync(IndexPageLayout layout, long rootPage, CancellationToken cancellationToken)
    {
        long current = rootPage;
        for (int depth = 0; depth < 16; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await this.ReadAndClonePageAsync(current, cancellationToken).ConfigureAwait(false);

            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                return current;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return 0;
            }

            long firstChild = IndexPageCodec.ReadFirstChildPointer(layout, page, writer.PageSizeBytes);
            if (firstChild <= 0)
            {
                return 0;
            }

            current = firstChild;
        }

        return 0;
    }

    /// <summary>
    /// Append-only tail-page fast path. When every key in
    /// <paramref name="addEntries"/> sorts strictly above the current
    /// tail-leaf max, splices them into the tail leaf and rewrites that one
    /// page in place (preserving its <c>prev_page</c>, re-emitting
    /// <c>next_page = 0</c>/<c>tail_page = 0</c>). No sibling-chain or
    /// intermediate-summary updates are done, so the rightmost intermediate
    /// summary becomes stale; per the §4.5 design, cursors compensate by
    /// following the intermediate's <c>tail_page</c> on overshoot (as
    /// <see cref="IndexCursor"/> does). Returns <see langword="true"/> on
    /// success; <see langword="false"/> (missing root <c>tail_page</c>, an
    /// insert key &lt;= tail max, or single-page overflow) falls the caller
    /// through to the descend-walk-rebuild path.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="rootPage">The root page.</param>
    /// <param name="addEntries">The add entries.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<bool> TryAppendToTailLeafAsync(
        IndexPageLayout layout,
        long tdefPage,
        byte[] rootPage,
        List<IndexEntry> addEntries,
        CancellationToken cancellationToken)
    {
        long tailLeafPage = IndexPageCodec.ReadTailPage(layout, rootPage);
        if (tailLeafPage <= 0)
        {
            return false;
        }

        byte[] tailLeaf = await this.ReadAndClonePageAsync(tailLeafPage, cancellationToken).ConfigureAwait(false);

        if (tailLeaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        long tailPrev = IndexPageCodec.ReadPrevPage(layout, tailLeaf);
        long tailNext = IndexPageCodec.ReadNextPage(layout, tailLeaf);
        if (tailNext != 0)
        {
            // The tail leaf must be the rightmost leaf (next_page == 0). If
            // a previous fast-path append already grew the chain and the
            // root's tail_page wasn't updated, give up — the bulk path will
            // resync the whole tree.
            return false;
        }

        int originalTailPrefLen = Ru16(tailLeaf, layout.PrefLenOffset);

        List<IndexEntry> existingTail = IndexPageCodec.DecodeLeafEntries(layout, tailLeaf, writer.PageSizeBytes);

        // Every new key must sort strictly after the current tail max.
        // Empty tail leaf trivially satisfies the predicate.
        if (existingTail.Count > 0)
        {
            byte[] tailMax = existingTail[^1].Key;
            for (int i = 0; i < addEntries.Count; i++)
            {
                if (IndexHelpers.CompareKeyBytes(addEntries[i].Key, tailMax) <= 0)
                {
                    return false;
                }
            }
        }

        // Splice (existing tail entries unchanged + new entries appended).
        // Splice() handles the (no-removes, sorted-merge) case efficiently;
        // since adds already sort > existing max, the stable merge produces
        // existing-then-new in the right order.
        List<IndexEntry>? spliced = IndexEntrySplicer.Splice(
            existingTail,
            addEntries,
            []);
        if (spliced is null)
        {
            return false;
        }

        byte[] rewritten;
        try
        {
            rewritten = IndexPageCodec.BuildLeafPage(
                layout,
                writer.PageSizeBytes,
                tdefPage,
                spliced,
                prevPage: tailPrev,
                nextPage: 0,
                tailPage: 0,
                enablePrefixCompression: true,
                maxPrefixLength: originalTailPrefLen);
        }
        catch (ArgumentOutOfRangeException)
        {
            // Tail leaf would overflow a single page. Fall through to the
            // bulk path, which will resnap the tree (and emit a fresh tail leaf).
            return false;
        }

        await writer.WritePageAsync(tailLeafPage, rewritten, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Surgical single-leaf mutation: when every change in the batch lands on
    /// the same leaf (verified by path-capturing descent) and the spliced
    /// entries either fit one page or split N-way without overflowing the
    /// parent intermediate, rewrites the affected leaf (and any ancestor
    /// summaries) in place at their existing page numbers. Returns
    /// <see langword="false"/> on any bail (multi-leaf change-set, leaf
    /// becomes empty, parent overflow, descent overshoot into a tail-page
    /// chain, malformed page, or encoder rejection); the caller then falls
    /// through to the bulk rebuild.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="firstDp">The first data page.</param>
    /// <param name="addEntries">The add entries.</param>
    /// <param name="removeEntries">The remove entries.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<bool> TrySurgicalMultiLevelMaintainAsync(
        IndexPageLayout layout,
        long tdefPage,
        long firstDp,
        List<IndexEntry> addEntries,
        List<IndexEntry> removeEntries,
        CancellationToken cancellationToken)
    {
        if (addEntries.Count == 0 && removeEntries.Count == 0)
        {
            return true;
        }

        bool hasAdds = addEntries.Count > 0;
        byte[] firstKey = hasAdds ? addEntries[0].Key : removeEntries[0].Key;
        var path = new List<DescentStep>();
        long targetLeafPage = await this.DescendCapturingAsync(layout, firstDp, firstKey, path, cancellationToken).ConfigureAwait(false);
        if (targetLeafPage <= 0 || path.Count == 0)
        {
            // Either descent overshot (search key > every summary, follows
            // tail_page) or the root was a leaf (single-root-leaf path
            // should have caught it). Either way: bail.
            return false;
        }

        int firstUncheckedAdd = hasAdds ? 1 : 0;
        for (int i = firstUncheckedAdd; i < addEntries.Count; i++)
        {
            if (!IndexHelpers.ConfirmKeyTargetsSamePath(path, addEntries[i].Key))
            {
                return false;
            }
        }

        int firstUncheckedRemove = hasAdds ? 0 : 1;
        for (int i = firstUncheckedRemove; i < removeEntries.Count; i++)
        {
            if (!IndexHelpers.ConfirmKeyTargetsSamePath(path, removeEntries[i].Key))
            {
                return false;
            }
        }

        byte[] leaf = await this.ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

        if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        List<IndexEntry> existingLeafEntries = IndexPageCodec.DecodeLeafEntries(layout, leaf, writer.PageSizeBytes);
        if (existingLeafEntries.Count == 0)
        {
            // Empty leaf — descent shouldn't normally land here. Bail.
            return false;
        }

        var removePtrs = new List<(long DataPage, byte DataRow)>(removeEntries.Count);
        for (int i = 0; i < removeEntries.Count; i++)
        {
            IndexEntry removeEntry = removeEntries[i];
            removePtrs.Add((removeEntry.DataPage, removeEntry.DataRow));
        }

        List<IndexEntry>? spliced = IndexEntrySplicer.Splice(existingLeafEntries, addEntries, removePtrs);
        if (spliced is not { Count: > 0 })
        {
            // Splice rejection and empty-leaf underflow are out of scope for this path.
            return false;
        }

        long leafPrev = IndexPageCodec.ReadPrevPage(layout, leaf);
        long leafNext = IndexPageCodec.ReadNextPage(layout, leaf);
        long leafTail = IndexPageCodec.ReadTailPage(layout, leaf);
        int originalPrefLen = Ru16(leaf, layout.PrefLenOffset);

        byte[] oldMaxKey = existingLeafEntries[^1].Key;

        byte[]? rebuilt = IndexPageCodec.TryBuildLeafPage(
            layout, writer.PageSizeBytes, tdefPage, spliced, leafPrev, leafNext, leafTail);
        if (rebuilt != null)
        {
            IndexEntry newLast = spliced[^1];
            List<(long PageNum, byte[] Bytes)>? ancestorWrites = null;

            if (IndexHelpers.CompareKeyBytes(newLast.Key, oldMaxKey) != 0)
            {
                var newSummary = new DecodedIntermediateEntry(new(newLast.Key, newLast.DataPage, newLast.DataRow), ChildPage: targetLeafPage);
                ancestorWrites = this.PrepareAncestorReplaceWrites(layout, tdefPage, path, newSummary);
                if (ancestorWrites is null)
                {
                    return false;
                }
            }

            await writer.WritePageAsync(targetLeafPage, rebuilt, cancellationToken).ConfigureAwait(false);
            if (ancestorWrites is not null)
            {
                foreach ((long pn, byte[] bytes) in ancestorWrites)
                {
                    await writer.WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
                }
            }

            return true;
        }

        // Bails only if a single entry exceeds page payload area.
        SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, writer.PageSizeBytes, spliced);
        if (splitPages is null)
        {
            return false;
        }

        // First page reuses the original leaf page; remaining pages are
        // freshly appended at end-of-file.
        int splitCount = splitPages.Count;
        long firstFreshPage = writer.PhysicalPageCount;
        long[] pageNumbers = AllocateSplitPageNumbers(targetLeafPage, splitCount, firstFreshPage);

        byte[][]? pageBytesAll = this.TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, originalPrefLen);
        if (pageBytesAll is null)
        {
            return false;
        }

        DecodedIntermediateEntry[] summaries = BuildSplitSummaries(splitPages, pageNumbers);
        List<(long PageNum, byte[] Bytes)>? splitAncestorWrites = this.PrepareAncestorSplitWrites(
            layout, tdefPage, path, summaries);
        if (splitAncestorWrites is null)
        {
            return false;
        }

        // Commit order (no transactions; minimise observable half-state):
        //   (a) Append every new right page (orphans only on a partial append).
        //   (b) Patch leafNext.prev_page to point at the LAST new page.
        //   (c) Rewrite the original leaf in place as the new LEFT-most.
        //   (d) Rewrite parent + ancestors in place with the new summaries.
        long lastSplitPage = pageNumbers[^1];
        if (!await this.TryAppendContiguousAsync(new ArraySegment<byte[]>(pageBytesAll, 1, splitCount - 1), cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        if (leafNext > 0)
        {
            await this.PatchPrevPointerAsync(layout, leafNext, lastSplitPage, cancellationToken).ConfigureAwait(false);
        }

        await writer.WritePageAsync(targetLeafPage, pageBytesAll[0], cancellationToken).ConfigureAwait(false);

        foreach ((long pn, byte[] bytes) in splitAncestorWrites)
        {
            await writer.WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Descends an index B-tree from <paramref name="rootPage"/>, picking the
    /// child at each intermediate level by <paramref name="searchKey"/> (first
    /// summary &gt;= key wins, mirroring
    /// <see cref="IndexCursor.ContainsKeyAsync"/>) and pushing each level
    /// (page number, raw bytes, decoded entries, followed-child index) onto
    /// <paramref name="path"/>. Returns the leaf page reached, or 0 on any
    /// failure (overshoot, malformed page, excessive depth) — surgical
    /// mutation bails on 0. When <paramref name="allowTailOvershoot"/> is
    /// <see langword="true"/>, an overshoot follows <c>tail_page</c> (or the
    /// last child pointer) without recording the step, as the catalog-splice
    /// path doesn't need a clean (page, taken-index) pair at every level.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="rootPage">The root page.</param>
    /// <param name="searchKey">The search key.</param>
    /// <param name="path">Optional collector for the descent steps taken.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <param name="allowTailOvershoot">Whether to follow the page tail pointer when the search key is beyond the last entry.</param>
    internal async ValueTask<long> DescendCapturingAsync(
        IndexPageLayout layout,
        long rootPage,
        byte[] searchKey,
        List<DescentStep> path,
        CancellationToken cancellationToken,
        bool allowTailOvershoot = false)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await this.ReadAndClonePageAsync(current, cancellationToken).ConfigureAwait(false);

            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                return current;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return 0;
            }

            List<DecodedIntermediateEntry> entries =
                IndexPageCodec.DecodeIntermediateEntries(layout, page, writer.PageSizeBytes);
            if (entries.Count == 0)
            {
                return 0;
            }

            int idx = IndexHelpers.SelectChildIndexFromDecoded(entries, searchKey);
            if (idx < 0)
            {
                if (!allowTailOvershoot)
                {
                    // Search key sorts strictly above every summary on this
                    // intermediate. The cursor would follow tail_page here,
                    // but the surgical path needs a clean (page, taken-index)
                    // pair at every level for an in-place ancestor rewrite — bail.
                    return 0;
                }

                long tail = IndexPageCodec.ReadTailPage(layout, page);
                long nextChild = tail > 0 ? tail : ReadLastChildPointer(page, writer.PageSizeBytes, layout);
                if (nextChild <= 0)
                {
                    return 0;
                }

                current = nextChild;
                continue;
            }

            path.Add(new DescentStep(current, page, entries, idx));
            current = entries[idx].ChildPage;
            if (current <= 0)
            {
                return 0;
            }
        }

        return 0;
    }

    /// <summary>
    /// Computes the in-place rewrites required for a max-key change at the
    /// parent-of-leaf level. Replaces the entry at
    /// <c>path[^1].TakenIndex</c> with <paramref name="newSummary"/> (same
    /// child page, new key + summary row pointer). When that entry was the
    /// LAST on the parent intermediate, the parent's max key has changed
    /// too, so we walk up replacing the grandparent's entry that summarises
    /// this parent (and so on, up to the root). Returns <see langword="null"/>
    /// when any intermediate page would overflow on rebuild — caller bails
    /// to bulk rebuild without committing any partial state.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="path">Captured root-to-leaf descent path.</param>
    /// <param name="newSummary">The new summary.</param>
    private List<(long PageNum, byte[] Bytes)>? PrepareAncestorReplaceWrites(
        IndexPageLayout layout,
        long tdefPage,
        List<DescentStep> path,
        DecodedIntermediateEntry newSummary)
    {
        var writes = new List<(long PageNum, byte[] Bytes)>(path.Count);
        DecodedIntermediateEntry current = newSummary;
        for (int level = path.Count - 1; level >= 0; level--)
        {
            DescentStep step = path[level];
            List<DecodedIntermediateEntry> entries = step.Entries;

            var newEntries = new List<DecodedIntermediateEntry>(entries.Count);
            for (int i = 0; i < entries.Count; i++)
            {
                if (i == step.TakenIndex)
                {
                    newEntries.Add(current);
                }
                else
                {
                    newEntries.Add(entries[i]);
                }
            }

            byte[] pageBytes = step.PageBytes;
            (long prev, long next, long tail) = IndexPageCodec.ReadSiblingPointers(layout, pageBytes);
            int originalPrefLen = Ru16(pageBytes, layout.PrefLenOffset);

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, writer.PageSizeBytes, tdefPage, newEntries, prev, next, tail, originalPrefLen);
            if (rebuilt is null)
            {
                return null;
            }

            writes.Add((step.PageNumber, rebuilt));

            bool wasLast = step.TakenIndex == entries.Count - 1;
            if (!wasLast)
            {
                // Parent's max didn't change → no need to walk further up.
                return writes;
            }

            // Was last → grandparent's summary for this intermediate also
            // needs the new max key. Carry the new max upward; the
            // grandparent's entry's ChildPage is this intermediate's page.
            current = current with { ChildPage = step.PageNumber };
        }

        return writes;
    }

    /// <summary>
    /// Computes the in-place rewrites required for a leaf split. At the
    /// parent-of-leaf level, replaces the single entry at
    /// <c>path[^1].TakenIndex</c> with every entry in
    /// <paramref name="summaries"/> (<c>[0]</c> is the left page; the rest
    /// are the new right pages). When the original entry was the LAST on the
    /// parent, the parent's max key has changed too and we propagate via
    /// <see cref="PrepareAncestorReplaceWrites"/> using the right-most new
    /// summary's key. Returns <see langword="null"/> on overflow at any
    /// captured ancestor level (recursive intermediate split lives in the
    /// cross-leaf path's <see cref="TryStageIntermediateRewritesAsync"/>;
    /// the single-leaf surgical path bails to the bulk rebuild when its parent
    /// overflows). Callers commit the writes after the leaf-side writes.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="path">Captured root-to-leaf descent path.</param>
    /// <param name="summaries">Per-page split summaries; <c>[0]</c> is the left page.</param>
    internal List<(long PageNum, byte[] Bytes)>? PrepareAncestorSplitWrites(
        IndexPageLayout layout,
        long tdefPage,
        List<DescentStep> path,
        IReadOnlyList<DecodedIntermediateEntry> summaries)
    {
        if (summaries.Count < 2)
        {
            return null;
        }

        int level = path.Count - 1;
        DescentStep step = path[level];
        List<DecodedIntermediateEntry> entries = step.Entries;

        var newEntries = new List<DecodedIntermediateEntry>(entries.Count + summaries.Count - 1);
        for (int i = 0; i < entries.Count; i++)
        {
            if (i == step.TakenIndex)
            {
                for (int s = 0; s < summaries.Count; s++)
                {
                    newEntries.Add(summaries[s]);
                }
            }
            else
            {
                newEntries.Add(entries[i]);
            }
        }

        byte[] parentBytes = step.PageBytes;
        (long parentPrev, long parentNext, long parentTail) = IndexPageCodec.ReadSiblingPointers(layout, parentBytes);
        int originalPrefLen = Ru16(parentBytes, layout.PrefLenOffset);

        byte[]? rebuiltParent = IndexBTreeBuilder.TryBuildIntermediatePage(
            layout, writer.PageSizeBytes, tdefPage, newEntries, parentPrev, parentNext, parentTail, originalPrefLen);
        if (rebuiltParent is null)
        {
            // Parent overflow on insertion of the new summary entries —
            // single-leaf surgical path has no recursive parent-split
            // (that lives in the cross-leaf staging walker). Bail.
            return null;
        }

        var writes = new List<(long PageNum, byte[] Bytes)>(path.Count) { (step.PageNumber, rebuiltParent) };

        bool wasLast = step.TakenIndex == entries.Count - 1;
        if (!wasLast || level == 0)
        {
            return writes;
        }

        // The right-most new summary became this parent's new max →
        // grandparent's summary entry for this parent must carry the new
        // max key.
        DecodedIntermediateEntry rightmost = summaries[^1];
        DecodedIntermediateEntry newAncestor = rightmost with { ChildPage = step.PageNumber };
        List<DescentStep> subPath = path.GetRange(0, level);
        List<(long PageNum, byte[] Bytes)>? more = this.PrepareAncestorReplaceWrites(layout, tdefPage, subPath, newAncestor);
        if (more is null)
        {
            return null;
        }

        writes.AddRange(more);
        return writes;
    }

    // ════════════════════════════════════════════════════════════════
    // cross-leaf surgical multi-level mutation
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Per-leaf bucket built by <see cref="GroupChangesByTargetLeafAsync"/>.
    /// Adds and removes routed to the same leaf are accumulated here; the
    /// captured intermediate path is shared across all keys that descended
    /// to this leaf (every key in the bucket picked the same child at every
    /// level above, by definition of "same target leaf").
    /// </summary>
    /// <param name="leafPage">The leaf page.</param>
    /// <param name="path">Captured root-to-leaf descent path for this leaf.</param>
    private sealed class LeafGroup(long leafPage, List<DescentStep> path)
    {
        /// <summary>Gets the page number of the target leaf.</summary>
        public long LeafPage { get; } = leafPage;

        /// <summary>Gets the captured path from root intermediate down to the parent-of-leaf.</summary>
        public List<DescentStep> Path { get; } = path;

        /// <summary>Gets the encoded inserts that landed on this leaf.</summary>
        public List<IndexEntry> Adds { get; } = [];

        /// <summary>Gets the row pointers whose entries should be removed from this leaf.</summary>
        public List<(long DataPage, byte DataRow)> RemovePtrs { get; } = [];
    }

    /// <summary>
    /// Per-leaf splice outcome captured in the cross-leaf maintenance I/O
    /// pass so the processing pass can run without re-reading or re-splicing
    /// the leaf.
    /// </summary>
    /// <param name="Spliced">The post-splice leaf entry list (empty = the leaf merges out).</param>
    /// <param name="Prev">The leaf's prev_page sibling pointer.</param>
    /// <param name="Next">The leaf's next_page sibling pointer.</param>
    /// <param name="Tail">The leaf's tail_page header.</param>
    /// <param name="PrefLen">The leaf's original prefix-compression length.</param>
    /// <param name="OldMaxKey">The leaf's pre-splice maximum key.</param>
    private readonly record struct LeafSplicePlan(
        List<IndexEntry> Spliced,
        long Prev,
        long Next,
        long Tail,
        int PrefLen,
        byte[] OldMaxKey);

    /// <summary>
    /// Cross-leaf surgical mutation. Invoked by
    /// <see cref="IndexMaintainer.TryMaintainIndexesIncrementalAsync"/> after
    /// the single-leaf path (<see cref="TrySurgicalMultiLevelMaintainAsync"/>)
    /// bails. Groups every change-set key by its target leaf via
    /// path-capturing descent, applies a per-leaf splice (in-place rewrite,
    /// N-way split, or merge-out), and aggregates all parent-intermediate
    /// updates into one rewrite per intermediate page. Returns
    /// <see langword="true"/> when every leaf was mutated at its existing page
    /// number (plus any appended split/root pages); returns
    /// <see langword="false"/> on any bail trigger, so the caller falls
    /// through to the bulk rebuild. Bail triggers:
    /// <list type="bullet">
    ///   <item>More than 64 distinct target leaves (the bulk walk is then faster).</item>
    ///   <item>Any per-leaf splice would need 3+ pages.</item>
    ///   <item>Any parent intermediate would overflow on its aggregated
    ///   summary updates.</item>
    ///   <item>A split's sibling-pointer patch would land on a leaf another
    ///   group is also mutating.</item>
    ///   <item>Any descent overshoots into a tail_page chain.</item>
    /// </list>
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="firstDp">The first data page.</param>
    /// <param name="firstDpOffset">The first data page offset.</param>
    /// <param name="addEntries">The add entries.</param>
    /// <param name="removeEntries">The remove entries.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<bool> TrySurgicalCrossLeafMaintainAsync(
        IndexPageLayout layout,
        long tdefPage,
        long firstDp,
        int firstDpOffset,
        List<IndexEntry> addEntries,
        List<IndexEntry> removeEntries,
        CancellationToken cancellationToken)
    {
        const int maxLeafGroupCount = 64;

        if (addEntries.Count == 0 && removeEntries.Count == 0)
        {
            return true;
        }

        // ── Phase A: per-key descent → group by leaf ─────────────────
        Dictionary<long, LeafGroup>? groups = await this.GroupChangesByTargetLeafAsync(
            layout,
            firstDp,
            addEntries,
            removeEntries,
            maxLeafGroupCount,
            cancellationToken).ConfigureAwait(false);
        if (groups is null)
        {
            return false;
        }

        // A single group reaching here means the single-leaf path bailed
        // (e.g. parent overflow); the code below still handles it (including
        // leaf-merge). Zero groups = nothing to do.
        if (groups.Count == 0)
        {
            return true;
        }

        // ── Phase B: per-leaf splice + classify outcome ──────────────
        // Everything is staged in memory; we commit only after every leaf
        // plan and aggregated intermediate rewrite validates.
        var existingPageRewrites = new Dictionary<long, byte[]>(groups.Count * 2);
        var newPageAppends = new List<byte[]>(groups.Count); // appended in order
        var leafNextPointerPatches = new Dictionary<long, long>(); // page → new prev_page
        var leafPrevPointerPatches = new Dictionary<long, long>(); // page → new next_page

        // Aggregated ops per parent intermediate, keyed by parent page; each
        // op references an ORIGINAL child index. Ops sharing an index (e.g.
        // Replace + InsertAfter for a split) keep declaration order.
        var parentOps = new Dictionary<long, List<IntermediateOp>>();

        // Each emptying leaf records its (prev, next) so the post-loop
        // boundary pass can re-link survivors across contiguous dead runs.
        var emptyingLeafSiblings = new Dictionary<long, (long Prev, long Next)>();

        long nextAllocatedPageNumber = writer.PhysicalPageCount;

        // Single I/O pass: read each target leaf once, splice its change-set,
        // and capture everything the processing pass needs (sibling pointers,
        // prefix length, old max key). Classifying emptying leaves up front
        // lets the merge logic below tolerate contiguous runs of dead leaves.
        var plans = new Dictionary<long, LeafSplicePlan>(groups.Count);
        var emptyingLeaves = new HashSet<long>();
        foreach (LeafGroup group in groups.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] leaf = await this.ReadAndClonePageAsync(group.LeafPage, cancellationToken).ConfigureAwait(false);
            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                return false;
            }

            List<IndexEntry> existing = IndexPageCodec.DecodeLeafEntries(layout, leaf, writer.PageSizeBytes);
            if (existing.Count == 0)
            {
                return false;
            }

            List<IndexEntry>? spliced = IndexEntrySplicer.Splice(existing, group.Adds, group.RemovePtrs);
            if (spliced is null)
            {
                return false;
            }

            plans[group.LeafPage] = new LeafSplicePlan(
                spliced,
                IndexPageCodec.ReadPrevPage(layout, leaf),
                IndexPageCodec.ReadNextPage(layout, leaf),
                IndexPageCodec.ReadTailPage(layout, leaf),
                Ru16(leaf, layout.PrefLenOffset),
                existing[^1].Key);

            if (spliced.Count == 0)
            {
                emptyingLeaves.Add(group.LeafPage);
            }
        }

        foreach (LeafGroup group in groups.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            LeafSplicePlan plan = plans[group.LeafPage];
            List<IndexEntry> spliced = plan.Spliced;
            long leafPrev = plan.Prev;
            long leafNext = plan.Next;
            long leafTail = plan.Tail;
            int originalPrefLen = plan.PrefLen;

            if (spliced.Count == 0)
            {
                // Leaf merges out: drop it (orphaned for Compact & Repair,
                // like the bulk path) and stage a parent Remove. tail_page
                // fix-up for a rightmost dead leaf is handled in
                // TryStageIntermediateRewrites. Bail when the parent has only
                // one child (would cascade-collapse the parent) or when a
                // leaf-chain neighbour is itself being content-mutated by
                // another group (needs coordinated writes); neighbours that
                // are themselves emptying are fine — the boundary-stitching
                // pass re-links surviving pages across whole dead runs.
                DescentStep mergeParent = group.Path[^1];
                if (mergeParent.Entries.Count < 2)
                {
                    return false;
                }

                if (leafPrev > 0 && groups.ContainsKey(leafPrev) && !emptyingLeaves.Contains(leafPrev))
                {
                    return false;
                }

                if (leafNext > 0 && groups.ContainsKey(leafNext) && !emptyingLeaves.Contains(leafNext))
                {
                    return false;
                }

                emptyingLeafSiblings[group.LeafPage] = (leafPrev, leafNext);
                AddParentOp(parentOps, mergeParent.PageNumber, mergeParent.TakenIndex, IntermediateOpType.Remove, default);

                continue;
            }

            byte[] oldMaxKey = plan.OldMaxKey;

            DescentStep parentStep = group.Path[^1];

            // ── Try in-place rewrite first ──
            byte[]? rebuilt = IndexPageCodec.TryBuildLeafPage(
                layout, writer.PageSizeBytes, tdefPage, spliced, leafPrev, leafNext, leafTail);
            if (rebuilt != null)
            {
                if (existingPageRewrites.ContainsKey(group.LeafPage))
                {
                    // Two groups targeted the same leaf — shouldn't happen
                    // (groups are keyed by leaf page). Defensive bail.
                    return false;
                }

                existingPageRewrites[group.LeafPage] = rebuilt;

                IndexEntry newLast = spliced[^1];
                if (IndexHelpers.CompareKeyBytes(newLast.Key, oldMaxKey) != 0)
                {
                    // Parent's summary entry for this leaf must be replaced.
                    AddParentOp(parentOps, parentStep.PageNumber, parentStep.TakenIndex, IntermediateOpType.Replace, new(newLast, group.LeafPage));
                }

                continue;
            }

            // ── N-way split ──
            // Greedy left-fill into N pages; bails only if a single entry
            // exceeds the page payload area.
            SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, writer.PageSizeBytes, spliced);
            if (splitPages is null)
            {
                return false;
            }

            int splitCount = splitPages.Count;

            // First page reuses group.LeafPage; remaining pages are
            // freshly allocated from the staging counter.
            long[] pageNumbers = AllocateSplitPageNumbers(group.LeafPage, splitCount, nextAllocatedPageNumber);
            nextAllocatedPageNumber += splitCount - 1;

            byte[][]? pageBytesAll = this.TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, originalPrefLen);
            if (pageBytesAll is null)
            {
                return false;
            }

            if (existingPageRewrites.ContainsKey(group.LeafPage))
            {
                return false;
            }

            existingPageRewrites[group.LeafPage] = pageBytesAll[0];
            for (int p = 1; p < splitCount; p++)
            {
                newPageAppends.Add(pageBytesAll[p]);
            }

            // Patch leafNext.prev_page to point at the LAST new page.
            // If leafNext is itself a leaf in another group, we'd need
            // coordinated writes — bail to keep this path simple.
            if (leafNext > 0)
            {
                if (groups.ContainsKey(leafNext))
                {
                    return false;
                }

                if (!leafNextPointerPatches.TryAdd(leafNext, pageNumbers[splitCount - 1]))
                {
                    // Two splits both want to patch the same neighbour leaf.
                    // Should not happen (each leaf has one prev), but defensive.
                    return false;
                }
            }

            // Parent ops: replace existing summary with the LEFT-most's
            // summary, then insert one summary per right page (N-1 of them)
            // immediately after, in left-to-right order. ApplyIntermediateOps
            // preserves declaration order at the same OriginalIndex.
            AddParentOpsForSplitPages(parentOps, parentStep.PageNumber, parentStep.TakenIndex, splitPages, pageNumbers);
        }

        // run-boundary stitching ───────────────────────────
        // For each contiguous run of one or more emptying leaves, patch the
        // surviving pages on either side so their sibling pointers skip OVER
        // every dead leaf in the run. This is the single place leaf-chain
        // re-linking happens for merges (a standalone dead leaf is just a
        // run of length one).
        foreach ((long deadPage, (long deadPrev, long deadNext)) in emptyingLeafSiblings)
        {
            // Only act at run boundaries: this dead leaf has at least one
            // non-emptying immediate neighbour OR a chain terminus (0).
            bool prevIsLeftBoundary = deadPrev == 0 || !emptyingLeafSiblings.ContainsKey(deadPrev);
            bool nextIsRightBoundary = deadNext == 0 || !emptyingLeafSiblings.ContainsKey(deadNext);

            if (!prevIsLeftBoundary && !nextIsRightBoundary)
            {
                continue; // strictly internal to a run; nothing to do
            }

            // Walk the run rightwards from deadPage to find the first
            // non-emptying page (or 0 = chain terminus).
            long survRight = deadNext;
            while (survRight > 0 && emptyingLeafSiblings.ContainsKey(survRight))
            {
                survRight = emptyingLeafSiblings[survRight].Next;
            }

            // Walk leftwards similarly.
            long survLeft = deadPrev;
            while (survLeft > 0 && emptyingLeafSiblings.ContainsKey(survLeft))
            {
                survLeft = emptyingLeafSiblings[survLeft].Prev;
            }

            // Apply the patches at run boundaries (idempotent — multiple
            // dead leaves in the same run all compute the same survLeft /
            // survRight, so TryAdd may legitimately collide; treat the
            // collision as success when the staged value matches).
            if (prevIsLeftBoundary && deadPrev > 0 && !groups.ContainsKey(deadPrev) && !leafPrevPointerPatches.TryAdd(deadPrev, survRight) &&
                    leafPrevPointerPatches[deadPrev] != survRight)
            {
                return false;
            }

            if (nextIsRightBoundary && deadNext > 0 && !groups.ContainsKey(deadNext) && !leafNextPointerPatches.TryAdd(deadNext, survLeft) &&
                    leafNextPointerPatches[deadNext] != survLeft)
            {
                return false;
            }
        }

        // ── Phase C: aggregate intermediate rewrites ─────────────────
        // Rebuild every touched parent intermediate in place (splitting and
        // propagating up the captured paths as needed); see
        // TryStageIntermediateRewritesAsync.
        var stagingState = new IntermediateStagingState
        {
            NextAllocatedPageNumber = nextAllocatedPageNumber,
        };
        bool stagingOk = await this.TryStageIntermediateRewritesAsync(
            layout,
            tdefPage,
            groups,
            parentOps,
            existingPageRewrites,
            stagingState,
            newPageAppends,
            cancellationToken).ConfigureAwait(false);

        if (!stagingOk)
        {
            return false;
        }

        // ── Phase D: validate + Phase E: commit ──────────────────────
        // Validation is already done implicitly (every staged page was built
        // via a try-call that returned null/false on overflow). Commit in
        // safe order: append new pages first (so their numbers exist before
        // any in-place rewrite references them), patch sibling pointers, then
        // rewrite all in-place pages.
        if (!await this.TryAppendContiguousAsync(newPageAppends, cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        foreach ((long neighbourPage, long newPrevValue) in leafNextPointerPatches)
        {
            await this.PatchPrevPointerAsync(layout, neighbourPage, newPrevValue, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long neighbourPage, long newNextValue) in leafPrevPointerPatches)
        {
            await this.PatchNextPointerAsync(layout, neighbourPage, newNextValue, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long pageNum, byte[] bytes) in existingPageRewrites)
        {
            await writer.WritePageAsync(pageNum, bytes, cancellationToken).ConfigureAwait(false);
        }

        // If the root intermediate split, patch the real-idx first_dp slot
        // on the TDEF page to point at the freshly-allocated root. The new
        // root page itself was already appended via newPageAppends above, so
        // the page number is stable.
        if (stagingState.NewRootPage is long newRootPage)
        {
            byte[] tdefBytes = await this.ReadAndClonePageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

            Wi32(tdefBytes, firstDpOffset, checked((int)newRootPage));
            await writer.WritePageAsync(tdefPage, tdefBytes, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Per-key path-capturing descent. Builds one <see cref="LeafGroup"/>
    /// per distinct target leaf, sharing the captured intermediate path
    /// across all keys that landed on the same leaf. Returns
    /// <see langword="null"/> on any descent failure (overshoot into
    /// tail_page chain, malformed page, encoder mismatch) or when the
    /// distinct-leaf count exceeds the cap supplied by the caller.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="firstDp">The first data page.</param>
    /// <param name="addEntries">The add entries.</param>
    /// <param name="removeEntries">The remove entries.</param>
    /// <param name="maxLeafGroupCount">The max leaf group count.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<Dictionary<long, LeafGroup>?> GroupChangesByTargetLeafAsync(
        IndexPageLayout layout,
        long firstDp,
        List<IndexEntry> addEntries,
        List<IndexEntry> removeEntries,
        int maxLeafGroupCount,
        CancellationToken cancellationToken)
    {
        var groups = new Dictionary<long, LeafGroup>();

        for (int i = 0; i < addEntries.Count; i++)
        {
            (byte[] key, long dp, byte dr) = addEntries[i];
            LeafGroup? g = await this.DescendOrLookupGroupAsync(layout, firstDp, key, groups, cancellationToken).ConfigureAwait(false);
            if (g is null)
            {
                return null;
            }

            var decoded = new IndexEntry(key, dp, dr);
            g.Adds.Add(decoded);

            if (groups.Count > maxLeafGroupCount)
            {
                return null;
            }
        }

        for (int i = 0; i < removeEntries.Count; i++)
        {
            (byte[] key, long dp, byte dr) = removeEntries[i];
            LeafGroup? g = await this.DescendOrLookupGroupAsync(layout, firstDp, key, groups, cancellationToken).ConfigureAwait(false);
            if (g is null)
            {
                return null;
            }

            g.RemovePtrs.Add((dp, dr));
            if (groups.Count > maxLeafGroupCount)
            {
                return null;
            }
        }

        return groups;
    }

    private async ValueTask<LeafGroup?> DescendOrLookupGroupAsync(
        IndexPageLayout layout,
        long firstDp,
        byte[] key,
        Dictionary<long, LeafGroup> groups,
        CancellationToken cancellationToken)
    {
        // Always descend: the page cache amortises the cost, and the
        // captured path lets us verify the key actually landed there
        // (reusing a stale path could mis-route a key that overshoots).
        var path = new List<DescentStep>();
        long leafPage = await this.DescendCapturingAsync(layout, firstDp, key, path, cancellationToken).ConfigureAwait(false);
        if (leafPage <= 0 || path.Count == 0)
        {
            return null;
        }

        if (groups.TryGetValue(leafPage, out LeafGroup? existing))
        {
            return existing;
        }

        var fresh = new LeafGroup(leafPage, path);
        groups[leafPage] = fresh;
        return fresh;
    }

    /// <summary>
    /// Mutable staging state shared between
    /// <see cref="TrySurgicalCrossLeafMaintainAsync"/> and
    /// <see cref="TryStageIntermediateRewritesAsync"/>. Replaces the
    /// <c>ref</c>/<c>out</c> parameters that the original synchronous helper
    /// used (async signatures cannot carry <c>ref</c>/<c>out</c>).
    /// </summary>
    private sealed class IntermediateStagingState
    {
        /// <summary>Gets or sets the next page number to allocate from the end of the file.</summary>
        public long NextAllocatedPageNumber { get; set; }

        /// <summary>Gets or sets the page number of the freshly-allocated root intermediate when the root split.</summary>
        public long? NewRootPage { get; set; }
    }

    /// <summary>
    /// helper. Returns the effective <c>tail_page</c> (rightmost
    /// leaf reachable through <paramref name="intermediatePage"/>'s subtree)
    /// taking pending mutations into account. Lookup priority:
    /// <list type="number">
    ///   <item><paramref name="overrides"/> (explicit per-page tail recorded
    ///   when an intermediate was rewritten or split earlier in the same
    ///   batch);</item>
    ///   <item><paramref name="rewrites"/> (staged in-memory rewrite of the
    ///   page \u2014 read its <c>tail_page</c> header bytes);</item>
    ///   <item>live page bytes via the page cache (untouched intermediates).</item>
    /// </list>
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="intermediatePage">The intermediate page.</param>
    /// <param name="overrides">The overrides.</param>
    /// <param name="rewrites">The rewrites.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<long> GetEffectiveTailPageAsync(
        IndexPageLayout layout,
        long intermediatePage,
        Dictionary<long, long> overrides,
        Dictionary<long, byte[]> rewrites,
        CancellationToken cancellationToken)
    {
        if (overrides.TryGetValue(intermediatePage, out long staged))
        {
            return staged;
        }

        if (rewrites.TryGetValue(intermediatePage, out byte[]? rewriteBytes))
        {
            return IndexPageCodec.ReadTailPage(layout, rewriteBytes);
        }

        byte[] raw = await writer.ReadPageAsync(intermediatePage, cancellationToken).ConfigureAwait(false);
        try
        {
            return IndexPageCodec.ReadTailPage(layout, raw);
        }
        finally
        {
            AccessBase.ReturnPage(raw);
        }
    }

    /// <summary>
    /// Stages rewrites for every parent intermediate touched by per-leaf ops,
    /// then propagates any resulting max-key changes up each LeafGroup's
    /// captured path. When an in-place rebuild overflows, the page is
    /// greedy-split N-way and the new summaries are either pushed to the
    /// grandparent (Replace + InsertAfter) or, if the splitting page is the
    /// root, used to build a fresh root whose <c>first_dp</c> the caller
    /// patches. Each split page's <c>tail_page</c> is its rightmost child's
    /// effective tail (its own leaf for parent-of-leaf pages, else the child
    /// intermediate's tail via staged overrides, staged rewrites, or a
    /// cache-backed read). Recursive splits up to root reallocation are
    /// supported; a single entry too large for any page still bails. Returns
    /// <see langword="false"/> on any such bail.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="groups">The groups.</param>
    /// <param name="parentOps">The parent ops.</param>
    /// <param name="existingPageRewrites">The existing page rewrites.</param>
    /// <param name="stagingState">The staging state.</param>
    /// <param name="newPageAppends">The new page appends.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> TryStageIntermediateRewritesAsync(
        IndexPageLayout layout,
        long tdefPage,
        Dictionary<long, LeafGroup> groups,
        Dictionary<long, List<IntermediateOp>> parentOps,
        Dictionary<long, byte[]> existingPageRewrites,
        IntermediateStagingState stagingState,
        List<byte[]> newPageAppends,
        CancellationToken cancellationToken)
    {
        stagingState.NewRootPage = null;

        // Track which intermediates are "parent-of-leaf" (children are
        // leaves, NOT intermediates). These are the only pages the leaf-split
        // helper is willing to split — splitting a higher-level intermediate
        // requires reading its children's tail_page values to recompute
        // the split halves' tail_page headers, handled by the recursive
        // helper below.
        var parentOfLeaf = new HashSet<long>(parentOps.Keys);

        // Per-touched-intermediate maps. Multiple groups may pass through the
        // same intermediate; they all carry identical canonical bytes (the
        // page cache returns the same content per call in single-writer mode,
        // as no mid-batch write touches these pages yet), so the first
        // DescentStep seen is the reference for header + original entries.
        var intermediateRefs = new Dictionary<long, DescentStep>(parentOps.Count * 2);
        var intermediateGrandparent = new Dictionary<long, (long ParentPage, int IndexInParent)>(parentOps.Count * 2);

        // tail_page propagation: when a splice drops a parent's rightmost
        // child (or a split appends a new one), the parent's tail_page must
        // point at the new rightmost leaf, and that cascades up to any
        // ancestor whose rightmost child is the page we changed. Recorded
        // here as we process deepest-first so shallower rebuilds inherit it.
        var intermediateTailOverrides = new Dictionary<long, long>(parentOps.Count * 2);

        // One pass over every captured path fills the reference-step,
        // grandparent, and deepest-level maps. Depth drives the deepest-first
        // processing order; parentOps starts keyed on parent-of-leaf pages
        // only, and propagating max-key changes adds shallower ones as we go.
        var depthOf = new Dictionary<long, int>(parentOps.Count * 2);
        foreach (LeafGroup group in groups.Values)
        {
            for (int level = 0; level < group.Path.Count; level++)
            {
                DescentStep step = group.Path[level];
                long pn = step.PageNumber;

                if (!intermediateRefs.ContainsKey(pn))
                {
                    intermediateRefs[pn] = step;
                }

                if (level > 0)
                {
                    DescentStep parent = group.Path[level - 1];
                    intermediateGrandparent[pn] = (parent.PageNumber, parent.TakenIndex);
                }

                if (!depthOf.TryGetValue(pn, out int existingDepth) || existingDepth < level)
                {
                    depthOf[pn] = level;
                }
            }
        }

        // Process pages in descending depth (deepest first).
        var pending = new List<long>(parentOps.Keys);
        while (pending.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Pick the deepest pending page.
            int deepestIdx = 0;
            int deepestDepth = depthOf.GetValueOrDefault(pending[0], -1);
            for (int i = 1; i < pending.Count; i++)
            {
                int cd = depthOf.GetValueOrDefault(pending[i], -1);
                if (cd > deepestDepth)
                {
                    deepestIdx = i;
                    deepestDepth = cd;
                }
            }

            long deepest = pending[deepestIdx];
            pending.RemoveAt(deepestIdx);

            if (!parentOps.TryGetValue(deepest, out List<IntermediateOp>? ops) || ops.Count == 0)
            {
                continue;
            }

            if (!intermediateRefs.TryGetValue(deepest, out DescentStep refStep))
            {
                // No descent passed through this page — shouldn't happen
                // because all ops were registered against pages we descended
                // through. Defensive bail.
                return false;
            }

            // Validate every op's OriginalIndex is in range.
            foreach (IntermediateOp op in ops)
            {
                if (op.OriginalIndex < 0 || op.OriginalIndex >= refStep.Entries.Count)
                {
                    return false;
                }
            }

            List<DecodedIntermediateEntry> newEntries =
                IndexHelpers.ApplyIntermediateOps(refStep.Entries, ops);

            if (newEntries.Count == 0)
            {
                // Cascading collapse: a delete batch emptied this intermediate.
                // Stage a Remove on the grandparent and re-enqueue it; the dead
                // page is orphaned (Compact & Repair sweeps it). A root collapse
                // (no grandparent) bails to the bulk path, which correctly emits
                // a fresh empty single-leaf root and patches first_dp.
                if (!intermediateGrandparent.TryGetValue(deepest, out (long ParentPage, int IndexInParent) gpCollapse))
                {
                    return false;
                }

                AddParentOp(parentOps, gpCollapse.ParentPage, gpCollapse.IndexInParent, IntermediateOpType.Remove, default);

                if (!pending.Contains(gpCollapse.ParentPage))
                {
                    pending.Add(gpCollapse.ParentPage);
                }

                // No staged rewrite for `deepest`: it's orphaned. Skip the
                // rest of the per-page rebuild path.
                continue;
            }

            byte[] origBytes = refStep.PageBytes;
            (long origPrev, long origNext, long origTail) = IndexPageCodec.ReadSiblingPointers(layout, origBytes);

            // Recompute tail_page from the post-mutation entries. Parent-of-
            // leaf pages: rightmost leaf = last entry's ChildPage. Higher
            // pages: inherit the rightmost child intermediate's effective tail
            // (GetEffectiveTailPageAsync — staged override, staged rewrite, or
            // live/disk header, which matters when a Remove exposes an
            // untouched rightmost child). Only applies when origTail != 0;
            // single-leaf-root state (origTail = 0) stays untouched.
            long newTail = origTail;
            if (origTail != 0)
            {
                long lastChildPage = newEntries[^1].ChildPage;
                if (parentOfLeaf.Contains(deepest))
                {
                    newTail = lastChildPage;
                }
                else
                {
                    newTail = await this.GetEffectiveTailPageAsync(
                        layout, lastChildPage, intermediateTailOverrides, existingPageRewrites, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            if (newTail != origTail)
            {
                intermediateTailOverrides[deepest] = newTail;
            }

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, writer.PageSizeBytes, tdefPage, newEntries, origPrev, origNext, newTail);
            if (rebuilt is null)
            {
                // Intermediate overflow → greedy left-fill split into N pages
                // (each new page freshly allocated). The grandparent then
                // absorbs the N summaries (Replace + (N-1) InsertAfter) and we
                // recurse into it; if this page was the root, we build a fresh
                // root over the split pages and signal the caller to patch
                // first_dp. Per-page tail_page is computed just below.
                List<List<DecodedIntermediateEntry>>? splitInts =
                    IndexHelpers.TryGreedySplitIntermediateInN(layout, writer.PageSizeBytes, tdefPage, newEntries);
                if (splitInts is null)
                {
                    // Single entry too big for any intermediate page — bail.
                    return false;
                }

                int nSplit = splitInts.Count;

                // First split page reuses `deepest`; remaining pages are
                // freshly allocated from the staging counter.
                long[] intPageNumbers = AllocateSplitPageNumbers(deepest, nSplit, stagingState.NextAllocatedPageNumber);
                stagingState.NextAllocatedPageNumber += nSplit - 1;

                // Compute each split page's tail_page.
                long[] intTails = new long[nSplit];
                if (parentOfLeaf.Contains(deepest))
                {
                    for (int p = 0; p < nSplit; p++)
                    {
                        DecodedIntermediateEntry lastEntry = splitInts[p][^1];

                        // Last split page inherits origTail when non-zero
                        // (preserves the existing rightmost-leaf pointer
                        // semantics on the rightmost subtree); other pages
                        // get their own rightmost child as the leaf tail.
                        intTails[p] = (p == nSplit - 1 && origTail != 0) ? origTail : lastEntry.ChildPage;
                    }
                }
                else
                {
                    for (int p = 0; p < nSplit; p++)
                    {
                        DecodedIntermediateEntry lastEntry = splitInts[p][^1];
                        intTails[p] = await this.GetEffectiveTailPageAsync(
                            layout, lastEntry.ChildPage, intermediateTailOverrides, existingPageRewrites, cancellationToken)
                            .ConfigureAwait(false);
                    }
                }

                byte[][]? intPageBytesAll = this.TryBuildSplitIntermediatePages(
                    layout, tdefPage, splitInts, intPageNumbers, origPrev, origNext, intTails);
                if (intPageBytesAll is null)
                {
                    return false;
                }

                if (existingPageRewrites.ContainsKey(deepest))
                {
                    return false;
                }

                existingPageRewrites[deepest] = intPageBytesAll[0];
                for (int p = 1; p < nSplit; p++)
                {
                    newPageAppends.Add(intPageBytesAll[p]);
                }

                // Record every split page's tail so any shallower split
                // that looks up these pages picks up the post-split values
                // without re-reading the (now stale) live pages.
                for (int p = 0; p < nSplit; p++)
                {
                    intermediateTailOverrides[intPageNumbers[p]] = intTails[p];
                }

                if (intermediateGrandparent.TryGetValue(deepest, out (long ParentPage, int IndexInParent) gpSplit))
                {
                    // Grandparent absorbs: Replace the original summary at
                    // IndexInParent with the FIRST split page's summary,
                    // then InsertAfter one summary per remaining split page
                    // in left-to-right order. Recurse into grandparent in
                    // case it also overflows.
                    // Use helper for Replace + InsertAfter ops for split intermediate pages
                    AddParentOpsForSplitPages(
                        parentOps,
                        gpSplit.ParentPage,
                        gpSplit.IndexInParent,
                        [.. splitInts.ConvertAll(s => s.ConvertAll(si => si.Entry))],
                        intPageNumbers);

                    if (!pending.Contains(gpSplit.ParentPage))
                    {
                        pending.Add(gpSplit.ParentPage);
                    }
                }
                else
                {
                    // No grandparent — this WAS the root intermediate.
                    // Allocate a fresh root with one summary entry per
                    // split page. tail_page of the new root = the LAST
                    // split page's tail (= rightmost leaf in the tree).
                    if (stagingState.NewRootPage.HasValue)
                    {
                        // Already split a root once in this batch (multi-
                        // group case); only one root is allowed. Bail.
                        return false;
                    }

                    long newRootPageAlloc = stagingState.NextAllocatedPageNumber++;

                    // Root summaries must point at the freshly split pages
                    // (intPageNumbers), exactly like the grandparent branch
                    // above — NOT at the original children carried in
                    // splitInts. Reuse the shared summary builder so both
                    // branches stay consistent.
                    DecodedIntermediateEntry[] rootEntries = BuildSplitSummaries(
                        [.. splitInts.ConvertAll(s => s.ConvertAll(si => si.Entry))],
                        intPageNumbers);

                    byte[]? newRootBytes;
                    try
                    {
                        newRootBytes = IndexBTreeBuilder.TryBuildIntermediatePage(
                            layout, writer.PageSizeBytes, tdefPage, rootEntries, prevPage: 0, nextPage: 0, tailPage: intTails[nSplit - 1]);
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        return false;
                    }

                    if (newRootBytes is null)
                    {
                        return false;
                    }

                    newPageAppends.Add(newRootBytes);
                    stagingState.NewRootPage = newRootPageAlloc;
                }

                continue;
            }

            if (existingPageRewrites.ContainsKey(deepest))
            {
                // An intermediate page should never collide with a leaf
                // rewrite (different page-type populations). Defensive bail.
                return false;
            }

            existingPageRewrites[deepest] = rebuilt;

            // Did the page's max key change? Compare new last entry to
            // original last entry's key.
            DecodedIntermediateEntry newMax = newEntries[^1];
            DecodedIntermediateEntry oldMax = refStep.Entries[^1];
            bool maxChanged = newMax != oldMax;

            if (maxChanged && intermediateGrandparent.TryGetValue(deepest, out (long ParentPage, int IndexInParent) gp))
            {
                // Propagate: grandparent's summary entry for this
                // intermediate (at IndexInParent) carries the new max key but
                // must keep pointing at `deepest` (the rebuilt intermediate),
                // NOT at deepest's last child (newMax.ChildPage is a leaf).
                AddParentOp(parentOps, gp.ParentPage, gp.IndexInParent, IntermediateOpType.Replace, newMax with { ChildPage = deepest });

                if (!pending.Contains(gp.ParentPage))
                {
                    pending.Add(gp.ParentPage);
                }
            }

            // If maxChanged but no grandparent (this WAS the root) — that's
            // fine, the root's max key doesn't need propagation anywhere.
        }

        return true;
    }
}
