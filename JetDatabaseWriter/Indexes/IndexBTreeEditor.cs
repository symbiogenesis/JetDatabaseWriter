namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Plans and applies in-place JET index B-tree mutations for <see cref="IndexMaintainer"/>.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pager">The writer's page file, through which index pages are written and appended.</param>
/// <param name="tdefWriter">Writes a moved index root back into the table's TDEF chain.</param>
/// <param name="pageAllocator">The page allocator.</param>
internal sealed class IndexBTreeEditor(JetFormat format, Pager pager, TDefWriter tdefWriter, PageAllocator pageAllocator)
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

            allExisting.AddRange(IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize));
            walkPage = IndexPageCodec.ReadNextPage(layout, leaf);
        }

        List<IndexEntry>? spliced = IndexEntrySplicer.Splice(allExisting, addEntries, []);
        if (spliced is null)
        {
            return false;
        }

        // TryPlaceTreeAsync releases its own run when it cannot place the
        // tree; the run is linked by the first_dp patch below.
        var runs = new ReservedPageRuns(pageAllocator);
        IndexBTreeBuildResult? build = await this.TryPlaceTreeAsync(layout, tdefPage, spliced, runs, cancellationToken).ConfigureAwait(false);
        if (build is not { } placed)
        {
            return false;
        }

        runs.MarkLinked();
        await tdefWriter.WriteInt32Async(tdefPage, firstDpOffset, checked((int)placed.RootPageNumber), cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Builds a B-tree over <paramref name="entries"/>, reserves its pages,
    /// and writes them. See <see cref="TryPlaceTreeAsync(Func{long, IndexBTreeBuildResult}, ReservedPageRuns, CancellationToken)"/>.
    /// </summary>
    /// <param name="layout">The index page layout.</param>
    /// <param name="tdefPage">The owning table's TDEF page, stamped on every index page.</param>
    /// <param name="entries">The sorted leaf entries.</param>
    /// <param name="runs">Records the written run until the caller links it.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The placed tree, or <see langword="null"/> when it cannot be built.</returns>
    internal ValueTask<IndexBTreeBuildResult?> TryPlaceTreeAsync(
        IndexPageLayout layout,
        long tdefPage,
        IReadOnlyList<IndexEntry> entries,
        ReservedPageRuns runs,
        CancellationToken cancellationToken)
        => this.TryPlaceTreeAsync(
            firstPage => IndexBTreeBuilder.Build(layout, format.PageSize, tdefPage, entries, firstPage),
            runs,
            cancellationToken);

    /// <summary>
    /// Builds a tree with <paramref name="buildAt"/> at the provisional end of
    /// file, then places it with <see cref="PlaceBuiltTreeAsync"/>. Returns
    /// <see langword="null"/>, with nothing reserved, when the tree cannot be
    /// built (<see cref="IndexCapacityException"/>: an entry larger than a
    /// page, or page numbers past the 24-bit limit).
    /// </summary>
    /// <param name="buildAt">Builds the tree with its first page at the given page number; production passes <see cref="IndexBTreeBuilder.Build(IndexPageLayout, int, long, IReadOnlyList{IndexEntry}, long)"/>.</param>
    /// <param name="runs">Records the written run until the caller links it.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The placed tree, or <see langword="null"/> when it cannot be built.</returns>
    internal async ValueTask<IndexBTreeBuildResult?> TryPlaceTreeAsync(
        Func<long, IndexBTreeBuildResult> buildAt,
        ReservedPageRuns runs,
        CancellationToken cancellationToken)
    {
        IndexBTreeBuildResult provisional;
        try
        {
            provisional = buildAt(pager.PageCount);
        }
        catch (IndexCapacityException)
        {
            return null;
        }

        return await this.PlaceBuiltTreeAsync(provisional, buildAt, runs, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reserves a contiguous run for <paramref name="provisional"/>'s pages,
    /// rebuilds the tree with <paramref name="buildAt"/> when the allocator
    /// hands back a different first page (the child and sibling pointers
    /// inside the tree must name the pages it lands on), and writes every page.
    /// The written run is recorded in <paramref name="runs"/>: nothing points
    /// at it yet, so the caller must call <see cref="ReservedPageRuns.MarkLinked"/>
    /// before the write that links it (a <c>first_dp</c> patch or a usage-map
    /// row), and <see cref="ReservedPageRuns.ReleaseAsync"/> when it abandons
    /// it. If the relocated build fails or changes size, the run is released
    /// and the method returns <see langword="null"/>; if a page write or the
    /// relocated build throws anything else, the run is released and the
    /// exception propagates.
    /// </summary>
    /// <param name="provisional">The tree built at the provisional first page, which fixes the page count.</param>
    /// <param name="buildAt">Rebuilds the tree with its first page at the given page number.</param>
    /// <param name="runs">Records the written run until the caller links it.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The tree as written, or <see langword="null"/> when it could not be placed.</returns>
    internal async ValueTask<IndexBTreeBuildResult?> PlaceBuiltTreeAsync(
        IndexBTreeBuildResult provisional,
        Func<long, IndexBTreeBuildResult> buildAt,
        ReservedPageRuns runs,
        CancellationToken cancellationToken)
    {
        int pageCount = provisional.Pages.Count;
        long firstPage = await pageAllocator.ReserveContiguousPagesAsync(pageCount, cancellationToken).ConfigureAwait(false);
        bool placed = false;
        try
        {
            IndexBTreeBuildResult build = provisional;
            if (firstPage != provisional.FirstPageNumber)
            {
                try
                {
                    build = buildAt(firstPage);
                }
                catch (IndexCapacityException)
                {
                    return null;
                }

                if (build.Pages.Count != pageCount)
                {
                    return null;
                }
            }

            for (int i = 0; i < pageCount; i++)
            {
                await pager.WritePageAsync(firstPage + i, build.Pages[i], cancellationToken).ConfigureAwait(false);
            }

            runs.Add(firstPage, pageCount);
            placed = true;
            return build;
        }
        finally
        {
            if (!placed)
            {
                await pageAllocator.ReleaseReservedPagesAsync(firstPage, pageCount).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Reads the 4-byte big-endian child-page pointer at the END of the LAST
    /// entry on an intermediate (<c>0x03</c>) page. Each intermediate entry
    /// trails with <c>[3 B BE data page][1 B data row][4 B BE child page]</c>;
    /// the bitmask-driven entry layout means the last entry ends exactly at
    /// <c>payloadEnd</c>; shared prefixes can include child-pointer bytes.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="layout">The layout.</param>
    private static long ReadLastChildPointer(byte[] page, int pageSize, IndexPageLayout layout)
        => IndexPageCodec.ReadLastChildPointer(layout, page, pageSize);

    /// <summary>
    /// Reads a page through the writer cache and returns a caller-owned clone.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<byte[]> ReadAndClonePageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] pageBytes = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])pageBytes.Clone();
        }
        finally
        {
            PageBuffers.Return(pageBytes);
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
        long expected = pager.PageCount;
        for (int i = 0; i < pages.Count; i++)
        {
            long appended = await pager.AppendPageAsync(pages[i], cancellationToken).ConfigureAwait(false);
            if (appended != expected)
            {
                return false;
            }

            expected++;
        }

        return true;
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
    /// (<see cref="IndexCapacityException"/> from the page builder).
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
                    format.PageSize,
                    tdefPage,
                    splitPages[p],
                    prevPage: thisPrev,
                    nextPage: thisNext,
                    tailPage: 0,
                    enablePrefixCompression: true,
                    maxPrefixLength: maxPrefixLength);
            }
        }
        catch (IndexCapacityException)
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
                    layout, format.PageSize, tdefPage, splitInts[p], prev, next, tails[p]);
                if (built is null)
                {
                    return null;
                }

                pages[p] = built;
            }
        }
        catch (IndexCapacityException)
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
                format.PageSize,
                parentTdefPage: 0,
                entries,
                enablePrefixCompression: true,
                maxPrefixLength: maxPrefixLength);
            freeSpace = Ru16(page, 2);
            return true;
        }
        catch (IndexCapacityException)
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

            long firstChild = IndexPageCodec.ReadFirstChildPointer(layout, page, format.PageSize);
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

        int originalTailPrefLen = IndexPageCodec.ReadPrefixLength(layout, tailLeaf);

        List<IndexEntry> existingTail = IndexPageCodec.DecodeLeafEntries(layout, tailLeaf, format.PageSize);

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
                format.PageSize,
                tdefPage,
                spliced,
                prevPage: tailPrev,
                nextPage: 0,
                tailPage: 0,
                enablePrefixCompression: true,
                maxPrefixLength: originalTailPrefLen);
        }
        catch (IndexCapacityException)
        {
            // Tail leaf would overflow a single page. Fall through to the
            // bulk path, which will resnap the tree (and emit a fresh tail leaf).
            return false;
        }

        await pager.WritePageAsync(tailLeafPage, rewritten, cancellationToken).ConfigureAwait(false);
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

        LeafMutationPlan? plan = await this.PrepareMultiLevelPlanAsync(layout, tdefPage, firstDp, addEntries, removeEntries, cancellationToken).ConfigureAwait(false);
        if (plan is null || !this.ValidateLeafMutationPlan(plan))
        {
            return false;
        }

        return await this.CommitLeafMutationPlanAsync(plan, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Prepares leaf, ancestor and neighbor bytes without publishing a link.</summary>
    /// <param name="layout">The index page layout.</param>
    /// <param name="tdefPage">The owner table definition.</param>
    /// <param name="firstDp">The current root.</param>
    /// <param name="addEntries">The encoded additions.</param>
    /// <param name="removeEntries">The encoded removals.</param>
    /// <param name="cancellationToken">Cancels planning before any page write.</param>
    private async ValueTask<LeafMutationPlan?> PrepareMultiLevelPlanAsync(
        IndexPageLayout layout,
        long tdefPage,
        long firstDp,
        List<IndexEntry> addEntries,
        List<IndexEntry> removeEntries,
        CancellationToken cancellationToken)
    {
        bool hasAdds = addEntries.Count > 0;
        byte[] firstKey = hasAdds ? addEntries[0].Key : removeEntries[0].Key;
        var path = new List<DescentStep>();
        long targetLeafPage = await this.DescendCapturingAsync(layout, firstDp, firstKey, path, cancellationToken).ConfigureAwait(false);
        if (targetLeafPage <= 0 || path.Count == 0)
        {
            // Either descent overshot (search key > every summary, follows
            // tail_page) or the root was a leaf (single-root-leaf path
            // should have caught it). Either way: bail.
            return null;
        }

        int firstUncheckedAdd = hasAdds ? 1 : 0;
        for (int i = firstUncheckedAdd; i < addEntries.Count; i++)
        {
            if (!IndexHelpers.ConfirmKeyTargetsSamePath(path, addEntries[i].Key))
            {
                return null;
            }
        }

        int firstUncheckedRemove = hasAdds ? 0 : 1;
        for (int i = firstUncheckedRemove; i < removeEntries.Count; i++)
        {
            if (!IndexHelpers.ConfirmKeyTargetsSamePath(path, removeEntries[i].Key))
            {
                return null;
            }
        }

        byte[] leaf = await this.ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

        if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return null;
        }

        List<IndexEntry> existingLeafEntries = IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize);
        if (existingLeafEntries.Count == 0)
        {
            // Empty leaf — descent shouldn't normally land here. Bail.
            return null;
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
            return null;
        }

        long leafPrev = IndexPageCodec.ReadPrevPage(layout, leaf);
        long leafNext = IndexPageCodec.ReadNextPage(layout, leaf);
        long leafTail = IndexPageCodec.ReadTailPage(layout, leaf);
        int originalPrefLen = IndexPageCodec.ReadPrefixLength(layout, leaf);

        byte[] oldMaxKey = existingLeafEntries[^1].Key;

        byte[]? rebuilt = IndexPageCodec.TryBuildLeafPage(
            layout, format.PageSize, tdefPage, spliced, leafPrev, leafNext, leafTail);
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
                    return null;
                }
            }

            return new LeafMutationPlan(pager.PageCount, [], [], targetLeafPage, rebuilt, ancestorWrites ?? []);
        }

        // Bails only if a single entry exceeds page payload area.
        SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, format.PageSize, spliced);
        if (splitPages is null)
        {
            return null;
        }

        // First page reuses the original leaf page; remaining pages are
        // freshly appended at end-of-file.
        int splitCount = splitPages.Count;
        long firstFreshPage = pager.PageCount;
        long[] pageNumbers = AllocateSplitPageNumbers(targetLeafPage, splitCount, firstFreshPage);

        byte[][]? pageBytesAll = this.TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, originalPrefLen);
        if (pageBytesAll is null)
        {
            return null;
        }

        DecodedIntermediateEntry[] summaries = BuildSplitSummaries(splitPages, pageNumbers);
        List<(long PageNum, byte[] Bytes)>? splitAncestorWrites = this.PrepareAncestorSplitWrites(
            layout, tdefPage, path, summaries);
        if (splitAncestorWrites is null)
        {
            return null;
        }

        List<(long PageNum, byte[] Bytes)> neighborWrites = await this.StageNeighborWritesAsync(
            layout, leafNext > 0 ? new Dictionary<long, long> { [leafNext] = pageNumbers[^1] } : [], [], cancellationToken).ConfigureAwait(false);
        return new LeafMutationPlan(firstFreshPage, new ArraySegment<byte[]>(pageBytesAll, 1, splitCount - 1), neighborWrites, targetLeafPage, pageBytesAll[0], splitAncestorWrites);
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
                IndexPageCodec.DecodeIntermediateEntries(layout, page, format.PageSize);
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
                long nextChild = tail > 0 ? tail : ReadLastChildPointer(page, format.PageSize, layout);
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
            int originalPrefLen = IndexPageCodec.ReadPrefixLength(layout, pageBytes);

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, format.PageSize, tdefPage, newEntries, prev, next, tail, originalPrefLen);
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
        int originalPrefLen = IndexPageCodec.ReadPrefixLength(layout, parentBytes);

        byte[]? rebuiltParent = IndexBTreeBuilder.TryBuildIntermediatePage(
            layout, format.PageSize, tdefPage, newEntries, parentPrev, parentNext, parentTail, originalPrefLen);
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

        long nextAllocatedPageNumber = pager.PageCount;

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

            List<IndexEntry> existing = IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize);
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
                IndexPageCodec.ReadPrefixLength(layout, leaf),
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
                layout, format.PageSize, tdefPage, spliced, leafPrev, leafNext, leafTail);
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
            SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, format.PageSize, spliced);
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

        List<(long PageNum, byte[] Bytes)> neighborWrites = await this.StageNeighborWritesAsync(layout, leafNextPointerPatches, leafPrevPointerPatches, cancellationToken).ConfigureAwait(false);
        var commitPlan = new CrossLeafMutationPlan(pager.PageCount, newPageAppends, existingPageRewrites, neighborWrites, stagingState.NewRootPage);
        return await this.CommitCrossLeafPlanAsync(tdefPage, firstDpOffset, commitPlan, cancellationToken).ConfigureAwait(false);
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

        byte[] raw = await pager.ReadPageAsync(intermediatePage, cancellationToken).ConfigureAwait(false);
        try
        {
            return IndexPageCodec.ReadTailPage(layout, raw);
        }
        finally
        {
            PageBuffers.Return(raw);
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
        IntermediateRewriteContext context = CaptureIntermediateContext(groups, parentOps, existingPageRewrites, stagingState, newPageAppends);
        while (context.Pending.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long deepest = context.TakeDeepestPage();
            if (!context.Operations.TryGetValue(deepest, out List<IntermediateOp>? ops) || ops.Count == 0)
            {
                continue;
            }

            if (!context.TryApplyOperations(deepest, ops, out DescentStep refStep, out List<DecodedIntermediateEntry> newEntries))
            {
                return false;
            }

            if (newEntries.Count == 0)
            {
                if (!context.TryStageCollapse(deepest))
                {
                    return false;
                }

                continue;
            }

            (long origPrev, long origNext, long origTail) = IndexPageCodec.ReadSiblingPointers(layout, refStep.PageBytes);
            long newTail = origTail;
            if (origTail != 0)
            {
                long lastChildPage = newEntries[^1].ChildPage;
                newTail = context.ParentsOfLeaves.Contains(deepest)
                    ? lastChildPage
                    : await this.GetEffectiveTailPageAsync(layout, lastChildPage, context.Tails, context.Rewrites, cancellationToken).ConfigureAwait(false);
            }

            if (newTail != origTail)
            {
                context.Tails[deepest] = newTail;
            }

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(layout, format.PageSize, tdefPage, newEntries, origPrev, origNext, newTail);
            if (rebuilt is null)
            {
                if (!await this.TryStageIntermediateSplitAsync(layout, tdefPage, context, deepest, newEntries, origPrev, origNext, origTail, cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }
            }
            else if (!context.TryStageReplacement(deepest, refStep, newEntries, rebuilt))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Stages a full intermediate split before propagating its summaries or a new root.</summary>
    /// <param name="layout">The page format.</param>
    /// <param name="tdefPage">The owner definition.</param>
    /// <param name="context">The only owner of pending and prepared pages.</param>
    /// <param name="deepest">The intermediate being rebuilt.</param>
    /// <param name="newEntries">Validated post-mutation entries.</param>
    /// <param name="origPrev">Original previous sibling.</param>
    /// <param name="origNext">Original next sibling.</param>
    /// <param name="origTail">Original tail leaf.</param>
    /// <param name="cancellationToken">Cancels preparation before commit.</param>
    private async ValueTask<bool> TryStageIntermediateSplitAsync(
        IndexPageLayout layout,
        long tdefPage,
        IntermediateRewriteContext context,
        long deepest,
        List<DecodedIntermediateEntry> newEntries,
        long origPrev,
        long origNext,
        long origTail,
        CancellationToken cancellationToken)
    {
        // Intermediate overflow → greedy left-fill split into N pages
        // (each new page freshly allocated). The grandparent then
        // absorbs the N summaries (Replace + (N-1) InsertAfter) and we
        // recurse into it; if this page was the root, we build a fresh
        // root over the split pages and signal the caller to patch
        // first_dp. Per-page tail_page is computed just below.
        List<List<DecodedIntermediateEntry>>? splitInts =
            IndexHelpers.TryGreedySplitIntermediateInN(layout, format.PageSize, tdefPage, newEntries);
        if (splitInts is null)
        {
            // Single entry too big for any intermediate page — bail.
            return false;
        }

        int nSplit = splitInts.Count;

        // First split page reuses `deepest`; remaining pages are
        // freshly allocated from the staging counter.
        long[] intPageNumbers = AllocateSplitPageNumbers(deepest, nSplit, context.State.NextAllocatedPageNumber);
        context.State.NextAllocatedPageNumber += nSplit - 1;

        // Compute each split page's tail_page.
        long[] intTails = new long[nSplit];
        if (context.ParentsOfLeaves.Contains(deepest))
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
                    layout, lastEntry.ChildPage, context.Tails, context.Rewrites, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        byte[][]? intPageBytesAll = this.TryBuildSplitIntermediatePages(
            layout, tdefPage, splitInts, intPageNumbers, origPrev, origNext, intTails);
        if (intPageBytesAll is null)
        {
            return false;
        }

        if (!context.TryRecordSplit(deepest, intPageNumbers, intPageBytesAll, intTails))
        {
            return false;
        }

        if (context.Grandparents.TryGetValue(deepest, out (long ParentPage, int IndexInParent) gpSplit))
        {
            // Grandparent absorbs: Replace the original summary at
            // IndexInParent with the FIRST split page's summary,
            // then InsertAfter one summary per remaining split page
            // in left-to-right order. Recurse into grandparent in
            // case it also overflows.
            // Use helper for Replace + InsertAfter ops for split intermediate pages
            AddParentOpsForSplitPages(
                context.Operations,
                gpSplit.ParentPage,
                gpSplit.IndexInParent,
                [.. splitInts.ConvertAll(s => s.ConvertAll(si => si.Entry))],
                intPageNumbers);

            context.Enqueue(gpSplit.ParentPage);
        }
        else
        {
            return this.TryStageSplitRoot(layout, tdefPage, context, splitInts, intPageNumbers, intTails);
        }

        return true;
    }

    /// <summary>Stages exactly one root linking prepared split intermediates, without publishing first_dp.</summary>
    /// <param name="layout">The page format.</param>
    /// <param name="tdefPage">The owner definition.</param>
    /// <param name="context">The staging owner.</param>
    /// <param name="splitInts">Prepared split entries.</param>
    /// <param name="intPageNumbers">Their planned addresses.</param>
    /// <param name="intTails">Their effective leaf tails.</param>
    private bool TryStageSplitRoot(
        IndexPageLayout layout,
        long tdefPage,
        IntermediateRewriteContext context,
        List<List<DecodedIntermediateEntry>> splitInts,
        long[] intPageNumbers,
        long[] intTails)
    {
        // No grandparent — this WAS the root intermediate.
        // Allocate a fresh root with one summary entry per
        // split page. tail_page of the new root = the LAST
        // split page's tail (= rightmost leaf in the tree).
        if (context.State.NewRootPage.HasValue)
        {
            // Already split a root once in this batch (multi-
            // group case); only one root is allowed. Bail.
            return false;
        }

        long newRootPageAlloc = context.State.NextAllocatedPageNumber++;

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
                layout, format.PageSize, tdefPage, rootEntries, prevPage: 0, nextPage: 0, tailPage: intTails[^1]);
        }
        catch (IndexCapacityException)
        {
            return false;
        }

        if (newRootBytes is null)
        {
            return false;
        }

        context.Appends.Add(newRootBytes);
        context.State.NewRootPage = newRootPageAlloc;
        return true;
    }

    /// <summary>Stages and applies one encoded catalog entry using the ordinary tree editor.</summary>
    /// <param name="layout">The layout for this phase.</param>
    /// <param name="tdefPage">The tdefPage for this phase.</param>
    /// <param name="firstDp">The firstDp for this phase.</param>
    /// <param name="firstDpOffset">The firstDpOffset for this phase.</param>
    /// <param name="composite">The composite for this phase.</param>
    /// <param name="newRowLoc">The newRowLoc for this phase.</param>
    /// <param name="cancellationToken">The cancellationToken for this phase.</param>
    internal async ValueTask<bool> TryInsertCatalogEntryAsync(
        IndexPageLayout layout,
        long tdefPage,
        long firstDp,
        int firstDpOffset,
        byte[] composite,
        RowLocation newRowLoc,
        CancellationToken cancellationToken)
    {
        // Descend by binary-searching child summaries. First try
        // without tail overshoot so we capture a clean path for
        // ancestor updates (needed when the leaf splits). Fall back
        // to allowTailOvershoot when the key overshoots every summary
        // on an intermediate — in that case the chain walk below still
        // finds the correct leaf and we accept that ancestor updates
        // won't be possible (but a split can still chain-append).
        var descentPath = new List<DescentStep>();
        bool hasCleanPath = true;
        long targetLeafPage = await this.DescendCapturingAsync(
            layout, firstDp, composite, descentPath, cancellationToken, allowTailOvershoot: false).ConfigureAwait(false);
        if (targetLeafPage <= 0)
        {
            // Overshoot — retry with tail following. Path will be
            // incomplete but the chain walk handles placement.
            descentPath.Clear();
            hasCleanPath = false;
            targetLeafPage = await this.DescendCapturingAsync(
                layout, firstDp, composite, descentPath, cancellationToken, allowTailOvershoot: true).ConfigureAwait(false);
            if (targetLeafPage <= 0)
            {
                return false;
            }
        }

        byte[] leaf = await this.ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

        if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        // If the descent landed before the true tail of a sibling
        // chain (Access can store mostly-monotonic data with stale
        // intermediate summaries plus a rightward chain), walk
        // next_page while every existing entry on the current leaf
        // is < composite. That way we still find the correct
        // insertion leaf.
        int chainBudget = 1_000_000;
        while (true)
        {
            long nextLeaf = IndexPageCodec.ReadNextPage(layout, leaf);
            if (nextLeaf <= 0)
            {
                break;
            }

            List<IndexEntry> probe = IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize);
            if (probe.Count == 0 || IndexHelpers.CompareKeyBytes(composite, probe[^1].Key) <= 0)
            {
                // composite belongs in this leaf (or earlier).
                break;
            }

            if (--chainBudget <= 0)
            {
                return false;
            }

            targetLeafPage = nextLeaf;
            leaf = await this.ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                return false;
            }
        }

        long leafPrev = IndexPageCodec.ReadPrevPage(layout, leaf);
        long leafNext = IndexPageCodec.ReadNextPage(layout, leaf);
        long leafTail = IndexPageCodec.ReadTailPage(layout, leaf);
        int originalPrefLen = IndexPageCodec.ReadPrefixLength(layout, leaf);

        List<IndexEntry> existing = IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize);

        var addEntries = new List<IndexEntry>(1)
        {
            new(composite, newRowLoc.PageNumber, (byte)newRowLoc.RowIndex),
        };

        List<IndexEntry>? spliced = IndexEntrySplicer.Splice(
            existing,
            addEntries,
            []);
        if (spliced is null)
        {
            return false;
        }

        byte[]? rewritten = IndexPageCodec.TryBuildLeafPage(
            layout,
            format.PageSize,
            tdefPage,
            spliced,
            prevPage: leafPrev,
            nextPage: leafNext,
            tailPage: leafTail,
            enablePrefixCompression: true,
            maxPrefixLength: originalPrefLen);
        if (rewritten is null)
        {
            // Leaf overflow → N-way split.
            SplitPages? splitPages = this.TryBalancedTwoWayLeafSplit(layout, spliced, originalPrefLen)
                ?? IndexHelpers.TryGreedySplitLeafInN(layout, format.PageSize, spliced);
            if (splitPages is null)
            {
                return false;
            }

            // Plan the split at the provisional end of file and reserve
            // its new pages only once an in-place split is certain: the
            // rebuild fallbacks below reserve their own pages, and a run
            // reserved first would be left behind, unwritten and marked
            // used.
            int splitCount = splitPages.Count;
            bool canSplitInPlace = hasCleanPath && descentPath.Count > 0;
            CatalogLeafSplitPlan? plan = this.TryPlanCatalogLeafSplit(
                layout, tdefPage, splitPages, targetLeafPage, pager.PageCount, leafPrev, leafNext, originalPrefLen, canSplitInPlace ? descentPath : null);
            if (plan is null)
            {
                return false;
            }

            var runs = new ReservedPageRuns(pageAllocator);
            try
            {
                if (plan.AncestorWrites is not null)
                {
                    long firstFreshPage = await runs.ReserveAsync(splitCount - 1, cancellationToken).ConfigureAwait(false);
                    if (firstFreshPage != plan.PageNumbers[1])
                    {
                        plan = this.TryPlanCatalogLeafSplit(
                            layout, tdefPage, splitPages, targetLeafPage, firstFreshPage, leafPrev, leafNext, originalPrefLen, descentPath);
                    }
                }

                if (plan?.AncestorWrites is not { } ancestorWrites)
                {
                    // No clean ancestor path, or the ancestor summaries
                    // overflow: rebuild just this index from its entries.
                    await runs.ReleaseAsync().ConfigureAwait(false);
                    return await this.TryRebuildCatalogIndexTreeAsync(
                        layout,
                        tdefPage,
                        firstDp,
                        firstDpOffset,
                        addEntries,
                        cancellationToken).ConfigureAwait(false);
                }

                // Complete sibling staging before the first page write.
                byte[]? nextLeafBytes = null;
                if (leafNext > 0)
                {
                    nextLeafBytes = await this.ReadAndClonePageAsync(leafNext, cancellationToken).ConfigureAwait(false);
                    IndexPageCodec.WritePrevPage(layout, nextLeafBytes, plan.PageNumbers[splitCount - 1]);
                }

                await this.CommitCatalogSplitAsync(targetLeafPage, leafNext, plan, nextLeafBytes, ancestorWrites, runs, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (!runs.IsEmpty)
                {
                    await runs.ReleaseAsync().ConfigureAwait(false);
                }
            }

            return true;
        }

        await pager.WritePageAsync(targetLeafPage, rewritten, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Builds the pages of a catalog leaf split whose new right-hand pages
    /// start at <paramref name="firstNewPage"/>, and, when
    /// <paramref name="descentPath"/> is supplied, the ancestor rewrites that
    /// link them. Writes and reserves nothing, so the splice can plan at the
    /// provisional end of file, reserve only when the in-place split is
    /// certain, and plan again if the reservation lands elsewhere.
    /// </summary>
    /// <param name="layout">The index page layout.</param>
    /// <param name="tdefPage">The catalog TDEF page.</param>
    /// <param name="splitPages">The entries of each split page, left to right.</param>
    /// <param name="targetLeafPage">The leaf being split; it stays the left-most page.</param>
    /// <param name="firstNewPage">The page number of the first new right-hand page.</param>
    /// <param name="leafPrev">The split leaf's prev_page.</param>
    /// <param name="leafNext">The split leaf's next_page.</param>
    /// <param name="maxPrefixLength">The split leaf's prefix-length cap.</param>
    /// <param name="descentPath">The clean root-to-leaf path, or <see langword="null"/> when there is none.</param>
    /// <returns>The plan, or <see langword="null"/> when a split page cannot be built.</returns>
    private CatalogLeafSplitPlan? TryPlanCatalogLeafSplit(
        IndexPageLayout layout,
        long tdefPage,
        SplitPages splitPages,
        long targetLeafPage,
        long firstNewPage,
        long leafPrev,
        long leafNext,
        int maxPrefixLength,
        List<DescentStep>? descentPath)
    {
        long[] pageNumbers = IndexBTreeEditor.AllocateSplitPageNumbers(targetLeafPage, splitPages.Count, firstNewPage);
        byte[][]? pages = this.TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, maxPrefixLength);
        if (pages is null)
        {
            return null;
        }

        List<(long PageNum, byte[] Bytes)>? ancestorWrites = null;
        if (descentPath is not null)
        {
            DecodedIntermediateEntry[] summaries = IndexBTreeEditor.BuildSplitSummaries(splitPages, pageNumbers);
            ancestorWrites = this.PrepareAncestorSplitWrites(layout, tdefPage, descentPath, summaries);
        }

        return new CatalogLeafSplitPlan(pageNumbers, pages, ancestorWrites);
    }

    /// <summary>A planned in-place catalog leaf split.</summary>
    /// <param name="PageNumbers">The page number of each split page; <c>[0]</c> is the original leaf.</param>
    /// <param name="Pages">The bytes of each split page, parallel to <paramref name="PageNumbers"/>.</param>
    /// <param name="AncestorWrites">The ancestor rewrites that link the new pages, or <see langword="null"/> when the split cannot be linked in place.</param>
    private sealed record CatalogLeafSplitPlan(
        long[] PageNumbers,
        byte[][] Pages,
        List<(long PageNum, byte[] Bytes)>? AncestorWrites);

    /// <summary>Verifies that planned append addresses still name the end of the file.</summary>
    /// <param name="plan">The prepared leaf plan.</param>
    private bool ValidateLeafMutationPlan(LeafMutationPlan plan)
        => plan.FirstNewPage == pager.PageCount && plan.LeafBytes.Length == format.PageSize;

    /// <summary>Publishes prepared pages in append, neighbor, leaf and ancestor order.</summary>
    /// <param name="plan">The complete prepared plan.</param>
    /// <param name="cancellationToken">Cancels before publishing and between writes.</param>
    private async ValueTask<bool> CommitLeafMutationPlanAsync(LeafMutationPlan plan, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!this.ValidateLeafMutationPlan(plan) || !await this.TryAppendContiguousAsync(plan.NewPages, cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        foreach ((long page, byte[] bytes) in plan.NeighborWrites)
        {
            await pager.WritePageAsync(page, bytes, cancellationToken).ConfigureAwait(false);
        }

        await pager.WritePageAsync(plan.LeafPage, plan.LeafBytes, cancellationToken).ConfigureAwait(false);
        foreach ((long page, byte[] bytes) in plan.AncestorWrites)
        {
            await pager.WritePageAsync(page, bytes, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>Stages every fallible sibling read before appending or linking pages.</summary>
    /// <param name="layout">The index header layout.</param>
    /// <param name="previousPointers">Pages whose previous pointer changes.</param>
    /// <param name="nextPointers">Pages whose next pointer changes.</param>
    /// <param name="cancellationToken">Cancels sibling preparation.</param>
    private async ValueTask<List<(long PageNum, byte[] Bytes)>> StageNeighborWritesAsync(
        IndexPageLayout layout,
        Dictionary<long, long> previousPointers,
        Dictionary<long, long> nextPointers,
        CancellationToken cancellationToken)
    {
        var staged = new Dictionary<long, byte[]>();
        foreach ((long page, long previous) in previousPointers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] bytes = await this.ReadAndClonePageAsync(page, cancellationToken).ConfigureAwait(false);
            IndexPageCodec.WritePrevPage(layout, bytes, previous);
            staged.Add(page, bytes);
        }

        foreach ((long page, long next) in nextPointers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!staged.TryGetValue(page, out byte[]? bytes))
            {
                bytes = await this.ReadAndClonePageAsync(page, cancellationToken).ConfigureAwait(false);
                staged.Add(page, bytes);
            }

            IndexPageCodec.WriteNextPage(layout, bytes, next);
        }

        var writes = new List<(long PageNum, byte[] Bytes)>(staged.Count);
        foreach ((long page, byte[] bytes) in staged)
        {
            writes.Add((page, bytes));
        }

        return writes;
    }

    /// <summary>A leaf mutation whose bytes and external reads are complete before commit.</summary>
    /// <param name="FirstNewPage">Expected append address.</param>
    /// <param name="NewPages">Prepared new right leaves.</param>
    /// <param name="NeighborWrites">Prepared sibling patches.</param>
    /// <param name="LeafPage">Original leaf to replace.</param>
    /// <param name="LeafBytes">Prepared original leaf bytes.</param>
    /// <param name="AncestorWrites">Prepared ancestor summaries, deepest first.</param>
    private sealed record LeafMutationPlan(
        long FirstNewPage,
        IReadOnlyList<byte[]> NewPages,
        List<(long PageNum, byte[] Bytes)> NeighborWrites,
        long LeafPage,
        byte[] LeafBytes,
        List<(long PageNum, byte[] Bytes)> AncestorWrites);

    /// <summary>Validates append addresses and disjoint page ownership before any cross-leaf write.</summary>
    /// <param name="plan">All prepared pages and external neighbor patches.</param>
    private bool ValidateCrossLeafPlan(CrossLeafMutationPlan plan)
    {
        if (plan.FirstNewPage != pager.PageCount
            || (plan.NewRootPage is long root && (root < plan.FirstNewPage || root >= plan.FirstNewPage + plan.NewPages.Count)))
        {
            return false;
        }

        foreach ((long page, _) in plan.NeighborWrites)
        {
            if (plan.ExistingPages.ContainsKey(page))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Commits a fully staged cross-leaf mutation in dependency order.</summary>
    /// <param name="tdefPage">The tdefPage for this phase.</param>
    /// <param name="firstDpOffset">The firstDpOffset for this phase.</param>
    /// <param name="plan">The plan for this phase.</param>
    /// <param name="cancellationToken">The cancellationToken for this phase.</param>
    private async ValueTask<bool> CommitCrossLeafPlanAsync(
        long tdefPage,
        int firstDpOffset,
        CrossLeafMutationPlan plan,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        List<byte[]> newPageAppends = plan.NewPages;
        if (!this.ValidateCrossLeafPlan(plan))
        {
            return false;
        }

        Dictionary<long, byte[]> existingPageRewrites = plan.ExistingPages;

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

        foreach ((long pageNum, byte[] bytes) in plan.NeighborWrites)
        {
            await pager.WritePageAsync(pageNum, bytes, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long pageNum, byte[] bytes) in existingPageRewrites)
        {
            await pager.WritePageAsync(pageNum, bytes, cancellationToken).ConfigureAwait(false);
        }

        // If the root intermediate split, patch the real-idx first_dp slot
        // in the TDEF to point at the freshly-allocated root. The new root
        // page itself was already appended via newPageAppends above, so the
        // page number is stable. firstDpOffset is a logical TDEF offset and
        // may fall on a continuation page of a wide table's TDEF chain.
        if (plan.NewRootPage is long newRootPage)
        {
            await tdefWriter.WriteInt32Async(tdefPage, firstDpOffset, checked((int)newRootPage), cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>Validated page bytes and links awaiting an ordered cross-leaf commit.</summary>
    /// <param name="FirstNewPage">Expected first append address.</param>
    /// <param name="NewPages">The NewPages for this phase.</param>
    /// <param name="ExistingPages">The ExistingPages for this phase.</param>
    /// <param name="NeighborWrites">Prepared neighboring pages.</param>
    /// <param name="NewRootPage">The NewRootPage for this phase.</param>
    private sealed record CrossLeafMutationPlan(
        long FirstNewPage,
        List<byte[]> NewPages,
        Dictionary<long, byte[]> ExistingPages,
        List<(long PageNum, byte[] Bytes)> NeighborWrites,
        long? NewRootPage);

    /// <summary>Captures immutable ancestry before any intermediate operation is staged.</summary>
    /// <param name="groups">The groups for this phase.</param>
    /// <param name="parentOps">The parentOps for this phase.</param>
    /// <param name="rewrites">Prepared existing pages owned by the context.</param>
    /// <param name="state">Planned append addresses and root.</param>
    /// <param name="appends">Prepared appended pages owned by the context.</param>
    private static IntermediateRewriteContext CaptureIntermediateContext(
        Dictionary<long, LeafGroup> groups,
        Dictionary<long, List<IntermediateOp>> parentOps,
        Dictionary<long, byte[]> rewrites,
        IntermediateStagingState state,
        List<byte[]> appends)
    {
        // Per-touched-intermediate maps. Multiple groups may pass through the
        // same intermediate; they all carry identical canonical bytes (the
        // page cache returns the same content per call in single-writer mode,
        // as no mid-batch write touches these pages yet), so the first
        // DescentStep seen is the reference for header + original entries.
        var intermediateRefs = new Dictionary<long, DescentStep>(parentOps.Count * 2);
        var intermediateGrandparent = new Dictionary<long, (long ParentPage, int IndexInParent)>(parentOps.Count * 2);

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
        return new IntermediateRewriteContext([.. parentOps.Keys], intermediateRefs, intermediateGrandparent, new Dictionary<long, long>(parentOps.Count * 2), depthOf, pending, parentOps, rewrites, state, appends);
    }

    /// <summary>Owns ancestry and pending parent work throughout deepest-first intermediate staging.</summary>
    /// <param name="ParentsOfLeaves">The ParentsOfLeaves for this phase.</param>
    /// <param name="References">The References for this phase.</param>
    /// <param name="Grandparents">The Grandparents for this phase.</param>
    /// <param name="Tails">The Tails for this phase.</param>
    /// <param name="Depths">The Depths for this phase.</param>
    /// <param name="Pending">The Pending for this phase.</param>
    /// <param name="Operations">Pending operations by original child index.</param>
    /// <param name="Rewrites">Prepared existing page replacements.</param>
    /// <param name="State">Append address and root ownership.</param>
    /// <param name="Appends">Prepared appended pages in address order.</param>
    private sealed record IntermediateRewriteContext(
        HashSet<long> ParentsOfLeaves,
        Dictionary<long, DescentStep> References,
        Dictionary<long, (long ParentPage, int IndexInParent)> Grandparents,
        Dictionary<long, long> Tails,
        Dictionary<long, int> Depths,
        List<long> Pending,
        Dictionary<long, List<IntermediateOp>> Operations,
        Dictionary<long, byte[]> Rewrites,
        IntermediateStagingState State,
        List<byte[]> Appends)
    {
        /// <summary>Dequeues the deepest pending ancestor, so children always stage before parents.</summary>
        internal long TakeDeepestPage()
        {
            int deepest = 0;
            for (int i = 1; i < this.Pending.Count; i++)
            {
                if (this.Depths.GetValueOrDefault(this.Pending[i], -1) > this.Depths.GetValueOrDefault(this.Pending[deepest], -1))
                {
                    deepest = i;
                }
            }

            long page = this.Pending[deepest];
            this.Pending.RemoveAt(deepest);
            return page;
        }

        /// <summary>Validates every original child index before applying a page's pending operations.</summary>
        /// <param name="page">The pending intermediate.</param>
        /// <param name="operations">Its original-index operations.</param>
        /// <param name="reference">Captured pre-mutation bytes and entries.</param>
        /// <param name="entries">Validated resulting entries.</param>
        internal bool TryApplyOperations(
            long page,
            List<IntermediateOp> operations,
            out DescentStep reference,
            out List<DecodedIntermediateEntry> entries)
        {
            entries = [];
            if (!this.References.TryGetValue(page, out reference))
            {
                return false;
            }

            foreach (IntermediateOp operation in operations)
            {
                if (operation.OriginalIndex < 0 || operation.OriginalIndex >= reference.Entries.Count)
                {
                    return false;
                }
            }

            entries = IndexHelpers.ApplyIntermediateOps(reference.Entries, operations);
            return true;
        }

        /// <summary>Orphans an emptied intermediate and schedules its parent's removal; root collapse refuses.</summary>
        /// <param name="page">The emptied page.</param>
        internal bool TryStageCollapse(long page)
        {
            if (!this.Grandparents.TryGetValue(page, out (long ParentPage, int IndexInParent) parent))
            {
                return false;
            }

            AddParentOp(this.Operations, parent.ParentPage, parent.IndexInParent, IntermediateOpType.Remove, default);
            this.Enqueue(parent.ParentPage);
            return true;
        }

        /// <summary>Stages one page exactly once and propagates its changed maximum to its parent.</summary>
        /// <param name="page">The intermediate to replace.</param>
        /// <param name="reference">Its original entries.</param>
        /// <param name="entries">Post-mutation entries.</param>
        /// <param name="bytes">Validated replacement bytes.</param>
        internal bool TryStageReplacement(
            long page,
            DescentStep reference,
            List<DecodedIntermediateEntry> entries,
            byte[] bytes)
        {
            if (!this.Rewrites.TryAdd(page, bytes))
            {
                return false;
            }

            DecodedIntermediateEntry maximum = entries[^1];
            if (maximum != reference.Entries[^1] && this.Grandparents.TryGetValue(page, out (long ParentPage, int IndexInParent) parent))
            {
                AddParentOp(this.Operations, parent.ParentPage, parent.IndexInParent, IntermediateOpType.Replace, maximum with { ChildPage = page });
                this.Enqueue(parent.ParentPage);
            }

            return true;
        }

        /// <summary>Accepts one fully built split and records its page tails before any parent looks them up.</summary>
        /// <param name="page">The intermediate reused on the left.</param>
        /// <param name="numbers">Planned page addresses.</param>
        /// <param name="bytes">Validated split bytes in left-to-right order.</param>
        /// <param name="tails">Effective leaf tails parallel to the addresses.</param>
        internal bool TryRecordSplit(long page, long[] numbers, byte[][] bytes, long[] tails)
        {
            if (!this.Rewrites.TryAdd(page, bytes[0]))
            {
                return false;
            }

            for (int i = 1; i < bytes.Length; i++)
            {
                this.Appends.Add(bytes[i]);
            }

            for (int i = 0; i < numbers.Length; i++)
            {
                this.Tails[numbers[i]] = tails[i];
            }

            return true;
        }

        /// <summary>Schedules an ancestor once even when several child operations affect it.</summary>
        /// <param name="page">The ancestor.</param>
        internal void Enqueue(long page)
        {
            if (!this.Pending.Contains(page))
            {
                this.Pending.Add(page);
            }
        }
    }

    /// <summary>Writes reserved pages before publishing sibling, leaf and ancestor links.</summary>
    /// <param name="targetLeafPage">The targetLeafPage for this phase.</param>
    /// <param name="nextLeafPage">The nextLeafPage for this phase.</param>
    /// <param name="plan">The plan for this phase.</param>
    /// <param name="nextLeafBytes">The nextLeafBytes for this phase.</param>
    /// <param name="ancestorWrites">The ancestorWrites for this phase.</param>
    /// <param name="runs">The runs for this phase.</param>
    /// <param name="cancellationToken">The cancellationToken for this phase.</param>
    private async ValueTask CommitCatalogSplitAsync(
        long targetLeafPage,
        long nextLeafPage,
        CatalogLeafSplitPlan plan,
        byte[]? nextLeafBytes,
        List<(long PageNum, byte[] Bytes)> ancestorWrites,
        ReservedPageRuns runs,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        for (int p = 1; p < plan.Pages.Length; p++)
        {
            await pager.WritePageAsync(plan.PageNumbers[p], plan.Pages[p], cancellationToken).ConfigureAwait(false);
        }

        runs.MarkLinked();
        if (nextLeafBytes is not null)
        {
            await pager.WritePageAsync(nextLeafPage, nextLeafBytes, cancellationToken).ConfigureAwait(false);
        }

        await pager.WritePageAsync(targetLeafPage, plan.Pages[0], cancellationToken).ConfigureAwait(false);
        foreach ((long pageNumber, byte[] bytes) in ancestorWrites)
        {
            await pager.WritePageAsync(pageNumber, bytes, cancellationToken).ConfigureAwait(false);
        }
    }
}
