namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Builds a complete JET index B-tree from a sorted list of leaf
/// entries: one or more leaf pages (<c>0x04</c>) chained through
/// <c>prev_page</c> / <c>next_page</c>, plus zero or more levels of
/// intermediate pages (<c>0x03</c>) above them. The page layouts are
/// described in <see href="docs/design/index-and-relationship-format-notes.md" />
/// §4.1 (header), §4.2 (entry-start bitmask), §4.3 (per-entry record), and
/// §4.5 (tail-page chain).
/// <para>
/// Both Jet4 / ACE and Jet3 layouts are emitted via the
/// <see cref="IndexPageLayout"/> descriptor passed to
/// the layout-aware <c>Build</c> overload. Jet3 live-leaf lifted the
/// previous Jet4-only restriction.
/// </para>
/// <para>
/// <b>Constraints / not done</b>:
/// </para>
/// <list type="bullet">
///   <item>Shared-prefix compression on leaves and intermediates. §4.4.</item>
///   <item>Tail-page recorded on every intermediate page: the
///   <c>tail_page</c> header field on each <c>0x03</c> page points at the
///   absolute page number of the rightmost leaf so a reader / cursor can short-circuit
///   to it without descending. Single-leaf trees keep <c>tail_page = 0</c> (the leaf
///   itself is the tail). §4.5.</item>
///   <item>No incremental updates: this builds a fresh tree from a sorted
///   entry list. Maintenance hooks on insert / update / delete are index maintenance.</item>
/// </list>
/// </summary>
internal static class IndexBTreeBuilder
{
    /// <summary>
    /// Result of <c>Build</c>: the rendered pages (in the order they
    /// should be appended to the database) and the absolute page number of
    /// the root, which the caller writes into the real-index
    /// <c>first_dp</c> field on the TDEF.
    /// </summary>
    /// <param name="pages">The pages.</param>
    /// <param name="rootPageNumber">The root page number.</param>
    /// <param name="firstPageNumber">The first page number.</param>
    internal readonly struct BuildResult(IReadOnlyList<byte[]> pages, long rootPageNumber, long firstPageNumber)
    {
        /// <summary>Gets the rendered pages, indexed [0..N-1]. Page i lives at
        /// absolute database page number <see cref="FirstPageNumber"/> + i.</summary>
        public IReadOnlyList<byte[]> Pages { get; } = pages;

        /// <summary>Gets the absolute page number of the root (leaf for a
        /// single-page tree, otherwise the topmost intermediate).</summary>
        public long RootPageNumber { get; } = rootPageNumber;

        /// <summary>Gets the absolute page number assigned to <c>Pages[0]</c>.</summary>
        public long FirstPageNumber { get; } = firstPageNumber;
    }

    /// <summary>
    /// Builds a complete index B-tree. <paramref name="entries"/> must already be
    /// sorted by encoded key. <paramref name="firstPageNumber"/> is the next free
    /// page number in the database; the builder allocates contiguous pages
    /// starting there. The caller is responsible for appending the returned
    /// pages in order.
    /// </summary>
    /// <param name="pageSize">Database page size (4096 for Jet4 / ACE).</param>
    /// <param name="parentTdefPage">Page number of the table's TDEF page,
    /// recorded in every index page's <c>parent_page</c> header field (§4.1).</param>
    /// <param name="entries">Sorted leaf entries. Empty input produces a single
    /// empty leaf page (the leaf-page emission placeholder behaviour).</param>
    /// <param name="firstPageNumber">First absolute page number to allocate.</param>
    public static BuildResult Build(
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long firstPageNumber)
        => Build(IndexPageLayout.Jet4, pageSize, parentTdefPage, entries, firstPageNumber);

    /// <summary>
    /// Builds a complete index B-tree using the supplied per-format
    /// <paramref name="layout"/> (Jet3 or Jet4 / ACE). See the parameterless-layout
    /// overload for the contract.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="firstPageNumber">The first page number.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the page size, entry size, or allocated page range cannot fit the B-tree format.</exception>
    public static BuildResult Build(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long firstPageNumber)
    {
        if (pageSize <= layout.FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be greater than {layout.FirstEntryOffset}.");
        }

        Guard.NotNull(entries, nameof(entries));
        Guard.InRange(firstPageNumber, 0, 0xFFFFFF, nameof(firstPageNumber));

        int entryAreaSize = pageSize - layout.FirstEntryOffset;

        // ── Step 1: Pack entries into leaves greedily, in input order, and
        // remember the last entry of each leaf for the level above. Each leaf
        // entry occupies EncodedKey.Length + 4 bytes (3-byte BE data page +
        // 1-byte data row). The entry-start bitmask spans the area from
        // 0x1B..0x1DF — 485 bytes = 3880 bits. The largest entry stride is
        // limited by the entry area (3616 bytes on a 4096-byte page) so the
        // bitmask never overflows in practice.
        // ──────────────────────────────────────────────────────────────────
        // Rough capacity estimate: assume average entry ~64 bytes ⇒
        // entryAreaSize / 64 entries per leaf. Errs on the high side which is
        // cheap; underestimating just causes extra resizes.

        // Step 1: Pack entries into split pages (SplitPages) greedily, in input order.
        // Each inner list in SplitPages contains the IndexEntry objects for a single page after splitting.
        // This is sometimes called a "leaf group" in legacy comments, but that term is non-standard and discouraged.
        int estSplitPageCount = entries.Count == 0
            ? 1
            : Math.Max(1, ((entries.Count * 64) + entryAreaSize - 1) / entryAreaSize);

        var splitPages = new SplitPages(estSplitPageCount);
        var splitPageLastEntries = new List<IndexEntry>(estSplitPageCount);
        if (entries.Count == 0)
        {
            splitPages.Add([]);

            // No last entry for an empty placeholder;
            // the splitPageCount == 1 path below short-circuits before consulting splitPageLastEntries.
        }
        else
        {
            var current = new List<IndexEntry>();
            int currentSize = 0;
            for (int i = 0; i < entries.Count; i++)
            {
                IndexEntry e = entries[i];
                int entryLen = e.Key.Length + 4;
                if (entryLen > entryAreaSize)
                {
                    throw new ArgumentOutOfRangeException(nameof(entries), $"Single index entry of {entryLen} bytes exceeds the {entryAreaSize}-byte payload area; one entry must fit on one page.");
                }

                if (currentSize + entryLen > entryAreaSize)
                {
                    splitPages.Add(current);
                    splitPageLastEntries.Add(current[^1]);
                    current = [];
                    currentSize = 0;
                }

                current.Add(e);
                currentSize += entryLen;
            }

            splitPages.Add(current);
            splitPageLastEntries.Add(current[^1]);
        }

        // Step 2: Validate split page-number range. Pages are sequential starting at firstPageNumber,
        // so we never need to materialize the per-page array — the i'th split page lives at firstPageNumber + i.
        int splitPageCount = splitPages.Count;
        if (firstPageNumber + splitPageCount - 1 > 0xFFFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(firstPageNumber), "Allocated page numbers exceed the 24-bit child-pointer range.");
        }

        // Step 3: Render split pages (leaves) with prev/next sibling chain.
        // Pre-size pages list assuming up to ~10% intermediates above the leaves.
        var pages = new List<byte[]>(splitPageCount + Math.Max(1, splitPageCount / 10));
        for (int i = 0; i < splitPageCount; i++)
        {
            long prev = i == 0 ? 0 : firstPageNumber + i - 1;
            long next = i == splitPageCount - 1 ? 0 : firstPageNumber + i + 1;
            byte[] leaf = IndexPageCodec.BuildLeafPage(
                layout,
                pageSize,
                parentTdefPage,
                splitPages[i],
                prevPage: prev,
                nextPage: next,
                tailPage: 0,
                enablePrefixCompression: true);
            pages.Add(leaf);
        }

        // Single split page is its own root — no intermediates needed.
        if (splitPageCount == 1)
        {
            return new BuildResult(pages, firstPageNumber, firstPageNumber);
        }

        // Step 4: Build intermediate levels until we reach a single root.
        // Each intermediate entry summarizes the LAST entry of its child page
        // and appends the 4-byte child page pointer (§4.3). The child pages
        // of every level are themselves sequential, so we track each level
        // as (base page, count) instead of a per-page array.
        long childPageBase = firstPageNumber;
        int childPageCount = splitPageCount;
        IReadOnlyList<IndexEntry> childLastEntries = splitPageLastEntries;
        long nextFreePage = firstPageNumber + splitPageCount;

        // tail-leaf is the rightmost split page the builder just emitted
        // (firstPageNumber + splitPageCount - 1). Stamp it into every
        // intermediate-page tail_page header so the cursor can jump directly
        // to the tail on overshoot, and so the append-only
        // incremental fast path can locate it from the root in one read.
        long tailLeafPage = firstPageNumber + splitPageCount - 1;

        while (childPageCount > 1)
        {
            (List<List<DecodedIntermediateEntry>>? groups, List<IndexEntry>? nextLevelLast) =
                PackIntermediate(childPageBase, childPageCount, childLastEntries, entryAreaSize);

            int levelCount = groups.Count;
            if (nextFreePage + levelCount - 1 > 0xFFFFFF)
            {
                throw new ArgumentOutOfRangeException(nameof(firstPageNumber), "Allocated page numbers exceed the 24-bit child-pointer range.");
            }

            for (int i = 0; i < levelCount; i++)
            {
                long prev = i == 0 ? 0 : nextFreePage + i - 1;
                long next = i == levelCount - 1 ? 0 : nextFreePage + i + 1;
                byte[] page = IndexPageCodec.BuildIntermediatePage(
                    layout,
                    pageSize,
                    parentTdefPage,
                    groups[i],
                    prevPage: prev,
                    nextPage: next,
                    tailPage: tailLeafPage);
                pages.Add(page);
            }

            childPageBase = nextFreePage;
            childPageCount = levelCount;
            childLastEntries = nextLevelLast;
            nextFreePage += levelCount;
        }

        return new BuildResult(pages, childPageBase, firstPageNumber);
    }

    /// <summary>
    /// Surgical-rewrite helper. Re-emits a single intermediate (<c>0x03</c>)
    /// page from an arbitrary list of: <code>(summaryKey, dataPage, dataRow,
    /// childPage)</code> tuples (sorted by summary key), preserving the supplied
    /// <c>prev_page</c> / <c>next_page</c> / <c>tail_page</c> headers. Returns
    /// <see langword="null"/> when the entry list overflows the per-page
    /// payload area; callers fall back to <see cref="Build(IndexPageLayout, int, long, IReadOnlyList{IndexEntry}, long)"/>
    /// (full-tree rebuild) on overflow.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="maxPrefixLength">The max prefix length.</param>
    public static byte[]? TryBuildIntermediatePage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<DecodedIntermediateEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        int? maxPrefixLength = null)
    {
        Guard.NotNull(entries, nameof(entries));

        if (entries.Count == 0)
        {
            // Empty intermediate makes no sense — caller must collapse / merge.
            return null;
        }

        return IndexPageCodec.TryBuildIntermediatePage(
            layout,
            pageSize,
            parentTdefPage,
            entries,
            prevPage,
            nextPage,
            tailPage,
            maxPrefixLength);
    }

    private static (List<List<DecodedIntermediateEntry>> Groups, List<IndexEntry> LastPerGroup) PackIntermediate(
        long childPageBase,
        int childPageCount,
        IReadOnlyList<IndexEntry> childLastEntries,
        int entryAreaSize)
    {
        // Rough capacity hint: assume ~64-byte average summary key ⇒ each
        // intermediate page holds entryAreaSize / (64 + 8) entries. Errs high.
        int estPagesAtThisLevel = Math.Max(1, ((childPageCount * 72) + entryAreaSize - 1) / entryAreaSize);
        var groups = new List<List<DecodedIntermediateEntry>>(estPagesAtThisLevel);
        var lastPerGroup = new List<IndexEntry>(estPagesAtThisLevel);

        var current = new List<DecodedIntermediateEntry>();
        int currentSize = 0;
        for (int i = 0; i < childPageCount; i++)
        {
            var entry = new DecodedIntermediateEntry(childLastEntries[i], childPageBase + i);
            int len = entry.Entry.Key.Length + 4 + 4;
            if (len > entryAreaSize)
            {
                throw new ArgumentOutOfRangeException(nameof(childLastEntries), "Intermediate entry exceeds page payload area.");
            }

            if (currentSize + len > entryAreaSize)
            {
                groups.Add(current);
                lastPerGroup.Add(current[^1].Entry);
                current = [];
                currentSize = 0;
            }

            current.Add(entry);
            currentSize += len;
        }

        groups.Add(current);
        lastPerGroup.Add(current[^1].Entry);
        return (groups, lastPerGroup);
    }
}
