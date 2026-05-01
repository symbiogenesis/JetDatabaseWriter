namespace JetDatabaseWriter.Internal.Builders;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;

/// <summary>
/// Builds a complete JET index B-tree from a sorted list of leaf
/// entries: one or more leaf pages (<c>0x04</c>) chained through
/// <c>prev_page</c> / <c>next_page</c>, plus zero or more levels of
/// intermediate pages (<c>0x03</c>) above them. The page layouts are
/// described in <c>docs/design/index-and-relationship-format-notes.md</c>
/// §4.1 (header), §4.2 (entry-start bitmask), §4.3 (per-entry record), and
/// §4.5 (tail-page chain).
/// <para>
/// Both Jet4 / ACE and Jet3 layouts are emitted via the
/// <see cref="IndexLeafPageBuilder.LeafPageLayout"/> descriptor passed to
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
///   absolute page number of the rightmost leaf so a reader / seeker can short-circuit
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
        => Build(IndexLeafPageBuilder.LeafPageLayout.Jet4, pageSize, parentTdefPage, entries, firstPageNumber);

    /// <summary>
    /// Builds a complete index B-tree using the supplied per-format
    /// <paramref name="layout"/> (Jet3 or Jet4 / ACE). See the parameterless-layout
    /// overload for the contract.
    /// </summary>
    public static BuildResult Build(
        IndexLeafPageBuilder.LeafPageLayout layout,
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
                    splitPageLastEntries.Add(current[current.Count - 1]);
                    current = [];
                    currentSize = 0;
                }

                current.Add(e);
                currentSize += entryLen;
            }

            splitPages.Add(current);
            splitPageLastEntries.Add(current[current.Count - 1]);
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
            byte[] leaf = IndexLeafPageBuilder.BuildLeafPage(
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
        // intermediate-page tail_page header so the seeker can jump directly
        // to the tail without descending the tree, and so the append-only
        // incremental fast path can locate it from the root in one read.
        long tailLeafPage = firstPageNumber + splitPageCount - 1;

        while (childPageCount > 1)
        {
            (List<List<IntermediateEntry>> groups, List<IndexEntry> nextLevelLast) =
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
                byte[] page = BuildIntermediatePage(
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
    /// page from an arbitrary list of <c>(summaryKey, dataPage, dataRow,
    /// childPage)</c> tuples (sorted by summary key), preserving the supplied
    /// <c>prev_page</c> / <c>next_page</c> / <c>tail_page</c> headers. Returns
    /// <see langword="null"/> when the entry list overflows the per-page
    /// payload area; callers fall back to <see cref="Build(IndexLeafPageBuilder.LeafPageLayout, int, long, IReadOnlyList{IndexEntry}, long)"/>
    /// (full-tree rebuild) on overflow.
    /// </summary>
    public static byte[]? TryBuildIntermediatePage(
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<DecodedIntermediateEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage)
    {
        Guard.NotNull(entries, nameof(entries));

        if (entries.Count == 0)
        {
            // Empty intermediate makes no sense — caller must collapse / merge.
            return null;
        }

        var packed = new List<IntermediateEntry>(entries.Count);
        foreach (DecodedIntermediateEntry e in entries)
        {
            packed.Add(new IntermediateEntry(e.Entry, e.ChildPage));
        }

        try
        {
            return BuildIntermediatePage(layout, pageSize, parentTdefPage, packed, prevPage, nextPage, tailPage);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

    private readonly struct IntermediateEntry(IndexEntry summary, long childPage)
    {
        public IndexEntry Summary { get; } = summary;

        public long ChildPage { get; } = childPage;

        public int OnDiskSize => Summary.Key.Length + 4 + 4; // key + (3B page + 1B row) + 4B child
    }

    private static (List<List<IntermediateEntry>> Groups, List<IndexEntry> LastPerGroup) PackIntermediate(
        long childPageBase,
        int childPageCount,
        IReadOnlyList<IndexEntry> childLastEntries,
        int entryAreaSize)
    {
        // Rough capacity hint: assume ~64-byte average summary key ⇒ each
        // intermediate page holds entryAreaSize / (64 + 8) entries. Errs high.
        int estPagesAtThisLevel = Math.Max(1, ((childPageCount * 72) + entryAreaSize - 1) / entryAreaSize);
        var groups = new List<List<IntermediateEntry>>(estPagesAtThisLevel);
        var lastPerGroup = new List<IndexEntry>(estPagesAtThisLevel);

        var current = new List<IntermediateEntry>();
        int currentSize = 0;
        for (int i = 0; i < childPageCount; i++)
        {
            var entry = new IntermediateEntry(childLastEntries[i], childPageBase + i);
            int len = entry.OnDiskSize;
            if (len > entryAreaSize)
            {
                throw new ArgumentOutOfRangeException(nameof(childLastEntries), "Intermediate entry exceeds page payload area.");
            }

            if (currentSize + len > entryAreaSize)
            {
                groups.Add(current);
                lastPerGroup.Add(current[current.Count - 1].Summary);
                current = [];
                currentSize = 0;
            }

            current.Add(entry);
            currentSize += len;
        }

        groups.Add(current);
        lastPerGroup.Add(current[current.Count - 1].Summary);
        return (groups, lastPerGroup);
    }

    private static byte[] BuildIntermediatePage(
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IntermediateEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage)
    {
        byte[] page = new byte[pageSize];

        page[0] = Constants.IndexLeafPage.PageTypeIntermediate; // page_type = 0x03
        page[1] = 0x01;                 // unknown

        // free_space patched at end.
        Wi32(page, 4, checked((int)parentTdefPage));
        Wi32(page, 8, checked((int)prevPage));
        Wi32(page, 12, checked((int)nextPage));
        Wi32(page, 16, checked((int)tailPage));   // tail_page (rightmost leaf in the tree)

        // §4.4 prefix compression on intermediate pages: hoist the longest
        // shared encoded-key prefix into the header and strip it from every
        // entry beyond the first.
        int prefLen = ComputeIntermediatePrefixLength(entries);
        Wu16(page, 20, prefLen);

        int payloadCursor = layout.FirstEntryOffset;
        int payloadLimit = pageSize;

        for (int i = 0; i < entries.Count; i++)
        {
            IntermediateEntry e = entries[i];
            byte[] key = e.Summary.Key;
            int keyOffset = i == 0 ? 0 : prefLen;
            int keyLen = key.Length - keyOffset;
            int entryLen = keyLen + 4 + 4;
            int entryStart = payloadCursor;

            if (entryStart + entryLen > payloadLimit)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), "Intermediate page overflow (internal error).");
            }

            Buffer.BlockCopy(key, keyOffset, page, entryStart, keyLen);

            // 3-byte BE data page + 1-byte data row (summary of last child entry).
            long dp = e.Summary.DataPage;
            int rpOff = entryStart + keyLen;
            WriteUInt24Be(page, rpOff, (int)dp);
            page[rpOff + 3] = e.Summary.DataRow;

            // 4-byte child page pointer (little-endian, like every other 32-bit
            // page-number field in the JET on-disk format).
            long cp = e.ChildPage;
            if (cp < 0 || cp > 0xFFFFFFFFL)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), "Child page exceeds 32-bit range.");
            }

            int cpOff = rpOff + 4;
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(cpOff, 4), (uint)cp);

            // §4.2 bitmask: every entry except the first sets a bit at its start
            // offset relative to the first-entry offset, LSB-first.
            if (i > 0)
            {
                int bitIndex = entryStart - layout.FirstEntryOffset;
                int byteOff = layout.BitmaskOffset + (bitIndex / 8);
                int bit = bitIndex % 8;
                if (byteOff >= layout.FirstEntryOffset)
                {
                    throw new ArgumentOutOfRangeException(nameof(entries), "Bitmask overflow on intermediate page.");
                }

                page[byteOff] |= (byte)(1 << bit);
            }

            payloadCursor += entryLen;
        }

        Wu16(page, 2, payloadLimit - payloadCursor); // free_space
        return page;
    }

    private static int ComputeIntermediatePrefixLength(IReadOnlyList<IntermediateEntry> entries)
    {
        if (entries.Count < 2)
        {
            return 0;
        }

        byte[] first = entries[0].Summary.Key;
        int prefixLen = first.Length;
        for (int i = 1; i < entries.Count && prefixLen > 0; i++)
        {
            byte[] other = entries[i].Summary.Key;
            int max = Math.Min(prefixLen, other.Length);
            int j = 0;
            while (j < max && first[j] == other[j])
            {
                j++;
            }

            prefixLen = j;
        }

        if (prefixLen > 0xFFFF)
        {
            prefixLen = 0xFFFF;
        }

        return prefixLen;
    }

    private static void Wu16(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(o, 2), (ushort)value);

    private static void Wi32(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(o, 4), value);

    private static void WriteUInt24Be(byte[] b, int o, int value)
    {
        b[o] = (byte)((value >> 16) & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
        b[o + 2] = (byte)(value & 0xFF);
    }
}
