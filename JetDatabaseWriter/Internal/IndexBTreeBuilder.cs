namespace JetDatabaseWriter;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;

/// <summary>
/// Builds a complete JET index B-tree (W4 phase) from a sorted list of leaf
/// entries: one or more leaf pages (<c>0x04</c>) chained through
/// <c>prev_page</c> / <c>next_page</c>, plus zero or more levels of
/// intermediate pages (<c>0x03</c>) above them. The page layouts are
/// described in <c>docs/design/index-and-relationship-format-notes.md</c>
/// §4.1 (header), §4.2 (entry-start bitmask), §4.3 (per-entry record), and
/// §4.5 (tail-page chain).
/// <para>
/// Jet4 / ACE only. Bitmask is at <c>0x1B</c> and the first entry is at
/// <c>0x1E0</c> on every page (matching <see cref="IndexLeafPageBuilder"/>).
/// </para>
/// <para>
/// <b>Constraints / not done</b>:
/// </para>
/// <list type="bullet">
///   <item>No prefix compression (<c>pref_len</c> is always 0). §4.4.</item>
///   <item>No tail-page append optimisation (<c>tail_page</c> is always 0). §4.5.</item>
///   <item>No incremental updates: this builds a fresh tree from a sorted
///   entry list. Maintenance hooks on insert / update / delete are W5.</item>
/// </list>
/// </summary>
internal static class IndexBTreeBuilder
{
    /// <summary>Page type byte for index intermediate pages.</summary>
    internal const byte PageTypeIntermediate = 0x03;

    /// <summary>
    /// Result of <see cref="Build"/>: the rendered pages (in the order they
    /// should be appended to the database) and the absolute page number of
    /// the root, which the caller writes into the real-index
    /// <c>first_dp</c> field on the TDEF.
    /// </summary>
    internal readonly struct BuildResult
    {
        public BuildResult(IReadOnlyList<byte[]> pages, long rootPageNumber, long firstPageNumber)
        {
            Pages = pages;
            RootPageNumber = rootPageNumber;
            FirstPageNumber = firstPageNumber;
        }

        /// <summary>Gets the rendered pages, indexed [0..N-1]. Page i lives at
        /// absolute database page number <see cref="FirstPageNumber"/> + i.</summary>
        public IReadOnlyList<byte[]> Pages { get; }

        /// <summary>Gets the absolute page number of the root (leaf for a
        /// single-page tree, otherwise the topmost intermediate).</summary>
        public long RootPageNumber { get; }

        /// <summary>Gets the absolute page number assigned to <c>Pages[0]</c>.</summary>
        public long FirstPageNumber { get; }
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
    /// empty leaf page (the W3 placeholder behaviour).</param>
    /// <param name="firstPageNumber">First absolute page number to allocate.</param>
    public static BuildResult Build(
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexLeafPageBuilder.LeafEntry> entries,
        long firstPageNumber)
    {
        if (pageSize <= IndexLeafPageBuilder.Jet4FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be greater than {IndexLeafPageBuilder.Jet4FirstEntryOffset}.");
        }

        Guard.NotNull(entries, nameof(entries));

        if (firstPageNumber < 0 || firstPageNumber > 0xFFFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(firstPageNumber), "Page number exceeds the 24-bit child-pointer range.");
        }

        int entryAreaSize = pageSize - IndexLeafPageBuilder.Jet4FirstEntryOffset;

        // ── Step 1: Pack entries into leaves greedily, in input order. ────
        // Each leaf entry occupies EncodedKey.Length + 4 bytes (3-byte BE data
        // page + 1-byte data row). The entry-start bitmask spans the area from
        // 0x1B..0x1DF — 485 bytes = 3880 bits. The largest entry stride is
        // limited by the entry area (3616 bytes on a 4096-byte page) so the
        // bitmask never overflows in practice.
        var leafGroups = new List<List<IndexLeafPageBuilder.LeafEntry>>();
        if (entries.Count == 0)
        {
            leafGroups.Add(new List<IndexLeafPageBuilder.LeafEntry>(0));
        }
        else
        {
            var current = new List<IndexLeafPageBuilder.LeafEntry>();
            int currentSize = 0;
            for (int i = 0; i < entries.Count; i++)
            {
                IndexLeafPageBuilder.LeafEntry e = entries[i];
                int entryLen = e.EncodedKey.Length + 4;
                if (entryLen > entryAreaSize)
                {
                    throw new ArgumentOutOfRangeException(nameof(entries), $"Single index entry of {entryLen} bytes exceeds the {entryAreaSize}-byte payload area; one entry must fit on one page.");
                }

                if (currentSize + entryLen > entryAreaSize)
                {
                    leafGroups.Add(current);
                    current = new List<IndexLeafPageBuilder.LeafEntry>();
                    currentSize = 0;
                }

                current.Add(e);
                currentSize += entryLen;
            }

            leafGroups.Add(current);
        }

        // ── Step 2: Assign absolute page numbers to leaves. ───────────────
        int leafCount = leafGroups.Count;
        long[] leafPageNumbers = new long[leafCount];
        for (int i = 0; i < leafCount; i++)
        {
            leafPageNumbers[i] = firstPageNumber + i;
        }

        if (leafPageNumbers[leafCount - 1] > 0xFFFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(firstPageNumber), "Allocated page numbers exceed the 24-bit child-pointer range.");
        }

        // ── Step 3: Render leaves with prev/next sibling chain. ───────────
        var pages = new List<byte[]>(leafCount);
        for (int i = 0; i < leafCount; i++)
        {
            long prev = i == 0 ? 0 : leafPageNumbers[i - 1];
            long next = i == leafCount - 1 ? 0 : leafPageNumbers[i + 1];
            byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(
                pageSize,
                parentTdefPage,
                leafGroups[i],
                prevPage: prev,
                nextPage: next,
                tailPage: 0,
                enablePrefixCompression: true);
            pages.Add(leaf);
        }

        // Single leaf is its own root — no intermediates needed.
        if (leafCount == 1)
        {
            return new BuildResult(pages, leafPageNumbers[0], firstPageNumber);
        }

        // ── Step 4: Build intermediate levels until we reach a single root.
        // Each intermediate entry summarises the LAST leaf entry of its child
        // and appends the 4-byte child page pointer (§4.3). An empty child has
        // no last entry — that case is impossible here because an empty leaf
        // can only occur when entries.Count == 0, which short-circuits at
        // leafCount == 1.
        long[] childPageNumbers = leafPageNumbers;
        IReadOnlyList<IndexLeafPageBuilder.LeafEntry> childLastEntries = LastEntries(leafGroups);
        long nextFreePage = firstPageNumber + leafCount;

        while (childPageNumbers.Length > 1)
        {
            (List<List<IntermediateEntry>> groups, List<IndexLeafPageBuilder.LeafEntry> nextLevelLast) =
                PackIntermediate(childPageNumbers, childLastEntries, entryAreaSize);

            int levelCount = groups.Count;
            long[] levelPageNumbers = new long[levelCount];
            for (int i = 0; i < levelCount; i++)
            {
                levelPageNumbers[i] = nextFreePage + i;
            }

            if (levelPageNumbers[levelCount - 1] > 0xFFFFFF)
            {
                throw new ArgumentOutOfRangeException(nameof(firstPageNumber), "Allocated page numbers exceed the 24-bit child-pointer range.");
            }

            for (int i = 0; i < levelCount; i++)
            {
                long prev = i == 0 ? 0 : levelPageNumbers[i - 1];
                long next = i == levelCount - 1 ? 0 : levelPageNumbers[i + 1];
                byte[] page = BuildIntermediatePage(
                    pageSize,
                    parentTdefPage,
                    groups[i],
                    prevPage: prev,
                    nextPage: next);
                pages.Add(page);
            }

            childPageNumbers = levelPageNumbers;
            childLastEntries = nextLevelLast;
            nextFreePage += levelCount;
        }

        long rootPageNumber = childPageNumbers[0];
        return new BuildResult(pages, rootPageNumber, firstPageNumber);
    }

    private static List<IndexLeafPageBuilder.LeafEntry> LastEntries(List<List<IndexLeafPageBuilder.LeafEntry>> groups)
    {
        var last = new List<IndexLeafPageBuilder.LeafEntry>(groups.Count);
        for (int i = 0; i < groups.Count; i++)
        {
            List<IndexLeafPageBuilder.LeafEntry> g = groups[i];
            last.Add(g[g.Count - 1]);
        }

        return last;
    }

    private readonly struct IntermediateEntry
    {
        public IntermediateEntry(IndexLeafPageBuilder.LeafEntry summary, long childPage)
        {
            Summary = summary;
            ChildPage = childPage;
        }

        public IndexLeafPageBuilder.LeafEntry Summary { get; }

        public long ChildPage { get; }

        public int OnDiskSize => Summary.EncodedKey.Length + 4 + 4; // key + (3B page + 1B row) + 4B child
    }

    private static (List<List<IntermediateEntry>> Groups, List<IndexLeafPageBuilder.LeafEntry> LastPerGroup) PackIntermediate(
        long[] childPageNumbers,
        IReadOnlyList<IndexLeafPageBuilder.LeafEntry> childLastEntries,
        int entryAreaSize)
    {
        var groups = new List<List<IntermediateEntry>>();
        var lastPerGroup = new List<IndexLeafPageBuilder.LeafEntry>();

        var current = new List<IntermediateEntry>();
        int currentSize = 0;
        for (int i = 0; i < childPageNumbers.Length; i++)
        {
            var entry = new IntermediateEntry(childLastEntries[i], childPageNumbers[i]);
            int len = entry.OnDiskSize;
            if (len > entryAreaSize)
            {
                throw new ArgumentOutOfRangeException(nameof(childLastEntries), "Intermediate entry exceeds page payload area.");
            }

            if (currentSize + len > entryAreaSize)
            {
                groups.Add(current);
                lastPerGroup.Add(current[current.Count - 1].Summary);
                current = new List<IntermediateEntry>();
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
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IntermediateEntry> entries,
        long prevPage,
        long nextPage)
    {
        byte[] page = new byte[pageSize];

        page[0] = PageTypeIntermediate; // page_type = 0x03
        page[1] = 0x01;                 // unknown

        // free_space patched at end.
        Wi32(page, 4, checked((int)parentTdefPage));
        Wi32(page, 8, checked((int)prevPage));
        Wi32(page, 12, checked((int)nextPage));
        Wi32(page, 16, 0);   // tail_page

        // §4.4 prefix compression on intermediate pages: hoist the longest
        // shared encoded-key prefix into the header and strip it from every
        // entry beyond the first.
        int prefLen = ComputeIntermediatePrefixLength(entries);
        Wu16(page, 20, prefLen);

        int payloadCursor = IndexLeafPageBuilder.Jet4FirstEntryOffset;
        int payloadLimit = pageSize;

        for (int i = 0; i < entries.Count; i++)
        {
            IntermediateEntry e = entries[i];
            byte[] key = e.Summary.EncodedKey;
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
            page[rpOff + 0] = (byte)((dp >> 16) & 0xFF);
            page[rpOff + 1] = (byte)((dp >> 8) & 0xFF);
            page[rpOff + 2] = (byte)(dp & 0xFF);
            page[rpOff + 3] = e.Summary.DataRow;

            // 4-byte child page pointer (little-endian, like every other 32-bit
            // page-number field in the JET on-disk format).
            long cp = e.ChildPage;
            if (cp < 0 || cp > 0xFFFFFFFFL)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), "Child page exceeds 32-bit range.");
            }

            int cpOff = rpOff + 4;
            page[cpOff + 0] = (byte)(cp & 0xFF);
            page[cpOff + 1] = (byte)((cp >> 8) & 0xFF);
            page[cpOff + 2] = (byte)((cp >> 16) & 0xFF);
            page[cpOff + 3] = (byte)((cp >> 24) & 0xFF);

            // §4.2 bitmask: every entry except the first sets a bit at its start
            // offset relative to the first-entry offset, LSB-first.
            if (i > 0)
            {
                int bitIndex = entryStart - IndexLeafPageBuilder.Jet4FirstEntryOffset;
                int byteOff = IndexLeafPageBuilder.Jet4BitmaskOffset + (bitIndex / 8);
                int bit = bitIndex % 8;
                if (byteOff >= IndexLeafPageBuilder.Jet4FirstEntryOffset)
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

        byte[] first = entries[0].Summary.EncodedKey;
        int prefixLen = first.Length;
        for (int i = 1; i < entries.Count && prefixLen > 0; i++)
        {
            byte[] other = entries[i].Summary.EncodedKey;
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
}
