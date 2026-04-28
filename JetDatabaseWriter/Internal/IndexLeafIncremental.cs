namespace JetDatabaseWriter;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;

/// <summary>
/// Single-leaf fast-path helper: in-place incremental insert and delete
/// against a single-leaf JET index B-tree (Jet4 / ACE only). Decodes the
/// existing leaf back into the per-entry tuples it would have been built from
/// (honouring the §4.4 prefix compression header), splices the change into the
/// sorted entry list, and re-emits the leaf via
/// <see cref="IndexLeafPageBuilder.BuildJet4LeafPage(int, long, IReadOnlyList{IndexLeafPageBuilder.LeafEntry}, long, long, long, bool)"/>
/// with prefix compression enabled.
/// <para>
/// The fast path applies only when the index B-tree is rooted at a single
/// leaf page (<c>page_type = 0x04</c>) and the resulting entry set still fits
/// on one page. Any tree with one or more intermediate (<c>0x03</c>) levels —
/// or any single-page tree whose post-mutation entry list overflows the
/// payload area — falls back to the bulk <c>MaintainIndexesAsync</c> rebuild
/// path or the multi-leaf surgical paths. See
/// <c>docs/design/index-and-relationship-format-notes.md</c> §7.
/// </para>
/// </summary>
internal static class IndexLeafIncremental
{
    /// <summary>Page type byte for index intermediate (<c>0x03</c>) pages.</summary>
    internal const byte PageTypeIntermediate = IndexBTreeBuilder.PageTypeIntermediate;

    private const byte PageTypeLeaf = IndexLeafPageBuilder.PageTypeLeaf;

    /// <summary>
    /// A decoded leaf entry: the canonical (uncompressed) key bytes plus the
    /// <c>(data_page, data_row)</c> row pointer.
    /// </summary>
    internal readonly struct DecodedEntry
    {
        public DecodedEntry(byte[] key, long dataPage, byte dataRow)
        {
            Key = key;
            DataPage = dataPage;
            DataRow = dataRow;
        }

        public byte[] Key { get; }

        public long DataPage { get; }

        public byte DataRow { get; }
    }

    /// <summary>
    /// A decoded intermediate (<c>0x03</c>) page entry: the canonical
    /// (uncompressed) summary key bytes, the row pointer of the LAST entry on
    /// the referenced child page, and the absolute page number of that child.
    /// Used by the surgical multi-level mutation path so the
    /// writer can re-emit a single intermediate page in place after a
    /// summary-key change or a leaf-split insert.
    /// </summary>
    internal readonly struct DecodedIntermediateEntry
    {
        public DecodedIntermediateEntry(byte[] key, long dataPage, byte dataRow, long childPage)
        {
            Key = key;
            DataPage = dataPage;
            DataRow = dataRow;
            ChildPage = childPage;
        }

        public byte[] Key { get; }

        public long DataPage { get; }

        public byte DataRow { get; }

        public long ChildPage { get; }
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="page"/> is an
    /// intermediate page (<c>page_type = 0x03</c>). Used by the multi-level
    /// fast path to decide whether to descend through the tree to the
    /// leftmost leaf before walking the leaf-sibling chain.
    /// </summary>
    public static bool IsIntermediate(byte[] page)
    {
        if (page == null || page.Length < 32)
        {
            return false;
        }

        return page[0] == PageTypeIntermediate;
    }

    /// <summary>
    /// Returns the page number recorded in the <c>next_page</c> sibling
    /// pointer of a leaf page (offset 12, signed 32-bit little-endian).
    /// 0 means "no further leaf to the right".
    /// </summary>
    public static long ReadNextLeafPage(byte[] leafPage)
    {
        if (leafPage == null || leafPage.Length < 16)
        {
            return 0;
        }

        return (uint)BinaryPrimitives.ReadInt32LittleEndian(leafPage.AsSpan(12, 4));
    }

    /// <summary>
    /// Returns the page number recorded in the <c>tail_page</c> header
    /// field of any index page (offset 16, signed 32-bit little-endian per
    /// §4.1). On intermediates emitted by <see cref="IndexBTreeBuilder"/>
    /// this is the absolute page number of the rightmost leaf. On a single-leaf root it is 0 (the leaf is its own tail).
    /// </summary>
    public static long ReadTailPage(byte[] page)
    {
        if (page == null || page.Length < 20)
        {
            return 0;
        }

        return (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(16, 4));
    }

    /// <summary>
    /// Returns the page number recorded in the <c>prev_page</c> sibling
    /// pointer of any index page (offset 8, signed 32-bit little-endian).
    /// </summary>
    public static long ReadPrevPage(byte[] page)
    {
        if (page == null || page.Length < 12)
        {
            return 0;
        }

        return (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(8, 4));
    }

    /// <summary>
    /// Decodes the child-page pointer of the FIRST entry on an intermediate
    /// (<c>0x03</c>) page. Each intermediate entry is laid out as
    /// <c>[stripped key bytes][3 B BE data page][1 B data row][4 B LE child page]</c>
    /// (§4.3); the entry's end is determined by the §4.2 bitmask (or the
    /// payload end when the entry is the only one on the page). Returns 0 on
    /// a malformed page so the caller can bail to the bulk-rebuild path.
    /// </summary>
    public static long ReadFirstChildPointer(byte[] intermediatePage, int pageSize)
        => ReadFirstChildPointer(IndexLeafPageBuilder.LeafPageLayout.Jet4, intermediatePage, pageSize);

    /// <summary>
    /// Layout-aware overload of <see cref="ReadFirstChildPointer(byte[], int)"/>
    /// for callers that may target Jet3 (§4.2 bitmask at <c>0x16</c>, first
    /// entry at <c>0xF8</c>).
    /// </summary>
    public static long ReadFirstChildPointer(IndexLeafPageBuilder.LeafPageLayout layout, byte[] intermediatePage, int pageSize)
    {
        if (intermediatePage == null || intermediatePage.Length < pageSize || intermediatePage[0] != PageTypeIntermediate)
        {
            return 0;
        }

        int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(intermediatePage.AsSpan(2, 2));
        int payloadEnd = pageSize - freeSpace;
        if (payloadEnd <= layout.FirstEntryOffset)
        {
            return 0;
        }

        int next = NextEntryStart(layout, intermediatePage, payloadEnd, layout.FirstEntryOffset);
        int entryEnd = next < 0 ? payloadEnd : next;
        int totalLen = entryEnd - layout.FirstEntryOffset;

        // Each intermediate entry trails with [3 B page][1 B row][4 B child page].
        if (totalLen < 8)
        {
            return 0;
        }

        int childOff = entryEnd - 4;
        return BinaryPrimitives.ReadUInt32LittleEndian(intermediatePage.AsSpan(childOff, 4));
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="page"/> is a leaf
    /// (<c>page_type = 0x04</c>) AND has no sibling leaves
    /// (<c>prev_page == 0</c> and <c>next_page == 0</c>). The latter is
    /// enforced because a single-leaf root never has siblings; non-zero
    /// sibling pointers indicate an intermediate's child or a tail-page
    /// chain, neither of which the fast path handles.
    /// </summary>
    public static bool IsSingleRootLeaf(byte[] page)
    {
        if (page == null || page.Length < 32)
        {
            return false;
        }

        if (page[0] != PageTypeLeaf)
        {
            return false;
        }

        int prev = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(8, 4));
        int next = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(12, 4));
        int tail = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(16, 4));
        return prev == 0 && next == 0 && tail == 0;
    }

    /// <summary>
    /// Decodes every entry on a single Jet4 / ACE leaf page back into its
    /// canonical (key, data_page, data_row) tuple, re-prepending the §4.4
    /// shared prefix to entries beyond the first.
    /// </summary>
    public static List<DecodedEntry> DecodeEntries(byte[] page, int pageSize)
        => DecodeEntries(IndexLeafPageBuilder.LeafPageLayout.Jet4, page, pageSize);

    /// <summary>
    /// Layout-aware overload of <see cref="DecodeEntries(byte[], int)"/> for
    /// callers that may target Jet3 leaf pages (§4.2 bitmask at <c>0x16</c>,
    /// first entry at <c>0xF8</c>).
    /// </summary>
    public static List<DecodedEntry> DecodeEntries(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page, int pageSize)
    {
        var result = new List<DecodedEntry>();
        int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(20, 2));
        int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
        int payloadEnd = pageSize - freeSpace;
        if (payloadEnd <= layout.FirstEntryOffset)
        {
            return result;
        }

        byte[]? sharedPrefix = null;
        int entryStart = layout.FirstEntryOffset;
        bool isFirst = true;
        while (entryStart < payloadEnd)
        {
            int next = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = next < 0 ? payloadEnd : next;
            int totalLen = entryEnd - entryStart;

            // Leaf entry: [stripped key bytes] + 3-byte BE data page + 1-byte data row.
            int suffixLen = totalLen - 4;
            if (suffixLen < 0)
            {
                break;
            }

            byte[] canonical;
            if (isFirst)
            {
                canonical = new byte[suffixLen];
                Buffer.BlockCopy(page, entryStart, canonical, 0, suffixLen);
                if (pref > 0 && suffixLen >= pref)
                {
                    sharedPrefix = new byte[pref];
                    Buffer.BlockCopy(canonical, 0, sharedPrefix, 0, pref);
                }
            }
            else
            {
                canonical = new byte[pref + suffixLen];
                if (pref > 0 && sharedPrefix != null)
                {
                    Buffer.BlockCopy(sharedPrefix, 0, canonical, 0, pref);
                }

                Buffer.BlockCopy(page, entryStart, canonical, pref, suffixLen);
            }

            int dpOff = entryStart + suffixLen;
            long dp = AccessBase.ReadUInt24BigEndian(page.AsSpan(dpOff, 3));
            byte dr = page[dpOff + 3];
            result.Add(new DecodedEntry(canonical, dp, dr));

            isFirst = false;
            if (next < 0)
            {
                break;
            }

            entryStart = next;
        }

        return result;
    }

    /// <summary>
    /// Builds the post-mutation entry list by inserting <paramref name="adds"/>
    /// (in sort order) and removing every entry whose <c>(data_page, data_row)</c>
    /// matches one in <paramref name="removes"/>. Returns <see langword="null"/>
    /// when removal fails to locate a target entry (defensive — should not
    /// happen if the caller's hint is consistent with the leaf state).
    /// </summary>
    /// <param name="existing">Decoded entries from the live leaf.</param>
    /// <param name="adds">New entries to insert; need not be sorted.</param>
    /// <param name="removes">Row pointers whose entries should be removed.</param>
    public static List<IndexLeafPageBuilder.LeafEntry>? Splice(
        List<DecodedEntry> existing,
        IReadOnlyList<(byte[] Key, long DataPage, byte DataRow)> adds,
        IReadOnlyList<(long DataPage, byte DataRow)> removes)
    {
        var working = new List<(byte[] Key, long DataPage, byte DataRow)>(existing.Count + adds.Count);
        if (removes.Count == 0)
        {
            foreach (DecodedEntry e in existing)
            {
                working.Add((e.Key, e.DataPage, e.DataRow));
            }
        }
        else
        {
            // Build a hashset of removal pointers for O(1) lookup. RowLocation
            // tuples are unique per row by construction (page + slot index).
            var removeSet = new HashSet<long>(removes.Count);
            foreach ((long page, byte row) in removes)
            {
                removeSet.Add(EncodePtr(page, row));
            }

            int removed = 0;
            foreach (DecodedEntry e in existing)
            {
                if (removeSet.Remove(EncodePtr(e.DataPage, e.DataRow)))
                {
                    removed++;
                    continue;
                }

                working.Add((e.Key, e.DataPage, e.DataRow));
            }

            if (removed != removes.Count)
            {
                // One or more removes did not resolve. Bail to bulk rebuild.
                return null;
            }
        }

        foreach ((byte[] k, long dp, byte dr) in adds)
        {
            working.Add((k, dp, dr));
        }

        // Stable sort by key bytes. Ties retain insertion order, which keeps
        // the (page, row) ordering of equal-key entries deterministic.
        // List<T>.Sort is not stable; emulate by tagging the original index.
        var indexed = new (byte[] Key, long DataPage, byte DataRow, int Order)[working.Count];
        for (int i = 0; i < working.Count; i++)
        {
            indexed[i] = (working[i].Key, working[i].DataPage, working[i].DataRow, i);
        }

        Array.Sort(indexed, static (a, b) =>
        {
            int c = CompareBytes(a.Key, b.Key);
            return c != 0 ? c : a.Order - b.Order;
        });

        var result = new List<IndexLeafPageBuilder.LeafEntry>(indexed.Length);
        foreach ((byte[] k, long dp, byte dr, _) in indexed)
        {
            result.Add(new IndexLeafPageBuilder.LeafEntry(k, dp, dr));
        }

        return result;
    }

    /// <summary>
    /// Attempts to re-emit the leaf with the spliced entry list. Returns
    /// <see langword="null"/> when the new entries do not fit on one page —
    /// the caller must fall back to the bulk-rebuild path.
    /// </summary>
    public static byte[]? TryRebuildLeaf(
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexLeafPageBuilder.LeafEntry> entries)
        => TryRebuildLeaf(IndexLeafPageBuilder.LeafPageLayout.Jet4, pageSize, parentTdefPage, entries);

    /// <summary>
    /// Layout-aware overload of <see cref="TryRebuildLeaf(int, long, IReadOnlyList{IndexLeafPageBuilder.LeafEntry})"/>
    /// for callers that may target Jet3 leaf pages.
    /// </summary>
    public static byte[]? TryRebuildLeaf(
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexLeafPageBuilder.LeafEntry> entries)
    {
        try
        {
            return IndexLeafPageBuilder.BuildLeafPage(
                layout,
                pageSize,
                parentTdefPage,
                entries,
                prevPage: 0,
                nextPage: 0,
                tailPage: 0,
                enablePrefixCompression: true);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

    /// <summary>
    /// Re-emits a leaf page in place at its existing position
    /// in the sibling-leaf chain, preserving its <c>prev_page</c> /
    /// <c>next_page</c> / <c>tail_page</c> header fields. Returns
    /// <see langword="null"/> when the spliced entry list overflows a single
    /// page (caller falls back to a leaf split or the bulk rebuild).
    /// </summary>
    public static byte[]? TryRebuildLeafWithSiblings(
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexLeafPageBuilder.LeafEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage)
    {
        try
        {
            return IndexLeafPageBuilder.BuildLeafPage(
                layout,
                pageSize,
                parentTdefPage,
                entries,
                prevPage: prevPage,
                nextPage: nextPage,
                tailPage: tailPage,
                enablePrefixCompression: true);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

    /// <summary>
    /// Decodes every entry on a single intermediate (<c>0x03</c>) index page
    /// back into its canonical <c>(key, data_page, data_row, child_page)</c>
    /// tuple, re-prepending the §4.4 shared prefix to entries beyond the
    /// first. Used by the surgical multi-level mutation path
    /// when it needs to update a parent intermediate's stale summary entry or
    /// insert a new summary for a freshly-split leaf without rebuilding the
    /// whole tree.
    /// </summary>
    public static List<DecodedIntermediateEntry> DecodeIntermediateEntries(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize)
    {
        var result = new List<DecodedIntermediateEntry>();
        if (page == null || page.Length < pageSize || page[0] != PageTypeIntermediate)
        {
            return result;
        }

        int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(20, 2));
        int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
        int payloadEnd = pageSize - freeSpace;
        if (payloadEnd <= layout.FirstEntryOffset)
        {
            return result;
        }

        byte[]? sharedPrefix = null;
        int entryStart = layout.FirstEntryOffset;
        bool isFirst = true;
        while (entryStart < payloadEnd)
        {
            int next = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = next < 0 ? payloadEnd : next;
            int totalLen = entryEnd - entryStart;

            // Intermediate entry: [stripped key bytes] + 3-byte BE data page +
            // 1-byte data row + 4-byte LE child page → suffix length = totalLen - 8.
            int suffixLen = totalLen - 8;
            if (suffixLen < 0)
            {
                break;
            }

            byte[] canonical;
            if (isFirst)
            {
                canonical = new byte[suffixLen];
                Buffer.BlockCopy(page, entryStart, canonical, 0, suffixLen);
                if (pref > 0 && suffixLen >= pref)
                {
                    sharedPrefix = new byte[pref];
                    Buffer.BlockCopy(canonical, 0, sharedPrefix, 0, pref);
                }
            }
            else
            {
                canonical = new byte[pref + suffixLen];
                if (pref > 0 && sharedPrefix != null)
                {
                    Buffer.BlockCopy(sharedPrefix, 0, canonical, 0, pref);
                }

                Buffer.BlockCopy(page, entryStart, canonical, pref, suffixLen);
            }

            int trailerOff = entryStart + suffixLen;
            long dp = ((long)page[trailerOff] << 16) | ((long)page[trailerOff + 1] << 8) | page[trailerOff + 2];
            byte dr = page[trailerOff + 3];
            long childPage = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(trailerOff + 4, 4));
            result.Add(new DecodedIntermediateEntry(canonical, dp, dr, childPage));

            isFirst = false;
            if (next < 0)
            {
                break;
            }

            entryStart = next;
        }

        return result;
    }

    private static long EncodePtr(long page, byte row) => (page << 8) | row;

    private static int CompareBytes(byte[] a, byte[] b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                return diff;
            }
        }

        return a.Length - b.Length;
    }

    private static int NextEntryStart(byte[] page, int payloadEnd, int currentStart)
        => NextEntryStart(IndexLeafPageBuilder.LeafPageLayout.Jet4, page, payloadEnd, currentStart);

    private static int NextEntryStart(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page, int payloadEnd, int currentStart)
    {
        int searchStart = currentStart - layout.FirstEntryOffset + 1;
        for (int bit = searchStart; bit < payloadEnd - layout.FirstEntryOffset; bit++)
        {
            int byteOff = layout.BitmaskOffset + (bit / 8);
            if (byteOff >= layout.FirstEntryOffset)
            {
                return -1;
            }

            if ((page[byteOff] & (1 << (bit % 8))) != 0)
            {
                int candidate = layout.FirstEntryOffset + bit;
                return candidate < payloadEnd ? candidate : -1;
            }
        }

        return -1;
    }
}
