namespace JetDatabaseWriter.Internal;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;

/// <summary>
/// Single-leaf fast-path helper: in-place incremental insert and delete
/// against a single-leaf JET index B-tree (Jet4 / ACE only). Decodes the
/// existing leaf back into the per-entry tuples it would have been built from
/// (honouring the §4.4 prefix compression header), splices the change into the
/// sorted entry list, and re-emits the leaf via
/// <see cref="IndexLeafPageBuilder.BuildJet4LeafPage(int, long, IReadOnlyList{IndexEntry}, long, long, long, bool)"/>
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

        return page[0] == Constants.IndexLeafPage.PageTypeIntermediate;
    }

    /// <summary>
    /// Returns the page number recorded in the <c>next_page</c> sibling
    /// pointer of a leaf page. 0 means "no further leaf to the right".
    /// Defaults to the Jet4 / ACE layout (next_page at offset 16);
    /// callers targeting Jet3 should use the layout overload.
    /// </summary>
    public static long ReadNextLeafPage(byte[] leafPage)
        => ReadNextLeafPage(IndexLeafPageBuilder.LeafPageLayout.Jet4, leafPage);

    /// <summary>
    /// Layout-aware overload of <see cref="ReadNextLeafPage(byte[])"/>.
    /// </summary>
    public static long ReadNextLeafPage(IndexLeafPageBuilder.LeafPageLayout layout, byte[] leafPage)
    {
        if (leafPage == null || leafPage.Length < layout.NextPageOffset + 4)
        {
            return 0;
        }

        return (uint)BinaryPrimitives.ReadInt32LittleEndian(leafPage.AsSpan(layout.NextPageOffset, 4));
    }

    /// <summary>
    /// Returns the page number recorded in the <c>tail_page</c> header
    /// field of any index page (signed 32-bit little-endian per §4.1).
    /// On intermediates emitted by <see cref="IndexBTreeBuilder"/> this is
    /// the absolute page number of the rightmost leaf. On a single-leaf
    /// root it is 0 (the leaf is its own tail). Defaults to the Jet4 / ACE
    /// layout (tail_page at offset 20).
    /// </summary>
    public static long ReadTailPage(byte[] page)
        => ReadTailPage(IndexLeafPageBuilder.LeafPageLayout.Jet4, page);

    /// <summary>
    /// Layout-aware overload of <see cref="ReadTailPage(byte[])"/>.
    /// </summary>
    public static long ReadTailPage(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < layout.TailPageOffset + 4)
        {
            return 0;
        }

        return (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(layout.TailPageOffset, 4));
    }

    /// <summary>
    /// Returns the page number recorded in the <c>prev_page</c> sibling
    /// pointer of any index page (signed 32-bit little-endian). Defaults to
    /// the Jet4 / ACE layout (prev_page at offset 12).
    /// </summary>
    public static long ReadPrevPage(byte[] page)
        => ReadPrevPage(IndexLeafPageBuilder.LeafPageLayout.Jet4, page);

    /// <summary>
    /// Layout-aware overload of <see cref="ReadPrevPage(byte[])"/>.
    /// </summary>
    public static long ReadPrevPage(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < layout.PrevPageOffset + 4)
        {
            return 0;
        }

        return (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(layout.PrevPageOffset, 4));
    }

    /// <summary>
    /// Reads the three sibling-pointer header fields (<c>prev_page</c>,
    /// <c>next_page</c>, <c>tail_page</c>) of any index page in one call.
    /// Each is interpreted as an unsigned 32-bit little-endian page number
    /// (zero = absent). Returns <c>(0, 0, 0)</c> when the page is too short
    /// to contain all three fields.
    /// </summary>
    public static (long Prev, long Next, long Tail) ReadSiblingPointers(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < layout.TailPageOffset + 4)
        {
            return (0, 0, 0);
        }

        long prev = (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(layout.PrevPageOffset, 4));
        long next = (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(layout.NextPageOffset, 4));
        long tail = (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(layout.TailPageOffset, 4));
        return (prev, next, tail);
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
        if (intermediatePage == null || intermediatePage.Length < pageSize || intermediatePage[0] != Constants.IndexLeafPage.PageTypeIntermediate)
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
        return DecodeIntermediateChildPointer(intermediatePage, childOff);
    }

    /// <summary>
    /// Reads the 4-byte big-endian child-page pointer at
    /// <paramref name="offset"/> on an intermediate (<c>0x03</c>) page. The
    /// 3-byte data-page summary preceding it on the same entry is also
    /// big-endian, while every other 32-bit page-number field elsewhere on
    /// the page (prev/next/tail/parent) is little-endian — that mixture is
    /// the on-disk convention used by Microsoft Access-authored
    /// intermediates and is now matched by
    /// <see cref="IndexBTreeBuilder"/>.
    /// </summary>
    internal static long DecodeIntermediateChildPointer(byte[] page, int offset)
    {
        if (page == null || offset < 0 || offset + 4 > page.Length)
        {
            return 0;
        }

        return BinaryPrimitives.ReadUInt32BigEndian(page.AsSpan(offset, 4));
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
        => IsSingleRootLeaf(IndexLeafPageBuilder.LeafPageLayout.Jet4, page);

    /// <summary>
    /// Layout-aware overload of <see cref="IsSingleRootLeaf(byte[])"/>.
    /// </summary>
    public static bool IsSingleRootLeaf(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < layout.TailPageOffset + 4)
        {
            return false;
        }

        if (page[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        var (prev, next, tail) = ReadSiblingPointers(layout, page);
        return prev == 0 && next == 0 && tail == 0;
    }

    /// <summary>
    /// Decodes every entry on a single Jet4 / ACE leaf page back into its
    /// canonical (key, data_page, data_row) tuple, re-prepending the §4.4
    /// shared prefix to entries beyond the first.
    /// </summary>
    public static List<IndexEntry> DecodeEntries(byte[] page, int pageSize)
        => DecodeEntries(IndexLeafPageBuilder.LeafPageLayout.Jet4, page, pageSize);

    /// <summary>
    /// Layout-aware overload of <see cref="DecodeEntries(byte[], int)"/> for
    /// callers that may target Jet3 leaf pages (§4.2 bitmask at <c>0x16</c>,
    /// first entry at <c>0xF8</c>).
    /// </summary>
    public static List<IndexEntry> DecodeEntries(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page, int pageSize)
    {
        var result = new List<IndexEntry>();
        int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(layout.PrefLenOffset, 2));
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
            result.Add(new IndexEntry(canonical, dp, dr));

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
    public static List<IndexEntry>? Splice(
        List<IndexEntry> existing,
        IReadOnlyList<IndexEntry> adds,
        IReadOnlyList<(long DataPage, byte DataRow)> removes)
    {
        var working = new List<IndexEntry>(existing.Count + adds.Count);
        if (removes.Count == 0)
        {
            foreach (IndexEntry e in existing)
            {
                working.Add(new IndexEntry(e.Key, e.DataPage, e.DataRow));
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
            foreach (IndexEntry e in existing)
            {
                if (removeSet.Remove(EncodePtr(e.DataPage, e.DataRow)))
                {
                    removed++;
                    continue;
                }

                working.Add(new IndexEntry(e.Key, e.DataPage, e.DataRow));
            }

            if (removed != removes.Count)
            {
                // One or more removes did not resolve. Bail to bulk rebuild.
                return null;
            }
        }

        foreach ((byte[] k, long dp, byte dr) in adds)
        {
            working.Add(new IndexEntry(k, dp, dr));
        }

        // Stable sort by key bytes. Ties retain insertion order, which keeps
        // the (page, row) ordering of equal-key entries deterministic.
        // List<T>.Sort is not stable; emulate by tagging the original index.
        var indexed = new (IndexEntry Entry, int Order)[working.Count];
        for (int i = 0; i < working.Count; i++)
        {
            indexed[i] = (working[i], i);
        }

        Array.Sort(indexed, static (a, b) =>
        {
            int c = CompareBytes(a.Entry.Key, b.Entry.Key);
            return c != 0 ? c : a.Order - b.Order;
        });

        var result = new List<IndexEntry>(indexed.Length);
        foreach ((IndexEntry entry, _) in indexed)
        {
            result.Add(entry);
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
        IReadOnlyList<IndexEntry> entries)
        => TryRebuildLeaf(IndexLeafPageBuilder.LeafPageLayout.Jet4, pageSize, parentTdefPage, entries);

    /// <summary>
    /// Layout-aware overload of <see cref="TryRebuildLeaf(int, long, IReadOnlyList{IndexEntry})"/>
    /// for callers that may target Jet3 leaf pages.
    /// </summary>
    public static byte[]? TryRebuildLeaf(
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries)
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
        IReadOnlyList<IndexEntry> entries,
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
        if (page == null || page.Length < pageSize || page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
        {
            return result;
        }

        int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(layout.PrefLenOffset, 2));
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
            // 1-byte data row + 4-byte BE child page → suffix length = totalLen - 8.
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
            long childPage = DecodeIntermediateChildPointer(page, trailerOff + 4);
            result.Add(new DecodedIntermediateEntry(new(canonical, dp, dr), childPage));

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
