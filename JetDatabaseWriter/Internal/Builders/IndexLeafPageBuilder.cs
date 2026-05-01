namespace JetDatabaseWriter.Internal.Builders;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;

/// <summary>
/// Builds JET index leaf pages (page type <c>0x04</c>). Encodes the
/// fixed page header described in <c>docs/design/index-and-relationship-format-notes.md</c>
/// §4.1, the entry-start bitmask in §4.2, and the per-entry record layout in
/// §4.3 (excluding the intermediate-page child pointer — deferred to
/// <see cref="IndexBTreeBuilder"/>). §4.4 prefix compression is supported.
/// <para>
/// Both Jet4 / ACE (bitmask at <c>0x1B</c>, first entry at <c>0x1E0</c>) and
/// Jet3 (bitmask at <c>0x16</c>, first entry at <c>0xF8</c>) leaf layouts
/// are emitted via the shared <see cref="LeafPageLayout"/> descriptor.
/// </para>
/// <para>
/// <b>What this builder does NOT do</b> (deferred to later writer phases):
/// </para>
/// <list type="bullet">
///   <item>B-tree splits or intermediate (<c>0x03</c>) page emission.</item>
///   <item>Tail-page chain maintenance.</item>
///   <item>Prefix compression — <c>pref_len</c> is always 0.</item>
///   <item>Index maintenance on insert / update / delete.</item>
/// </list>
/// <para>
/// As a result, a leaf page produced here is consistent with the matching
/// real-index physical descriptor (<c>first_dp</c>) only at the moment it is
/// emitted. Once the table mutates, the leaf goes stale until Microsoft Access
/// rebuilds it during Compact &amp; Repair.
/// </para>
/// </summary>
internal static class IndexLeafPageBuilder
{
    /// <summary>
    /// Per-format index page layout descriptor. The §4.1 page header is
    /// identical between Jet3 and Jet4 / ACE; only
    /// the §4.2 entry-start bitmask offset and the first-entry offset
    /// differ, along with the database page size (2048 vs 4096).
    /// </summary>
    internal readonly struct LeafPageLayout(int bitmaskOffset, int firstEntryOffset)
    {
        /// <summary>Gets the Jet3 (<c>.mdb</c> Access 97) leaf page layout.</summary>
        public static LeafPageLayout Jet3 => new(Constants.IndexLeafPage.Jet3BitmaskOffset, Constants.IndexLeafPage.Jet3FirstEntryOffset);

        /// <summary>Gets the Jet4 / ACE leaf page layout.</summary>
        public static LeafPageLayout Jet4 => new(Constants.IndexLeafPage.Jet4BitmaskOffset, Constants.IndexLeafPage.Jet4FirstEntryOffset);

        /// <summary>Gets the byte offset of the entry-start bitmask within the page.</summary>
        public int BitmaskOffset { get; } = bitmaskOffset;

        /// <summary>Gets the byte offset of the first entry payload within the page.</summary>
        public int FirstEntryOffset { get; } = firstEntryOffset;
    }

    /// <summary>
    /// Returns the correct <see cref="LeafPageLayout"/> for the specified <see cref="DatabaseFormat"/>.
    /// </summary>
    /// <param name="format">The database format (Jet3 or Jet4/ACE).</param>
    /// <returns>The corresponding <see cref="LeafPageLayout"/>.</returns>
    public static LeafPageLayout GetLayout(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? LeafPageLayout.Jet3 : LeafPageLayout.Jet4;

    /// <summary>
    /// Builds a single Jet4 / ACE index leaf page. Returns a buffer of size
    /// <paramref name="pageSize"/> that the caller is expected to append via
    /// <c>AppendPageAsync</c>.
    /// </summary>
    /// <param name="pageSize">Database page size (4096 for ACE, 4096 for Jet4 .mdb).</param>
    /// <param name="parentTdefPage">Page number of the table's TDEF page, recorded
    /// in the header at offset 4 so Access can navigate up the index hierarchy.</param>
    /// <param name="entries">Index entries to write, already in sort-key order.
    /// Pass an empty collection to emit an empty leaf (still valid: Access treats
    /// it as a placeholder root that will be rebuilt on next Compact &amp; Repair).</param>
    /// <exception cref="ArgumentOutOfRangeException">The combined entry payload
    /// (sum of <c>EncodedKey.Length + 4</c> for each entry) exceeds the available
    /// payload area, which means the table is too large for a single-page
    /// leaf and B-tree builder (B-tree splits) is required.</exception>
    public static byte[] BuildJet4LeafPage(int pageSize, long parentTdefPage, IReadOnlyList<IndexEntry> entries)
        => BuildLeafPage(LeafPageLayout.Jet4, pageSize, parentTdefPage, entries, prevPage: 0, nextPage: 0, tailPage: 0, enablePrefixCompression: false);

    /// <summary>
    /// Builds a single Jet4 / ACE index leaf page with caller-supplied sibling
    /// pointers. Used by <see cref="IndexBTreeBuilder"/> to chain a row of
    /// leaf pages together.
    /// </summary>
    public static byte[] BuildJet4LeafPage(
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage)
        => BuildLeafPage(LeafPageLayout.Jet4, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression: false);

    /// <summary>
    /// Builds a single Jet4 / ACE index leaf page with caller-supplied sibling
    /// pointers and optional shared-prefix compression (§4.4). When
    /// <paramref name="enablePrefixCompression"/> is <c>true</c> and at least
    /// two entries are supplied, the longest byte-wise prefix common to every
    /// <see cref="IndexEntry.Key"/> is hoisted into the page header
    /// (<c>pref_len</c> at offset 20) and stripped from every entry beyond
    /// the first. The first entry is always written whole because it carries
    /// the canonical bytes that subsequent entries logically prepend (§4.4).
    /// </summary>
    public static byte[] BuildJet4LeafPage(
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression)
        => BuildLeafPage(LeafPageLayout.Jet4, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression);

    /// <summary>
    /// Builds a single index leaf page using the supplied per-format
    /// <paramref name="layout"/> (Jet3 or Jet4 / ACE). Encodes the §4.1
    /// page header, §4.2 entry-start bitmask, §4.3 per-entry record, and
    /// — when <paramref name="enablePrefixCompression"/> is <c>true</c>
    /// and at least two entries are supplied — the §4.4 shared-prefix
    /// compression header. Jet3 live-leaf: the same code path now drives
    /// Jet3 leaf pages, lifting the previous "Jet3 indexes are schema-only"
    /// limitation.
    /// </summary>
    public static byte[] BuildLeafPage(
        LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression)
    {
        if (pageSize <= layout.FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be greater than {layout.FirstEntryOffset}.");
        }

        Guard.NotNull(entries, nameof(entries));

        byte[] page = new byte[pageSize];

        // ── Header (§4.1) ────────────────────────────────────────────────
        page[0] = Constants.IndexLeafPage.PageTypeLeaf; // page_type
        page[1] = 0x01;         // unknown (always 0x01)

        // free_space (offset 2, u16) is patched after we know the entry size.
        Wi32(page, 4, checked((int)parentTdefPage)); // parent_page (TDEF)
        Wi32(page, 8, checked((int)prevPage));   // prev_page
        Wi32(page, 12, checked((int)nextPage));  // next_page
        Wi32(page, 16, checked((int)tailPage));  // tail_page

        // §4.4: pref_len is the number of leading bytes that every entry
        // shares with the first entry. Stripped from every entry beyond the
        // first; the first entry is always written whole because it supplies
        // the canonical prefix bytes.
        int prefLen = enablePrefixCompression ? ComputeSharedPrefixLength(entries) : 0;
        Wu16(page, 20, prefLen);

        // Bytes 22..(layout.BitmaskOffset-1) are reserved; left zeroed.
        // Bitmask spans [layout.BitmaskOffset .. layout.FirstEntryOffset-1].
        // Entry payload starts at layout.FirstEntryOffset.

        int payloadCursor = layout.FirstEntryOffset;
        int payloadLimit = pageSize;

        for (int i = 0; i < entries.Count; i++)
        {
            IndexEntry e = entries[i];

            // §4.3 + §4.4: first entry includes the shared-prefix bytes; later
            // entries strip the leading prefLen bytes (the reader logically
            // re-prepends them on read).
            int keyOffset = i == 0 ? 0 : prefLen;
            int keyLen = e.Key.Length - keyOffset;
            int entryLen = keyLen + 3 + 1;
            int entryStart = payloadCursor;

            if (entryStart + entryLen > payloadLimit)
            {
                string message = $"Index entries do not fit on a single leaf page (need {entryStart + entryLen} bytes, have {payloadLimit}). B-tree splitting is required for tables this large.";
                throw new ArgumentOutOfRangeException(nameof(entries), message);
            }

            Buffer.BlockCopy(e.Key, keyOffset, page, entryStart, keyLen);

            // Data page: 24-bit big-endian.
            long dp = e.DataPage;
            if (dp < 0 || dp > 0xFFFFFF)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), $"Index entry data page {dp} exceeds the 24-bit range.");
            }

            int dpOff = entryStart + keyLen;
            WriteUInt24Be(page, dpOff, (int)dp);
            page[dpOff + 3] = e.DataRow;

            // §4.3 + §4.2: every entry except the first sets a bit in the bitmask
            // at the entry's start offset (relative to first_entry_offset, LSB-first).
            if (i > 0)
            {
                int bitIndex = entryStart - layout.FirstEntryOffset;
                int byteOff = layout.BitmaskOffset + (bitIndex / 8);
                int bit = bitIndex % 8;
                if (byteOff >= layout.FirstEntryOffset)
                {
                    throw new ArgumentOutOfRangeException(nameof(entries), "Bitmask overflow: too many entries for a single leaf page.");
                }

                page[byteOff] |= (byte)(1 << bit);
            }

            payloadCursor += entryLen;
        }

        // free_space is the count of unused bytes between the last written byte
        // and the page end. Write at offset 2 as u16.
        int freeSpace = payloadLimit - payloadCursor;
        Wu16(page, 2, freeSpace);

        return page;
    }

    /// <summary>
    /// Builds an empty Jet3 (<c>.mdb</c> Access 97) index leaf page.
    /// Page header layout matches Jet4 (§4.1, probe-confirmed identical between
    /// formats by format probe) but the entry-start bitmask lives at <c>0x16</c> and
    /// the first entry begins at <c>0xF8</c> (§4.2). Thin wrapper over
    /// <see cref="BuildLeafPage"/> with an empty entry list — preserved for
    /// the Jet3 empty-leaf create-time placeholder path; populated Jet3 leaf pages
    /// flow through <see cref="BuildLeafPage"/> directly.
    /// </summary>
    /// <param name="pageSize">Database page size (2048 for Jet3).</param>
    /// <param name="parentTdefPage">Page number of the table's TDEF page.</param>
    public static byte[] BuildJet3EmptyLeafPage(int pageSize, long parentTdefPage)
        => BuildLeafPage(LeafPageLayout.Jet3, pageSize, parentTdefPage, [], prevPage: 0, nextPage: 0, tailPage: 0, enablePrefixCompression: false);

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

    private static int ComputeSharedPrefixLength(IReadOnlyList<IndexEntry> entries)
    {
        if (entries.Count < 2)
        {
            return 0;
        }

        byte[] first = entries[0].Key;
        int prefixLen = first.Length;
        for (int i = 1; i < entries.Count && prefixLen > 0; i++)
        {
            byte[] other = entries[i].Key;
            int max = Math.Min(prefixLen, other.Length);
            int j = 0;
            while (j < max && first[j] == other[j])
            {
                j++;
            }

            prefixLen = j;
        }

        // pref_len is a u16 on disk; the cap is well below 65535 in practice
        // because encoded keys never exceed the 3616-byte payload area.
        if (prefixLen > 0xFFFF)
        {
            prefixLen = 0xFFFF;
        }

        return prefixLen;
    }

    /// <summary>
    /// Computes the longest byte prefix shared by every encoded key in the
    /// supplied entries. Exposed for <see cref="IndexBTreeBuilder"/> so
    /// intermediate (<c>0x03</c>) pages can apply the same §4.4 compression.
    /// </summary>
    internal static int ComputeSharedPrefixLengthExternal(IReadOnlyList<IndexEntry> entries)
        => ComputeSharedPrefixLength(entries);
}
