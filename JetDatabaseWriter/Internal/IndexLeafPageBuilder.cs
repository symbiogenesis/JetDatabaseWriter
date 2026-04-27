namespace JetDatabaseWriter;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;

/// <summary>
/// Builds JET index leaf pages (page type <c>0x04</c>, W3 phase). Encodes the
/// fixed page header described in <c>docs/design/index-and-relationship-format-notes.md</c>
/// §4.1, the entry-start bitmask in §4.2, and the per-entry record layout in
/// §4.3 (excluding prefix compression and the intermediate-page child pointer
/// — both deferred to W4).
/// <para>
/// Only Jet4 / ACE leaf layouts are emitted (bitmask at <c>0x1B</c>,
/// first entry at <c>0x1E0</c>). Jet3 (<c>.mdb</c> Access 97) bitmask
/// offsets differ and are out of scope until a separate Jet3 fixture is
/// available to validate the emitted bytes.
/// </para>
/// <para>
/// <b>What this builder does NOT do</b> (deferred to later writer phases):
/// </para>
/// <list type="bullet">
///   <item>B-tree splits or intermediate (<c>0x03</c>) page emission (W4).</item>
///   <item>Tail-page chain maintenance (W4).</item>
///   <item>Prefix compression — <c>pref_len</c> is always 0 (W4 optimization).</item>
///   <item>Index maintenance on insert / update / delete (W5).</item>
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
    /// <summary>Page type byte for index leaf pages.</summary>
    internal const byte PageTypeLeaf = 0x04;

    /// <summary>Bitmask offset on a Jet4 leaf page (§4.2).</summary>
    internal const int Jet4BitmaskOffset = 0x1B;

    /// <summary>First-entry offset on a Jet4 leaf page (§4.2).</summary>
    internal const int Jet4FirstEntryOffset = 0x1E0;

    /// <summary>Bitmask offset on a Jet3 leaf page (§4.2, W17a-confirmed).</summary>
    internal const int Jet3BitmaskOffset = 0x16;

    /// <summary>First-entry offset on a Jet3 leaf page (§4.2, W17a-confirmed).</summary>
    internal const int Jet3FirstEntryOffset = 0xF8;

    /// <summary>
    /// A single leaf entry: the encoded key block produced by
    /// <see cref="IndexKeyEncoder.EncodeEntry(byte, object?, bool)"/> followed
    /// by the row pointer (data page + data row).
    /// </summary>
    internal readonly struct LeafEntry
    {
        public LeafEntry(byte[] encodedKey, long dataPage, byte dataRow)
        {
            EncodedKey = encodedKey ?? throw new ArgumentNullException(nameof(encodedKey));
            DataPage = dataPage;
            DataRow = dataRow;
        }

        /// <summary>Gets the flag byte + key bytes (per <see cref="IndexKeyEncoder"/>).</summary>
        public byte[] EncodedKey { get; }

        /// <summary>Gets the page number of the row this entry indexes (24-bit big-endian on disk).</summary>
        public long DataPage { get; }

        /// <summary>Gets the row index on the data page.</summary>
        public byte DataRow { get; }
    }

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
    /// leaf and W4 (B-tree splits) is required.</exception>
    public static byte[] BuildJet4LeafPage(int pageSize, long parentTdefPage, IReadOnlyList<LeafEntry> entries)
        => BuildJet4LeafPage(pageSize, parentTdefPage, entries, prevPage: 0, nextPage: 0, tailPage: 0, enablePrefixCompression: false);

    /// <summary>
    /// Builds a single Jet4 / ACE index leaf page with caller-supplied sibling
    /// pointers. Used by <see cref="IndexBTreeBuilder"/> (W4) to chain a row of
    /// leaf pages together.
    /// </summary>
    public static byte[] BuildJet4LeafPage(
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<LeafEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage)
        => BuildJet4LeafPage(pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression: false);

    /// <summary>
    /// Builds a single Jet4 / ACE index leaf page with caller-supplied sibling
    /// pointers and optional shared-prefix compression (§4.4). When
    /// <paramref name="enablePrefixCompression"/> is <c>true</c> and at least
    /// two entries are supplied, the longest byte-wise prefix common to every
    /// <see cref="LeafEntry.EncodedKey"/> is hoisted into the page header
    /// (<c>pref_len</c> at offset 20) and stripped from every entry beyond
    /// the first. The first entry is always written whole because it carries
    /// the canonical bytes that subsequent entries logically prepend (§4.4).
    /// </summary>
    public static byte[] BuildJet4LeafPage(
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<LeafEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression)
    {
        if (pageSize <= Jet4FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be greater than {Jet4FirstEntryOffset}.");
        }

        Guard.NotNull(entries, nameof(entries));

        byte[] page = new byte[pageSize];

        // ── Header (§4.1) ────────────────────────────────────────────────
        page[0] = PageTypeLeaf; // page_type
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

        // Bytes 22..0x1A inclusive are reserved; left zeroed.
        // Bitmask spans [0x1B .. 0x1DF] inclusive (485 bytes = 3880 bits) on Jet4.
        // Entry payload starts at 0x1E0.

        int payloadCursor = Jet4FirstEntryOffset;
        int payloadLimit = pageSize;

        for (int i = 0; i < entries.Count; i++)
        {
            LeafEntry e = entries[i];

            // §4.3 + §4.4: first entry includes the shared-prefix bytes; later
            // entries strip the leading prefLen bytes (the reader logically
            // re-prepends them on read).
            int keyOffset = i == 0 ? 0 : prefLen;
            int keyLen = e.EncodedKey.Length - keyOffset;
            int entryLen = keyLen + 3 + 1;
            int entryStart = payloadCursor;

            if (entryStart + entryLen > payloadLimit)
            {
                string message = $"Index entries do not fit on a single Jet4 leaf page (need {entryStart + entryLen} bytes, have {payloadLimit}). B-tree splitting (W4) is required for tables this large.";
                throw new ArgumentOutOfRangeException(nameof(entries), message);
            }

            Buffer.BlockCopy(e.EncodedKey, keyOffset, page, entryStart, keyLen);

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
                int bitIndex = entryStart - Jet4FirstEntryOffset;
                int byteOff = Jet4BitmaskOffset + (bitIndex / 8);
                int bit = bitIndex % 8;
                if (byteOff >= Jet4FirstEntryOffset)
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
    /// Builds an empty Jet3 (<c>.mdb</c> Access 97) index leaf page (W17b).
    /// Page header layout matches Jet4 (§4.1, probe-confirmed identical between
    /// formats by W17a) but the entry-start bitmask lives at <c>0x16</c> and
    /// the first entry begins at <c>0xF8</c> (§4.2). The page is emitted with
    /// no entries because Jet3 live index maintenance is not implemented;
    /// Microsoft Access rebuilds the leaf on the next Compact &amp; Repair
    /// pass once any rows are inserted (same schema-only fallback model used
    /// for unsupported Jet4 key types).
    /// </summary>
    /// <param name="pageSize">Database page size (2048 for Jet3).</param>
    /// <param name="parentTdefPage">Page number of the table's TDEF page.</param>
    public static byte[] BuildJet3EmptyLeafPage(int pageSize, long parentTdefPage)
    {
        if (pageSize <= Jet3FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be greater than {Jet3FirstEntryOffset}.");
        }

        byte[] page = new byte[pageSize];

        // ── Header (§4.1, identical between Jet3 and Jet4) ───────────────
        page[0] = PageTypeLeaf; // page_type
        page[1] = 0x01;         // unknown (always 0x01)
        Wi32(page, 4, checked((int)parentTdefPage)); // parent_page (TDEF)

        // prev/next/tail @ 8/12/16 are 0; pref_len @ 20 is 0 (no entries).
        // Bitmask spans [0x16 .. 0xF7] inclusive on Jet3 (§4.2). Empty page:
        // no bitmask bits are set and no entry bytes are written.
        int freeSpace = pageSize - Jet3FirstEntryOffset;
        Wu16(page, 2, freeSpace);
        return page;
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

    private static int ComputeSharedPrefixLength(IReadOnlyList<LeafEntry> entries)
    {
        if (entries.Count < 2)
        {
            return 0;
        }

        byte[] first = entries[0].EncodedKey;
        int prefixLen = first.Length;
        for (int i = 1; i < entries.Count && prefixLen > 0; i++)
        {
            byte[] other = entries[i].EncodedKey;
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
    internal static int ComputeSharedPrefixLengthExternal(IReadOnlyList<LeafEntry> entries)
        => ComputeSharedPrefixLength(entries);
}
