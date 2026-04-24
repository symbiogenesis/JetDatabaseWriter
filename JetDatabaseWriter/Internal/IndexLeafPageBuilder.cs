namespace JetDatabaseWriter;

using System;
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
        Wi32(page, 8, 0);   // prev_page
        Wi32(page, 12, 0);  // next_page
        Wi32(page, 16, 0);  // tail_page (W4 will populate)
        Wu16(page, 20, 0);  // pref_len (no prefix compression in W3)

        // Bytes 22..0x1A inclusive are reserved; left zeroed.
        // Bitmask spans [0x1B .. 0x1DF] inclusive (485 bytes = 3880 bits) on Jet4.
        // Entry payload starts at 0x1E0.

        int payloadCursor = Jet4FirstEntryOffset;
        int payloadLimit = pageSize;

        for (int i = 0; i < entries.Count; i++)
        {
            LeafEntry e = entries[i];

            // §4.3: entry = encoded_key + 3-byte BE data_page + 1-byte data_row
            // (intermediate pages would also append a 4-byte child pointer; not used here).
            int entryLen = e.EncodedKey.Length + 3 + 1;
            int entryStart = payloadCursor;

            if (entryStart + entryLen > payloadLimit)
            {
                string message = $"Index entries do not fit on a single Jet4 leaf page (need {entryStart + entryLen} bytes, have {payloadLimit}). B-tree splitting (W4) is required for tables this large.";
                throw new ArgumentOutOfRangeException(nameof(entries), message);
            }

            Buffer.BlockCopy(e.EncodedKey, 0, page, entryStart, e.EncodedKey.Length);

            // Data page: 24-bit big-endian.
            long dp = e.DataPage;
            if (dp < 0 || dp > 0xFFFFFF)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), $"Index entry data page {dp} exceeds the 24-bit range.");
            }

            int dpOff = entryStart + e.EncodedKey.Length;
            page[dpOff + 0] = (byte)((dp >> 16) & 0xFF);
            page[dpOff + 1] = (byte)((dp >> 8) & 0xFF);
            page[dpOff + 2] = (byte)(dp & 0xFF);
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

    private static void Wu16(byte[] b, int o, int value)
    {
        b[o + 0] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
    }

    private static void Wi32(byte[] b, int o, int value)
    {
        b[o + 0] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
        b[o + 2] = (byte)((value >> 16) & 0xFF);
        b[o + 3] = (byte)((value >> 24) & 0xFF);
    }
}
