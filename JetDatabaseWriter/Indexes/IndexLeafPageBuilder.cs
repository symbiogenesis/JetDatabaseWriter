namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Builds JET index leaf pages (page type <c>0x04</c>). Encodes the
/// fixed page header described in <see href="docs/design/index-and-relationship-format-notes.md" />
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
    /// Per-format index page layout descriptor. The §4.1 page header layout
    /// differs between Jet3 (no unknown(0) at offset 8) and Jet4 / ACE
    /// (unknown(0) at offset 8 shifts prev/next/tail/pref_len 4 bytes
    /// later); the §4.2 entry-start bitmask offset and first-entry offset
    /// also differ, along with the database page size (2048 vs 4096).
    /// All offsets verified against Jackcess <c>JetFormat</c> constants and
    /// empirically against Access-authored fixtures (see
    /// <c>Constants.IndexLeafPage</c> for the reference observation).
    /// </summary>
    /// <param name="bitmaskOffset">The bitmask offset.</param>
    /// <param name="firstEntryOffset">The first entry offset.</param>
    /// <param name="prevPageOffset">The prev page offset.</param>
    /// <param name="nextPageOffset">The next page offset.</param>
    /// <param name="tailPageOffset">The tail page offset.</param>
    /// <param name="prefLenOffset">The pref len offset.</param>
    internal readonly struct LeafPageLayout(
        int bitmaskOffset,
        int firstEntryOffset,
        int prevPageOffset,
        int nextPageOffset,
        int tailPageOffset,
        int prefLenOffset)
    {
        /// <summary>Gets the Jet3 (<c>.mdb</c> Access 97) leaf page layout.</summary>
        public static LeafPageLayout Jet3 => new(
            Constants.IndexLeafPage.Jet3.BitmaskOffset,
            Constants.IndexLeafPage.Jet3.FirstEntryOffset,
            Constants.IndexLeafPage.Jet3.PrevPageOffset,
            Constants.IndexLeafPage.Jet3.NextPageOffset,
            Constants.IndexLeafPage.Jet3.TailPageOffset,
            Constants.IndexLeafPage.Jet3.PrefLenOffset);

        /// <summary>Gets the Jet4 / ACE leaf page layout.</summary>
        public static LeafPageLayout Jet4 => new(
            Constants.IndexLeafPage.Jet4.BitmaskOffset,
            Constants.IndexLeafPage.Jet4.FirstEntryOffset,
            Constants.IndexLeafPage.Jet4.PrevPageOffset,
            Constants.IndexLeafPage.Jet4.NextPageOffset,
            Constants.IndexLeafPage.Jet4.TailPageOffset,
            Constants.IndexLeafPage.Jet4.PrefLenOffset);

        /// <summary>Gets the byte offset of the entry-start bitmask within the page.</summary>
        public int BitmaskOffset { get; } = bitmaskOffset;

        /// <summary>Gets the byte offset of the first entry payload within the page.</summary>
        public int FirstEntryOffset { get; } = firstEntryOffset;

        /// <summary>Gets the byte offset of the prev_page header field.</summary>
        public int PrevPageOffset { get; } = prevPageOffset;

        /// <summary>Gets the byte offset of the next_page header field.</summary>
        public int NextPageOffset { get; } = nextPageOffset;

        /// <summary>Gets the byte offset of the tail_page (childTail) header field.</summary>
        public int TailPageOffset { get; } = tailPageOffset;

        /// <summary>Gets the byte offset of the pref_len (page-shared prefix length, u16) header field.</summary>
        public int PrefLenOffset { get; } = prefLenOffset;
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
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
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
    /// (<c>pref_len</c> in the page header) and stripped from every entry beyond
    /// the first. The first entry is always written whole because it carries
    /// the canonical bytes that subsequent entries logically prepend (§4.4).
    /// </summary>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="enablePrefixCompression">Whether to emit shared-prefix compression metadata.</param>
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
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="enablePrefixCompression">Whether to emit shared-prefix compression metadata.</param>
    /// <param name="maxPrefixLength">Maximum number of leading bytes that may be shared through prefix compression.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the page size, entry payload, data-page pointer, or entry bitmask exceeds the format limits.</exception>
    public static byte[] BuildLeafPage(
        LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression,
        int? maxPrefixLength = null)
        => IndexPageCodec.BuildLeafPage(
            layout,
            pageSize,
            parentTdefPage,
            entries,
            prevPage,
            nextPage,
            tailPage,
            enablePrefixCompression,
            maxPrefixLength);

    /// <summary>
    /// Attempts to build an index leaf page, returning <see langword="null"/>
    /// when the supplied entries do not fit in one page.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    public static byte[]? TryBuildLeafPage(
        LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries)
        => TryBuildLeafPage(layout, pageSize, parentTdefPage, entries, prevPage: 0, nextPage: 0, tailPage: 0, enablePrefixCompression: true);

    /// <summary>
    /// Attempts to build an index leaf page while preserving sibling pointers,
    /// returning <see langword="null"/> when the supplied entries do not fit in one page.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
    public static byte[]? TryBuildLeafPage(
        LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage)
        => TryBuildLeafPage(layout, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression: true);

    /// <summary>
    /// Attempts to build an index leaf page, returning <see langword="null"/>
    /// when the supplied entries do not fit in one page.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="enablePrefixCompression">Whether to emit shared-prefix compression metadata.</param>
    /// <param name="maxPrefixLength">Maximum number of leading bytes that may be shared through prefix compression.</param>
    public static byte[]? TryBuildLeafPage(
        LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression,
        int? maxPrefixLength = null)
    {
        try
        {
            return BuildLeafPage(layout, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression, maxPrefixLength);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
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
}
