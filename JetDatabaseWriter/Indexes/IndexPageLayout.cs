namespace JetDatabaseWriter.Indexes;

using JetDatabaseWriter.Enums;

/// <summary>
/// Per-format index page layout descriptor. The index page header layout
/// differs between Jet3 (no unknown(0) at offset 8) and Jet4 / ACE
/// (unknown(0) at offset 8 shifts prev/next/tail/pref_len 4 bytes later);
/// the entry-start bitmask offset and first-entry offset also differ.
/// </summary>
/// <param name="bitmaskOffset">The bitmask offset.</param>
/// <param name="firstEntryOffset">The first entry offset.</param>
/// <param name="prevPageOffset">The prev page offset.</param>
/// <param name="nextPageOffset">The next page offset.</param>
/// <param name="tailPageOffset">The tail page offset.</param>
/// <param name="prefLenOffset">The pref len offset.</param>
internal readonly struct IndexPageLayout(
    int bitmaskOffset,
    int firstEntryOffset,
    int prevPageOffset,
    int nextPageOffset,
    int tailPageOffset,
    int prefLenOffset)
{
    /// <summary>Gets the Jet3 (<c>.mdb</c> Access 97) index page layout.</summary>
    public static IndexPageLayout Jet3 => new(
        Constants.IndexLeafPage.Jet3.BitmaskOffset,
        Constants.IndexLeafPage.Jet3.FirstEntryOffset,
        Constants.IndexLeafPage.Jet3.PrevPageOffset,
        Constants.IndexLeafPage.Jet3.NextPageOffset,
        Constants.IndexLeafPage.Jet3.TailPageOffset,
        Constants.IndexLeafPage.Jet3.PrefLenOffset);

    /// <summary>Gets the Jet4 / ACE index page layout.</summary>
    public static IndexPageLayout Jet4 => new(
        Constants.IndexLeafPage.Jet4.BitmaskOffset,
        Constants.IndexLeafPage.Jet4.FirstEntryOffset,
        Constants.IndexLeafPage.Jet4.PrevPageOffset,
        Constants.IndexLeafPage.Jet4.NextPageOffset,
        Constants.IndexLeafPage.Jet4.TailPageOffset,
        Constants.IndexLeafPage.Jet4.PrefLenOffset);

    /// <summary>
    /// Returns the correct <see cref="IndexPageLayout"/> for the specified <see cref="DatabaseFormat"/>.
    /// </summary>
    /// <param name="format">The database format.</param>
    public static IndexPageLayout ForFormat(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? Jet3 : Jet4;

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
