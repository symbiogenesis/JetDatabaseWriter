namespace JetDatabaseWriter.Internal;

using System;

/// <summary>
/// "General" (Access 2010+ default) text-index sort-key encoder. Port of
/// <c>com.healthmarketscience.jackcess.impl.GeneralIndexCodes</c> (Apache
/// 2.0 — see <c>THIRD-PARTY-NOTICES.md</c>).
/// <para>
/// Structurally identical to <see cref="GeneralLegacyTextIndexEncoder"/>:
/// upstream Jackcess models <c>GeneralIndexCodes</c> as a subclass of
/// <c>GeneralLegacyIndexCodes</c> that overrides <c>getCharHandler</c> only.
/// We mirror that by sharing
/// <see cref="GeneralLegacyTextIndexEncoder.EncodeWithTables"/> and supplying
/// the <c>index_codes_gen.txt</c> / <c>index_codes_ext_gen.txt</c> code
/// tables (gzipped resources) instead of the General-Legacy tables.
/// </para>
/// </summary>
internal static class GeneralTextIndexEncoder
{
    private const string GenResource = "JetDatabaseWriter.IndexCodeTables.index_codes_gen.txt.gz";
    private const string GenExtResource = "JetDatabaseWriter.IndexCodeTables.index_codes_ext_gen.txt.gz";

    private const char FirstChar = (char)0x0000;
    private const char LastChar = (char)0x00FF;
    private const char FirstExtChar = (char)0x0100;
    private const char LastExtChar = (char)0xFFFF;

    private static readonly Lazy<GeneralLegacyTextIndexEncoder.CharHandler[]> Codes = new(
        () => GeneralLegacyTextIndexEncoder.LoadCodes(GenResource, FirstChar, LastChar));

    private static readonly Lazy<GeneralLegacyTextIndexEncoder.CharHandler[]> ExtCodes = new(
        () => GeneralLegacyTextIndexEncoder.LoadCodes(GenExtResource, FirstExtChar, LastExtChar));

    /// <summary>
    /// Encodes a single text value as the complete per-column entry block
    /// (flag byte + payload + END_EXTRA_TEXT) using the Access 2010+ General
    /// sort-order code tables. For null inputs returns a single-byte block
    /// with the null flag.
    /// </summary>
    public static byte[] Encode(string? text, bool ascending)
        => GeneralLegacyTextIndexEncoder.EncodeWithTables(
            text,
            ascending,
            Codes.Value,
            ExtCodes.Value,
            GeneralLegacyTextIndexEncoder.LongRowSeparatorGeneral);
}
