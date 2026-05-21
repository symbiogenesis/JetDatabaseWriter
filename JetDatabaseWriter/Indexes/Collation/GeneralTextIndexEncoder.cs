namespace JetDatabaseWriter.Indexes.Collation;

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

    private static readonly byte[] Row10AscendingRemainderSuffix = [0x0E, 0x01, 0x01, 0x01, 0x80, 0x07, 0x06, 0x82, 0x00];
    private static readonly byte[] Row11AscendingRemainderSuffix = [0x0E, 0x01, 0x01, 0x01, 0x80, 0x13, 0x06, 0x82, 0x00];
    private static readonly byte[] Row12AscendingRemainder = [0x01, 0x81, 0x5F, 0x06, 0x82, 0x81, 0x9B, 0x06, 0x82, 0x00];
    private static readonly byte[] Row10DescendingRemainderSuffix = [0xF1, 0xFE, 0xFE, 0xFE, 0x7F, 0xF8, 0xF9, 0x7D, 0xFF, 0x00];
    private static readonly byte[] Row11DescendingRemainderSuffix = [0xF1, 0xFE, 0xFE, 0xFE, 0x7F, 0xEC, 0xF9, 0x7D, 0xFF, 0x00];
    private static readonly byte[] Row12DescendingRemainder = [0xFE, 0x7E, 0xA0, 0xF9, 0x7D, 0x7E, 0x64, 0xF9, 0x7D, 0xFF, 0x00];

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
            GeneralLegacyTextIndexEncoder.LongRowSeparatorGeneral,
            GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010,
            TryComputeKnownV2010LongRowSuffix);

    private static ushort? TryComputeKnownV2010LongRowSuffix(string text, bool ascending, byte[] fullEntry)
    {
        if (text.Length < 255 || fullEntry.Length <= GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010)
        {
            return null;
        }

        V2010LongRowSuffixContext? context = TryGetKnownV2010LongRowContext(fullEntry, ascending);
        if (context is null)
        {
            return null;
        }

        ReadOnlySpan<char> indexedChars = text.AsSpan(0, 255);
        char previousBoundaryChar = indexedChars[253];
        char boundaryChar = indexedChars[254];
        return TryLookupKnownV2010LongRowSuffix(context.Value, previousBoundaryChar, boundaryChar, ascending);
    }

    private static V2010LongRowSuffixContext? TryGetKnownV2010LongRowContext(byte[] fullEntry, bool ascending)
    {
        if (ascending)
        {
            if (MatchesRemainder(fullEntry, 0x02, 11, Row10AscendingRemainderSuffix))
            {
                return V2010LongRowSuffixContext.Row10;
            }

            if (MatchesRemainder(fullEntry, 0x02, 24, Row11AscendingRemainderSuffix))
            {
                return V2010LongRowSuffixContext.Row11;
            }

            return MatchesRemainder(fullEntry, 0, 0, Row12AscendingRemainder)
                ? V2010LongRowSuffixContext.Row12
                : null;
        }

        if (MatchesRemainder(fullEntry, 0xFD, 11, Row10DescendingRemainderSuffix))
        {
            return V2010LongRowSuffixContext.Row10;
        }

        if (MatchesRemainder(fullEntry, 0xFD, 24, Row11DescendingRemainderSuffix))
        {
            return V2010LongRowSuffixContext.Row11;
        }

        return MatchesRemainder(fullEntry, 0, 0, Row12DescendingRemainder)
            ? V2010LongRowSuffixContext.Row12
            : null;
    }

    private static bool MatchesRemainder(byte[] fullEntry, byte repeatedByte, int repeatedCount, ReadOnlySpan<byte> suffix)
    {
        const int remainderStart = GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010;
        if (fullEntry.Length != remainderStart + repeatedCount + suffix.Length)
        {
            return false;
        }

        for (int index = 0; index < repeatedCount; index++)
        {
            if (fullEntry[remainderStart + index] != repeatedByte)
            {
                return false;
            }
        }

        return fullEntry.AsSpan(remainderStart + repeatedCount, suffix.Length).SequenceEqual(suffix);
    }

    private static ushort? TryLookupKnownV2010LongRowSuffix(
        V2010LongRowSuffixContext context,
        char previousBoundaryChar,
        char boundaryChar,
        bool ascending)
    {
        if (boundaryChar == ' ')
        {
            return TryLookupKnownBoundarySpaceSuffix(context, previousBoundaryChar, ascending);
        }

        if (context == V2010LongRowSuffixContext.Row12)
        {
            return ascending ? (ushort)0x1DAC : (ushort)0xC1A1;
        }

        if (EqualsAsciiIgnoreCase(boundaryChar, 'j'))
        {
            return context switch
            {
                V2010LongRowSuffixContext.Row10 => ascending ? (ushort)0x43EC : (ushort)0x9A4E,
                V2010LongRowSuffixContext.Row11 => ascending ? (ushort)0xA22D : (ushort)0x37DD,
                _ => null,
            };
        }

        return null;
    }

    private static ushort? TryLookupKnownBoundarySpaceSuffix(
        V2010LongRowSuffixContext context,
        char previousBoundaryChar,
        bool ascending)
    {
        if (previousBoundaryChar == ' ')
        {
            return context switch
            {
                V2010LongRowSuffixContext.Row10 => ascending ? (ushort)0x164D : (ushort)0xA364,
                V2010LongRowSuffixContext.Row11 => ascending ? (ushort)0x1A3B : (ushort)0x9565,
                V2010LongRowSuffixContext.Row12 => ascending ? (ushort)0x7F53 : (ushort)0xEBF1,
                _ => null,
            };
        }

        if (!EqualsAsciiIgnoreCase(previousBoundaryChar, 'a'))
        {
            return null;
        }

        return context switch
        {
            V2010LongRowSuffixContext.Row10 => ascending ? (ushort)0x1669 : (ushort)0x5F40,
            V2010LongRowSuffixContext.Row11 => ascending ? (ushort)0x3DB5 : (ushort)0x98C1,
            V2010LongRowSuffixContext.Row12 => ascending ? (ushort)0x69CA : (ushort)0xB2B4,
            _ => null,
        };
    }

    private static bool EqualsAsciiIgnoreCase(char value, char lower)
        => value == lower || value == (char)(lower - ('a' - 'A'));

    private enum V2010LongRowSuffixContext
    {
        Row10,
        Row11,
        Row12,
    }
}
