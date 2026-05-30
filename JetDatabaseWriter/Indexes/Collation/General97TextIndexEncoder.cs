namespace JetDatabaseWriter.Indexes.Collation;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;
using static JetDatabaseWriter.Constants.IndexEntryFlags;

/// <summary>
/// "General 97" (Access 1997 / Jet3) text-index sort-key encoder. Port of
/// <c>com.healthmarketscience.jackcess.impl.General97IndexCodes</c> (Apache
/// 2.0 — see <see href="THIRD-PARTY-NOTICES.md" />).
/// <para>
/// Differs from <see cref="GeneralLegacyTextIndexEncoder"/> in two ways:
/// </para>
/// <list type="number">
/// <item><description>Per-codepoint table covers only BMP <c>U+0000</c>–<c>U+00FF</c>
/// (loaded from <c>index_codes_gen_97.txt.gz</c>). Codepoints above
/// <c>U+00FF</c> use a small sparse <c>U+0152</c>–<c>U+2122</c> mapping
/// (from <c>index_mappings_ext_gen_97.txt.gz</c>) that redirects each
/// extended codepoint back into the BMP table; chars outside the mapped
/// range are ignored.</description></item>
/// <item><description>State machine: no <c>END_TEXT</c> framing, no
/// unprintable / crazy code streams. Extras are nibble-packed (two per byte,
/// hi nibble first) into a single trailing block bracketed by
/// <c>EXT_CODES_BOUNDS_NIBBLE = 0x0</c>. Each non-simple, non-significant
/// char contributes one extra-byte nibble preceded by
/// <c>INTERNATIONAL_EXTRA_PLACEHOLDER = 0x2</c> nibbles for any "significant"
/// (e.g. ASCII letter) chars seen since the last extra. If a value yields no
/// extras the trailer is a single <c>END_EXTRA_TEXT (0x00)</c> byte.</description></item>
/// </list>
/// </summary>
internal static class General97TextIndexEncoder
{
    private const string CodesResource = "JetDatabaseWriter.IndexCodeTables.index_codes_gen_97.txt.gz";
    private const string ExtMappingsResource = "JetDatabaseWriter.IndexCodeTables.index_mappings_ext_gen_97.txt.gz";

    private const char FirstChar = (char)0x0000;
    private const char LastChar = (char)0x00FF;
    private const char FirstMapChar = (char)338;
    private const char LastMapChar = (char)8482;

    private const byte ExtCodesBoundsNibble = 0x00;
    private const byte InternationalExtraPlaceholder = 0x02;

    private static readonly Lazy<GeneralLegacyTextIndexEncoder.CharHandler[]> Codes = new(
        () => GeneralLegacyTextIndexEncoder.LoadCodes(CodesResource, FirstChar, LastChar));

    private static readonly Lazy<short[]> ExtMappings = new(
        () => LoadMappings(ExtMappingsResource, FirstMapChar, LastMapChar));

    /// <summary>
    /// Encodes a single text value as the complete per-column entry block
    /// (flag byte + payload). For null inputs returns a single-byte block
    /// with the null flag.
    /// </summary>
    /// <param name="text">The text to encode.</param>
    /// <param name="ascending">The ascending.</param>
    public static byte[] Encode(string? text, bool ascending)
    {
        if (text is null)
        {
            return [ascending
                ? AscendingNull
                : DescendingNull];
        }

        // Per Jackcess GeneralLegacyIndexCodes.toIndexCharSequence — same
        // truncation/trim rule used for all sort orders (TEXT_FIELD_MAX_LENGTH
        // / TEXT_FIELD_UNIT_SIZE = 127 chars).
        ReadOnlySpan<char> chars = text.AsSpan(0, Math.Min(text.Length, Constants.IndexTextEncoding.MaxTextIndexCharLength)).TrimEnd(' ');
        int extraByteCapacity = GetExtraByteCapacity(chars.Length);

        var bytes = new List<byte>(chars.Length + extraByteCapacity + 2)
        {
            ascending
                ? AscendingNonNull
                : DescendingNonNull,
        };

        Span<byte> extraBytes = stackalloc byte[extraByteCapacity];
        int extraNibbleCount = 0;
        int significantCharCount = 0;
        GeneralLegacyTextIndexEncoder.CharHandler[] codes = Codes.Value;
        short[]? extMappings = null;

        foreach (char currentChar in chars)
        {
            GeneralLegacyTextIndexEncoder.CharHandler handler = GetCharHandler(currentChar, codes, ref extMappings);

            ReadOnlySpan<byte> inline = handler.GetInlineBytes(currentChar);
            if (!inline.IsEmpty)
            {
                AppendBytes(bytes, inline);
            }

            if (handler.Type == GeneralLegacyTextIndexEncoder.CharHandlerType.Simple)
            {
                continue;
            }

            if (handler.Type == GeneralLegacyTextIndexEncoder.CharHandlerType.Significant)
            {
                significantCharCount++;
                continue;
            }

            ReadOnlySpan<byte> extra = handler.ExtraBytes;
            if (!extra.IsEmpty)
            {
                if (extraNibbleCount == 0)
                {
                    WriteNibble(extraBytes, ref extraNibbleCount, ExtCodesBoundsNibble);
                }

                if (significantCharCount > 0)
                {
                    WriteFillNibbles(
                        extraBytes,
                        ref extraNibbleCount,
                        significantCharCount,
                        InternationalExtraPlaceholder);
                    significantCharCount = 0;
                }

                // General 97 only consumes the first extra-byte (low nibble).
                WriteNibble(extraBytes, ref extraNibbleCount, extra[0]);
            }
        }

        if (extraNibbleCount > 0)
        {
            WriteNibble(extraBytes, ref extraNibbleCount, ExtCodesBoundsNibble);
            AppendBytes(bytes, extraBytes[..GetByteLength(extraNibbleCount)]);
        }
        else
        {
            bytes.Add(GeneralLegacyTextIndexEncoder.EndExtraText);
        }

        if (!ascending)
        {
            for (int byteIndex = 1; byteIndex < bytes.Count; byteIndex++)
            {
                bytes[byteIndex] = unchecked((byte)~bytes[byteIndex]);
            }
        }

        return [.. bytes];
    }

    private static GeneralLegacyTextIndexEncoder.CharHandler GetCharHandler(
        char currentChar,
        GeneralLegacyTextIndexEncoder.CharHandler[] codes,
        ref short[]? extMappings)
    {
        if (currentChar <= LastChar)
        {
            return codes[currentChar];
        }

        if (currentChar is < FirstMapChar or > LastMapChar)
        {
            return GeneralLegacyTextIndexEncoder.IgnoredHandlerInstance;
        }

        // Some extended chars are equivalent to single-byte chars; the rest
        // map to 0 (which itself is an "ignored" char in the BMP table).
        extMappings ??= ExtMappings.Value;
        int extOffset = currentChar - FirstMapChar;
        return codes[extMappings[extOffset]];
    }

    private static int GetExtraByteCapacity(int charCount) => (charCount + 3) / 2;

    private static int GetByteLength(int nibbleCount) => (nibbleCount + 1) / 2;

    private static void WriteNibble(Span<byte> bytes, ref int nibbleCount, int value)
    {
        int byteIndex = nibbleCount / 2;
        if (nibbleCount % 2 == 0)
        {
            bytes[byteIndex] = unchecked((byte)((value << 4) & 0xF0));
        }
        else
        {
            bytes[byteIndex] = unchecked((byte)(bytes[byteIndex] | (value & 0x0F)));
        }

        nibbleCount++;
    }

    private static void WriteFillNibbles(Span<byte> bytes, ref int nibbleCount, int length, byte value)
    {
        for (int fillIndex = 0; fillIndex < length; fillIndex++)
        {
            WriteNibble(bytes, ref nibbleCount, value);
        }
    }

    private static void AppendBytes(List<byte> sink, ReadOnlySpan<byte> bytes)
    {
        foreach (byte value in bytes)
        {
            sink.Add(value);
        }
    }

    private static short[] LoadMappings(string resourceName, char firstChar, char lastChar)
    {
        int numMappings = lastChar - firstChar + 1;
        short[] values = new short[numMappings];

        Assembly asm = typeof(General97TextIndexEncoder).Assembly;
        using Stream raw = asm.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{resourceName}' not found.");
        using var gz = new GZipStream(raw, CompressionMode.Decompress);
        using var reader = new StreamReader(gz, Encoding.ASCII);

        // Sparse file with <fromCode>,<toCode> entries; missing rows stay 0
        // (which the BMP "ignored" handler at index 0 absorbs).
        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            ReadOnlySpan<char> trimmedLine = line.AsSpan().Trim();
            if (trimmedLine.IsEmpty)
            {
                continue;
            }

            int comma = trimmedLine.IndexOf(',');
            int fromCode = int.Parse(trimmedLine[..comma], NumberStyles.Integer, CultureInfo.InvariantCulture);
            int toCode = int.Parse(trimmedLine[(comma + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture);
            values[fromCode - firstChar] = (short)toCode;
        }

        return values;
    }
}
