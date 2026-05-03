namespace JetDatabaseWriter.Internal;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;

/// <summary>
/// "General 97" (Access 1997 / Jet3) text-index sort-key encoder. Port of
/// <c>com.healthmarketscience.jackcess.impl.General97IndexCodes</c> (Apache
/// 2.0 — see <c>THIRD-PARTY-NOTICES.md</c>).
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
    internal const int MaxTextIndexCharLength = GeneralLegacyTextIndexEncoder.MaxTextIndexCharLength;

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
    public static byte[] Encode(string? text, bool ascending)
    {
        if (text is null)
        {
            return [ascending
                ? GeneralLegacyTextIndexEncoder.FlagAscendingNull
                : GeneralLegacyTextIndexEncoder.FlagDescendingNull];
        }

        // Per Jackcess GeneralLegacyIndexCodes.toIndexCharSequence — same
        // truncation/trim rule used for all sort orders (TEXT_FIELD_MAX_LENGTH
        // / TEXT_FIELD_UNIT_SIZE = 127 chars).
        var chars = text.AsSpan(0, Math.Min(text.Length, MaxTextIndexCharLength)).TrimEnd(' ');

        var bout = new List<byte>(chars.Length + 4)
        {
            ascending
                ? GeneralLegacyTextIndexEncoder.FlagAscendingNonNull
                : GeneralLegacyTextIndexEncoder.FlagDescendingNonNull,
        };

        // Position immediately AFTER the start flag — never bit-flipped.
        int payloadStart = bout.Count;

        NibbleStream? extraCodes = null;
        int sigCharCount = 0;
        GeneralLegacyTextIndexEncoder.CharHandler[] codes = Codes.Value;
        short[] extMappings = ExtMappings.Value;

        foreach (char c in chars)
        {
            GeneralLegacyTextIndexEncoder.CharHandler ch = GetCharHandler(c, codes, extMappings);

            byte[]? inline = ch.GetInlineBytes(c);
            if (inline is not null)
            {
                bout.AddRange(inline);
            }

            if (ch.Type == GeneralLegacyTextIndexEncoder.CharHandlerType.Simple)
            {
                continue;
            }

            if (ch.Type == GeneralLegacyTextIndexEncoder.CharHandlerType.Significant)
            {
                sigCharCount++;
                continue;
            }

            byte[]? extra = ch.ExtraBytes;
            if (extra is not null && extra.Length > 0)
            {
                if (extraCodes is null)
                {
                    extraCodes = new NibbleStream(chars.Length);
                    extraCodes.WriteNibble(ExtCodesBoundsNibble);
                }

                if (sigCharCount > 0)
                {
                    extraCodes.WriteFillNibbles(sigCharCount, InternationalExtraPlaceholder);
                    sigCharCount = 0;
                }

                // General 97 only consumes the first extra-byte (low nibble).
                extraCodes.WriteNibble(extra[0]);
            }
        }

        if (extraCodes is not null)
        {
            extraCodes.WriteNibble(ExtCodesBoundsNibble);
            extraCodes.WriteTo(bout);
        }
        else
        {
            bout.Add(GeneralLegacyTextIndexEncoder.EndExtraText);
        }

        if (!ascending)
        {
            for (int j = payloadStart; j < bout.Count; j++)
            {
                bout[j] = unchecked((byte)~bout[j]);
            }
        }

        return [.. bout];
    }

    private static GeneralLegacyTextIndexEncoder.CharHandler GetCharHandler(
        char c,
        GeneralLegacyTextIndexEncoder.CharHandler[] codes,
        short[] extMappings)
    {
        if (c <= LastChar)
        {
            return codes[c];
        }

        if (c < FirstMapChar || c > LastMapChar)
        {
            return GeneralLegacyTextIndexEncoder.IgnoredHandlerInstance;
        }

        // Some extended chars are equivalent to single-byte chars; the rest
        // map to 0 (which itself is an "ignored" char in the BMP table).
        int extOffset = c - FirstMapChar;
        return codes[extMappings[extOffset]];
    }

    private static short[] LoadMappings(string resourceName, char firstChar, char lastChar)
    {
        int numMappings = (lastChar - firstChar) + 1;
        var values = new short[numMappings];

        Assembly asm = typeof(General97TextIndexEncoder).Assembly;
        using Stream? raw = asm.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{resourceName}' not found.");
        using var gz = new GZipStream(raw, CompressionMode.Decompress);
        using var reader = new StreamReader(gz, Encoding.ASCII);

        // Sparse file with <fromCode>,<toCode> entries; missing rows stay 0
        // (which the BMP "ignored" handler at index 0 absorbs).
        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            line = line.Trim();
            if (line.Length == 0)
            {
                continue;
            }

            int comma = line.IndexOf(',', StringComparison.Ordinal);
            int fromCode = int.Parse(line.AsSpan(0, comma), NumberStyles.Integer, CultureInfo.InvariantCulture);
            int toCode = int.Parse(line.AsSpan(comma + 1), NumberStyles.Integer, CultureInfo.InvariantCulture);
            values[fromCode - firstChar] = (short)toCode;
        }

        return values;
    }

    /// <summary>
    /// Bit-stream that accepts 4-bit nibbles, packing two per byte with the
    /// first nibble in the high four bits. Mirrors Jackcess's
    /// <c>NibbleStream</c> helper used by <c>General97IndexCodes</c>.
    /// </summary>
    private sealed class NibbleStream(int initialCapacity)
    {
        private readonly List<byte> _bytes = new(Math.Max(1, (initialCapacity + 1) / 2));
        private int _nibbleLen;

        public void WriteNibble(int b)
        {
            if (_nibbleLen % 2 == 0)
            {
                _bytes.Add(unchecked((byte)((b << 4) & 0xF0)));
            }
            else
            {
                int idx = _bytes.Count - 1;
                _bytes[idx] = unchecked((byte)(_bytes[idx] | (b & 0x0F)));
            }

            _nibbleLen++;
        }

        public void WriteFillNibbles(int length, byte b)
        {
            // Faithful port of Jackcess writeFillNibbles — emits a single
            // mid-byte half-nibble if the stream is currently mid-byte, then
            // bulk-emits double-nibble bytes, then a trailing half-nibble.
            int newNibbleLen = _nibbleLen + length;
            int remaining = length;

            if (_nibbleLen % 2 != 0)
            {
                int idx = _bytes.Count - 1;
                _bytes[idx] = unchecked((byte)(_bytes[idx] | (b & 0x0F)));
                remaining--;
            }

            if (remaining > 1)
            {
                byte doubleB = unchecked((byte)(((b << 4) & 0xF0) | (b & 0x0F)));
                do
                {
                    _bytes.Add(doubleB);
                    remaining -= 2;
                }
                while (remaining > 1);
            }

            if (remaining == 1)
            {
                _bytes.Add(unchecked((byte)((b << 4) & 0xF0)));
            }

            _nibbleLen = newNibbleLen;
        }

        public void WriteTo(List<byte> sink) => sink.AddRange(_bytes);
    }
}
