namespace JetDatabaseWriter.Indexes.Collation;

using System;
#if NET8_0_OR_GREATER
using System.Buffers;
#endif
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;
using static JetDatabaseWriter.Constants.IndexEntryFlags;

/// <summary>
/// "General Legacy" (Access 2000–2007 default; Access 2010+ legacy fallback)
/// text-index sort-key encoder. Port of
/// <c>com.healthmarketscience.jackcess.impl.GeneralLegacyIndexCodes</c>
/// (Apache 2.0 — see <see href="THIRD-PARTY-NOTICES.md" />).
/// <para>
/// Returns the complete per-column entry block: a leading flag byte
/// (<c>0x7F</c> ascending non-null / <c>0x80</c> descending non-null /
/// <c>0x00</c> ascending null / <c>0xFF</c> descending null) followed by
/// the encoded key bytes and the END_EXTRA_TEXT terminator. For descending
/// keys the inline + extra/unprintable/crazy payload is one's-complemented
/// in place per the Jackcess <c>writeNonNullIndexTextValue</c> rule; the
/// flag byte and the trailing END_EXTRA_TEXT are NOT flipped (the leading
/// 0x7F vs 0x80 is the "is-descending" signal itself).
/// </para>
/// <para>
/// <b>Validation status:</b> the per-codepoint code tables come verbatim
/// from Jackcess and the state machine is a faithful port. Byte sequences
/// have not been independently re-validated against an Access-authored
/// fixture; see <see href="docs/design/index-and-relationship-format-notes.md" />
/// §8 for the standing Microsoft Access compact-and-repair gap.
/// </para>
/// </summary>
internal static class GeneralLegacyTextIndexEncoder
{
    internal const byte EndText = 0x01;
    internal const byte EndExtraText = 0x00;

    private const byte InternationalExtraPlaceholder = 0x02;
    private const int UnprintableCountStart = 7;
    private const int UnprintableCountMultiplier = 4;
    private const int UnprintableOffsetFlags = 0x8000;

    // V2010 (ACE / 4 KiB pages) hard cap on the encoded leaf-entry length.
    // Empirically every Access-authored Table11 / Table11_desc long-row
    // entry in testIndexCodesV2010.accdb is exactly 510 bytes; the encoder
    // truncates the unflipped form mid-stream at this byte boundary,
    // dropping END_TEXT, extras, END_EXTRA_TEXT, and the unflipped
    // descending sentinel as needed. The exact derivation of 510 is
    // unconfirmed; likely page_size/8 - 2 for 4 KiB pages.
    // See docs/format-probe/format-probe-long-row-index-encoding.md.
    internal const int MaxEntryLengthGeneralV2010 = 510;
    private const byte UnprintableMidfix = 0x06;
    private const byte CrazyCodeStart = 0x80;
    private const byte CrazyCode1 = 0x02;
    private const byte CrazyCode2 = 0x03;
    private const byte CrazyCodesUnprintSuffix = 0xFF;

    private const char FirstChar = (char)0x0000;
    private const char LastChar = (char)0x00FF;
    private const char FirstExtChar = (char)0x0100;
    private const char LastExtChar = (char)0xFFFF;

    private const string GenLegResource = "JetDatabaseWriter.IndexCodeTables.index_codes_genleg.txt.gz";
    private const string GenLegExtResource = "JetDatabaseWriter.IndexCodeTables.index_codes_ext_genleg.txt.gz";

    private static ReadOnlySpan<byte> CrazyCodesSuffix => [0xFF, 0x02, 0x80, 0xFF, 0x80];

#if NET8_0_OR_GREATER
    private static readonly SearchValues<char> LineBreakChars = SearchValues.Create("\r\n");
#else
    private static readonly char[] LineBreakChars = ['\r', '\n'];
#endif

    internal static ReadOnlySpan<byte> SurrogateExtraBytes => [0x3F];

    // 2-chunk "long-row" separators reverse-engineered from Access-authored
    // testIndexCodes V2000/V2003/V2007/V2010 fixtures (Table11 / Table11_desc).
    // <see href="docs/format-probe/format-probe-long-row-index-encoding.md" /> for details.
    internal static ReadOnlySpan<byte> LongRowSeparatorGeneralLegacy => [0x08, 0x07, 0x08, 0x04];

    internal static ReadOnlySpan<byte> LongRowSeparatorGeneral => [0x07, 0x09, 0x07, 0x06];

    internal delegate ushort? LongRowSuffixProvider(string text, bool ascending, byte[] fullEntry);

    private static readonly Lazy<CharHandler[]> Codes =
        new(() => LoadCodes(GenLegResource, FirstChar, LastChar));

    private static readonly Lazy<CharHandler[]> ExtCodes =
        new(() => LoadCodes(GenLegExtResource, FirstExtChar, LastExtChar));

    /// <summary>
    /// Encodes a single text value into the complete per-column entry block
    /// (flag byte + payload + END_EXTRA_TEXT). For null inputs returns a
    /// single-byte block with the null flag.
    /// </summary>
    public static byte[] Encode(string? text, bool ascending)
        => EncodeWithTables(text, ascending, Codes.Value, ExtCodes.Value);

    /// <summary>
    /// Shared "General Legacy"-shape state machine that both
    /// <see cref="GeneralLegacyTextIndexEncoder"/> and
    /// <see cref="GeneralTextIndexEncoder"/> drive with their own per-codepoint
    /// tables. Access 2010+ "General" sort order is structurally identical to
    /// "General Legacy" — only the per-codepoint code tables differ
    /// (Jackcess `GeneralIndexCodes` extends `GeneralLegacyIndexCodes` and
    /// only overrides `getCharHandler`). Access 1997 "General 97" uses a
    /// different state machine (no END_TEXT framing, nibble-packed extras)
    /// and has its own dedicated encoder.
    /// </summary>
    /// <param name="text">Input text (null encodes as a single null-flag byte).</param>
    /// <param name="ascending">True for ascending sort, false for descending.</param>
    /// <param name="codes">Per-BMP-codepoint handler table (0x0000–0x00FF).</param>
    /// <param name="extCodes">Extended handler table (0x0100–0xFFFF).</param>
    /// <param name="longRowSeparator">
    /// 4-byte separator emitted between chunks when the input is split across
    /// two chunks (only used when <paramref name="text"/> exceeds
    /// <see cref="Constants.IndexTextEncoding.MaxTextIndexCharLength"/> and contains an embedded line-break).
    /// </param>
    /// <param name="maxEntryLength">
    /// Optional hard cap on the encoded entry length (0 = no cap). When set,
    /// the encoder truncates the produced bytes at this boundary; used by the
    /// V2010 long-row path with <see cref="MaxEntryLengthGeneralV2010"/>.
    /// </param>
    /// <param name="longRowSuffixProvider">
    /// Optional callback that can replace the V2010 long-row suffix bytes
    /// immediately before the hard cap truncation is applied.
    /// </param>
    internal static byte[] EncodeWithTables(
        string? text,
        bool ascending,
        CharHandler[] codes,
        CharHandler[] extCodes,
        ReadOnlySpan<byte> longRowSeparator = default,
        int maxEntryLength = 0,
        LongRowSuffixProvider? longRowSuffixProvider = null)
    {
        if (text is null)
        {
            return [ascending ? AscendingNull : DescendingNull];
        }

        if (maxEntryLength > 0)
        {
            // V2010 / ACE: continuous encoding of up to 255 characters with
            // no chunk split. ApplyMaxEntryLength handles the byte cap.
            ReadOnlySpan<char> v2010Chars = text.AsSpan(0, Math.Min(text.Length, Constants.IndexTextEncoding.MaxTextIndexByteLength));
            return EncodeSingleChunk(
                text,
                v2010Chars,
                ascending,
                codes,
                extCodes,
                maxEntryLength,
                longRowSuffixProvider);
        }

        if (text.Length > Constants.IndexTextEncoding.MaxTextIndexCharLength)
        {
            int splitAt = FindFirstLineBreak(text);
            if (splitAt >= 0)
            {
                int resumeAt = splitAt + 1;
                if (resumeAt < text.Length
                    && ((text[splitAt] == '\r' && text[resumeAt] == '\n')
                        || (text[splitAt] == '\n' && text[resumeAt] == '\r')))
                {
                    resumeAt++;
                }

                return EncodeTwoChunks(
                    text,
                    splitAt,
                    resumeAt,
                    ascending,
                    codes,
                    extCodes,
                    longRowSeparator.IsEmpty ? LongRowSeparatorGeneralLegacy : longRowSeparator);
            }
        }

        ReadOnlySpan<char> chars = text.AsSpan(0, Math.Min(text.Length, Constants.IndexTextEncoding.MaxTextIndexCharLength)).TrimEnd(' ');

        return EncodeSingleChunk(text, chars, ascending, codes, extCodes, 0, null);
    }

    private static byte[] EncodeSingleChunk(
        string text,
        ReadOnlySpan<char> chars,
        bool ascending,
        CharHandler[] codes,
        CharHandler[] extCodes,
        int maxEntryLength,
        LongRowSuffixProvider? longRowSuffixProvider)
    {
        var bout = CreateEntryBuffer(chars.Length, ascending);
        int payloadStart = bout.Count;

        var state = new ChunkEmitState(chars.Length);
        EmitChunkInline(chars, codes, extCodes, bout, state);
        FinishEntry(bout, payloadStart, state, ascending);
        ApplyMaxEntryLength(bout, text, ascending, maxEntryLength, longRowSuffixProvider);

        return [.. bout];
    }

    private static byte[] EncodeTwoChunks(
        string text,
        int splitAt,
        int resumeAt,
        bool ascending,
        CharHandler[] codes,
        CharHandler[] extCodes,
        ReadOnlySpan<byte> separator)
    {
        var chunk1 = text.AsSpan(0, splitAt);

        int chunk2Cap = Math.Min(text.Length, Constants.IndexTextEncoding.MaxTextIndexByteLength);
        int chunk2Take = chunk2Cap - resumeAt;

        var chunk2 = chunk2Take > 0
            ? text.AsSpan(resumeAt, chunk2Take).TrimEnd(' ')
            : ReadOnlySpan<char>.Empty;

        var bout = CreateEntryBuffer(chunk1.Length + chunk2.Length + separator.Length, ascending);
        int payloadStart = bout.Count;

        var state = new ChunkEmitState(chunk1.Length + chunk2.Length);
        EmitChunkInline(chunk1, codes, extCodes, bout, state);
        AppendBytes(bout, separator);
        EmitChunkInline(chunk2, codes, extCodes, bout, state);

        FinishEntry(bout, payloadStart, state, ascending);
        return [.. bout];
    }

    private static List<byte> CreateEntryBuffer(int payloadCapacity, bool ascending)
        => new(payloadCapacity + 4)
        {
            ascending ? AscendingNonNull : DescendingNonNull,
        };

    private static void AppendBytes(List<byte> sink, ReadOnlySpan<byte> bytes)
    {
        foreach (byte value in bytes)
        {
            sink.Add(value);
        }
    }

#if NET8_0_OR_GREATER
    private static int FindFirstLineBreak(string text) => text.AsSpan().IndexOfAny(LineBreakChars);
#else
    private static int FindFirstLineBreak(string text) => text.IndexOfAny(LineBreakChars);
#endif

    /// <summary>
    /// Runs the per-codepoint state machine over <paramref name="chars"/>
    /// and appends inline bytes to <paramref name="bout"/>, while
    /// accumulating extras / unprintable / crazy state in
    /// <paramref name="state"/>.
    /// </summary>
    private static void EmitChunkInline(
        ReadOnlySpan<char> chars,
        CharHandler[] codes,
        CharHandler[] extCodes,
        List<byte> bout,
        ChunkEmitState state)
    {
        foreach (char c in chars)
        {
            CharHandler ch = c <= LastChar ? codes[c] : extCodes[c - FirstExtChar];
            int curCharOffset = state.CharOffset;

            ReadOnlySpan<byte> inline = ch.GetInlineBytes(c);
            if (!inline.IsEmpty)
            {
                AppendBytes(bout, inline);
                state.CharOffset++;
            }

            if (!ch.HasTrailingBytes)
            {
                continue;
            }

            ReadOnlySpan<byte> extra = ch.ExtraBytes;
            byte extraCodeModifier = ch.ExtraByteModifier;
            if (!extra.IsEmpty || extraCodeModifier != 0)
            {
                WriteExtraCodes(curCharOffset, extra, extraCodeModifier, state.GetOrCreateExtraCodes());
            }

            ReadOnlySpan<byte> unprint = ch.UnprintableBytes;
            if (!unprint.IsEmpty)
            {
                state.UnprintableCodes ??= [];
                WriteUnprintableCodes(curCharOffset, unprint, state.UnprintableCodes, state.ExtraCodes);
            }

            if (ch.CrazyFlag != 0)
            {
                state.CrazyCodes ??= [];
                state.CrazyCodes.Add(ch.CrazyFlag);
            }
        }
    }

    /// <summary>
    /// Writes the trailing END_TEXT, extras stream, optional unprintable +
    /// crazy block, and END_EXTRA_TEXT terminator (with the descending
    /// one's-complement pass) to <paramref name="bout"/>. Shared between the
    /// single-chunk and two-chunk paths.
    /// </summary>
    private static void FinishEntry(
        List<byte> bout,
        int payloadStart,
        ChunkEmitState state,
        bool ascending)
    {
        bout.Add(EndText);

        bool hasExtraCodes = TrimExtraCodes(state.ExtraCodes, 0x00, InternationalExtraPlaceholder);
        bool hasUnprintableCodes = state.UnprintableCodes is { Count: > 0 };
        bool hasCrazyCodes = state.CrazyCodes is { Count: > 0 };

        if (hasExtraCodes)
        {
            bout.AddRange(state.ExtraCodes!.Bytes);
        }

        if (hasCrazyCodes || hasUnprintableCodes)
        {
            bout.Add(EndText);
            bout.Add(EndText);

            if (hasCrazyCodes)
            {
                WriteCrazyCodes(state.CrazyCodes!, bout);
                if (hasUnprintableCodes)
                {
                    bout.Add(CrazyCodesUnprintSuffix);
                }
            }

            if (hasUnprintableCodes)
            {
                bout.Add(EndText);
                bout.AddRange(state.UnprintableCodes!);
            }
        }

        if (!ascending)
        {
            // Per Jackcess: write END_EXTRA_TEXT (0x00) BEFORE flipping, then
            // flip the entire payload (which converts the just-written 0x00
            // to 0xFF), then write another unflipped END_EXTRA_TEXT.
            bout.Add(EndExtraText);
            for (int j = payloadStart; j < bout.Count; j++)
            {
                bout[j] = unchecked((byte)~bout[j]);
            }
        }

        bout.Add(EndExtraText);
    }

    private static void ApplyMaxEntryLength(
        List<byte> bout,
        string text,
        bool ascending,
        int maxEntryLength,
        LongRowSuffixProvider? longRowSuffixProvider)
    {
        if (maxEntryLength <= 0 || bout.Count <= maxEntryLength)
        {
            return;
        }

        if (longRowSuffixProvider is not null
            && maxEntryLength == MaxEntryLengthGeneralV2010
            && maxEntryLength >= 510
            && longRowSuffixProvider(text, ascending, [.. bout]) is ushort suffix)
        {
            bout[508] = (byte)(suffix >> 8);
            bout[509] = unchecked((byte)suffix);
        }

        bout.RemoveRange(maxEntryLength, bout.Count - maxEntryLength);
    }

    private sealed class ChunkEmitState(int initialCapacity)
    {
        public ExtraCodesStream? ExtraCodes { get; set; }

        public List<byte>? UnprintableCodes { get; set; }

        public List<byte>? CrazyCodes { get; set; }

        public int CharOffset { get; set; }

        public ExtraCodesStream GetOrCreateExtraCodes()
        {
            ExtraCodes ??= new ExtraCodesStream(initialCapacity);
            return ExtraCodes;
        }
    }

    private static void WriteExtraCodes(
        int charOffset,
        ReadOnlySpan<byte> bytes,
        byte extraCodeModifier,
        ExtraCodesStream extraCodes)
    {
        int fillChars = charOffset - extraCodes.NumChars;
        if (fillChars > 0)
        {
            for (int i = 0; i < fillChars; i++)
            {
                extraCodes.Bytes.Add(InternationalExtraPlaceholder);
            }

            extraCodes.NumChars = charOffset;
        }

        if (!bytes.IsEmpty)
        {
            AppendBytes(extraCodes.Bytes, bytes);
            extraCodes.NumChars++;
        }
        else if (extraCodes.Bytes.Count > 0)
        {
            int lastIdx = extraCodes.Bytes.Count - 1;
            extraCodes.Bytes[lastIdx] = unchecked((byte)(extraCodes.Bytes[lastIdx] + extraCodeModifier));
        }
        else
        {
            extraCodes.Bytes.Add(extraCodeModifier);
            extraCodes.UnprintablePrefixLen = 1;
        }
    }

    private static bool TrimExtraCodes(ExtraCodesStream? extraCodes, byte minTrimCode, byte maxTrimCode)
    {
        if (extraCodes is null)
        {
            return false;
        }

        TrimTrailing(extraCodes.Bytes, minTrimCode, maxTrimCode);
        return extraCodes.Bytes.Count > 0;
    }

    private static void TrimTrailing(List<byte> bytes, byte minTrimCode, byte maxTrimCode)
    {
        int idx = bytes.Count - 1;
        while (idx >= 0)
        {
            byte b = bytes[idx];
            if (b < minTrimCode || b > maxTrimCode)
            {
                break;
            }

            idx--;
        }

        int newLen = idx + 1;
        if (newLen < bytes.Count)
        {
            bytes.RemoveRange(newLen, bytes.Count - newLen);
        }
    }

    private static void WriteUnprintableCodes(
        int charOffset,
        ReadOnlySpan<byte> bytes,
        List<byte> unprintableCodes,
        ExtraCodesStream? extraCodes)
    {
        int unprintCharOffset = extraCodes is null
            ? charOffset
            : extraCodes.Bytes.Count + (charOffset - extraCodes.NumChars) - extraCodes.UnprintablePrefixLen;

        int offset = (UnprintableCountStart + (UnprintableCountMultiplier * unprintCharOffset))
            | UnprintableOffsetFlags;

        unprintableCodes.Add(unchecked((byte)(offset >> 8)));
        unprintableCodes.Add(unchecked((byte)offset));
        unprintableCodes.Add(UnprintableMidfix);
        AppendBytes(unprintableCodes, bytes);
    }

    private static void WriteCrazyCodes(List<byte> crazyCodes, List<byte> bout)
    {
        TrimTrailing(crazyCodes, CrazyCode2, CrazyCode2);

        if (crazyCodes.Count > 0)
        {
            byte curByte = CrazyCodeStart;
            int idx = 0;
            foreach (byte code in crazyCodes)
            {
                curByte |= unchecked((byte)(code << ((2 - idx) * 2)));
                if (++idx == 3)
                {
                    bout.Add(curByte);
                    curByte = CrazyCodeStart;
                    idx = 0;
                }
            }

            if (idx > 0)
            {
                bout.Add(curByte);
            }
        }

        AppendBytes(bout, CrazyCodesSuffix);
    }

    internal static CharHandler[] LoadCodes(string resourceName, char firstChar, char lastChar)
    {
        int numCodes = lastChar - firstChar + 1;
        var values = new CharHandler[numCodes];

        Assembly asm = typeof(GeneralLegacyTextIndexEncoder).Assembly;
        using Stream? raw = asm.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{resourceName}' not found.");
        using var gz = new GZipStream(raw, CompressionMode.Decompress);
        using var reader = new StreamReader(gz, Encoding.ASCII);

        for (int i = 0; i < numCodes; i++)
        {
            char c = (char)(firstChar + i);
            if (char.IsHighSurrogate(c))
            {
                values[i] = HighSurrogateHandler.Instance;
            }
            else if (char.IsLowSurrogate(c))
            {
                values[i] = LowSurrogateHandler.Instance;
            }
            else
            {
                values[i] = ParseCodes(reader.ReadLine()
                    ?? throw new InvalidDataException($"Premature end of resource '{resourceName}' at index {i}."));
            }
        }

        return values;
    }

    private static CharHandler ParseCodes(string codeLine)
    {
        char prefix = codeLine[0];
        string[] parts = (codeLine.Length > 1 ? codeLine[1..] : string.Empty).Split(',');

        return prefix switch
        {
            'X' => IgnoredHandler.Instance,
            'S' => new SimpleCharHandler(CodesToBytes(parts[0], required: true)!),
            'I' => new InternationalCharHandler(
                CodesToBytes(parts[0], required: true)!,
                CodesToBytes(parts[1], required: true)!),
            'U' => new UnprintableCharHandler(CodesToBytes(parts[0], required: true)!),
            'P' => new UnprintableExtCharHandler(CodesToBytes(parts[0], required: true)![0]),
            'Z' => new InternationalExtCharHandler(
                CodesToBytes(parts[0], required: true)!,
                CodesToBytes(parts[1], required: false),
                parts[2] == "1" ? CrazyCode1 : CrazyCode2),
            'G' => new SignificantCharHandler(CodesToBytes(parts[0], required: true)!),
            _ => throw new InvalidDataException($"Unknown code prefix '{prefix}' in '{codeLine}'."),
        };
    }

    private static byte[]? CodesToBytes(string codes, bool required)
    {
        if (codes.Length == 0)
        {
            if (required)
            {
                throw new InvalidDataException("Empty code bytes.");
            }

            return null;
        }

        if (codes.Length % 2 != 0)
        {
            codes = "0" + codes;
        }

        byte[] bytes = new byte[codes.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = byte.Parse(codes.AsSpan(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return bytes;
    }

    internal enum CharHandlerType
    {
        Simple,
        International,
        Unprintable,
        UnprintableExt,
        InternationalExt,
        Significant,
        Surrogate,
        Ignored,
    }

    internal abstract class CharHandler
    {
        public abstract CharHandlerType Type { get; }

        public virtual bool HasTrailingBytes => false;

        public virtual ReadOnlySpan<byte> GetInlineBytes(char c) => [];

        public virtual ReadOnlySpan<byte> ExtraBytes => [];

        public virtual ReadOnlySpan<byte> UnprintableBytes => [];

        public virtual byte ExtraByteModifier => 0;

        public virtual byte CrazyFlag => 0;
    }

    private sealed class SimpleCharHandler(byte[] bytes) : CharHandler
    {
        public override CharHandlerType Type => CharHandlerType.Simple;

        public override ReadOnlySpan<byte> GetInlineBytes(char c) => bytes;
    }

    private sealed class InternationalCharHandler(byte[] bytes, byte[] extraBytes) : CharHandler
    {
        public override CharHandlerType Type => CharHandlerType.International;

        public override bool HasTrailingBytes => true;

        public override ReadOnlySpan<byte> GetInlineBytes(char c) => bytes;

        public override ReadOnlySpan<byte> ExtraBytes => extraBytes;
    }

    private sealed class UnprintableCharHandler(byte[] unprintBytes) : CharHandler
    {
        public override CharHandlerType Type => CharHandlerType.Unprintable;

        public override bool HasTrailingBytes => true;

        public override ReadOnlySpan<byte> UnprintableBytes => unprintBytes;
    }

    private sealed class UnprintableExtCharHandler(byte extraByteMod) : CharHandler
    {
        public override CharHandlerType Type => CharHandlerType.UnprintableExt;

        public override bool HasTrailingBytes => true;

        public override byte ExtraByteModifier => extraByteMod;
    }

    private sealed class InternationalExtCharHandler(byte[] bytes, byte[]? extraBytes, byte crazyFlag) : CharHandler
    {
        public override CharHandlerType Type => CharHandlerType.InternationalExt;

        public override bool HasTrailingBytes => true;

        public override ReadOnlySpan<byte> GetInlineBytes(char c) => bytes;

        public override ReadOnlySpan<byte> ExtraBytes => extraBytes is null ? [] : extraBytes;

        public override byte CrazyFlag => crazyFlag;
    }

    private sealed class SignificantCharHandler(byte[] bytes) : CharHandler
    {
        public override CharHandlerType Type => CharHandlerType.Significant;

        public override ReadOnlySpan<byte> GetInlineBytes(char c) => bytes;
    }

    private sealed class IgnoredHandler : CharHandler
    {
        public static readonly IgnoredHandler Instance = new();

        public override CharHandlerType Type => CharHandlerType.Ignored;
    }

    /// <summary>
    /// Gets the singleton "ignored char" handler exposed for sibling encoders
    /// (<see cref="General97TextIndexEncoder"/>) that need to short-circuit
    /// out-of-range BMP codepoints to the same no-op behaviour the parser's
    /// <c>'X'</c> prefix produces.
    /// </summary>
    internal static CharHandler IgnoredHandlerInstance => IgnoredHandler.Instance;

    private sealed class HighSurrogateHandler : CharHandler
    {
        public static readonly HighSurrogateHandler Instance = new();

        public override CharHandlerType Type => CharHandlerType.Surrogate;

        public override bool HasTrailingBytes => true;

        public override ReadOnlySpan<byte> GetInlineBytes(char c)
        {
            int idxC = c - 10238;
            return new byte[] { unchecked((byte)(idxC >> 8)), unchecked((byte)idxC) };
        }

        public override ReadOnlySpan<byte> ExtraBytes => SurrogateExtraBytes;
    }

    private sealed class LowSurrogateHandler : CharHandler
    {
        public static readonly LowSurrogateHandler Instance = new();

        public override CharHandlerType Type => CharHandlerType.Surrogate;

        public override bool HasTrailingBytes => true;

        public override ReadOnlySpan<byte> GetInlineBytes(char c)
        {
            int charOffset = (c - 0xDC00) % 1024;
            int idxOffset = charOffset switch
            {
                < 8 => 9992,
                < 8 + 254 => 9990,
                < 8 + (254 * 2) => 9988,
                < 8 + (254 * 3) => 9986,
                _ => 9984,
            };

            int idxC = c - idxOffset;
            return new byte[] { unchecked((byte)(idxC >> 8)), unchecked((byte)idxC) };
        }

        public override ReadOnlySpan<byte> ExtraBytes => SurrogateExtraBytes;
    }

    private sealed class ExtraCodesStream(int initialCapacity)
    {
        public List<byte> Bytes { get; } = new List<byte>(initialCapacity);

        public int NumChars { get; set; }

        public int UnprintablePrefixLen { get; set; }
    }
}
