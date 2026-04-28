namespace JetDatabaseWriter.Internal;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;

/// <summary>
/// "General Legacy" (Access 2000–2007 default; Access 2010+ legacy fallback)
/// text-index sort-key encoder. Port of
/// <c>com.healthmarketscience.jackcess.impl.GeneralLegacyIndexCodes</c>
/// (Apache 2.0 — see <c>THIRD-PARTY-NOTICES.md</c>).
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
/// fixture; see <c>docs/design/index-and-relationship-format-notes.md</c>
/// §8 for the standing Microsoft Access compact-and-repair gap.
/// </para>
/// </summary>
internal static class GeneralLegacyTextIndexEncoder
{
    // Per JetFormat.TEXT_FIELD_MAX_LENGTH (255 bytes) / TEXT_FIELD_UNIT_SIZE (2 bytes/char in Jet4/ACE).
    internal const int MaxTextIndexCharLength = 255 / 2;

    internal const byte EndText = 0x01;
    internal const byte EndExtraText = 0x00;

    internal const byte FlagAscendingNonNull = 0x7F;
    internal const byte FlagDescendingNonNull = 0x80;
    internal const byte FlagAscendingNull = 0x00;
    internal const byte FlagDescendingNull = 0xFF;

    private const byte InternationalExtraPlaceholder = 0x02;
    private const int UnprintableCountStart = 7;
    private const int UnprintableCountMultiplier = 4;
    private const int UnprintableOffsetFlags = 0x8000;
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

    private static readonly byte[] CrazyCodesSuffix = [0xFF, 0x02, 0x80, 0xFF, 0x80];

    private static readonly byte[] SurrogateExtraBytes = [0x3F];

    private static readonly Lazy<CharHandler[]> Codes = new(
        () => LoadCodes(GenLegResource, FirstChar, LastChar));

    private static readonly Lazy<CharHandler[]> ExtCodes = new(
        () => LoadCodes(GenLegExtResource, FirstExtChar, LastExtChar));

    /// <summary>
    /// Encodes a single text value into the complete per-column entry block
    /// (flag byte + payload + END_EXTRA_TEXT). For null inputs returns a
    /// single-byte block with the null flag.
    /// </summary>
    public static byte[] Encode(string? text, bool ascending)
    {
        if (text is null)
        {
            return [ascending ? FlagAscendingNull : FlagDescendingNull];
        }

        // Truncate to the indexed-prefix length and strip trailing spaces
        // (Jackcess GeneralLegacyIndexCodes.toIndexCharSequence).
        if (text.Length > MaxTextIndexCharLength)
        {
            text = text.Substring(0, MaxTextIndexCharLength);
        }

        int len = text.Length;
        while (len > 0 && text[len - 1] == ' ')
        {
            len--;
        }

        if (len != text.Length)
        {
            text = text.Substring(0, len);
        }

        var bout = new List<byte>(text.Length + 4);
        bout.Add(ascending ? FlagAscendingNonNull : FlagDescendingNonNull);

        // Position immediately AFTER the start flag — the start flag is never
        // bit-flipped by the descending pass.
        int payloadStart = bout.Count;

        ExtraCodesStream? extraCodes = null;
        List<byte>? unprintableCodes = null;
        List<byte>? crazyCodes = null;
        int charOffset = 0;

        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            CharHandler ch = GetCharHandler(c);

            int curCharOffset = charOffset;
            byte[]? bytes = ch.GetInlineBytes(c);
            if (bytes is not null)
            {
                bout.AddRange(bytes);
                charOffset++;
            }

            if (ch.Type == CharHandlerType.Simple)
            {
                continue;
            }

            bytes = ch.ExtraBytes;
            byte extraCodeModifier = ch.ExtraByteModifier;
            if (bytes is not null || extraCodeModifier != 0)
            {
                extraCodes ??= new ExtraCodesStream(text.Length);
                WriteExtraCodes(curCharOffset, bytes, extraCodeModifier, extraCodes);
            }

            bytes = ch.UnprintableBytes;
            if (bytes is not null)
            {
                unprintableCodes ??= [];
                WriteUnprintableCodes(curCharOffset, bytes, unprintableCodes, extraCodes);
            }

            byte crazyFlag = ch.CrazyFlag;
            if (crazyFlag != 0)
            {
                crazyCodes ??= [];
                crazyCodes.Add(crazyFlag);
            }
        }

        bout.Add(EndText);

        bool hasExtraCodes = TrimExtraCodes(extraCodes, 0x00, InternationalExtraPlaceholder);
        bool hasUnprintableCodes = unprintableCodes is { Count: > 0 };
        bool hasCrazyCodes = crazyCodes is { Count: > 0 };

        if (hasExtraCodes || hasUnprintableCodes || hasCrazyCodes)
        {
            if (hasExtraCodes)
            {
                bout.AddRange(extraCodes!.Bytes);
            }

            if (hasCrazyCodes || hasUnprintableCodes)
            {
                bout.Add(EndText);
                bout.Add(EndText);

                if (hasCrazyCodes)
                {
                    WriteCrazyCodes(crazyCodes!, bout);
                    if (hasUnprintableCodes)
                    {
                        bout.Add(CrazyCodesUnprintSuffix);
                    }
                }

                if (hasUnprintableCodes)
                {
                    bout.Add(EndText);
                    bout.AddRange(unprintableCodes!);
                }
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
        return [.. bout];
    }

    private static CharHandler GetCharHandler(char c)
    {
        if (c <= LastChar)
        {
            return Codes.Value[c];
        }

        return ExtCodes.Value[c - FirstExtChar];
    }

    private static void WriteExtraCodes(
        int charOffset,
        byte[]? bytes,
        byte extraCodeModifier,
        ExtraCodesStream extraCodes)
    {
        int numChars = extraCodes.NumChars;
        if (numChars < charOffset)
        {
            int fillChars = charOffset - numChars;
            for (int i = 0; i < fillChars; i++)
            {
                extraCodes.Bytes.Add(InternationalExtraPlaceholder);
            }

            extraCodes.NumChars += fillChars;
        }

        if (bytes is not null)
        {
            extraCodes.Bytes.AddRange(bytes);
            extraCodes.NumChars++;
        }
        else
        {
            int lastIdx = extraCodes.Bytes.Count - 1;
            if (lastIdx >= 0)
            {
                byte lastByte = extraCodes.Bytes[lastIdx];
                lastByte = unchecked((byte)(lastByte + extraCodeModifier));
                extraCodes.Bytes[lastIdx] = lastByte;
            }
            else
            {
                extraCodes.Bytes.Add(extraCodeModifier);
                extraCodes.UnprintablePrefixLen = 1;
            }
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
        byte[] bytes,
        List<byte> unprintableCodes,
        ExtraCodesStream? extraCodes)
    {
        int unprintCharOffset = charOffset;
        if (extraCodes is not null)
        {
            unprintCharOffset = extraCodes.Bytes.Count
                + (charOffset - extraCodes.NumChars)
                - extraCodes.UnprintablePrefixLen;
        }

        int offset = (UnprintableCountStart + (UnprintableCountMultiplier * unprintCharOffset))
            | UnprintableOffsetFlags;

        unprintableCodes.Add((byte)((offset >> 8) & 0xFF));
        unprintableCodes.Add((byte)(offset & 0xFF));
        unprintableCodes.Add(UnprintableMidfix);
        unprintableCodes.AddRange(bytes);
    }

    private static void WriteCrazyCodes(List<byte> crazyCodes, List<byte> bout)
    {
        TrimTrailing(crazyCodes, CrazyCode2, CrazyCode2);

        if (crazyCodes.Count > 0)
        {
            byte curByte = CrazyCodeStart;
            int idx = 0;
            for (int i = 0; i < crazyCodes.Count; i++)
            {
                byte nextByte = crazyCodes[i];
                nextByte = unchecked((byte)(nextByte << ((2 - idx) * 2)));
                curByte |= nextByte;

                idx++;
                if (idx == 3)
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

        bout.AddRange(CrazyCodesSuffix);
    }

    private static CharHandler[] LoadCodes(string resourceName, char firstChar, char lastChar)
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
            CharHandler ch;
            if (char.IsHighSurrogate(c))
            {
                ch = HighSurrogateHandler.Instance;
            }
            else if (char.IsLowSurrogate(c))
            {
                ch = LowSurrogateHandler.Instance;
            }
            else
            {
                string? line = reader.ReadLine()
                    ?? throw new InvalidDataException($"Premature end of resource '{resourceName}' at index {i}.");
                ch = ParseCodes(line);
            }

            values[i] = ch;
        }

        return values;
    }

    private static CharHandler ParseCodes(string codeLine)
    {
        char prefix = codeLine[0];
        string suffix = codeLine.Length > 1 ? codeLine.Substring(1) : string.Empty;
        string[] parts = suffix.Split(',');

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

        public virtual byte[]? GetInlineBytes(char c) => null;

        public virtual byte[]? ExtraBytes => null;

        public virtual byte[]? UnprintableBytes => null;

        public virtual byte ExtraByteModifier => 0;

        public virtual byte CrazyFlag => 0;
    }

    private sealed class SimpleCharHandler(byte[] bytes) : CharHandler
    {
        private readonly byte[] bytes = bytes;

        public override CharHandlerType Type => CharHandlerType.Simple;

        public override byte[] GetInlineBytes(char c) => this.bytes;
    }

    private sealed class InternationalCharHandler(byte[] bytes, byte[] extraBytes) : CharHandler
    {
        private readonly byte[] bytes = bytes;
        private readonly byte[] extraBytes = extraBytes;

        public override CharHandlerType Type => CharHandlerType.International;

        public override byte[] GetInlineBytes(char c) => this.bytes;

        public override byte[] ExtraBytes => this.extraBytes;
    }

    private sealed class UnprintableCharHandler(byte[] unprintBytes) : CharHandler
    {
        private readonly byte[] unprintBytes = unprintBytes;

        public override CharHandlerType Type => CharHandlerType.Unprintable;

        public override byte[] UnprintableBytes => this.unprintBytes;
    }

    private sealed class UnprintableExtCharHandler(byte extraByteMod) : CharHandler
    {
        private readonly byte extraByteMod = extraByteMod;

        public override CharHandlerType Type => CharHandlerType.UnprintableExt;

        public override byte ExtraByteModifier => this.extraByteMod;
    }

    private sealed class InternationalExtCharHandler(byte[] bytes, byte[]? extraBytes, byte crazyFlag) : CharHandler
    {
        private readonly byte[] bytes = bytes;
        private readonly byte[]? extraBytes = extraBytes;
        private readonly byte crazyFlag = crazyFlag;

        public override CharHandlerType Type => CharHandlerType.InternationalExt;

        public override byte[] GetInlineBytes(char c) => this.bytes;

        public override byte[]? ExtraBytes => this.extraBytes;

        public override byte CrazyFlag => this.crazyFlag;
    }

    private sealed class SignificantCharHandler(byte[] bytes) : CharHandler
    {
        private readonly byte[] bytes = bytes;

        public override CharHandlerType Type => CharHandlerType.Significant;

        public override byte[] GetInlineBytes(char c) => this.bytes;
    }

    private sealed class IgnoredHandler : CharHandler
    {
        public static readonly IgnoredHandler Instance = new();

        public override CharHandlerType Type => CharHandlerType.Ignored;
    }

    private sealed class HighSurrogateHandler : CharHandler
    {
        public static readonly HighSurrogateHandler Instance = new();

        public override CharHandlerType Type => CharHandlerType.Surrogate;

        public override byte[] GetInlineBytes(char c)
        {
            int idxC = c - 10238;
            return [(byte)((idxC >> 8) & 0xFF), (byte)(idxC & 0xFF)];
        }

        public override byte[] ExtraBytes => SurrogateExtraBytes;
    }

    private sealed class LowSurrogateHandler : CharHandler
    {
        public static readonly LowSurrogateHandler Instance = new();

        public override CharHandlerType Type => CharHandlerType.Surrogate;

        public override byte[] GetInlineBytes(char c)
        {
            int charOffset = (c - 0xDC00) % 1024;
            int idxOffset;
            if (charOffset < 8)
            {
                idxOffset = 9992;
            }
            else if (charOffset < 8 + 254)
            {
                idxOffset = 9990;
            }
            else if (charOffset < 8 + 254 + 254)
            {
                idxOffset = 9988;
            }
            else if (charOffset < 8 + 254 + 254 + 254)
            {
                idxOffset = 9986;
            }
            else
            {
                idxOffset = 9984;
            }

            int idxC = c - idxOffset;
            return [(byte)((idxC >> 8) & 0xFF), (byte)(idxC & 0xFF)];
        }

        public override byte[] ExtraBytes => SurrogateExtraBytes;
    }

    private sealed class ExtraCodesStream(int initialCapacity)
    {
        public List<byte> Bytes { get; } = new List<byte>(initialCapacity);

        public int NumChars { get; set; }

        public int UnprintablePrefixLen { get; set; }
    }
}
