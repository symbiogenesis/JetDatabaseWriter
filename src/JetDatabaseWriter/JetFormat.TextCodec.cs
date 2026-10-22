namespace JetDatabaseWriter;

using System;
using System.Runtime.InteropServices;
using System.Text;

internal sealed partial class JetFormat
{
    // ── UTF-16LE bytes \u2192 string helpers ─────────────────────────────────
    // The on-disk text encoding for Jet4/ACE is UCS-2 LE, which is the exact
    // in-memory layout of <c>char</c> on supported .NET runtimes
    // (all little-endian). Re-interpreting the byte span as a char span
    // therefore skips the validation pass and intermediate buffers that
    // <c>Encoding.Unicode.GetString</c> performs. On a hypothetical big-endian
    // host the JIT-constant <c>BitConverter.IsLittleEndian</c> check folds
    // the fast path away and we fall back to <c>Encoding.Unicode</c>.

    /// <summary>
    /// Decodes a UCS-2 LE byte slice into a <see cref="string"/>. The slice
    /// length must be even; the caller is responsible for trimming any odd
    /// trailing byte before calling. Allocates exactly one string.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    internal static string DecodeUtf16LE(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return string.Empty;
        }

        if (BitConverter.IsLittleEndian)
        {
            return new string(MemoryMarshal.Cast<byte, char>(bytes));
        }

        return Encoding.Unicode.GetString(bytes);
    }

    // ── JET4 compressed-unicode text codec ──────────────────
    // Static encode/decode for Jet4/ACE Text and Memo columns.

    /// <summary>
    /// Encodes a string for storage in a Jet4 text/memo column.
    /// When all characters are in the U+0001..U+00FF range, emits the
    /// compressed form (<c>0xFF 0xFE</c> marker + 1 byte per character),
    /// which the reader decodes via <c>DecompressJet4</c>.
    /// Otherwise emits plain UCS-2 LE.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="compress">The compress.</param>
    /// <remarks>
    /// The "no NUL" restriction (chars must be > U+0000) avoids ambiguity
    /// with the compressed-mode toggle byte (<c>0x00</c>). The compressed
    /// form is only chosen when it actually saves bytes (length &gt;= 3
    /// characters), so 1- and 2-character strings are still written as
    /// plain UCS-2 to avoid the 2-byte marker overhead.
    /// </remarks>
    internal static byte[] EncodeJet4Text(string value, bool compress = true) => EncodeJet4Text(value, int.MaxValue, compress);

    /// <summary>
    /// Encodes a string into Jet4 text format, truncating to at most
    /// <paramref name="maxBytes"/> output bytes. Avoids a secondary
    /// <c>Array.Resize</c> when the caller has a column-size limit.
    /// </summary>
    /// <param name="value">The string to encode.</param>
    /// <param name="maxBytes">Maximum output byte count.</param>
    /// <param name="compress">When <see langword="true"/> (the default) and
    /// all characters fit in Latin-1, emits the compressed form
    /// (<c>0xFF 0xFE</c> marker + 1 byte/char). When <see langword="false"/>
    /// always emits plain UCS-2 LE. Callers should pass <see langword="false"/>
    /// for columns whose <c>ExtraFlags</c> byte does not have the
    /// <see cref="Constants.CompressedUnicodeExtFlagMask"/> bit set.</param>
    internal static byte[] EncodeJet4Text(string value, int maxBytes, bool compress = true)
    {
        if (string.IsNullOrEmpty(value))
        {
            return [];
        }

        bool compressible = compress && value.Length >= 3;
        if (compressible)
        {
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                if (c is '\0' or > (char)0xFF)
                {
                    compressible = false;
                    break;
                }
            }
        }

        if (!compressible)
        {
            int charCount = Math.Min(value.Length, maxBytes / 2);
            byte[] result = new byte[charCount * 2];
            Encoding.Unicode.GetBytes(value.AsSpan(0, charCount), result);
            return result;
        }

        int compressedLen = Math.Min(value.Length + 2, maxBytes);
        int charsToEncode = compressedLen - 2;
        if (charsToEncode <= 0)
        {
            return [];
        }

        byte[] compressed = new byte[charsToEncode + 2];
        compressed[0] = 0xFF;
        compressed[1] = 0xFE;
        for (int i = 0; i < charsToEncode; i++)
        {
            compressed[i + 2] = (byte)value[i];
        }

        return compressed;
    }

    /// <summary>
    /// Decodes Jet4 text (UCS-2 / UTF-16LE).
    /// If data starts with the compressed-unicode marker 0xFF 0xFE, the
    /// JET4 compressed-string algorithm is applied first.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decoded string.</returns>
    internal static string DecodeJet4Text(byte[] bytes, int start, int len)
        => DecodeJet4TextCore(bytes, start, len, allowTerminalLowByte: false);

    /// <summary>Decodes a native calculated Text payload, including its verified terminal low-byte representation.</summary>
    /// <param name="bytes">The unwrapped payload bytes.</param>
    /// <param name="start">The payload start.</param>
    /// <param name="len">The declared payload length.</param>
    /// <returns>The native cached Text result without recalculating the expression.</returns>
    internal string DecodeCalculatedText(byte[] bytes, int start, int len)
        => this.IsJet3 ? this.DecodeText(bytes, start, len) : DecodeJet4TextCore(bytes, start, len, allowTerminalLowByte: true);

    private static string DecodeJet4TextCore(byte[] bytes, int start, int len, bool allowTerminalLowByte)
    {
        if (len < 2)
        {
            return string.Empty;
        }

        if (bytes[start] == 0xFF && bytes[start + 1] == 0xFE)
        {
            return DecompressJet4(bytes, start + 2, len - 2, allowTerminalLowByte);
        }

        // Plain UCS-2 LE — length must be even
        int evenLen = len & ~1;
        return evenLen > 0 ? DecodeUtf16LE(bytes.AsSpan(start, evenLen)) : string.Empty;
    }

    /// <summary>
    /// Decodes the JET4 "compressed unicode" encoding.
    /// A 0x00 byte toggles between 1-byte compressed (ASCII) and 2-byte
    /// uncompressed (UCS-2) mode.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <param name="allowTerminalLowByte">Whether independently verified calculated Text permits a terminal low byte.</param>
    /// <returns>The decompressed string.</returns>
    private static string DecompressJet4(byte[] bytes, int start, int len, bool allowTerminalLowByte)
    {
        // Fast path: if no 0x00 byte appears in the data, the entire string
        // is compressed Latin-1 with no mode switches. This is the overwhelming
        // majority of text values in real Jet4 databases.
        int end = start + len;
        bool allCompressed = true;
        for (int index = start; index < end; index++)
        {
            if (bytes[index] == 0x00)
            {
                allCompressed = false;
                break;
            }
        }

        if (allCompressed)
        {
            return CreateFromCompressed(bytes, start, len);
        }

        return DecompressJet4Slow(bytes, start, len, allowTerminalLowByte);
    }

    private static string CreateFromCompressed(byte[] bytes, int start, int len) =>
#if NET6_0_OR_GREATER
        Encoding.Latin1.GetString(bytes, start, len);
#else
        string.Create(
            len,
            (Bytes: bytes, Start: start),
            static (chars, state) =>
            {
                for (int index = 0; index < chars.Length; index++)
                {
                    chars[index] = (char)state.Bytes[state.Start + index];
                }
            });
#endif

    private static string DecompressJet4Slow(byte[] bytes, int start, int len, bool allowTerminalLowByte)
    {
        int charCount = CountDecompressedChars(bytes, start, len, allowTerminalLowByte);
        return string.Create(
            charCount,
            (Bytes: bytes, Start: start, Length: len, AllowTerminalLowByte: allowTerminalLowByte),
            static (chars, state) => FillDecompressed(state.Bytes, state.Start, state.Length, chars, state.AllowTerminalLowByte));
    }

    private static int CountDecompressedChars(ReadOnlySpan<byte> bytes, int start, int len, bool allowTerminalLowByte)
    {
        int count = 0;
        bool compressed = true;
        int i = start, end = start + len;

        while (i < end)
        {
            if (compressed)
            {
                if (bytes[i] == 0x00)
                {
                    compressed = false;
                    i++;
                    continue;
                }

                count++;
                i++;
            }
            else
            {
                int runStart = i;
                while (i + 1 < end && bytes[i] != 0x00)
                {
                    i += 2;
                }

                count += (i - runStart) / 2;

                if (i + 1 >= end)
                {
                    // DAO's native calculated Text controls accept a terminal
                    // uncompressed low byte with an implicit zero high byte.
                    if (allowTerminalLowByte && i < end && bytes[i] != 0)
                    {
                        count++;
                    }

                    break;
                }

                compressed = true;
                i++;
            }
        }

        return count;
    }

    private static void FillDecompressed(ReadOnlySpan<byte> bytes, int start, int len, Span<char> output, bool allowTerminalLowByte)
    {
        int pos = 0;
        bool compressed = true;
        int i = start, end = start + len;

        while (i < end)
        {
            if (compressed)
            {
                if (bytes[i] == 0x00)
                {
                    compressed = false;
                    i++;
                    continue;
                }

                output[pos++] = (char)bytes[i++];
            }
            else
            {
                int runStart = i;
                while (i + 1 < end && bytes[i] != 0x00)
                {
                    i += 2;
                }

                int runLen = i - runStart;
                for (int r = 0; r < runLen; r += 2)
                {
                    output[pos++] = (char)(bytes[runStart + r] | (bytes[runStart + r + 1] << 8));
                }

                if (i + 1 >= end)
                {
                    if (allowTerminalLowByte && i < end && bytes[i] != 0)
                    {
                        output[pos++] = (char)bytes[i];
                    }

                    break;
                }

                compressed = true;
                i++;
            }
        }
    }
}
