namespace JetDatabaseWriter.ValueDecoding;

using System;
#if NET8_0_OR_GREATER
using System.Buffers;
#endif
using JetDatabaseWriter.Infrastructure;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Decodes JET/ACE OLE Object column payloads. Unwraps common OLE 1.0 package
/// envelopes, locates the embedded file bytes, and recognizes well-known file
/// signatures. Raw-byte extraction (<see cref="DecodeOleValueBytes"/>) is the
/// hot read path; the <c>data:</c>-URI/MIME-detection convenience
/// (<see cref="TryDecodeOleObject"/>) is kept separate so byte projections never
/// base64-encode. Extracted from <see cref="AccessReader"/> to keep content-type
/// detection and data-URI formatting out of the storage-format reader.
/// </summary>
internal static class OleObjectDecoder
{
#if NET8_0_OR_GREATER
    private static readonly SearchValues<byte> OlePayloadSignatureFirstBytes = SearchValues.Create([0x25, 0x42, 0x47, 0x49, 0x4D, 0x50, 0x7B, 0x89, 0xD0, 0xFF]);
#else
    private static readonly byte[] OlePayloadSignatureFirstBytes = [0x25, 0x42, 0x47, 0x49, 0x4D, 0x50, 0x7B, 0x89, 0xD0, 0xFF];
#endif

    private delegate bool BytePatternMatcher<TState>(ReadOnlySpan<byte> window, ref TState state);

    /// <summary>
    /// Unwraps common OLE 1.0 package envelopes and scans the resulting payload
    /// for known file signatures (images, PDFs, Office docs, archives),
    /// returning an RFC-2397 base64 <c>data:</c> URI. Typical Access OLE fields
    /// prepend a package header before the embedded file bytes, so package-aware
    /// extraction must run before the generic sliding magic-byte scan.
    /// </summary>
    /// <param name="b">The second value or byte buffer.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    internal static string? TryDecodeOleObject(byte[] b, int start, int len)
    {
        if (b == null || len < 4)
        {
            return null;
        }

        if (TryExtractEmbeddedOlePackagePayload(b, start, len, out int payloadStart, out int payloadLength))
        {
            return TryCreateOleDataUriFromKnownMagic(b, payloadStart, payloadLength)
                ?? ("data:application/octet-stream;base64," + Convert.ToBase64String(b, payloadStart, payloadLength));
        }

        return TryCreateOleDataUriFromKnownMagic(b, start, len);
    }

    internal static byte[] DecodeOleValueBytes(byte[] buffer, int offset, int length, bool allowInputReuse = false)
    {
        if (buffer == null || length <= 0 || offset < 0 || offset >= buffer.Length)
        {
            return [];
        }

        if (TryExtractEmbeddedOlePackagePayload(buffer, offset, length, out int payloadStart, out int payloadLength))
        {
            return CreateOlePayloadBytes(buffer, payloadStart, payloadLength, allowInputReuse);
        }

        if (TryFindOlePayloadRange(buffer, offset, length, out payloadStart, out payloadLength, out _))
        {
            return CreateOlePayloadBytes(buffer, payloadStart, payloadLength, allowInputReuse);
        }

        int boundedLength = Math.Min(length, buffer.Length - offset);
        return boundedLength <= 0 ? [] : CreateOlePayloadBytes(buffer, offset, boundedLength, allowInputReuse);
    }

    private static string? TryCreateOleDataUriFromKnownMagic(byte[] buffer, int start, int len)
    {
        if (!TryFindOlePayloadRange(buffer, start, len, out int payloadStart, out int payloadLength, out string? mimeType))
        {
            return null;
        }

        return "data:" + mimeType + ";base64," + Convert.ToBase64String(buffer, payloadStart, payloadLength);
    }

    private static bool TryFindOlePayloadRange(byte[] buffer, int start, int len, out int payloadStart, out int payloadLength, out string? mimeType)
    {
        payloadStart = 0;
        payloadLength = 0;
        mimeType = null;

        int valueStart = Math.Max(start, 0);
        int valueEnd = Math.Min(start + len, buffer.Length);
        if (valueEnd - valueStart < 4)
        {
            return false;
        }

        int scanEnd = Math.Min(valueEnd, valueStart + 512);
        string? matchedMimeType = null;
        int candidate = FindMatchingBytePattern(
            buffer,
            valueStart,
            scanEnd,
            4,
            OlePayloadSignatureFirstBytes,
            static (window, ref state) => TryMatchOlePayloadMagic(window, out state),
            ref matchedMimeType);
        if (candidate < 0)
        {
            return false;
        }

        payloadStart = candidate;
        payloadLength = valueEnd - candidate;
        mimeType = matchedMimeType;
        return true;
    }

    private static int FindMatchingBytePattern<TState>(
        byte[] buffer,
        int searchStart,
        int searchEnd,
        int minimumPatternLength,
#if NET8_0_OR_GREATER
        SearchValues<byte> firstBytes,
#else
        byte[] firstBytes,
#endif
        BytePatternMatcher<TState> matcher,
        ref TState state)
    {
        int searchLimit = searchEnd - minimumPatternLength + 1;
        if (searchLimit <= searchStart)
        {
            return -1;
        }

        ReadOnlySpan<byte> searchWindow = buffer.AsSpan(searchStart, searchLimit - searchStart);
        int consumed = 0;
        while (consumed < searchWindow.Length)
        {
            int relative = IndexOfAny(searchWindow[consumed..], firstBytes);
            if (relative < 0)
            {
                return -1;
            }

            int candidate = searchStart + consumed + relative;
            ReadOnlySpan<byte> window = buffer.AsSpan(candidate, searchEnd - candidate);
            if (matcher(window, ref state))
            {
                return candidate;
            }

            consumed += relative + 1;
        }

        return -1;
    }

#if NET8_0_OR_GREATER
    private static int IndexOfAny(ReadOnlySpan<byte> source, SearchValues<byte> values) => source.IndexOfAny(values);
#else
    private static int IndexOfAny(ReadOnlySpan<byte> source, byte[] values) => source.IndexOfAny(values);
#endif

    private static bool TryMatchOlePayloadMagic(ReadOnlySpan<byte> window, out string? mimeType)
    {
        // ── Images ──
        if (window.StartsWith(Constants.OleMagicBytes.Jpeg))
        {
            mimeType = "image/jpeg";
            return true;
        }

        if (window.StartsWith(Constants.OleMagicBytes.Png))
        {
            mimeType = "image/png";
            return true;
        }

        if (window.StartsWith(Constants.OleMagicBytes.Gif))
        {
            mimeType = "image/gif";
            return true;
        }

        if (window.StartsWith(Constants.OleMagicBytes.Bmp))
        {
            mimeType = "image/bmp";
            return true;
        }

        if (window.StartsWith(Constants.OleMagicBytes.TiffLittleEndian) ||
            window.StartsWith(Constants.OleMagicBytes.TiffBigEndian))
        {
            mimeType = "image/tiff";
            return true;
        }

        // ── Documents ──
        if (window.StartsWith(Constants.OleMagicBytes.Pdf))
        {
            mimeType = "application/pdf";
            return true;
        }

        // ZIP (also DOCX/XLSX/PPTX). For simplicity, return generic zip MIME.
        if (window.StartsWith(Constants.OleMagicBytes.Zip))
        {
            mimeType = "application/zip";
            return true;
        }

        // DOC (Word 97-2003): OLE compound file.
        if (window.StartsWith(Constants.OleMagicBytes.OleCompound))
        {
            mimeType = "application/msword";
            return true;
        }

        // RTF: {\rt
        if (window.StartsWith(Constants.OleMagicBytes.Rtf))
        {
            mimeType = "application/rtf";
            return true;
        }

        mimeType = null;
        return false;
    }

    private static bool TryExtractEmbeddedOlePackagePayload(byte[] buffer, int start, int len, out int payloadStart, out int payloadLength)
    {
        const ushort olePackageSignature = 0x1C15;
        const int oleVersion = 0x0501;
        const ushort olePackageStreamSignature = 0x0002;
        const int embeddedFilePackageType = 0x030000;

        payloadStart = 0;
        payloadLength = 0;

        if (start < 0 || len < 24 || start > buffer.Length - 4)
        {
            return false;
        }

        int valueEnd = Math.Min(start + len, buffer.Length);
        ReadOnlySpan<byte> value = buffer.AsSpan(start, valueEnd - start);
        if (value.Length < 24 || Ru16(value, 0) != olePackageSignature)
        {
            return false;
        }

        int headerSize = Ru16(value, 2);
        if (headerSize < 20 || headerSize > value.Length - 24)
        {
            return false;
        }

        int oleHeaderOffset = headerSize;
        if (Ri32(value, oleHeaderOffset) != oleVersion)
        {
            return false;
        }

        int typeNameLength = Ri32(value, oleHeaderOffset + 8);
        if (typeNameLength <= 0)
        {
            return false;
        }

        int dataBlockLengthOffset = oleHeaderOffset + 20 + typeNameLength;
        if (dataBlockLengthOffset + 4 > value.Length)
        {
            return false;
        }

        int dataBlockLength = Ri32(value, dataBlockLengthOffset);
        int dataBlockOffset = dataBlockLengthOffset + 4;
        if (dataBlockLength <= 0 || dataBlockOffset + dataBlockLength > value.Length)
        {
            return false;
        }

        ReadOnlySpan<byte> dataBlock = value.Slice(dataBlockOffset, dataBlockLength);
        if (dataBlock.Length < 2 || Ru16(dataBlock, 0) != olePackageStreamSignature)
        {
            return false;
        }

        int cursor = 2;
        if (!TrySkipZeroTermAsciiString(dataBlock, ref cursor) ||
            !TrySkipZeroTermAsciiString(dataBlock, ref cursor) ||
            cursor + 8 > dataBlock.Length)
        {
            return false;
        }

        int packageType = Ri32(dataBlock, cursor);
        cursor += 4;
        if (packageType != embeddedFilePackageType)
        {
            return false;
        }

        int localFilePathLength = Ri32(dataBlock, cursor);
        cursor += 4;
        if (localFilePathLength < 0 || cursor + localFilePathLength + 4 > dataBlock.Length)
        {
            return false;
        }

        cursor += localFilePathLength;

        int embeddedLength = Ri32(dataBlock, cursor);
        cursor += 4;
        if (embeddedLength <= 0 || cursor + embeddedLength > dataBlock.Length)
        {
            return false;
        }

        payloadStart = start + dataBlockOffset + cursor;
        payloadLength = embeddedLength;
        return true;
    }

    private static bool TrySkipZeroTermAsciiString(ReadOnlySpan<byte> value, ref int offset)
    {
        if ((uint)offset >= (uint)value.Length)
        {
            return false;
        }

        int terminator = value[offset..].IndexOf((byte)0x00);
        if (terminator < 0)
        {
            return false;
        }

        offset += terminator + 1;
        return true;
    }

    private static byte[] CreateOlePayloadBytes(byte[] buffer, int offset, int length, bool allowInputReuse) =>
        allowInputReuse && offset == 0 && length == buffer.Length
            ? buffer
            : BinaryBuffer.CopySlice(buffer, offset, length);
}
