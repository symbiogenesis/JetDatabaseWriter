namespace JetDatabaseWriter.ComplexColumns.Models;

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encodes / decodes the Access 2007+ Attachment <c>FileData</c> wrapper per
/// <see href="docs/design/complex-columns-format-notes.md" /> §3. The encoder is the
/// authoritative round-trip path used by the writer; the decoder is also used
/// by the typed
/// <see cref="IAccessReader.GetAttachmentsAsync(string, string, System.Threading.CancellationToken)"/>
/// surface.
/// </summary>
internal static class AttachmentWrapper
{
    /// <summary>
    /// Per Jackcess COMPRESSED_FORMATS: deflate is skipped for already-compressed media.
    /// </summary>
    private static readonly HashSet<string> CompressedFormats = new(StringComparer.OrdinalIgnoreCase)
    {
        "jpg", "zip", "gz", "bz2", "z", "7z", "cab", "rar", "mp3", "mpg",
    };

    /// <summary>
    /// Wraps <paramref name="payload"/> per spec §3.1. The 4-byte typeFlag is
    /// <c>0x00</c> (raw) when <paramref name="fileExtension"/> is in the
    /// COMPRESSED_FORMATS skip-list (or <see cref="ShouldCompress"/> overrides
    /// to <see langword="false"/>); otherwise <c>0x01</c> (raw deflate).
    /// </summary>
    /// <param name="fileExtension">Lowercase extension without leading dot (e.g. <c>"pdf"</c>).</param>
    /// <param name="payload">Raw uncompressed file bytes.</param>
    public static byte[] Encode(string fileExtension, byte[] payload)
    {
        Guard.NotNull(payload, nameof(payload));

        fileExtension ??= string.Empty;
        bool compress = ShouldCompress(fileExtension);

        // Build the contentStream: headerLen(4) + unknownFlag(4) + extLen(4) +
        // extBytes(UCS-2 LE NUL-terminated) + payload
        byte[] extBytes = EncodeExtension(fileExtension);
        int headerLen = 12 + extBytes.Length;

        byte[] contentStream = new byte[headerLen + payload.Length];
        Wu32(contentStream, 0, headerLen);
        Wu32(contentStream, 4, 1u); // unknownFlag (Jackcess writes 1)
        Wu32(contentStream, 8, extBytes.Length);
        Buffer.BlockCopy(extBytes, 0, contentStream, 12, extBytes.Length);
        Buffer.BlockCopy(payload, 0, contentStream, headerLen, payload.Length);

        byte[] body = compress ? RawDeflate(contentStream) : contentStream;
        uint typeFlag = compress ? 1u : 0u;

        byte[] wrapped = new byte[8 + body.Length];
        Wu32(wrapped, 0, typeFlag);
        Wu32(wrapped, 4, body.Length);
        Buffer.BlockCopy(body, 0, wrapped, 8, body.Length);
        return wrapped;
    }

    /// <summary>
    /// Reverses <see cref="Encode"/> and decodes Access-authored wrappers.
    /// Returns the decoded extension and raw payload, or the input bytes
    /// unchanged when the wrapper signature is not recognised.
    /// </summary>
    /// <remarks>
    /// A compressed body (typeFlag <c>1</c>) is either zlib-wrapped deflate
    /// (Access and Jackcess; <c>dataLen</c> is then the uncompressed content
    /// length) or raw deflate (this library's <see cref="Encode"/>;
    /// <c>dataLen</c> is the compressed length). Both are accepted, and the
    /// compressed body is taken to run to the end of the value.
    /// </remarks>
    /// <param name="wrapped">The wrapped.</param>
    /// <param name="fileExtension">The file extension.</param>
    /// <param name="payload">The payload.</param>
    public static bool TryDecode(byte[] wrapped, out string fileExtension, out byte[] payload)
    {
        fileExtension = string.Empty;
        payload = wrapped ?? [];
        if (wrapped == null || wrapped.Length < 8 + 12)
        {
            return false;
        }

        uint typeFlag = Ru32(wrapped, 0);
        uint dataLen = Ru32(wrapped, 4);
        if (typeFlag > 1 || dataLen == 0)
        {
            return false;
        }

        if (typeFlag == 0)
        {
            return dataLen <= (uint)(wrapped.Length - 8)
                && TryParseContent(wrapped.AsSpan(8, (int)dataLen).ToArray(), ref fileExtension, ref payload);
        }

        // A zlib stream starts with CMF = 0x?8 (deflate) and a CMF/FLG pair
        // divisible by 31. Try that reading first, then raw deflate.
        int bodyLength = wrapped.Length - 8;
        bool zlibHeader = (wrapped[8] & 0x0F) == 8 && ((wrapped[8] << 8) | wrapped[9]) % 31 == 0;
        return (zlibHeader
                && TryRawInflate(wrapped, 10, bodyLength - 2, out byte[] zlibContent)
                && TryParseContent(zlibContent, ref fileExtension, ref payload))
            || (TryRawInflate(wrapped, 8, bodyLength, out byte[] rawContent)
                && TryParseContent(rawContent, ref fileExtension, ref payload));
    }

    /// <summary>
    /// Returns <see langword="true"/> when the extension is NOT in the
    /// COMPRESSED_FORMATS skip-list (i.e. the payload should be deflate-compressed).
    /// Empty extensions are compressed.
    /// </summary>
    /// <param name="fileExtension">The file extension.</param>
    public static bool ShouldCompress(string fileExtension)
        => !CompressedFormats.Contains(fileExtension ?? string.Empty);

    private static byte[] EncodeExtension(string ext)
    {
        // UCS-2 LE, NUL-terminated. extLen counts the NUL.
        if (string.IsNullOrEmpty(ext))
        {
            return [0x00, 0x00];
        }

        byte[] raw = Encoding.Unicode.GetBytes(ext);
        byte[] withNul = new byte[raw.Length + 2];
        Buffer.BlockCopy(raw, 0, withNul, 0, raw.Length);
        return withNul;
    }

    private static string DecodeExtension(byte[] content, int offset, int length)
    {
        if (length <= 0 || offset + length > content.Length)
        {
            return string.Empty;
        }

        // Strip trailing NULs.
        int effectiveLen = length;
        while (effectiveLen >= 2 &&
               content[offset + effectiveLen - 1] == 0 &&
               content[offset + effectiveLen - 2] == 0)
        {
            effectiveLen -= 2;
        }

        return effectiveLen <= 0 ? string.Empty : Encoding.Unicode.GetString(content, offset, effectiveLen);
    }

    private static byte[] RawDeflate(byte[] data)
    {
        using var ms = new MemoryStream();
        using (var deflate = new DeflateStream(ms, CompressionLevel.Fastest, leaveOpen: true))
        {
            deflate.Write(data, 0, data.Length);
        }

        return ms.ToArray();
    }

    private static bool TryParseContent(byte[] content, ref string fileExtension, ref byte[] payload)
    {
        if (content.Length < 12)
        {
            return false;
        }

        uint headerLen = Ru32(content, 0);
        uint extLen = Ru32(content, 8);
        if (headerLen < 12 || headerLen > (uint)content.Length || extLen > headerLen - 12)
        {
            return false;
        }

        fileExtension = DecodeExtension(content, 12, (int)extLen);
        payload = content.AsSpan((int)headerLen).ToArray();
        return true;
    }

    private static bool TryRawInflate(byte[] data, int offset, int length, out byte[] content)
    {
        try
        {
            content = RawInflate(data, offset, length);
            return true;
        }
        catch (InvalidDataException)
        {
            content = [];
            return false;
        }
    }

    private static byte[] RawInflate(byte[] data, int offset, int length)
    {
        using var input = new MemoryStream(data, offset, length);
        using var deflate = new DeflateStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        deflate.CopyTo(output);
        return output.ToArray();
    }
}
