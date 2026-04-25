namespace JetDatabaseWriter.Internal;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

/// <summary>
/// Encodes / decodes the Access 2007+ Attachment <c>FileData</c> wrapper per
/// <c>docs/design/complex-columns-format-notes.md</c> §3. The encoder is the
/// authoritative round-trip path for Phase C4; the decoder is also used by
/// the typed
/// <see cref="IAccessReader.GetAttachmentsAsync(string, string, System.Threading.CancellationToken)"/>
/// surface.
/// </summary>
internal static class AttachmentWrapper
{
    // Per Jackcess COMPRESSED_FORMATS: deflate is skipped for already-compressed media.
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
        ArgumentNullException.ThrowIfNull(payload);

        fileExtension ??= string.Empty;
        bool compress = ShouldCompress(fileExtension);

        // Build the contentStream: headerLen(4) + unknownFlag(4) + extLen(4) +
        // extBytes(UCS-2 LE NUL-terminated) + payload
        byte[] extBytes = EncodeExtension(fileExtension);
        int headerLen = 12 + extBytes.Length;

        byte[] contentStream = new byte[headerLen + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(contentStream.AsSpan(0, 4), (uint)headerLen);
        BinaryPrimitives.WriteUInt32LittleEndian(contentStream.AsSpan(4, 4), 1u); // unknownFlag (Jackcess writes 1)
        BinaryPrimitives.WriteUInt32LittleEndian(contentStream.AsSpan(8, 4), (uint)extBytes.Length);
        Buffer.BlockCopy(extBytes, 0, contentStream, 12, extBytes.Length);
        Buffer.BlockCopy(payload, 0, contentStream, headerLen, payload.Length);

        byte[] body;
        uint typeFlag;
        if (compress)
        {
            body = RawDeflate(contentStream);
            typeFlag = 1u;
        }
        else
        {
            body = contentStream;
            typeFlag = 0u;
        }

        byte[] wrapped = new byte[8 + body.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(wrapped.AsSpan(0, 4), typeFlag);
        BinaryPrimitives.WriteUInt32LittleEndian(wrapped.AsSpan(4, 4), (uint)body.Length);
        Buffer.BlockCopy(body, 0, wrapped, 8, body.Length);
        return wrapped;
    }

    /// <summary>
    /// Reverses <see cref="Encode"/>. Returns the decoded extension and raw payload.
    /// Returns the input bytes unchanged when the wrapper signature is not
    /// recognised (legacy / heuristic path).
    /// </summary>
    public static bool TryDecode(byte[] wrapped, out string fileExtension, out byte[] payload)
    {
        fileExtension = string.Empty;
        payload = wrapped ?? Array.Empty<byte>();
        if (wrapped == null || wrapped.Length < 8 + 12)
        {
            return false;
        }

        uint typeFlag = BinaryPrimitives.ReadUInt32LittleEndian(wrapped.AsSpan(0, 4));
        uint dataLen = BinaryPrimitives.ReadUInt32LittleEndian(wrapped.AsSpan(4, 4));
        if (typeFlag > 1)
        {
            return false;
        }

        if (dataLen == 0 || dataLen > (uint)(wrapped.Length - 8))
        {
            return false;
        }

        byte[] content;
        if (typeFlag == 1)
        {
            try
            {
                content = RawInflate(wrapped, 8, (int)dataLen);
            }
            catch (InvalidDataException)
            {
                return false;
            }
        }
        else
        {
            content = new byte[(int)dataLen];
            Buffer.BlockCopy(wrapped, 8, content, 0, (int)dataLen);
        }

        if (content.Length < 12)
        {
            return false;
        }

        uint headerLen = BinaryPrimitives.ReadUInt32LittleEndian(content.AsSpan(0, 4));
        uint extLen = BinaryPrimitives.ReadUInt32LittleEndian(content.AsSpan(8, 4));
        if (headerLen < 12 || headerLen > content.Length || 12 + extLen > headerLen)
        {
            return false;
        }

        fileExtension = DecodeExtension(content, 12, (int)extLen);
        int payloadLen = content.Length - (int)headerLen;
        payload = payloadLen > 0 ? new byte[payloadLen] : Array.Empty<byte>();
        if (payloadLen > 0)
        {
            Buffer.BlockCopy(content, (int)headerLen, payload, 0, payloadLen);
        }

        return true;
    }

    /// <summary>
    /// Returns <see langword="true"/> when the extension is NOT in the
    /// COMPRESSED_FORMATS skip-list (i.e. the payload should be deflate-compressed).
    /// Empty extensions are compressed.
    /// </summary>
    public static bool ShouldCompress(string fileExtension)
        => !CompressedFormats.Contains(fileExtension ?? string.Empty);

    private static byte[] EncodeExtension(string ext)
    {
        // UCS-2 LE, NUL-terminated. extLen counts the NUL.
        if (string.IsNullOrEmpty(ext))
        {
            return new byte[] { 0x00, 0x00 };
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

    private static byte[] RawInflate(byte[] data, int offset, int length)
    {
        using var input = new MemoryStream(data, offset, length);
        using var deflate = new DeflateStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        deflate.CopyTo(output);
        return output.ToArray();
    }
}
