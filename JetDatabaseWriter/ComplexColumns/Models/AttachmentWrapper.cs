namespace JetDatabaseWriter.ComplexColumns.Models;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Text;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encodes / decodes the Access 2007+ Attachment <c>FileData</c> wrapper per
/// <see href="docs/design/complex-columns-format-notes.md" /> §3. The encoder
/// writes what Access writes (checked against every Access-authored attachment
/// in the test fixtures). The decoder also serves the typed
/// <see cref="IAccessReader.GetAttachmentsAsync(string, string, System.Threading.CancellationToken)"/>
/// surface and the row reads.
/// </summary>
internal static class AttachmentWrapper
{
    private const int WrapperHeaderSize = 8;
    private const int ContentHeaderSize = 12;
    private const uint ContentHeaderFlag = 1;

    /// <summary>The zlib CMF byte Access writes: deflate with a 32 KB window.</summary>
    private const byte ZlibCmf = 0x78;

    /// <summary>
    /// The zlib FLG byte Access writes: FLEVEL 1, the level Jackcess's
    /// <c>Deflater(3)</c> also announces. Decoders ignore FLEVEL.
    /// </summary>
    private const byte ZlibFlg = 0x5E;

    /// <summary>
    /// The formats Access stores raw (typeFlag 0). Access deflates everything
    /// else, including formats that are compressed already, such as gz and mp3.
    /// This is Jackcess 4.0.12's <c>COMPRESSED_FORMATS</c>, from its author's
    /// observation of Access; the fixtures store jpg raw and txt compressed.
    /// </summary>
    private static readonly HashSet<string> RawFormats = new(StringComparer.OrdinalIgnoreCase)
    {
        "jpg", "jpeg", "gif", "png", "zip", "cab", "docx", "xlsx", "xlsb", "pptx",
    };

    /// <summary>
    /// Wraps <paramref name="payload"/> per spec §3.1 as Access does: typeFlag
    /// <c>0</c> (stored raw) when the extension is one Access stores raw, else
    /// <c>1</c> (a zlib stream); <c>dataLen</c> is the length of the uncompressed
    /// content stream either way. The content header holds the lowercased
    /// extension as NUL-terminated UTF-16LE, with its length counted in
    /// characters including the NUL.
    /// </summary>
    /// <param name="fileExtension">The extension without its leading dot (e.g. <c>"pdf"</c>); it is stored lowercased.</param>
    /// <param name="payload">Raw uncompressed file bytes.</param>
    public static byte[] Encode(string fileExtension, byte[] payload)
    {
        Guard.NotNull(payload, nameof(payload));

        string extension = NormalizeExtension(fileExtension);
        bool compress = ShouldCompress(extension);
        byte[] content = BuildContent(extension, payload);
        byte[] body = compress ? ZlibDeflate(content) : content;

        byte[] wrapped = new byte[WrapperHeaderSize + body.Length];
        Wu32(wrapped, 0, compress ? 1u : 0u);
        Wu32(wrapped, 4, content.Length);
        Buffer.BlockCopy(body, 0, wrapped, WrapperHeaderSize, body.Length);
        return wrapped;
    }

    /// <summary>
    /// Reverses <see cref="Encode"/> and decodes Access-authored wrappers.
    /// Returns the decoded extension and raw payload, or the input bytes
    /// unchanged when the wrapper signature is not recognised.
    /// </summary>
    /// <param name="wrapped">The wrapped.</param>
    /// <param name="fileExtension">The file extension.</param>
    /// <param name="payload">The payload.</param>
    public static bool TryDecode(byte[] wrapped, out string fileExtension, out byte[] payload)
    {
        fileExtension = string.Empty;
        payload = wrapped ?? [];
        if (wrapped == null || wrapped.Length < WrapperHeaderSize + ContentHeaderSize)
        {
            return false;
        }

        uint typeFlag = Ru32(wrapped, 0);
        uint dataLen = Ru32(wrapped, 4);
        if (typeFlag > 1 || dataLen == 0)
        {
            return false;
        }

        int bodyLength = wrapped.Length - WrapperHeaderSize;
        if (typeFlag == 0)
        {
            return dataLen <= (uint)bodyLength
                && TryParseContent(wrapped.AsSpan(WrapperHeaderSize, (int)dataLen).ToArray(), ref fileExtension, ref payload);
        }

        // Native Access compressed attachments use zlib without a dictionary.
        if (bodyLength < 6 || dataLen > int.MaxValue
            || (wrapped[WrapperHeaderSize] & 0x0F) != 8
            || (wrapped[WrapperHeaderSize] >> 4) > 7
            || (wrapped[WrapperHeaderSize + 1] & 0x20) != 0
            || ((wrapped[WrapperHeaderSize] << 8) | wrapped[WrapperHeaderSize + 1]) % 31 != 0)
        {
            return false;
        }

        return TryRawInflate(wrapped, WrapperHeaderSize + 2, bodyLength - 6, (int)dataLen, out byte[] content)
            && Adler32.Compute(content) == BinaryPrimitives.ReadUInt32BigEndian(wrapped.AsSpan(wrapped.Length - 4))
            && TryParseContent(content, ref fileExtension, ref payload);
    }
    /// <summary>
    /// Returns <see langword="true"/> when Access deflates files with this
    /// extension, i.e. it is not one Access stores raw. Case-insensitive; an
    /// empty extension is compressed.
    /// </summary>
    /// <param name="fileExtension">The file extension.</param>
    public static bool ShouldCompress(string fileExtension)
        => !RawFormats.Contains(fileExtension ?? string.Empty);

    [SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase", Justification = "Access and Jackcess store attachment extensions in lowercase.")]
    private static string NormalizeExtension(string? fileExtension)
        => string.IsNullOrEmpty(fileExtension) ? string.Empty : fileExtension.ToLowerInvariant();

    /// <summary>
    /// Builds the content stream: <c>headerLen</c>, the flag <c>1</c>, the
    /// extension length in characters including its NUL, the extension as
    /// NUL-terminated UTF-16LE, then the file bytes.
    /// </summary>
    /// <param name="extension">The extension, already lowercased.</param>
    /// <param name="payload">The file bytes.</param>
    private static byte[] BuildContent(string extension, byte[] payload)
    {
        int extensionChars = extension.Length + 1;
        int headerLen = ContentHeaderSize + (extensionChars * 2);

        byte[] content = new byte[headerLen + payload.Length];
        Wu32(content, 0, headerLen);
        Wu32(content, 4, ContentHeaderFlag);
        Wu32(content, 8, extensionChars);
        _ = Encoding.Unicode.GetBytes(extension, 0, extension.Length, content, ContentHeaderSize);
        Buffer.BlockCopy(payload, 0, content, headerLen, payload.Length);
        return content;
    }

    /// <summary>
    /// Compresses <paramref name="content"/> as a zlib stream: Access's
    /// <c>78 5E</c> header, the deflate blocks, and the big-endian Adler-32 of
    /// the content. The deflate blocks differ from Access's, which no .NET
    /// compressor reproduces; any zlib decoder reads both.
    /// </summary>
    /// <param name="content">The content stream.</param>
    private static byte[] ZlibDeflate(byte[] content)
    {
        using var ms = new MemoryStream();
        ms.WriteByte(ZlibCmf);
        ms.WriteByte(ZlibFlg);
        using (var deflate = new DeflateStream(ms, CompressionLevel.Optimal, leaveOpen: true))
        {
            deflate.Write(content, 0, content.Length);
        }

        Span<byte> adler = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(adler, Adler32.Compute(content));
        ms.Write(adler);
        return ms.ToArray();
    }

    /// <summary>
    /// Reads the UTF-16LE extension from <paramref name="length"/> bytes at
    /// <paramref name="offset"/>, up to its NUL.
    /// </summary>
    /// <param name="content">The content stream.</param>
    /// <param name="offset">Where the extension starts.</param>
    /// <param name="length">The bytes the header leaves for it.</param>
    private static string DecodeExtension(byte[] content, int offset, int length)
    {
        int chars = 0;
        while (((chars + 1) * 2) <= length
            && (content[offset + (chars * 2)] != 0 || content[offset + (chars * 2) + 1] != 0))
        {
            chars++;
        }

        return chars == 0 ? string.Empty : Encoding.Unicode.GetString(content, offset, chars * 2);
    }

    private static bool TryParseContent(byte[] content, ref string fileExtension, ref byte[] payload)
    {
        if (content.Length < ContentHeaderSize)
        {
            return false;
        }

        uint headerLen = Ru32(content, 0);
        if (headerLen < ContentHeaderSize || headerLen > (uint)content.Length)
        {
            return false;
        }

        fileExtension = DecodeExtension(content, ContentHeaderSize, (int)headerLen - ContentHeaderSize);
        payload = content.AsSpan((int)headerLen).ToArray();
        return true;
    }

    private static bool TryRawInflate(byte[] data, int offset, int length, int expectedLength, out byte[] content)
    {
        try
        {
            content = RawInflate(data, offset, length, expectedLength);
            return true;
        }
        catch (InvalidDataException)
        {
            content = [];
            return false;
        }
    }

    private static byte[] RawInflate(byte[] data, int offset, int length, int expectedLength)
    {
        using var input = new MemoryStream(data, offset, length);
        using var deflate = new DeflateStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        byte[] buffer = new byte[8192];
        while (output.Length < expectedLength)
        {
            int wanted = Math.Min(buffer.Length, expectedLength - checked((int)output.Length));
            int read = deflate.Read(buffer, 0, wanted);
            if (read == 0)
            {
                throw new InvalidDataException("The attachment content is shorter than its declared length.");
            }

            output.Write(buffer, 0, read);
        }

        if (deflate.ReadByte() != -1)
        {
            throw new InvalidDataException("The attachment content exceeds its declared length.");
        }
        return output.ToArray();
    }
}
