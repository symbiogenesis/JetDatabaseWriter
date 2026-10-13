namespace JetDatabaseWriter.ComplexColumns.Models;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using JetDatabaseWriter.Exceptions;
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
    private const int ProcessingChunkSize = 8192;

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
    /// <param name="maxContentBytes">The maximum uncompressed content size.</param>
    /// <param name="contentBudget">The aggregate content budget for this operation.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetLimitationException">The declared content exceeds the configured byte budget.</exception>
    public static bool TryDecode(byte[] wrapped, out string fileExtension, out byte[] payload, int maxContentBytes = 64 * 1024 * 1024, ContentBudget? contentBudget = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
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

        if (dataLen > maxContentBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, $"The attachment declares {dataLen} content byte(s), exceeding the configured limit {maxContentBytes}.");
        }

        (contentBudget ?? new ContentBudget(256 * 1024 * 1024, 2L * 1024 * 1024 * 1024)).Consume(dataLen, wrapped.Length);
        int bodyLength = wrapped.Length - WrapperHeaderSize;
        if (typeFlag == 0)
        {
            return dataLen <= (uint)bodyLength
                && TryParseContent(wrapped, WrapperHeaderSize, (int)dataLen, ref fileExtension, ref payload, cancellationToken);
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

        if (!TryRawInflate(wrapped, WrapperHeaderSize + 2, bodyLength - 6, (int)dataLen, cancellationToken, out byte[] content))
        {
            return false;
        }

        bool checksumMatches = Adler32.Compute(content, cancellationToken) == BinaryPrimitives.ReadUInt32BigEndian(wrapped.AsSpan(wrapped.Length - 4));
        cancellationToken.ThrowIfCancellationRequested();
        return checksumMatches && TryParseContent(content, 0, content.Length, ref fileExtension, ref payload, cancellationToken);
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private static string DecodeExtension(byte[] content, int offset, int length, CancellationToken cancellationToken)
    {
        int chars = 0;
        while (((chars + 1) * 2) <= length
            && (content[offset + (chars * 2)] != 0 || content[offset + (chars * 2) + 1] != 0))
        {
            if (chars % (ProcessingChunkSize / 2) == 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }

            chars++;
        }

        cancellationToken.ThrowIfCancellationRequested();
        return string.Create(chars, (Content: content, Offset: offset, Length: chars * 2, Cancellation: cancellationToken), static (destination, state) =>
        {
            Decoder decoder = Encoding.Unicode.GetDecoder();
            int bytesRead = 0;
            int charsWritten = 0;
            while (bytesRead < state.Length)
            {
                state.Cancellation.ThrowIfCancellationRequested();
                int count = Math.Min(ProcessingChunkSize, state.Length - bytesRead);
                charsWritten += decoder.GetChars(state.Content.AsSpan(state.Offset + bytesRead, count), destination[charsWritten..], flush: bytesRead + count == state.Length);
                bytesRead += count;
            }

            state.Cancellation.ThrowIfCancellationRequested();
        });
    }

    private static bool TryParseContent(byte[] content, int offset, int length, ref string fileExtension, ref byte[] payload, CancellationToken cancellationToken)
    {
        if (length < ContentHeaderSize)
        {
            return false;
        }

        uint headerLen = Ru32(content, offset);
        if (headerLen < ContentHeaderSize || headerLen > (uint)length)
        {
            return false;
        }

        fileExtension = DecodeExtension(content, offset + ContentHeaderSize, (int)headerLen - ContentHeaderSize, cancellationToken);
        payload = new byte[length - (int)headerLen];
        for (int copied = 0; copied < payload.Length;)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int count = Math.Min(ProcessingChunkSize, payload.Length - copied);
            content.AsSpan(offset + (int)headerLen + copied, count).CopyTo(payload.AsSpan(copied));
            copied += count;
        }

        cancellationToken.ThrowIfCancellationRequested();
        return true;
    }

    private static bool TryRawInflate(byte[] data, int offset, int length, int expectedLength, CancellationToken cancellationToken, out byte[] content)
    {
        try
        {
            content = RawInflate(data, offset, length, expectedLength, cancellationToken);
            return true;
        }
        catch (InvalidDataException)
        {
            content = [];
            return false;
        }
    }

    private static byte[] RawInflate(byte[] data, int offset, int length, int expectedLength, CancellationToken cancellationToken)
    {
        using var input = new MemoryStream(data, offset, length);
        return Inflate(input, expectedLength, cancellationToken);
    }

    /// <summary>Inflates exactly the declared content length from a caller-owned deflate stream.</summary>
    /// <param name="input">The compressed input.</param>
    /// <param name="expectedLength">The declared uncompressed content length.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The uncompressed content.</returns>
    /// <exception cref="InvalidDataException">The content length differs from its declaration.</exception>
    internal static byte[] Inflate(Stream input, int expectedLength, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var boundedInput = new CancellationCheckingStream(input, cancellationToken);
        using var deflate = new DeflateStream(boundedInput, CompressionMode.Decompress, leaveOpen: true);
        byte[] output = new byte[expectedLength];
        int position = 0;
        while (position < expectedLength)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int wanted = Math.Min(ProcessingChunkSize, expectedLength - position);
            int read = deflate.Read(output, position, wanted);
            cancellationToken.ThrowIfCancellationRequested();
            if (read == 0)
            {
                throw new InvalidDataException("The attachment content is shorter than its declared length.");
            }

            position += read;
        }

        cancellationToken.ThrowIfCancellationRequested();
        int extraByte = deflate.ReadByte();
        cancellationToken.ThrowIfCancellationRequested();
        if (extraByte != -1)
        {
            throw new InvalidDataException("The attachment content exceeds its declared length.");
        }

        return output;
    }

    /// <summary>Bounds aggregate declared content and byte-processing work decoded by one operation.</summary>
    /// <param name="limit">The maximum aggregate content length.</param>
    /// <param name="workLimit">The maximum aggregate byte-processing work.</param>
    internal sealed class ContentBudget(long limit, long workLimit)
    {
#if NET9_0_OR_GREATER
        private readonly Lock budgetLock = new();
#else
        private readonly object budgetLock = new();
#endif
        private long remaining = limit;
        private long workRemaining = workLimit;

        /// <summary>Reserves content bytes before allocating or decompressing them.</summary>
        /// <param name="byteCount">The declared uncompressed content length.</param>
        /// <param name="storedLength">The full stored wrapper length.</param>
        /// <exception cref="JetLimitationException">The aggregate content limit is exceeded.</exception>
        internal void Consume(uint byteCount, int storedLength)
        {
            long work = storedLength + (4L * byteCount);
            lock (this.budgetLock)
            {
                if (byteCount > this.remaining)
                {
                    throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Attachment content for this read exceeds MaxAttachmentContentBytesPerOperation.");
                }

                if (work > this.workRemaining)
                {
                    throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Attachment decoding for this read exceeds MaxAttachmentDecodeWorkBytesPerOperation.");
                }

                this.remaining -= byteCount;
                this.workRemaining -= work;
            }
        }
    }

    /// <summary>Checks cancellation even when the inflater consumes many empty blocks in a single read.</summary>
    /// <param name="input">The caller-owned compressed stream.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private sealed class CancellationCheckingStream(Stream input, CancellationToken cancellationToken) : Stream
    {
        public override bool CanRead => input.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int read = input.Read(buffer, offset, Math.Min(count, ProcessingChunkSize));
            cancellationToken.ThrowIfCancellationRequested();
            return read;
        }

        public override int Read(Span<byte> buffer)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int read = input.Read(buffer[..Math.Min(buffer.Length, ProcessingChunkSize)]);
            cancellationToken.ThrowIfCancellationRequested();
            return read;
        }

        public override void Flush() => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
