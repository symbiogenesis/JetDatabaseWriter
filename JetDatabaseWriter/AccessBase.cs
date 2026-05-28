namespace JetDatabaseWriter;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using static JetDatabaseWriter.Constants.ColumnTypes;
using static JetDatabaseWriter.Schema.JetTypeInfo;

#pragma warning disable SA1401 // Field should be private — fields are private protected (assembly-only)

/// <summary>
/// Abstract base class for Access database readers and writers.
/// Contains shared JET format parsing, page I/O, catalog access, and text decoding.
/// </summary>
public abstract class AccessBase : IAccessBase
{
    // ── Format-specific layouts ───────────────────────────────────────
    // Each struct groups a related set of byte offsets / entry sizes that
    // differ between Jet3 (Access 97 .mdb) and Jet4/ACE (.mdb + .accdb).
    // Populated once at construction so reader/writer call sites do not need
    // to inline `jet3 ? ... : ...` ternaries on every access.

    /// <summary>Per-format byte offsets within a data-page (page type 0x01) header — see <see cref="DataPageLayout"/>.</summary>
    internal readonly DataPageLayout dataPage;

    /// <summary>Per-format byte offsets within a TDEF block plus real-idx entry size — see <see cref="TDefHeaderLayout"/>.</summary>
    internal readonly TDefHeaderLayout tdef;

    /// <summary>Per-format byte offsets within one column descriptor — see <see cref="ColumnDescriptorLayout"/>.</summary>
    internal readonly ColumnDescriptorLayout colDesc;

    /// <summary>Per-format byte sizes of the in-row trailer fields — see <see cref="RowFieldSizes"/>.</summary>
    internal readonly RowFieldSizes rowSz;

    /// <summary>
    /// Per-format byte offsets and entry sizes for the TDEF page's real-idx
    /// physical descriptor (§3.1) and logical-idx entry (§3.2) sections.
    /// </summary>
    internal readonly IndexLayout indexLayout;

    internal readonly int pgSz;
    internal readonly DatabaseFormat format;
    internal readonly Stream stream;
    private readonly bool leaveOpen;
    private protected readonly Encoding ansiEncoding;
    private protected readonly int codePage;
    private protected readonly string path;

    internal Encoding AnsiEncoding => ansiEncoding;

    /// <summary>
    /// Per-page decryption keys (Jet3 XOR, Jet4 RC4, ACCDB AES). Populated during
    /// reader construction by <see cref="EncryptionManager"/>. Mutated only on the
    /// constructor thread; consulted by every page read via
    /// <see cref="EncryptionManager.DecryptPageInPlace(byte[], long, int, PageDecryptionKeys)"/>.
    /// </summary>
    private protected readonly PageDecryptionKeys pageKeys = new();

    internal bool disposed;
    private readonly SemaphoreSlim ioGate = new(1, 1);
    private volatile List<CatalogEntry>? catalogCache;
    private volatile List<LinkedTableInfo>? linkedTableCache;

    /// <summary>
    /// Cooperative JET byte-range lock helper (Win32 <c>LockFileEx</c>). Defaults to
    /// <see cref="JetByteRangeLock.Disabled"/> so page-write paths can dispatch
    /// without a null check; <see cref="AccessReader"/> / <see cref="AccessWriter"/>
    /// replace it with a stream-bound instance once options are known.
    /// </summary>
    private protected JetByteRangeLock byteRangeLock = JetByteRangeLock.Disabled;

    /// <summary>
    /// Gets or sets the in-memory page journal for an explicit <see cref="JetTransaction"/>.
    /// When non-null, page writes/appends are buffered into the journal
    /// instead of being flushed to the underlying stream, and page reads
    /// consult the journal first so the transaction sees its own writes.
    /// Set/cleared exclusively by <see cref="AccessWriter"/> while holding
    /// <see cref="ioGate"/>.
    /// </summary>
    internal PageJournal? ActiveJournal { get; set; }

    /// <summary>Gets the writer's internal I/O gate so derived types may serialise transaction commit / rollback.</summary>
    internal SemaphoreSlim IoGate => ioGate;

    static AccessBase()
    {
        // On .NET Core / .NET 5+ code-page encodings (e.g. Windows-1252) are not
        // available by default. Register them once so GetEncoding() works for any
        // ANSI code page stored in the JET database header.
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessBase"/> class
    /// from a pre-read database file header.
    /// </summary>
    /// <param name="stream">An open, seekable <see cref="Stream"/> for the database file.</param>
    /// <param name="hdr">Header bytes read from page 0.</param>
    /// <param name="path">Path to the database file, or empty when opened from a stream.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller retains ownership of <paramref name="stream"/> and it will not be disposed.</param>
    private protected AccessBase(Stream stream, byte[] hdr, string path = "", bool leaveOpen = false)
    {
        this.stream = stream;
        this.leaveOpen = leaveOpen;
        this.path = path ?? string.Empty;

        format = EncryptionConverter.DetectFormat(hdr);
        pgSz = GetPageSize(format);

        pageKeys.Jet3XorMask = EncryptionManager.GetJet3PageMask(format, hdr);

        // Codepage / sort order: stored as a UInt16 at hdr[0x3C], scrambled by
        // the constant-key RC4 stream Microsoft Access applies to header bytes
        // [0x18 .. 0x18+126/128]. EncryptionManager.DecodeHeaderCodePage handles
        // the descrambling so we recover the real codepage (e.g. 1252) instead
        // of a corrupted byte. ACE / ACCDB stores text as UTF-16 in user data
        // so the codepage there is largely cosmetic, but Jet3 .mdb files (and
        // Jet4 catalog names) need it correct to round-trip non-ASCII names.
        codePage = EncryptionManager.DecodeHeaderCodePage(hdr, format);
        if (codePage <= 0)
        {
            codePage = 1252;  // default to Windows-1252 if unknown
        }

        try
        {
            ansiEncoding = Encoding.GetEncoding(codePage);
        }
        catch (ArgumentException)
        {
            ansiEncoding = Encoding.UTF8;
            codePage = 65001;
        }
        catch (NotSupportedException)
        {
            ansiEncoding = Encoding.UTF8;
            codePage = 65001;
        }

        // Format-specific TDEF / page / column / row layouts:
        //   Jet4 / ACE (Access 2000–2019): TDEF 8+55 = 63 bytes, column descriptor 25 bytes.
        //   Jet3        (Access 97):       TDEF 8+35 = 43 bytes, column descriptor 18 bytes.
        dataPage = DataPageLayout.For(format);
        tdef = TDefHeaderLayout.For(format);
        colDesc = ColumnDescriptorLayout.For(format);
        rowSz = RowFieldSizes.For(format);
        indexLayout = IndexLayout.For(format);
    }

    /// <inheritdoc/>
    public DatabaseFormat DatabaseFormat => format;

    /// <inheritdoc/>
    public int PageSize => pgSz;

    /// <inheritdoc/>
    public int CodePage => codePage;

    internal bool UsesRandomAccessPageReads { get; private set; }

    private protected void EnableRandomAccessPageReadsIfSupported()
    {
#if NET6_0_OR_GREATER
        if (stream is FileStream fileStream &&
            !fileStream.SafeFileHandle.IsInvalid &&
            !fileStream.SafeFileHandle.IsClosed)
        {
            UsesRandomAccessPageReads = true;
        }
#else
        UsesRandomAccessPageReads = false;
#endif
    }

    /// <inheritdoc/>
    public virtual async ValueTask DisposeAsync()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;
        if (!leaveOpen)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }

        ioGate.Dispose();
        pageKeys.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>Returns the page size in bytes for the given database format (2048 for Jet3, 4096 for Jet4/ACE).</summary>
    /// <param name="format">The format.</param>
    internal static int GetPageSize(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet4 : Constants.PageSizes.Jet3;

    /// <summary>
    /// Asynchronously reads the fixed-size JET header (first 0x80 bytes) from page 0.
    /// </summary>
    /// <param name="fs">An open, seekable stream positioned anywhere.</param>
    /// <param name="cancellationToken">Token used to cancel the read operation.</param>
    /// <returns>A 0x80-byte header buffer.</returns>
    private protected static async ValueTask<byte[]> ReadHeaderAsync(Stream fs, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var hdr = new byte[0x80];
        _ = fs.Seek(0, SeekOrigin.Begin);
        await fs.ReadExactlyAsync(hdr.AsMemory(), cancellationToken).ConfigureAwait(false);

        return hdr;
    }

    // ── Static helpers ────────────────────────────────────────────────

    internal static void ReturnPage(byte[] page)
    {
        ArrayPool<byte>.Shared.Return(page);
    }

    // Little-endian primitives (Ru16/Ri32/Ru32/Ri64/Wu16/Wu32/Wi32/Wi64) and
    // float/24-bit/hex helpers live in JetTypeInfo so non-Core callers
    // (Encryption layer, IndexLeafIncremental, …) can use them without
    // taking an upward dependency on AccessBase. They are surfaced here
    // through the file-level `using static JetDatabaseWriter.Schema.JetTypeInfo;`.

    internal static void WriteUInt24(byte[] b, int o, int value)
    {
        Wu16(b, o, value & 0xFFFF);
        b[o + 2] = (byte)((value >> 16) & 0xFF);
    }

    internal static void WriteField(byte[] b, int o, int fieldSize, int value)
    {
        if (fieldSize == 1)
        {
            b[o] = (byte)value;
        }
        else
        {
            Wu16(b, o, value);
        }
    }

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
                if (c == '\0' || c > 0xFF)
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
    {
        if (len < 2)
        {
            return string.Empty;
        }

        if (bytes[start] == 0xFF && bytes[start + 1] == 0xFE)
        {
            return DecompressJet4(bytes, start + 2, len - 2);
        }

        // Plain UCS-2 LE — length must be even
        int evenLen = len & ~1;
        return evenLen > 0 ? JetTypeInfo.DecodeUtf16LE(bytes.AsSpan(start, evenLen)) : string.Empty;
    }

    /// <summary>
    /// Decodes Jet4 text from a span-backed buffer. Array-backed reader hot paths
    /// use the byte-array overload so compressed strings can be built directly.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decoded string.</returns>
    internal static string DecodeJet4Text(ReadOnlySpan<byte> bytes, int start, int len)
    {
        if (len < 2)
        {
            return string.Empty;
        }

        if (bytes[start] == 0xFF && bytes[start + 1] == 0xFE)
        {
            return DecompressJet4(bytes, start + 2, len - 2);
        }

        // Plain UCS-2 LE — length must be even
        int evenLen = len & ~1;
        return evenLen > 0 ? JetTypeInfo.DecodeUtf16LE(bytes.Slice(start, evenLen)) : string.Empty;
    }

    /// <summary>
    /// Decodes the JET4 "compressed unicode" encoding.
    /// A 0x00 byte toggles between 1-byte compressed (ASCII) and 2-byte
    /// uncompressed (UCS-2) mode.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decompressed string.</returns>
    private protected static string DecompressJet4(byte[] bytes, int start, int len)
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

        return DecompressJet4Slow(bytes, start, len);
    }

    /// <summary>
    /// Decodes the JET4 "compressed unicode" encoding from a span-backed buffer.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decompressed string.</returns>
    private protected static string DecompressJet4(ReadOnlySpan<byte> bytes, int start, int len)
    {
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

        return DecompressJet4Slow(bytes, start, len);
    }

    private static string CreateFromCompressed(byte[] bytes, int start, int len)
    {
#if NET6_0_OR_GREATER
        return Encoding.Latin1.GetString(bytes, start, len);
#else
        return string.Create(
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
    }

    private static string CreateFromCompressed(ReadOnlySpan<byte> bytes, int start, int len)
    {
        var chars = new char[len];
        for (int index = 0; index < len; index++)
        {
            chars[index] = (char)bytes[start + index];
        }

        return new string(chars);
    }

    private static string DecompressJet4Slow(byte[] bytes, int start, int len)
    {
        int charCount = CountDecompressedChars(bytes, start, len);
        return string.Create(
            charCount,
            (Bytes: bytes, Start: start, Length: len),
            static (chars, state) => FillDecompressed(state.Bytes, state.Start, state.Length, chars));
    }

    private static string DecompressJet4Slow(ReadOnlySpan<byte> bytes, int start, int len)
    {
        // Two-pass: count output chars first, then fill directly into char[].
        int charCount = CountDecompressedChars(bytes, start, len);
        var chars = new char[charCount];
        FillDecompressed(bytes, start, len, chars);
        return new string(chars);
    }

    private static int CountDecompressedChars(ReadOnlySpan<byte> bytes, int start, int len)
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
                while (i + 1 < end && !(bytes[i] == 0x00 && bytes[i + 1] == 0x00))
                {
                    i += 2;
                }

                count += (i - runStart) / 2;

                if (i + 1 >= end)
                {
                    break;
                }

                compressed = true;
                i += 2;
            }
        }

        return count;
    }

    private static void FillDecompressed(ReadOnlySpan<byte> bytes, int start, int len, Span<char> output)
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
                while (i + 1 < end && !(bytes[i] == 0x00 && bytes[i + 1] == 0x00))
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
                    break;
                }

                compressed = true;
                i += 2;
            }
        }
    }

    // ── File-stream factory ──────────────────────────────────────────

    /// <summary>
    /// Opens a database file with the given access / share / option combination.
    /// Used by both <see cref="AccessReader"/> (read-only sequential) and
    /// <see cref="AccessWriter"/> (read-write random-access).
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <param name="access">The access.</param>
    /// <param name="share">The share.</param>
    /// <param name="options">The options.</param>
    private protected static FileStream OpenDatabaseFileStream(string path, FileAccess access, FileShare share, FileOptions options)
    {
        return FileStreamFactory.Open(path, FileMode.Open, access, share, options);
    }

    // Fixed-column decoding (ReadFixedString / ReadFixedTyped) lives in
    // JetTypeInfo so the per-type byte→value switch sits next to its
    // metadata siblings (GetFixedSize, GetClrType, GetTypeDisplayName).

    // ── Page I/O ─────────────────────────────────────────────────────

    internal async ValueTask<byte[]> ReadPageAsync(long n, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        byte[] buf = ArrayPool<byte>.Shared.Rent(pgSz);
        try
        {
#if NET6_0_OR_GREATER
            if (UsesRandomAccessPageReads && ActiveJournal is null && stream is FileStream fileStream)
            {
                await ReadPageRandomAccessAsync(fileStream, n, buf, cancellationToken).ConfigureAwait(false);
            }
            else
#endif
            {
                await ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    // Inside an explicit transaction, prefer the journal: the page may
                    // be a transaction-local mutation (or an appended page that has no
                    // on-disk slot yet). Journal bytes are plaintext; bypass decrypt.
                    byte[]? journaled = ActiveJournal?.TryGet(n);
                    if (journaled is not null)
                    {
                        Buffer.BlockCopy(journaled, 0, buf, 0, pgSz);
                        return buf;
                    }

                    _ = stream.Seek(n * pgSz, SeekOrigin.Begin);
                    await stream.ReadExactlyAsync(buf.AsMemory(0, pgSz), cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _ = ioGate.Release();
                }
            }

            EncryptionManager.DecryptPageInPlace(buf, n, pgSz, pageKeys);

            return buf;
        }
        catch
        {
            ReturnPage(buf);
            throw;
        }
    }

#if NET6_0_OR_GREATER
    private async ValueTask ReadPageRandomAccessAsync(FileStream fileStream, long pageNumber, byte[] page, CancellationToken cancellationToken)
    {
        long fileOffset = pageNumber * pgSz;
        int totalRead = 0;
        while (totalRead < pgSz)
        {
            int bytesRead = await RandomAccess.ReadAsync(
                fileStream.SafeFileHandle,
                page.AsMemory(totalRead, pgSz - totalRead),
                fileOffset + totalRead,
                cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                throw new EndOfStreamException();
            }

            totalRead += bytesRead;
        }
    }
#endif

    // ── TDEF parsing ─────────────────────────────────────────────────

    /// <summary>
    /// Concatenates the TDEF page chain starting at <paramref name="startPage"/>
    /// into a single byte array. Pages after the first have their 8-byte
    /// TDEF header stripped before appending.
    /// </summary>
    /// <param name="startPage">The start page.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private protected async ValueTask<byte[]?> ReadTDefBytesAsync(long startPage, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var parts = new List<byte[]>();
        var seen = new HashSet<long>();
        long pg = startPage;

        while (pg != 0 && !seen.Contains(pg))
        {
            cancellationToken.ThrowIfCancellationRequested();
            _ = seen.Add(pg);
            byte[] p = await ReadPageAsync(pg, cancellationToken).ConfigureAwait(false);
            if (p[0] != Constants.PageTypes.TableDefinition)
            {
                ReturnPage(p);
                break;
            }

            parts.Add(p);
            pg = Ru32(p, 4);
        }

        if (parts.Count == 0)
        {
            return null;
        }

        if (parts.Count == 1)
        {
            var single = new byte[pgSz];
            Buffer.BlockCopy(parts[0], 0, single, 0, pgSz);
            ReturnPage(parts[0]);
            return single;
        }

        int total = pgSz;
        for (int i = 1; i < parts.Count; i++)
        {
            total += pgSz - 8;
        }

        var result = new byte[total];
        Buffer.BlockCopy(parts[0], 0, result, 0, pgSz);
        int pos = pgSz;
        for (int i = 1; i < parts.Count; i++)
        {
            int len = pgSz - 8;
            Buffer.BlockCopy(parts[i], 8, result, pos, len);
            pos += len;
        }

        for (int i = 0; i < parts.Count; i++)
        {
            ReturnPage(parts[i]);
        }

        return result;
    }

    internal async ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken = default)
    {
        byte[]? td = await ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        if (td == null || td.Length < tdef.BlockEnd)
        {
            return null;
        }

        int numCols = Ru16(td, tdef.NumCols);
        int numRealIdx = Ri32(td, tdef.NumRealIdx);

        // Safety: corrupt or unusual TDEFs can report absurd index counts
        if (numRealIdx < 0 || numRealIdx > Constants.TableDefinition.MaxIndexes)
        {
            numRealIdx = 0;
        }

        if (numCols > Constants.TableDefinition.MaxColumns)
        {
            return null;
        }

        // Column descriptors follow immediately after block + first real-idx entries
        int colStart = tdef.BlockEnd + (numRealIdx * tdef.RealIdxEntrySz);
        int namePos = colStart + (numCols * colDesc.Size);

        if (namePos > td.Length)
        {
            return null;
        }

        var cols = new List<ColumnInfo>(numCols);
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * colDesc.Size);
            if (o + colDesc.Size > td.Length)
            {
                break;
            }

            cols.Add(new ColumnInfo
            {
                Type = td[o + colDesc.TypeOff],
                ColNum = Ru16(td, o + colDesc.NumOff),
                VarIdx = Ru16(td, o + colDesc.VarOff),
                FixedOff = Ru16(td, o + colDesc.FixedOff),
                Size = Ru16(td, o + colDesc.SzOff),
                Flags = td[o + colDesc.FlagsOff],

                // Extra flags byte at descriptor offset 16 (Jet4/ACE only \u2014 the
                // Jet3 18-byte descriptor has no such slot). Carries the Access
                // 2010+ calculated-column marker (Jackcess CALCULATED_EXT_FLAG_MASK
                // = 0xC0). Read unconditionally for Jet4/ACE so calc columns
                // round-trip through the schema-rewrite path; harmless for cols
                // Access wrote with the slot at zero.
                ExtraFlags = format != DatabaseFormat.Jet3Mdb && o + 16 < td.Length ? td[o + 16] : (byte)0,
                Misc = Ri32(td, o + colDesc.MiscOff),

                // For Numeric the misc 4-byte slot reuses bytes 11/12
                // (descriptor-relative) to carry the declared precision and
                // scale Access shows in Design View. Same byte positions as
                // the Jackcess `FixedPointColumnDescriptor` parser. Other
                // column types leave these at 0.
                NumericPrecision = td[o + colDesc.TypeOff] == NumericType ? td[o + colDesc.MiscOff] : (byte)0,
                NumericScale = td[o + colDesc.TypeOff] == NumericType ? td[o + colDesc.MiscOff + 1] : (byte)0,
            });
        }

        // Column names follow directly after all descriptors (in TDEF / descriptor order).
        // Names MUST be read before sorting so each name maps to the correct descriptor.
        for (int i = 0; i < cols.Count; i++)
        {
            int nameLen = ReadColumnName(td, ref namePos, out string name);
            if (nameLen < 0)
            {
                break;
            }

            cols[i].Name = name;
        }

        // Sort by col_num AFTER names are assigned.
        cols.Sort((a, b) => a.ColNum.CompareTo(b.ColNum));

        // Detect deleted-column gaps: if ColNum sequence has gaps, flag it
        bool hasDeletedColumns = cols.Count >= 2
            && cols[cols.Count - 1].ColNum - cols[0].ColNum != cols.Count - 1;

        var tableDef = new TableDef
        {
            Columns = cols,
            RowCount = td.Length >= Constants.TableDefinition.RowCountOffset + sizeof(uint)
                ? Ru32(td, Constants.TableDefinition.RowCountOffset)
                : 0,
            HasDeletedColumns = hasDeletedColumns,
        };
        tableDef.InitializeColumnMetadata();
        return tableDef;
    }

    /// <summary>
    /// Reads the per-row column count from the row header at
    /// <paramref name="rowStart"/>. Jet3 stores it as a single byte; Jet4/ACE
    /// uses a 16-bit little-endian word. Consolidates the format ternary
    /// previously repeated at every row-cracker entry point.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int ReadRowColumnCount(byte[] page, int rowStart)
        => format == DatabaseFormat.Jet3Mdb ? page[rowStart] : Ru16(page, rowStart);

    internal int RowColumnCountFieldSize => rowSz.NumCols;

    /// <summary>
    /// Decodes a text/memo slice using the format-appropriate codec
    /// (Jet4 compressed/UCS-2 or Jet3 ANSI). Empty slices return
    /// <see cref="string.Empty"/>.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal string DecodeTextForFormat(byte[] bytes, int start, int len)
    {
        if (len <= 0)
        {
            return string.Empty;
        }

        return format == DatabaseFormat.Jet3Mdb ? ansiEncoding.GetString(bytes, start, len) : DecodeJet4Text(bytes, start, len);
    }

    /// <summary>
    /// Encodes a string for storage using the format-appropriate codec
    /// (Jet4 with optional compression vs Jet3 ANSI code-page bytes).
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="compress">The compress.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeTextForFormat(string value, bool compress = true)
        => format == DatabaseFormat.Jet3Mdb ? ansiEncoding.GetBytes(value) : EncodeJet4Text(value, compress);

    /// <summary>
    /// Encodes a string for storage using the format-appropriate codec,
    /// truncating the Jet4 path to at most <paramref name="maxBytes"/> output bytes.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="maxBytes">The max bytes.</param>
    /// <param name="compress">The compress.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeTextForFormat(string value, int maxBytes, bool compress = true)
        => format == DatabaseFormat.Jet3Mdb ? ansiEncoding.GetBytes(value) : EncodeJet4Text(value, maxBytes, compress);

    /// <summary>
    /// Throws <see cref="ObjectDisposedException"/> when this instance has been
    /// disposed. Wraps <see cref="Guard.ThrowIfDisposed(bool, object)"/> with
    /// the common <c>(_disposed, this)</c> arguments.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ThrowIfDisposed() => Guard.ThrowIfDisposed(disposed, this);

    /// <summary>
    /// Combined disposed-and-cancelled guard. Mirrors the call-site pattern
    /// <c>ThrowIfDisposed(); cancellationToken.ThrowIfCancellationRequested();</c>
    /// that opens nearly every public writer entry point.
    /// </summary>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
    {
        Guard.ThrowIfDisposed(disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Reads a single column name from the TDEF byte array at <paramref name="pos"/>,
    /// advancing <paramref name="pos"/> past the name bytes.
    /// Returns the byte length consumed, or -1 if the name extends beyond <paramref name="td"/>.
    /// </summary>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="pos">The byte position.</param>
    /// <param name="name">The name.</param>
    internal int ReadColumnName(byte[] td, ref int pos, out string name)
    {
        name = string.Empty;
        if (pos >= td.Length)
        {
            return -1;
        }

        if (format != DatabaseFormat.Jet3Mdb)
        {
            if (pos + 2 > td.Length)
            {
                return -1;
            }

            int len = Ru16(td, pos);
            pos += 2;
            if (pos + len > td.Length)
            {
                return -1;
            }

            name = JetTypeInfo.DecodeUtf16LE(td.AsSpan(pos, len));
            pos += len;
            return len + 2;
        }
        else
        {
            int len = td[pos++];
            if (pos + len > td.Length)
            {
                return -1;
            }

            name = ansiEncoding.GetString(td, pos, len);
            pos += len;
            return len + 1;
        }
    }

    // ── Page write I/O ───────────────────────────────────────────────

    /// <summary>
    /// Returns <paramref name="page"/> unchanged when no page-encryption is
    /// active, or a freshly allocated, encrypted copy otherwise. The caller's
    /// buffer is never mutated so it can be reused safely after writing.
    /// Page 0 (the unencrypted header) is always returned as-is.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    private protected byte[] PrepareEncryptedPageForWrite(long pageNumber, byte[] page)
    {
        if (pageNumber < 1 || !EncryptionManager.HasPageEncryption(pageKeys))
        {
            return page;
        }

        var copy = new byte[pgSz];
        Buffer.BlockCopy(page, 0, copy, 0, pgSz);
        EncryptionManager.EncryptPageInPlace(copy, pageNumber, pgSz, pageKeys);
        return copy;
    }

    internal async ValueTask WritePageAsync(long pageNumber, byte[] page, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (ActiveJournal is { } journal)
            {
                journal.Write(pageNumber, page.AsSpan(0, pgSz));
                return;
            }

            byte[] toWrite = PrepareEncryptedPageForWrite(pageNumber, page);
            var pageLock = await byteRangeLock.AcquirePageLockAsync(pageNumber, pgSz, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = stream.Seek(pageNumber * pgSz, SeekOrigin.Begin);
                await stream.WriteAsync(toWrite.AsMemory(0, pgSz), cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = ioGate.Release();
        }
    }

    internal async ValueTask<long> AppendPageAsync(byte[] page, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (ActiveJournal is { } journal)
            {
                return journal.Append(page.AsSpan(0, pgSz));
            }

            long pageNumber = stream.Length / pgSz;
            byte[] toWrite = PrepareEncryptedPageForWrite(pageNumber, page);
            var pageLock = await byteRangeLock.AcquirePageLockAsync(pageNumber, pgSz, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = stream.Seek(pageNumber * pgSz, SeekOrigin.Begin);
                await stream.WriteAsync(toWrite.AsMemory(0, pgSz), cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                return pageNumber;
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = ioGate.Release();
        }
    }

    // ── Catalog access ───────────────────────────────────────────────

    /// <summary>Finds a catalog entry by name (case-insensitive).</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async ValueTask<CatalogEntry?> GetCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var userTables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return userTables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private protected abstract ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken = default);

    // ── Table page enumeration ───────────────────────────────────────

    /// <summary>
    /// Yields the bounds (row index, start offset, size) of every live (non-deleted, non-overflow)
    /// row on the given data <paramref name="page"/>.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    internal IEnumerable<RowBound> EnumerateLiveRowBounds(byte[] page)
    {
        int numRows = Ru16(page, dataPage.NumRows);
        if (numRows == 0)
        {
            yield break;
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - dataPage.RowsStart) / 2;
        if (numRows > maxPossibleRows)
        {
            numRows = maxPossibleRows;
        }

        if (numRows <= 0)
        {
            yield break;
        }

        var rawOffsets = new int[numRows];
        for (int r = 0; r < numRows; r++)
        {
            rawOffsets[r] = Ru16(page, dataPage.RowsStart + (r * 2));
        }

        var positions = new int[numRows];
        int posCount = 0;
        for (int r = 0; r < numRows; r++)
        {
            int pos = rawOffsets[r] & Constants.DataPage.RowOffsetMask;
            if (pos > 0 && pos < pgSz)
            {
                positions[posCount++] = pos;
            }
        }

        Array.Sort(positions, 0, posCount);

        for (int r = 0; r < numRows; r++)
        {
            int raw = rawOffsets[r];
            if ((raw & Constants.DataPage.NonLiveRowFlags) != 0)
            {
                continue;
            }

            int rowStart = raw & Constants.DataPage.RowOffsetMask;
            int rowEnd = pgSz - 1;
            int searchIdx = Array.BinarySearch(positions, 0, posCount, rowStart);
            int nextIdx = searchIdx >= 0 ? searchIdx + 1 : ~searchIdx;
            if (nextIdx < posCount)
            {
                rowEnd = positions[nextIdx] - 1;
            }

            yield return new RowBound(r, rowStart, rowEnd - rowStart + 1);
        }
    }

    /// <summary>
    /// Eager array form of <see cref="EnumerateLiveRowBounds"/>. Allocates a
    /// single <see cref="RowBound"/>[] (or <see cref="Array.Empty{T}"/> when the
    /// page has no live rows) instead of returning an iterator. Suitable as a
    /// memoization target for <see cref="AccessReader"/>'s page cache,
    /// where the same page may be visited by multiple
    /// streaming consumers.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    private protected RowBound[] ComputeLiveRowBoundsArray(byte[] page)
    {
        int numRows = Ru16(page, dataPage.NumRows);
        if (numRows == 0)
        {
            return Array.Empty<RowBound>();
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - dataPage.RowsStart) / 2;
        if (numRows > maxPossibleRows)
        {
            numRows = maxPossibleRows;
        }

        if (numRows <= 0)
        {
            return [];
        }

        var rawOffsets = new int[numRows];
        var positions = new int[numRows];

        int posCount = 0;
        int liveCount = 0;
        for (int r = 0; r < numRows; r++)
        {
            int raw = Ru16(page, dataPage.RowsStart + (r * 2));
            rawOffsets[r] = raw;

            int pos = raw & Constants.DataPage.RowOffsetMask;
            if (pos > 0 && pos < pgSz)
            {
                positions[posCount++] = pos;
            }

            if ((raw & Constants.DataPage.NonLiveRowFlags) == 0)
            {
                liveCount++;
            }
        }

        if (liveCount == 0)
        {
            return Array.Empty<RowBound>();
        }

        Array.Sort(positions, 0, posCount);

        var result = new RowBound[liveCount];
        int idx = 0;
        for (int r = 0; r < numRows; r++)
        {
            int raw = rawOffsets[r];
            if ((raw & Constants.DataPage.NonLiveRowFlags) != 0)
            {
                continue;
            }

            int rowStart = raw & Constants.DataPage.RowOffsetMask;
            int rowEnd = pgSz - 1;
            int searchIdx = Array.BinarySearch(positions, 0, posCount, rowStart);
            int nextIdx = searchIdx >= 0 ? searchIdx + 1 : ~searchIdx;
            if (nextIdx < posCount)
            {
                rowEnd = positions[nextIdx] - 1;
            }

            result[idx++] = new RowBound(r, rowStart, rowEnd - rowStart + 1);
        }

        return result;
    }

    // ── Row layout decoding (shared by AccessReader.CrackRowAsync and AccessWriter.ReadColumnValue) ────

    /// <summary>
    /// Parses the row-trailer metadata (numCols, null-mask position, var-table
    /// position and EOD pointer) for a row at <paramref name="rowStart"/>.
    /// Returns <see langword="false"/> when the row is too small or otherwise
    /// malformed; on success <paramref name="layout"/> is populated and can be
    /// passed to <see cref="ResolveColumnSlice"/> for any column.
    /// </summary>
    /// <param name="page">Data page containing the row.</param>
    /// <param name="rowStart">Offset of the row within <paramref name="page"/>.</param>
    /// <param name="rowSize">Total size of the row in bytes.</param>
    /// <param name="hasVarColumns">When <see langword="false"/>, the var-length
    /// metadata is assumed to be omitted entirely (no varLen byte, no jump
    /// bytes, no var-offset table, no EOD marker) — which is how Jet lays out
    /// rows for tables with zero variable-length columns.</param>
    /// <param name="layout">Receives the parsed layout on success.</param>
    private protected bool TryParseRowLayout(ReadOnlySpan<byte> page, int rowStart, int rowSize, bool hasVarColumns, out RowLayout layout)
    {
        layout = default;
        if (rowSize < rowSz.NumCols)
        {
            return false;
        }

        int numCols = rowSz.ReadNumCols(page, rowStart);
        if (numCols == 0)
        {
            return false;
        }

        int nullMaskSz = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSz;
        if (nullMaskPos < rowSz.NumCols)
        {
            return false;
        }

        int varLen;
        int varTableStart;
        int eod;
        if (!hasVarColumns)
        {
            varLen = 0;
            varTableStart = nullMaskPos;
            eod = nullMaskPos;
        }
        else
        {
            int varLenPos = nullMaskPos - rowSz.VarLen;
            if (varLenPos < rowSz.NumCols)
            {
                return false;
            }

            varLen = rowSz.ReadVarLen(page, rowStart + varLenPos);
            int jumpSz = format != DatabaseFormat.Jet3Mdb ? 0 : (rowSize / 256);
            varTableStart = varLenPos - jumpSz - (varLen * rowSz.VarEntry);
            int eodPos = varTableStart - rowSz.Eod;
            if (eodPos < rowSz.NumCols)
            {
                return false;
            }

            eod = rowSz.ReadEod(page, rowStart + eodPos);
        }

        layout = new RowLayout(numCols, nullMaskPos, varLen, varTableStart, eod);
        return true;
    }

    /// <summary>
    /// Resolves the per-column data slice (or null/bool/empty marker) for
    /// <paramref name="col"/> within a row whose layout has been parsed by
    /// <see cref="TryParseRowLayout"/>.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="col">The column descriptor.</param>
    private protected ColumnSlice ResolveColumnSlice(ReadOnlySpan<byte> page, int rowStart, int rowSize, in RowLayout layout, ColumnInfo col)
    {
        bool nullBit = false;
        if (col.ColNum < layout.NumCols)
        {
            int mByte = layout.NullMaskPos + (col.ColNum / 8);
            int mBit = col.ColNum % 8;
            if (mByte < rowSize)
            {
                nullBit = (page[rowStart + mByte] & (1 << mBit)) != 0;
            }
        }

        if (col.Type == BooleanType && !col.IsCalculated)
        {
            return new ColumnSlice(ColumnSliceKind.Bool, 0, 0, nullBit);
        }

        if (col.ColNum >= layout.NumCols || !nullBit)
        {
            return new ColumnSlice(ColumnSliceKind.Null, 0, 0, false);
        }

        if (col.IsFixed)
        {
            int start = rowSz.NumCols + col.FixedOff;
            int sz = col.IsCalculated ? col.Size : JetTypeInfo.GetFixedSize(col.Type);
            if (sz == 0 || start + sz > rowSize)
            {
                return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
            }

            return new ColumnSlice(col.IsCalculated ? ColumnSliceKind.Var : ColumnSliceKind.Fixed, start, sz, false);
        }

        if (col.VarIdx >= layout.VarLen)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        int entryPos = layout.VarTableStart + ((layout.VarLen - 1 - col.VarIdx) * rowSz.VarEntry);
        if (entryPos < 0 || entryPos + rowSz.VarEntry > rowSize)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        int varOff = rowSz.ReadVarEntry(page, rowStart + entryPos);

        int varEnd;
        if (col.VarIdx + 1 < layout.VarLen)
        {
            int nextEntry = layout.VarTableStart + ((layout.VarLen - 2 - col.VarIdx) * rowSz.VarEntry);
            varEnd = rowSz.ReadVarEntry(page, rowStart + nextEntry);
        }
        else
        {
            varEnd = layout.Eod;
        }

        int dataStart = varOff;
        int dataLen = varEnd - varOff;
        if (dataLen < 0 || dataStart < 0 || dataStart + dataLen > rowSize)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        return new ColumnSlice(ColumnSliceKind.Var, dataStart, dataLen, false);
    }

    internal bool TryParseRowLayoutForDecodePlan(ReadOnlySpan<byte> page, int rowStart, int rowSize, bool hasVarColumns, out RowLayout layout)
        => TryParseRowLayout(page, rowStart, rowSize, hasVarColumns, out layout);

    internal ColumnSlice ResolveColumnSliceForDecodePlan(ReadOnlySpan<byte> page, int rowStart, int rowSize, in RowLayout layout, ColumnInfo column)
        => ResolveColumnSlice(page, rowStart, rowSize, layout, column);

    /// <summary>
    /// Yields <see cref="RowLocation"/>s (row index + start/size) for every live, non-overflow
    /// row on <paramref name="page"/>, paired with <paramref name="pageNumber"/>. A thin wrapper
    /// over <see cref="EnumerateLiveRowBounds(byte[])"/> for callers that need to round-trip
    /// the originating page number (update / delete paths).
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    internal IEnumerable<RowLocation> EnumerateLiveRowLocations(long pageNumber, byte[] page)
    {
        foreach (var rb in EnumerateLiveRowBounds(page))
        {
            yield return new RowLocation(pageNumber, rb.RowIndex, rb.RowStart, rb.RowSize);
        }
    }

    /// <summary>
    /// Reads a single column value as a string, supporting bool, fixed-width and inline-var
    /// (Text / Binary) columns. Variable-width MEMO / OLE / Complex columns are NOT
    /// followed (they require LVAL chain traversal); those return <see cref="string.Empty"/>
    /// here. Used by writer-side catalog walks that only need scalar metadata columns.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="column">The column.</param>
    internal string DecodeSimpleColumnValue(byte[] page, int rowStart, int rowSize, ColumnInfo column)
    {
        if (column == null || rowSize < rowSz.NumCols)
        {
            return string.Empty;
        }

        if (!TryParseRowLayout(page, rowStart, rowSize, hasVarColumns: true, out var layout))
        {
            return string.Empty;
        }

        var slice = ResolveColumnSlice(page, rowStart, rowSize, layout, column);
        switch (slice.Kind)
        {
            case ColumnSliceKind.Bool:
                return slice.BoolValue ? "True" : "False";

            case ColumnSliceKind.Null:
            case ColumnSliceKind.Empty:
                return string.Empty;

            case ColumnSliceKind.Fixed:
                return JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, column, slice.DataLen);

            case ColumnSliceKind.Var:
                if (slice.DataLen <= 0)
                {
                    return string.Empty;
                }

                switch (column.Type)
                {
                    case TextType:
                        return DecodeTextForFormat(page, rowStart + slice.DataStart, slice.DataLen);
                    case BinaryType:
                        return JetTypeInfo.ToHexStringNoSeparator(page.AsSpan(rowStart + slice.DataStart, slice.DataLen));
                    case ByteType:
                    case IntegerType:
                    case LongIntegerType:
                    case FloatType:
                    case DoubleType:
                    case DateTimeType:
                    case MoneyType:
                    case GuidType:
                    case ComplexType:
                    case AttachmentType:
                        int required = column.Type is ComplexType or AttachmentType ? 4 : JetTypeInfo.GetFixedSize(column.Type);
                        return required > 0 && slice.DataLen >= required
                            ? JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, column, required)
                            : string.Empty;
                    default:
                        return string.Empty;
                }

            default:
                return string.Empty;
        }
    }

    // ── Catalog cache ────────────────────────────────────────────────
    // Each cache is a single reference; volatile-write of a fully-built list is atomic
    // in .NET, so a lock is unnecessary (subsequent readers see either the old or the
    // new list, never a torn value).

    /// <summary>Returns the cached catalog list, or <see langword="null"/> if not yet populated.</summary>
    private protected List<CatalogEntry>? GetCatalogCache() => catalogCache;

    /// <summary>Stores the catalog list returned by <see cref="GetUserTablesAsync"/>.</summary>
    /// <param name="cache">The cache.</param>
    private protected void SetCatalogCache(List<CatalogEntry> cache) => catalogCache = cache;

    /// <summary>Returns the cached linked-table list, or <see langword="null"/> if not yet populated.</summary>
    private protected List<LinkedTableInfo>? GetLinkedTableCache() => linkedTableCache;

    /// <summary>Stores the linked-table list returned by the MSysObjects linked-table scan.</summary>
    /// <param name="cache">The cache.</param>
    private protected void SetLinkedTableCache(List<LinkedTableInfo> cache) => linkedTableCache = cache;

    /// <summary>Discards the cached catalog lists so the next call re-scans MSysObjects.</summary>
    internal void InvalidateCatalogCache()
    {
        catalogCache = null;
        linkedTableCache = null;
    }

    // ── Inner types ──────────────────────────────────────────────────

    internal readonly record struct RowBound(int RowIndex, int RowStart, int RowSize);

    /// <summary>Parsed row-trailer metadata — see <see cref="TryParseRowLayout"/>.</summary>
    /// <param name="NumCols">The number of cols.</param>
    /// <param name="NullMaskPos">The null mask pos.</param>
    /// <param name="VarLen">The var len.</param>
    /// <param name="VarTableStart">The var table start.</param>
    /// <param name="Eod">The end-of-data marker size.</param>
    internal readonly record struct RowLayout(
        int NumCols,
        int NullMaskPos,
        int VarLen,
        int VarTableStart,
        int Eod);

    /// <summary>Classification returned by <see cref="ResolveColumnSlice"/>.</summary>
    internal enum ColumnSliceKind
    {
        /// <summary>Column is missing/empty/out-of-bounds — caller should emit empty/default.</summary>
        Empty,

        /// <summary>Column is null (null-mask bit unset, or column index ≥ row's numCols).</summary>
        Null,

        /// <summary>Boolean column: <see cref="ColumnSlice.BoolValue"/> holds the null-mask bit.</summary>
        Bool,

        /// <summary>Fixed-width column: <see cref="ColumnSlice.DataStart"/>/<see cref="ColumnSlice.DataLen"/>
        /// are valid (relative to the row start).</summary>
        Fixed,

        /// <summary>Variable-width column: <see cref="ColumnSlice.DataStart"/>/<see cref="ColumnSlice.DataLen"/>
        /// are valid (relative to the row start); <c>DataLen</c> may be 0.</summary>
        Var,
    }

    /// <summary>Per-column slice produced by <see cref="ResolveColumnSlice"/>.</summary>
    /// <param name="Kind">The table name kind.</param>
    /// <param name="DataStart">The data start.</param>
    /// <param name="DataLen">The data len.</param>
    /// <param name="BoolValue">The bool value.</param>
    internal readonly record struct ColumnSlice(
        ColumnSliceKind Kind,
        int DataStart,
        int DataLen,
        bool BoolValue);
}
