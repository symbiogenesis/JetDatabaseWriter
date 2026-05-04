namespace JetDatabaseWriter;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using static JetDatabaseWriter.Constants.ColumnTypes;

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
    internal readonly DataPageLayout _dataPage;

    /// <summary>Per-format byte offsets within a TDEF block plus real-idx entry size — see <see cref="TDefHeaderLayout"/>.</summary>
    internal readonly TDefHeaderLayout _tdef;

    /// <summary>Per-format byte offsets within one column descriptor — see <see cref="ColumnDescriptorLayout"/>.</summary>
    internal readonly ColumnDescriptorLayout _colDesc;

    /// <summary>Per-format byte sizes of the in-row trailer fields — see <see cref="RowFieldSizes"/>.</summary>
    internal readonly RowFieldSizes _rowSz;

    /// <summary>
    /// Per-format byte offsets and entry sizes for the TDEF page's real-idx
    /// physical descriptor (§3.1) and logical-idx entry (§3.2) sections.
    /// </summary>
    internal readonly IndexLayout _indexLayout;

    internal readonly int _pgSz;
    internal readonly DatabaseFormat _format;
    internal readonly Stream _stream;
    private protected readonly Encoding _ansiEncoding;
    private protected readonly int _codePage;
    private protected readonly string _path;

    internal Encoding AnsiEncoding => _ansiEncoding;

    /// <summary>
    /// Per-page decryption keys (Jet3 XOR, Jet4 RC4, ACCDB AES). Populated during
    /// reader construction by <see cref="EncryptionManager"/>. Mutated only on the
    /// constructor thread; consulted by every page read via
    /// <see cref="EncryptionManager.DecryptPageInPlace(byte[], long, int, EncryptionManager.PageDecryptionKeys)"/>.
    /// </summary>
    private protected readonly EncryptionManager.PageDecryptionKeys _pageKeys = new();

    internal bool _disposed;
    private readonly SemaphoreSlim _ioGate = new(1, 1);
    private volatile List<CatalogEntry>? _catalogCache;

    /// <summary>
    /// Cooperative JET byte-range lock helper (Win32 <c>LockFileEx</c>). Defaults to
    /// <see cref="JetByteRangeLock.Disabled"/> so page-write paths can dispatch
    /// without a null check; <see cref="AccessReader"/> / <see cref="AccessWriter"/>
    /// replace it with a stream-bound instance once options are known.
    /// </summary>
    private protected JetByteRangeLock _byteRangeLock = JetByteRangeLock.Disabled;

    /// <summary>
    /// Gets or sets the in-memory page journal for an explicit <see cref="JetTransaction"/>.
    /// When non-null, page writes/appends are buffered into the journal
    /// instead of being flushed to the underlying stream, and page reads
    /// consult the journal first so the transaction sees its own writes.
    /// Set/cleared exclusively by <see cref="AccessWriter"/> while holding
    /// <see cref="_ioGate"/>.
    /// </summary>
    internal PageJournal? ActiveJournal { get; set; }

    /// <summary>Gets the writer's internal I/O gate so derived types may serialise transaction commit / rollback.</summary>
    internal SemaphoreSlim IoGate => _ioGate;

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
    private protected AccessBase(Stream stream, byte[] hdr, string path = "")
    {
        _stream = stream;
        _path = path ?? string.Empty;

        _format = EncryptionConverter.DetectFormat(hdr);
        _pgSz = GetPageSize(_format);

        _pageKeys.Jet3XorMask = EncryptionManager.GetJet3PageMask(_format, hdr);

        // Codepage / sort order: stored as a UInt16 at hdr[0x3C], scrambled by
        // the constant-key RC4 stream Microsoft Access applies to header bytes
        // [0x18 .. 0x18+126/128]. EncryptionManager.DecodeHeaderCodePage handles
        // the descrambling so we recover the real codepage (e.g. 1252) instead
        // of a corrupted byte. ACE / ACCDB stores text as UTF-16 in user data
        // so the codepage there is largely cosmetic, but Jet3 .mdb files (and
        // Jet4 catalog names) need it correct to round-trip non-ASCII names.
        _codePage = EncryptionManager.DecodeHeaderCodePage(hdr, _format);
        if (_codePage <= 0)
        {
            _codePage = 1252;  // default to Windows-1252 if unknown
        }

        try
        {
            _ansiEncoding = Encoding.GetEncoding(_codePage);
        }
        catch (ArgumentException)
        {
            _ansiEncoding = Encoding.UTF8;
            _codePage = 65001;
        }
        catch (NotSupportedException)
        {
            _ansiEncoding = Encoding.UTF8;
            _codePage = 65001;
        }

        // Format-specific TDEF / page / column / row layouts:
        //   Jet4 / ACE (Access 2000–2019): TDEF 8+55 = 63 bytes, column descriptor 25 bytes.
        //   Jet3        (Access 97):       TDEF 8+35 = 43 bytes, column descriptor 18 bytes.
        _dataPage = DataPageLayout.For(_format);
        _tdef = TDefHeaderLayout.For(_format);
        _colDesc = ColumnDescriptorLayout.For(_format);
        _rowSz = RowFieldSizes.For(_format);
        _indexLayout = IndexLayout.For(_format);
    }

    /// <inheritdoc/>
    public DatabaseFormat DatabaseFormat => _format;

    /// <inheritdoc/>
    public int PageSize => _pgSz;

    /// <inheritdoc/>
    public int CodePage => _codePage;

    /// <inheritdoc/>
    public virtual async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _stream.DisposeAsync().ConfigureAwait(false);
        _ioGate.Dispose();
        _pageKeys.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>Returns the page size in bytes for the given database format (2048 for Jet3, 4096 for Jet4/ACE).</summary>
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

        int read = 0;
        while (read < hdr.Length)
        {
            int got = await fs.ReadAsync(hdr.AsMemory(read, hdr.Length - read), cancellationToken).ConfigureAwait(false);
            if (got == 0)
            {
                break;
            }

            read += got;
        }

        return hdr;
    }

    // ── Static helpers ────────────────────────────────────────────────
    internal static void ReturnPage(byte[] page)
    {
        ArrayPool<byte>.Shared.Return(page);
    }

    internal static ushort Ru16(byte[] b, int o) =>
        BinaryPrimitives.ReadUInt16LittleEndian(b.AsSpan(o, 2));

    internal static ushort Ru16(ReadOnlySpan<byte> b, int o) =>
        BinaryPrimitives.ReadUInt16LittleEndian(b.Slice(o, 2));

    internal static int Ri32(byte[] b, int o) =>
        BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(o, 4));

    internal static uint Ru32(byte[] b, int o) =>
        BinaryPrimitives.ReadUInt32LittleEndian(b.AsSpan(o, 4));

    internal static void Wu16(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(o, 2), (ushort)value);

    internal static void Wi32(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(o, 4), value);

    // Pure byte-decoding helpers (ReadUInt24LittleEndian / ReadUInt24BigEndian /
    // ReadSingleLittleEndian / ReadDoubleLittleEndian / ToHexStringNoSeparator)
    // live in JetTypeInfo so the per-type byte→value switches don't take an
    // upward dependency on Core.

    private protected static void WriteUInt24(byte[] b, int o, int value)
    {
        Span<byte> span = b.AsSpan(o, 3);
        BinaryPrimitives.WriteUInt16LittleEndian(span, (ushort)(value & 0xFFFF));
        span[2] = (byte)((value >> 16) & 0xFF);
    }

    private protected static void WriteField(byte[] b, int o, int fieldSize, int value)
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
    /// which the reader decodes via <see cref="DecompressJet4"/>.
    /// Otherwise emits plain UCS-2 LE.
    /// </summary>
    /// <remarks>
    /// The "no NUL" restriction (chars must be > U+0000) avoids ambiguity
    /// with the compressed-mode toggle byte (<c>0x00</c>). The compressed
    /// form is only chosen when it actually saves bytes (length &gt;= 3
    /// characters), so 1- and 2-character strings are still written as
    /// plain UCS-2 to avoid the 2-byte marker overhead.
    /// </remarks>
    private protected static byte[] EncodeJet4Text(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return [];
        }

        bool compressible = value.Length >= 3;
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
            return Encoding.Unicode.GetBytes(value);
        }

        byte[] result = new byte[value.Length + 2];
        result[0] = 0xFF;
        result[1] = 0xFE;
        for (int i = 0; i < value.Length; i++)
        {
            result[i + 2] = (byte)value[i];
        }

        return result;
    }

    /// <summary>
    /// Decodes Jet4 text (UCS-2 / UTF-16LE).
    /// If data starts with the compressed-unicode marker 0xFF 0xFE, the
    /// JET4 compressed-string algorithm is applied first.
    /// </summary>
    /// <returns>The decoded string.</returns>
    private protected static string DecodeJet4Text(ReadOnlySpan<byte> b, int start, int len)
    {
        if (len < 2)
        {
            return string.Empty;
        }

        if (b[start] == 0xFF && b[start + 1] == 0xFE)
        {
            return DecompressJet4(b, start + 2, len - 2);
        }

        // Plain UCS-2 LE — length must be even
        int evenLen = len & ~1;
        return evenLen > 0 ? JetTypeInfo.DecodeUtf16LE(b.Slice(start, evenLen)) : string.Empty;
    }

    /// <summary>
    /// Decodes the JET4 "compressed unicode" encoding.
    /// A 0x00 byte toggles between 1-byte compressed (ASCII) and 2-byte
    /// uncompressed (UCS-2) mode.
    /// </summary>
    /// <returns>The decompressed string.</returns>
    private protected static string DecompressJet4(ReadOnlySpan<byte> b, int start, int len)
    {
        var sb = new StringBuilder(len);
        bool compressed = true;
        int i = start, end = start + len;

        while (i < end)
        {
            if (compressed)
            {
                if (b[i] == 0x00)
                {
                    compressed = false;
                    i++;
                    continue;
                }

                _ = sb.Append((char)b[i++]);
            }
            else
            {
                // Find the end of the uncompressed run: either a 0x00 0x00
                // terminator at a pair boundary, or the end of the buffer.
                int runStart = i;
                while (i + 1 < end && !(b[i] == 0x00 && b[i + 1] == 0x00))
                {
                    i += 2;
                }

                int runLen = i - runStart;
                if (runLen > 0)
                {
                    JetTypeInfo.AppendUtf16LE(sb, b.Slice(runStart, runLen));
                }

                if (i + 1 >= end)
                {
                    break;
                }

                // Consume the 0x00 0x00 terminator and switch back to compressed mode.
                compressed = true;
                i += 2;
            }
        }

        return sb.ToString();
    }

    // ── File-stream factory ──────────────────────────────────────────

    /// <summary>
    /// Opens a database file with the given access / share / option combination.
    /// Used by both <see cref="AccessReader"/> (read-only sequential) and
    /// <see cref="AccessWriter"/> (read-write random-access).
    /// </summary>
    private protected static FileStream OpenDatabaseFileStream(string path, FileAccess access, FileShare share, FileOptions options)
    {
        return new FileStream(path, FileMode.Open, access, share, 4096, options);
    }

    // Fixed-column decoding (ReadFixedString / ReadFixedTyped) lives in
    // JetTypeInfo so the per-type byte→value switch sits next to its
    // metadata siblings (GetFixedSize, GetClrType, GetTypeDisplayName).

    // ── Page I/O ─────────────────────────────────────────────────────

    internal async ValueTask<byte[]> ReadPageAsync(long n, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var buf = ArrayPool<byte>.Shared.Rent(_pgSz);
        await _ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Inside an explicit transaction, prefer the journal: the page may
            // be a transaction-local mutation (or an appended page that has no
            // on-disk slot yet). Journal bytes are plaintext; bypass decrypt.
            byte[]? journaled = ActiveJournal?.TryGet(n);
            if (journaled is not null)
            {
                Buffer.BlockCopy(journaled, 0, buf, 0, _pgSz);
                return buf;
            }

            _ = _stream.Seek(n * _pgSz, SeekOrigin.Begin);

            int read = 0;
            while (read < _pgSz)
            {
                int got = await _stream.ReadAsync(buf.AsMemory(read, _pgSz - read), cancellationToken).ConfigureAwait(false);
                if (got == 0)
                {
                    break;
                }

                read += got;
            }
        }
        finally
        {
            _ = _ioGate.Release();
        }

        EncryptionManager.DecryptPageInPlace(buf, n, _pgSz, _pageKeys);

        return buf;
    }

    // ── TDEF parsing ─────────────────────────────────────────────────

    /// <summary>
    /// Concatenates the TDEF page chain starting at <paramref name="startPage"/>
    /// into a single byte array. Pages after the first have their 8-byte
    /// TDEF header stripped before appending.
    /// </summary>
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
            if (p[0] != 0x02)
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
            var single = new byte[_pgSz];
            Buffer.BlockCopy(parts[0], 0, single, 0, _pgSz);
            ReturnPage(parts[0]);
            return single;
        }

        int total = _pgSz;
        for (int i = 1; i < parts.Count; i++)
        {
            total += _pgSz - 8;
        }

        var result = new byte[total];
        Buffer.BlockCopy(parts[0], 0, result, 0, _pgSz);
        int pos = _pgSz;
        for (int i = 1; i < parts.Count; i++)
        {
            int len = _pgSz - 8;
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

        if (td == null || td.Length < _tdef.BlockEnd)
        {
            return null;
        }

        int numCols = Ru16(td, _tdef.NumCols);
        int numRealIdx = Ri32(td, _tdef.NumRealIdx);

        // Safety: corrupt or unusual TDEFs can report absurd index counts
        if (numRealIdx < 0 || numRealIdx > 1000)
        {
            numRealIdx = 0;
        }

        if (numCols > 4096)
        {
            return null;
        }

        // Column descriptors follow immediately after block + first real-idx entries
        int colStart = _tdef.BlockEnd + (numRealIdx * _tdef.RealIdxEntrySz);
        int namePos = colStart + (numCols * _colDesc.Size);

        if (namePos > td.Length)
        {
            return null;
        }

        var cols = new List<ColumnInfo>(numCols);
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * _colDesc.Size);
            if (o + _colDesc.Size > td.Length)
            {
                break;
            }

            cols.Add(new ColumnInfo
            {
                Type = td[o + _colDesc.TypeOff],
                ColNum = Ru16(td, o + _colDesc.NumOff),
                VarIdx = Ru16(td, o + _colDesc.VarOff),
                FixedOff = Ru16(td, o + _colDesc.FixedOff),
                Size = Ru16(td, o + _colDesc.SzOff),
                Flags = td[o + _colDesc.FlagsOff],

                // Extra flags byte at descriptor offset 16 (Jet4/ACE only \u2014 the
                // Jet3 18-byte descriptor has no such slot). Carries the Access
                // 2010+ calculated-column marker (Jackcess CALCULATED_EXT_FLAG_MASK
                // = 0xC0). Read unconditionally for Jet4/ACE so calc columns
                // round-trip through the schema-rewrite path; harmless for cols
                // Access wrote with the slot at zero.
                ExtraFlags = _format != DatabaseFormat.Jet3Mdb && o + 16 < td.Length ? td[o + 16] : (byte)0,
                Misc = Ri32(td, o + _colDesc.MiscOff),

                // For T_NUMERIC the misc 4-byte slot reuses bytes 11/12
                // (descriptor-relative) to carry the declared precision and
                // scale Access shows in Design View. Same byte positions as
                // the Jackcess `FixedPointColumnDescriptor` parser. Other
                // column types leave these at 0.
                NumericPrecision = td[o + _colDesc.TypeOff] == /* T_NUMERIC */ 0x10 ? td[o + _colDesc.MiscOff] : (byte)0,
                NumericScale = td[o + _colDesc.TypeOff] == /* T_NUMERIC */ 0x10 ? td[o + _colDesc.MiscOff + 1] : (byte)0,
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
            RowCount = td.Length > 20 ? Ru32(td, 16) : 0,
            HasDeletedColumns = hasDeletedColumns,
        };
        tableDef.InitializeColumnMetadata();
        return tableDef;
    }

    /// <summary>
    /// Reads a single column name from the TDEF byte array at <paramref name="pos"/>,
    /// advancing <paramref name="pos"/> past the name bytes.
    /// Returns the byte length consumed, or -1 if the name extends beyond <paramref name="td"/>.
    /// </summary>
    internal int ReadColumnName(byte[] td, ref int pos, out string name)
    {
        name = string.Empty;
        if (pos >= td.Length)
        {
            return -1;
        }

        if (_format != DatabaseFormat.Jet3Mdb)
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

            name = _ansiEncoding.GetString(td, pos, len);
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
    private protected byte[] PrepareEncryptedPageForWrite(long pageNumber, byte[] page)
    {
        if (pageNumber < 1 || !EncryptionManager.HasPageEncryption(_pageKeys))
        {
            return page;
        }

        var copy = new byte[_pgSz];
        Buffer.BlockCopy(page, 0, copy, 0, _pgSz);
        EncryptionManager.EncryptPageInPlace(copy, pageNumber, _pgSz, _pageKeys);
        return copy;
    }

    private protected void WritePage(long pageNumber, byte[] page)
    {
        _ioGate.Wait();
        try
        {
            // Buffer into the active transaction journal (plaintext) instead
            // of touching disk. Encryption + locks are applied at commit time.
            if (ActiveJournal is { } journal)
            {
                journal.Write(pageNumber, page.AsSpan(0, _pgSz));
                return;
            }

            byte[] toWrite = PrepareEncryptedPageForWrite(pageNumber, page);
            using IDisposable pageLock = _byteRangeLock.AcquirePageLock(pageNumber, _pgSz);
            _ = _stream.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
            _stream.Write(toWrite, 0, _pgSz);
            _stream.Flush();
        }
        finally
        {
            _ = _ioGate.Release();
        }
    }

    internal async ValueTask WritePageAsync(long pageNumber, byte[] page, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await _ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (ActiveJournal is { } journal)
            {
                journal.Write(pageNumber, page.AsSpan(0, _pgSz));
                return;
            }

            byte[] toWrite = PrepareEncryptedPageForWrite(pageNumber, page);
            IDisposable pageLock = await _byteRangeLock.AcquirePageLockAsync(pageNumber, _pgSz, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = _stream.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
                await _stream.WriteAsync(toWrite.AsMemory(0, _pgSz), cancellationToken).ConfigureAwait(false);
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = _ioGate.Release();
        }
    }

    private protected long AppendPage(byte[] page)
    {
        _ioGate.Wait();
        try
        {
            if (ActiveJournal is { } journal)
            {
                return journal.Append(page.AsSpan(0, _pgSz));
            }

            long pageNumber = _stream.Length / _pgSz;
            byte[] toWrite = PrepareEncryptedPageForWrite(pageNumber, page);
            using IDisposable pageLock = _byteRangeLock.AcquirePageLock(pageNumber, _pgSz);
            _ = _stream.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
            _stream.Write(toWrite, 0, _pgSz);
            _stream.Flush();
            return pageNumber;
        }
        finally
        {
            _ = _ioGate.Release();
        }
    }

    internal async ValueTask<long> AppendPageAsync(byte[] page, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await _ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (ActiveJournal is { } journal)
            {
                return journal.Append(page.AsSpan(0, _pgSz));
            }

            long pageNumber = _stream.Length / _pgSz;
            byte[] toWrite = PrepareEncryptedPageForWrite(pageNumber, page);
            IDisposable pageLock = await _byteRangeLock.AcquirePageLockAsync(pageNumber, _pgSz, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = _stream.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
                await _stream.WriteAsync(toWrite.AsMemory(0, _pgSz), cancellationToken).ConfigureAwait(false);
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                return pageNumber;
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = _ioGate.Release();
        }
    }

    // ── Catalog access ───────────────────────────────────────────────

    /// <summary>Finds a catalog entry by name (case-insensitive).</summary>
    internal async ValueTask<CatalogEntry?> GetCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var userTables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return userTables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    private protected abstract ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken = default);

    // ── Table page enumeration ───────────────────────────────────────

    /// <summary>
    /// Yields the bounds (row index, start offset, size) of every live (non-deleted, non-overflow)
    /// row on the given data <paramref name="page"/>.
    /// </summary>
    internal IEnumerable<RowBound> EnumerateLiveRowBounds(byte[] page)
    {
        int numRows = Ru16(page, _dataPage.NumRows);
        if (numRows == 0)
        {
            yield break;
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - _dataPage.RowsStart) / 2;
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
            rawOffsets[r] = Ru16(page, _dataPage.RowsStart + (r * 2));
        }

        var positions = new int[numRows];
        int posCount = 0;
        for (int r = 0; r < numRows; r++)
        {
            int pos = rawOffsets[r] & 0x1FFF;
            if (pos > 0 && pos < _pgSz)
            {
                positions[posCount++] = pos;
            }
        }

        Array.Sort(positions, 0, posCount);

        for (int r = 0; r < numRows; r++)
        {
            int raw = rawOffsets[r];
            if ((raw & 0xC000) != 0)
            {
                continue;
            }

            int rowStart = raw & 0x1FFF;
            int rowEnd = _pgSz - 1;
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
    private protected RowBound[] ComputeLiveRowBoundsArray(byte[] page)
    {
        int numRows = Ru16(page, _dataPage.NumRows);
        if (numRows == 0)
        {
            return Array.Empty<RowBound>();
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - _dataPage.RowsStart) / 2;
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
            int raw = Ru16(page, _dataPage.RowsStart + (r * 2));
            rawOffsets[r] = raw;

            int pos = raw & 0x1FFF;
            if (pos > 0 && pos < _pgSz)
            {
                positions[posCount++] = pos;
            }

            if ((raw & 0xC000) == 0)
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
            if ((raw & 0xC000) != 0)
            {
                continue;
            }

            int rowStart = raw & 0x1FFF;
            int rowEnd = _pgSz - 1;
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
        if (rowSize < _rowSz.NumCols)
        {
            return false;
        }

        int numCols = _rowSz.ReadNumCols(page, rowStart);
        if (numCols == 0)
        {
            return false;
        }

        int nullMaskSz = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSz;
        if (nullMaskPos < _rowSz.NumCols)
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
            int varLenPos = nullMaskPos - _rowSz.VarLen;
            if (varLenPos < _rowSz.NumCols)
            {
                return false;
            }

            varLen = _rowSz.ReadVarLen(page, rowStart + varLenPos);
            int jumpSz = _format != DatabaseFormat.Jet3Mdb ? 0 : (rowSize / 256);
            varTableStart = varLenPos - jumpSz - (varLen * _rowSz.VarEntry);
            int eodPos = varTableStart - _rowSz.Eod;
            if (eodPos < _rowSz.NumCols)
            {
                return false;
            }

            eod = _rowSz.ReadEod(page, rowStart + eodPos);
        }

        layout = new RowLayout(numCols, nullMaskPos, varLen, varTableStart, eod);
        return true;
    }

    /// <summary>
    /// Resolves the per-column data slice (or null/bool/empty marker) for
    /// <paramref name="col"/> within a row whose layout has been parsed by
    /// <see cref="TryParseRowLayout"/>.
    /// </summary>
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

        if (col.Type == T_BOOL)
        {
            return new ColumnSlice(ColumnSliceKind.Bool, 0, 0, nullBit);
        }

        if (col.ColNum >= layout.NumCols || !nullBit)
        {
            return new ColumnSlice(ColumnSliceKind.Null, 0, 0, false);
        }

        if (col.IsFixed)
        {
            int start = _rowSz.NumCols + col.FixedOff;
            int sz = JetTypeInfo.GetFixedSize(col.Type);
            if (sz == 0 || start + sz > rowSize)
            {
                return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
            }

            return new ColumnSlice(ColumnSliceKind.Fixed, start, sz, false);
        }

        if (col.VarIdx >= layout.VarLen)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        int entryPos = layout.VarTableStart + ((layout.VarLen - 1 - col.VarIdx) * _rowSz.VarEntry);
        if (entryPos < 0 || entryPos + _rowSz.VarEntry > rowSize)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        int varOff = _rowSz.ReadVarEntry(page, rowStart + entryPos);

        int varEnd;
        if (col.VarIdx + 1 < layout.VarLen)
        {
            int nextEntry = layout.VarTableStart + ((layout.VarLen - 2 - col.VarIdx) * _rowSz.VarEntry);
            varEnd = _rowSz.ReadVarEntry(page, rowStart + nextEntry);
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

    /// <summary>
    /// Yields <see cref="RowLocation"/>s (row index + start/size) for every live, non-overflow
    /// row on <paramref name="page"/>, paired with <paramref name="pageNumber"/>. A thin wrapper
    /// over <see cref="EnumerateLiveRowBounds(byte[])"/> for callers that need to round-trip
    /// the originating page number (update / delete paths).
    /// </summary>
    internal IEnumerable<RowLocation> EnumerateLiveRowLocations(long pageNumber, byte[] page)
    {
        foreach (RowBound rb in EnumerateLiveRowBounds(page))
        {
            yield return new RowLocation(pageNumber, rb.RowIndex, rb.RowStart, rb.RowSize);
        }
    }

    /// <summary>
    /// Reads a single column value as a string, supporting bool, fixed-width and inline-var
    /// (T_TEXT / T_BINARY) columns. Variable-width MEMO / OLE / Complex columns are NOT
    /// followed (they require LVAL chain traversal); those return <see cref="string.Empty"/>
    /// here. Used by writer-side catalog walks that only need scalar metadata columns.
    /// </summary>
    internal string DecodeSimpleColumnValue(byte[] page, int rowStart, int rowSize, ColumnInfo column)
    {
        if (column == null || rowSize < _rowSz.NumCols)
        {
            return string.Empty;
        }

        if (!TryParseRowLayout(page, rowStart, rowSize, hasVarColumns: true, out RowLayout layout))
        {
            return string.Empty;
        }

        ColumnSlice slice = ResolveColumnSlice(page, rowStart, rowSize, layout, column);
        switch (slice.Kind)
        {
            case ColumnSliceKind.Bool:
                return slice.BoolValue ? "True" : "False";

            case ColumnSliceKind.Null:
            case ColumnSliceKind.Empty:
                return string.Empty;

            case ColumnSliceKind.Fixed:
                return JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, column.Type, slice.DataLen);

            case ColumnSliceKind.Var:
                if (slice.DataLen <= 0)
                {
                    return string.Empty;
                }

                switch (column.Type)
                {
                    case T_TEXT:
                        return _format != DatabaseFormat.Jet3Mdb
                            ? DecodeJet4Text(page, rowStart + slice.DataStart, slice.DataLen)
                            : _ansiEncoding.GetString(page, rowStart + slice.DataStart, slice.DataLen);
                    case T_BINARY:
                        return JetTypeInfo.ToHexStringNoSeparator(page.AsSpan(rowStart + slice.DataStart, slice.DataLen));
                    default:
                        return string.Empty;
                }

            default:
                return string.Empty;
        }
    }

    // ── Catalog cache ────────────────────────────────────────────────
    // The cache is a single reference; volatile-write of a fully-built list is atomic
    // in .NET, so a lock is unnecessary (subsequent readers see either the old or the
    // new list, never a torn value).

    /// <summary>Returns the cached catalog list, or <see langword="null"/> if not yet populated.</summary>
    private protected List<CatalogEntry>? GetCatalogCache() => _catalogCache;

    /// <summary>Stores the catalog list returned by <see cref="GetUserTablesAsync"/>.</summary>
    private protected void SetCatalogCache(List<CatalogEntry> cache) => _catalogCache = cache;

    /// <summary>Discards the cached catalog so the next <see cref="GetUserTablesAsync"/> call re-scans MSysObjects.</summary>
    internal void InvalidateCatalogCache() => _catalogCache = null;

    // ── Inner types ──────────────────────────────────────────────────

    internal readonly record struct RowBound(int RowIndex, int RowStart, int RowSize);

    /// <summary>Parsed row-trailer metadata — see <see cref="TryParseRowLayout"/>.</summary>
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
    internal readonly record struct ColumnSlice(
        ColumnSliceKind Kind,
        int DataStart,
        int DataLen,
        bool BoolValue);
}
