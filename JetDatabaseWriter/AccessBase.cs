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
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

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

    /// <summary>Gets per-format byte offsets within a data-page (page type 0x01) header — see <see cref="DataPageLayout"/>.</summary>
    internal DataPageLayout DataPage { get; }

    /// <summary>Gets per-format byte offsets within a TDEF block plus real-idx entry size — see <see cref="TDefHeaderLayout"/>.</summary>
    internal TDefHeaderLayout TDef { get; }

    /// <summary>Gets per-format byte offsets within one column descriptor — see <see cref="ColumnDescriptorLayout"/>.</summary>
    internal ColumnDescriptorLayout ColumnDescriptor { get; }

    /// <summary>Gets per-format byte sizes of the in-row trailer fields — see <see cref="RowFieldSizes"/>.</summary>
    internal RowFieldSizes RowFields { get; }

    /// <summary>
    /// Gets per-format byte offsets and entry sizes for the TDEF page's real-idx
    /// physical descriptor (§3.1) and logical-idx entry (§3.2) sections.
    /// </summary>
    internal IndexLayout IndexLayoutInfo { get; }

    /// <summary>Gets the database page size in bytes.</summary>
    internal int PageSizeBytes { get; }

    /// <summary>Gets the detected database format.</summary>
    internal DatabaseFormat Format { get; }

    /// <summary>Gets the database backing stream for derived reader/writer implementations.</summary>
    private protected Stream DatabaseStream { get; }

    private readonly bool leaveOpen;

    /// <summary>Gets the ANSI code-page encoding used by Jet3 text and Jet4 catalog names.</summary>
    private protected Encoding AnsiEncodingCore { get; }

    /// <summary>Gets the decoded database code page.</summary>
    private protected int CodePageCore { get; }

    /// <summary>Gets the database path, or an empty string when opened from a caller-owned stream.</summary>
    private protected string DatabasePath { get; }

    internal Encoding AnsiEncoding => this.AnsiEncodingCore;

    /// <summary>
    /// Gets per-page decryption keys (Jet3 XOR, Jet4 RC4, ACCDB AES). Built during
    /// reader/writer construction by <see cref="EncryptionManager"/>; consulted by every page read via
    /// <see cref="EncryptionManager.DecryptPageInPlace(byte[], long, int, PageDecryptionKeys)"/>.
    /// </summary>
    private protected PageDecryptionKeys PageKeys { get; }

    internal bool IsDisposed { get; private set; }

    private volatile List<CatalogEntry>? catalogCache;
    private volatile List<LinkedTableInfo>? linkedTableCache;

    /// <summary>
    /// Gets or sets the cooperative JET byte-range lock helper. Defaults to
    /// <see cref="JetByteRangeLock.Disabled"/> so page-write paths can dispatch
    /// without a null check; <see cref="AccessReader"/> / <see cref="AccessWriter"/>
    /// replace it with a stream-bound instance once options are known.
    /// </summary>
    private protected JetByteRangeLock ByteRangeLockCore { get; set; } = JetByteRangeLock.Disabled;

    /// <summary>
    /// Gets or sets the in-memory page journal for an explicit <see cref="JetTransaction"/>.
    /// When non-null, page writes/appends are buffered into the journal
    /// instead of being flushed to the underlying stream, and page reads
    /// consult the journal first so the transaction sees its own writes.
    /// Set and cleared exclusively by <see cref="AccessWriter"/> while holding
    /// <see cref="IoGate"/>.
    /// </summary>
    internal PageJournal? ActiveJournal { get; set; }

    private readonly AsyncLazyInitializer<Dictionary<long, long[]>> ownedDataPageIndex;
#if NET9_0_OR_GREATER
    private readonly Lock ownedDataPagesCacheLock = new();
#else
    private readonly object ownedDataPagesCacheLock = new();
#endif
    private readonly Dictionary<long, long[]> ownedDataPagesByTdef = [];

    /// <summary>Gets the writer's internal I/O gate so derived types may serialise transaction commit / rollback.</summary>
    internal SemaphoreSlim IoGate { get; } = new(1, 1);

    static AccessBase() => Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessBase"/> class
    /// from a pre-read database file header.
    /// </summary>
    /// <param name="stream">An open, seekable <see cref="Stream"/> for the database file.</param>
    /// <param name="header">Header bytes read from page 0.</param>
    /// <param name="password">The database password.</param>
    /// <param name="path">Path to the database file, or empty when opened from a stream.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller retains ownership of <paramref name="stream"/> and it will not be disposed.</param>
    private protected AccessBase(
        Stream stream,
        byte[] header,
        ReadOnlyMemory<char> password,
        string path = "",
        bool leaveOpen = false)
    {
        this.DatabaseStream = stream;
        this.leaveOpen = leaveOpen;
        this.DatabasePath = path ?? string.Empty;
        this.ownedDataPageIndex = new(this.BuildOwnedDataPageIndexAsync);

        this.Format = EncryptionConverter.DetectFormat(header);
        this.PageSizeBytes = GetPageSize(this.Format);
        bool isLegacyAesCfb = EncryptionManager.IsCompoundFileEncrypted(header);
        this.PageKeys = EncryptionManager.CreatePageDecryptionKeys(header, this.Format, isLegacyAesCfb, password);

        // Codepage / sort order: stored as a UInt16 at hdr[0x3C], scrambled by
        // the constant-key RC4 stream Microsoft Access applies to header bytes
        // [0x18 .. 0x18+126/128]. EncryptionManager.DecodeHeaderCodePage handles
        // the descrambling so we recover the real codepage (e.g. 1252) instead
        // of a corrupted byte. ACE / ACCDB stores text as UTF-16 in user data
        // so the codepage there is largely cosmetic, but Jet3 .mdb files (and
        // Jet4 catalog names) need it correct to round-trip non-ASCII names.
        this.CodePageCore = EncryptionManager.DecodeHeaderCodePage(header, this.Format);
        if (this.CodePageCore <= 0)
        {
            this.CodePageCore = 1252;  // default to Windows-1252 if unknown
        }

        try
        {
            this.AnsiEncodingCore = Encoding.GetEncoding(this.CodePageCore);
        }
        catch (ArgumentException)
        {
            this.AnsiEncodingCore = Encoding.UTF8;
            this.CodePageCore = 65001;
        }
        catch (NotSupportedException)
        {
            this.AnsiEncodingCore = Encoding.UTF8;
            this.CodePageCore = 65001;
        }

        // Format-specific TDEF / page / column / row layouts:
        //   Jet4 / ACE (Access 2000–2019): TDEF 8+55 = 63 bytes, column descriptor 25 bytes.
        //   Jet3        (Access 97):       TDEF 8+35 = 43 bytes, column descriptor 18 bytes.
        this.DataPage = DataPageLayout.For(this.Format);
        this.TDef = TDefHeaderLayout.For(this.Format);
        this.ColumnDescriptor = ColumnDescriptorLayout.For(this.Format);
        this.RowFields = RowFieldSizes.For(this.Format);
        this.IndexLayoutInfo = IndexLayout.For(this.Format);
    }

    /// <inheritdoc/>
    public DatabaseFormat DatabaseFormat => this.Format;

    /// <inheritdoc/>
    public int PageSize => this.PageSizeBytes;

    /// <inheritdoc/>
    public int CodePage => this.CodePageCore;

    internal bool UsesRandomAccessPageReads { get; private set; }

    internal long DatabaseLengthBytes => this.DatabaseStream.Length;

    internal long PhysicalPageCount => this.DatabaseStream.Length / this.PageSizeBytes;

    internal long LogicalPageCount => this.ActiveJournal?.NextAppendPageNumber ?? this.PhysicalPageCount;

    private protected virtual bool CanCacheOwnedDataPages => true;

    private protected void EnableRandomAccessPageReadsIfSupported()
    {
#if NET6_0_OR_GREATER
        if (this.DatabaseStream is FileStream fileStream &&
            !fileStream.SafeFileHandle.IsInvalid &&
            !fileStream.SafeFileHandle.IsClosed)
        {
            this.UsesRandomAccessPageReads = true;
        }
#else
        this.UsesRandomAccessPageReads = false;

        _ = this.UsesRandomAccessPageReads;
#endif
    }

    internal async ValueTask SetDatabaseLengthAsync(long length, CancellationToken cancellationToken)
    {
        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.DatabaseStream.SetLength(length);
            await this.DatabaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = this.IoGate.Release();
        }
    }

    internal async ValueTask FlushDatabaseStreamAsync(bool flushToDisk, CancellationToken cancellationToken)
    {
        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (flushToDisk && this.DatabaseStream is FileStream fileStream)
            {
#pragma warning disable CA1849
                fileStream.Flush(flushToDisk: true);
#pragma warning restore CA1849
            }
            else
            {
                await this.DatabaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = this.IoGate.Release();
        }
    }

    /// <inheritdoc/>
    public virtual async ValueTask DisposeAsync()
    {
        if (this.IsDisposed)
        {
            return;
        }

        this.IsDisposed = true;
        try
        {
            if (!this.leaveOpen)
            {
                await this.DatabaseStream.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            this.DisposeBaseManagedResources();
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Disposes base-owned managed resources other than the backing stream.
    /// Used by derived constructors when construction fails before an instance
    /// can be returned to the caller and disposed normally.
    /// </summary>
    private protected void DisposeBaseManagedResources()
    {
        this.IoGate.Dispose();
        this.PageKeys.Dispose();
        this.ownedDataPagesByTdef.Clear();
        this.ownedDataPageIndex.Dispose();
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

        byte[] hdr = new byte[0x80];
        _ = fs.Seek(0, SeekOrigin.Begin);
        await fs.ReadExactlyAsync(hdr.AsMemory(), cancellationToken).ConfigureAwait(false);

        return hdr;
    }

    // ── Static helpers ────────────────────────────────────────────────

    internal static void ReturnPage(byte[] page) => ArrayPool<byte>.Shared.Return(page);

    // Little-endian primitives (Ru16/Ri32/Ru32/Ri64/Wu16/Wu32/Wi32/Wi64) and
    // float/24-bit/hex helpers live in JetTypeInfo so non-Core callers
    // (Encryption layer, index codecs, …) can use them without
    // taking an upward dependency on AccessBase. They are surfaced here
    // through the file-level `using static JetDatabaseWriter.Schema.JetTypeInfo;`.

    // ── File-stream factory ──────────────────────────────────────────

    /// <summary>
    /// Opens a database file with the given access / share / option combination.
    /// Used by both <see cref="AccessReader"/> (read-only sequential) and
    /// <see cref="AccessWriter"/> (read-write random-access).
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="access">The access.</param>
    /// <param name="share">The share.</param>
    /// <param name="options">The options.</param>
    private protected static FileStream OpenDatabaseFileStream(string path, FileAccess access, FileShare share, FileOptions options) => FileStreamFactory.Open(path, FileMode.Open, access, share, options);

    // Fixed-column decoding (ReadFixedString / ReadFixedTyped) lives in
    // JetTypeInfo so the per-type byte→value switch sits next to its
    // metadata siblings (GetFixedSize, GetClrType, GetTypeDisplayName).

    // ── Page I/O ─────────────────────────────────────────────────────

    internal async ValueTask<byte[]> ReadPageAsync(long n, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        byte[] buf = ArrayPool<byte>.Shared.Rent(this.PageSizeBytes);
        try
        {
#if NET6_0_OR_GREATER
            if (this.UsesRandomAccessPageReads && this.ActiveJournal is null && this.DatabaseStream is FileStream fileStream)
            {
                await this.ReadPageRandomAccessAsync(fileStream, n, buf, cancellationToken).ConfigureAwait(false);
            }
            else
#endif
            {
                await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    // Inside an explicit transaction, prefer the journal: the page may
                    // be a transaction-local mutation (or an appended page that has no
                    // on-disk slot yet). Journal bytes are plaintext; bypass decrypt.
                    byte[]? journaled = this.ActiveJournal?.TryGet(n);
                    if (journaled is not null)
                    {
                        Buffer.BlockCopy(journaled, 0, buf, 0, this.PageSizeBytes);
                        return buf;
                    }

                    _ = this.DatabaseStream.Seek(n * this.PageSizeBytes, SeekOrigin.Begin);
                    await this.DatabaseStream.ReadExactlyAsync(buf.AsMemory(0, this.PageSizeBytes), cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _ = this.IoGate.Release();
                }
            }

            EncryptionManager.DecryptPageInPlace(buf, n, this.PageSizeBytes, this.PageKeys);

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
        long fileOffset = pageNumber * this.PageSizeBytes;
        int totalRead = 0;
        while (totalRead < this.PageSizeBytes)
        {
            int bytesRead = await RandomAccess.ReadAsync(
                fileStream.SafeFileHandle,
                page.AsMemory(totalRead, this.PageSizeBytes - totalRead),
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private protected async ValueTask<byte[]?> ReadTDefBytesAsync(long startPage, CancellationToken cancellationToken = default)
    {
        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
            startPage,
            this.PageSizeBytes,
            this.ReadPageAsync,
            ReturnPage,
            retainPageNumbers: false,
            cancellationToken).ConfigureAwait(false);

        return chain?.Bytes;
    }

    internal async ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken = default)
    {
        byte[]? td = await this.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        if (td == null || td.Length < this.TDef.BlockEnd)
        {
            return null;
        }

        int numCols = Ru16(td, this.TDef.NumCols);
        int numRealIdx = Ri32(td, this.TDef.NumRealIdx);

        // Safety: corrupt or unusual TDEFs can report absurd index counts
        if (numRealIdx is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            numRealIdx = 0;
        }

        if (numCols > Constants.TableDefinition.MaxColumns)
        {
            return null;
        }

        // Column descriptors follow immediately after block + first real-idx entries
        int colStart = this.TDef.BlockEnd + (numRealIdx * this.TDef.RealIdxEntrySz);
        int namePos = colStart + (numCols * this.ColumnDescriptor.Size);

        if (namePos > td.Length)
        {
            return null;
        }

        var descriptors = new List<ParsedColumnDescriptor>(numCols);
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * this.ColumnDescriptor.Size);
            if (o + this.ColumnDescriptor.Size > td.Length)
            {
                break;
            }

            var type = (ColumnType)td[o + this.ColumnDescriptor.TypeOff];

            // Extra flags byte at descriptor offset 16 (Jet4/ACE only — the
            // Jet3 18-byte descriptor has no such slot). Carries the Access
            // 2010+ calculated-column marker (Jackcess CALCULATED_EXT_FLAG_MASK
            // = 0xC0). Read unconditionally for Jet4/ACE so calc columns
            // round-trip through the schema-rewrite path; harmless for cols
            // Access wrote with the slot at zero.
            byte extraFlags = this.Format != DatabaseFormat.Jet3Mdb && o + 16 < td.Length ? td[o + 16] : (byte)0;
            int misc = Ri32(td, o + this.ColumnDescriptor.MiscOff);

            // For Numeric the misc 4-byte slot reuses bytes 11/12
            // (descriptor-relative) to carry the declared precision and
            // scale Access shows in Design View. Same byte positions as
            // the Jackcess `FixedPointColumnDescriptor` parser. Other
            // column types leave these at 0.
            byte numericPrecision = type == NumericType ? td[o + this.ColumnDescriptor.MiscOff] : (byte)0;
            byte numericScale = type == NumericType ? td[o + this.ColumnDescriptor.MiscOff + 1] : (byte)0;

            descriptors.Add(new ParsedColumnDescriptor(
                type,
                Ru16(td, o + this.ColumnDescriptor.NumOff),
                Ru16(td, o + this.ColumnDescriptor.VarOff),
                Ru16(td, o + this.ColumnDescriptor.FixedOff),
                Ru16(td, o + this.ColumnDescriptor.SzOff),
                td[o + this.ColumnDescriptor.FlagsOff],
                extraFlags,
                misc,
                numericPrecision,
                numericScale));
        }

        // Column names follow directly after all descriptors (in TDEF / descriptor order).
        // Names MUST be read before sorting so each name maps to the correct descriptor.
        var cols = new List<ColumnInfo>(descriptors.Count);
        bool readNames = true;
        for (int i = 0; i < descriptors.Count; i++)
        {
            string name = string.Empty;
            if (readNames)
            {
                int nameLen = this.ReadColumnName(td, ref namePos, out string parsedName);
                if (nameLen >= 0)
                {
                    name = parsedName;
                }
                else
                {
                    readNames = false;
                }
            }

            cols.Add(descriptors[i].ToColumnInfo(name));
        }

        // Sort by col_num AFTER names are assigned.
        cols.Sort((a, b) => a.ColNum.CompareTo(b.ColNum));

        // Detect deleted-column gaps: if ColNum sequence has gaps, flag it
        bool hasDeletedColumns = cols.Count >= 2
            && cols[^1].ColNum - cols[0].ColNum != cols.Count - 1;

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
        => this.Format == DatabaseFormat.Jet3Mdb ? page[rowStart] : Ru16(page, rowStart);

    internal int RowColumnCountFieldSize => this.RowFields.NumCols;

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

        return this.Format == DatabaseFormat.Jet3Mdb ? this.AnsiEncodingCore.GetString(bytes, start, len) : DecodeJet4Text(bytes, start, len);
    }

    /// <summary>
    /// Encodes a string for storage using the format-appropriate codec
    /// (Jet4 with optional compression vs Jet3 ANSI code-page bytes).
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="compress">The compress.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeTextForFormat(string value, bool compress = true)
        => this.Format == DatabaseFormat.Jet3Mdb ? this.AnsiEncodingCore.GetBytes(value) : EncodeJet4Text(value, compress);

    /// <summary>
    /// Encodes a string for storage using the format-appropriate codec,
    /// truncating the Jet4 path to at most <paramref name="maxBytes"/> output bytes.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="maxBytes">The max bytes.</param>
    /// <param name="compress">The compress.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeTextForFormat(string value, int maxBytes, bool compress = true)
        => this.Format == DatabaseFormat.Jet3Mdb ? this.AnsiEncodingCore.GetBytes(value) : EncodeJet4Text(value, maxBytes, compress);

    /// <summary>
    /// Throws <see cref="ObjectDisposedException"/> when this instance has been
    /// disposed. Wraps <see cref="Guard.ThrowIfDisposed(bool, object)"/> with
    /// the common <c>(_disposed, this)</c> arguments.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ThrowIfDisposed() => Guard.ThrowIfDisposed(this.IsDisposed, this);

    /// <summary>
    /// Combined disposed-and-cancelled guard. Mirrors the call-site pattern
    /// <c>ThrowIfDisposed(); cancellationToken.ThrowIfCancellationRequested();</c>
    /// that opens nearly every public writer entry point.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
    {
        Guard.ThrowIfDisposed(this.IsDisposed, this);
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Reads a single column name from the TDEF byte array at <paramref name="pos"/>,
    /// advancing <paramref name="pos"/> past the name bytes.
    /// Returns the byte length consumed, or -1 if the name extends beyond <paramref name="td"/>.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="pos">The byte position.</param>
    /// <param name="name">The name.</param>
    internal int ReadColumnName(byte[] td, ref int pos, out string name)
    {
        name = string.Empty;
        if (pos >= td.Length)
        {
            return -1;
        }

        if (this.Format != DatabaseFormat.Jet3Mdb)
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

            name = DecodeUtf16LE(td.AsSpan(pos, len));
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

            name = this.AnsiEncodingCore.GetString(td, pos, len);
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
        if (pageNumber < 1 || !EncryptionManager.HasPageEncryption(this.PageKeys))
        {
            return page;
        }

        byte[] copy = new byte[this.PageSizeBytes];
        Buffer.BlockCopy(page, 0, copy, 0, this.PageSizeBytes);
        EncryptionManager.EncryptPageInPlace(copy, pageNumber, this.PageSizeBytes, this.PageKeys);
        return copy;
    }

    internal async ValueTask WritePageAsync(long pageNumber, byte[] page, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveJournal is { } journal)
            {
                journal.Write(pageNumber, page.AsSpan(0, this.PageSizeBytes));
                return;
            }

            byte[] toWrite = this.PrepareEncryptedPageForWrite(pageNumber, page);
            IDisposable pageLock = await this.ByteRangeLockCore.AcquirePageLockAsync(pageNumber, this.PageSizeBytes, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = this.DatabaseStream.Seek(pageNumber * this.PageSizeBytes, SeekOrigin.Begin);
                await this.DatabaseStream.WriteAsync(toWrite.AsMemory(0, this.PageSizeBytes), cancellationToken).ConfigureAwait(false);
                await this.DatabaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = this.IoGate.Release();
        }
    }

    internal async ValueTask<long> AppendPageAsync(byte[] page, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveJournal is { } journal)
            {
                return journal.Append(page.AsSpan(0, this.PageSizeBytes));
            }

            long pageNumber = this.DatabaseStream.Length / this.PageSizeBytes;
            byte[] toWrite = this.PrepareEncryptedPageForWrite(pageNumber, page);
            IDisposable pageLock = await this.ByteRangeLockCore.AcquirePageLockAsync(pageNumber, this.PageSizeBytes, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = this.DatabaseStream.Seek(pageNumber * this.PageSizeBytes, SeekOrigin.Begin);
                await this.DatabaseStream.WriteAsync(toWrite.AsMemory(0, this.PageSizeBytes), cancellationToken).ConfigureAwait(false);
                await this.DatabaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                return pageNumber;
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = this.IoGate.Release();
        }
    }

    // ── Catalog access ───────────────────────────────────────────────

    /// <summary>Finds a catalog entry by name (case-insensitive).</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<CatalogEntry?> GetCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
    {
        List<CatalogEntry> userTables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return userTables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private protected abstract ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken = default);

    // ── Table page enumeration ───────────────────────────────────────

    internal async ValueTask<IReadOnlyList<long>> GetOwnedDataPagesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        if (tdefPage <= 0)
        {
            return [];
        }

        bool canUseCache = this.CanCacheOwnedDataPages && this.ActiveJournal is null;
        if (canUseCache && this.TryGetCachedOwnedDataPages(tdefPage, out long[] cachedPages))
        {
            return cachedPages;
        }

        long[]? mappedPages = await this.TryGetOwnedDataPagesFromUsageMapAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (mappedPages is not null)
        {
            if (canUseCache)
            {
                this.CacheOwnedDataPages(tdefPage, mappedPages);
            }

            return mappedPages;
        }

        Dictionary<long, long[]> pageIndex = canUseCache
            ? await this.ownedDataPageIndex.GetAsync(cancellationToken).ConfigureAwait(false)
            : await this.BuildOwnedDataPageIndexAsync(cancellationToken).ConfigureAwait(false);
        return pageIndex.TryGetValue(tdefPage, out long[]? pageNumbers)
            ? pageNumbers
            : [];
    }

    internal async ValueTask ForEachLiveTableRowAsync(
        long tdefPage,
        TableRowVisitor visitRowAsync,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(visitRowAsync, nameof(visitRowAsync));

        await this.ForEachOwnedDataPageAsync(
            tdefPage,
            async (pageNumber, page, token) =>
            {
                foreach (RowLocation row in this.EnumerateLiveRowLocations(pageNumber, page))
                {
                    if (!await visitRowAsync(new TableRow(page, row), token).ConfigureAwait(false))
                    {
                        return false;
                    }
                }

                return true;
            },
            cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask ForEachOwnedDataPageAsync(
        long tdefPage,
        DataPageVisitor visitPageAsync,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(visitPageAsync, nameof(visitPageAsync));

        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, this.DataPage.TDefOff) != tdefPage)
                {
                    continue;
                }

                if (!await visitPageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false))
                {
                    return;
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }
    }

    internal async ValueTask<List<RowLocation>> GetLiveRowLocationsAsync(long tdefPage, CancellationToken cancellationToken)
    {
        var result = new List<RowLocation>();
        await this.ForEachLiveTableRowAsync(
            tdefPage,
            (row, _) =>
            {
                result.Add(row.Location);
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return result;
    }

    private bool TryGetCachedOwnedDataPages(long tdefPage, out long[] pageNumbers)
    {
        lock (this.ownedDataPagesCacheLock)
        {
            bool found = this.ownedDataPagesByTdef.TryGetValue(tdefPage, out long[]? cachedPages);
            pageNumbers = cachedPages ?? [];
            return found;
        }
    }

    private void CacheOwnedDataPages(long tdefPage, long[] pageNumbers)
    {
        lock (this.ownedDataPagesCacheLock)
        {
            this.ownedDataPagesByTdef[tdefPage] = pageNumbers;
        }
    }

    private async ValueTask<long[]?> TryGetOwnedDataPagesFromUsageMapAsync(long tdefPage, CancellationToken cancellationToken)
    {
        long totalPages = this.DatabaseStream.Length / this.PageSizeBytes;
        if (tdefPage <= 0 || tdefPage >= totalPages)
        {
            return null;
        }

        byte[] tdef = await this.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (tdef[0] != Constants.PageTypes.TableDefinition
                || !UsageMap.TryReadPointer(tdef, Constants.TableDefinition.OwnedPagesRowOffset, out UsageMapPointer pointer)
                || pointer.PageNumber <= 0)
            {
                return null;
            }

            uint declaredRows = tdef.Length >= Constants.TableDefinition.RowCountOffset + sizeof(uint)
                ? Ru32(tdef, Constants.TableDefinition.RowCountOffset)
                : 0;
            return await this.TryReadMappedOwnedDataPagesAsync(
                tdefPage,
                pointer.PageNumber,
                pointer.RowIndex,
                declaredRows,
                totalPages,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(tdef);
        }
    }

    private async ValueTask<long[]?> TryReadMappedOwnedDataPagesAsync(
        long tdefPage,
        int usageMapPageNumber,
        int usageMapRow,
        uint declaredRows,
        long totalPages,
        CancellationToken cancellationToken)
    {
        if (usageMapPageNumber <= 0 || usageMapPageNumber >= totalPages)
        {
            return null;
        }

        byte[] usageMapPage = await this.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (usageMapPage[0] != Constants.PageTypes.Data
                || !UsageMap.TryGetRowBound(usageMapPage, this.DataPage, this.PageSizeBytes, usageMapRow, out RowBound rowBound))
            {
                return null;
            }

            var mappedPages = new List<long>();
            bool recognizedMap = await UsageMap.TryEnumeratePagesAsync(
                usageMapPage,
                rowBound,
                this.PageSizeBytes,
                totalPages,
                minimumPageNumber: 1,
                strict: true,
                this.ReadPageAsync,
                ReturnPage,
                mappedPages,
                cancellationToken).ConfigureAwait(false);
            if (!recognizedMap)
            {
                return null;
            }

            if (mappedPages.Count == 0)
            {
                return declaredRows == 0 ? [] : null;
            }

            return await this.ValidateOwnedDataPagesAsync(tdefPage, mappedPages, declaredRows, cancellationToken).ConfigureAwait(false)
                ? [.. mappedPages]
                : null;
        }
        finally
        {
            ReturnPage(usageMapPage);
        }
    }

    private async ValueTask<bool> ValidateOwnedDataPagesAsync(
        long tdefPage,
        List<long> pageNumbers,
        uint declaredRows,
        CancellationToken cancellationToken)
    {
        long liveRows = 0;
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, this.DataPage.TDefOff) != tdefPage)
                {
                    return false;
                }

                if (declaredRows > 0)
                {
                    liveRows += this.ComputeLiveRowBoundsArray(page).Length;
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return declaredRows == 0 || liveRows >= declaredRows;
    }

    private async ValueTask<Dictionary<long, long[]>> BuildOwnedDataPageIndexAsync(CancellationToken cancellationToken)
    {
        var pagesByOwner = new Dictionary<long, List<long>>();
        long totalPages = this.DatabaseStream.Length / this.PageSizeBytes;

        for (long pageNumber = 3; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data)
                {
                    continue;
                }

                long owner = Ri32(page, this.DataPage.TDefOff);
                if (owner <= 0)
                {
                    continue;
                }

                if (!pagesByOwner.TryGetValue(owner, out List<long>? ownedPages))
                {
                    ownedPages = [];
                    pagesByOwner.Add(owner, ownedPages);
                }

                ownedPages.Add(pageNumber);
            }
            finally
            {
                ReturnPage(page);
            }
        }

        var result = new Dictionary<long, long[]>(pagesByOwner.Count);
        foreach ((long owner, List<long>? ownedPages) in pagesByOwner)
        {
            result.Add(owner, [.. ownedPages]);
        }

        return result;
    }

    /// <summary>
    /// Yields the bounds (row index, start offset, size) of every live (non-deleted, non-overflow)
    /// row on the given data <paramref name="page"/>.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    internal IEnumerable<RowBound> EnumerateLiveRowBounds(byte[] page)
    {
        int numRows = Ru16(page, this.DataPage.NumRows);
        if (numRows == 0)
        {
            yield break;
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - this.DataPage.RowsStart) / 2;
        if (numRows > maxPossibleRows)
        {
            numRows = maxPossibleRows;
        }

        if (numRows <= 0)
        {
            yield break;
        }

        int[] rawOffsets = new int[numRows];
        for (int r = 0; r < numRows; r++)
        {
            rawOffsets[r] = Ru16(page, this.DataPage.RowsStart + (r * 2));
        }

        int[] positions = new int[numRows];
        int posCount = 0;
        for (int r = 0; r < numRows; r++)
        {
            int pos = rawOffsets[r] & Constants.DataPage.RowOffsetMask;
            if (pos > 0 && pos < this.PageSizeBytes)
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
            int rowEnd = this.PageSizeBytes - 1;
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
        int numRows = Ru16(page, this.DataPage.NumRows);
        if (numRows == 0)
        {
            return [];
        }

        // Clamp numRows to the maximum that can physically fit in the page's
        // row-offset table region (each entry is 2 bytes, starting at RowsStart).
        int maxPossibleRows = (page.Length - this.DataPage.RowsStart) / 2;
        if (numRows > maxPossibleRows)
        {
            numRows = maxPossibleRows;
        }

        if (numRows <= 0)
        {
            return [];
        }

        // Cold (cache-miss) scan only: warm rescans are served from the
        // row-bounds cache. Rent the two scratch buffers from the shared pool
        // instead of allocating int[numRows] per page; numRows is bounded by the
        // page's row-offset table size. The existing Array.Sort/Array.BinarySearch
        // logic is preserved (Span<int>.Sort is unavailable on netstandard2.1).
        int[] rawOffsets = ArrayPool<int>.Shared.Rent(numRows);
        int[] positions = ArrayPool<int>.Shared.Rent(numRows);
        try
        {
            int posCount = 0;
            int liveCount = 0;
            for (int r = 0; r < numRows; r++)
            {
                int raw = Ru16(page, this.DataPage.RowsStart + (r * 2));
                rawOffsets[r] = raw;

                int pos = raw & Constants.DataPage.RowOffsetMask;
                if (pos > 0 && pos < this.PageSizeBytes)
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
                return [];
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
                int rowEnd = this.PageSizeBytes - 1;
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
        finally
        {
            ArrayPool<int>.Shared.Return(rawOffsets);
            ArrayPool<int>.Shared.Return(positions);
        }
    }

    // ── Row layout decoding (forwards to RowDecodePlan; used by writer column reads) ────

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
    internal bool TryParseRowLayout(ReadOnlySpan<byte> page, int rowStart, int rowSize, bool hasVarColumns, out RowLayout layout)
        => RowDecodePlan.TryParseRowLayout(this.Format, this.RowFields, page, rowStart, rowSize, hasVarColumns, out layout);

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
    internal ColumnSlice ResolveColumnSlice(ReadOnlySpan<byte> page, int rowStart, int rowSize, in RowLayout layout, ColumnInfo col)
        => RowDecodePlan.ResolveColumnSlice(this.RowFields, page, rowStart, rowSize, layout, col);

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
        foreach (RowBound rb in this.EnumerateLiveRowBounds(page))
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
    /// <exception cref="InvalidOperationException">Thrown when the column type is unknown.</exception>
    internal string DecodeSimpleColumnValue(byte[] page, int rowStart, int rowSize, ColumnInfo column)
    {
        if (column == null || rowSize < this.RowFields.NumCols)
        {
            return string.Empty;
        }

        if (!this.TryParseRowLayout(page, rowStart, rowSize, hasVarColumns: true, out RowLayout layout))
        {
            return string.Empty;
        }

        ColumnSlice slice = this.ResolveColumnSlice(page, rowStart, rowSize, layout, column);
        switch (slice.Kind)
        {
            case ColumnSliceKind.Bool:
                return slice.BoolValue ? "True" : "False";

            case ColumnSliceKind.Null:
            case ColumnSliceKind.Empty:
                return string.Empty;

            case ColumnSliceKind.Fixed:
                return ReadFixedString(page, rowStart + slice.DataStart, column, slice.DataLen);

            case ColumnSliceKind.Var:
                if (slice.DataLen <= 0)
                {
                    return string.Empty;
                }

                if (TryGetVariableSlotFixedPayloadSize(column.Type, out int required))
                {
                    return slice.DataLen >= required
                        ? ReadFixedString(page, rowStart + slice.DataStart, column, required)
                        : string.Empty;
                }

                if (column.Type == TextType)
                {
                    return this.DecodeTextForFormat(page, rowStart + slice.DataStart, slice.DataLen);
                }

                if (column.Type == BinaryType)
                {
                    return ToHexStringNoSeparator(page.AsSpan(rowStart + slice.DataStart, slice.DataLen));
                }

                if (column.Type is BooleanType or OleType or MemoType)
                {
                    return string.Empty;
                }

                throw new InvalidOperationException($"Unknown column type: {GetTypeDisplayName(column.Type)}");

            default:
                return string.Empty;
        }
    }

    // ── Catalog cache ────────────────────────────────────────────────
    // Each cache is a single reference; volatile-write of a fully-built list is atomic
    // in .NET, so a lock is unnecessary (subsequent readers see either the old or the
    // new list, never a torn value).

    /// <summary>Returns the cached catalog list, or <see langword="null"/> if not yet populated.</summary>
    private protected List<CatalogEntry>? GetCatalogCache() => this.catalogCache;

    /// <summary>Stores the catalog list returned by <see cref="GetUserTablesAsync"/>.</summary>
    /// <param name="cache">The cache.</param>
    private protected void SetCatalogCache(List<CatalogEntry> cache) => this.catalogCache = cache;

    /// <summary>Returns the cached linked-table list, or <see langword="null"/> if not yet populated.</summary>
    private protected List<LinkedTableInfo>? GetLinkedTableCache() => this.linkedTableCache;

    /// <summary>Stores the linked-table list returned by the MSysObjects linked-table scan.</summary>
    /// <param name="cache">The cache.</param>
    private protected void SetLinkedTableCache(List<LinkedTableInfo> cache) => this.linkedTableCache = cache;

    /// <summary>Discards the cached catalog lists so the next call re-scans MSysObjects.</summary>
    internal void InvalidateCatalogCache()
    {
        this.catalogCache = null;
        this.linkedTableCache = null;
    }

    // ── Inner types ──────────────────────────────────────────────────

    internal delegate ValueTask<bool> TableRowVisitor(TableRow row, CancellationToken cancellationToken);

    internal delegate ValueTask<bool> DataPageVisitor(long pageNumber, byte[] page, CancellationToken cancellationToken);

    internal readonly record struct TableRow(byte[] Page, RowLocation Location);

    private readonly record struct ParsedColumnDescriptor(
        ColumnType Type,
        int ColNum,
        int VarIdx,
        int FixedOff,
        int Size,
        byte Flags,
        byte ExtraFlags,
        int Misc,
        byte NumericPrecision,
        byte NumericScale)
    {
        internal ColumnInfo ToColumnInfo(string name) => new()
        {
            Name = name,
            Type = this.Type,
            ColNum = this.ColNum,
            VarIdx = this.VarIdx,
            FixedOff = this.FixedOff,
            Size = this.Size,
            Flags = this.Flags,
            ExtraFlags = this.ExtraFlags,
            Misc = this.Misc,
            NumericPrecision = this.NumericPrecision,
            NumericScale = this.NumericScale,
        };
    }
}
