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
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// One open JET database file: the backing stream, its format profile
/// (<see cref="JetFormat"/>, whose members it forwards), page I/O
/// (decryption, the transaction journal, cooperative byte-range locks), TDEF
/// parsing, and owned data-page and live-row enumeration. Shared by the
/// reader and writer service graphs; it knows nothing about either facade.
/// </summary>
internal sealed class DatabaseFile : IAsyncDisposable
{
    /// <summary>
    /// <see langword="true"/> for the writer's file, whose pages change; a
    /// read-only file memoizes each table's owned data pages instead.
    /// </summary>
    private readonly bool writable;

    private readonly AsyncLazyInitializer<Dictionary<long, long[]>> ownedDataPageIndex;
#if NET9_0_OR_GREATER
    private readonly Lock ownedDataPagesCacheLock = new();
#else
    private readonly object ownedDataPagesCacheLock = new();
#endif
    private readonly Dictionary<long, long[]> ownedDataPagesByTdef = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="DatabaseFile"/> class
    /// from a pre-read database file header.
    /// </summary>
    /// <param name="stream">An open, seekable <see cref="Stream"/> for the database file.</param>
    /// <param name="header">Header bytes read from page 0.</param>
    /// <param name="password">The database password.</param>
    /// <param name="path">Path to the database file, or empty when opened from a stream.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller retains ownership of <paramref name="stream"/> and it will not be disposed.</param>
    /// <param name="ownerType">The public type that owns this file, named by <see cref="ObjectDisposedException"/>s raised after disposal; its options type is named by a missing-password error.</param>
    /// <param name="writable">
    /// <see langword="true"/> for the writer: the file gets a <see cref="Pager"/>,
    /// which can write and journal pages. <see langword="false"/> for a reader:
    /// the file gets a read-only <see cref="PageFile"/>, its write members
    /// throw, and each table's owned data pages are memoized, which is only
    /// safe because nothing writes through it.
    /// </param>
    internal DatabaseFile(
        Stream stream,
        byte[] header,
        ReadOnlyMemory<char> password,
        string path,
        bool leaveOpen,
        Type ownerType,
        bool writable)
    {
        this.writable = writable;
        this.DatabasePath = path ?? string.Empty;
        this.ownedDataPageIndex = new(this.BuildOwnedDataPageIndexAsync);

        // The profile detects the format and decodes the code page; neither
        // step can throw, so a password error below is still the first error
        // an encrypted file reports, as when the code page was decoded after
        // the page keys.
        this.Profile = JetFormat.FromHeader(header);
        bool isLegacyAesCfb = EncryptionManager.IsCompoundFileEncrypted(header);
        string passwordOptionName = ownerType == typeof(AccessWriter)
            ? EncryptionManager.WriterPasswordOption
            : EncryptionManager.ReaderPasswordOption;
        PageDecryptionKeys pageKeys = EncryptionManager.CreatePageDecryptionKeys(header, this.Profile.Kind, isLegacyAesCfb, password, passwordOptionName);
        this.Pages = writable
            ? new Pager(stream, this.Profile.PageSize, pageKeys, leaveOpen, ownerType)
            : new PageFile(stream, this.Profile.PageSize, pageKeys, leaveOpen, ownerType);
        this.TableDefs = new TableDefReader(this.Pages, this.Profile);
    }

    internal delegate ValueTask<bool> TableRowVisitor(TableRow row, CancellationToken cancellationToken);

    internal delegate ValueTask<bool> DataPageVisitor(long pageNumber, byte[] page, CancellationToken cancellationToken);

    /// <summary>
    /// Gets the file's immutable format profile: format, page size, code page,
    /// byte layouts and text codecs. The format members below forward to it.
    /// </summary>
    internal JetFormat Profile { get; }

    /// <summary>
    /// Gets the file's page I/O: a read-only <see cref="PageFile"/> for a
    /// reader, or the writer's <see cref="Pager"/>. The I/O members below
    /// forward to it; the write members throw on a read-only file.
    /// </summary>
    internal PageFile Pages { get; }

    /// <summary>
    /// Gets the file's table-definition reader, which reads TDEF chains
    /// through <see cref="Pages"/>. The TDEF members below forward to it; the
    /// in-place TDEF write-backs stay here until the writer gets its own
    /// TDEF writer.
    /// </summary>
    internal TableDefReader TableDefs { get; }

    // ── Format-specific layouts (forwarders to Profile) ──────────────

    /// <summary>Gets per-format byte offsets within a data-page (page type 0x01) header — see <see cref="JetFormat.DataPage"/>.</summary>
    internal DataPageLayout DataPage => this.Profile.DataPage;

    /// <summary>Gets the per-format layout of an LVAL page — see <see cref="JetFormat.LvalPage"/>.</summary>
    internal LvalPageLayout LvalPage => this.Profile.LvalPage;

    /// <summary>Gets per-format byte offsets within a TDEF block plus real-idx entry size — see <see cref="JetFormat.TDef"/>.</summary>
    internal TDefHeaderLayout TDef => this.Profile.TDef;

    /// <summary>Gets per-format byte offsets within one column descriptor — see <see cref="JetFormat.ColumnDescriptor"/>.</summary>
    internal ColumnDescriptorLayout ColumnDescriptor => this.Profile.ColumnDescriptor;

    /// <summary>Gets per-format byte sizes of the in-row trailer fields — see <see cref="JetFormat.RowFields"/>.</summary>
    internal RowFieldSizes RowFields => this.Profile.RowFields;

    /// <summary>Gets the per-format real-idx and logical-idx layouts — see <see cref="JetFormat.Index"/>.</summary>
    internal IndexLayout IndexLayoutInfo => this.Profile.Index;

    /// <summary>Gets the database page size in bytes — see <see cref="JetFormat.PageSize"/>.</summary>
    internal int PageSizeBytes => this.Profile.PageSize;

    /// <summary>Gets the detected database format — see <see cref="JetFormat.Kind"/>.</summary>
    internal DatabaseFormat Format => this.Profile.Kind;

    /// <summary>Gets the database's ANSI code-page encoding — see <see cref="JetFormat.AnsiEncoding"/>.</summary>
    internal Encoding AnsiEncoding => this.Profile.AnsiEncoding;

    /// <summary>Gets the decoded database code page — see <see cref="JetFormat.CodePage"/>.</summary>
    internal int CodePage => this.Profile.CodePage;

    // ── Page I/O state (forwarders to Pages) ─────────────────────────

    /// <summary>Gets the database backing stream — see <see cref="PageFile.Stream"/>.</summary>
    internal Stream DatabaseStream => this.Pages.Stream;

    /// <summary>Gets the database path, or an empty string when opened from a caller-owned stream.</summary>
    internal string DatabasePath { get; }

    /// <summary>Gets a value indicating whether the file has been disposed — see <see cref="PageFile.IsDisposed"/>.</summary>
    internal bool IsDisposed => this.Pages.IsDisposed;

    /// <summary>
    /// Gets or sets the writer's cooperative JET byte-range lock helper — see
    /// <see cref="Pager.ByteRangeLock"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    internal JetByteRangeLock ByteRangeLock
    {
        get => this.WritablePages.ByteRangeLock;
        set => this.WritablePages.ByteRangeLock = value;
    }

    /// <summary>Gets a value indicating whether the writer has a transaction journal attached — see <see cref="Pager.IsJournalActive"/>.</summary>
    internal bool IsJournalActive => this.Pages is Pager { IsJournalActive: true };

    /// <summary>
    /// Gets a value indicating whether <see cref="EnableRandomAccessPageReadsIfSupported"/>
    /// switched page reads to <c>RandomAccess</c> reads — see <see cref="PageFile.UsesRandomAccessPageReads"/>.
    /// </summary>
    internal bool UsesRandomAccessPageReads => this.Pages.UsesRandomAccessPageReads;

    /// <summary>
    /// Gets or sets a value indicating whether page reads that start on a
    /// thread-pool thread run inline — see <see cref="PageFile.ReadsInlineOnThreadPool"/>.
    /// </summary>
    internal bool ReadsInlineOnThreadPool
    {
        get => this.Pages.ReadsInlineOnThreadPool;
        set => this.Pages.ReadsInlineOnThreadPool = value;
    }

    /// <summary>Gets the length of the backing stream — see <see cref="PageFile.LengthBytes"/>.</summary>
    internal long DatabaseLengthBytes => this.Pages.LengthBytes;

    /// <summary>
    /// Gets the end of file in pages, journal-aware in the writer — see
    /// <see cref="IPageSource.PageCount"/>.
    /// </summary>
    internal long PageCount => this.Pages.PageCount;

    internal int RowColumnCountFieldSize => this.Profile.RowFields.NumCols;

    /// <summary>Gets the writer's <see cref="Pager"/>, or throws on a read-only file.</summary>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    private Pager WritablePages => this.Pages as Pager
        ?? throw new InvalidOperationException("This database file was opened read-only; only the writer's file can write pages.");

    /// <summary>Asynchronously reads the fixed-size JET header — see <see cref="PageFile.ReadHeaderAsync"/>.</summary>
    /// <param name="fs">An open, seekable stream positioned anywhere.</param>
    /// <param name="cancellationToken">Token used to cancel the read operation.</param>
    /// <returns>A 0x80-byte header buffer.</returns>
    internal static ValueTask<byte[]> ReadHeaderAsync(Stream fs, CancellationToken cancellationToken = default)
        => PageFile.ReadHeaderAsync(fs, cancellationToken);

    /// <summary>Gives a pooled page buffer back — see <see cref="PageBuffers.Return"/>.</summary>
    /// <param name="page">The pooled page buffer.</param>
    internal static void ReturnPage(byte[] page) => PageBuffers.Return(page);

    // Little-endian primitives (Ru16/Ri32/Ru32/Ri64/Wu16/Wu32/Wi32/Wi64) and
    // float/24-bit/hex helpers live in JetTypeInfo so non-Core callers
    // (Encryption layer, index codecs, …) can use them without taking a
    // dependency on this class. They are surfaced here through the
    // file-level `using static JetDatabaseWriter.Schema.JetTypeInfo;`.

    /// <summary>Opens a database file — see <see cref="PageFile.OpenFileStream"/>.</summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="access">The access.</param>
    /// <param name="share">The share.</param>
    /// <param name="options">The options.</param>
    /// <returns>The opened stream.</returns>
    internal static FileStream OpenFileStream(string path, FileAccess access, FileShare share, FileOptions options) => PageFile.OpenFileStream(path, access, share, options);

    /// <summary>Switches page reads to positional reads — see <see cref="PageFile.EnableRandomAccessPageReadsIfSupported"/>.</summary>
    internal void EnableRandomAccessPageReadsIfSupported() => this.Pages.EnableRandomAccessPageReadsIfSupported();

    /// <summary>Sets the length of the backing stream — see <see cref="Pager.SetLengthAsync"/>.</summary>
    /// <param name="length">The new length in bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the stream is resized.</returns>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    internal ValueTask SetDatabaseLengthAsync(long length, CancellationToken cancellationToken)
        => this.WritablePages.SetLengthAsync(length, cancellationToken);

    /// <summary>Flushes the backing stream — see <see cref="Pager.FlushAsync"/>.</summary>
    /// <param name="flushToDisk">Whether to flush a file through to the device.</param>
    /// <param name="cancellationToken">A token used to cancel the flush.</param>
    /// <returns>A task that completes when the stream is flushed.</returns>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    internal ValueTask FlushDatabaseStreamAsync(bool flushToDisk, CancellationToken cancellationToken)
        => this.WritablePages.FlushAsync(flushToDisk, cancellationToken);

    /// <summary>Takes the writer's I/O gate for a journal attach or detach — see <see cref="Pager.EnterJournalGateAsync"/>.</summary>
    /// <param name="cancellationToken">A token used to cancel the wait for the gate.</param>
    /// <returns>The lease, holding the gate.</returns>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    internal ValueTask<Pager.JournalGate> EnterJournalGateAsync(CancellationToken cancellationToken)
        => this.WritablePages.EnterJournalGateAsync(cancellationToken);

    /// <summary>Detaches the writer's journal without the gate, at dispose — see <see cref="Pager.ForceDetachJournal"/>.</summary>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    internal void ForceDetachJournal() => this.WritablePages.ForceDetachJournal();

    /// <summary>
    /// Disposes the page file (marking it disposed, then the stream unless the
    /// caller kept it, then the I/O gate and the page cipher) and drops the
    /// owned-page caches.
    /// </summary>
    /// <returns>A task that completes when the file is disposed.</returns>
    public async ValueTask DisposeAsync()
    {
        if (this.IsDisposed)
        {
            return;
        }

        try
        {
            await this.Pages.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            this.DisposeOwnedPageCaches();
        }
    }

    /// <summary>
    /// Disposes owned managed resources other than the backing stream. Used by
    /// the owning reader or writer when its construction fails before an
    /// instance can be returned to the caller and disposed normally.
    /// </summary>
    internal void DisposeManagedResources()
    {
        this.Pages.DisposeManagedResources();
        this.DisposeOwnedPageCaches();
    }

    /// <summary>Throws when the file has been disposed — see <see cref="PageFile.ThrowIfDisposed"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ThrowIfDisposed() => this.Pages.ThrowIfDisposed();

    /// <summary>Throws when the file has been disposed or the token cancelled — see <see cref="PageFile.ThrowIfDisposedOrCancelled"/>.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => this.Pages.ThrowIfDisposedOrCancelled(cancellationToken);

    // Fixed-column decoding (ReadFixedString / ReadFixedTyped) lives in
    // JetTypeInfo so the per-type byte→value switch sits next to its
    // metadata siblings (GetFixedSize, GetClrType, GetTypeDisplayName).

    // ── Page I/O (forwarders to Pages) ───────────────────────────────

    /// <summary>Reads and decrypts one page into a pooled buffer — see <see cref="PageFile.ReadPageAsync"/>.</summary>
    /// <param name="n">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    /// <returns>The pooled page buffer.</returns>
    internal ValueTask<byte[]> ReadPageAsync(long n, CancellationToken cancellationToken = default)
        => this.Pages.ReadPageAsync(n, cancellationToken);

    /// <summary>Returns a caller-owned copy of a decrypted page — see <see cref="PageBuffers.ReadPageCopyAsync"/>.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The page copy.</returns>
    internal ValueTask<byte[]> ReadPageCopyAsync(long pageNumber, CancellationToken cancellationToken = default)
        => this.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);

    // ── TDEF parsing (forwarders to TableDefs) ───────────────────────

    /// <summary>Reads a TDEF page chain as logical bytes — see <see cref="TableDefReader.ReadTDefBytesAsync"/>.</summary>
    /// <param name="startPage">The start page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The logical TDEF bytes, or <see langword="null"/>.</returns>
    internal ValueTask<byte[]?> ReadTDefBytesAsync(long startPage, CancellationToken cancellationToken = default)
        => this.TableDefs.ReadTDefBytesAsync(startPage, cancellationToken);

    /// <summary>Reads a TDEF page chain that remembers its physical pages — see <see cref="TableDefReader.ReadTDefChainAsync"/>.</summary>
    /// <param name="startPage">The first TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The chain.</returns>
    internal ValueTask<LogicalTDefChain> ReadTDefChainAsync(long startPage, CancellationToken cancellationToken = default)
        => this.TableDefs.ReadTDefChainAsync(startPage, cancellationToken);

    /// <summary>
    /// Writes a chain read by <see cref="ReadTDefChainAsync"/> back in place,
    /// mapping each logical byte to the physical page that holds it. Only
    /// pages whose bytes changed are written.
    /// </summary>
    /// <param name="chain">The chain whose <see cref="LogicalTDefChain.Bytes"/> were patched.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask WriteTDefChainInPlaceAsync(LogicalTDefChain chain, CancellationToken cancellationToken = default)
        => chain.WriteInPlaceAsync(this.ReadPageAsync, ReturnPage, this.WritePageAsync, cancellationToken);

    /// <summary>
    /// Patches one 32-bit field at a logical offset of the TDEF chain rooted
    /// at <paramref name="tdefPage"/>, such as a real index's <c>first_dp</c>
    /// root pointer, which sits on a continuation page in a wide table.
    /// </summary>
    /// <param name="tdefPage">The first TDEF page.</param>
    /// <param name="logicalOffset">The field's offset in the logical TDEF buffer.</param>
    /// <param name="value">The value to write.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask WriteTDefInt32Async(long tdefPage, int logicalOffset, int value, CancellationToken cancellationToken = default)
    {
        LogicalTDefChain chain = await this.ReadTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        Wi32(chain.Bytes, logicalOffset, value);
        await this.WriteTDefChainInPlaceAsync(chain, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads and parses a table definition — see <see cref="TableDefReader.ReadTableDefAsync"/>.</summary>
    /// <param name="tdefPage">The first TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The table definition, or <see langword="null"/>.</returns>
    internal ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken = default)
        => this.TableDefs.ReadTableDefAsync(tdefPage, cancellationToken);

    /// <summary>Reads a table definition or throws — see <see cref="TableDefReader.ReadRequiredTableDefAsync"/>.</summary>
    /// <param name="tdefPage">The first TDEF page.</param>
    /// <param name="tableName">The table's name, for the error message.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The table definition.</returns>
    internal ValueTask<TableDef> ReadRequiredTableDefAsync(long tdefPage, string tableName, CancellationToken cancellationToken = default)
        => this.TableDefs.ReadRequiredTableDefAsync(tdefPage, tableName, cancellationToken);

    /// <summary>Reads the per-row column count at <paramref name="rowStart"/> — see <see cref="JetFormat.ReadRowColumnCount"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int ReadRowColumnCount(byte[] page, int rowStart) => this.Profile.ReadRowColumnCount(page, rowStart);

    /// <summary>Decodes a text/memo slice — see <see cref="JetFormat.DecodeText"/>.</summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal string DecodeTextForFormat(byte[] bytes, int start, int len) => this.Profile.DecodeText(bytes, start, len);

    /// <summary>Encodes a string for storage — see <see cref="JetFormat.EncodeText(string, bool)"/>.</summary>
    /// <param name="value">The value.</param>
    /// <param name="compress">The compress.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeTextForFormat(string value, bool compress = true) => this.Profile.EncodeText(value, compress);

    /// <summary>Encodes a string for storage with a byte cap — see <see cref="JetFormat.EncodeText(string, int, bool)"/>.</summary>
    /// <param name="value">The value.</param>
    /// <param name="maxBytes">The max bytes.</param>
    /// <param name="compress">The compress.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeTextForFormat(string value, int maxBytes, bool compress = true) => this.Profile.EncodeText(value, maxBytes, compress);

    /// <summary>Encodes Jet3 text or a Jet3 name in the code page — see <see cref="JetFormat.EncodeAnsiText"/>.</summary>
    /// <param name="value">The text.</param>
    /// <returns>The code-page bytes.</returns>
    internal byte[] EncodeAnsiText(string value) => this.Profile.EncodeAnsiText(value);

    /// <summary>Describes the first character the database cannot store — see <see cref="JetFormat.DescribeUnstorableCharacter"/>.</summary>
    /// <param name="text">The name or value.</param>
    /// <returns>The character and its code point, or <see langword="null"/>.</returns>
    internal string? DescribeUnstorableCharacter(string text) => this.Profile.DescribeUnstorableCharacter(text);

    /// <summary>Builds the message for unstorable text — see <see cref="JetFormat.UnstorableTextMessage"/>.</summary>
    /// <param name="subject">What the text is.</param>
    /// <param name="character">The description of the character.</param>
    /// <returns>The message.</returns>
    internal string UnstorableTextMessage(string subject, string character) => this.Profile.UnstorableTextMessage(subject, character);

    /// <summary>Reads one TDEF column name — see <see cref="JetFormat.ReadColumnName"/>.</summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="pos">The byte position.</param>
    /// <param name="name">The name.</param>
    internal int ReadColumnName(byte[] td, ref int pos, out string name) => this.Profile.ReadColumnName(td, ref pos, out name);

    /// <summary>Encodes a TDEF name record — see <see cref="JetFormat.EncodeTDefNameRecord"/>.</summary>
    /// <param name="name">The column or index name.</param>
    /// <returns>The length-prefixed name record.</returns>
    internal byte[] EncodeTDefNameRecord(string name) => this.Profile.EncodeTDefNameRecord(name);

    // ── Page write I/O (forwarders to the writer's Pager) ────────────

    /// <summary>Writes one page in place, or buffers it in the transaction journal — see <see cref="Pager.WritePageAsync"/>.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The plaintext page.</param>
    /// <param name="cancellationToken">A token used to cancel the write.</param>
    /// <returns>A task that completes when the page is written or buffered.</returns>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    internal ValueTask WritePageAsync(long pageNumber, byte[] page, CancellationToken cancellationToken = default)
        => this.WritablePages.WritePageAsync(pageNumber, page, cancellationToken);

    /// <summary>Appends one page at the end of file — see <see cref="Pager.AppendPageAsync"/>.</summary>
    /// <param name="page">The plaintext page.</param>
    /// <param name="cancellationToken">A token used to cancel the append.</param>
    /// <returns>The appended page's number.</returns>
    /// <exception cref="InvalidOperationException">The file is read-only.</exception>
    internal ValueTask<long> AppendPageAsync(byte[] page, CancellationToken cancellationToken = default)
        => this.WritablePages.AppendPageAsync(page, cancellationToken);

    // ── Table page enumeration ───────────────────────────────────────

    internal async ValueTask<IReadOnlyList<long>> GetOwnedDataPagesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        if (tdefPage <= 0)
        {
            return [];
        }

        bool canUseCache = !this.writable;
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

    /// <summary>
    /// Visits every live row of the table rooted at <paramref name="tdefPage"/>, in
    /// page and slot order, including overflow rows (see <see cref="ForEachRowOnPageAsync"/>).
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="visitRowAsync">Called for each row; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask ForEachLiveTableRowAsync(
        long tdefPage,
        TableRowVisitor visitRowAsync,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(visitRowAsync, nameof(visitRowAsync));

        await this.ForEachOwnedDataPageAsync(
            tdefPage,
            (pageNumber, page, token) => this.ForEachRowOnPageAsync(pageNumber, page, visitRowAsync, token),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Visits every live row of the data page <paramref name="pageNumber"/> in slot
    /// order. An overflow row is visited at its header slot: its location keeps the
    /// header as <see cref="RowLocation.PageNumber"/> / <see cref="RowLocation.RowIndex"/>
    /// and names the moved bytes through <see cref="RowLocation.DataPageNumber"/> /
    /// <see cref="RowLocation.DataRowIndex"/>, and <see cref="TableRow.Page"/> holds the
    /// page with those bytes. An overflow pointer that cannot be resolved skips the
    /// row, as an undecodable row is skipped. Visitors must not keep
    /// <see cref="TableRow.Page"/> after they return.
    /// </summary>
    /// <param name="pageNumber">The data page's number.</param>
    /// <param name="page">The data page's bytes.</param>
    /// <param name="visitRowAsync">Called for each row; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="false"/> when the visitor stopped the walk.</returns>
    internal async ValueTask<bool> ForEachRowOnPageAsync(
        long pageNumber,
        byte[] page,
        TableRowVisitor visitRowAsync,
        CancellationToken cancellationToken)
    {
        foreach (RowBound entry in this.ComputeRowDirectory(page))
        {
            if (!entry.IsOverflowPointer)
            {
                var location = new RowLocation(pageNumber, entry.RowIndex, entry.RowStart, entry.RowSize);
                if (!await visitRowAsync(new TableRow(page, location), cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }

                continue;
            }

            OverflowRowTarget? resolved = await this.TryResolveOverflowRowAsync(
                page,
                entry,
                this.ReadPageAsync,
                ReturnPage,
                cancellationToken).ConfigureAwait(false);
            if (resolved is not { } target)
            {
                continue;
            }

            try
            {
                var location = new RowLocation(pageNumber, entry.RowIndex, target.Bound.RowStart, target.Bound.RowSize)
                {
                    DataPageNumber = target.PageNumber,
                    DataRowIndex = target.RowIndex,
                };
                if (!await visitRowAsync(new TableRow(target.Page, location), cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }
            }
            finally
            {
                ReturnPage(target.Page);
            }
        }

        return true;
    }

    /// <summary>
    /// Follows the overflow pointer held in <paramref name="header"/> (a row-directory
    /// entry with <see cref="RowBound.IsOverflowPointer"/> set) to the row data. The
    /// pointer is the target row index (1 byte) followed by the target page number
    /// (3 bytes, little-endian); when the target slot is itself flagged overflow, the
    /// walk continues from it, up to <see cref="Constants.DataPage.MaxOverflowHops"/>
    /// hops (Jackcess <c>TableImpl.positionAtRowData</c>). The target slot's deleted
    /// flag is ignored, because Access always flags the moved bytes deleted. Returns
    /// <see langword="null"/>, without throwing, when the pointer is shorter than four
    /// bytes, names a page outside the file, a page that is not a data page of the
    /// same table, or a slot the page does not have, or when the walk runs out of hops.
    /// </summary>
    /// <param name="headerPage">The data page holding the header slot.</param>
    /// <param name="header">The header slot's directory entry.</param>
    /// <param name="readPage">Reads a page; the walk reads each target page through it.</param>
    /// <param name="returnPage">
    /// Releases a page <paramref name="readPage"/> returned that the walk no longer needs,
    /// or <see langword="null"/> when its pages are not pooled (a page cache). The page of
    /// the returned target is not released: the caller owns it.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<OverflowRowTarget?> TryResolveOverflowRowAsync(
        byte[] headerPage,
        RowBound header,
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        Action<byte[]>? returnPage,
        CancellationToken cancellationToken)
    {
        int owner = Ri32(headerPage, this.DataPage.TDefOff);
        long totalPages = this.PageCount;
        byte[] page = headerPage;
        RowBound pointer = header;
        try
        {
            for (int hop = 0; hop < Constants.DataPage.MaxOverflowHops; hop++)
            {
                if (pointer.RowSize < Constants.DataPage.OverflowPointerSize)
                {
                    return null;
                }

                int targetRow = page[pointer.RowStart];
                long targetPageNumber = page[pointer.RowStart + 1]
                    | (page[pointer.RowStart + 2] << 8)
                    | (page[pointer.RowStart + 3] << 16);
                if (targetPageNumber <= 0 || targetPageNumber >= totalPages)
                {
                    return null;
                }

                byte[] target = await readPage(targetPageNumber, cancellationToken).ConfigureAwait(false);
                ReleaseIntermediate(page);
                page = target;

                if (target[0] != Constants.PageTypes.Data
                    || Ri32(target, this.DataPage.TDefOff) != owner
                    || !this.TryGetSlotBound(target, targetRow, out RowBound bound))
                {
                    return null;
                }

                int raw = Ru16(target, this.DataPage.RowsStart + (targetRow * 2));
                if ((raw & Constants.DataPage.OverflowRowFlag) == 0)
                {
                    page = headerPage;
                    return new OverflowRowTarget(targetPageNumber, targetRow, target, bound);
                }

                pointer = bound;
            }

            return null;
        }
        finally
        {
            ReleaseIntermediate(page);
        }

        void ReleaseIntermediate(byte[] buffer)
        {
            if (!ReferenceEquals(buffer, headerPage))
            {
                returnPage?.Invoke(buffer);
            }
        }
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

    /// <summary>
    /// Yields the bounds (row index, start offset, size) of every live (non-deleted, non-overflow)
    /// row on the given data <paramref name="page"/>. Overflow headers are left out, so
    /// this suits pages that never hold overflow rows (usage-map and LVAL pages) and
    /// callers that look for a slot by index; table scans use
    /// <see cref="ComputeRowDirectory"/> or <see cref="ForEachRowOnPageAsync"/>.
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
            int rowEnd = FindNextRowStart(positions, posCount, rowStart, this.PageSizeBytes);
            yield return new RowBound(r, rowStart, rowEnd - rowStart);
        }
    }

    /// <summary>
    /// Returns the row directory of the data <paramref name="page"/>, in slot order:
    /// every live row, plus every overflow row's header (a slot flagged
    /// <see cref="Constants.DataPage.OverflowRowFlag"/> but not deleted) with
    /// <see cref="RowBound.IsOverflowPointer"/> set and its pointer bytes as the bound.
    /// Deleted slots, including the moved bytes of overflow rows, are left out.
    /// Allocates a single <see cref="RowBound"/>[] (or <see cref="Array.Empty{T}"/>
    /// when the page has no such slot) instead of returning an iterator. Suitable as
    /// a memoization target for <see cref="ReaderPageCache"/>, where the same page may
    /// be visited by multiple streaming consumers.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    internal RowBound[] ComputeRowDirectory(byte[] page)
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
        // page's row-offset table size. Array.Sort is used because Span<int>.Sort
        // is unavailable on netstandard2.1.
        int[] rawOffsets = ArrayPool<int>.Shared.Rent(numRows);
        int[] positions = ArrayPool<int>.Shared.Rent(numRows);
        try
        {
            int posCount = 0;
            int entryCount = 0;
            for (int r = 0; r < numRows; r++)
            {
                int raw = Ru16(page, this.DataPage.RowsStart + (r * 2));
                rawOffsets[r] = raw;

                int pos = raw & Constants.DataPage.RowOffsetMask;
                if (pos > 0 && pos < this.PageSizeBytes)
                {
                    positions[posCount++] = pos;
                }

                if ((raw & Constants.DataPage.DeletedRowFlag) == 0)
                {
                    entryCount++;
                }
            }

            if (entryCount == 0)
            {
                return [];
            }

            Array.Sort(positions, 0, posCount);

            var result = new RowBound[entryCount];
            int idx = 0;
            for (int r = 0; r < numRows; r++)
            {
                int raw = rawOffsets[r];
                if ((raw & Constants.DataPage.DeletedRowFlag) != 0)
                {
                    continue;
                }

                int rowStart = raw & Constants.DataPage.RowOffsetMask;
                int rowEnd = FindNextRowStart(positions, posCount, rowStart, this.PageSizeBytes);
                bool isOverflowPointer = (raw & Constants.DataPage.OverflowRowFlag) != 0;
                result[idx++] = new RowBound(r, rowStart, rowEnd - rowStart, isOverflowPointer);
            }

            return result;
        }
        finally
        {
            ArrayPool<int>.Shared.Return(rawOffsets);
            ArrayPool<int>.Shared.Return(positions);
        }
    }

    /// <summary>
    /// Returns the end (exclusive) of the row that starts at <paramref name="rowStart"/>:
    /// the first offset in <paramref name="sortedPositions"/> strictly greater than
    /// <paramref name="rowStart"/>, or <paramref name="pageSize"/> when there is none.
    /// The offsets include deleted slots, and Access leaves deleted slots pointing
    /// at the same offset as a live row, so the search skips every entry equal to
    /// <paramref name="rowStart"/> rather than taking the neighbour of whichever
    /// equal entry a binary search lands on.
    /// </summary>
    /// <param name="sortedPositions">Every slot's masked row offset, sorted ascending.</param>
    /// <param name="count">The number of valid entries in <paramref name="sortedPositions"/>.</param>
    /// <param name="rowStart">The row's start offset.</param>
    /// <param name="pageSize">The page size, which ends the highest row.</param>
    private static int FindNextRowStart(int[] sortedPositions, int count, int rowStart, int pageSize)
    {
        int lo = 0;
        int hi = count;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (sortedPositions[mid] <= rowStart)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo < count ? sortedPositions[lo] : pageSize;
    }

    /// <summary>
    /// Returns the bounds of slot <paramref name="rowIndex"/> on the data
    /// <paramref name="page"/> whatever its flags, for following an overflow pointer
    /// to a slot Access flagged deleted. The slot must exist within the page's
    /// (clamped) row count and start past the row-offset table and inside the page;
    /// it ends at the next greater offset of any slot, as in
    /// <see cref="ComputeRowDirectory"/>.
    /// </summary>
    /// <param name="page">The data page bytes.</param>
    /// <param name="rowIndex">The slot's row index.</param>
    /// <param name="bound">Receives the slot's bounds on success.</param>
    internal bool TryGetSlotBound(byte[] page, int rowIndex, out RowBound bound)
    {
        bound = default;
        int numRows = Math.Min(Ru16(page, this.DataPage.NumRows), (page.Length - this.DataPage.RowsStart) / 2);
        if (rowIndex < 0 || rowIndex >= numRows)
        {
            return false;
        }

        int rowStart = Ru16(page, this.DataPage.RowsStart + (rowIndex * 2)) & Constants.DataPage.RowOffsetMask;
        if (rowStart < this.DataPage.RowsStart + (numRows * 2) || rowStart >= this.PageSizeBytes)
        {
            return false;
        }

        int rowEnd = this.PageSizeBytes;
        for (int r = 0; r < numRows; r++)
        {
            int candidate = Ru16(page, this.DataPage.RowsStart + (r * 2)) & Constants.DataPage.RowOffsetMask;
            if (candidate > rowStart && candidate < rowEnd)
            {
                rowEnd = candidate;
            }
        }

        bound = new RowBound(rowIndex, rowStart, rowEnd - rowStart);
        return true;
    }

    // ── Row layout decoding (forwards to RowDecodePlan; used by writer column reads) ────

    /// <summary>
    /// Parses the row-trailer metadata (numCols, null-mask position, var-table
    /// position and EOD pointer) for a row at <paramref name="rowStart"/>.
    /// Returns <see langword="false"/> when the row is too small or otherwise
    /// malformed; on success <paramref name="layout"/> is populated and can be
    /// passed to <see cref="ResolveColumnSlice"/> for any column. Jet3 offsets
    /// past 255 are resolved through the row's jump table
    /// (<see cref="Pages.Jet3JumpTable"/>).
    /// </summary>
    /// <param name="page">Data page containing the row.</param>
    /// <param name="rowStart">Offset of the row within <paramref name="page"/>.</param>
    /// <param name="rowSize">Total size of the row in bytes.</param>
    /// <param name="hasVarColumns">When <see langword="false"/>, the var-length
    /// metadata is not read (no varLen byte, no jump table, no var-offset
    /// table, no EOD marker): Access omits it for tables with zero
    /// variable-length columns, and the writer's unused trailer is skipped.</param>
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
    /// here. Used by catalog walks that only need scalar metadata columns.
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

    /// <summary>
    /// Reads <paramref name="columnOrdinals"/>'s typed values out of a single
    /// row at <paramref name="loc"/> on a data page belonging to
    /// <paramref name="tableDef"/>. Returns <see langword="null"/> when the
    /// row layout cannot be parsed OR when any requested column needs
    /// long-value (Memo, Ole) or complex (Complex, Attachment)
    /// traversal outside this inline reader; the cascade-seek caller falls back to the snapshot
    /// path in that case. Index-key column types (the focus of this helper)
    /// usually include scalar fixed and var-inline kinds. Memo is indexable
    /// but routes through the snapshot path when pre-write uniqueness checks
    /// need existing-row values; OLE / Attachment / Complex columns are
    /// rejected by <see cref="Indexes.Helpers.IndexHelpers.ResolveIndexes"/>.
    /// </summary>
    /// <param name="loc">The row location.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="columnOrdinals">The column ordinals.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<object?[]?> TryReadColumnValuesTypedAsync(
        RowLocation loc,
        TableDef tableDef,
        int[] columnOrdinals,
        CancellationToken cancellationToken)
    {
        byte[] pageBytes = await this.ReadPageAsync(loc.DataPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (pageBytes[0] != Constants.PageTypes.Data)
            {
                return null;
            }

            var decodePlan = RowDecodePlan.CreatePartial(tableDef, columnOrdinals);
            object?[] result = new object?[columnOrdinals.Length];
            return decodePlan.TryDecodePartialColumns(this, pageBytes, loc.RowStart, loc.RowSize, result)
                ? result
                : null;
        }
        finally
        {
            ReturnPage(pageBytes);
        }
    }

    /// <summary>Drops the memoized owned data pages and the whole-file owner index.</summary>
    private void DisposeOwnedPageCaches()
    {
        this.ownedDataPagesByTdef.Clear();
        this.ownedDataPageIndex.Dispose();
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
        // Journal-aware: a table created or grown inside a transaction owns
        // pages appended past the physical end of the file.
        long totalPages = this.PageCount;
        if (tdefPage <= 0 || tdefPage >= totalPages)
        {
            return null;
        }

        byte[] tdef = await this.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (tdef[0] != Constants.PageTypes.TableDefinition
                || !UsageMap.TryReadPointer(tdef, this.TDef.UsedPages, out UsageMapPointer pointer)
                || pointer.PageNumber <= 0)
            {
                return null;
            }

            uint declaredRows = Ru32(tdef, this.TDef.NumRows);
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
                    // Each overflow header counts once, as Access counts the row in num_rows.
                    liveRows += this.ComputeRowDirectory(page).Length;
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
        long totalPages = this.PageCount;

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

    // ── Inner types ──────────────────────────────────────────────────

    internal readonly record struct TableRow(byte[] Page, RowLocation Location);
}
