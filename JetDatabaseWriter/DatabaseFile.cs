namespace JetDatabaseWriter;

using System;
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
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// One open JET database file, composed of its parts: the format profile
/// (<see cref="JetFormat"/>), the page I/O (a read-only <see cref="PageFile"/>
/// or the writer's <see cref="Pager"/>), the TDEF parser
/// (<see cref="TableDefReader"/>) and the owned-page and row enumeration
/// (<see cref="OwnedDataPages"/>, <see cref="DataPageRows"/>). It forwards to
/// them, except for the row format it copies for the row decoder, and keeps
/// only the in-place TDEF write-backs, which move to the writer's TDEF writer
/// in core-split-b. Shared by the reader and writer service graphs; it knows
/// nothing about either facade.
/// </summary>
internal sealed class DatabaseFile : IAsyncDisposable
{
    /// <summary>
    /// Whether the file is Jet3, copied from <see cref="Profile"/> with the row
    /// format below, so the row column count and text decoders branch on it as
    /// <see cref="JetFormat"/> does.
    /// </summary>
    private readonly bool isJet3;

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
    /// throw, and each table's TDEF bytes and owned data pages are memoized
    /// until it is disposed. Nothing writes through it, and the reader assumes
    /// that no other process changes the file while it is open: a change made
    /// after a table was read is not seen.
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
        this.DatabasePath = path ?? string.Empty;

        // The profile detects the format and decodes the code page; neither
        // step can throw, so a password error below is still the first error
        // an encrypted file reports, as when the code page was decoded after
        // the page keys.
        this.Profile = JetFormat.FromHeader(header);
        this.Format = this.Profile.Kind;
        this.isJet3 = this.Profile.IsJet3;
        this.RowFields = this.Profile.RowFields;
        this.AnsiEncoding = this.Profile.AnsiEncoding;
        bool isLegacyAesCfb = EncryptionManager.IsCompoundFileEncrypted(header);
        string passwordOptionName = ownerType == typeof(AccessWriter)
            ? EncryptionManager.WriterPasswordOption
            : EncryptionManager.ReaderPasswordOption;
        PageDecryptionKeys pageKeys = EncryptionManager.CreatePageDecryptionKeys(header, this.Profile.Kind, isLegacyAesCfb, password, passwordOptionName);
        this.Pages = writable
            ? new Pager(stream, this.Profile.PageSize, pageKeys, leaveOpen, ownerType)
            : new PageFile(stream, this.Profile.PageSize, pageKeys, leaveOpen, ownerType);
        this.TableDefs = new TableDefReader(this.Pages, this.Profile, cacheResults: !writable);
        this.OwnedPages = new OwnedDataPages(this.Pages, this.Profile, cacheResults: !writable);
    }

    /// <summary>
    /// Gets the file's immutable format profile: format, page size, code page,
    /// byte layouts and text codecs. The format members below forward to it,
    /// except the row format, which the constructor copies from it.
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
    /// through <see cref="Pages"/> and memoizes their bytes only on a
    /// read-only file. The TDEF members below forward to it; the in-place
    /// TDEF write-backs stay here until the writer gets its own TDEF writer.
    /// </summary>
    internal TableDefReader TableDefs { get; }

    /// <summary>
    /// Gets the file's owned-page discovery and row enumeration, which reads
    /// through <see cref="Pages"/> and memoizes only on a read-only file. The
    /// owned-page and row members below forward to it.
    /// </summary>
    internal OwnedDataPages OwnedPages { get; }

    // ── Row format (copied from Profile) ─────────────────────────────
    //
    // The row decoder reads these per row and per column. Read through
    // Profile, each read takes a second dependent load, which shows on wide
    // rows, so the constructor copies them, and the Jet3 flag, once from the
    // immutable profile.

    /// <summary>Gets the detected database format — <see cref="JetFormat.Kind"/>, copied from <see cref="Profile"/>.</summary>
    internal DatabaseFormat Format { get; }

    /// <summary>Gets per-format byte sizes of the in-row trailer fields — <see cref="JetFormat.RowFields"/>, copied from <see cref="Profile"/>.</summary>
    internal RowFieldSizes RowFields { get; }

    /// <summary>Gets the database's ANSI code-page encoding — <see cref="JetFormat.AnsiEncoding"/>, copied from <see cref="Profile"/>.</summary>
    internal Encoding AnsiEncoding { get; }

    /// <summary>Gets the byte size of a row's column-count field: 1 on Jet3, 2 on Jet4 and ACE.</summary>
    internal int RowColumnCountFieldSize => this.RowFields.NumCols;

    // ── Format-specific layouts (forwarders to Profile) ──────────────

    /// <summary>Gets per-format byte offsets within a data-page (page type 0x01) header — see <see cref="JetFormat.DataPage"/>.</summary>
    internal DataPageLayout DataPage => this.Profile.DataPage;

    /// <summary>Gets the per-format layout of an LVAL page — see <see cref="JetFormat.LvalPage"/>.</summary>
    internal LvalPageLayout LvalPage => this.Profile.LvalPage;

    /// <summary>Gets per-format byte offsets within a TDEF block plus real-idx entry size — see <see cref="JetFormat.TDef"/>.</summary>
    internal TDefHeaderLayout TDef => this.Profile.TDef;

    /// <summary>Gets per-format byte offsets within one column descriptor — see <see cref="JetFormat.ColumnDescriptor"/>.</summary>
    internal ColumnDescriptorLayout ColumnDescriptor => this.Profile.ColumnDescriptor;

    /// <summary>Gets the per-format real-idx and logical-idx layouts — see <see cref="JetFormat.Index"/>.</summary>
    internal IndexLayout IndexLayoutInfo => this.Profile.Index;

    /// <summary>Gets the database page size in bytes — see <see cref="JetFormat.PageSize"/>.</summary>
    internal int PageSizeBytes => this.Profile.PageSize;

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
    /// caller kept it, then the I/O gate and the page cipher), then the TDEF
    /// and owned-page caches.
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
            this.TableDefs.Dispose();
            this.OwnedPages.Dispose();
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
        this.TableDefs.Dispose();
        this.OwnedPages.Dispose();
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

    /// <summary>
    /// Reads the per-row column count at <paramref name="rowStart"/> as
    /// <see cref="JetFormat.ReadRowColumnCount"/> does, from the Jet3 flag
    /// copied from <see cref="Profile"/>: one byte on Jet3, a 16-bit
    /// little-endian word on Jet4/ACE.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int ReadRowColumnCount(byte[] page, int rowStart)
        => this.isJet3 ? page[rowStart] : Ru16(page, rowStart);

    /// <summary>
    /// Decodes a text/memo slice as <see cref="JetFormat.DecodeText"/> does,
    /// from the Jet3 flag and the <see cref="AnsiEncoding"/> copied from
    /// <see cref="Profile"/>: Jet4 compressed/UCS-2 or Jet3 ANSI. Empty slices
    /// return <see cref="string.Empty"/>.
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

        return this.isJet3 ? this.AnsiEncoding.GetString(bytes, start, len) : DecodeJet4Text(bytes, start, len);
    }

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

    // ── Owned pages and rows (forwarders to OwnedPages, DataPageRows and the column readers) ──

    /// <summary>Returns the data pages a table owns — see <see cref="OwnedDataPages.GetOwnedDataPagesAsync"/>.</summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The owned data pages.</returns>
    internal ValueTask<IReadOnlyList<long>> GetOwnedDataPagesAsync(long tdefPage, CancellationToken cancellationToken)
        => this.OwnedPages.GetOwnedDataPagesAsync(tdefPage, cancellationToken);

    /// <summary>Visits every live row of a table — see <see cref="OwnedDataPages.ForEachLiveTableRowAsync"/>.</summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="visitRowAsync">Called for each row; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the walk ends.</returns>
    internal ValueTask ForEachLiveTableRowAsync(long tdefPage, OwnedDataPages.TableRowVisitor visitRowAsync, CancellationToken cancellationToken)
        => this.OwnedPages.ForEachLiveTableRowAsync(tdefPage, visitRowAsync, cancellationToken);

    /// <summary>Visits every live row of one data page — see <see cref="OwnedDataPages.ForEachRowOnPageAsync"/>.</summary>
    /// <param name="pageNumber">The data page's number.</param>
    /// <param name="page">The data page's bytes.</param>
    /// <param name="visitRowAsync">Called for each row; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="false"/> when the visitor stopped the walk.</returns>
    internal ValueTask<bool> ForEachRowOnPageAsync(long pageNumber, byte[] page, OwnedDataPages.TableRowVisitor visitRowAsync, CancellationToken cancellationToken)
        => this.OwnedPages.ForEachRowOnPageAsync(pageNumber, page, visitRowAsync, cancellationToken);

    /// <summary>Follows an overflow row's pointer to its bytes — see <see cref="OwnedDataPages.TryResolveOverflowRowAsync"/>.</summary>
    /// <param name="headerPage">The data page holding the header slot.</param>
    /// <param name="header">The header slot's directory entry.</param>
    /// <param name="readPage">Reads a page.</param>
    /// <param name="returnPage">Releases a page <paramref name="readPage"/> returned, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The target slot and its page, or <see langword="null"/>.</returns>
    internal ValueTask<OverflowRowTarget?> TryResolveOverflowRowAsync(
        byte[] headerPage,
        RowBound header,
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        Action<byte[]>? returnPage,
        CancellationToken cancellationToken)
        => this.OwnedPages.TryResolveOverflowRowAsync(headerPage, header, readPage, returnPage, cancellationToken);

    /// <summary>Visits each data page a table owns — see <see cref="OwnedDataPages.ForEachOwnedDataPageAsync"/>.</summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="visitPageAsync">Called for each data page; returns <see langword="false"/> to stop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the walk ends.</returns>
    internal ValueTask ForEachOwnedDataPageAsync(long tdefPage, OwnedDataPages.DataPageVisitor visitPageAsync, CancellationToken cancellationToken)
        => this.OwnedPages.ForEachOwnedDataPageAsync(tdefPage, visitPageAsync, cancellationToken);

    /// <summary>Yields a data page's live, non-overflow rows — see <see cref="DataPageRows.EnumerateLiveRowBounds"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <returns>The live rows' bounds.</returns>
    internal IEnumerable<RowBound> EnumerateLiveRowBounds(byte[] page) => DataPageRows.EnumerateLiveRowBounds(this.Profile, page);

    /// <summary>Returns a data page's row directory — see <see cref="DataPageRows.ComputeRowDirectory"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <returns>The row directory.</returns>
    internal RowBound[] ComputeRowDirectory(byte[] page) => DataPageRows.ComputeRowDirectory(this.Profile, page);

    /// <summary>Returns one slot's bounds whatever its flags — see <see cref="DataPageRows.TryGetSlotBound"/>.</summary>
    /// <param name="page">The data page bytes.</param>
    /// <param name="rowIndex">The slot's row index.</param>
    /// <param name="bound">Receives the slot's bounds on success.</param>
    /// <returns><see langword="true"/> when the slot exists.</returns>
    internal bool TryGetSlotBound(byte[] page, int rowIndex, out RowBound bound) => DataPageRows.TryGetSlotBound(this.Profile, page, rowIndex, out bound);

    /// <summary>Yields a data page's live rows as locations — see <see cref="DataPageRows.EnumerateLiveRowLocations"/>.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    /// <returns>The live rows' locations.</returns>
    internal IEnumerable<RowLocation> EnumerateLiveRowLocations(long pageNumber, byte[] page) => DataPageRows.EnumerateLiveRowLocations(this.Profile, pageNumber, page);

    /// <summary>Reads one scalar column as a string — see <see cref="ScalarColumnReader.DecodeSimpleColumnValue"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="column">The column.</param>
    /// <returns>The value as a string, or <see cref="string.Empty"/>.</returns>
    internal string DecodeSimpleColumnValue(byte[] page, int rowStart, int rowSize, ColumnInfo column)
        => ScalarColumnReader.DecodeSimpleColumnValue(this.Profile, page, rowStart, rowSize, column);

    /// <summary>Reads a few columns of one row as typed values — see <see cref="PartialColumnReader.TryReadColumnValuesTypedAsync"/>.</summary>
    /// <param name="loc">The row location.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="columnOrdinals">The column ordinals.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The values, or <see langword="null"/>.</returns>
    internal ValueTask<object?[]?> TryReadColumnValuesTypedAsync(RowLocation loc, TableDef tableDef, int[] columnOrdinals, CancellationToken cancellationToken)
        => PartialColumnReader.TryReadColumnValuesTypedAsync(this, loc, tableDef, columnOrdinals, cancellationToken);
}
