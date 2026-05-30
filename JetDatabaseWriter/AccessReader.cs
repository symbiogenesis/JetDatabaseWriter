namespace JetDatabaseWriter;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// <para>
/// Pure-managed reader for Microsoft Access JET databases (.mdb / .accdb).
/// No OleDB, ODBC, or ACE/Jet driver installation required.
/// </para>
/// <para>
/// Supported formats:
/// </para>
/// <list type="bullet">
///   <item><description>Jet3 – Access 97 (.mdb)</description></item>
///   <item><description>Jet4+ – Access 2000-2019 (.mdb / .accdb)</description></item>
/// </list>
/// <para>
/// Features:
/// </para>
/// <list type="bullet">
///   <item><description>All standard data types (Text, Integer, Date, GUID, Currency, etc.).</description></item>
///   <item><description>MEMO fields (inline, single-page, and multi-page LVAL chains).</description></item>
///   <item><description>OLE Object fields — auto-detects images (JPEG/PNG/GIF/BMP), documents (PDF/DOC/RTF), and archives (ZIP).</description></item>
///   <item><description>Streaming API — process millions of rows without OOM (StreamRows, ReadTable).</description></item>
///   <item><description>Progress reporting — IProgress&lt;int&gt; callbacks for long operations.</description></item>
///   <item><description>Page cache — 256-page LRU cache (default 1 MB) for 50%+ performance boost.</description></item>
///   <item><description>Catalog caching — single MSysObjects scan, reused across calls.</description></item>
///   <item><description>Non-Western text — auto-detects code page from database header (Cyrillic, Japanese, etc.).</description></item>
///   <item><description>Password-protected databases — supports the implemented Jet/ACE encryption formats.</description></item>
/// </list>
/// <para>
/// Limitations:
/// </para>
/// <list type="bullet">
///   <item><description>Attachment and multi-value complex fields — decoded via hidden flat tables.</description></item>
///   <item><description>Access-file linked tables — read-through via trusted source paths.</description></item>
///   <item><description>CSV/text linked tables — managed string-valued delimited-text read-through via trusted source paths.</description></item>
///   <item><description>ODBC linked tables — metadata only.</description></item>
///   <item><description>Overflow rows (span multiple pages) — silently skipped (rare edge case).</description></item>
/// </list>
/// <para>
/// Based on the <see href="https://github.com/mdbtools/mdbtools/blob/master/HACKING.md">mdbtools format specification</see>.
/// </para>
/// </summary>
public sealed class AccessReader : AccessBase, IAccessReader
{
    private const int MinimumTableScanReadAheadCacheSlots = 3;

#if NET8_0_OR_GREATER
    private static readonly SearchValues<byte> OlePayloadSignatureFirstBytes = SearchValues.Create([0x25, 0x42, 0x47, 0x49, 0x4D, 0x50, 0x7B, 0x89, 0xD0, 0xFF]);
#else
    private static readonly byte[] OlePayloadSignatureFirstBytes = [0x25, 0x42, 0x47, 0x49, 0x4D, 0x50, 0x7B, 0x89, 0xD0, 0xFF];
#endif

    private readonly AsyncReentrantOperationGate operationGate = new();
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via DisposeReaderResourcesAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    private readonly AsyncLazyInitializer<Dictionary<long, long[]>> ownedDataPageIndex;
#if NET8_0_OR_GREATER
    private readonly Lock ownedDataPagesCacheLock = new();
#else
    private readonly object ownedDataPagesCacheLock = new();
#endif
    private readonly Dictionary<long, long[]> ownedDataPagesByTdef = [];
    private readonly LockFileCoordinator lockFile;
    private readonly bool strictParsing;
    private readonly ComplexColumnReader complexColumns;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in DisposeReaderResourcesAsync, invoked via LockFileCoordinator.DisposeAfterAsync.")]
    private readonly LruCache<long, byte[]>? pageCache;
    private readonly ValueDecoding.LongValueDecoder longValueDecoder;

    /// <summary>
    /// Memoize the parsed live-row directory per data page. Same eviction
    /// profile as pageCache (sized 1:1 with it) so a page that's still hot in
    /// the byte-cache also keeps its bounds array. Stale entries left behind
    /// after a page is evicted from pageCache simply age out of this LRU on
    /// their own — correctness doesn't depend on the two caches being kept in
    /// lock-step.
    /// </summary>
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in DisposeReaderResourcesAsync, invoked via LockFileCoordinator.DisposeAfterAsync.")]
    private readonly LruCache<long, RowBound[]>? rowBoundsCache;

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReader"/> class.
    /// Opens <paramref name="path"/> and detects the JET version.
    /// </summary>
    /// <param name="path">The path to the Access database file. May be empty when opened from a stream.</param>
    /// <param name="options">Options for configuring the AccessReader.</param>
    /// <param name="stream">An open, seekable stream for the database file.</param>
    /// <param name="hdr">Header bytes read from page 0.</param>
    /// <param name="leaveOpen">Whether the caller retains ownership of the stream. If false, the stream is disposed when the reader is disposed.</param>
    /// <param name="suppressPageCache">Whether to skip allocating the per-reader page caches regardless of options.</param>
    private AccessReader(
        string path,
        AccessReaderOptions options,
        Stream stream,
        byte[] hdr,
        bool leaveOpen = false,
        bool suppressPageCache = false)
        : base(stream, hdr, path, leaveOpen)
    {
        Guard.NotNull(options, nameof(options));

        this.ownedDataPageIndex = new(this.BuildOwnedDataPageIndexAsync);
        this.lockFile = LockFileCoordinator.ForReader(path, options);
        this.strictParsing = options.StrictParsing;
        this.complexColumns = new ComplexColumnReader(this);
        this.LinkedSourceOpenOptions = LinkedTableManager.CreateLinkedSourceOpenOptions(options, path);
        ReadOnlyMemory<char> password = this.LinkedSourceOpenOptions.Password;

        this.DiagnosticsEnabled = options.DiagnosticsEnabled;
        this.PageCacheSize = options.PageCacheSize;
        this.ParallelPageReadsEnabled = options.ParallelPageReadsEnabled;

        // Cache is created up front when enabled (>0); negative or zero leaves
        // it null and ReadPageCachedAsync bypasses caching entirely.
        if (!suppressPageCache && this.PageCacheSize > 0)
        {
            this.pageCache = new LruCache<long, byte[]>(this.PageCacheSize, ReturnPage);
            this.rowBoundsCache = new LruCache<long, RowBound[]>(this.PageCacheSize);
        }

        this.longValueDecoder = new ValueDecoding.LongValueDecoder(this);

        bool isAccdbCfbEncrypted = EncryptionManager.IsCompoundFileEncrypted(hdr);
        (this.PageKeys.Rc4DbKey, this.PageKeys.AesPageKey) =
            EncryptionManager.ResolveReaderPageKeys(hdr, this.Format, isAccdbCfbEncrypted, password);

        if (isAccdbCfbEncrypted)
        {
            // ACCDB AES (legacy synthetic CFB header path): page-level
            // decryption is now configured; skip catalog validation because
            // the header bytes themselves are still raw CFB until ReadPageAsync
            // decrypts page 1+ on first access.
            return;
        }

        if (options.ValidateOnOpen)
        {
            this.ValidateDatabaseFormat();
        }

        // Release the lock-file slot if post-acquire setup throws. OpenAsync's
        // catch only owns the stream and never sees this half-built reader.
        this.lockFile.Acquire();
        try
        {
            this.ByteRangeLockCore = JetByteRangeLock.Create(stream, options.UseByteRangeLocks, options.LockTimeoutMilliseconds);
        }
        catch
        {
            this.lockFile.Dispose();
            throw;
        }
    }

    /// <summary>Gets a value indicating whether to print console logs with verbose hex dumps for debugging. Default: false.</summary>
    public bool DiagnosticsEnabled { get; }

    /// <summary>Gets the maximum number of pages to keep in cache. Positive values enable caching; 0 or negative disables it. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; } = 256;

    /// <summary>Gets a value indicating whether asynchronous full-table reads use parallel processing for reading multiple pages. Can improve performance for large tables. Default: false.</summary>
    public bool ParallelPageReadsEnabled { get; }

    /// <summary>Gets diagnostic output populated after each call to <see cref="ListTablesAsync"/>.</summary>
    public string LastDiagnostics { get; private set; } = string.Empty;

    /// <summary>Gets the absolute path of the database backing this reader, or empty when opened from a stream. Used by <see cref="LinkedTableManager"/> to anchor relative source paths.</summary>
    internal string HostDatabasePath => this.DatabasePath;

    /// <summary>
    /// Gets the cached options used to re-open linked-source databases referenced
    /// by this reader. Carries the normalised allowlist (resolved against the host
    /// database directory) and the optional path validator on its own properties,
    /// so transitively linked databases inherit the same security policy.
    /// </summary>
    internal AccessReaderOptions LinkedSourceOpenOptions { get; }

    /// <summary>
    /// Asynchronously opens a JET database file and returns a new <see cref="AccessReader"/> instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessReader"/> for the specified database.</returns>
    public static ValueTask<AccessReader> OpenAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
        => OpenAsync(path, options, suppressPageCache: false, cancellationToken);

    internal static ValueTask<AccessReader> OpenUncachedAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
        => OpenAsync(path, options, suppressPageCache: true, cancellationToken);

    private static async ValueTask<AccessReader> OpenAsync(
        string path,
        AccessReaderOptions? options,
        bool suppressPageCache,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        options ??= new AccessReaderOptions();

        // CA2000: OpenAsync(stream, leaveOpen:false) intentionally takes ownership and disposes on all paths.
#pragma warning disable CA2000
        FileStream fs = CreateStream(path, options);
#pragma warning restore CA2000
        AccessReader reader = await OpenAsync(
            fs,
            options,
            leaveOpen: false,
            suppressPageCache: suppressPageCache,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        if (reader.ParallelPageReadsEnabled)
        {
            reader.EnableRandomAccessPageReadsIfSupported();
        }

        return reader;
    }

    /// <summary>
    /// Asynchronously opens a JET database from a caller-supplied <see cref="Stream"/> and returns a new <see cref="AccessReader"/> instance.
    /// The stream must be readable and seekable. The caller retains ownership unless <paramref name="leaveOpen"/> is false (the default),
    /// in which case the stream will be disposed when the reader is disposed.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="leaveOpen">If <c>true</c>, the stream is not disposed when the reader is disposed. Default is <c>false</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessReader"/> for the database.</returns>
    public static ValueTask<AccessReader> OpenAsync(Stream stream, AccessReaderOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
        => OpenAsync(stream, options, leaveOpen, suppressPageCache: false, cancellationToken);

    internal static ValueTask<AccessReader> OpenUncachedAsync(Stream stream, AccessReaderOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
        => OpenAsync(stream, options, leaveOpen, suppressPageCache: true, cancellationToken);

    private static async ValueTask<AccessReader> OpenAsync(
        Stream stream,
        AccessReaderOptions? options,
        bool leaveOpen,
        bool suppressPageCache,
        CancellationToken cancellationToken)
    {
        Guard.RequireReadableSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        options ??= new AccessReaderOptions();
        try
        {
            string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
            byte[] header = await ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);

            // Office Crypto API ("Agile") encryption: the file is a real OLE
            // compound document with EncryptionInfo + EncryptedPackage streams.
            // EncryptionManager handles detection, password verification, and
            // package decryption; on success we re-enter on the inner ACCDB
            // bytes.
            byte[]? decryptedAgile = await EncryptionManager
                .TryDecryptAgileCompoundFileAsync(stream, header, options.Password, cancellationToken)
                .ConfigureAwait(false);
            if (decryptedAgile != null)
            {
                // We no longer need the source stream: dispose it unless the
                // caller retains ownership via leaveOpen.
                if (!leaveOpen)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }

                var inner = new MemoryStream(decryptedAgile, writable: false);
                byte[] innerHeader = await ReadHeaderAsync(inner, cancellationToken).ConfigureAwait(false);
                return new AccessReader(string.Empty, options, inner, innerHeader, suppressPageCache: suppressPageCache);
            }

            return new AccessReader(path, options, stream, header, leaveOpen, suppressPageCache);
        }
        catch
        {
            if (!leaveOpen)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public async ValueTask<DataTable> ReadFirstTableAsStringsAsync(uint? maxRows = null, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        if (tables.Count == 0)
        {
            return new DataTable();
        }

        CatalogEntry entry = tables[0];
        TableDef? td = await this.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null || td.Columns.Count == 0)
        {
            return new DataTable(entry.Name);
        }

        DataTable? dt = null;
        try
        {
            dt = new DataTable(entry.Name);
            foreach (ColumnInfo col in td.Columns)
            {
                _ = dt.Columns.Add(col.Name, typeof(string));
            }

            IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

            await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                await foreach (string[] row in this.EnumerateRowsAsync(scanPage.PageNumber, scanPage.Page, td, cancellationToken).ConfigureAwait(false))
                {
                    _ = dt.Rows.Add(row);
                    if (maxRows.HasValue && dt.Rows.Count >= maxRows.Value)
                    {
                        DataTable result = dt;
                        dt = null;
                        return result;
                    }
                }
            }

            DataTable final = dt;
            dt = null;
            return final;
        }
        finally
        {
            dt?.Dispose();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<List<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        List<LinkedTableInfo> links = await this.GetLinkedTablesCachedAsync(cancellationToken).ConfigureAwait(false);
        return links.ConvertAll(static link => link with { }); // Clone to detach from internal cache instances
    }

    /// <inheritdoc/>
    public async ValueTask<List<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> entries = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        var result = new List<TableStat>(entries.Count);

        foreach (CatalogEntry entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TableDef? td = await this.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            result.Add(new TableStat
            {
                Name = entry.Name,
                RowCount = td?.RowCount ?? 0L,
                ColumnCount = td?.Columns.Count ?? 0,
            });
        }

        return result;
    }

    /// <inheritdoc/>
    public async ValueTask<DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        DataTable? dt = null;
        try
        {
            dt = new DataTable("Tables");
            _ = dt.Columns.Add("TableName", typeof(string));
            _ = dt.Columns.Add("RowCount", typeof(long));
            _ = dt.Columns.Add("ColumnCount", typeof(int));

            List<TableStat> stats = await this.GetTableStatsAsync(cancellationToken).ConfigureAwait(false);
            foreach (TableStat s in stats)
            {
                _ = dt.Rows.Add(s.Name, s.RowCount, s.ColumnCount);
            }

            DataTable result = dt;
            dt = null;
            return result;
        }
        finally
        {
            dt?.Dispose();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            long? linkedCount = await this.TryGetLinkedTableRowCountAsync(tableName, cancellationToken).ConfigureAwait(false);
            return linkedCount ?? 0;
        }

        long count = 0;
        long tdefPage = resolved.Value.Entry.TDefPage;
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageCachedAsync(pageNumber, cancellationToken).ConfigureAwait(false);

            int numRows = Ru16(page, this.DataPage.NumRows);
            for (int r = 0; r < numRows; r++)
            {
                int raw = Ru16(page, this.DataPage.RowsStart + (r * 2));
                if ((raw & Constants.DataPage.NonLiveRowFlags) != 0)
                {
                    continue;
                }

                count++;
            }
        }

        return count;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<object[]> Rows(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (object[] row in this.EnumerateLinkedRowsAsync(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        (CatalogEntry? entry, TableDef? td) = resolved.Value;
        await foreach (object?[] row in this.EnumerateTypedRowsAsync(tableName, entry, td, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return (object[])row;
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<T> Rows<T>(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (T? row in this.EnumerateLinkedRowsAsync<T>(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        (CatalogEntry? entry, TableDef? td) = resolved.Value;

        // Bind the compiled mapper directly against the per-table column
        // headers + ClrTypes; avoids the GetColumnMetadataAsync round-trip
        // and the second async-iterator state machine that the previous
        // implementation built by re-entering Rows().
        var headers = new string[td.Columns.Count];
        for (int i = 0; i < td.Columns.Count; i++)
        {
            headers[i] = td.Columns[i].Name;
        }

        // Try to compile a direct page → T decoder that skips the per-row
        // object?[] buffer and primitive boxing entirely. The builder returns
        // null when any bound column requires the slow path (Memo/Ole
        // LVAL chain, Binary, Numeric, Complex/Attachment, Hyperlink
        // prop).
        DirectRowDecoder<T>? directDecoder = td.HasComplexColumns
            ? null
            : DirectRowDecoderBuilder.TryBuild<T>(headers, td.Columns, td.ClrTypes);

        if (directDecoder != null)
        {
            await foreach (T? item in this.EnumerateDirectRowsAsync(entry, td, directDecoder, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            yield break;
        }

        Func<object?[], T> factory = RowMapper<T>.Build(headers, td.ClrTypes);

        // Skip per-row decode of columns the mapper never reads. For wide
        // tables and narrow DTOs this can eliminate the bulk of the per-row
        // decode + boxing cost. We suppress the projection when the table has
        // complex/attachment columns, because complex resolution needs the
        // parent-id LongInteger which may not be in the projection set.
        bool[]? wantedColumns = td.HasComplexColumns
            ? null
            : RowMapper<T>.GetBoundColumnMask(headers);

        await foreach (T? mapped in this.EnumerateMappedRowsPooledAsync(tableName, entry, td, wantedColumns, factory, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return mapped;
        }
    }

    /// <summary>
    /// Fallback path for <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>:
    /// walks every owned data page for <paramref name="entry"/>, decodes each
    /// row into a single <see cref="ArrayPool{T}.Shared"/>-rented buffer,
    /// applies the mapper, and yields the produced <typeparamref name="T"/>.
    /// The buffer is reused across every row and returned to the pool on
    /// completion (or exception); the mapper consumes values out of the
    /// buffer before the next iteration overwrites it, so no caller ever
    /// observes the pooled array.
    /// </summary>
    /// <typeparam name="T">The mapped row type yielded by the enumerator.</typeparam>
    /// <param name="tableName">The table name.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="wantedColumns">The wanted columns.</param>
    /// <param name="factory">The factory.</param>
    /// <param name="progress">The progress.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private async IAsyncEnumerable<T> EnumerateMappedRowsPooledAsync<T>(
        string tableName,
        CatalogEntry entry,
        TableDef td,
        bool[]? wantedColumns,
        Func<object?[], T> factory,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long rowCount = 0;

        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType, AttachmentType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));

        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await this.complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, this.strictParsing);

        int colCount = td.Columns.Count;
        object?[] rowBuffer = ArrayPool<object?>.Shared.Rent(colCount);
        try
        {
            await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
                {
                    if (rb.RowSize < this.RowFields.NumCols)
                    {
                        continue;
                    }

                    bool ok = await this.CrackRowTypedIntoBufferAsync(scanPage.Page, rb.RowStart, rb.RowSize, decodePlan, rowBuffer, cancellationToken).ConfigureAwait(false);
                    if (!ok)
                    {
                        continue;
                    }

                    if (needsComplexPass)
                    {
                        ComplexColumnReader.ResolveColumns(rowBuffer, td.Columns, complexData);
                    }

                    if (needsHyperlinkPass)
                    {
                        WrapHyperlinkColumns(rowBuffer, td.ClrTypes);
                    }

                    yield return factory(rowBuffer);
                    rowCount++;
                }

                progress?.Report(rowCount);
            }
        }
        finally
        {
            ArrayPool<object?>.Shared.Return(rowBuffer, clearArray: true);
        }
    }

    /// <summary>
    /// Shared typed-row enumerator used by <see cref="Rows(string, IProgress{long}?, CancellationToken)"/>
    /// and <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>. Walks every
    /// owned data page for <paramref name="entry"/>, emitting per-row
    /// <c>object?[]</c> buffers with complex-attachment and Hyperlink
    /// post-processing applied (gated by the per-table flags). Centralising
    /// the page scan here keeps the typed and projected entry points on a
    /// single iterator (one C# async state machine instead of two).
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="progress">The progress.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private async IAsyncEnumerable<object?[]> EnumerateTypedRowsAsync(
        string tableName,
        CatalogEntry entry,
        TableDef td,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (object?[] row in this.EnumerateTypedRowsAsync(tableName, entry, td, wantedColumns: null, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    /// <summary>
    /// Projection-aware overload of <c>EnumerateTypedRowsAsync</c>.
    /// When <paramref name="wantedColumns"/> is non-<see langword="null"/>, only the
    /// flagged column indices are decoded and the complex-attachment / Hyperlink
    /// post-processing passes are skipped when no wanted column is affected by them.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="wantedColumns">The wanted columns.</param>
    /// <param name="progress">The progress.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private async IAsyncEnumerable<object?[]> EnumerateTypedRowsAsync(
        string tableName,
        CatalogEntry entry,
        TableDef td,
        bool[]? wantedColumns,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long rowCount = 0;

        // Decide which post-processing passes are needed up front. When a
        // projection mask is supplied, skip a pass entirely if no wanted
        // column requires it; otherwise run with the table-wide flag.
        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType, AttachmentType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));

        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await this.complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, this.strictParsing);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
            {
                if (rb.RowSize < this.RowFields.NumCols)
                {
                    continue;
                }

                object?[]? row = await this.CrackRowTypedAsync(scanPage.Page, rb.RowStart, rb.RowSize, decodePlan, cancellationToken).ConfigureAwait(false);
                if (row == null)
                {
                    continue;
                }

                if (needsComplexPass)
                {
                    ComplexColumnReader.ResolveColumns(row, td.Columns, complexData);
                }

                if (needsHyperlinkPass)
                {
                    WrapHyperlinkColumns(row, td.ClrTypes);
                }

                yield return row;
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <summary>
    /// Direct-decoder fast-path enumerator: walks every owned data page for
    /// <paramref name="entry"/> and invokes the compiled
    /// <paramref name="directDecoder"/> against each live row, allocating a
    /// fresh <typeparamref name="T"/> per row but no <c>object?[]</c> buffer.
    /// Used by <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>
    /// when every bound column is directly decodable; otherwise the
    /// projection-aware fallback path runs.
    /// </summary>
    /// <typeparam name="T">The row type decoded directly from page bytes.</typeparam>
    /// <param name="entry">The entry.</param>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="directDecoder">The direct decoder.</param>
    /// <param name="progress">The progress.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private async IAsyncEnumerable<T> EnumerateDirectRowsAsync<T>(
        CatalogEntry entry,
        TableDef td,
        DirectRowDecoder<T> directDecoder,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class, new()
    {
        long rowCount = 0;
        bool hasVarColumns = td.HasVarColumns;
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
            {
                if (rb.RowSize < this.RowFields.NumCols)
                {
                    continue;
                }

                T target = new();
                if (!directDecoder(this, scanPage.Page, rb.RowStart, rb.RowSize, hasVarColumns, target))
                {
                    continue;
                }

                yield return target;
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    private async IAsyncEnumerable<TableScanPage> EnumerateTableScanPagesAsync(
        TableDef tableDef,
        IReadOnlyList<long> pageNumbers,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!this.ShouldReadAheadTablePages(tableDef, pageNumbers))
        {
            foreach (long pageNumber in pageNumbers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return await this.ReadTableScanPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }

            yield break;
        }

        Task<TableScanPage>? nextPageTask = null;
        try
        {
            for (int pageIndex = 0; pageIndex < pageNumbers.Count; pageIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Task<TableScanPage> currentPageTask = nextPageTask
                    ?? this.ReadTableScanPageAsync(pageNumbers[pageIndex], cancellationToken).AsTask();
                nextPageTask = pageIndex + 1 < pageNumbers.Count
                    ? this.ReadTableScanPageAsync(pageNumbers[pageIndex + 1], cancellationToken).AsTask()
                    : null;

                yield return await currentPageTask.ConfigureAwait(false);
            }
        }
        finally
        {
            if (nextPageTask is not null)
            {
                await ObserveAbandonedTableScanReadAsync(nextPageTask).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Determines whether table pages should be read ahead.
    /// The cache returns page buffers to the shared pool on eviction, so read-ahead
    /// needs room for the previous, current, and prefetched data pages.
    /// </summary>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="pageNumbers">The list of page numbers for the table.</param>
    /// <returns><c>true</c> if table pages should be read ahead; otherwise, <c>false</c>.</returns>
    private bool ShouldReadAheadTablePages(TableDef tableDef, IReadOnlyList<long> pageNumbers) =>
        this.ParallelPageReadsEnabled
            && this.pageCache is not null
            && this.PageCacheSize >= MinimumTableScanReadAheadCacheSlots
            && pageNumbers.Count > 1
            && !HasCacheReentrantScanColumns(tableDef);

    private async ValueTask<TableScanPage> ReadTableScanPageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] page = await this.ReadPageCachedAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        return new TableScanPage(pageNumber, page);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<string[]> RowsAsStrings(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (string[] row in this.EnumerateLinkedRowsAsStringsAsync(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        (CatalogEntry? entry, TableDef? td) = resolved.Value;
        long rowCount = 0;
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            await foreach (string[] row in this.EnumerateRowsAsync(scanPage.PageNumber, scanPage.Page, td, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<List<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            List<ColumnMetadata>? linkedMetadata = await this.TryReadLinkedTableAsync(
                tableName,
                link => LinkedTableManager.GetLinkedTextColumnMetadataAsync(this, link, cancellationToken),
                (source, link) => source.GetColumnMetadataAsync(link.SourceObjectName, cancellationToken),
                cancellationToken).ConfigureAwait(false);
            return linkedMetadata ?? [];
        }

        Dictionary<string, string> complexSubtypes = new(StringComparer.OrdinalIgnoreCase);
        bool hasComplex = resolved.Value.Td.Columns.Any(c => c.Type is ComplexType or AttachmentType);
        if (hasComplex)
        {
            complexSubtypes = await this.complexColumns.ReadColumnSubtypesAsync(tableName, cancellationToken).ConfigureAwait(false);
        }

        ColumnPropertyBlock? properties = await this.ReadLvPropForTableAsync(
            resolved.Value.Entry.TDefPage, cancellationToken).ConfigureAwait(false);

        return resolved.Value.Td.Columns.Select((col, index) =>
        {
            ColumnPropertyTarget? target = properties?.FindTarget(col.Name);
            bool isCalc = col.IsCalculated;
            string? calcExpr = isCalc
                ? target?.GetTextValue(Constants.ColumnPropertyNames.Expression, this.Format)
                : null;
            ColumnType calcResultType = isCalc ? ResolveCalculatedResultType(target) : default;

            return new ColumnMetadata
            {
                Name = col.Name,
                TypeName = (col.Type == ComplexType && complexSubtypes.TryGetValue(col.Name, out string? subtype))
                    ? subtype
                    : ResolveTypeName(col),
                ClrType = JetTypeInfo.ResolveClrType(col),
                MaxLength = GetMetadataMaxLength(col),
                IsNullable = ResolveIsNullable(col, target),
                IsFixedLength = col.IsFixed,
                IsHyperlink = JetTypeInfo.IsHyperlinkColumn(col),
                Ordinal = index,
                Size = JetTypeInfo.GetColumnSize(JetTypeInfo.ResolveValueType(col), GetMetadataDeclaredSize(col)),
                DefaultValueExpression = target?.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, this.Format),
                ValidationRuleExpression = target?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, this.Format),
                ValidationText = target?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, this.Format),
                Description = target?.GetTextValue(Constants.ColumnPropertyNames.Description, this.Format),
                NumericPrecision = col.NumericPrecision,
                NumericScale = col.NumericScale,
                IsCalculated = isCalc,
                CalculationExpression = calcExpr,
                CalculatedResultType = (byte)(calcResultType != default ? calcResultType : col.CalculatedResultType),
            };
        }).ToList();
    }

    private static ColumnType ResolveCalculatedResultType(ColumnPropertyTarget? target)
    {
        ColumnPropertyEntry? rt = target?.Find(Constants.ColumnPropertyNames.ResultType);
        return rt?.Value.Length >= 1
            && (rt.DataType == ColumnType.ByteType
                || rt.DataType == ColumnType.IntegerType
                || rt.DataType == ColumnType.LongIntegerType)
            ? (ColumnType)rt.Value[0]
            : default;
    }

    /// <summary>
    /// Resolves a column's <c>IsNullable</c> from the persisted <c>Required</c>
    /// LvProp property when present, falling back to the legacy writer-private
    /// TDEF flag bit <c>0x08</c> for back-compat with files written by older
    /// JetDatabaseWriter revisions. DAO/Access never emit <c>0x08</c> in the
    /// flag byte, so the fallback reads as <c>true</c> (nullable) for any file
    /// authored outside this library.
    /// </summary>
    /// <param name="col">The column descriptor.</param>
    /// <param name="target">The target.</param>
    private static bool ResolveIsNullable(ColumnInfo col, ColumnPropertyTarget? target)
    {
        if ((col.Flags & Constants.ColumnDescriptorFlags.AutoNumber) != 0)
        {
            return false;
        }

        bool? required = target?.GetBooleanValue(Constants.ColumnPropertyNames.Required);
        if (required is bool r)
        {
            return !r;
        }

        return (col.Flags & Constants.ColumnDescriptorFlags.LegacyNotNull) == 0;
    }

    private static int? GetMetadataMaxLength(ColumnInfo col)
    {
        int declaredSize = GetMetadataDeclaredSize(col);
        return declaredSize > 0 ? declaredSize : null;
    }

    private static int GetMetadataDeclaredSize(ColumnInfo col)
    {
        if (col.IsCalculated && (col.Type == TextType || col.Type == BinaryType) && col.Size > Constants.CalculatedColumn.ExtraDataLen)
        {
            return col.Size - Constants.CalculatedColumn.ExtraDataLen;
        }

        return col.Size;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        byte[]? td = await this.ReadTDefBytesAsync(resolved.Value.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null || td.Length < this.TDef.BlockEnd)
        {
            return [];
        }

        return this.ParseIndexMetadata(td, resolved.Value.Td.Columns);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<object[]> SeekRowsAsync(
        string tableName,
        string indexName,
        IReadOnlyList<object?> keyValues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(indexName, nameof(indexName));
        Guard.NotNull(keyValues, nameof(keyValues));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            yield break;
        }

        if (this.Format == DatabaseFormat.Jet3Mdb)
        {
            throw new NotSupportedException("Index seeks are currently supported for Jet4/ACE databases only.");
        }

        (CatalogEntry? entry, TableDef? td) = resolved.Value;
        byte[]? tdefBytes = await this.ReadTDefBytesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (tdefBytes == null || tdefBytes.Length < this.TDef.BlockEnd)
        {
            yield break;
        }

        List<IndexMetadata> indexes = this.ParseIndexMetadata(tdefBytes, td.Columns);

        IndexMetadata? index = indexes.Find(i => string.Equals(i.Name, indexName, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Index '{indexName}' was not found on table '{tableName}'.", nameof(indexName));

        if (index.FirstDp <= 0 || index.Columns.Count == 0)
        {
            yield break;
        }

        byte[] searchKey = this.EncodeIndexSeekKey(tableName, index, td, keyValues);
        var cursor = new IndexCursor(
            this.ReadPageCachedAsync,
            this.PageSizeBytes);
        List<(long DataPage, int RowIndex)> hits = await cursor.FindRowLocationsAsync(
            index.FirstDp,
            searchKey,
            cancellationToken).ConfigureAwait(false);

        bool needsComplexPass = td.HasComplexColumns;
        bool needsHyperlinkPass = td.HasHyperlinkColumns;
        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await this.complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;

        foreach ((long dataPage, int rowIndex) in hits)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object?[]? row = await this.MaterializeSeekRowAsync(
                entry,
                td,
                dataPage,
                rowIndex,
                complexData,
                needsComplexPass,
                needsHyperlinkPass,
                cancellationToken).ConfigureAwait(false);
            if (row == null)
            {
                continue;
            }

            yield return (object[])row;
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();
        return await this.complexColumns.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        cancellationToken.ThrowIfCancellationRequested();
        return await this.complexColumns.GetAttachmentsAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<(int ConceptualTableId, object? Value)>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        cancellationToken.ThrowIfCancellationRequested();
        return await this.complexColumns.GetMultiValueItemsAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
    }

    private static void EndDataTableLoad(DataTable table, ref bool dataLoadStarted)
    {
        if (!dataLoadStarted)
        {
            return;
        }

        dataLoadStarted = false;
        table.EndLoadData();
    }

    private static int ResolveDataTableMinimumCapacity(long rowCount, uint? maxRows)
    {
        long capacity = rowCount;
        if (maxRows.HasValue)
        {
            long limit = maxRows.Value;
            capacity = capacity > 0 ? Math.Min(capacity, limit) : limit;
        }

        return capacity is > 0 and <= int.MaxValue ? (int)capacity : 0;
    }

    private List<IndexMetadata> ParseIndexMetadata(byte[] td, List<ColumnInfo> columns)
    {
        int numCols = Ru16(td, this.TDef.NumCols);
        int numIdx = Ri32(td, this.TDef.NumCols + 2);
        int numRealIdx = Ri32(td, this.TDef.NumRealIdx);

        // Defensive bounds: corrupt TDEFs can report absurd counts.
        if (numIdx is <= 0 or > Constants.TableDefinition.MaxIndexes)
        {
            return [];
        }

        if (numRealIdx is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            numRealIdx = 0;
        }

        // Section walk mirrors AccessBase.ReadTableDefAsync and FormatProbe.
        int colStart = this.TDef.BlockEnd + (numRealIdx * this.TDef.RealIdxEntrySz);

        // Walk column-name length-prefix block to find where it ends.
        int pos = colStart + (numCols * this.ColumnDescriptor.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (this.ReadColumnName(td, ref pos, out _) < 0)
            {
                return [];
            }
        }

        int realIdxDescStart = pos;
        (int _, int logicalIdxStart, int logicalIdxNamesStart, int _, int _) = this.IndexLayoutInfo.GetIndexSection(realIdxDescStart, numRealIdx, numIdx);

        if (logicalIdxNamesStart > td.Length)
        {
            return [];
        }

        // Build a col_num → name lookup honouring deleted-column gaps.
        var colNumToName = new Dictionary<int, string>(columns.Count);
        foreach (ColumnInfo c in columns)
        {
            colNumToName[c.ColNum] = c.Name;
        }

        // Pre-walk index names so we can pair each logical-idx entry with its name.
        var names = new string[numIdx];
        int npos = logicalIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (this.ReadColumnName(td, ref npos, out string n) < 0)
            {
                names[i] = string.Empty;
            }
            else
            {
                names[i] = n;
            }
        }

        var result = new List<IndexMetadata>(numIdx);
        for (int i = 0; i < numIdx; i++)
        {
            if (!this.IndexLayoutInfo.TryReadLogicalEntry(td, logicalIdxStart, i, out IndexLayout.LogicalIdxEntry entry))
            {
                break;
            }

            (int _, int indexNum, int realIdxNum, int relIdxNum, int relTblPage, byte cascadeUps, byte cascadeDels, IndexKind indexType) = entry;

            // Read the col_map for the backing real-idx entry to recover key columns.
            var keyColumns = new List<IndexColumnReference>();
            byte flags = 0x00;
            int firstDp = 0;
            if (numRealIdx > 0 && realIdxNum >= 0 && realIdxNum < numRealIdx
                && this.IndexLayoutInfo.TryReadRealIdxSlotWithKeyColumns(td, realIdxDescStart, realIdxNum, out IndexLayout.RealIdxSlot slot, out List<IndexLayout.KeyColumn>? kcs))
            {
                foreach ((int cn, bool ascending) in kcs)
                {
                    keyColumns.Add(new IndexColumnReference
                    {
                        Name = colNumToName.TryGetValue(cn, out string? n) ? n : string.Empty,
                        ColumnNumber = cn,
                        IsAscending = ascending,
                    });
                }

                flags = slot.Flags;
                if (slot.FirstDpOffset >= 0 && slot.FirstDpOffset + 4 <= td.Length)
                {
                    firstDp = Ri32(td, slot.FirstDpOffset);
                }
            }

            // Access often leaves the real-index unique flag clear on primary
            // keys; their semantic uniqueness is conveyed by index_type=0x01.
            bool hasUniqueFlag = (flags & Constants.TableDefinition.UniqueIndexFlag) != 0;

            result.Add(new IndexMetadata
            {
                Name = names[i],
                IndexNumber = indexNum,
                RealIndexNumber = realIdxNum,
                Kind = indexType,
                HasUniqueFlag = hasUniqueFlag,
                IgnoreNulls = (flags & Constants.TableDefinition.IgnoreNullsIndexFlag) != 0,
                IsRequired = (flags & Constants.TableDefinition.RequiredIndexFlag) != 0,
                IsForeignKey = relIdxNum != -1,
                RelatedTablePage = relIdxNum != -1 ? relTblPage : 0,

                // Per Jackcess IndexImpl: only bit 0x01 (CASCADE_DELETES_FLAG /
                // CASCADE_UPDATES_FLAG) signals "cascade enabled". DAO/Access stamps
                // a non-zero default (0x04 = CASCADE_SET_DEFAULT_FLAG) into these
                // bytes for every index — including PK and standalone indexes — so
                // a bare `!= 0` check would surface false positives. Mask to bit 0x01.
                CascadeUpdates = (cascadeUps & 0x01) != 0,
                CascadeDeletes = (cascadeDels & 0x01) != 0,
                Columns = keyColumns,
                FirstDp = firstDp,
            });
        }

        return result;
    }

    private byte[] EncodeIndexSeekKey(string tableName, IndexMetadata index, TableDef tableDef, IReadOnlyList<object?> keyValues)
    {
        if (keyValues.Count != index.Columns.Count)
        {
            throw new ArgumentException(
                $"Index '{index.Name}' on table '{tableName}' expects {index.Columns.Count} key value(s), but {keyValues.Count} were supplied.",
                nameof(keyValues));
        }

        bool legacyNumeric = this.Format == DatabaseFormat.Jet4Mdb;
        byte[][] perColumn = new byte[index.Columns.Count][];
        int totalLength = 0;

        for (int i = 0; i < index.Columns.Count; i++)
        {
            IndexColumnReference keyColumn = index.Columns[i];

            ColumnInfo? column = tableDef.Columns.Find(c => c.ColNum == keyColumn.ColumnNumber)
                ?? throw new InvalidDataException($"Index '{index.Name}' on table '{tableName}' references missing column number {keyColumn.ColumnNumber}.");

            object? value = keyValues[i];
            perColumn[i] = column.Type == NumericType
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, keyColumn.IsAscending, column.NumericScale, legacyNumeric)
                : IndexKeyEncoder.EncodeEntry(column.Type, value, keyColumn.IsAscending);
            totalLength += perColumn[i].Length;
        }

        byte[] composite = new byte[totalLength];
        int offset = 0;
        for (int i = 0; i < perColumn.Length; i++)
        {
            Buffer.BlockCopy(perColumn[i], 0, composite, offset, perColumn[i].Length);
            offset += perColumn[i].Length;
        }

        return composite;
    }

    private async ValueTask<object?[]?> MaterializeSeekRowAsync(
        CatalogEntry entry,
        TableDef td,
        long dataPage,
        int rowIndex,
        Dictionary<int, Dictionary<int, byte[]>>? complexData,
        bool needsComplexPass,
        bool needsHyperlinkPass,
        CancellationToken cancellationToken)
    {
        byte[] page = await this.ReadPageCachedAsync(dataPage, cancellationToken).ConfigureAwait(false);
        if (page[0] != Constants.PageTypes.Data || Ri32(page, this.DataPage.TDefOff) != entry.TDefPage)
        {
            return null;
        }

        if (!this.TryFindLiveRowBound(page, dataPage, rowIndex, out RowBound rowBound) || rowBound.RowSize < this.RowFields.NumCols)
        {
            return null;
        }

        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns: null, this.strictParsing);
        object?[]? row = await this.CrackRowTypedAsync(page, rowBound.RowStart, rowBound.RowSize, decodePlan, cancellationToken).ConfigureAwait(false);
        if (row == null)
        {
            return null;
        }

        if (needsComplexPass)
        {
            ComplexColumnReader.ResolveColumns(row, td.Columns, complexData);
        }

        if (needsHyperlinkPass)
        {
            WrapHyperlinkColumns(row, td.ClrTypes);
        }

        return row;
    }

    private bool TryFindLiveRowBound(byte[] page, long pageNumber, int rowIndex, out RowBound rowBound)
    {
        foreach (RowBound candidate in this.GetLiveRowBoundsCached(pageNumber, page))
        {
            if (candidate.RowIndex == rowIndex)
            {
                rowBound = candidate;
                return true;
            }
        }

        rowBound = default;
        return false;
    }

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    /// <returns>A list of user table names.</returns>
    public async ValueTask<List<string>> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return tables.ConvertAll(e => e.Name);
    }

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTimeType, decimal, etc.).
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns.</returns>
    public ValueTask<DataTable> ReadDataTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.ReadDataTableCoreAsync(tableName, maxRows, progress, preserveComplexReferences: false, cancellationToken);

    internal ValueTask<DataTable> ReadDataTableForSchemaRewriteAsync(string tableName, CancellationToken cancellationToken = default)
        => this.ReadDataTableCoreAsync(tableName, maxRows: null, progress: null, preserveComplexReferences: true, cancellationToken);

    private async ValueTask<DataTable> ReadDataTableCoreAsync(
        string? tableName,
        uint? maxRows,
        IProgress<long>? progress,
        bool preserveComplexReferences,
        CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrEmpty(tableName))
        {
            List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
            if (tables.Count == 0)
            {
                return new DataTable();
            }

            tableName = tables[0].Name;
        }

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            DataTable? linkedTable = await this.TryReadLinkedTableAsync(
                tableName,
                link => LinkedTableManager.ReadLinkedTextDataTableAsync(this, link, maxRows, progress, cancellationToken),
                (source, link) => source.ReadDataTableCoreAsync(link.SourceObjectName, maxRows, progress, preserveComplexReferences, cancellationToken),
                cancellationToken).ConfigureAwait(false);

#pragma warning disable CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
            return linkedTable ?? new DataTable(tableName);
#pragma warning restore CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
        }

        (CatalogEntry? entry, TableDef? td) = resolved.Value;
        DataTable? dt = null;
        bool dataLoadStarted = false;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                Type clrType = preserveComplexReferences && (col.Type == ComplexType || col.Type == AttachmentType)
                    ? typeof(object)
                    : JetTypeInfo.ResolveClrType(col);
                _ = dt.Columns.Add(col.Name, clrType);
            }

            Dictionary<int, Dictionary<int, byte[]>>? complexData = td.HasComplexColumns && !preserveComplexReferences
                ? await this.complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
                : null;
            IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

            int minimumCapacity = ResolveDataTableMinimumCapacity(td.RowCount, maxRows);
            if (minimumCapacity > 0)
            {
                dt.MinimumCapacity = minimumCapacity;
            }

            dt.BeginLoadData();
            dataLoadStarted = true;

            // Rent a single object?[] from the shared pool and
            // reuse it across every row. The DataRow ingestion below
            // copies values out via the per-cell setter, so the buffer is
            // never retained by the table.
            int colCount = td.Columns.Count;
            long loadedRows = 0;
            var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns: null, this.strictParsing);
            object?[] rowBuffer = ArrayPool<object?>.Shared.Rent(colCount);
            try
            {
                await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
                    {
                        if (rb.RowSize < this.RowFields.NumCols)
                        {
                            continue;
                        }

                        bool ok = await this.CrackRowTypedIntoBufferAsync(scanPage.Page, rb.RowStart, rb.RowSize, decodePlan, rowBuffer, cancellationToken).ConfigureAwait(false);
                        if (!ok)
                        {
                            continue;
                        }

                        if (td.HasComplexColumns && !preserveComplexReferences)
                        {
                            ComplexColumnReader.ResolveColumns(rowBuffer, td.Columns, complexData);
                        }

                        if (td.HasHyperlinkColumns)
                        {
                            WrapHyperlinkColumns(rowBuffer, td.ClrTypes);
                        }

                        DataRow newRow = dt.NewRow();
                        for (int i = 0; i < colCount; i++)
                        {
                            newRow[i] = rowBuffer[i] ?? DBNull.Value;
                        }

                        dt.Rows.Add(newRow);
                        loadedRows++;
                        if (maxRows.HasValue && loadedRows >= maxRows.Value)
                        {
                            progress?.Report(loadedRows);
                            EndDataTableLoad(dt, ref dataLoadStarted);
                            DataTable result = dt;
                            dt = null;
                            return result;
                        }
                    }

                    progress?.Report(loadedRows);
                }
            }
            finally
            {
                ArrayPool<object?>.Shared.Return(rowBuffer, clearArray: true);
            }

            EndDataTableLoad(dt, ref dataLoadStarted);
            DataTable final = dt;
            dt = null;
            return final;
        }
        finally
        {
            if (dt != null && dataLoadStarted)
            {
                EndDataTableLoad(dt, ref dataLoadStarted);
            }

            dt?.Dispose();
        }
    }

    private async ValueTask<long?> TryGetLinkedTableRowCountAsync(string tableName, CancellationToken cancellationToken)
    {
        LinkedTableInfo? link = await LinkedTableManager.FindLinkedTableAsync(this, tableName, cancellationToken).ConfigureAwait(false);
        if (link == null)
        {
            return null;
        }

        if (link.Kind == LinkedTableKind.Text)
        {
            return await LinkedTableManager.CountLinkedTextRowsAsync(this, link, cancellationToken).ConfigureAwait(false);
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(this, link, cancellationToken).ConfigureAwait(false);
        return await source.GetRealRowCountAsync(link.SourceObjectName, cancellationToken).ConfigureAwait(false);
    }

    private IAsyncEnumerable<object[]> EnumerateLinkedRowsAsync(
        string tableName,
        IProgress<long>? progress,
        CancellationToken cancellationToken) =>
        this.EnumerateLinkedTableRowsAsync(
            tableName,
            link => LinkedTableManager.RowsLinkedTextAsStringsAsync(this, link, progress, cancellationToken),
            (source, link) => source.Rows(link.SourceObjectName, progress, cancellationToken),
            cancellationToken);

    private IAsyncEnumerable<T> EnumerateLinkedRowsAsync<T>(
        string tableName,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
        where T : class, new()
        => this.EnumerateLinkedTableRowsAsync(
            tableName,
            link => this.EnumerateLinkedTextRowsAsync<T>(link, progress, cancellationToken),
            (source, link) => source.Rows<T>(link.SourceObjectName, progress, cancellationToken),
            cancellationToken);

    private IAsyncEnumerable<string[]> EnumerateLinkedRowsAsStringsAsync(
        string tableName,
        IProgress<long>? progress,
        CancellationToken cancellationToken) =>
        this.EnumerateLinkedTableRowsAsync(
            tableName,
            link => LinkedTableManager.RowsLinkedTextAsStringsAsync(this, link, progress, cancellationToken),
            (source, link) => source.RowsAsStrings(link.SourceObjectName, progress, cancellationToken),
            cancellationToken);

    private async IAsyncEnumerable<T> EnumerateLinkedTextRowsAsync<T>(
        LinkedTableInfo link,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class, new()
    {
        List<ColumnMetadata> meta = await LinkedTableManager.GetLinkedTextColumnMetadataAsync(this, link, cancellationToken).ConfigureAwait(false);
        Func<object?[], T> textFactory = RowMapper<T>.Build(meta);
        await foreach (string[] row in LinkedTableManager.RowsLinkedTextAsStringsAsync(this, link, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return textFactory(row);
        }
    }

    private async IAsyncEnumerable<TRow> EnumerateLinkedTableRowsAsync<TRow>(
        string tableName,
        Func<LinkedTableInfo, IAsyncEnumerable<TRow>> readText,
        Func<AccessReader, LinkedTableInfo, IAsyncEnumerable<TRow>> readAccess,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        LinkedTableInfo? link = await LinkedTableManager.FindLinkedTableAsync(this, tableName, cancellationToken).ConfigureAwait(false);
        if (link == null)
        {
            yield break;
        }

        if (link.Kind == LinkedTableKind.Text)
        {
            await foreach (TRow? row in readText(link).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(this, link, cancellationToken).ConfigureAwait(false);
        await foreach (TRow? row in readAccess(source, link).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    private async ValueTask<TResult?> TryReadLinkedTableAsync<TResult>(
        string tableName,
        Func<LinkedTableInfo, ValueTask<TResult>> readText,
        Func<AccessReader, LinkedTableInfo, ValueTask<TResult>> readAccess,
        CancellationToken cancellationToken)
        where TResult : class
    {
        LinkedTableInfo? link = await LinkedTableManager.FindLinkedTableAsync(this, tableName, cancellationToken).ConfigureAwait(false);
        if (link == null)
        {
            return null;
        }

        if (link.Kind == LinkedTableKind.Text)
        {
            return await readText(link).ConfigureAwait(false);
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(this, link, cancellationToken).ConfigureAwait(false);
        return await readAccess(source, link).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<List<T>> ReadTableAsync<T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
        where T : class, new()
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved != null)
        {
            List<string> resolvedHeaders = resolved.Value.Td.Columns.ConvertAll(column => column.Name);
            var projectedColumns = new List<(string Name, ColumnInfo Column)>(resolvedHeaders.Count);
            RowMapper<T>.Accessor?[] fullIndex = RowMapper<T>.BuildIndex(resolvedHeaders);

            for (int i = 0; i < resolvedHeaders.Count; i++)
            {
                if (fullIndex[i] != null)
                {
                    projectedColumns.Add((resolvedHeaders[i], resolved.Value.Td.Columns[i]));
                }
            }

            bool canUseDirectMap = projectedColumns.Count > 0
                && projectedColumns.TrueForAll(static projection => projection.Column.Type is not ComplexType and not AttachmentType);

            if (canUseDirectMap && projectedColumns.Count == resolvedHeaders.Count)
            {
                Func<object?[], T> fullFactory = RowMapper<T>.Build(resolved.Value.Td);
                return await this.ReadMappedTableAsync(
                    resolved.Value.Entry.TDefPage,
                    resolved.Value.Td,
                    fullFactory,
                    maxRows,
                    cancellationToken).ConfigureAwait(false);
            }

            bool canProject = canUseDirectMap && projectedColumns.Count < resolvedHeaders.Count;

            if (canProject)
            {
                return await this.ReadProjectedTableAsync<T>(
                    resolved.Value.Entry.TDefPage,
                    resolved.Value.Td,
                    projectedColumns,
                    maxRows,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        List<ColumnMetadata> meta = await this.GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        uint? linkedTextMaxMaterializedRows = await LinkedTableManager.GetLinkedTextMaterializedRowLimitAsync(
            this,
            tableName,
            cancellationToken).ConfigureAwait(false);
        Func<object?[], T> factoryFallback = RowMapper<T>.Build(meta);
        var items = new List<T>();
        int count = 0;

        await foreach (object[] row in this.Rows(tableName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            LinkedTableManager.ThrowIfLinkedTextMaterializedRowLimitExceeded(
                tableName,
                count,
                linkedTextMaxMaterializedRows);
            items.Add(factoryFallback(row));
            count++;
            if (maxRows.HasValue && count >= maxRows.Value)
            {
                break;
            }
        }

        return items;
    }

    private async ValueTask<List<T>> ReadMappedTableAsync<T>(
        long tdefPage,
        TableDef td,
        Func<object?[], T> factory,
        uint? maxRows,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        var items = new List<T>();
        bool hasVarCols = false;
        for (int i = 0; i < td.Columns.Count; i++)
        {
            if (!td.Columns[i].IsFixed)
            {
                hasVarCols = true;
                break;
            }
        }

        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
            {
                cancellationToken.ThrowIfCancellationRequested();

                object[]? row = await this.CrackMappedRowAsync(
                    scanPage.Page,
                    rb.RowStart,
                    rb.RowSize,
                    td,
                    hasVarCols,
                    cancellationToken).ConfigureAwait(false);
                if (row == null)
                {
                    continue;
                }

                items.Add(factory(row));
                if (maxRows.HasValue && items.Count >= maxRows.Value)
                {
                    return items;
                }
            }
        }

        return items;
    }

    private async ValueTask<List<T>> ReadProjectedTableAsync<T>(
        long tdefPage,
        TableDef td,
        List<(string Name, ColumnInfo Column)> projectedColumns,
        uint? maxRows,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        var headers = new string[projectedColumns.Count];
        var projectedSourceTypes = new Type[projectedColumns.Count];
        for (int i = 0; i < projectedColumns.Count; i++)
        {
            headers[i] = projectedColumns[i].Name;
            projectedSourceTypes[i] = JetTypeInfo.ResolveClrType(projectedColumns[i].Column);
        }

        Func<object?[], T> factory = RowMapper<T>.Build(headers, projectedSourceTypes);
        var items = new List<T>();
        bool hasVarCols = false;
        for (int i = 0; i < td.Columns.Count; i++)
        {
            if (!td.Columns[i].IsFixed)
            {
                hasVarCols = true;
                break;
            }
        }

        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
            {
                cancellationToken.ThrowIfCancellationRequested();

                object[]? projectedRow = await this.CrackProjectedRowAsync(
                    scanPage.Page,
                    rb.RowStart,
                    rb.RowSize,
                    td,
                    projectedColumns,
                    hasVarCols,
                    cancellationToken).ConfigureAwait(false);
                if (projectedRow == null)
                {
                    continue;
                }

                items.Add(factory(projectedRow));
                if (maxRows.HasValue && items.Count >= maxRows.Value)
                {
                    return items;
                }
            }
        }

        return items;
    }

    private async ValueTask<object[]?> CrackMappedRowAsync(
        byte[] page,
        int rowStart,
        int rowSize,
        TableDef td,
        bool hasVarCols,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (rowSize < this.RowFields.NumCols)
        {
            return null;
        }

        int rawNumCols = this.ReadRowColumnCount(page, rowStart);
        if (rawNumCols == 0)
        {
            return null;
        }

        // Stale rows from before a column deletion carry a higher rawNumCols
        // than the current schema. The surviving columns' absolute offsets
        // (ColNum, FixedOff, VarIdx) are stable across deletions, so
        // ResolveColumnSlice can decode them correctly. Force var-area parsing
        // because we don't know whether a deleted column was variable-length.
        bool effectiveHasVarCols = hasVarCols || (td.HasDeletedColumns && rawNumCols > td.Columns.Count);

        if (!this.TryParseRowLayout(page, rowStart, rowSize, effectiveHasVarCols, out RowLayout layout))
        {
            return null;
        }

        var values = new object[td.Columns.Count];
        for (int i = 0; i < td.Columns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = td.Columns[i];
            ColumnSlice slice = this.ResolveColumnSlice(page, rowStart, rowSize, layout, col);
            values[i] = await this.ReadColumnValueAsync(page, rowStart, slice, col, cancellationToken).ConfigureAwait(false);
        }

        return values;
    }

    private async ValueTask<object[]?> CrackProjectedRowAsync(
        byte[] page,
        int rowStart,
        int rowSize,
        TableDef td,
        List<(string Name, ColumnInfo Column)> projectedColumns,
        bool hasVarCols,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (rowSize < this.RowFields.NumCols)
        {
            return null;
        }

        int rawNumCols = this.ReadRowColumnCount(page, rowStart);
        if (rawNumCols == 0)
        {
            return null;
        }

        // Stale rows: force var-area parsing when deleted-column gaps exist.
        bool effectiveHasVarCols = hasVarCols || (td.HasDeletedColumns && rawNumCols > td.Columns.Count);

        if (!this.TryParseRowLayout(page, rowStart, rowSize, effectiveHasVarCols, out RowLayout layout))
        {
            return null;
        }

        var values = new object[projectedColumns.Count];
        for (int i = 0; i < projectedColumns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = projectedColumns[i].Column;
            ColumnSlice slice = this.ResolveColumnSlice(page, rowStart, rowSize, layout, col);
            string rawValue = slice.Kind switch
            {
                ColumnSliceKind.Bool => slice.BoolValue ? "True" : "False",
                ColumnSliceKind.Null => string.Empty,
                ColumnSliceKind.Empty => string.Empty,
                ColumnSliceKind.Fixed => JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, col, slice.DataLen, strictNumeric: true),
                ColumnSliceKind.Var => await this.ReadVarAsync(page, rowStart + slice.DataStart, slice.DataLen, col, cancellationToken).ConfigureAwait(false),
                _ => string.Empty,
            };

            values[i] = TypedValueParser.ParseValue(rawValue, JetTypeInfo.ResolveClrType(col), this.strictParsing);
        }

        return values;
    }

    private async ValueTask<object> ReadColumnValueAsync(
        byte[] page,
        int rowStart,
        ColumnSlice slice,
        ColumnInfo col,
        CancellationToken cancellationToken) => slice.Kind switch
        {
            ColumnSliceKind.Bool => slice.BoolValue,
            ColumnSliceKind.Null => DBNull.Value,
            ColumnSliceKind.Empty => DBNull.Value,
            ColumnSliceKind.Fixed => this.ParseColumnValue(JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, col, slice.DataLen, strictNumeric: true), col),
            ColumnSliceKind.Var => await this.ReadVarValueAsync(page, rowStart + slice.DataStart, slice.DataLen, col, cancellationToken).ConfigureAwait(false),
            _ => DBNull.Value,
        };

    private object ParseColumnValue(string rawValue, ColumnInfo col) =>
        TypedValueParser.ParseValue(rawValue, JetTypeInfo.ResolveClrType(col), this.strictParsing);

    private async ValueTask<object> ReadVarValueAsync(byte[] row, int start, int len, ColumnInfo col, CancellationToken cancellationToken)
    {
        if (len <= 0)
        {
            return DBNull.Value;
        }

        if (col.IsCalculated)
        {
            return await this.ReadCalculatedVarValueAsync(row, start, len, col, cancellationToken).ConfigureAwait(false);
        }

        Type targetType = JetTypeInfo.ResolveClrType(col);
        if (targetType == typeof(byte[]))
        {
            if (col.Type is BinaryType)
            {
                return row.AsSpan(start, len).ToArray();
            }
            else if (col.Type is OleType)
            {
                return await this.longValueDecoder.ReadOleValueBytesAsync(row, start, len, cancellationToken).ConfigureAwait(false);
            }
        }

        string rawValue = await this.ReadVarAsync(row, start, len, col, cancellationToken).ConfigureAwait(false);
        return TypedValueParser.ParseValue(rawValue, targetType, this.strictParsing);
    }

    private async ValueTask<object> ReadCalculatedVarValueAsync(byte[] row, int start, int len, ColumnInfo col, CancellationToken cancellationToken)
    {
        switch (col.Type)
        {
            case TextType:
                return this.DecodeCalculatedTextPayload(CalculatedColumnUtil.Unwrap(row.AsSpan(start, len)));
            case BinaryType:
                return CalculatedColumnUtil.Unwrap(row.AsSpan(start, len));
            case MemoType:
            {
                byte[] raw = await this.longValueDecoder.ReadLongValueRawBytesAsync(row, start, len, cancellationToken).ConfigureAwait(false);
                byte[] payload = CalculatedColumnUtil.Unwrap(raw);
                return this.longValueDecoder.DecodeLongValue(payload, 0, payload.Length, isOle: false);
            }

            case OleType:
            {
                byte[] raw = await this.longValueDecoder.ReadLongValueRawBytesAsync(row, start, len, cancellationToken).ConfigureAwait(false);
                byte[] payload = CalculatedColumnUtil.Unwrap(raw);
                return DecodeOleValueBytes(payload, 0, payload.Length);
            }

            default:
                return CalculatedColumnUtil.ReadPayloadTyped(
                    CalculatedColumnUtil.Unwrap(row.AsSpan(start, len)),
                    JetTypeInfo.ResolveValueType(col),
                    this.strictParsing);
        }
    }

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows as a string-typed <see cref="DataTable"/> asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read, or <c>null</c> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> with all columns typed as <see cref="string"/>.</returns>
    public async ValueTask<DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        (CatalogEntry Entry, TableDef Td)? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            DataTable? linkedTable = await this.TryReadLinkedTableAsync(
                tableName,
                link => LinkedTableManager.ReadLinkedTextDataTableAsync(this, link, maxRows, progress, cancellationToken),
                (source, link) => source.ReadTableAsStringsAsync(link.SourceObjectName, maxRows, progress, cancellationToken),
                cancellationToken).ConfigureAwait(false);

#pragma warning disable CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
            return linkedTable ?? new DataTable(tableName);
#pragma warning restore CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
        }

        (CatalogEntry? entry, TableDef? td) = resolved.Value;
        DataTable? dt = null;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                _ = dt.Columns.Add(col.Name, typeof(string));
            }

            IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

            foreach (long pageNumber in pageNumbers)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await this.ReadPageCachedAsync(pageNumber, cancellationToken).ConfigureAwait(false);

                await foreach (string[] row in this.EnumerateRowsAsync(pageNumber, page, td, cancellationToken).ConfigureAwait(false))
                {
                    _ = dt.Rows.Add(row);
                    if (maxRows.HasValue && dt.Rows.Count >= maxRows.Value)
                    {
                        DataTable result = dt;
                        dt = null;
                        return result;
                    }
                }

                progress?.Report(dt.Rows.Count);
            }

            DataTable final = dt;
            dt = null;
            return final;
        }
        finally
        {
            dt?.Dispose();
        }
    }

    /// <summary>
    /// Returns statistical information about the database asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    /// <returns>A <see cref="DatabaseStatistics"/> object containing various metrics about the database.</returns>
    public async ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        var tableRowCounts = new Dictionary<string, long>();
        long totalRows = 0;

        foreach (CatalogEntry table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TableDef? td = await this.ReadTableDefAsync(table.TDefPage, cancellationToken).ConfigureAwait(false);
            if (td != null)
            {
                tableRowCounts[table.Name] = td.RowCount;
                totalRows += td.RowCount;
            }
        }

        long cacheHits = this.pageCache?.Hits ?? 0;
        long cacheMisses = this.pageCache?.Misses ?? 0;
        long totalAccess = cacheHits + cacheMisses;
        int pageCacheHitRate = totalAccess > 0 ? (int)(cacheHits * 100 / totalAccess) : 0;

        return new DatabaseStatistics
        {
            TotalPages = this.DatabaseStream.Length / this.PageSizeBytes,
            DatabaseSizeBytes = this.DatabaseStream.Length,
            TableCount = tables.Count,
            TotalRows = totalRows,
            TableRowCounts = tableRowCounts,
            PageCacheHitRate = pageCacheHitRate,
            Version = this.Format == DatabaseFormat.Jet3Mdb ? "Jet3" : "Jet4/ACE",
            Format = this.Format,
            CodePage = this.CodePageCore,
        };
    }

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns asynchronously.
    /// Each table's columns use their native CLR types (int, DateTimeType, decimal, etc.).
    /// </summary>
    /// <param name="progress">Optional progress reporter for table read operations.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A dictionary mapping table names to their corresponding DataTables.</returns>
    public async ValueTask<Dictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();

        var result = new Dictionary<string, DataTable>(StringComparer.OrdinalIgnoreCase);
        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < tables.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CatalogEntry table = tables[i];
            progress?.Report(new TableProgress { TableName = table.Name, TableIndex = i, TableCount = tables.Count });
            result[table.Name] = await this.ReadDataTableAsync(table.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with all columns typed as strings asynchronously.
    /// Use this for compatibility scenarios.
    /// </summary>
    /// <param name="progress">Optional progress reporter for table read operations.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A dictionary mapping table names to their corresponding DataTables with all columns as strings.</returns>
    public async ValueTask<Dictionary<string, DataTable>> ReadAllTablesAsStringsAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();

        var result = new Dictionary<string, DataTable>(StringComparer.OrdinalIgnoreCase);
        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < tables.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CatalogEntry table = tables[i];
            progress?.Report(new TableProgress { TableName = table.Name, TableIndex = i, TableCount = tables.Count });
            result[table.Name] = await this.ReadTableAsStringsAsync(table.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc/>
    [SuppressMessage("Usage", "CA2215:Dispose methods should call base class dispose", Justification = "base.DisposeAsync is invoked from DisposeReaderResourcesAsync, passed as a step to LockFileCoordinator.DisposeAfterAsync.")]
    public override async ValueTask DisposeAsync()
    {
        if (!this.operationGate.TryBeginDispose(out Task? waitForOperations))
        {
            await this.operationGate.DisposeCompleted.ConfigureAwait(false);
            return;
        }

        try
        {
            // The coordinator drains every step in order, aggregates failures,
            // then unconditionally releases the .ldb / .laccdb slot.
            await this.lockFile.DisposeAfterAsync(
                waitForOperations,
                this.DisposeReaderResourcesAsync).ConfigureAwait(false);
            this.operationGate.CompleteDispose();
        }
        catch (Exception ex)
        {
            this.operationGate.CompleteDispose(ex);
            throw;
        }
    }

    private static FileStream CreateStream(string path, AccessReaderOptions options)
    {
        FileOptions accessPattern = options.ParallelPageReadsEnabled ? FileOptions.RandomAccess : FileOptions.SequentialScan;
        return OpenDatabaseFileStream(path, options.FileAccess, options.FileShare, FileOptions.Asynchronous | accessPattern);
    }

    private static string ResolveTypeName(ColumnInfo col) =>
        JetTypeInfo.IsHyperlinkColumn(col) ? "Hyperlink" : JetTypeInfo.GetTypeDisplayName(JetTypeInfo.ResolveValueType(col));

    /// <summary>
    /// Unwraps common OLE 1.0 package envelopes and scans the resulting payload
    /// for known file signatures (images, PDFs, Office docs, archives).
    /// Typical Access OLE fields prepend a package header before the embedded
    /// file bytes, so package-aware extraction must run before the generic
    /// sliding magic-byte scan.
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

    private delegate bool BytePatternMatcher<TState>(ReadOnlySpan<byte> window, ref TState state);

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

    /// <summary>
    /// Wraps text payloads of Hyperlink-flagged columns in a typed row into
    /// <see cref="Hyperlink"/> instances, mirroring the projection
    /// <see cref="JetTypeInfo.ResolveClrType"/> exposes via the public API.
    /// Non-string slots (e.g. <see cref="DBNull.Value"/>) are left untouched;
    /// strings that fail to parse collapse to <see cref="DBNull.Value"/>
    /// (matching <see cref="TypedValueParser.ParseValue"/>'s legacy behaviour).
    /// </summary>
    /// <param name="columns">The columns.</param>
    /// <param name="wantedColumns">The wanted columns.</param>
    /// <param name="type1">The type1.</param>
    /// <param name="type2">The type2.</param>
    private static bool HasWantedColumnOfType(List<ColumnInfo> columns, bool[] wantedColumns, ColumnType type1, ColumnType type2)
    {
        int limit = Math.Min(columns.Count, wantedColumns.Length);
        for (int i = 0; i < limit; i++)
        {
            if (wantedColumns[i] && (columns[i].Type == type1 || columns[i].Type == type2))
            {
                return true;
            }
        }

        return false;
    }

    private static bool HasWantedHyperlinkColumn(Type[] clrTypes, bool[] wantedColumns)
    {
        int limit = Math.Min(clrTypes.Length, wantedColumns.Length);
        for (int i = 0; i < limit; i++)
        {
            if (wantedColumns[i] && clrTypes[i] == typeof(Hyperlink))
            {
                return true;
            }
        }

        return false;
    }

    private static bool HasCacheReentrantScanColumns(TableDef tableDef)
    {
        foreach (ColumnInfo column in tableDef.Columns)
        {
            if (column.Type is MemoType or OleType or ComplexType or AttachmentType)
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask ObserveAbandonedTableScanReadAsync(Task<TableScanPage> task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
            return;
        }

        await task.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default).ConfigureAwait(false);
    }

    private static void WrapHyperlinkColumns(object?[] typedRow, Type[] clrTypes)
    {
        int limit = Math.Min(clrTypes.Length, typedRow.Length);
        for (int i = 0; i < limit; i++)
        {
            if (clrTypes[i] != typeof(Hyperlink))
            {
                continue;
            }

            if (typedRow[i] is string s)
            {
                typedRow[i] = (object?)Hyperlink.Parse(s) ?? DBNull.Value;
            }
        }
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

    private static byte[] CreateOlePayloadBytes(byte[] buffer, int offset, int length, bool allowInputReuse) => allowInputReuse && offset == 0 && length == buffer.Length
            ? buffer
            : buffer.AsSpan(offset, length).ToArray();

    private async ValueTask DisposeReaderResourcesAsync()
    {
        this.pageCache?.Clear();
        this.pageCache?.Dispose();
        this.rowBoundsCache?.Clear();
        this.rowBoundsCache?.Dispose();
        this.ownedDataPageIndex.Dispose();
        this.InvalidateCatalogCache();
        await base.DisposeAsync().ConfigureAwait(false);
    }

    private async ValueTask<IReadOnlyList<long>> GetOwnedDataPagesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        if (tdefPage <= 0)
        {
            return [];
        }

        if (this.ActiveJournal is null && this.TryGetCachedOwnedDataPages(tdefPage, out long[] cachedPages))
        {
            return cachedPages;
        }

        long[]? mappedPages = await this.TryGetOwnedDataPagesFromUsageMapAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (mappedPages is not null)
        {
            if (this.ActiveJournal is null)
            {
                this.CacheOwnedDataPages(tdefPage, mappedPages);
            }

            return mappedPages;
        }

        Dictionary<long, long[]> pageIndex = await this.ownedDataPageIndex.GetAsync(cancellationToken).ConfigureAwait(false);
        return pageIndex.TryGetValue(tdefPage, out long[]? pageNumbers)
            ? pageNumbers
            : [];
    }

    private bool TryGetCachedOwnedDataPages(long tdefPage, out long[] pageNumbers)
    {
        lock (this.ownedDataPagesCacheLock)
        {
            return this.ownedDataPagesByTdef.TryGetValue(tdefPage, out pageNumbers);
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
                || !UsageMap.TryReadPointer(tdef, Constants.TableDefinition.OwnedPagesRowOffset, out UsageMap.Pointer pointer)
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
                    liveRows += this.GetLiveRowBoundsCached(pageNumber, page).Length;
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

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private protected override async ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken)
    {
        List<CatalogEntry>? cached = this.GetCatalogCache();
        if (cached != null)
        {
            return cached;
        }

        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await this.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            this.LastDiagnostics = "ERROR: Page 2 is not a valid TDEF page (null returned).";
            var empty = new List<CatalogEntry>();
            this.SetCatalogCache(empty);
            return empty;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxName = msys.FindColumnIndex("Name");
        int idxType = msys.FindColumnIndex("Type");
        int idxFlags = msys.FindColumnIndex("Flags");

        if (idxName < 0 || idxType < 0)
        {
            this.LastDiagnostics = "ERROR: Required catalog columns not found. Column name mismatch?";
            var empty = new List<CatalogEntry>();
            this.SetCatalogCache(empty);
            return empty;
        }

        var result = new List<CatalogEntry>();
        IReadOnlyList<long> catalogPageNumbers = await this.GetOwnedDataPagesAsync(2, cancellationToken).ConfigureAwait(false);
        int catPages = catalogPageNumbers.Count;
        int allRows = 0;

        foreach (long pageNumber in catalogPageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageCachedAsync(pageNumber, cancellationToken).ConfigureAwait(false);

            await foreach (string[] row in this.EnumerateRowsAsync(pageNumber, page, msys, cancellationToken).ConfigureAwait(false))
            {
                allRows++;
                string typeStr = CatalogValueReader.GetStringOrEmpty(row, idxType);
                string nameStr = CatalogValueReader.GetStringOrEmpty(row, idxName);
                string flagsStr = CatalogValueReader.GetStringOrEmpty(row, idxFlags);

                if (!CatalogValueReader.TryParseInt32(typeStr, out int objType) || objType != Constants.SystemObjects.UserTableType)
                {
                    continue;
                }

                if (!CatalogValueReader.TryParseInt64(flagsStr, out long flagsLong))
                {
                    flagsLong = 0;
                }

                if ((unchecked((uint)flagsLong) & Constants.SystemObjects.SystemTableMask) != 0)
                {
                    continue;
                }

                if (string.IsNullOrEmpty(nameStr))
                {
                    continue;
                }

                long tdefPage = 0;
                if (idxId >= 0)
                {
                    if (!CatalogValueReader.TryParseInt64(row, idxId, out long id))
                    {
                        id = 0;
                    }

                    tdefPage = id & 0x00FFFFFFL;
                }

                if (tdefPage > 0)
                {
                    result.Add(new CatalogEntry(nameStr, tdefPage));
                }
            }
        }

        if (this.DiagnosticsEnabled)
        {
            var diag = new StringBuilder();
            _ = diag.AppendLine($"JET: {(this.Format == DatabaseFormat.Jet3Mdb ? "Jet3" : "Jet4/ACE")}  PageSize: {this.PageSizeBytes}  TotalPages: {this.DatabaseStream.Length / this.PageSizeBytes}");
            _ = diag.AppendLine($"MSysObjects cols ({msys.Columns.Count}): " +
                string.Join(", ", msys.Columns.ConvertAll(c => $"{c.Name}[0x{(byte)c.Type:X2}]")));
            _ = diag.AppendLine($"Catalog pages: {catPages}  Total rows scanned: {allRows}  User tables: {result.Count}");
            foreach (CatalogEntry e in result)
            {
                _ = diag.AppendLine($"  [{e.Name}] TDEF page {e.TDefPage}");
            }

            this.LastDiagnostics = diag.ToString();
        }
        else
        {
            this.LastDiagnostics = string.Empty;
        }

        this.SetCatalogCache(result);
        return result;
    }

    internal async ValueTask<List<LinkedTableInfo>> GetLinkedTablesCachedAsync(CancellationToken cancellationToken)
    {
        List<LinkedTableInfo>? cached = this.GetLinkedTableCache();
        if (cached != null)
        {
            return cached;
        }

        List<LinkedTableInfo> links = await LinkedTableManager.GetLinkedTablesAsync(this, cancellationToken).ConfigureAwait(false);
        this.SetLinkedTableCache(links);
        return links;
    }

    private AsyncReentrantOperationGate.Lease EnterOperation() =>
        this.operationGate.Enter(this);

    private void ValidateDatabaseFormat()
    {
        if (this.DatabaseStream.Length < 128)
        {
            throw new InvalidDataException("File too small to be a valid JET database");
        }

        // Verify the JET magic signature at offset 0: 00 01 00 00
        _ = this.DatabaseStream.Seek(0, SeekOrigin.Begin);
        var magic = new byte[4];
        int read = this.DatabaseStream.Read(magic, 0, 4);
        if (read < 4 || magic[0] != 0x00 || magic[1] != 0x01 || magic[2] != 0x00 || magic[3] != 0x00)
        {
            var msg = $"File does not have a valid JET magic signature (expected 00 01 00 00, got {magic[0]:X2} {magic[1]:X2} {magic[2]:X2} {magic[3]:X2}).";
            throw new InvalidDataException(msg);
        }
    }

    /// <summary>Reads a page through the cache when one is configured (PageCacheSize &gt; 0) and no transaction journal is active.</summary>
    /// <param name="n">The item count.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async ValueTask<byte[]> ReadPageCachedAsync(long n, CancellationToken cancellationToken)
    {
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        if (this.ActiveJournal is not null)
        {
            return await this.ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        }

        if (this.pageCache is null)
        {
            return await this.ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        }

        if (this.pageCache.TryGetValue(n, out byte[] cached))
        {
            return cached;
        }

        byte[] page = await this.ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        this.pageCache.Add(n, page);
        return page;
    }

    internal bool TryGetCachedPage(long n, out byte[] page)
    {
        if (this.pageCache is not null && this.pageCache.TryGetValue(n, out page))
        {
            return true;
        }

        page = [];
        return false;
    }

    /// <summary>
    /// Returns the live row-bound directory for <paramref name="page"/>, computing
    /// it on first request and caching the result keyed by <paramref name="pageNumber"/>
    /// when a page cache is configured. The returned array is owned by the cache —
    /// callers must not mutate it. Used by the typed/untyped scan paths to avoid
    /// re-parsing the row-offset trailer on repeated scans of the same table.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    internal RowBound[] GetLiveRowBoundsCached(long pageNumber, byte[] page)
    {
        if (this.ActiveJournal is not null)
        {
            return this.ComputeLiveRowBoundsArray(page);
        }

        if (this.rowBoundsCache is not null && this.rowBoundsCache.TryGetValue(pageNumber, out RowBound[]? cached))
        {
            return cached;
        }

        RowBound[] bounds = this.ComputeLiveRowBoundsArray(page);
        this.rowBoundsCache?.Add(pageNumber, bounds);
        return bounds;
    }

    internal async ValueTask<(CatalogEntry Entry, TableDef Td)?> ResolveTableAsync(string tableName, CancellationToken cancellationToken)
    {
        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        CatalogEntry? entry = tables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
        if (entry != null)
        {
            TableDef? td = await this.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            if (td?.Columns.Count > 0)
            {
                await this.HydrateCalculatedResultTypesAsync(entry.TDefPage, td, cancellationToken).ConfigureAwait(false);
                return (entry, td);
            }
        }

        // Fall back to a system-table lookup (MSysObjects, MSysRelationships, etc.).
        // GetUserTablesAsync filters out rows whose Flags carry SYSTABLE_MASK, so
        // a name match against the catalog scan is needed for those.
        long sysPage = await this.FindSystemTablePageAsync(
            n => string.Equals(n, tableName, StringComparison.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);
        if (sysPage > 0)
        {
            TableDef? sysTd = await this.ReadTableDefAsync(sysPage, cancellationToken).ConfigureAwait(false);
            if (sysTd?.Columns.Count > 0)
            {
                await this.HydrateCalculatedResultTypesAsync(sysPage, sysTd, cancellationToken).ConfigureAwait(false);
                return (new CatalogEntry(tableName, sysPage), sysTd);
            }
        }

        return null;
    }

    private async ValueTask HydrateCalculatedResultTypesAsync(long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        if (!tableDef.Columns.Exists(static col => col.IsCalculated))
        {
            return;
        }

        ColumnPropertyBlock? properties = await this.ReadLvPropForTableAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (properties is null)
        {
            return;
        }

        bool changed = false;
        foreach (ColumnInfo col in tableDef.Columns)
        {
            if (!col.IsCalculated)
            {
                continue;
            }

            ColumnType resultType = ResolveCalculatedResultType(properties.FindTarget(col.Name));
            if (resultType != default && resultType != col.CalculatedResultType)
            {
                col.CalculatedResultType = resultType;
                changed = true;
            }
        }

        if (changed)
        {
            tableDef.InitializeColumnMetadata();
        }
    }

    /// <summary>Yields decoded rows from a single data page.</summary>
    /// <param name="pageNumber">The page number, used to memoize the parsed live-row directory in the row-bounds cache.</param>
    /// <param name="page">The data page to enumerate rows from.</param>
    /// <param name="td">The table definition containing column information.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for rows.</param>
    private async IAsyncEnumerable<string[]> EnumerateRowsAsync(long pageNumber, byte[] page, TableDef td, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (RowBound rb in this.GetLiveRowBoundsCached(pageNumber, page))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (rb.RowSize < this.RowFields.NumCols)
            {
                continue;
            }

            string[]? values = await this.CrackRowAsync(page, rb.RowStart, rb.RowSize, td, cancellationToken).ConfigureAwait(false);
            if (values != null)
            {
                yield return values;
            }
        }
    }

    private async ValueTask<string[]?> CrackRowAsync(byte[] page, int rowStart, int rowSize, TableDef td, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (rowSize < this.RowFields.NumCols)
        {
            return null;
        }

        // Pre-parse numCols just for the schema-evolution sanity check; the full
        // layout parse repeats this read but the cost is negligible.
        int rawNumCols = this.ReadRowColumnCount(page, rowStart);
        if (rawNumCols == 0)
        {
            return null;
        }

        // Tables with zero variable-length columns omit the var-length
        // metadata entirely (no varLen byte, no jump bytes, no var-offset
        // table, no EOD marker). Detect that and let the layout parser skip
        // the var-area read. When deleted-column gaps exist, force var-area
        // parsing because we don't know if a deleted column was var-length.
        bool hasVarCols = td.HasVarColumns || (td.HasDeletedColumns && rawNumCols > td.Columns.Count);

        if (!this.TryParseRowLayout(page, rowStart, rowSize, hasVarCols, out RowLayout layout))
        {
            return null;
        }

        var result = new string[td.Columns.Count];

        for (int i = 0; i < td.Columns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = td.Columns[i];
            ColumnSlice slice = this.ResolveColumnSlice(page, rowStart, rowSize, layout, col);

            result[i] = slice.Kind switch
            {
                ColumnSliceKind.Bool => slice.BoolValue ? "True" : "False",
                ColumnSliceKind.Null or ColumnSliceKind.Empty => string.Empty,
                ColumnSliceKind.Fixed => JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, col, slice.DataLen, strictNumeric: true),
                ColumnSliceKind.Var => await this.ReadVarAsync(page, rowStart + slice.DataStart, slice.DataLen, col, cancellationToken).ConfigureAwait(false),
                _ => string.Empty,
            };
        }

        return result;
    }

    private async ValueTask<string> ReadVarAsync(byte[] row, int start, int len, ColumnInfo col, CancellationToken cancellationToken)
    {
        if (len <= 0)
        {
            return string.Empty;
        }

        if (col.IsCalculated)
        {
            return await this.ReadCalculatedVarAsync(row, start, len, col, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            switch (col.Type)
            {
                case TextType:
                    return this.DecodeTextForFormat(row, start, len);

                case BinaryType:
                    return JetTypeInfo.ToHexStringNoSeparator(row.AsSpan(start, len));

                case MemoType:
                case OleType:
                    return await this.longValueDecoder.ReadLongValueAsync(row, start, len, col.Type == OleType, cancellationToken).ConfigureAwait(false);

                case ByteType:
                case IntegerType:
                case LongIntegerType:
                case FloatType:
                case DoubleType:
                case DateTimeType:
                case MoneyType:
                case BigIntType:
                case GuidType:
                case ComplexType:
                case AttachmentType:
                    // Delegate fixed-width primitive and complex-id formatting to the shared
                    // JetTypeInfo.ReadFixedString helper to avoid duplicating
                    // the per-type Invariant-culture formatting block. The
                    // length guard mirrors the historical behaviour (return
                    // empty when the variable-length slice is too short to
                    // contain the type's fixed payload) — JetTypeInfo gives
                    // 4 bytes for COMPLEX/ATTACHMENT (the complex-id int32)
                    // since they have no fixed-area size of their own.
                    int required = col.Type is ComplexType or AttachmentType ? 4 : JetTypeInfo.GetFixedSize(col.Type);
                    return len >= required ? JetTypeInfo.ReadFixedString(row, start, col, required, strictNumeric: true) : string.Empty;
                case BooleanType:
                case NumericType:
                case DateTimeExtendedType:
                    return string.Empty;
                default:
                    throw new InvalidOperationException($"Column '{col.Name}' has unknown type code 0x{(byte)col.Type:X2}.");
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
        catch (IndexOutOfRangeException)
        {
            return string.Empty;
        }
    }

    private async ValueTask<string> ReadCalculatedVarAsync(byte[] row, int start, int len, ColumnInfo col, CancellationToken cancellationToken)
    {
        try
        {
            switch (col.Type)
            {
                case TextType:
                    return this.DecodeCalculatedTextPayload(CalculatedColumnUtil.Unwrap(row.AsSpan(start, len)));
                case BinaryType:
                    return JetTypeInfo.ToHexStringNoSeparator(CalculatedColumnUtil.Unwrap(row.AsSpan(start, len)));
                case MemoType:
                {
                    byte[] raw = await this.longValueDecoder.ReadLongValueRawBytesAsync(row, start, len, cancellationToken).ConfigureAwait(false);
                    byte[] payload = CalculatedColumnUtil.Unwrap(raw);
                    return this.longValueDecoder.DecodeLongValue(payload, 0, payload.Length, isOle: false);
                }

                case OleType:
                {
                    byte[] raw = await this.longValueDecoder.ReadLongValueRawBytesAsync(row, start, len, cancellationToken).ConfigureAwait(false);
                    byte[] payload = CalculatedColumnUtil.Unwrap(raw);
                    return this.longValueDecoder.DecodeLongValue(payload, 0, payload.Length, isOle: true);
                }

                default:
                    return CalculatedColumnUtil.ReadPayloadString(
                        CalculatedColumnUtil.Unwrap(row.AsSpan(start, len)),
                        JetTypeInfo.ResolveValueType(col),
                        this.strictParsing);
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
        catch (IndexOutOfRangeException)
        {
            return string.Empty;
        }
    }

    private string DecodeCalculatedTextPayload(byte[] payload)
        => this.DecodeTextForFormat(payload, 0, payload.Length);

    // ── Typed row cracker ────────────────────────────────────
    //
    // CrackRowTypedAsync fills an object?[] of length td.Columns.Count
    // directly from the page bytes — no intermediate List<string> + per-
    // column culture-invariant formatting + re-parse round-trip. Fixed-
    // width primitives go through JetTypeInfo.ReadFixedTyped; variable-
    // width text goes straight to a managed string; Binary is copied as
    // byte[]; Memo/Ole keep their async branch only when the LVAL
    // chain actually needs to be walked (the inline 0x80 case stays sync).
    // RowDecodePlan carries the optional projection mask: unwanted columns
    // are left as null, while the row layout is still parsed once so variable
    // offsets remain valid for every wanted column.
    //
    // The split is exposed as TryCrackRowSync — callers that know they
    // are on the fully-sync hot path (e.g. fixed-only / inline-only
    // tables) can avoid the await/state-machine cost entirely.
    // Cancellation is checked once per row, not per column.
    //
    // The public Rows() / ReadDataTableAsync entry points wire this in;
    // complex-attachment resolution and Hyperlink wrapping are applied as
    // post-processing passes (ResolveComplexColumns / WrapHyperlinkColumns)
    // gated by the per-table HasComplexColumns / HasHyperlinkColumns flags.

    private ValueTask<object?[]?> CrackRowTypedAsync(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!this.TryCrackRowSync(page, rowStart, rowSize, decodePlan, out object?[]? row, out bool needsLongValue))
        {
            return new ValueTask<object?[]?>((object?[]?)null);
        }

        // Fast path: no Memo/Ole LVAL chain walk needed — return a
        // sync-completed ValueTask so the caller never builds an async
        // state machine for fixed-only / inline-only rows.
        if (!needsLongValue)
        {
            return new ValueTask<object?[]?>(row);
        }

        return this.ResolveLongValueRefsAsync(row!, page, cancellationToken);
    }

    /// <summary>
    /// Buffer-filling counterpart to <c>CrackRowTypedAsync</c>.
    /// Returns <see langword="true"/> when the row was successfully decoded
    /// into the first <c>td.Columns.Count</c> slots of
    /// <paramref name="buffer"/>; <see langword="false"/> when the row
    /// trailer was malformed (caller should skip without resetting the
    /// buffer — the next iteration will overwrite it). Used by
    /// <see cref="ReadDataTableAsync"/> and the projection-aware fallback in
    /// <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>
    /// to reuse a single <see cref="ArrayPool{T}.Shared"/>-rented array
    /// across the entire scan.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="decodePlan">The decode plan.</param>
    /// <param name="buffer">The buffer.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private ValueTask<bool> CrackRowTypedIntoBufferAsync(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, object?[] buffer, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!this.TryCrackRowSyncIntoBuffer(page, rowStart, rowSize, decodePlan, buffer, out bool needsLongValue))
        {
            return new ValueTask<bool>(false);
        }

        if (!needsLongValue)
        {
            return new ValueTask<bool>(true);
        }

        return this.ResolveLongValueRefsIntoBufferAsync(buffer, decodePlan.ColumnCount, page, cancellationToken);
    }

    /// <summary>
    /// Buffer-aware mirror of <c>ResolveLongValueRefsAsync</c>: walks only
    /// the first <paramref name="validLength"/> slots of
    /// <paramref name="buffer"/> (the pooled array may be larger than
    /// <c>td.Columns.Count</c>).
    /// </summary>
    /// <param name="buffer">The buffer.</param>
    /// <param name="validLength">The valid length.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private async ValueTask<bool> ResolveLongValueRefsIntoBufferAsync(object?[] buffer, int validLength, byte[] page, CancellationToken cancellationToken)
    {
        for (int i = 0; i < validLength; i++)
        {
            if (buffer[i] is RowDecodePlan.LongValueRef lvr)
            {
                buffer[i] = lvr.IsOle
                    ? await this.longValueDecoder.ReadOleValueBytesAsync(page, lvr.Start, lvr.Len, cancellationToken).ConfigureAwait(false)
                    : await this.longValueDecoder.ReadLongValueAsync(page, lvr.Start, lvr.Len, isOle: false, cancellationToken).ConfigureAwait(false);
            }
            else if (buffer[i] is RowDecodePlan.CalculatedLongValueRef clvr)
            {
                buffer[i] = await this.ResolveCalculatedLongValueRefAsync(page, clvr, cancellationToken).ConfigureAwait(false);
            }
        }

        return true;
    }

    /// <summary>
    /// Async slow-path that walks the LVAL chain for any
    /// <see cref="RowDecodePlan.LongValueRef"/> sentinels left in <paramref name="row"/>
    /// by <c>TryCrackRowSync</c>. Only invoked when at least one
    /// such sentinel was emitted — fixed-only / inline-only rows skip this
    /// entirely and never allocate an async state machine.
    /// </summary>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    private async ValueTask<object?[]?> ResolveLongValueRefsAsync(object?[] row, byte[] page, CancellationToken cancellationToken)
    {
        _ = await this.ResolveLongValueRefsIntoBufferAsync(row, row.Length, page, cancellationToken).ConfigureAwait(false);
        return row;
    }

    private async ValueTask<object> ResolveCalculatedLongValueRefAsync(byte[] page, RowDecodePlan.CalculatedLongValueRef reference, CancellationToken cancellationToken)
    {
        byte[] raw = await this.longValueDecoder.ReadLongValueRawBytesAsync(page, reference.Start, reference.Len, cancellationToken).ConfigureAwait(false);
        byte[] payload = CalculatedColumnUtil.Unwrap(raw);
        return reference.IsOle
            ? DecodeOleValueBytes(payload, 0, payload.Length)
            : this.longValueDecoder.DecodeLongValue(payload, 0, payload.Length, isOle: false);
    }

    /// <summary>
    /// Synchronously decodes a row into a typed <c>object?[]</c>. Returns
    /// <see langword="false"/> when the row trailer is malformed or the
    /// schema sanity-check rejects the row (caller should skip).
    /// <paramref name="needsLongValue"/> is set when one or more
    /// <c>Memo</c>/<c>Ole</c> slots require an LVAL-chain walk; those
    /// slots are filled with a <see cref="RowDecodePlan.LongValueRef"/> sentinel that the
    /// async wrapper (<c>CrackRowTypedAsync</c>) replaces.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="decodePlan">The decode plan.</param>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="needsLongValue">The needs long value.</param>
    private bool TryCrackRowSync(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, out object?[]? row, out bool needsLongValue)
    {
        var result = new object?[decodePlan.ColumnCount];
        if (!this.TryCrackRowSyncIntoBuffer(page, rowStart, rowSize, decodePlan, result, out needsLongValue))
        {
            row = null;
            return false;
        }

        row = result;
        return true;
    }

    /// <summary>
    /// Buffer-filling core of <c>TryCrackRowSync</c>: lets non-yielding callers
    /// (<see cref="ReadDataTableAsync"/>, the projection-aware fallback in
    /// <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>)
    /// rent a single <c>object?[]</c> from <see cref="ArrayPool{T}.Shared"/>
    /// and re-use it across every row instead of allocating a fresh array
    /// per row. <paramref name="buffer"/> must have length
    /// &gt;= <c>td.Columns.Count</c>; the first <c>td.Columns.Count</c>
    /// slots are fully overwritten on success.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="decodePlan">The decode plan.</param>
    /// <param name="buffer">The buffer.</param>
    /// <param name="needsLongValue">The needs long value.</param>
    private bool TryCrackRowSyncIntoBuffer(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, object?[] buffer, out bool needsLongValue)
        => decodePlan.TryDecodeTypedIntoBuffer(this, page, rowStart, rowSize, this.longValueDecoder, buffer, out needsLongValue);

    // ── Direct page → T decoder support ───────────────────────────────
    //
    // The "direct decoder" eliminates the per-row object?[] buffer and
    // the box/unbox round-trip on every primitive column. RowMapper<T>
    // compiles a delegate that reads typed values straight out of the
    // page bytes and assigns them to T's properties; only the columns
    // the mapper actually binds are decoded (the projection mask is
    // baked in). Callers gate the fast path with
    // RowMapper<T>.TryBuildDirectDecoder which inspects each bound
    // column and returns null when any column requires the slow path
    // (Memo/Ole LVAL chain, Binary, Numeric, Complex/
    // Attachment, Hyperlink-typed properties).
    //
    // The compiled delegate calls back into a small set of internal
    // helpers below for the reader's per-instance state (format,
    // ANSI encoding) and the row-trailer parse.

    /// <summary>
    /// Internal accessor for <see cref="AccessBase.TryParseRowLayout"/>
    /// callable from <see cref="JetDatabaseWriter.ValueDecoding.RowMapper{T}"/>'s
    /// compiled direct-decoder delegate.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="hasVarColumns">A value indicating whether has var columns.</param>
    /// <param name="layout">The layout.</param>
    internal bool TryParseRowLayoutForDirectDecode(byte[] page, int rowStart, int rowSize, bool hasVarColumns, out RowLayout layout)
        => this.TryParseRowLayout(page, rowStart, rowSize, hasVarColumns, out layout);

    /// <summary>
    /// Internal accessor for <see cref="AccessBase.ResolveColumnSlice"/>
    /// callable from the compiled direct-decoder delegate. Takes
    /// <paramref name="layout"/> by value (not <c>in</c>) so expression
    /// trees can pass a <c>ParameterExpression</c> directly.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="col">The column descriptor.</param>
    internal ColumnSlice ResolveColumnSliceForDirectDecode(byte[] page, int rowStart, int rowSize, RowLayout layout, ColumnInfo col)
        => this.ResolveColumnSlice(page, rowStart, rowSize, layout, col);

    /// <summary>
    /// Internal text decoder used by the compiled direct-decoder delegate.
    /// Picks the format-appropriate path (Jet4 Unicode/compressed vs Jet3
    /// ANSI) and returns <see cref="string.Empty"/> for empty slices.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    internal string DecodeTextSliceForDirectDecode(byte[] page, int start, int len)
        => this.DecodeTextForFormat(page, start, len);

    /// <summary>
    /// Gets the minimum row size below which the row trailer parser will
    /// reject the row outright. Used by the compiled direct-decoder
    /// delegate's preflight check (mirrors <c>TryCrackRowSync</c>).
    /// </summary>
    internal int NumColsFieldSize => this.RowFields.NumCols;

    /// <summary>
    /// Internal helper for the compiled direct decoder's first-row-bytes
    /// peek (matches the rawNumCols extraction in <c>TryCrackRowSync</c>).
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    internal int ReadRawNumCols(byte[] page, int rowStart)
        => this.ReadRowColumnCount(page, rowStart);

    private readonly record struct TableScanPage(long PageNumber, byte[] Page);

    /// <summary>
    /// Yields rows from every data page whose owning TDEF page equals <paramref name="tdefPage"/>.
    /// Centralises the common scan-all-pages-and-decode-rows pattern used by catalog/system-table readers.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async IAsyncEnumerable<string[]> EnumerateRowsForTdefAsync(
        long tdefPage,
        TableDef td,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageCachedAsync(pageNumber, cancellationToken).ConfigureAwait(false);

            await foreach (string[] row in this.EnumerateRowsAsync(pageNumber, page, td, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }
        }
    }

    /// <summary>Loads the MSysObjects TableDef (page 2). Exposed for <see cref="LinkedTableManager"/>.</summary>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal ValueTask<TableDef?> GetMSysObjectsTableDefAsync(CancellationToken cancellationToken) =>
        this.ReadTableDefAsync(2, cancellationToken);

    /// <summary>Enumerates every row of MSysObjects. Exposed for <see cref="LinkedTableManager"/>.</summary>
    /// <param name="msys">The system-table data.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal IAsyncEnumerable<string[]> EnumerateMSysObjectsRowsAsync(TableDef msys, CancellationToken cancellationToken) =>
        this.EnumerateRowsForTdefAsync(2, msys, cancellationToken);

    /// <summary>
    /// Returns the concatenated TDEF page-chain bytes for <paramref name="tdefPage"/>,
    /// with the 8-byte page header included for the first page and stripped from
    /// continuations (matches <see cref="AccessBase.ReadTDefBytesAsync"/>). Returns
    /// <see langword="null"/> when the page is not a valid TDEF root. Diagnostic-only
    /// helper for the format-probe tool under <c>JetDatabaseWriter.FormatProbe</c>.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal ValueTask<byte[]?> GetRawTDefBytesAsync(long tdefPage, CancellationToken cancellationToken) =>
        this.ReadTDefBytesAsync(tdefPage, cancellationToken);

    /// <summary>
    /// Returns a heap-allocated copy of the raw bytes of <paramref name="pageNumber"/>
    /// (post-decryption). Diagnostic-only helper for the format-probe tool under
    /// <c>JetDatabaseWriter.FormatProbe</c>; production code should not call this.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async ValueTask<byte[]> GetRawPageBytesAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] pooled = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        var copy = new byte[this.PageSizeBytes];
        Buffer.BlockCopy(pooled, 0, copy, 0, this.PageSizeBytes);
        ReturnPage(pooled);
        return copy;
    }

    /// <summary>
    /// Reads and parses the <c>MSysObjects.LvProp</c> blob for the catalog row whose
    /// <c>Id</c> column's low-24 bits match <paramref name="tdefPage"/>. Returns
    /// <see langword="null"/> when the catalog has no <c>LvProp</c> column (slim
    /// schemas written by older versions of this library), the row is missing, the
    /// blob is empty, or the magic header is unrecognised.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async ValueTask<ColumnPropertyBlock?> ReadLvPropForTableAsync(long tdefPage, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await this.GetMSysObjectsTableDefAsync(cancellationToken).ConfigureAwait(false);
        if (msys is null)
        {
            return null;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxLvProp = msys.FindColumnIndex("LvProp");
        if (idxId < 0 || idxLvProp < 0)
        {
            return null;
        }

        await foreach (string[] row in this.EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            if (!CatalogValueReader.TryParseInt64(row, idxId, out long id))
            {
                continue;
            }

            if ((id & 0x00FFFFFFL) != tdefPage)
            {
                continue;
            }

            byte[]? blob = TryDecodeBase64DataUrl(CatalogValueReader.GetStringOrEmpty(row, idxLvProp));
            return ColumnPropertyBlock.Parse(blob, this.Format);
        }

        return null;

        static byte[]? TryDecodeBase64DataUrl(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

            const string prefix = "data:application/octet-stream;base64,";
            if (!value.StartsWith(prefix, StringComparison.Ordinal))
            {
                return null;
            }

            return BinaryStringParser.TryDecodeBase64(value.AsSpan(prefix.Length), out byte[] bytes) ? bytes : null;
        }
    }

    /// <summary>
    /// Finds the TDEF page number for a system table by name (case-insensitive).
    /// Unlike GetUserTables, this includes system tables (SYSTABLE_MASK set).
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal ValueTask<long> FindSystemTablePageAsync(string name, CancellationToken cancellationToken) =>
        this.FindSystemTablePageAsync(
            n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase),
            cancellationToken);

    /// <summary>
    /// Finds the TDEF page for the first system table whose name satisfies <paramref name="nameMatches"/>.
    /// Shared by exact-name and suffix lookups against MSysObjects.
    /// </summary>
    /// <param name="nameMatches">The name matches.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async ValueTask<long> FindSystemTablePageAsync(Predicate<string> nameMatches, CancellationToken cancellationToken)
    {
        TableDef? msys = await this.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxName = msys.FindColumnIndex("Name");
        int idxType = msys.FindColumnIndex("Type");

        if (idxId < 0 || idxName < 0 || idxType < 0)
        {
            return 0;
        }

        await foreach (string[] row in this.EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            string nameStr = CatalogValueReader.GetStringOrEmpty(row, idxName);
            if (!nameMatches(nameStr))
            {
                continue;
            }

            if (!CatalogValueReader.TryParseInt32(row, idxType, out int objType) || (objType != Constants.SystemObjects.UserTableType && objType != Constants.SystemObjects.LinkedOdbcType))
            {
                continue;
            }

            if (CatalogValueReader.TryParseInt64(row, idxId, out long id))
            {
                long tdefPage = id & 0x00FFFFFFL;
                if (tdefPage > 0)
                {
                    return tdefPage;
                }
            }
        }

        return 0;
    }

    // [memo_len: 3 bytes][bitmask: 1 byte][lval_dp: 4 bytes][LVAL token: 4 bytes]
    // 0x80 = inline data immediately after the 12-byte header
    // 0x40 = single LVAL page:  lval_dp = (page << 8) | row_index
    // 0x00 = chained LVAL pages
}
