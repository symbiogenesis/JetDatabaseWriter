namespace JetDatabaseWriter;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
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
    private const int MinimumAutoTableScanReadAheadPages = 3;
    private const int MinimumTableScanReadAheadCacheSlots = 3;

    private readonly AsyncReentrantOperationGate operationGate = new();
    private readonly LockFileCoordinator lockFile;
    private readonly bool strictParsing;
    private readonly ComplexColumnReader complexColumns;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in DisposeReaderResourcesAsync, with failed construction cleaned up by DisposeReaderConstructionResources.")]
    private readonly LruCache<long, byte[]>? pageCache;
    private readonly LongValueDecoder longValueDecoder;

    /// <summary>
    /// Memoize the parsed live-row directory per data page. Same eviction
    /// profile as pageCache (sized 1:1 with it) so a page that's still hot in
    /// the byte-cache also keeps its bounds array. Stale entries left behind
    /// after a page is evicted from pageCache simply age out of this LRU on
    /// their own — correctness doesn't depend on the two caches being kept in
    /// lock-step.
    /// </summary>
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in DisposeReaderResourcesAsync, with failed construction cleaned up by DisposeReaderConstructionResources.")]
    private readonly LruCache<long, RowBound[]>? rowBoundsCache;

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReader"/> class.
    /// Opens <paramref name="path"/> and detects the JET version.
    /// </summary>
    /// <param name="path">The path to the Access database file. May be empty when opened from a stream.</param>
    /// <param name="options">Options for configuring the AccessReader.</param>
    /// <param name="stream">An open, seekable stream for the database file.</param>
    /// <param name="header">Header bytes read from page 0.</param>
    /// <param name="leaveOpen">Whether the caller retains ownership of the stream. If false, the stream is disposed when the reader is disposed.</param>
    /// <param name="suppressPageCache">Whether to skip allocating the per-reader page caches regardless of options.</param>
    private AccessReader(
        string path,
        AccessReaderOptions options,
        Stream stream,
        byte[] header,
        bool leaveOpen = false,
        bool suppressPageCache = false)
        : base(
            stream,
            header,
            options.Password,
            path,
            leaveOpen)
    {
        Guard.NotNull(options, nameof(options));

        this.lockFile = LockFileCoordinator.ForReader(path, options);
        bool constructionComplete = false;
        try
        {
            this.strictParsing = options.StrictParsing;
            this.complexColumns = new ComplexColumnReader(this);
            this.LinkedSourceOpenOptions = LinkedTableManager.CreateLinkedSourceOpenOptions(options, path);

            this.DiagnosticsEnabled = options.DiagnosticsEnabled;
            this.PageCacheSize = options.PageCacheSize;
            this.PageReadOptimizationMode = options.PageReadOptimizationMode;

            // Cache is created up front when enabled (>0); negative or zero leaves
            // it null and ReadPageCachedAsync bypasses caching entirely.
            if (!suppressPageCache && this.PageCacheSize > 0)
            {
                this.pageCache = new LruCache<long, byte[]>(this.PageCacheSize, ReturnPage);
                this.rowBoundsCache = new LruCache<long, RowBound[]>(this.PageCacheSize);
            }

            this.longValueDecoder = new LongValueDecoder(this);

            bool isLegacyAesCfb = EncryptionManager.IsCompoundFileEncrypted(header);
            if (isLegacyAesCfb)
            {
                // ACCDB AES (legacy synthetic CFB header path): page-level
                // decryption is now configured; skip catalog validation because
                // the header bytes themselves are still raw CFB until ReadPageAsync
                // decrypts page 1+ on first access.
                constructionComplete = true;
                return;
            }

            if (options.ValidateOnOpen)
            {
                this.ValidateDatabaseFormat();
            }

            // OpenAsync's catch owns only the stream and never sees this
            // half-built reader, so failed construction after slot acquisition
            // must release the lock-file slot through DisposeReaderConstructionResources.
            this.lockFile.Acquire();
            this.ByteRangeLockCore = JetByteRangeLock.Create(stream, options.UseByteRangeLocks, options.LockTimeoutMilliseconds);
            constructionComplete = true;
        }
        finally
        {
            if (!constructionComplete)
            {
                this.DisposeReaderConstructionResources();
            }
        }
    }

    /// <summary>Gets a value indicating whether to print console logs with verbose hex dumps for debugging. Default: false.</summary>
    public bool DiagnosticsEnabled { get; }

    /// <summary>Gets the maximum number of pages to keep in cache. Positive values enable caching; 0 or negative disables it. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; } = 256;

    /// <summary>Gets the page-I/O optimization mode used by this reader.</summary>
    public PageReadOptimizationMode PageReadOptimizationMode { get; }

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
        if (CanUseRandomAccessPageReads(reader.PageReadOptimizationMode))
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
            var decodePlan = RowDecodePlan.CreateStrings(td, this.strictParsing);

            await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                await foreach (string[] row in this.EnumerateRowsAsync(scanPage.PageNumber, scanPage.Page, decodePlan, cancellationToken).ConfigureAwait(false))
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
    public async ValueTask<IReadOnlyList<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        List<LinkedTableInfo> links = await this.GetLinkedTablesCachedAsync(cancellationToken).ConfigureAwait(false);
        return links.ConvertAll(static link => link with { }); // Clone to detach from internal cache instances
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default)
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

            IReadOnlyList<TableStat> stats = await this.GetTableStatsAsync(cancellationToken).ConfigureAwait(false);
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

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            long? linkedCount = await this.TryGetLinkedTableRowCountAsync(tableName, cancellationToken).ConfigureAwait(false);
            return linkedCount ?? 0;
        }

        long count = 0;
        long tdefPage = resolved.Entry.TDefPage;
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

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (object[] row in this.EnumerateLinkedRowsAsync(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        await foreach (object?[] row in this.EnumerateTypedRowsAsync(tableName, entry, td, wantedColumns: null, progress, cancellationToken).ConfigureAwait(false))
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

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (T? row in this.EnumerateLinkedRowsAsync<T>(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;

        // Bind the compiled mapper directly against the per-table column
        // headers + ClrTypes; avoids the GetColumnMetadataAsync round-trip
        // and the second async-iterator state machine that the previous
        // implementation built by re-entering Rows().
        string[] headers = new string[td.Columns.Count];
        for (int i = 0; i < td.Columns.Count; i++)
        {
            headers[i] = td.Columns[i].Name;
        }

        // Try to compile a direct page → T decoder that skips the per-row
        // object?[] buffer and primitive boxing entirely. The builder returns
        // null when any bound column requires the slow path (Memo/Ole
        // LVAL chain, Complex/Attachment, Hyperlink prop).
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

    /// <inheritdoc/>
    public IAsyncEnumerable<T> Rows<T>(
        string tableName,
        Expression<Func<T, bool>> predicate,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(predicate, nameof(predicate));

        Func<T, bool> compiled = predicate.Compile();
        RowCriteria pushable = IndexPredicateTranslator.ExtractPushableCriteria(predicate);
        return this.RowsInferredAsync(tableName, compiled, pushable, progress, cancellationToken);
    }

    /// <summary>
    /// Drives <see cref="Rows{T}(string, Expression{Func{T, bool}}, IProgress{long}?, CancellationToken)"/>:
    /// plans an index seek from the pushable predicate, streams from the index when
    /// one is usable (or scans otherwise), and applies the fully compiled predicate
    /// to every candidate row. The seek only ever narrows the candidate set, so the
    /// compiled filter guarantees the result is exactly the predicate's matches.
    /// </summary>
    /// <typeparam name="T">The mapped row type.</typeparam>
    /// <param name="tableName">The table to read.</param>
    /// <param name="predicate">The compiled row filter applied to every candidate.</param>
    /// <param name="pushable">The index-seekable necessary conditions extracted from the predicate.</param>
    /// <param name="progress">Optional matched-row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    private async IAsyncEnumerable<T> RowsInferredAsync<T>(
        string tableName,
        Func<T, bool> predicate,
        RowCriteria pushable,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class, new()
    {
        IndexPlan? plan = await this.TryPlanIndexReadAsync(tableName, pushable, cancellationToken).ConfigureAwait(false);

        IAsyncEnumerable<T> candidates = plan is not null
            ? this.ReadIndexRowsAsync<T>(tableName, plan.Index.Name, plan.Criteria, cancellationToken)
            : this.Rows<T>(tableName, progress: null, cancellationToken);

        long produced = 0;
        await foreach (T item in candidates.ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (predicate(item))
            {
                produced++;
                progress?.Report(produced);
                yield return item;
            }
        }
    }

    /// <summary>
    /// Picks the index that best satisfies <paramref name="pushable"/>, or returns
    /// <see langword="null"/> when a full scan is required (no pushable conditions,
    /// a Jet3 database, a linked or missing table, or no covering index).
    /// </summary>
    /// <param name="tableName">The table to read.</param>
    /// <param name="pushable">The index-seekable necessary conditions.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The chosen index plan, or <see langword="null"/> to scan.</returns>
    private async ValueTask<IndexPlan?> TryPlanIndexReadAsync(
        string tableName,
        RowCriteria pushable,
        CancellationToken cancellationToken)
    {
        // Index seeks are Jet4/ACE-only; everything else falls back to a scan.
        if (pushable.Count == 0 || this.Format == DatabaseFormat.Jet3Mdb)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await this.ListIndexesAsync(tableName, cancellationToken).ConfigureAwait(false);
        return IndexPlanner.TryPlan(indexes, pushable);
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
    /// <param name="tableName">The table to stream.</param>
    /// <param name="entry">Catalog entry for the table.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="wantedColumns">Optional bitmap selecting columns to decode.</param>
    /// <param name="factory">Delegate that maps decoded row values to <typeparamref name="T"/>.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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
    /// Shared typed-row enumerator used by <see cref="Rows(string, IProgress{long}?, CancellationToken)"/>.
    /// Walks every owned data page for <paramref name="entry"/>, emitting per-row
    /// <c>object?[]</c> buffers with complex-attachment and Hyperlink
    /// post-processing applied (gated by the per-table flags). Centralising
    /// the page scan here keeps the entry point on a single iterator
    /// (one C# async state machine instead of two).
    /// When <paramref name="wantedColumns"/> is non-<see langword="null"/>, only the
    /// flagged column indices are decoded and the complex-attachment / Hyperlink
    /// post-processing passes are skipped when no wanted column is affected by them.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="entry">Catalog entry for the table.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="wantedColumns">Optional bitmap selecting columns to decode.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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
    /// <param name="entry">Catalog entry for the table.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="directDecoder">Compiled direct-row decoder.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async IAsyncEnumerable<T> EnumerateDirectRowsAsync<T>(
        CatalogEntry entry,
        TableDef td,
        DirectRowDecoder<T> directDecoder,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class, new()
    {
        long rowCount = 0;
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns: null, this.strictParsing);

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
                if (!decodePlan.TryDecodeDirect(this, scanPage.Page, rb.RowStart, rb.RowSize, directDecoder, target))
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
            int pageIndex = 0;
            if (this.PageReadOptimizationMode == PageReadOptimizationMode.Auto)
            {
                yield return await this.ReadTableScanPageAsync(pageNumbers[pageIndex], cancellationToken).ConfigureAwait(false);
                pageIndex++;
            }

            for (; pageIndex < pageNumbers.Count; pageIndex++)
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
    /// Auto mode stays conservative: only file-backed, non-transactional scans
    /// with enough table pages use read-ahead, and the first page is yielded
    /// before prefetch begins to preserve first-row latency.
    /// </summary>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="pageNumbers">The list of page numbers for the table.</param>
    /// <returns><c>true</c> if table pages should be read ahead; otherwise, <c>false</c>.</returns>
    private bool ShouldReadAheadTablePages(TableDef tableDef, IReadOnlyList<long> pageNumbers) =>
        this.pageCache is not null
            && this.PageCacheSize >= MinimumTableScanReadAheadCacheSlots
            && this.ActiveJournal is null
            && !HasCacheReentrantScanColumns(tableDef)
            && this.HasEligibleTableScanReadAheadPageCount(pageNumbers);

    private bool HasEligibleTableScanReadAheadPageCount(IReadOnlyList<long> pageNumbers) =>
        this.PageReadOptimizationMode switch
        {
            PageReadOptimizationMode.Auto => this.DatabaseStream is FileStream && pageNumbers.Count >= MinimumAutoTableScanReadAheadPages,
            PageReadOptimizationMode.Disabled => false,
            PageReadOptimizationMode.Enabled => pageNumbers.Count > 1,
            _ => false,
        };

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

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (string[] row in this.EnumerateLinkedRowsAsStringsAsync(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        long rowCount = 0;
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateStrings(td, this.strictParsing);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            await foreach (string[] row in this.EnumerateRowsAsync(scanPage.PageNumber, scanPage.Page, decodePlan, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            IReadOnlyList<ColumnMetadata>? linkedMetadata = await this.TryReadLinkedTableAsync(
                tableName,
                link => LinkedTableManager.GetLinkedTextColumnMetadataAsync(this, link, cancellationToken),
                (source, link) => source.GetColumnMetadataAsync(link.SourceObjectName, cancellationToken),
                cancellationToken).ConfigureAwait(false);
            return linkedMetadata ?? [];
        }

        Dictionary<string, string> complexSubtypes = new(StringComparer.OrdinalIgnoreCase);
        bool hasComplex = resolved.Definition.Columns.Any(c => c.Type is ComplexType or AttachmentType);
        if (hasComplex)
        {
            complexSubtypes = await this.complexColumns.ReadColumnSubtypesAsync(tableName, cancellationToken).ConfigureAwait(false);
        }

        ColumnPropertyBlock? properties = await this.ReadLvPropForTableAsync(
            resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);

        return resolved.Definition.Columns.Select((col, index) =>
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
                ClrType = ResolveClrType(col),
                MaxLength = GetMetadataMaxLength(col),
                IsNullable = ResolveIsNullable(col, target),
                IsFixedLength = col.IsFixed,
                IsHyperlink = IsHyperlinkColumn(col),
                Ordinal = index,
                Size = GetColumnSize(ResolveValueType(col), GetMetadataDeclaredSize(col)),
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
            && (rt.DataType == ByteType
                || rt.DataType == IntegerType
                || rt.DataType == LongIntegerType)
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
    /// <param name="target">Column property metadata read from <c>MSysObjects.LvProp</c>.</param>
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

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        byte[]? td = await this.ReadTDefBytesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null || td.Length < this.TDef.BlockEnd)
        {
            return [];
        }

        return IndexCatalogReader.ReadMetadata(this, td, resolved.Definition.Columns);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<RelationshipMetadata>> ListRelationshipsAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();

        // MSysRelationships is a system table; ReadTableAsync resolves it through
        // the catalog fallback and returns an empty table when it is absent (Jet3
        // or slim-catalog files). The operation gate is reentrant, so the nested
        // ReadTableAsync call joins this root operation rather than blocking.
        DataTable table = await this.ReadTableAsync(Constants.SystemTableNames.Relationships, cancellationToken: cancellationToken).ConfigureAwait(false);
        try
        {
            return RelationshipMetadataAggregator.Aggregate(table);
        }
        finally
        {
            table.Dispose();
        }
    }

    /// <inheritdoc/>
    public IAccessIndexQuery<object[]> FromIndex(string tableName, string indexName)
        => new AccessObjectIndexQuery(this, tableName, indexName);

    /// <inheritdoc/>
    public IAccessIndexQuery<T> FromIndex<T>(string tableName, string indexName)
        where T : class, new()
            => new AccessTypedIndexQuery<T>(this, tableName, indexName);

    /// <inheritdoc/>
    public IQueryable<T> Query<T>(string tableName)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        var provider = new AccessQueryProvider<T>(this, tableName);
        return new AccessQueryable<T>(provider);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<object[]> SeekRowsAsync(
        string tableName,
        string indexName,
        IReadOnlyList<object?> keyValues,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (object[] row in this.ReadIndexRowsAsObjectsAsync(
                tableName,
                indexName,
                IndexQueryCriteria.Exact(keyValues),
                cancellationToken).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    internal IAsyncEnumerable<object[]> ReadIndexRowsAsObjectsAsync(
        string tableName,
        string indexName,
        IndexQueryCriteria criteria,
        CancellationToken cancellationToken = default) =>
        this.EnumerateIndexRowsAsync<object[]>(
            tableName,
            indexName,
            criteria,
            static _ => (static row => (object[])row, null),
            cancellationToken);

    internal IAsyncEnumerable<T> ReadIndexRowsAsync<T>(
        string tableName,
        string indexName,
        IndexQueryCriteria criteria,
        CancellationToken cancellationToken = default)
        where T : class, new() =>
        this.EnumerateIndexRowsAsync(
            tableName,
            indexName,
            criteria,
            static td =>
            {
                string[] headers = new string[td.Columns.Count];
                for (int i = 0; i < td.Columns.Count; i++)
                {
                    headers[i] = td.Columns[i].Name;
                }

                Func<object?[], T> factory = RowMapper<T>.Build(headers, td.ClrTypes);
                bool[]? wantedColumns = td.HasComplexColumns
                    ? null
                    : RowMapper<T>.GetBoundColumnMask(headers);

                return (factory, wantedColumns);
            },
            cancellationToken);

    private async IAsyncEnumerable<TRow> EnumerateIndexRowsAsync<TRow>(
        string tableName,
        string indexName,
        IndexQueryCriteria criteria,
        Func<TableDef, (Func<object?[], TRow> Factory, bool[]? WantedColumns)> createProjection,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(indexName, nameof(indexName));
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(createProjection, nameof(createProjection));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            yield break;
        }

        if (this.Format == DatabaseFormat.Jet3Mdb)
        {
            throw new NotSupportedException("Index seeks are currently supported for Jet4/ACE databases only.");
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        byte[]? tdefBytes = await this.ReadTDefBytesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (tdefBytes == null || tdefBytes.Length < this.TDef.BlockEnd)
        {
            yield break;
        }

        List<IndexMetadata> indexes = IndexCatalogReader.ReadMetadata(this, tdefBytes, td.Columns);

        IndexMetadata? index = indexes.Find(i => string.Equals(i.Name, indexName, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Index '{indexName}' was not found on table '{tableName}'.", nameof(indexName));

        if (index.FirstDp <= 0 || index.Columns.Count == 0)
        {
            yield break;
        }

        var cursor = new IndexCursor(
            this.ReadPageCachedAsync,
            this.PageSizeBytes);
        List<(long DataPage, int RowIndex)> hits = await cursor.FindRowLocationsForCriteriaAsync(
            this.Format,
            tableName,
            index,
            td,
            criteria,
            cancellationToken).ConfigureAwait(false);

        (Func<object?[], TRow> factory, bool[]? wantedColumns) = createProjection(td);

        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType, AttachmentType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));
        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await this.complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, this.strictParsing);

        foreach ((long dataPage, int rowIndex) in hits)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object?[]? row = await this.MaterializeSeekRowAsync(
                entry.TDefPage,
                td,
                dataPage,
                rowIndex,
                decodePlan,
                complexData,
                needsComplexPass,
                needsHyperlinkPass,
                cancellationToken).ConfigureAwait(false);
            if (row == null)
            {
                continue;
            }

            yield return factory(row);
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
    public async ValueTask<IReadOnlyList<MultiValueItem>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
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

    private async ValueTask<object?[]?> MaterializeSeekRowAsync(
        long expectedTDefPage,
        TableDef td,
        long dataPage,
        int rowIndex,
        RowDecodePlan decodePlan,
        Dictionary<int, Dictionary<int, byte[]>>? complexData,
        bool needsComplexPass,
        bool needsHyperlinkPass,
        CancellationToken cancellationToken)
    {
        byte[] page = await this.ReadPageCachedAsync(dataPage, cancellationToken).ConfigureAwait(false);
        if (page[0] != Constants.PageTypes.Data || Ri32(page, this.DataPage.TDefOff) != expectedTDefPage)
        {
            return null;
        }

        if (!this.TryFindLiveRowBound(page, dataPage, rowIndex, out RowBound rowBound) || rowBound.RowSize < this.RowFields.NumCols)
        {
            return null;
        }

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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A list of user table names.</returns>
    public async ValueTask<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return tables.ConvertAll(e => e.Name);
    }

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional progress reporter - receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns.</returns>
    public ValueTask<DataTable> ReadTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.ReadDataTableCoreAsync(tableName, maxRows, progress, preserveComplexReferences: false, cancellationToken);

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// This is a compatibility alias for <see cref="ReadTableAsync(string?, uint?, IProgress{long}?, CancellationToken)"/>.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional progress reporter - receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns.</returns>
    public ValueTask<DataTable> ReadDataTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.ReadTableAsync(tableName, maxRows, progress, cancellationToken);

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

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
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

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        DataTable? dt = null;
        bool dataLoadStarted = false;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                Type clrType = preserveComplexReferences && (col.Type == ComplexType || col.Type == AttachmentType)
                    ? typeof(object)
                    : ResolveClrType(col);
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
            link => LinkedTableManager.RowsLinkedTextMappedAsync(
                this,
                link,
                progress,
                static metadata => RowMapper<T>.Build(metadata),
                cancellationToken),
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
    public async ValueTask<IReadOnlyList<T>> ReadTableAsync<T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
        where T : class, new()
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved != null)
        {
            List<string> resolvedHeaders = resolved.Definition.Columns.ConvertAll(column => column.Name);
            var projectedColumns = new List<ColumnInfo>(resolvedHeaders.Count);
            RowMapper<T>.Accessor?[] fullIndex = RowMapper<T>.BuildIndex(resolvedHeaders);

            for (int i = 0; i < resolvedHeaders.Count; i++)
            {
                if (fullIndex[i] != null)
                {
                    projectedColumns.Add(resolved.Definition.Columns[i]);
                }
            }

            bool canUseDirectMap = projectedColumns.TrueForAll(static column => column.Type is not ComplexType and not AttachmentType);

            if (canUseDirectMap && projectedColumns.Count == resolvedHeaders.Count)
            {
                Func<object?[], T> fullFactory = RowMapper<T>.Build(resolved.Definition);
                return await this.ReadMappedTableAsync(
                    resolved.Entry.TDefPage,
                    resolved.Definition,
                    fullFactory,
                    maxRows,
                    cancellationToken).ConfigureAwait(false);
            }

            bool canProject = canUseDirectMap && projectedColumns.Count < resolvedHeaders.Count;

            if (canProject)
            {
                return await this.ReadProjectedTableAsync<T>(
                    resolved.Entry.TDefPage,
                    resolved.Definition,
                    projectedColumns,
                    maxRows,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        IReadOnlyList<T>? linkedRows = await this.TryReadLinkedTableAsync(
            tableName,
            link => LinkedTableManager.ReadLinkedTextMappedRowsAsync(
                this,
                link,
                maxRows,
                static metadata => RowMapper<T>.Build(metadata),
                cancellationToken),
            (source, link) => source.ReadTableAsync<T>(link.SourceObjectName, maxRows, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        return linkedRows ?? [];
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
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns: null, this.strictParsing);
        bool needsHyperlinkPass = td.HasHyperlinkColumns;
        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
            {
                cancellationToken.ThrowIfCancellationRequested();

                object?[]? row = await this.CrackRowTypedAsync(scanPage.Page, rb.RowStart, rb.RowSize, decodePlan, cancellationToken).ConfigureAwait(false);
                if (row == null)
                {
                    continue;
                }

                if (needsHyperlinkPass)
                {
                    WrapHyperlinkColumns(row, td.ClrTypes);
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
        List<ColumnInfo> projectedColumns,
        uint? maxRows,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        string[] headers = new string[projectedColumns.Count];
        var projectedSourceTypes = new Type[projectedColumns.Count];
        for (int i = 0; i < projectedColumns.Count; i++)
        {
            ColumnInfo column = projectedColumns[i];
            headers[i] = column.Name;
            projectedSourceTypes[i] = ResolveClrType(column);
        }

        Func<object?[], T> factory = RowMapper<T>.Build(headers, projectedSourceTypes);
        var items = new List<T>();
        bool[] wantedColumns = new bool[td.Columns.Count];
        int[] projectedOrdinals = new int[projectedColumns.Count];
        for (int i = 0; i < projectedColumns.Count; i++)
        {
            int ordinal = td.Columns.IndexOf(projectedColumns[i]);
            if (ordinal < 0)
            {
                return items;
            }

            projectedOrdinals[i] = ordinal;
            wantedColumns[ordinal] = true;
        }

        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, this.strictParsing);
        bool needsHyperlinkPass = td.HasHyperlinkColumns && HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns);
        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound rb in this.GetLiveRowBoundsCached(scanPage.PageNumber, scanPage.Page))
            {
                cancellationToken.ThrowIfCancellationRequested();

                object?[]? row = await this.CrackRowTypedAsync(scanPage.Page, rb.RowStart, rb.RowSize, decodePlan, cancellationToken).ConfigureAwait(false);
                if (row == null)
                {
                    continue;
                }

                if (needsHyperlinkPass)
                {
                    WrapHyperlinkColumns(row, td.ClrTypes);
                }

                object?[] projectedRow = new object?[projectedOrdinals.Length];
                for (int i = 0; i < projectedOrdinals.Length; i++)
                {
                    projectedRow[i] = row[projectedOrdinals[i]];
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

        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
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

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        DataTable? dt = null;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                _ = dt.Columns.Add(col.Name, typeof(string));
            }

            IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            var decodePlan = RowDecodePlan.CreateStrings(td, this.strictParsing);

            foreach (long pageNumber in pageNumbers)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await this.ReadPageCachedAsync(pageNumber, cancellationToken).ConfigureAwait(false);

                await foreach (string[] row in this.EnumerateRowsAsync(pageNumber, page, decodePlan, cancellationToken).ConfigureAwait(false))
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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
    public async ValueTask<IReadOnlyDictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default)
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
            result[table.Name] = await this.ReadTableAsync(table.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
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
        FileOptions accessPattern = CanUseRandomAccessPageReads(options.PageReadOptimizationMode) ? FileOptions.RandomAccess : FileOptions.SequentialScan;
        return OpenDatabaseFileStream(path, options.FileAccess, options.FileShare, FileOptions.Asynchronous | accessPattern);
    }

    private static string ResolveTypeName(ColumnInfo col) =>
        IsHyperlinkColumn(col) ? "Hyperlink" : GetTypeDisplayName(ResolveValueType(col));

    /// <summary>
    /// Wraps text payloads of Hyperlink-flagged columns in a typed row into
    /// <see cref="Hyperlink"/> instances, mirroring the projection
    /// <see cref="ResolveClrType"/> exposes via the public API.
    /// Non-string slots (e.g. <see cref="DBNull.Value"/>) are left untouched;
    /// strings that fail to parse collapse to <see cref="DBNull.Value"/>
    /// (matching <see cref="TypedValueParser.ParseValue"/>'s legacy behaviour).
    /// </summary>
    /// <param name="columns">The columns.</param>
    /// <param name="wantedColumns">Optional bitmap selecting columns to decode.</param>
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

    private static bool CanUseRandomAccessPageReads(PageReadOptimizationMode mode) =>
        mode != PageReadOptimizationMode.Disabled;

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

    private async ValueTask DisposeReaderResourcesAsync()
    {
        this.DisposePageCaches();
        this.InvalidateCatalogCache();
        await base.DisposeAsync().ConfigureAwait(false);
    }

    private void DisposeReaderConstructionResources()
    {
        this.DisposePageCaches();
        this.lockFile.Dispose();
        this.DisposeBaseManagedResources();
    }

    private void DisposePageCaches()
    {
        this.pageCache?.Clear();
        this.pageCache?.Dispose();
        this.rowBoundsCache?.Clear();
        this.rowBoundsCache?.Dispose();
    }

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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
            StringBuilder diag = new StringBuilder()
                .Append("JET: ")
                .Append(this.Format == DatabaseFormat.Jet3Mdb ? "Jet3" : "Jet4/ACE")
                .Append("  PageSize: ")
                .Append(this.PageSizeBytes)
                .Append("  TotalPages: ")
                .Append(this.DatabaseStream.Length / this.PageSizeBytes)
                .AppendLine()
                .Append("MSysObjects cols (")
                .Append(msys.Columns.Count)
                .Append("): ")
                .AppendJoin(", ", msys.Columns.Select(static c => $"{c.Name}[{GetTypeDisplayName(c.Type)}]"))
                .AppendLine()
                .Append("Catalog pages: ")
                .Append(catPages)
                .Append("  Total rows scanned: ")
                .Append(allRows)
                .Append("  User tables: ")
                .Append(result.Count)
                .AppendLine();

            foreach (CatalogEntry e in result)
            {
                _ = diag.Append("  [")
                    .Append(e.Name)
                    .Append("] TDEF page ")
                    .Append(e.TDefPage)
                    .AppendLine();
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
        byte[] magic = new byte[4];
        int read = this.DatabaseStream.Read(magic, 0, 4);
        if (read < 4 || magic[0] != 0x00 || magic[1] != 0x01 || magic[2] != 0x00 || magic[3] != 0x00)
        {
            string msg = $"File does not have a valid JET magic signature (expected 00 01 00 00, got {magic[0]:X2} {magic[1]:X2} {magic[2]:X2} {magic[3]:X2}).";
            throw new InvalidDataException(msg);
        }
    }

    /// <summary>Reads a page through the cache when one is configured (PageCacheSize &gt; 0) and no transaction journal is active.</summary>
    /// <param name="n">The item count.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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

    /// <summary>
    /// Returns the declared row count from <paramref name="tableName"/>'s TDEF header
    /// (a cheap lookup with no row scan), or 0 when the table cannot be resolved. Used
    /// as a cost estimate when choosing between per-key index seeks and a single scan.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The declared row count, or 0 when unknown.</returns>
    internal async ValueTask<long> GetDeclaredRowCountAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = this.EnterOperation();
        cancellationToken.ThrowIfCancellationRequested();
        ResolvedTable? resolved = await this.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        return resolved?.Definition.RowCount ?? 0;
    }

    internal async ValueTask<ResolvedTable?> ResolveTableAsync(string tableName, CancellationToken cancellationToken)
    {
        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        CatalogEntry? entry = tables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
        if (entry != null)
        {
            TableDef? td = await this.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            if (td?.Columns.Count > 0)
            {
                await this.HydrateCalculatedResultTypesAsync(entry.TDefPage, td, cancellationToken).ConfigureAwait(false);
                return new ResolvedTable(entry, td);
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
                return new ResolvedTable(new CatalogEntry(tableName, sysPage), sysTd);
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
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            if (!col.IsCalculated)
            {
                continue;
            }

            ColumnType resultType = ResolveCalculatedResultType(properties.FindTarget(col.Name));
            if (resultType != default && resultType != col.CalculatedResultType)
            {
                tableDef.Columns[i] = col.WithCalculatedResultType(resultType);
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
    private IAsyncEnumerable<string[]> EnumerateRowsAsync(long pageNumber, byte[] page, TableDef td, CancellationToken cancellationToken)
    {
        var decodePlan = RowDecodePlan.CreateStrings(td, this.strictParsing);
        return this.EnumerateRowsAsync(pageNumber, page, decodePlan, cancellationToken);
    }

    private async IAsyncEnumerable<string[]> EnumerateRowsAsync(long pageNumber, byte[] page, RowDecodePlan decodePlan, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (RowBound rb in this.GetLiveRowBoundsCached(pageNumber, page))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (rb.RowSize < this.RowFields.NumCols)
            {
                continue;
            }

            string[]? values = await decodePlan.TryDecodeStringRowAsync(this, page, rb.RowStart, rb.RowSize, this.longValueDecoder, cancellationToken).ConfigureAwait(false);
            if (values != null)
            {
                yield return values;
            }
        }
    }

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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> ResolveLongValueRefsIntoBufferAsync(object?[] buffer, int validLength, byte[] page, CancellationToken cancellationToken)
    {
        for (int i = 0; i < validLength; i++)
        {
            if (buffer[i] is LongValueRef lvr)
            {
                buffer[i] = lvr.IsOle
                    ? await this.longValueDecoder.ReadOleValueBytesAsync(page, lvr.Start, lvr.Len, cancellationToken).ConfigureAwait(false)
                    : await this.longValueDecoder.ReadLongValueAsync(page, lvr.Start, lvr.Len, isOle: false, cancellationToken).ConfigureAwait(false);
            }
            else if (buffer[i] is CalculatedLongValueRef clvr)
            {
                buffer[i] = await this.ResolveCalculatedLongValueRefAsync(page, clvr, cancellationToken).ConfigureAwait(false);
            }
        }

        return true;
    }

    /// <summary>
    /// Async slow-path that walks the LVAL chain for any
    /// <see cref="LongValueRef"/> sentinels left in <paramref name="row"/>
    /// by <c>TryCrackRowSync</c>. Only invoked when at least one
    /// such sentinel was emitted — fixed-only / inline-only rows skip this
    /// entirely and never allocate an async state machine.
    /// </summary>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<object?[]?> ResolveLongValueRefsAsync(object?[] row, byte[] page, CancellationToken cancellationToken)
    {
        _ = await this.ResolveLongValueRefsIntoBufferAsync(row, row.Length, page, cancellationToken).ConfigureAwait(false);
        return row;
    }

    private async ValueTask<object> ResolveCalculatedLongValueRefAsync(byte[] page, CalculatedLongValueRef reference, CancellationToken cancellationToken)
    {
        byte[] raw = await this.longValueDecoder.ReadLongValueRawBytesAsync(page, reference.Start, reference.Len, cancellationToken).ConfigureAwait(false);
        byte[] payload = CalculatedColumnUtil.Unwrap(raw);
        return reference.IsOle
            ? OleObjectDecoder.DecodeOleValueBytes(payload, 0, payload.Length)
            : this.longValueDecoder.DecodeLongValue(payload, 0, payload.Length, isOle: false);
    }

    /// <summary>
    /// Synchronously decodes a row into a typed <c>object?[]</c>. Returns
    /// <see langword="false"/> when the row trailer is malformed or the
    /// schema sanity-check rejects the row (caller should skip).
    /// <paramref name="needsLongValue"/> is set when one or more
    /// <c>Memo</c>/<c>Ole</c> slots require an LVAL-chain walk; those
    /// slots are filled with a <see cref="LongValueRef"/> sentinel that the
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
        object?[] result = new object?[decodePlan.ColumnCount];
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
    // the box/unbox round-trip on every primitive column. RowDecodePlan
    // still owns row-layout parsing and column-slice resolution; the
    // compiled delegate only assigns directly decodable slices to T's
    // properties.

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

    private readonly record struct TableScanPage(long PageNumber, byte[] Page);

    /// <summary>
    /// Yields rows from every data page whose owning TDEF page equals <paramref name="tdefPage"/>.
    /// Centralises the common scan-all-pages-and-decode-rows pattern used by catalog/system-table readers.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async IAsyncEnumerable<string[]> EnumerateRowsForTdefAsync(
        long tdefPage,
        TableDef td,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IReadOnlyList<long> pageNumbers = await this.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateStrings(td, this.strictParsing);
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageCachedAsync(pageNumber, cancellationToken).ConfigureAwait(false);

            await foreach (string[] row in this.EnumerateRowsAsync(pageNumber, page, decodePlan, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }
        }
    }

    /// <summary>Loads the MSysObjects TableDef (page 2). Exposed for <see cref="LinkedTableManager"/>.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<TableDef?> GetMSysObjectsTableDefAsync(CancellationToken cancellationToken) =>
        this.ReadTableDefAsync(2, cancellationToken);

    /// <summary>Enumerates every row of MSysObjects. Exposed for <see cref="LinkedTableManager"/>.</summary>
    /// <param name="msys">The system-table data.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<byte[]?> GetRawTDefBytesAsync(long tdefPage, CancellationToken cancellationToken) =>
        this.ReadTDefBytesAsync(tdefPage, cancellationToken);

    /// <summary>
    /// Returns a heap-allocated copy of the raw bytes of <paramref name="pageNumber"/>
    /// (post-decryption). Diagnostic-only helper for the format-probe tool under
    /// <c>JetDatabaseWriter.FormatProbe</c>; production code should not call this.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<byte[]> GetRawPageBytesAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] pooled = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        byte[] copy = new byte[this.PageSizeBytes];
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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

            byte[]? blob = BinaryStringParser.TryDecodeBase64DataUri(
                CatalogValueReader.GetStringOrEmpty(row, idxLvProp),
                "application/octet-stream",
                out byte[] bytes)
                ? bytes
                : null;
            return ColumnPropertyBlock.Parse(blob, this.Format);
        }

        return null;
    }

    /// <summary>
    /// Finds the TDEF page number for a system table by name (case-insensitive).
    /// Unlike GetUserTables, this includes system tables (SYSTABLE_MASK set).
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<long> FindSystemTablePageAsync(string name, CancellationToken cancellationToken) =>
        this.FindSystemTablePageAsync(
            n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase),
            cancellationToken);

    /// <summary>
    /// Finds the TDEF page for the first system table whose name satisfies <paramref name="nameMatches"/>.
    /// Shared by exact-name and suffix lookups against MSysObjects.
    /// </summary>
    /// <param name="nameMatches">The name matches.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
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
