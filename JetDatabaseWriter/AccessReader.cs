namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Transactions;

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
///   <item><description>Overflow rows (rows Access moved to another slot when they grew) — read through their pointer; one that cannot be resolved is skipped like an undecodable row.</description></item>
/// </list>
/// <para>
/// Based on the <see href="https://github.com/mdbtools/mdbtools/blob/master/HACKING.md">mdbtools format specification</see>.
/// </para>
/// </summary>
public sealed class AccessReader : AccessBase, IAccessReader
{
    private readonly LockFileCoordinator lockFile;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in DisposeReaderResourcesAsync after the database closes; failed open disposes the lease in OpenAsync.")]
    private readonly FileStream? snapshotLease;

    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in DisposeReaderResourcesAsync, passed as a step to LockFileCoordinator.DisposeAfterAsync; failed construction disposes it in the constructor.")]
    private readonly ReaderServices services;

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReader"/> class.
    /// Opens <paramref name="path"/> and detects the JET version.
    /// </summary>
    /// <param name="path">The path to the Access database file. May be empty when opened from a stream.</param>
    /// <param name="options">Options for configuring the AccessReader.</param>
    /// <param name="stream">An open, seekable stream for the database file.</param>
    /// <param name="header">Header bytes read from page 0.</param>
    /// <param name="snapshotLease">The read-only file handle that excludes writers during the reader lifetime.</param>
    /// <param name="leaveOpen">Whether the caller retains ownership of the stream. If false, the stream is disposed when the reader is disposed.</param>
    /// <param name="cancellationToken">Cancels password derivation during open.</param>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "AccessBase takes ownership of the database file; DisposeAsync disposes it after the reader's services.")]
    private AccessReader(
        string path,
        AccessReaderOptions options,
        Stream stream,
        byte[] header,
        FileStream? snapshotLease,
        bool leaveOpen = false,
        CancellationToken cancellationToken = default)
        : base(DatabaseFile.ForReader(stream, header, options.Password, path, leaveOpen, options, cancellationToken))
    {
        Guard.NotNull(options, nameof(options));
        this.snapshotLease = snapshotLease;

        this.lockFile = LockFileCoordinator.ForReader(path, options);
        this.DiagnosticsEnabled = options.DiagnosticsEnabled;
        this.PageCacheSize = options.PageCacheSize;
        this.PageReadOptimizationMode = options.PageReadOptimizationMode;

        ReaderServices? services = null;
        bool constructionComplete = false;
        try
        {
            services = new ReaderServices(this.Database, options);
            this.services = services;

            if (options.ValidateOnOpen)
            {
                this.ValidateDatabaseFormat();
            }

            // OpenAsync's catch owns only the stream and never sees this
            // half-built reader, so failed construction after slot acquisition
            // must release the lock-file slot here.
            this.lockFile.Acquire();

            // The reader never writes, so it takes no byte-range locks; the
            // timeout is still checked here, where the lock was once built.
            _ = AccessOptions.ValidateLockTimeoutMilliseconds(options.LockTimeoutMilliseconds);
            constructionComplete = true;
        }
        finally
        {
            if (!constructionComplete)
            {
                services?.Dispose();
                this.lockFile.Dispose();
                this.Database.DisposeManagedResources();
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
    public string LastDiagnostics => this.services.Catalog.FormatDiagnostics(this.DiagnosticsEnabled);

    /// <summary>
    /// Asynchronously opens a JET database file and returns a new <see cref="AccessReader"/> instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessReader"/> for the specified database.</returns>
    public static async ValueTask<AccessReader> OpenAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        options ??= new AccessReaderOptions();

        // CA2000: OpenAsync(stream, leaveOpen:false) intentionally takes ownership and disposes on all paths.
#pragma warning disable CA2000
        FileStream fs = CreateStream(path, options);
#pragma warning restore CA2000
        AccessReader reader = await OpenCoreAsync(fs, options, leaveOpen: false, acquireLease: false, cancellationToken).ConfigureAwait(false);
        if (options.UsesPositionalPageReads(openedFromPath: true))
        {
            reader.Database.Pages.EnableRandomAccessPageReadsIfSupported();
        }

        // CreateStream opened a synchronous handle, so a page read that starts
        // on a pool thread is cheaper done there than handed to another one.
        reader.Database.Pages.ReadsInlineOnThreadPool = true;
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
    /// <remarks>
    /// The reader reads every page through the stream, one seek and read at a time.
    /// On Windows, in our measurements, a <see cref="FileStream"/> opened without
    /// <see cref="FileOptions.Asynchronous"/> read pages about twice as fast as an
    /// overlapped one, because an overlapped read completes through the I/O
    /// completion port even when the page is already in the OS cache. The path
    /// overload opens its file that way.
    /// </remarks>
    /// <exception cref="NotSupportedException">Office compound packages are not native Microsoft Access database inputs.</exception>
    public static ValueTask<AccessReader> OpenAsync(Stream stream, AccessReaderOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
        => OpenCoreAsync(stream, options, leaveOpen, acquireLease: true, cancellationToken);

    private static async ValueTask<AccessReader> OpenCoreAsync(Stream stream, AccessReaderOptions? options, bool leaveOpen, bool acquireLease, CancellationToken cancellationToken)
    {
        Guard.RequireReadableSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        FileStream? snapshotLease = null;
        try
        {
            options = (options ?? new AccessReaderOptions()).WithEncryptionWorkBudget();
            string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
            if (stream is FileStream recoveryFile)
            {
                if (acquireLease)
                {
                    snapshotLease = new FileStream(recoveryFile.Name, FileMode.Open, FileAccess.Read, FileShare.Read);
                }

                PersistentRollbackJournal.RejectHotJournal(recoveryFile);
            }

            byte[] header = await EncryptionManager.ReadOpenHeaderPageAsync(stream, cancellationToken).ConfigureAwait(false);

            return new AccessReader(path, options, stream, header, snapshotLease, leaveOpen, cancellationToken);
        }
        catch
        {
            if (snapshotLease is not null)
            {
                await snapshotLease.DisposeAsync().ConfigureAwait(false);
            }

            if (!leaveOpen)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public ValueTask<DataTable> ReadFirstTableAsStringsAsync(uint? maxRows = null, CancellationToken cancellationToken = default)
        => this.services.Tables.ReadFirstTableAsStringsAsync(maxRows, cancellationToken);

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A list of user table names.</returns>
    public ValueTask<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default)
        => this.services.Schema.ListTablesAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<bool> TryLookupTableAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Tables.TryLookupTableAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default)
        => this.services.Schema.ListLinkedTablesAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default)
        => this.services.Schema.GetTableStatsAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken = default)
        => this.services.Schema.GetTablesAsDataTableAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Tables.GetRealRowCountAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<object[]> Rows(
        string tableName,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        => this.services.Tables.Rows(tableName, progress, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<T> Rows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
        => this.services.Tables.Rows<T>(tableName, progress, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<T> Rows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        Expression<Func<T, bool>> predicate,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
        => this.services.Indexes.Rows(tableName, predicate, progress, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<string[]> RowsAsStrings(
        string tableName,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        => this.services.Tables.RowsAsStrings(tableName, progress, cancellationToken);

    /// <summary>Reads a table's persisted validation expression and message.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The table rule, or null when no rule is stored.</returns>
    public ValueTask<TableValidationRule?> GetTableValidationRuleAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Schema.GetTableValidationRuleAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Schema.GetColumnMetadataAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Indexes.ListIndexesAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<RelationshipMetadata>> ListRelationshipsAsync(CancellationToken cancellationToken = default)
        => this.services.Schema.ListRelationshipsAsync(cancellationToken);

    /// <inheritdoc/>
    public IAccessIndexQuery<object[]> FromIndex(string tableName, string indexName)
        => new AccessObjectIndexQuery(this.services.Indexes, tableName, indexName);

    /// <inheritdoc/>
    public IAccessIndexQuery<T> FromIndex<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, string indexName)
        where T : class, new()
            => new AccessTypedIndexQuery<T>(this.services.Indexes, tableName, indexName);

    /// <inheritdoc/>
    [RequiresUnreferencedCode("LINQ queries and Include discover entity types and members at runtime. Use typed row readers for trimmed applications.")]
    [RequiresDynamicCode("LINQ queries and Include construct generic types at runtime. Use typed row readers for NativeAOT applications.")]
    public IQueryable<T> Query<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        var provider = new AccessQueryProvider<T>(this.services.Tables, this.services.Indexes, this.services.Schema, tableName);
        return new AccessQueryable<T>(provider);
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<object[]> SeekRowsAsync(
        string tableName,
        string indexName,
        IReadOnlyList<object?> keyValues,
        CancellationToken cancellationToken = default)
        => this.services.Indexes.SeekRowsAsync(tableName, indexName, keyValues, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Schema.GetComplexColumnsAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
        => this.services.ComplexItems.GetAttachmentsAsync(tableName, columnName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<MultiValueItem>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
        => this.services.ComplexItems.GetMultiValueItemsAsync(tableName, columnName, cancellationToken);

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
        => this.services.Tables.ReadTableAsync(tableName, maxRows, progress, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<T>> ReadTableAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
        where T : class, new()
        => this.services.Tables.ReadTableAsync<T>(tableName, maxRows, cancellationToken);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows as a string-typed <see cref="DataTable"/> asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read, or <c>null</c> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> with all columns typed as <see cref="string"/>.</returns>
    public ValueTask<DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.services.Tables.ReadTableAsStringsAsync(tableName, maxRows, progress, cancellationToken);

    /// <summary>
    /// Returns statistical information about the database asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="DatabaseStatistics"/> object containing various metrics about the database.</returns>
    public ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default)
        => this.services.Schema.GetStatisticsAsync(cancellationToken);

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns asynchronously.
    /// Each table's columns use their native CLR types (int, DateTimeType, decimal, etc.).
    /// </summary>
    /// <param name="progress">Optional progress reporter for table read operations.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A dictionary mapping table names to their corresponding DataTables.</returns>
    public ValueTask<IReadOnlyDictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default)
        => this.services.Tables.ReadAllTablesAsync(progress, cancellationToken);

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        AsyncReentrantOperationGate operations = this.services.Operations;
        if (!operations.TryBeginDispose(out Task? waitForOperations))
        {
            await operations.DisposeCompleted.ConfigureAwait(false);
            return;
        }

        try
        {
            // The coordinator drains every step in order, aggregates failures,
            // then unconditionally releases the .ldb / .laccdb slot.
            await this.lockFile.DisposeAfterAsync(
                waitForOperations,
                this.DisposeReaderResourcesAsync).ConfigureAwait(false);
            operations.CompleteDispose();
        }
        catch (Exception ex)
        {
            operations.CompleteDispose(ex);
            throw;
        }
    }

    /// <summary>
    /// Opens a path-opened reader's file with a synchronous handle and no
    /// access-pattern hint, in every <see cref="Enums.PageReadOptimizationMode"/>.
    /// </summary>
    /// <remarks>
    /// Measured on Windows (Arm64, NVMe, .NET 10 and 8): an overlapped
    /// (<see cref="FileOptions.Asynchronous"/>) read completes through the I/O
    /// completion port and a thread-pool callback even when the OS cache holds
    /// the page, so a cached 4 KB page read cost 14-19 µs, against 3-5 µs as a
    /// synchronous read on a pool thread. Long-value scans read about one page
    /// per row and ran 1.6-2.2 times faster. On files the OS had not cached,
    /// the <see cref="FileOptions.RandomAccess"/> and
    /// <see cref="FileOptions.SequentialScan"/> hints, and overlapped handles,
    /// defeated the OS read-ahead: 9,800 sequential page reads took 47 ms with
    /// no hint, 654 ms with <see cref="FileOptions.SequentialScan"/> and about
    /// 1 s on the old overlapped handle.
    /// </remarks>
    /// <param name="path">The database file.</param>
    /// <param name="options">The reader options, which supply the file access and sharing.</param>
    /// <returns>The opened stream.</returns>
    private static FileStream CreateStream(string path, AccessReaderOptions options) =>
        PageFile.OpenFileStream(path, options.FileAccess, options.FileShare & FileShare.Read, FileOptions.None);

    private async ValueTask DisposeReaderResourcesAsync()
    {
        this.services.Dispose();
        try
        {
            await this.Database.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            if (this.snapshotLease is not null)
            {
                await this.snapshotLease.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private void ValidateDatabaseFormat()
    {
        Stream stream = this.Database.Pages.Stream;
        if (stream.Length < 128)
        {
            throw new InvalidDataException("File too small to be a valid JET database");
        }

        // Verify the JET magic signature at offset 0: 00 01 00 00
        _ = stream.Seek(0, SeekOrigin.Begin);
        byte[] magic = new byte[4];
        int read = stream.Read(magic, 0, 4);
        if (read < 4 || magic[0] != 0x00 || magic[1] != 0x01 || magic[2] != 0x00 || magic[3] != 0x00)
        {
            string msg = $"File does not have a valid JET magic signature (expected 00 01 00 00, got {magic[0]:X2} {magic[1]:X2} {magic[2]:X2} {magic[3]:X2}).";
            throw new InvalidDataException(msg);
        }
    }
}
