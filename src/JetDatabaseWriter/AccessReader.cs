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
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Pure-managed reader for Microsoft Access Jet3, Jet4 and ACE databases (.mdb / .accdb).
/// No OleDB, ODBC, or ACE/Jet driver installation is required.
/// </summary>
/// <remarks>
/// <para>
/// Format generations, column types, collations and encryption providers have varying
/// coverage; recognition of a format does not establish support for every feature variant.
/// Pre-Jet3 databases and Office encrypted compound packages are unsupported.
/// </para>
/// <para>
/// Row APIs stream data or materialize it on request. Page caching and eligible page-scan
/// read-ahead are configurable; memory use also includes schema metadata, individual long
/// values and any results the caller retains. Resource budgets do not constitute a complete
/// hostile-input audit or a process-wide memory limit.
/// </para>
/// <para>
/// Access-file and supported delimited-text links require a caller-authorized source stream.
/// The reader uses that exact stream without opening metadata paths or filename-derived sidecars.
/// ODBC links expose metadata only. Database content is not executed as SQL, macros or VBA.
/// </para>
/// <para>
/// Keep the source unchanged throughout the reader lifetime; its caches do not refresh.
/// File sharing excludes cooperating writers, subject to platform enforcement. Custom streams
/// require caller-managed isolation. See <see cref="AccessReaderOptions.StrictParsing"/>
/// for malformed-value behavior; unreadable row layouts and unresolved overflow rows can
/// still be omitted by table scans.
/// </para>
/// </remarks>
public sealed class AccessReader : AccessBase, IAccessReader
{
    private readonly LockFileCoordinator lockFile;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed in DisposeReaderResourcesAsync after the database closes; failed open disposes the lease in OpenAsync.")]
    private readonly FileStream? snapshotLease;

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
    /// <param name="linkedSource">Whether the stream is a resolver capability whose path is only a hint.</param>
    /// <param name="cancellationToken">Cancels password derivation during open.</param>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "AccessBase takes ownership of the database file; DisposeAsync disposes it after the reader's services.")]
    private AccessReader(
        string path,
        AccessReaderOptions options,
        Stream stream,
        byte[] header,
        FileStream? snapshotLease,
        bool leaveOpen = false,
        bool linkedSource = false,
        CancellationToken cancellationToken = default)
        : base(DatabaseFile.ForReader(stream, header, options.Password, path, leaveOpen, options, cancellationToken))
    {
        Guard.NotNull(options, nameof(options));
        this.snapshotLease = snapshotLease;

        this.lockFile = LockFileCoordinator.ForReader(linkedSource ? string.Empty : path, options);
        this.DiagnosticsEnabled = options.DiagnosticsEnabled;
        this.PageCacheSize = options.PageCacheSize;
        this.PageReadOptimizationMode = options.PageReadOptimizationMode;

        ReaderServices? services = null;
        bool constructionComplete = false;
        try
        {
            services = new ReaderServices(this.Database, options);
            this.Services = services;

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
    public string LastDiagnostics => this.Services.Catalog.FormatDiagnostics(this.DiagnosticsEnabled);

    /// <summary>Gets the reader services for internal inspection.</summary>
    internal ReaderServices Services { get; }

    /// <summary>
    /// Asynchronously opens a JET database file and returns a new <see cref="AccessReader"/> instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessReader"/> for the specified database.</returns>
    public static async ValueTask<AccessReader> OpenAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
    {
        DiagnosticTrace.ThrowIfReentrant();
        cancellationToken.ThrowIfCancellationRequested();
        Guard.NotNullOrEmpty(path, nameof(path));
        EncryptionReplacementIntent.RejectPending(path);
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        options ??= new AccessReaderOptions();

        FileStream fs = CreateStream(path, options);
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
    /// <exception cref="JetNotSupportedException">Office compound packages are not native Microsoft Access database inputs.</exception>
    public static ValueTask<AccessReader> OpenAsync(Stream stream, AccessReaderOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
        => OpenCoreAsync(stream, options, leaveOpen, acquireLease: true, cancellationToken);

    /// <summary>Consumes an authorized linked source without filename-derived filesystem access.</summary>
    /// <param name="stream">The resolver-owned source stream.</param>
    /// <param name="sourceHint">The logical path used only to resolve nested catalog path hints.</param>
    /// <param name="options">The inherited reader options.</param>
    /// <param name="cancellationToken">Cancels the open operation.</param>
    /// <returns>The reader that owns the exact supplied stream.</returns>
    internal static ValueTask<AccessReader> OpenLinkedSourceAsync(Stream stream, string sourceHint, AccessReaderOptions options, CancellationToken cancellationToken)
        => OpenCoreAsync(stream, options, leaveOpen: false, acquireLease: false, cancellationToken, sourceHint);

    private static async ValueTask<AccessReader> OpenCoreAsync(Stream stream, AccessReaderOptions? options, bool leaveOpen, bool acquireLease, CancellationToken cancellationToken, string? linkedSourceHint = null)
    {
        DiagnosticTrace.ThrowIfReentrant();
        Guard.NotNull(stream, nameof(stream));

        FileStream? snapshotLease = null;
        try
        {
            Guard.RequireReadableSeekableStream(stream, nameof(stream));
            cancellationToken.ThrowIfCancellationRequested();
            options = (options ?? new AccessReaderOptions()).WithEncryptionWorkBudget();
            string path = linkedSourceHint ?? (stream is FileStream fileStream ? fileStream.Name : string.Empty);
            if (linkedSourceHint is null && stream is FileStream recoveryFile)
            {
                EncryptionReplacementIntent.RejectPending(recoveryFile.Name);
                if (acquireLease)
                {
                    snapshotLease = new FileStream(recoveryFile.Name, FileMode.Open, FileAccess.Read, FileShare.Read);
                }

                PersistentRollbackJournal.RejectHotJournal(recoveryFile);
            }

            byte[] header = await EncryptionManager.ReadOpenHeaderPageAsync(stream, cancellationToken).ConfigureAwait(false);

            return new AccessReader(path, options, stream, header, snapshotLease, leaveOpen, linkedSource: linkedSourceHint is not null, cancellationToken);
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
        => this.Services.Tables.ReadFirstTableAsStringsAsync(maxRows, cancellationToken);

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A list of user table names.</returns>
    public ValueTask<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default)
        => this.Services.Schema.ListTablesAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<bool> TryLookupTableAsync(string tableName, CancellationToken cancellationToken = default)
        => this.Services.Tables.TryLookupTableAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default)
        => this.Services.Schema.ListLinkedTablesAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default)
        => this.Services.Schema.GetTableStatsAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken = default)
        => this.Services.Schema.GetTablesAsDataTableAsync(cancellationToken);

    /// <inheritdoc/>
    public ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default)
        => this.Services.Tables.GetRealRowCountAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<object[]> Rows(
        string tableName,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        => this.Services.Tables.Rows(tableName, progress, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<T> Rows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
        => this.Services.Tables.Rows<T>(tableName, progress, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<T> Rows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        Expression<Func<T, bool>> predicate,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
        => this.Services.Indexes.Rows(tableName, predicate, progress, cancellationToken);

    /// <inheritdoc/>
    public IAsyncEnumerable<string[]> RowsAsStrings(
        string tableName,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        => this.Services.Tables.RowsAsStrings(tableName, progress, cancellationToken);

    /// <summary>Reads a table's persisted validation expression and message.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The table rule, or null when no rule is stored.</returns>
    public ValueTask<TableValidationRule?> GetTableValidationRuleAsync(string tableName, CancellationToken cancellationToken = default)
        => this.Services.Schema.GetTableValidationRuleAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
        => this.Services.Schema.GetColumnMetadataAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default)
        => this.Services.Indexes.ListIndexesAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<RelationshipMetadata>> ListRelationshipsAsync(CancellationToken cancellationToken = default)
        => this.Services.Schema.ListRelationshipsAsync(cancellationToken);

    /// <inheritdoc/>
    public IAccessIndexQuery<object[]> FromIndex(string tableName, string indexName)
        => new AccessObjectIndexQuery(this.Services.Indexes, tableName, indexName);

    /// <inheritdoc/>
    public IAccessIndexQuery<T> FromIndex<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, string indexName)
        where T : class, new()
            => new AccessTypedIndexQuery<T>(this.Services.Indexes, tableName, indexName);

    /// <inheritdoc/>
    [RequiresUnreferencedCode("LINQ queries and Include discover entity types and members at runtime. Use typed row readers for trimmed applications.")]
    [RequiresDynamicCode("LINQ queries and Include construct generic types at runtime. Use typed row readers for NativeAOT applications.")]
    public IQueryable<T> Query<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        var provider = new AccessQueryProvider<T>(this.Services.Tables, this.Services.Indexes, this.Services.Schema, tableName, this.Services.Operations);
        return new AccessQueryable<T>(provider);
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<object[]> SeekRowsAsync(
        string tableName,
        string indexName,
        IReadOnlyList<object?> keyValues,
        CancellationToken cancellationToken = default)
        => this.Services.Indexes.SeekRowsAsync(tableName, indexName, keyValues, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default)
        => this.Services.Schema.GetComplexColumnsAsync(tableName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
        => this.Services.ComplexItems.GetAttachmentsAsync(tableName, columnName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<MultiValueItem>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
        => this.Services.ComplexItems.GetMultiValueItemsAsync(tableName, columnName, cancellationToken);

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
        => this.Services.Tables.ReadTableAsync(tableName, maxRows, progress, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<T>> ReadTableAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
        where T : class, new()
        => this.Services.Tables.ReadTableAsync<T>(tableName, maxRows, cancellationToken);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows as a string-typed <see cref="DataTable"/> asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read, or <c>null</c> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> with all columns typed as <see cref="string"/>.</returns>
    public ValueTask<DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.Services.Tables.ReadTableAsStringsAsync(tableName, maxRows, progress, cancellationToken);

    /// <summary>
    /// Returns statistical information about the database asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="DatabaseStatistics"/> object containing various metrics about the database.</returns>
    public ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default)
        => this.Services.Schema.GetStatisticsAsync(cancellationToken);

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns asynchronously.
    /// Each table's columns use their native CLR types (int, DateTimeType, decimal, etc.).
    /// </summary>
    /// <param name="progress">Optional progress reporter for table read operations.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A dictionary mapping table names to their corresponding DataTables.</returns>
    public ValueTask<IReadOnlyDictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default)
        => this.Services.Tables.ReadAllTablesAsync(progress, cancellationToken);

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        AsyncReentrantOperationGate operations = this.Services.Operations;
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
        this.Services.Dispose();
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
            throw new JetCorruptDataException(JetErrorCode.CorruptDatabaseHeader, "File too small to be a valid JET database", new JetErrorInfo { PageNumber = 0 });
        }

        // Verify the JET magic signature at offset 0: 00 01 00 00
        _ = stream.Seek(0, SeekOrigin.Begin);
        byte[] magic = new byte[4];
        int read = 0;
        while (read < magic.Length)
        {
            int count = stream.Read(magic, read, magic.Length - read);
            if (count == 0)
            {
                break;
            }

            read += count;
        }

        if (read < 4 || magic[0] != 0x00 || magic[1] != 0x01 || magic[2] != 0x00 || magic[3] != 0x00)
        {
            string msg = $"File does not have a valid JET magic signature (expected 00 01 00 00, got {magic[0]:X2} {magic[1]:X2} {magic[2]:X2} {magic[3]:X2}).";
            throw new JetCorruptDataException(JetErrorCode.CorruptDatabaseHeader, msg, new JetErrorInfo { PageNumber = 0 });
        }
    }
}
