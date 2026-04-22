namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable SA1202 // Keep member order stable while synchronous APIs remain private compatibility helpers
#pragma warning disable SA1648 // Private compatibility helpers still carry inherited docs from previous public API

/// <summary>
/// Pure-managed reader for Microsoft Access JET databases (.mdb / .accdb).
/// No OleDB, ODBC, or ACE/Jet driver installation required.
///
/// Supports:
///   Jet3  – Access 97 (.mdb)
///   Jet4+ – Access 2000-2019 (.mdb / .accdb)
///
/// Features:
///   ✓ All standard data types (Text, Integer, Date, GUID, Currency, etc.)
///   ✓ MEMO fields (inline + single-page + multi-page LVAL chains)
///   ✓ OLE Object fields — auto-detects images (JPEG/PNG/GIF/BMP), documents (PDF/DOC/RTF), archives (ZIP)
///   ✓ Streaming API — process millions of rows without OOM (StreamRows, ReadTable)
///   ✓ Progress reporting — IProgress&lt;int&gt; callbacks for long operations
///   ✓ Page cache — 256-page LRU cache (default 1 MB) for 50%+ performance boost
///   ✓ Catalog caching — single MSysObjects scan, reused across calls
///   ✓ Non-Western text — auto-detects code page from database header (Cyrillic, Japanese, etc.)
///   ✓ Encryption detection — throws clear UnauthorizedAccessException for password-protected DBs
///
/// Limitations:
///   ✗ Encrypted (password-protected) databases — remove password in Access first
///   ✗ Attachment fields (Type 0x11) — not supported (rare, added in Access 2007)
///   ✗ Linked tables — only local tables returned
///   ✗ Overflow rows (span multiple pages) — silently skipped (rare edge case)
///
/// Based on the mdbtools format specification:
///   https://github.com/mdbtools/mdbtools/blob/master/HACKING.md
///
/// Original C implementation by mdbtools contributors (see HACKING.md for details).
/// </summary>
public sealed class AccessReader : AccessBase, IAccessReader
{
    // Jet4 password XOR mask (mdbtools / jackcess). Applied together with
    // the 4-byte creation date at offset 0x72 to decode the stored password.
    private static readonly byte[] Jet4PasswordMask =
    [
        0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
        0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
        0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
        0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
        0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
    ];

    // ACE legacy password mask used for password-only ACCDB files
    // created via DBEngine.CompactDatabase(..., ";pwd=...").
    private static readonly byte[] AccdbLegacyPasswordMask =
    [
        0x1F, 0x9B, 0xB7, 0xCA, 0xD4, 0x24, 0xD0, 0x07,
        0x49, 0x3E, 0x62, 0x1B, 0xF9, 0xD6, 0xB4, 0x9D,
        0xBE, 0xF4, 0x45, 0xCB, 0x1F, 0x12, 0xE1, 0x4C,
        0x9D, 0x94, 0x2D, 0xBE, 0x25, 0xCF, 0x8F, 0xCE,
        0xDE, 0x01, 0x47, 0xA6, 0x78, 0xD5, 0x42, 0xD7,
    ];

    private readonly object _cacheLock = new();
    private readonly object _catalogLock = new();
    private readonly string _path;
    private readonly bool _useLockFile;
    private readonly Func<LinkedTableInfo, string, bool>? _linkedSourcePathValidator;
    private readonly string[] _linkedSourcePathAllowlist;
    private readonly AccessReaderOptions _linkedSourceOpenOptions;
    private volatile List<CatalogEntry>? _catalogCache;
    private volatile LruCache<long, byte[]>? _pageCache;
    private long _cacheHits;
    private long _cacheMisses;

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReader"/> class.
    /// Opens <paramref name="path"/> and detects the JET version.
    /// </summary>
    /// <param name="path">The path to the Access database file. May be empty when opened from a stream.</param>
    /// <param name="options">Options for configuring the AccessReader.</param>
    /// <param name="stream">An open, seekable stream for the database file.</param>
    /// <param name="hdr">Header bytes read from page 0.</param>
    private AccessReader(string path, AccessReaderOptions options, Stream stream, byte[] hdr)
        : base(stream, hdr)
    {
        Guard.NotNull(options, nameof(options));

        _path = path;
        _useLockFile = options.UseLockFile && !string.IsNullOrEmpty(path);
        _linkedSourcePathValidator = options.LinkedSourcePathValidator;
        _linkedSourcePathAllowlist = NormalizeLinkedSourcePathAllowlist(options.LinkedSourcePathAllowlist, path);
        _linkedSourceOpenOptions = CreateLinkedSourceOpenOptions(options, _linkedSourcePathAllowlist, _linkedSourcePathValidator);
        var password = _linkedSourceOpenOptions.Password;

        DiagnosticsEnabled = options.DiagnosticsEnabled;
        PageCacheSize = options.PageCacheSize;
        ParallelPageReadsEnabled = options.ParallelPageReadsEnabled;

        // Offset 0x62: encryption flag — only valid for Jet4 (ver == 1, Access 2000-2003).
        // ACCDB format (ver >= 2, Access 2007+) has completely different header semantics
        // at this offset; applying the Jet4 check to ACCDB files produces false positives.
        // Truly encrypted ACCDB files are detected later when the catalog page is unreadable.
        byte ver = hdr[0x14];

        if (_jet4 && ver == 1 && hdr.Length > 0x62)
        {
            byte encFlag = hdr[0x62];

            // Jet4 encryption flag values (offset 0x62):
            //   0x01 = Office97 password only (no page encryption)
            //   0x02 = RC4 page encryption
            //   0x03 = RC4 + password
            // Other values at this offset (e.g. 0xC3 in Jackcess test databases)
            // do NOT indicate encryption — they have different format-specific meaning.
            if (encFlag >= 0x01 && encFlag <= 0x03)
            {
                if (SecureStringUtilities.IsNullOrEmpty(password))
                {
                    throw new UnauthorizedAccessException(
                        "This database is encrypted or password-protected. " +
                        "Provide a password via AccessReaderOptions.Password, or " +
                        "remove the password in Microsoft Access (File > Info > Encrypt with Password) and try again.");
                }

                // Verify the provided password against the stored password at 0x42.
                // The stored password is XOR'd with a fixed mask + creation date bytes.
                string storedPassword = DecodeJet4Password(hdr);
                if (!SecureStringUtilities.EqualsPlainText(password, storedPassword))
                {
                    throw new UnauthorizedAccessException(
                        "The provided password is incorrect for this database.");
                }

                // Enable RC4 page decryption using the database key at offset 0x3E
                if ((encFlag & 0x02) != 0)
                {
                    _rc4DbKey = BitConverter.ToUInt32(hdr, 0x3E);
                }
            }
        }

        bool isAccdbCfbEncrypted = _jet4 && ver >= 2 && hdr.Length >= 4 &&
            hdr[0] == 0xD0 && hdr[1] == 0xCF && hdr[2] == 0x11 && hdr[3] == 0xE0;

        // ACCDB legacy password-only mode (standard ACCDB header, ver >= 2).
        // In practice, many normal ACCDB files reuse overlapping header bits at 0x62,
        // so we only enforce password verification for the known legacy-password
        // signature used by Access 2010+ CompactDatabase(";pwd=...") test fixtures.
        if (_jet4 && ver >= 3 && !isAccdbCfbEncrypted && hdr.Length > 0x62)
        {
            byte encFlag = hdr[0x62];
            if (encFlag == 0x07)
            {
                if (SecureStringUtilities.IsNullOrEmpty(password))
                {
                    throw new UnauthorizedAccessException(
                        "This database is password-protected. " +
                        "Provide a password via AccessReaderOptions.Password.");
                }

                string storedPassword = DecodeAccdbPassword(hdr);
                if (!SecureStringUtilities.EqualsPlainText(password, storedPassword))
                {
                    throw new UnauthorizedAccessException(
                        "The provided password is incorrect for this database.");
                }
            }
        }

        // ACCDB genuine AES encryption (CFB wrapped file).
        if (isAccdbCfbEncrypted)
        {
            // CFB magic: the file is genuinely AES-encrypted.
            // When a password is supplied, attempt to read anyway — the caller has
            // acknowledged the encrypted format and provided credentials.
            // Without a password, throw immediately with a clear message.
            if (SecureStringUtilities.IsNullOrEmpty(password))
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Access 2007+ AES encryption (Office Crypto API). " +
                    "Full AES page decryption is not yet supported by this library. " +
                    "To open the file, remove the password in Microsoft Access " +
                    "(File > Info > Decrypt Database) and try again.");
            }

            // Verify the provided password against the stored password at 0x42.
            // ACCDB uses the same XOR scheme as Jet4 for the header password area.
            string storedPassword = DecodeJet4Password(hdr);
            if (!SecureStringUtilities.EqualsPlainText(password, storedPassword))
            {
                throw new UnauthorizedAccessException(
                    "The provided password is incorrect for this database.");
            }

            // Derive AES-128 page decryption key from the password.
            _aesPageKey = DeriveAesPageKey(password!);

            return;
        }

        if (options.ValidateOnOpen)
        {
            ValidateDatabaseFormat();
        }

        if (_useLockFile)
        {
            CreateLockFile();
        }
    }

    /// <summary>Gets a value indicating whether to print console logs with verbose hex dumps for debugging. Default: false.</summary>
    public bool DiagnosticsEnabled { get; }

    /// <summary>Gets the maximum number of pages to keep in cache. 0 = unlimited, -1 = disabled. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; } = 256;

    /// <summary>Gets a value indicating whether asynchronous full-table reads use parallel processing for reading multiple pages. Can improve performance for large tables. Default: false.</summary>
    public bool ParallelPageReadsEnabled { get; }

    /// <summary>Gets diagnostic output populated after each call to <see cref="ListTablesAsync"/>.</summary>
    public string LastDiagnostics { get; private set; } = string.Empty;

    /// <summary>
    /// Asynchronously opens a JET database file and returns a new <see cref="AccessReader"/> instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessReader"/> for the specified database.</returns>
    public static async ValueTask<AccessReader> OpenAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        options ??= new AccessReaderOptions();
        FileStream fs = CreateStream(path, options);
        return await OpenAsync(fs, options, leaveOpen: false, cancellationToken).ConfigureAwait(false);
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
    public static async ValueTask<AccessReader> OpenAsync(Stream stream, AccessReaderOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        if (!stream.CanRead)
        {
            throw new ArgumentException("Stream must be readable.", nameof(stream));
        }

        if (!stream.CanSeek)
        {
            throw new ArgumentException("Stream must be seekable.", nameof(stream));
        }

        cancellationToken.ThrowIfCancellationRequested();

        options ??= new AccessReaderOptions();
        Stream wrapped = leaveOpen ? new NonClosingStreamWrapper(stream) : stream;
        try
        {
            string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
            byte[] header = await ReadHeaderAsync(wrapped, cancellationToken).ConfigureAwait(false);
            return new AccessReader(path, options, wrapped, header);
        }
        catch
        {
            if (!leaveOpen)
            {
                await wrapped.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public async ValueTask<DataTable> ReadFirstTableAsync(uint? maxRows = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        if (tables.Count == 0)
        {
            return new DataTable();
        }

        CatalogEntry entry = tables[0];
        TableDef? td = await ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
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

            long totalPages = _stream.Length / _pgSz;

            for (long p = 3; p < totalPages; p++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
                if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != entry.TDefPage)
                {
                    continue;
                }

                await foreach (List<string> row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
                {
                    _ = dt.Rows.Add(row.ToArray());
                    if (maxRows.HasValue && dt.Rows.Count >= maxRows.Value)
                    {
                        var result = dt;
                        dt = null;
                        return result;
                    }
                }
            }

            var final = dt;
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
        ThrowIfDisposed();
        return await GetLinkedTablesAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<List<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> entries = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        var result = new List<TableStat>(entries.Count);

        foreach (CatalogEntry entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TableDef? td = await ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
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
        ThrowIfDisposed();
        var dt = new DataTable("Tables");
        _ = dt.Columns.Add("TableName", typeof(string));
        _ = dt.Columns.Add("RowCount", typeof(long));
        _ = dt.Columns.Add("ColumnCount", typeof(int));

        List<TableStat> stats = await GetTableStatsAsync(cancellationToken).ConfigureAwait(false);
        foreach (TableStat s in stats)
        {
            _ = dt.Rows.Add(s.Name, s.RowCount, s.ColumnCount);
        }

        return dt;
    }

    /// <inheritdoc/>
    public async ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return 0;
        }

        long count = 0;
        long tdefPage = resolved.Value.Entry.TDefPage;
        long total = _stream.Length / _pgSz;

        for (long p = 3; p < total; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != tdefPage)
            {
                continue;
            }

            int numRows = Ru16(page, _dpNumRows);
            for (int r = 0; r < numRows; r++)
            {
                int raw = Ru16(page, _dpRowsStart + (r * 2));
                if ((raw & 0xC000) != 0)
                {
                    continue;
                }

                count++;
            }
        }

        return count;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<object[]> StreamRowsAsync(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            LinkedTableInfo? link = await FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await OpenLinkedSourceAsync(link, _path, _linkedSourceOpenOptions, _linkedSourcePathAllowlist, _linkedSourcePathValidator, cancellationToken).ConfigureAwait(false);
                await foreach (object[] row in source.StreamRowsAsync(link.ForeignName, progress, cancellationToken).ConfigureAwait(false))
                {
                    yield return row;
                }
            }

            yield break;
        }

        var (entry, td) = resolved.Value;
        long rowCount = 0;
        Dictionary<int, Dictionary<int, byte[]>>? complexData = await BuildComplexColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false);
        long total = _stream.Length / _pgSz;

        for (long p = 3; p < total; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != entry.TDefPage)
            {
                continue;
            }

            await foreach (List<string> row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
            {
                yield return ConvertRowToTyped(row, td.Columns, tableName, complexData);
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<T> StreamRowsAsync<T>(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        List<ColumnMetadata> meta = await GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        var headers = meta.ConvertAll(m => m.Name);
        var index = RowMapper<T>.BuildIndex(headers);

        await foreach (object[] row in StreamRowsAsync(tableName, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return RowMapper<T>.Map(row, index);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<string[]> StreamRowsAsStringsAsync(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            LinkedTableInfo? link = await FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await OpenLinkedSourceAsync(link, _path, _linkedSourceOpenOptions, _linkedSourcePathAllowlist, _linkedSourcePathValidator, cancellationToken).ConfigureAwait(false);
                await foreach (string[] row in source.StreamRowsAsStringsAsync(link.ForeignName, progress, cancellationToken).ConfigureAwait(false))
                {
                    yield return row;
                }
            }

            yield break;
        }

        var (entry, td) = resolved.Value;
        long rowCount = 0;
        long total = _stream.Length / _pgSz;

        for (long p = 3; p < total; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != entry.TDefPage)
            {
                continue;
            }

            await foreach (List<string> row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
            {
                yield return row.ToArray();
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<List<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            LinkedTableInfo? link = await FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await OpenLinkedSourceAsync(link, _path, _linkedSourceOpenOptions, _linkedSourcePathAllowlist, _linkedSourcePathValidator, cancellationToken).ConfigureAwait(false);
                return await source.GetColumnMetadataAsync(link.ForeignName, cancellationToken).ConfigureAwait(false);
            }

            return [];
        }

        Dictionary<string, string> complexSubtypes = new(StringComparer.OrdinalIgnoreCase);
        bool hasComplex = resolved.Value.Td.Columns.Any(c => c.Type == T_COMPLEX || c.Type == T_ATTACHMENT);
        if (hasComplex)
        {
            complexSubtypes = await ReadComplexColumnSubtypesAsync(tableName, cancellationToken).ConfigureAwait(false);
        }

        return resolved.Value.Td.Columns.Select((col, index) => new ColumnMetadata
        {
            Name = col.Name,
            TypeName = (col.Type == T_COMPLEX && complexSubtypes.TryGetValue(col.Name, out string? subtype))
                ? subtype
                : TypeCodeToName(col.Type),
            ClrType = TypeCodeToClrType(col.Type),
            MaxLength = col.Size > 0 ? col.Size : null,
            IsNullable = true,
            IsFixedLength = col.IsFixed,
            Ordinal = index,
            Size = SizeForColumn(col),
        }).ToList();
    }

    /// <summary>
    /// Creates a fluent query interface for the specified table.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <returns>A <see cref="TableQuery"/> for the specified table.</returns>
    public TableQuery Query(string tableName)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        return new TableQuery(this, tableName);
    }

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <returns>A list of user table names.</returns>
    public async ValueTask<List<string>> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return tables.ConvertAll(e => e.Name);
    }

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns.</returns>
    public async ValueTask<DataTable> ReadDataTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrEmpty(tableName))
        {
            List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
            if (tables.Count == 0)
            {
                return new DataTable();
            }

            tableName = tables[0].Name;
        }

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            LinkedTableInfo? link = await FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await OpenLinkedSourceAsync(link, _path, _linkedSourceOpenOptions, _linkedSourcePathAllowlist, _linkedSourcePathValidator, cancellationToken).ConfigureAwait(false);
                return await source.ReadDataTableAsync(link.ForeignName, maxRows, progress, cancellationToken).ConfigureAwait(false);
            }

            return new DataTable(tableName);
        }

        var (entry, td) = resolved.Value;
        DataTable? dt = null;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                _ = dt.Columns.Add(col.Name, TypeCodeToClrType(col.Type));
            }

            Dictionary<int, Dictionary<int, byte[]>>? complexData = await BuildComplexColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false);
            long total = _stream.Length / _pgSz;

            for (long p = 3; p < total; p++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
                if (page[0] != 0x01)
                {
                    continue;
                }

                long owner = Ri32(page, _dpTDefOff);
                if (owner != entry.TDefPage)
                {
                    continue;
                }

                await foreach (List<string> row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
                {
                    _ = dt.Rows.Add(ConvertRowToTyped(row, td.Columns, tableName, complexData));
                    if (maxRows.HasValue && dt.Rows.Count >= maxRows.Value)
                    {
                        progress?.Report(dt.Rows.Count);
                        var result = dt;
                        dt = null;
                        return result;
                    }
                }

                progress?.Report(dt.Rows.Count);
            }

            var final = dt;
            dt = null;
            return final;
        }
        finally
        {
            dt?.Dispose();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<List<T>> ReadTableAsync<T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
        where T : class, new()
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        List<ColumnMetadata> meta = await GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        var headers = meta.ConvertAll(m => m.Name);
        var index = RowMapper<T>.BuildIndex(headers);
        var items = new List<T>();
        int count = 0;

        await foreach (object[] row in StreamRowsAsync(tableName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            items.Add(RowMapper<T>.Map(row, index));
            count++;
            if (maxRows.HasValue && count >= maxRows.Value)
            {
                break;
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
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return new DataTable(tableName);
        }

        var (entry, td) = resolved.Value;
        DataTable? dt = null;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                _ = dt.Columns.Add(col.Name, typeof(string));
            }

            long total = _stream.Length / _pgSz;

            for (long p = 3; p < total; p++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
                if (page[0] != 0x01)
                {
                    continue;
                }

                long owner = Ri32(page, _dpTDefOff);
                if (owner != entry.TDefPage)
                {
                    continue;
                }

                await foreach (List<string> row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
                {
                    _ = dt.Rows.Add(row.ToArray());
                    if (maxRows.HasValue && dt.Rows.Count >= maxRows.Value)
                    {
                        var result = dt;
                        dt = null;
                        return result;
                    }
                }

                progress?.Report(dt.Rows.Count);
            }

            var final = dt;
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
    /// <returns>A <see cref="DatabaseStatistics"/> object containing various metrics about the database.</returns>
    public async ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        var tableRowCounts = new Dictionary<string, long>();
        long totalRows = 0;

        foreach (CatalogEntry table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TableDef? td = await ReadTableDefAsync(table.TDefPage, cancellationToken).ConfigureAwait(false);
            if (td != null)
            {
                tableRowCounts[table.Name] = td.RowCount;
                totalRows += td.RowCount;
            }
        }

        long totalAccess = _cacheHits + _cacheMisses;
        int pageCacheHitRate = totalAccess > 0 ? (int)(_cacheHits * 100 / totalAccess) : 0;

        return new DatabaseStatistics
        {
            TotalPages = _stream.Length / _pgSz,
            DatabaseSizeBytes = _stream.Length,
            TableCount = tables.Count,
            TotalRows = totalRows,
            TableRowCounts = tableRowCounts,
            PageCacheHitRate = pageCacheHitRate,
            Version = _jet4 ? "Jet4/ACE" : "Jet3",
            PageSize = _pgSz,
            CodePage = _codePage,
        };
    }

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns asynchronously.
    /// Each table's columns use their native CLR types (int, DateTime, decimal, etc.).
    /// </summary>
    /// <param name="progress">Optional progress reporter for table read operations.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A dictionary mapping table names to their corresponding DataTables.</returns>
    public async ValueTask<Dictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var result = new Dictionary<string, DataTable>(StringComparer.OrdinalIgnoreCase);
        List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < tables.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CatalogEntry table = tables[i];
            progress?.Report(new TableProgress { TableName = table.Name, TableIndex = i, TableCount = tables.Count });
            result[table.Name] = await ReadDataTableAsync(table.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
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
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var result = new Dictionary<string, DataTable>(StringComparer.OrdinalIgnoreCase);
        List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < tables.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CatalogEntry table = tables[i];
            progress?.Report(new TableProgress { TableName = table.Name, TableIndex = i, TableCount = tables.Count });
            result[table.Name] = await ReadTableAsStringsAsync(table.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            _linkedSourceOpenOptions.Password?.Dispose();

            if (_useLockFile)
            {
                DeleteLockFile();
            }

            lock (_cacheLock)
            {
                _pageCache?.Clear(ReturnPage);
                _pageCache = null;
            }

            lock (_catalogLock)
            {
                _catalogCache?.Clear();
            }
        }
        finally
        {
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static string SafeGet(List<string> row, int idx) =>
        (idx >= 0 && idx < row.Count) ? row[idx] : string.Empty;

    private static FileStream CreateStream(string path, AccessReaderOptions options)
    {
        return new FileStream(path, FileMode.Open, options.FileAccess, options.FileShare, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    /// <summary>
    /// Decodes the stored Jet4 password from the database header.
    /// The 40 bytes at offset 0x42 are XOR'd with a fixed mask and the
    /// 4-byte creation date at offset 0x72.
    /// </summary>
    private static string DecodeJet4Password(byte[] hdr)
    {
        var decoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            decoded[i] = (byte)(hdr[0x42 + i] ^ Jet4PasswordMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        // Decode as UTF-16LE. Stop at the first null character rather than
        // trimming from the end, because the encryption flag at 0x62 overlaps
        // byte 32 of the password area and may produce a non-null artifact.
        string raw = Encoding.Unicode.GetString(decoded);
        int nullIdx = raw.IndexOf('\0', StringComparison.Ordinal);
        return nullIdx >= 0 ? raw.Substring(0, nullIdx) : raw;
    }

    private static string DecodeAccdbPassword(byte[] hdr)
    {
        var decoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            decoded[i] = (byte)(hdr[0x42 + i] ^ AccdbLegacyPasswordMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        // Offset 0x62 overlaps the password area for 40-byte decode blocks,
        // so stop at the first null character.
        string raw = Encoding.Unicode.GetString(decoded);
        int nullIdx = raw.IndexOf('\0', StringComparison.Ordinal);
        return nullIdx >= 0 ? raw.Substring(0, nullIdx) : raw;
    }

    /// <summary>
    /// Derives a 128-bit AES key from a <see cref="System.Security.SecureString"/> password
    /// using SHA-256 (truncated to 16 bytes).
    /// </summary>
    private static byte[] DeriveAesPageKey(System.Security.SecureString password)
    {
        IntPtr ptr = IntPtr.Zero;
        try
        {
            ptr = Marshal.SecureStringToGlobalAllocUnicode(password);
            int charCount = password.Length;

            // Single-pass: read chars from unmanaged memory and encode to UTF-8
            // directly, eliminating the intermediate char buffer and a separate
            // Encoding.UTF8.GetBytes pass.
            Span<byte> utf8 = stackalloc byte[charCount * 3];
            int utf8Len = 0;

            // Batch-read 4 chars (8 bytes) per Marshal call to reduce P/Invoke overhead.
            int i = 0;
            for (; i + 3 < charCount; i += 4)
            {
                long val = Marshal.ReadInt64(ptr, i * 2);
                char c0 = (char)(val & 0xFFFF);
                char c1 = (char)((val >> 16) & 0xFFFF);
                char c2 = (char)((val >> 32) & 0xFFFF);
                char c3 = (char)((val >> 48) & 0xFFFF);

                // Fast path: all 4 chars are ASCII — skip per-char branching.
                if ((c0 | c1 | c2 | c3) < 0x80)
                {
                    utf8[utf8Len] = (byte)c0;
                    utf8[utf8Len + 1] = (byte)c1;
                    utf8[utf8Len + 2] = (byte)c2;
                    utf8[utf8Len + 3] = (byte)c3;
                    utf8Len += 4;
                }
                else
                {
                    Utf8EncodeChar(c0, utf8, ref utf8Len);
                    Utf8EncodeChar(c1, utf8, ref utf8Len);
                    Utf8EncodeChar(c2, utf8, ref utf8Len);
                    Utf8EncodeChar(c3, utf8, ref utf8Len);
                }
            }

            for (; i < charCount; i++)
            {
                char c = (char)Marshal.ReadInt16(ptr, i * 2);
                if (c < 0x80)
                {
                    utf8[utf8Len++] = (byte)c;
                }
                else
                {
                    Utf8EncodeChar(c, utf8, ref utf8Len);
                }
            }

            Span<byte> hash = stackalloc byte[32];
            using var sha = SHA256.Create();
            if (!sha.TryComputeHash(utf8.Slice(0, utf8Len), hash, out _))
            {
                throw new CryptographicException("SHA-256 hash computation failed.");
            }

            utf8[..utf8Len].Clear();

            byte[] key = new byte[16];
            hash[..16].CopyTo(key);
            hash.Clear();
            return key; // AES-128
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                Marshal.ZeroFreeGlobalAllocUnicode(ptr);
            }
        }
    }

    /// <summary>Encodes a single BMP character as UTF-8 into the destination span.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Utf8EncodeChar(char c, Span<byte> buf, ref int pos)
    {
        if (c < 0x80)
        {
            buf[pos++] = (byte)c;
        }
        else if (c < 0x800)
        {
            buf[pos++] = (byte)(0xC0 | (c >> 6));
            buf[pos++] = (byte)(0x80 | (c & 0x3F));
        }
        else
        {
            buf[pos++] = (byte)(0xE0 | (c >> 12));
            buf[pos++] = (byte)(0x80 | ((c >> 6) & 0x3F));
            buf[pos++] = (byte)(0x80 | (c & 0x3F));
        }
    }

    private static string TypeCodeToName(byte t) => t switch
    {
        T_BOOL => "Yes/No",
        T_BYTE => "Byte",
        T_INT => "Integer",
        T_LONG => "Long Integer",
        T_MONEY => "Currency",
        T_FLOAT => "Single",
        T_DOUBLE => "Double",
        T_DATETIME => "Date/Time",
        T_BINARY => "Binary",
        T_TEXT => "Text",
        T_OLE => "OLE Object",
        T_MEMO => "Memo",
        T_GUID => "GUID",
        T_NUMERIC => "Decimal",
        T_ATTACHMENT => "Attachment",
        T_COMPLEX => "Complex",
        _ => $"0x{t:X2}",
    };

    private static ColumnSize SizeForColumn(ColumnInfo col) => col.Type switch
    {
        T_BOOL => ColumnSize.FromBits(1),
        T_BYTE => ColumnSize.FromBytes(1),
        T_INT => ColumnSize.FromBytes(2),
        T_LONG or T_FLOAT => ColumnSize.FromBytes(4),
        T_MONEY or T_DOUBLE or T_DATETIME => ColumnSize.FromBytes(8),
        T_GUID => ColumnSize.FromBytes(16),
        T_NUMERIC => ColumnSize.FromBytes(17),
        T_TEXT => ColumnSize.FromChars(col.Size > 0 ? col.Size / 2 : 255),
        T_MEMO or T_OLE or T_ATTACHMENT or T_COMPLEX => ColumnSize.Lval,
        _ => col.Size > 0 ? ColumnSize.FromBytes(col.Size) : ColumnSize.Variable, // such as T_BINARY
    };

    private static Type TypeCodeToClrType(byte typeCode) => typeCode switch
    {
        T_BOOL => typeof(bool),
        T_BYTE => typeof(byte),
        T_INT => typeof(short),
        T_LONG => typeof(int),
        T_MONEY => typeof(decimal),
        T_FLOAT => typeof(float),
        T_DOUBLE => typeof(double),
        T_DATETIME => typeof(DateTime),
        T_GUID => typeof(Guid),
        T_NUMERIC => typeof(decimal),
        T_ATTACHMENT => typeof(byte[]),
        T_COMPLEX => typeof(byte[]),
        _ => typeof(string),
    };

    private static string ReadFixed(byte[] row, int start, ColumnInfo col, int sz)
    {
        try
        {
            return col.Type switch
            {
                T_BYTE => row[start].ToString(System.Globalization.CultureInfo.InvariantCulture),
                T_INT => ((short)Ru16(row, start)).ToString(System.Globalization.CultureInfo.InvariantCulture),
                T_LONG => Ri32(row, start).ToString(System.Globalization.CultureInfo.InvariantCulture),
                T_FLOAT => BitConverter.ToSingle(row, start).ToString("G", System.Globalization.CultureInfo.InvariantCulture),
                T_DOUBLE => BitConverter.ToDouble(row, start).ToString("G", System.Globalization.CultureInfo.InvariantCulture),
                T_DATETIME => OaDateToString(BitConverter.ToDouble(row, start)),
                T_MONEY => (BitConverter.ToInt64(row, start) / 10000.0m).ToString("F4", System.Globalization.CultureInfo.InvariantCulture),
                T_NUMERIC => ReadNumeric(row, start),
                T_GUID => ReadGuid(row, start),
                T_COMPLEX or T_ATTACHMENT when sz >= 4 => $"__CX:{Ri32(row, start)}__",
                _ => BitConverter.ToString(row, start, Math.Min(sz, 8)),
            };
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
        catch (OverflowException)
        {
            return string.Empty;
        }
        catch (IndexOutOfRangeException)
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// Scans the first 512 bytes for known file magic numbers (images, PDFs, Office docs, archives).
    /// Typical Access OLE fields wrap files in an OLE container (~78-byte header),
    /// so this scans beyond the OLE envelope to find the real file bytes.
    /// Returns a data-URI with appropriate MIME type, or null if no known format is found.
    /// </summary>
    private static string? TryDecodeOleObject(byte[] b, int start, int len)
    {
        if (b == null || len < 4)
        {
            return null;
        }

        int scanEnd = Math.Min(start + len, start + 512);
        for (int i = start; i < scanEnd - 3; i++)
        {
            // ── Images ──
            // JPEG: FF D8 FF
            if (b[i] == 0xFF && b[i + 1] == 0xD8 && b[i + 2] == 0xFF)
            {
                int fileLen = start + len - i;
                return "data:image/jpeg;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // PNG: 89 50 4E 47
            if (b[i] == 0x89 && b[i + 1] == 0x50 && b[i + 2] == 0x4E && b[i + 3] == 0x47)
            {
                int fileLen = start + len - i;
                return "data:image/png;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // GIF: 47 49 46
            if (b[i] == 0x47 && b[i + 1] == 0x49 && b[i + 2] == 0x46)
            {
                int fileLen = start + len - i;
                return "data:image/gif;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // BMP: 42 4D
            if (b[i] == 0x42 && b[i + 1] == 0x4D)
            {
                int fileLen = start + len - i;
                return "data:image/bmp;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // ── Documents ──
            // PDF: 25 50 44 46 (%PDF)
            if (b[i] == 0x25 && b[i + 1] == 0x50 && b[i + 2] == 0x44 && b[i + 3] == 0x46)
            {
                int fileLen = start + len - i;
                return "data:application/pdf;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // ZIP (also DOCX/XLSX/PPTX): 50 4B 03 04 (PK..)
            if (i + 3 < scanEnd && b[i] == 0x50 && b[i + 1] == 0x4B && b[i + 2] == 0x03 && b[i + 3] == 0x04)
            {
                int fileLen = start + len - i;

                // Check if it's an Office Open XML file by looking for [Content_Types].xml signature
                // For simplicity, return generic zip MIME
                return "data:application/zip;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // DOC (Word 97-2003): D0 CF 11 E0 (OLE compound file)
            if (i + 3 < scanEnd && b[i] == 0xD0 && b[i + 1] == 0xCF && b[i + 2] == 0x11 && b[i + 3] == 0xE0)
            {
                int fileLen = start + len - i;
                return "data:application/msword;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // RTF: 7B 5C 72 74 ({\rt)
            if (i + 3 < scanEnd && b[i] == 0x7B && b[i + 1] == 0x5C && b[i + 2] == 0x72 && b[i + 3] == 0x74)
            {
                int fileLen = start + len - i;
                return "data:application/rtf;base64," + Convert.ToBase64String(b, i, fileLen);
            }
        }

        return null;
    }

    private static string OaDateToString(double oaDate)
    {
        try
        {
            return DateTime.FromOADate(oaDate).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// Reads a Jet NUMERIC (17 bytes):
    ///   [precision(1)][scale(1)][sign(1)][pad(1)][96-bit LE integer: lo(4)+mid(4)+hi(4)]
    /// Uses the <see cref="decimal(int,int,int,bool,byte)"/> constructor which accepts any
    /// 96-bit integer directly — no manual multiply-chain needed.
    /// </summary>
    private static string ReadNumeric(byte[] b, int start)
    {
        if (start + 16 > b.Length)
        {
            return string.Empty;
        }

        byte scale = b[start + 1];
        bool neg = b[start + 2] != 0;
        uint lo = Ru32(b, start + 4);
        uint mid = Ru32(b, start + 8);
        uint hi = Ru32(b, start + 12);

        // decimal(int lo, int mid, int hi, bool isNegative, byte scale) requires scale ≤ 28
        if (scale > 28)
        {
            throw new JetLimitationException(
                $"T_NUMERIC scale {scale} exceeds the .NET decimal maximum of 28.");
        }

        try
        {
            return new decimal((int)lo, (int)mid, (int)hi, neg, scale).ToString("G", System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (OverflowException ex)
        {
            throw new JetLimitationException(
                $"T_NUMERIC value overflow (hi=0x{hi:X8}, mid=0x{mid:X8}, lo=0x{lo:X8}, scale={scale})", ex);
        }
    }

    private static string ReadGuid(byte[] b, int start)
    {
        if (start + 16 > b.Length)
        {
            return string.Empty;
        }

        // First three groups are stored little-endian in the Jet format
        return string.Format(
            System.Globalization.CultureInfo.InvariantCulture,
            "{{{0:X2}{1:X2}{2:X2}{3:X2}-{4:X2}{5:X2}-{6:X2}{7:X2}-{8:X2}{9:X2}-{10:X2}{11:X2}{12:X2}{13:X2}{14:X2}{15:X2}}}",
            b[start + 3],
            b[start + 2],
            b[start + 1],
            b[start],
            b[start + 5],
            b[start + 4],
            b[start + 7],
            b[start + 6],
            b[start + 8],
            b[start + 9],
            b[start + 10],
            b[start + 11],
            b[start + 12],
            b[start + 13],
            b[start + 14],
            b[start + 15]);
    }

    private static async ValueTask<AccessReader> OpenLinkedSourceAsync(
        LinkedTableInfo link,
        string hostDatabasePath,
        AccessReaderOptions linkedSourceOpenOptions,
        string[] linkedSourcePathAllowlist,
        Func<LinkedTableInfo, string, bool>? linkedSourcePathValidator,
        CancellationToken cancellationToken)
    {
        string resolvedPath = ResolveLinkedSourcePath(link, hostDatabasePath, linkedSourcePathAllowlist, linkedSourcePathValidator);
        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException(
                $"Source database for linked table '{link.Name}' not found: {resolvedPath}",
                resolvedPath);
        }

        return await OpenAsync(resolvedPath, linkedSourceOpenOptions, cancellationToken).ConfigureAwait(false);
    }

    private static string ResolveLinkedSourcePath(
        LinkedTableInfo link,
        string hostDatabasePath,
        string[] linkedSourcePathAllowlist,
        Func<LinkedTableInfo, string, bool>? linkedSourcePathValidator)
    {
        if (string.IsNullOrWhiteSpace(link.SourceDatabasePath))
        {
            throw new FileNotFoundException(
                $"Source database for linked table '{link.Name}' not found: {link.SourceDatabasePath}",
                link.SourceDatabasePath);
        }

        string rawPath = link.SourceDatabasePath.Trim();
        string baseDirectory = Path.GetDirectoryName(hostDatabasePath) ?? Directory.GetCurrentDirectory();
        string resolvedPath = ResolvePath(rawPath, baseDirectory, $"linked table '{link.Name}'");
        bool escapesHostDatabaseDirectory =
            !Path.IsPathRooted(rawPath) &&
            !IsPathWithinDirectory(resolvedPath, baseDirectory);

        bool callbackApproved = linkedSourcePathValidator?.Invoke(link, resolvedPath) ?? false;

        if (escapesHostDatabaseDirectory && !callbackApproved)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{link.SourceDatabasePath}' escapes the host database directory. " +
                "Use AccessReaderOptions.LinkedSourcePathValidator to explicitly allow trusted paths.");
        }

        if (linkedSourcePathAllowlist.Length > 0 &&
            !linkedSourcePathAllowlist.Any(root => IsPathWithinDirectory(resolvedPath, root)))
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{resolvedPath}' is not permitted by AccessReaderOptions.LinkedSourcePathAllowlist.");
        }

        if (linkedSourcePathValidator != null && !callbackApproved)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{resolvedPath}' was rejected by AccessReaderOptions.LinkedSourcePathValidator.");
        }

        return resolvedPath;
    }

    private static AccessReaderOptions CreateLinkedSourceOpenOptions(
        AccessReaderOptions options,
        string[] normalizedAllowlist,
        Func<LinkedTableInfo, string, bool>? linkedSourcePathValidator)
    {
        return new AccessReaderOptions
        {
            PageCacheSize = options.PageCacheSize,
            DiagnosticsEnabled = options.DiagnosticsEnabled,
            ParallelPageReadsEnabled = options.ParallelPageReadsEnabled,
            ValidateOnOpen = options.ValidateOnOpen,
            FileAccess = options.FileAccess,
            FileShare = options.FileShare,
            Password = SecureStringUtilities.CopyAsReadOnly(options.Password),
            UseLockFile = options.UseLockFile,
            LinkedSourcePathAllowlist = normalizedAllowlist,
            LinkedSourcePathValidator = linkedSourcePathValidator,
        };
    }

    private static string[] NormalizeLinkedSourcePathAllowlist(IReadOnlyList<string> allowlist, string hostDatabasePath)
    {
        if (allowlist == null || allowlist.Count == 0)
        {
            return [];
        }

        string baseDirectory = Path.GetDirectoryName(hostDatabasePath) ?? Directory.GetCurrentDirectory();
        var normalized = new List<string>(allowlist.Count);

        foreach (string path in allowlist)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            string fullPath = ResolvePath(path.Trim(), baseDirectory, "linked-source allowlist");
            normalized.Add(EnsureTrailingDirectorySeparator(fullPath));
        }

        return normalized.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static string ResolvePath(string path, string baseDirectory, string context)
    {
        try
        {
            return Path.IsPathRooted(path)
                ? Path.GetFullPath(path)
                : Path.GetFullPath(Path.Combine(baseDirectory, path));
        }
        catch (Exception ex) when (
            ex is ArgumentException ||
            ex is NotSupportedException ||
            ex is PathTooLongException)
        {
            throw new UnauthorizedAccessException(
                $"Invalid path in {context}: '{path}'.",
                ex);
        }
    }

    private static bool IsPathWithinDirectory(string path, string directory)
    {
        string fullPath = Path.GetFullPath(path);
        string fullDirectory = EnsureTrailingDirectorySeparator(Path.GetFullPath(directory));
        return fullPath.StartsWith(fullDirectory, StringComparison.OrdinalIgnoreCase);
    }

    private static string EnsureTrailingDirectorySeparator(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            return path;
        }

        char last = path[path.Length - 1];
        if (last != Path.DirectorySeparatorChar && last != Path.AltDirectorySeparatorChar)
        {
            return path + Path.DirectorySeparatorChar;
        }

        return path;
    }

    private static object[] ConvertRowToTyped(List<string> row, List<ColumnInfo> columns, string? tableName = null, Dictionary<int, Dictionary<int, byte[]>>? complexData = null)
    {
        var typedRow = new object[row.Count];
        for (int i = 0; i < row.Count && i < columns.Count; i++)
        {
            string raw = row[i];
            ColumnInfo col = columns[i];

            // Resolve complex-field attachments using preloaded complex data (keyed by col ordinal).
            // The variable slot holds a marker "__CX:<complex_id>__" encoding the FK
            // that joins this row to the hidden flat data table.
            if ((col.Type == T_COMPLEX || col.Type == T_ATTACHMENT) &&
                complexData != null &&
                complexData.TryGetValue(i, out Dictionary<int, byte[]>? colData))
            {
                // Extract complex_id from marker string (e.g. "__CX:1__" → 1).
                int complexId = ExtractComplexId(raw);
                if (complexId <= 0)
                {
                    // Fallback: use the parent row's AutoNumber ID.
                    complexId = ExtractParentId(row, columns);
                }

                if (complexId > 0 && colData.TryGetValue(complexId, out byte[]? attachBytes) &&
                    attachBytes != null && attachBytes.Length > 0)
                {
                    typedRow[i] = attachBytes;
                    continue;
                }

                typedRow[i] = DBNull.Value;
                continue;
            }

            typedRow[i] = TypedValueParser.ParseValue(raw, TypeCodeToClrType(col.Type));
        }

        return typedRow;
    }

    /// <summary>Extracts the parent row's integer ID from the first fixed LONG column.</summary>
    private static int ExtractParentId(List<string> row, List<ColumnInfo> columns)
    {
        for (int i = 0; i < columns.Count && i < row.Count; i++)
        {
            if (columns[i].Type == T_LONG && !string.IsNullOrEmpty(row[i]))
            {
                if (int.TryParse(row[i], out int id))
                {
                    return id;
                }
            }
        }

        return 0;
    }

    /// <summary>Extracts the complex_id from a marker string like "__CX:123__".</summary>
    private static int ExtractComplexId(string raw)
    {
        if (raw != null &&
            raw.StartsWith("__CX:", StringComparison.Ordinal) &&
            raw.EndsWith("__", StringComparison.Ordinal) &&
            raw.Length > 7)
        {
            string numStr = raw.Substring(5, raw.Length - 7);
            if (int.TryParse(numStr, out int id))
            {
                return id;
            }
        }

        return 0;
    }

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    private protected override async ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken)
    {
        if (_catalogCache != null)
        {
            return _catalogCache;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var diag = new System.Text.StringBuilder();
        _ = diag.AppendLine($"JET: {(_jet4 ? "Jet4/ACE" : "Jet3")}  PageSize: {_pgSz}  TotalPages: {_stream.Length / _pgSz}");

        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            _ = diag.AppendLine("ERROR: Page 2 is not a valid TDEF page (null returned).");
            LastDiagnostics = diag.ToString();
            lock (_catalogLock)
            {
                _catalogCache ??= [];
                return _catalogCache;
            }
        }

        _ = diag.AppendLine($"MSysObjects cols ({msys.Columns.Count}): " +
            string.Join(", ", msys.Columns.ConvertAll(c => $"{c.Name}[0x{c.Type:X2}]")));

        int idxId = msys.Columns.FindIndex(c => string.Equals(c.Name, "Id", StringComparison.OrdinalIgnoreCase));
        int idxName = msys.Columns.FindIndex(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
        int idxType = msys.Columns.FindIndex(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));
        int idxFlags = msys.Columns.FindIndex(c => string.Equals(c.Name, "Flags", StringComparison.OrdinalIgnoreCase));

        if (idxName < 0 || idxType < 0)
        {
            _ = diag.AppendLine("ERROR: Required catalog columns not found. Column name mismatch?");
            LastDiagnostics = diag.ToString();
            lock (_catalogLock)
            {
                _catalogCache ??= [];
                return _catalogCache;
            }
        }

        var result = new List<CatalogEntry>();
        long totPages = _stream.Length / _pgSz;
        int catPages = 0;
        int allRows = 0;

        for (long p = 3; p < totPages; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01)
            {
                continue;
            }

            if (Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            catPages++;

            await foreach (List<string> row in EnumerateRowsAsync(page, msys, cancellationToken).ConfigureAwait(false))
            {
                allRows++;
                string typeStr = SafeGet(row, idxType);
                string nameStr = SafeGet(row, idxName);
                string flagsStr = SafeGet(row, idxFlags);

                if (!int.TryParse(typeStr, out int objType) || objType != OBJ_TABLE)
                {
                    continue;
                }

                if (!long.TryParse(flagsStr, out long flagsLong))
                {
                    flagsLong = 0;
                }

                if ((unchecked((uint)flagsLong) & SYSTABLE_MASK) != 0)
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
                    if (!long.TryParse(SafeGet(row, idxId), out long id))
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

        _ = diag.AppendLine($"Catalog pages: {catPages}  Total rows scanned: {allRows}  User tables: {result.Count}");
        if (DiagnosticsEnabled)
        {
            foreach (CatalogEntry e in result)
            {
                _ = diag.AppendLine($"  [{e.Name}] TDEF page {e.TDefPage}");
            }
        }

        LastDiagnostics = diag.ToString();

        lock (_catalogLock)
        {
            _catalogCache ??= result;
            return _catalogCache;
        }
    }

    private async ValueTask<List<LinkedTableInfo>> GetLinkedTablesAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return [];
        }

        int idxName = msys.Columns.FindIndex(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
        int idxType = msys.Columns.FindIndex(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));
        int idxFlags = msys.Columns.FindIndex(c => string.Equals(c.Name, "Flags", StringComparison.OrdinalIgnoreCase));
        int idxDatabase = msys.Columns.FindIndex(c => string.Equals(c.Name, "Database", StringComparison.OrdinalIgnoreCase));
        int idxForeignName = msys.Columns.FindIndex(c => string.Equals(c.Name, "ForeignName", StringComparison.OrdinalIgnoreCase));
        int idxConnect = msys.Columns.FindIndex(c => string.Equals(c.Name, "Connect", StringComparison.OrdinalIgnoreCase));

        if (idxName < 0 || idxType < 0)
        {
            return [];
        }

        var result = new List<LinkedTableInfo>();
        long totPages = _stream.Length / _pgSz;

        for (long p = 3; p < totPages; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            await foreach (List<string> row in EnumerateRowsAsync(page, msys, cancellationToken).ConfigureAwait(false))
            {
                string typeStr = SafeGet(row, idxType);
                if (!int.TryParse(typeStr, out int objType))
                {
                    continue;
                }

                if (objType != OBJ_LINKED_TABLE && objType != OBJ_LINKED_ODBC)
                {
                    continue;
                }

                string nameStr = SafeGet(row, idxName);
                if (string.IsNullOrEmpty(nameStr))
                {
                    continue;
                }

                string flagsStr = SafeGet(row, idxFlags);
                if (long.TryParse(flagsStr, out long flagsLong) &&
                    (unchecked((uint)flagsLong) & SYSTABLE_MASK) != 0)
                {
                    continue;
                }

                bool isOdbc = objType == OBJ_LINKED_ODBC;
                result.Add(new LinkedTableInfo
                {
                    Name = nameStr,
                    ForeignName = SafeGet(row, idxForeignName),
                    SourceDatabasePath = isOdbc ? null : SafeGet(row, idxDatabase),
                    ConnectionString = isOdbc ? SafeGet(row, idxConnect) : null,
                    IsOdbc = isOdbc,
                });
            }
        }

        return result;
    }

    private void ValidateDatabaseFormat()
    {
        if (_stream.Length < 128)
        {
            throw new InvalidDataException("File too small to be a valid JET database");
        }

        // Verify the JET magic signature at offset 0: 00 01 00 00
        _ = _stream.Seek(0, SeekOrigin.Begin);
        var magic = new byte[4];
        int read = _stream.Read(magic, 0, 4);
        if (read < 4 || magic[0] != 0x00 || magic[1] != 0x01 || magic[2] != 0x00 || magic[3] != 0x00)
        {
            throw new InvalidDataException(
                $"File does not have a valid JET magic signature " +
                $"(expected 00 01 00 00, got {magic[0]:X2} {magic[1]:X2} {magic[2]:X2} {magic[3]:X2}).");
        }
    }

    /// <summary>Reads a page through the cache if enabled (PageCacheSize > 0).</summary>
    private async ValueTask<byte[]> ReadPageCachedAsync(long n, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (PageCacheSize < 0)
        {
            return await ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        }

        if (_pageCache == null && PageCacheSize > 0)
        {
            lock (_cacheLock)
            {
                if (_pageCache == null)
                {
                    _pageCache = new LruCache<long, byte[]>(PageCacheSize);
                }
            }
        }

        if (_pageCache != null && _pageCache.TryGetValue(n, out byte[] cached))
        {
            _ = Interlocked.Increment(ref _cacheHits);
            return cached;
        }

        _ = Interlocked.Increment(ref _cacheMisses);
        byte[] page = await ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        _pageCache?.Add(n, page);
        return page;
    }

    private async ValueTask<(CatalogEntry Entry, TableDef Td)?> ResolveTableAsync(string tableName, CancellationToken cancellationToken)
    {
        List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = tables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
        if (entry != null)
        {
            TableDef? td = await ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            if (td != null && td.Columns.Count > 0)
            {
                return (entry, td);
            }
        }

        return null;
    }

    /// <summary>
    /// Finds a linked table entry by name (case-insensitive).
    /// Returns null if the table is not a linked table.
    /// </summary>
    private async ValueTask<LinkedTableInfo?> FindLinkedTableAsync(string tableName, CancellationToken cancellationToken)
    {
        List<LinkedTableInfo> links = await GetLinkedTablesAsync(cancellationToken).ConfigureAwait(false);
        return links.Find(l => string.Equals(l.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Yields decoded rows from a single data page.</summary>
    /// <param name="page">The data page to enumerate rows from.</param>
    /// <param name="td">The table definition containing column information.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for rows.</param>
    private async IAsyncEnumerable<List<string>> EnumerateRowsAsync(byte[] page, TableDef td, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (RowBound rb in EnumerateLiveRowBounds(page))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (rb.RowSize < _numColsFldSz)
            {
                continue;
            }

            List<string>? values = await CrackRowAsync(page, rb.RowStart, rb.RowSize, td, cancellationToken).ConfigureAwait(false);
            if (values != null)
            {
                yield return values;
            }
        }
    }

    private async ValueTask<List<string>?> CrackRowAsync(byte[] page, int rowStart, int rowSize, TableDef td, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (rowSize < _numColsFldSz)
        {
            return null;
        }

        int numCols = _jet4 ? Ru16(page, rowStart) : page[rowStart];
        if (numCols == 0)
        {
            return null;
        }

        if (td.HasDeletedColumns && numCols > td.Columns.Count)
        {
            throw new JetLimitationException(
                $"Row has {numCols} columns but current schema has {td.Columns.Count} with deleted-column gaps. " +
                "This row predates schema changes and data may be misaligned. " +
                "Solution: Compact & Repair the database in Microsoft Access to rebuild all rows.");
        }

        int nullMaskSz = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSz;
        if (nullMaskPos < _numColsFldSz)
        {
            return null;
        }

        int varLenPos = nullMaskPos - _varLenFldSz;
        if (varLenPos < _numColsFldSz)
        {
            return null;
        }

        int varLen = _jet4 ? Ru16(page, rowStart + varLenPos) : page[rowStart + varLenPos];
        int jumpSz = _jet4 ? 0 : (rowSize / 256);

        int varTableStart = varLenPos - jumpSz - (varLen * _varEntrySz);
        int eodPos = varTableStart - _eodFldSz;
        if (eodPos < _numColsFldSz)
        {
            return null;
        }

        int eod = _jet4 ? Ru16(page, rowStart + eodPos) : page[rowStart + eodPos];
        var result = new List<string>(td.Columns.Count);

        for (int i = 0; i < td.Columns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = td.Columns[i];
            bool nullBit = false;
            if (col.ColNum < numCols)
            {
                int mByte = nullMaskPos + (col.ColNum / 8);
                int mBit = col.ColNum % 8;
                if (mByte < rowSize)
                {
                    nullBit = (page[rowStart + mByte] & (1 << mBit)) != 0;
                }
            }

            if (col.Type == T_BOOL)
            {
                result.Add(nullBit ? "True" : "False");
                continue;
            }

            if (col.ColNum >= numCols || !nullBit)
            {
                result.Add(string.Empty);
                continue;
            }

            if (col.IsFixed)
            {
                int start = _numColsFldSz + col.FixedOff;
                int sz = FixedSize(col.Type, col.Size);
                if (sz == 0 || start + sz > rowSize)
                {
                    result.Add(string.Empty);
                    continue;
                }

                result.Add(ReadFixed(page, rowStart + start, col, sz));
            }
            else
            {
                if (col.VarIdx >= varLen)
                {
                    result.Add(string.Empty);
                    continue;
                }

                int entryPos = varTableStart + ((varLen - 1 - col.VarIdx) * _varEntrySz);
                if (entryPos < 0 || entryPos + _varEntrySz > rowSize)
                {
                    result.Add(string.Empty);
                    continue;
                }

                int varOff = _jet4 ? Ru16(page, rowStart + entryPos) : page[rowStart + entryPos];

                int varEnd;
                if (col.VarIdx + 1 < varLen)
                {
                    int nextEntry = varTableStart + ((varLen - 2 - col.VarIdx) * _varEntrySz);
                    varEnd = _jet4 ? Ru16(page, rowStart + nextEntry) : page[rowStart + nextEntry];
                }
                else
                {
                    varEnd = eod;
                }

                int dataStart = varOff;
                int dataLen = varEnd - varOff;
                if (dataLen < 0 || dataStart < 0 || dataStart + dataLen > rowSize)
                {
                    result.Add(string.Empty);
                    continue;
                }

                string value = await ReadVarAsync(page, rowStart + dataStart, dataLen, col, cancellationToken).ConfigureAwait(false);
                result.Add(value);
            }
        }

        return result;
    }

    private async ValueTask<string> ReadVarAsync(byte[] row, int start, int len, ColumnInfo col, CancellationToken cancellationToken)
    {
        if (len <= 0)
        {
            return string.Empty;
        }

        try
        {
            switch (col.Type)
            {
                case T_TEXT:
                    return _jet4 ? DecodeJet4Text(row, start, len)
                                 : _ansiEncoding.GetString(row, start, len);

                case T_BINARY:
                    return BitConverter.ToString(row, start, len);

                case T_MEMO:
                case T_OLE:
                    return await ReadLongValueAsync(row, start, len, col.Type == T_OLE, cancellationToken).ConfigureAwait(false);

                case T_COMPLEX:
                case T_ATTACHMENT:
                    if (len >= 4)
                    {
                        int complexId = Ri32(row, start);
                        return $"__CX:{complexId}__";
                    }

                    return string.Empty;

                case T_BYTE:
                    return len >= 1 ? row[start].ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty;
                case T_INT:
                    return len >= 2 ? ((short)Ru16(row, start)).ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty;
                case T_LONG:
                    return len >= 4 ? Ri32(row, start).ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty;
                case T_FLOAT:
                    return len >= 4 ? BitConverter.ToSingle(row, start).ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty;
                case T_DOUBLE:
                    return len >= 8 ? BitConverter.ToDouble(row, start).ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty;
                case T_DATETIME:
                    return len >= 8 ? ReadFixed(row, start, col, 8) : string.Empty;
                case T_MONEY:
                    return len >= 8 ? ReadFixed(row, start, col, 8) : string.Empty;
                case T_GUID:
                    return len >= 16 ? ReadFixed(row, start, col, 16) : string.Empty;

                default:
                    return string.Empty;
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

    private async ValueTask<Dictionary<string, string>> ReadComplexColumnSubtypesAsync(string tableName, CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            long tdefPage = await FindSystemTablePageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
            if (tdefPage <= 0)
            {
                return result;
            }

            TableDef? td = await ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            if (td == null)
            {
                return result;
            }

            int idxCol = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ColumnName", StringComparison.OrdinalIgnoreCase));
            int idxConceptualTable = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ConceptualTableID", StringComparison.OrdinalIgnoreCase)
                || string.Equals(c.Name, "TableName", StringComparison.OrdinalIgnoreCase));

            if (idxCol < 0)
            {
                return result;
            }

            var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            long targetTdefPage = resolved is { } resolvedValue ? resolvedValue.Entry.TDefPage : 0;
            long total = _stream.Length / _pgSz;

            for (long p = 3; p < total; p++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
                if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != tdefPage)
                {
                    continue;
                }

                await foreach (List<string> row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
                {
                    if (idxConceptualTable >= 0 && targetTdefPage > 0)
                    {
                        string tableIdStr = SafeGet(row, idxConceptualTable);
                        if (long.TryParse(tableIdStr, out long tableId))
                        {
                            long rowTdefPage = tableId & 0x00FFFFFFL;
                            if (rowTdefPage != targetTdefPage)
                            {
                                continue;
                            }
                        }
                        else if (!string.Equals(tableIdStr, tableName, StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }
                    }

                    string colName = SafeGet(row, idxCol);
                    result[colName] = "Attachment";
                }
            }
        }
        catch (InvalidDataException)
        {
            if (DiagnosticsEnabled)
            {
                System.Diagnostics.Trace.WriteLine("[AccessReader] Best-effort fallback in ReadComplexColumnSubtypesAsync: suppressed InvalidDataException while reading MSysComplexColumns.");
            }
        }
        catch (IndexOutOfRangeException)
        {
            if (DiagnosticsEnabled)
            {
                System.Diagnostics.Trace.WriteLine("[AccessReader] Best-effort fallback in ReadComplexColumnSubtypesAsync: suppressed IndexOutOfRangeException while reading MSysComplexColumns.");
            }
        }

        return result;
    }

    /// <summary>
    /// Finds the TDEF page number for a system table by name (case-insensitive).
    /// Unlike GetUserTables, this includes system tables (SYSTABLE_MASK set).
    /// </summary>
    private async ValueTask<long> FindSystemTablePageAsync(string name, CancellationToken cancellationToken)
    {
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        int idxId = msys.Columns.FindIndex(c => string.Equals(c.Name, "Id", StringComparison.OrdinalIgnoreCase));
        int idxName = msys.Columns.FindIndex(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
        int idxType = msys.Columns.FindIndex(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));

        if (idxId < 0 || idxName < 0 || idxType < 0)
        {
            return 0;
        }

        long totPages = _stream.Length / _pgSz;
        for (long p = 3; p < totPages; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            await foreach (List<string> row in EnumerateRowsAsync(page, msys, cancellationToken).ConfigureAwait(false))
            {
                string nameStr = SafeGet(row, idxName);
                if (!string.Equals(nameStr, name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!int.TryParse(SafeGet(row, idxType), out int objType) || (objType != OBJ_TABLE && objType != 6))
                {
                    continue;
                }

                if (long.TryParse(SafeGet(row, idxId), out long id))
                {
                    long tdefPage = id & 0x00FFFFFFL;
                    if (tdefPage > 0)
                    {
                        return tdefPage;
                    }
                }
            }
        }

        return 0;
    }

    /// <summary>
    /// Builds a preloaded map of complex column data for all complex columns in a table.
    /// Returns a dictionary: column ordinal → (parentId → attachment bytes).
    /// Returns null if the table has no complex columns.
    /// </summary>
    private async ValueTask<Dictionary<int, Dictionary<int, byte[]>>?> BuildComplexColumnDataAsync(
        string tableName,
        List<ColumnInfo> columns,
        CancellationToken cancellationToken)
    {
        Dictionary<int, Dictionary<int, byte[]>>? result = null;

        for (int i = 0; i < columns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = columns[i];
            if (col.Type != T_COMPLEX && col.Type != T_ATTACHMENT)
            {
                continue;
            }

            Dictionary<int, byte[]>? colData = await LoadAttachmentDataAsync(tableName, col.Name, cancellationToken).ConfigureAwait(false);
            if (colData != null && colData.Count > 0)
            {
                result ??= [];
                result[i] = colData;
            }
        }

        return result;
    }

    /// <summary>
    /// Reads MSysComplexColumns to find the FlatTableID for a given table + column.
    /// Returns the TDEF page number (lower 24 bits of FlatTableID), or 0 if not found.
    /// </summary>
    private async ValueTask<long> GetComplexFlatTablePageAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            long msysTdef = await FindSystemTablePageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
            if (msysTdef <= 0)
            {
                return 0;
            }

            TableDef? td = await ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false);
            if (td == null)
            {
                return 0;
            }

            int idxCol = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ColumnName", StringComparison.OrdinalIgnoreCase));
            int idxConceptualTable = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ConceptualTableID", StringComparison.OrdinalIgnoreCase));
            int idxFlatTable = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "FlatTableID", StringComparison.OrdinalIgnoreCase));

            if (idxCol < 0 || idxFlatTable < 0)
            {
                return 0;
            }

            List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
            CatalogEntry entry = tables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
            long targetTdefPage = entry?.TDefPage ?? 0;

            long total = _stream.Length / _pgSz;
            for (long p = 3; p < total; p++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (Ri32(page, _dpTDefOff) != msysTdef)
                {
                    continue;
                }

                await foreach (List<string> row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
                {
                    string colName = SafeGet(row, idxCol);
                    if (!string.Equals(colName, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (idxConceptualTable >= 0 && targetTdefPage > 0)
                    {
                        string tableIdStr = SafeGet(row, idxConceptualTable);
                        if (long.TryParse(tableIdStr, out long tableId))
                        {
                            long rowTdefPage = tableId & 0x00FFFFFFL;
                            if (rowTdefPage != targetTdefPage)
                            {
                                continue;
                            }
                        }
                    }

                    string flatIdStr = SafeGet(row, idxFlatTable);
                    if (long.TryParse(flatIdStr, out long flatId))
                    {
                        long flatTdef = flatId & 0x00FFFFFFL;
                        if (flatTdef > 0)
                        {
                            return flatTdef;
                        }
                    }
                }
            }
        }
        catch (InvalidDataException)
        {
            return 0;
        }

        return 0;
    }

    /// <summary>
    /// Loads attachment data from the hidden system table for a complex column.
    /// Access ACCDB stores attachment data in a hidden table found via MSysComplexColumns FlatTableID.
    /// The table's FK column is named <c>&lt;tableName&gt;_&lt;columnName&gt;</c> and holds the complex_id.
    /// Returns a dictionary mapping complex_id → serialized attachment bytes
    /// (2-byte FileName length LE, FileName UTF-16LE, FileData bytes).
    /// </summary>
    /// <param name="tableName">Name of the table containing the complex column.</param>
    /// <param name="columnName">Name of the complex column.</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A dictionary mapping complex_id to serialized attachment bytes.</returns>
    private async ValueTask<Dictionary<int, byte[]>?> LoadAttachmentDataAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            long tdefPage = await GetComplexFlatTablePageAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
            if (tdefPage <= 0)
            {
                string nameSuffix = $"_{columnName}";
                tdefPage = await FindSystemTablePageBySuffixAsync(nameSuffix, cancellationToken).ConfigureAwait(false);
            }

            if (tdefPage <= 0)
            {
                return null;
            }

            TableDef? td = await ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            if (td == null)
            {
                return null;
            }

            // Cache column indices to avoid repeated lookups
            string fkColName = $"{tableName}_{columnName}";
            int idxFk = td.Columns.FindIndex(c => string.Equals(c.Name, fkColName, StringComparison.OrdinalIgnoreCase));
            if (idxFk < 0)
            {
                idxFk = td.Columns.FindIndex(c => c.Type == T_LONG && !c.Name.StartsWith("Idx", StringComparison.OrdinalIgnoreCase));
            }

            if (idxFk < 0)
            {
                return null;
            }

            int idxFileName = td.Columns.FindIndex(c => string.Equals(c.Name, "FileName", StringComparison.OrdinalIgnoreCase));
            int idxFileData = td.Columns.FindIndex(c => string.Equals(c.Name, "FileData", StringComparison.OrdinalIgnoreCase));

            var result = new Dictionary<int, byte[]>(capacity: 32); // Preallocate for small tables
            long total = _stream.Length / _pgSz;

            // Use a buffer for the inner loop to avoid repeated allocations
            byte[]? buffer = null;
            for (long p = 3; p < total; p++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
                if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != tdefPage)
                {
                    continue;
                }

                await foreach (var row in EnumerateRowsAsync(page, td, cancellationToken).ConfigureAwait(false))
                {
                    string fkStr = SafeGet(row, idxFk);
                    if (!int.TryParse(fkStr, out int parentId))
                    {
                        continue;
                    }

                    byte[] fileNameBytes = [];
                    if (idxFileName >= 0)
                    {
                        string fileName = SafeGet(row, idxFileName);
                        if (!string.IsNullOrEmpty(fileName))
                        {
                            fileNameBytes = Encoding.Unicode.GetBytes(fileName);
                        }
                    }

                    byte[] fileDataBytes = [];
                    if (idxFileData >= 0)
                    {
                        string fileDataStr = SafeGet(row, idxFileData);
                        fileDataBytes = DecodeColumnBytes(fileDataStr ?? string.Empty, td.Columns[idxFileData].Type);
                        if (fileDataBytes.Length > 1)
                        {
                            if (fileDataBytes[0] == 0x01)
                            {
                                fileDataBytes = DecompressAttachmentData(fileDataBytes, 1);
                            }
                            else if (fileDataBytes[0] == 0x00)
                            {
                                fileDataBytes = fileDataBytes.AsSpan(1).ToArray();
                            }
                        }
                    }

                    if (fileNameBytes.Length == 0 && fileDataBytes.Length == 0)
                    {
                        continue;
                    }

                    int totalLen = 2 + fileNameBytes.Length + fileDataBytes.Length;
                    buffer = buffer is null || buffer.Length < totalLen ? new byte[totalLen] : buffer;
                    buffer[0] = (byte)(fileNameBytes.Length & 0xFF);
                    buffer[1] = (byte)((fileNameBytes.Length >> 8) & 0xFF);
                    if (fileNameBytes.Length > 0)
                    {
                        Buffer.BlockCopy(fileNameBytes, 0, buffer, 2, fileNameBytes.Length);
                    }

                    if (fileDataBytes.Length > 0)
                    {
                        Buffer.BlockCopy(fileDataBytes, 0, buffer, 2 + fileNameBytes.Length, fileDataBytes.Length);
                    }

                    // Copy to new array to avoid overwriting in next iteration
                    result[parentId] = [.. buffer[..totalLen]];
                }
            }

            if (result.Count > 0)
            {
                return result;
            }

            return null;
        }
        catch (InvalidDataException)
        {
            return null;
        }
        catch (IndexOutOfRangeException)
        {
            return null;
        }
        catch (IOException)
        {
            return null;
        }
    }

    /// <summary>
    /// Finds the TDEF page for the first system table whose name ends with
    /// <paramref name="nameSuffix"/> (case-insensitive). Used to find
    /// <c>f_&lt;GUID&gt;_&lt;columnName&gt;</c> attachment tables.
    /// </summary>
    private async ValueTask<long> FindSystemTablePageBySuffixAsync(string nameSuffix, CancellationToken cancellationToken)
    {
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        int idxId = -1, idxName = -1, idxType = -1;
        for (int i = 0; i < msys.Columns.Count; i++)
        {
            var col = msys.Columns[i];
            if (idxId < 0 && string.Equals(col.Name, "Id", StringComparison.OrdinalIgnoreCase))
            {
                idxId = i;
            }
            else if (idxName < 0 && string.Equals(col.Name, "Name", StringComparison.OrdinalIgnoreCase))
            {
                idxName = i;
            }
            else if (idxType < 0 && string.Equals(col.Name, "Type", StringComparison.OrdinalIgnoreCase))
            {
                idxType = i;
            }

            if (idxId >= 0 && idxName >= 0 && idxType >= 0)
            {
                break;
            }
        }

        if (idxId < 0 || idxName < 0 || idxType < 0)
        {
            return 0;
        }

        long totPages = _stream.Length / _pgSz;
        for (long p = 3; p < totPages; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageCachedAsync(p, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            await foreach (var row in EnumerateRowsAsync(page, msys, cancellationToken).ConfigureAwait(false))
            {
                string nameStr = SafeGet(row, idxName);
                if (!nameStr.EndsWith(nameSuffix, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!int.TryParse(SafeGet(row, idxType), out int objType) || (objType != OBJ_TABLE && objType != 6))
                {
                    continue;
                }

                if (long.TryParse(SafeGet(row, idxId), out long id))
                {
                    long tdefPage = id & 0x00FFFFFFL;
                    if (tdefPage > 0)
                    {
                        return tdefPage;
                    }
                }
            }
        }

        return 0;
    }

    /// <summary>
    /// Decompresses Access attachment file data using raw Deflate.
    /// Access stores attachment data with a 1-byte compression flag followed by
    /// deflate-compressed content.
    /// </summary>
#pragma warning disable SA1204
    private static byte[] DecompressAttachmentData(byte[] data, int offset)
#pragma warning restore SA1204
    {
        try
        {
            // Scan for zlib header (0x78) after the compression flag byte.
            // Access attachment data starts with a 1-byte compression flag,
            // followed by implementation-dependent header bytes, then zlib-compressed data.
            int zlibPos = -1;
            for (int i = offset; i < data.Length - 1; i++)
            {
                if (data[i] == 0x78 && (data[i + 1] == 0x01 || data[i + 1] == 0x5E || data[i + 1] == 0x9C || data[i + 1] == 0xDA))
                {
                    zlibPos = i;
                    break;
                }
            }

            if (zlibPos < 0 || zlibPos + 2 >= data.Length)
            {
                return data.AsSpan(offset).ToArray();
            }

            // Skip the 2-byte zlib header for raw DeflateStream
            int deflateStart = zlibPos + 2;
            using var input = new MemoryStream(data, deflateStart, data.Length - deflateStart);
            using var deflate = new System.IO.Compression.DeflateStream(input, System.IO.Compression.CompressionMode.Decompress);
            using var output = new MemoryStream();
            deflate.CopyTo(output);
            return output.ToArray();
        }
        catch (InvalidDataException)
        {
            // Not valid deflate — return raw bytes without compression flag.
            return data.AsSpan(offset).ToArray();
        }
    }

    /// <summary>
    /// Attempts to convert a string column value back to raw bytes.
    /// Handles: Base64 data URIs (OLE), hex strings (Binary), plain text (Memo).
    /// </summary>
#pragma warning disable SA1204 // Static helper placed here for proximity to its only caller
    private static byte[] DecodeColumnBytes(string value, byte colType)
#pragma warning restore SA1204
    {
        if (string.IsNullOrEmpty(value))
        {
            return [];
        }

        // Base64 data URI from OLE column: "data:...;base64,<b64>"
        if (value.StartsWith("data:", StringComparison.Ordinal))
        {
            int commaIdx = value.IndexOf(',', StringComparison.Ordinal);
            if (commaIdx >= 0)
            {
                try
                {
                    return Convert.FromBase64String(value.Substring(commaIdx + 1));
                }
                catch (FormatException)
                {
                    return [];
                }
            }

            return [];
        }

        // Hex string from Binary column: "XX-XX-XX-..."
        if (colType == T_BINARY && value.Contains('-', StringComparison.Ordinal))
        {
            try
            {
                string[] parts = value.Split('-');
                var bytes = new byte[parts.Length];
                for (int i = 0; i < parts.Length; i++)
                {
                    bytes[i] = Convert.ToByte(parts[i], 16);
                }

                return bytes;
            }
            catch (FormatException)
            {
                return [];
            }
        }

        // Plain text (Memo): encode as UTF-8
        return Encoding.UTF8.GetBytes(value);
    }

    // [memo_len: 3 bytes][bitmask: 1 byte][lval_dp: 4 bytes][unknown: 4 bytes]
    // 0x80 = inline data immediately after the 12-byte header
    // 0x40 = single LVAL page:  lval_dp = (page << 8) | row_index
    // 0x00 = chained LVAL pages (not decoded; placeholder returned)

    /// <summary>
    /// Reads <paramref name="maxLen"/> bytes from a single LVAL data page / row.
    /// lval_dp encoding: upper 24 bits = page number, lower 8 bits = row index.
    /// </summary>
    private async ValueTask<byte[]?> ReadLvalBytesAsync(uint lvalDp, int maxLen, CancellationToken cancellationToken)
    {
        try
        {
            int lvalPage = (int)(lvalDp >> 8);
            int lvalRow = (int)(lvalDp & 0xFF);
            if (lvalPage <= 0)
            {
                return null;
            }

            byte[] page = await ReadPageCachedAsync(lvalPage, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01)
            {
                return null;
            }

            int numRows = Ru16(page, _dpNumRows);
            if (lvalRow >= numRows)
            {
                return null;
            }

            int rawOff = Ru16(page, _dpRowsStart + (lvalRow * 2));
            if ((rawOff & 0xC000) != 0)
            {
                return null;
            }

            int rowStart = rawOff & 0x1FFF;
            if (rowStart == 0 || rowStart >= _pgSz)
            {
                return null;
            }

            int rowEnd = _pgSz - 1;
            for (int r = 0; r < numRows; r++)
            {
                int ofs = Ru16(page, _dpRowsStart + (r * 2)) & 0x1FFF;
                if (ofs > rowStart && ofs < rowEnd)
                {
                    rowEnd = ofs - 1;
                }
            }

            int rowSize = Math.Min(rowEnd - rowStart + 1, maxLen);
            if (rowSize <= 0)
            {
                return null;
            }

            var data = new byte[rowSize];
            Buffer.BlockCopy(page, rowStart, data, 0, rowSize);
            return data;
        }
        catch (ArgumentException)
        {
            return null;
        }
        catch (IndexOutOfRangeException)
        {
            return null;
        }
    }

    /// <summary>
    /// Reads multi-page LVAL chains (bitmask 0x00). Follows LVAL page links until
    /// the entire memo is reconstructed or maxLen is reached.
    /// LVAL page format (mdbtools): [next_page(4)][data_length(4)][data...].
    /// </summary>
    /// <returns>Success: concatenated data bytes from the entire LVAL chain, up to maxLen. Failure: error message.</returns>
    private async ValueTask<LvalChainResult> ReadLvalChainAsync(uint firstLvalDp, int maxLen, CancellationToken cancellationToken)
    {
        try
        {
            var chunks = new List<byte[]>();
            int totalLen = 0;
            uint currentDp = firstLvalDp;
            var seen = new HashSet<uint>();

            while (currentDp != 0 && totalLen < maxLen && !seen.Contains(currentDp))
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = seen.Add(currentDp);

                int lvalPage = (int)(currentDp >> 8);
                int lvalRow = (int)(currentDp & 0xFF);
                if (lvalPage <= 0)
                {
                    return LvalChainResult.Failure($"invalid page {lvalPage}");
                }

                byte[] page = await ReadPageCachedAsync(lvalPage, cancellationToken).ConfigureAwait(false);
                if (page[0] != 0x01)
                {
                    return LvalChainResult.Failure($"page {lvalPage} not data page");
                }

                int numRows = Ru16(page, _dpNumRows);
                if (lvalRow >= numRows)
                {
                    return LvalChainResult.Failure($"row {lvalRow} >= numRows {numRows}");
                }

                int rawOff = Ru16(page, _dpRowsStart + (lvalRow * 2));
                if ((rawOff & 0xC000) != 0)
                {
                    return LvalChainResult.Failure("deleted/overflow row");
                }

                int rowStart = rawOff & 0x1FFF;
                if (rowStart == 0 || rowStart >= _pgSz)
                {
                    return LvalChainResult.Failure($"invalid rowStart {rowStart}");
                }

                int rowEnd = _pgSz - 1;
                for (int r = 0; r < numRows; r++)
                {
                    int ofs = Ru16(page, _dpRowsStart + (r * 2)) & 0x1FFF;
                    if (ofs > rowStart && ofs < rowEnd)
                    {
                        rowEnd = ofs - 1;
                    }
                }

                int rowSize = rowEnd - rowStart + 1;
                if (rowSize < 8)
                {
                    return LvalChainResult.Failure($"rowSize {rowSize} < 8");
                }

                currentDp = Ru32(page, rowStart);
                int dataLen = (int)Ru32(page, rowStart + 4);
                int dataStart = rowStart + 8;
                int availableData = Math.Min(dataLen, rowSize - 8);

                if (availableData > 0 && dataStart + availableData <= _pgSz)
                {
                    var chunk = new byte[availableData];
                    Buffer.BlockCopy(page, dataStart, chunk, 0, availableData);
                    chunks.Add(chunk);
                    totalLen += availableData;
                }
            }

            if (chunks.Count == 0)
            {
                return LvalChainResult.Failure("no chunks read");
            }

            if (chunks.Count == 1)
            {
                return LvalChainResult.Success(chunks[0]);
            }

            int finalLen = Math.Min(totalLen, maxLen);
            var result = new byte[finalLen];
            int pos = 0;
            foreach (byte[] chunk in chunks)
            {
                int copyLen = Math.Min(chunk.Length, finalLen - pos);
                Buffer.BlockCopy(chunk, 0, result, pos, copyLen);
                pos += copyLen;
                if (pos >= finalLen)
                {
                    break;
                }
            }

            return LvalChainResult.Success(result);
        }
        catch (ArgumentException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
        catch (IndexOutOfRangeException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
        catch (OverflowException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
        catch (IOException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
    }

    private async ValueTask<string> ReadLongValueAsync(byte[] row, int start, int len, bool isOle, CancellationToken cancellationToken)
    {
        if (len < 12)
        {
            return isOle ? "(OLE)" : "(memo)";
        }

        byte bitmask = row[start + 3];
        int memoLen = row[start] | (row[start + 1] << 8) | (row[start + 2] << 16);

        if ((bitmask & 0x80) != 0)
        {
            int memoStart = start + 12;
            if (memoStart + memoLen > row.Length)
            {
                memoLen = row.Length - memoStart;
            }

            if (memoLen <= 0)
            {
                return string.Empty;
            }

            if (isOle)
            {
                return TryDecodeOleObject(row, memoStart, memoLen)
                    ?? "data:application/octet-stream;base64," + Convert.ToBase64String(row, memoStart, memoLen);
            }

            return _jet4 ? DecodeJet4Text(row, memoStart, memoLen)
                         : _ansiEncoding.GetString(row, memoStart, memoLen);
        }

        if ((bitmask & 0x40) != 0)
        {
            uint lvalDp = Ru32(row, start + 4);
            byte[]? lvalData = await ReadLvalBytesAsync(lvalDp, memoLen, cancellationToken).ConfigureAwait(false);

            if (lvalData != null)
            {
                if (isOle)
                {
                    return TryDecodeOleObject(lvalData, 0, lvalData.Length)
                        ?? "data:application/octet-stream;base64," + Convert.ToBase64String(lvalData);
                }

                return _jet4 ? DecodeJet4Text(lvalData, 0, lvalData.Length)
                             : _ansiEncoding.GetString(lvalData);
            }

            return isOle ? "(OLE)" : "(memo on LVAL page)";
        }

        uint chainDp = Ru32(row, start + 4);
        LvalChainResult chain = await ReadLvalChainAsync(chainDp, memoLen, cancellationToken).ConfigureAwait(false);

        if (chain.Data != null)
        {
            if (isOle)
            {
                return TryDecodeOleObject(chain.Data, 0, chain.Data.Length)
                    ?? "data:application/octet-stream;base64," + Convert.ToBase64String(chain.Data);
            }

            return _jet4 ? DecodeJet4Text(chain.Data, 0, chain.Data.Length)
                         : _ansiEncoding.GetString(chain.Data);
        }

        return isOle ? $"(OLE chain error: {chain.Error})" : $"(memo chain error: {chain.Error})";
    }

    private string GetLockFilePath()
    {
        string ext = Path.GetExtension(_path);
        string lockExt = ext.Equals(".accdb", StringComparison.OrdinalIgnoreCase) ? ".laccdb" : ".ldb";
        return Path.ChangeExtension(_path, lockExt);
    }

    private void CreateLockFile()
    {
        string lockPath = GetLockFilePath();
        try
        {
            using var fs = new FileStream(lockPath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        }
        catch (IOException ex)
        {
            // Best-effort: if another process holds the lock, continue without it.
            System.Diagnostics.Trace.WriteLine($"[AccessReader] Best-effort lock-file suppression in CreateLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
        catch (UnauthorizedAccessException ex)
        {
            // Best-effort: if we lack permission, continue without it.
            System.Diagnostics.Trace.WriteLine($"[AccessReader] Best-effort lock-file suppression in CreateLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
    }

    private void DeleteLockFile()
    {
        string lockPath = GetLockFilePath();
        try
        {
            File.Delete(lockPath);
        }
        catch (IOException ex)
        {
            // Best-effort: file may be held by another process.
            System.Diagnostics.Trace.WriteLine($"[AccessReader] Best-effort lock-file suppression in DeleteLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
        catch (UnauthorizedAccessException ex)
        {
            // Best-effort: we may lack permission.
            System.Diagnostics.Trace.WriteLine($"[AccessReader] Best-effort lock-file suppression in DeleteLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
    }
}
