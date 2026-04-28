namespace JetDatabaseWriter.Core;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Collections;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;

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
    private readonly object _cacheLock = new();
    private readonly object _catalogLock = new();
    private readonly bool _useLockFile;
    private readonly bool _strictParsing;
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
        : base(stream, hdr, path)
    {
        Guard.NotNull(options, nameof(options));

        _useLockFile = options.UseLockFile && !string.IsNullOrEmpty(path);
        _strictParsing = options.StrictParsing;
        LinkedSourcePathValidator = options.LinkedSourcePathValidator;
        LinkedSourcePathAllowlist = LinkedTableManager.NormalizeAllowlist(options.LinkedSourcePathAllowlist, path);
        LinkedSourceOpenOptions = LinkedTableManager.CreateLinkedSourceOpenOptions(options, LinkedSourcePathAllowlist, LinkedSourcePathValidator);
        var password = LinkedSourceOpenOptions.Password;

        DiagnosticsEnabled = options.DiagnosticsEnabled;
        PageCacheSize = options.PageCacheSize;
        ParallelPageReadsEnabled = options.ParallelPageReadsEnabled;

        bool isAccdbCfbEncrypted = EncryptionManager.IsCompoundFileEncrypted(hdr);
        (_pageKeys.Rc4DbKey, _pageKeys.AesPageKey) =
            EncryptionManager.ResolveReaderPageKeys(hdr, _format, isAccdbCfbEncrypted, password);

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
            ValidateDatabaseFormat();
        }

        if (_useLockFile)
        {
            LockFileManager.Create(_path, nameof(AccessReader));
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

    /// <summary>Gets the absolute path of the database backing this reader, or empty when opened from a stream. Used by <see cref="Internal.LinkedTableManager"/> to anchor relative source paths.</summary>
    internal string HostDatabasePath => _path;

    /// <summary>Gets the cached options used to re-open linked-source databases referenced by this reader.</summary>
    internal AccessReaderOptions LinkedSourceOpenOptions { get; }

    /// <summary>Gets the normalized allowlist of directories that linked-source paths must reside under (empty allows any directory).</summary>
    internal string[] LinkedSourcePathAllowlist { get; }

    /// <summary>Gets the optional callback that approves linked-source paths before opening.</summary>
    internal Func<LinkedTableInfo, string, bool>? LinkedSourcePathValidator { get; }

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

            // Office Crypto API ("Agile") encryption: the file is a real OLE
            // compound document with EncryptionInfo + EncryptedPackage streams.
            // EncryptionManager handles detection, password verification, and
            // package decryption; on success we re-enter on the inner ACCDB
            // bytes.
            byte[]? decryptedAgile = await EncryptionManager
                .TryDecryptAgileCompoundFileAsync(wrapped, header, options.Password, cancellationToken)
                .ConfigureAwait(false);
            if (decryptedAgile != null)
            {
                // Always release the source-wrapper now: when leaveOpen is
                // true wrapped is a NonClosingStreamWrapper that does not
                // close the user stream, otherwise it is the user stream we
                // own per the leaveOpen=false contract.
                await wrapped.DisposeAsync().ConfigureAwait(false);

                MemoryStream? inner = null;
                try
                {
                    inner = new MemoryStream(decryptedAgile, writable: false);
                    byte[] innerHeader = await ReadHeaderAsync(inner, cancellationToken).ConfigureAwait(false);
                    var reader = new AccessReader(string.Empty, options, inner, innerHeader);
                    inner = null;
                    return reader;
                }
                finally
                {
                    if (inner != null)
                    {
                        await inner.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }

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
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
        return await LinkedTableManager.GetLinkedTablesAsync(this, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<List<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
    public async IAsyncEnumerable<object[]> Rows(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            LinkedTableInfo? link = await LinkedTableManager.FindLinkedTableAsync(this, tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(this, link, cancellationToken).ConfigureAwait(false);
                await foreach (object[] row in source.Rows(link.ForeignName, progress, cancellationToken).ConfigureAwait(false))
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
                yield return ConvertRowToTyped(row, td.Columns, tableName, complexData, _strictParsing);
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<T> Rows<T>(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        List<ColumnMetadata> meta = await GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        var headers = meta.ConvertAll(m => m.Name);
        var index = RowMapper<T>.BuildIndex(headers);

        await foreach (object[] row in Rows(tableName, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return RowMapper<T>.Map(row, index);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<string[]> RowsAsStrings(
        string tableName,
        IProgress<long>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            LinkedTableInfo? link = await LinkedTableManager.FindLinkedTableAsync(this, tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(this, link, cancellationToken).ConfigureAwait(false);
                await foreach (string[] row in source.RowsAsStrings(link.ForeignName, progress, cancellationToken).ConfigureAwait(false))
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
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            LinkedTableInfo? link = await LinkedTableManager.FindLinkedTableAsync(this, tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(this, link, cancellationToken).ConfigureAwait(false);
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

        ColumnPropertyBlock? properties = await ReadLvPropForTableAsync(
            resolved.Value.Entry.TDefPage, cancellationToken).ConfigureAwait(false);

        return resolved.Value.Td.Columns.Select((col, index) =>
        {
            ColumnPropertyTarget? target = properties?.FindTarget(col.Name);
            return new ColumnMetadata
            {
                Name = col.Name,
                TypeName = (col.Type == T_COMPLEX && complexSubtypes.TryGetValue(col.Name, out string? subtype))
                    ? subtype
                    : ResolveTypeName(col),
                ClrType = ResolveClrType(col),
                MaxLength = col.Size > 0 ? col.Size : null,
                IsNullable = (col.Flags & 0x02) != 0,
                IsFixedLength = col.IsFixed,
                IsHyperlink = IsHyperlinkColumn(col),
                Ordinal = index,
                Size = SizeForColumn(col),
                DefaultValueExpression = target?.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, _format),
                ValidationRuleExpression = target?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, _format),
                ValidationText = target?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, _format),
                Description = target?.GetTextValue(Constants.ColumnPropertyNames.Description, _format),
                NumericPrecision = col.NumericPrecision,
                NumericScale = col.NumericScale,
            };
        }).ToList();
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        byte[]? td = await ReadTDefBytesAsync(resolved.Value.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null || td.Length < _tdBlockEnd)
        {
            return [];
        }

        return ParseIndexMetadata(td, resolved.Value.Td.Columns);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        // Complex columns are an Access 2007+ ACE feature. Older formats never carry them.
        if (_format == DatabaseFormat.Jet3Mdb)
        {
            return [];
        }

        var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        // Walk the parent TDEF column descriptors to extract per-column ComplexID
        // (the 4-byte misc/misc_ext slot, only meaningful when col_type is 0x11/0x12).
        byte[]? td = await ReadTDefBytesAsync(resolved.Value.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null)
        {
            return [];
        }

        int numCols = Ru16(td, _tdNumCols);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numRealIdx < 0 || numRealIdx > 1000)
        {
            numRealIdx = 0;
        }

        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);

        var byComplexId = new Dictionary<int, (string Name, byte Type)>();
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * _colDescSz);
            if (o + _colDescSz > td.Length)
            {
                break;
            }

            byte type = td[o + _colTypeOff];
            if (type != T_COMPLEX && type != T_ATTACHMENT)
            {
                continue;
            }

            int complexId = Ri32(td, o + _colMiscOff);
            if (complexId <= 0)
            {
                continue;
            }

            int colNum = Ru16(td, o + _colNumOff);
            ColumnInfo? info = resolved.Value.Td.Columns.Find(c => c.ColNum == colNum);
            string name = info?.Name ?? string.Empty;
            byComplexId[complexId] = (name, type);
        }

        if (byComplexId.Count == 0)
        {
            return [];
        }

        return await JoinComplexColumnsAsync(byComplexId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        cancellationToken.ThrowIfCancellationRequested();

        IReadOnlyList<ComplexColumnInfo> complex = await GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
        ComplexColumnInfo? info = null;
        foreach (ComplexColumnInfo c in complex)
        {
            if (string.Equals(c.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
            {
                info = c;
                break;
            }
        }

        if (info == null || string.IsNullOrEmpty(info.FlatTableName))
        {
            return [];
        }

        DataTable flat = await ReadDataTableAsync(info.FlatTableName, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (flat.Rows.Count == 0)
        {
            return [];
        }

        int idxFk = FindFlatLongFkIndex(flat);
        int idxFileURL = flat.Columns.IndexOf("FileURL");
        int idxFileName = flat.Columns.IndexOf("FileName");
        int idxFileType = flat.Columns.IndexOf("FileType");
        int idxFileTime = flat.Columns.IndexOf("FileTimeStamp");
        int idxFileData = flat.Columns.IndexOf("FileData");

        var result = new List<AttachmentRecord>(flat.Rows.Count);
        foreach (DataRow r in flat.Rows)
        {
            int fk = idxFk >= 0 && r[idxFk] is not DBNull ? Convert.ToInt32(r[idxFk], CultureInfo.InvariantCulture) : 0;
            byte[] rawData = ExtractOleBytes(idxFileData >= 0 ? r[idxFileData] : null);
            byte[] decoded = rawData;
            string ext = idxFileType >= 0 && r[idxFileType] is not DBNull ? Convert.ToString(r[idxFileType], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty;
            if (rawData.Length > 0 && AttachmentWrapper.TryDecode(rawData, out string decodedExt, out byte[] payload))
            {
                decoded = payload;
                if (string.IsNullOrEmpty(ext))
                {
                    ext = decodedExt;
                }
            }

            result.Add(new AttachmentRecord
            {
                ConceptualTableId = fk,
                FileName = idxFileName >= 0 && r[idxFileName] is not DBNull ? Convert.ToString(r[idxFileName], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty,
                FileType = ext,
                FileURL = idxFileURL >= 0 && r[idxFileURL] is not DBNull ? Convert.ToString(r[idxFileURL], CultureInfo.InvariantCulture) : null,
                FileTimeStamp = idxFileTime >= 0 && r[idxFileTime] is DateTime dt ? dt : null,
                FileData = decoded,
            });
        }

        return result;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<(int ConceptualTableId, object? Value)>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        cancellationToken.ThrowIfCancellationRequested();

        IReadOnlyList<ComplexColumnInfo> complex = await GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
        ComplexColumnInfo? info = null;
        foreach (ComplexColumnInfo c in complex)
        {
            if (string.Equals(c.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
            {
                info = c;
                break;
            }
        }

        if (info == null || string.IsNullOrEmpty(info.FlatTableName))
        {
            return [];
        }

        DataTable flat = await ReadDataTableAsync(info.FlatTableName, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (flat.Rows.Count == 0)
        {
            return [];
        }

        int idxFk = FindFlatLongFkIndex(flat);
        int idxValue = flat.Columns.IndexOf("value");
        if (idxValue < 0)
        {
            // Fallback: first non-FK column.
            for (int i = 0; i < flat.Columns.Count; i++)
            {
                if (i != idxFk)
                {
                    idxValue = i;
                    break;
                }
            }
        }

        var result = new List<(int, object?)>(flat.Rows.Count);
        foreach (DataRow r in flat.Rows)
        {
            int fk = idxFk >= 0 && r[idxFk] is not DBNull ? Convert.ToInt32(r[idxFk], CultureInfo.InvariantCulture) : 0;
            object? value = idxValue >= 0 && r[idxValue] is not DBNull ? r[idxValue] : null;
            result.Add((fk, value));
        }

        return result;
    }

    /// <summary>
    /// Coerces an OLE column cell to its raw bytes. <see cref="ReadDataTableAsync"/>
    /// surfaces OLE values either as a byte array (when the typed reader can recover
    /// them directly) or as <c>"data:...;base64,..."</c> data-URI strings; both
    /// shapes are handled here.
    /// </summary>
    private static byte[] ExtractOleBytes(object? cell)
    {
        if (cell is null or DBNull)
        {
            return [];
        }

        if (cell is byte[] b)
        {
            return b;
        }

        if (cell is string s && s.StartsWith("data:", StringComparison.Ordinal))
        {
            int comma = s.IndexOf(',', StringComparison.Ordinal);
            if (comma >= 0 && comma + 1 < s.Length)
            {
                try
                {
                    return Convert.FromBase64String(s.Substring(comma + 1));
                }
                catch (FormatException)
                {
                    return [];
                }
            }
        }

        return [];
    }

    private static int FindFlatLongFkIndex(DataTable flat)
    {
        // Prefer the conventional `_<userColumnName>` FK column (the
        // flat-table emitter and Access-authored fixtures both use this naming).
        for (int i = 0; i < flat.Columns.Count; i++)
        {
            DataColumn c = flat.Columns[i];
            if (c.DataType == typeof(int) && c.ColumnName.StartsWith('_'))
            {
                return i;
            }
        }

        for (int i = 0; i < flat.Columns.Count; i++)
        {
            if (flat.Columns[i].DataType == typeof(int))
            {
                return i;
            }
        }

        return -1;
    }

    private static ComplexColumnKind ClassifyComplexKind(string complexTypeName)
    {
        if (string.IsNullOrEmpty(complexTypeName))
        {
            return ComplexColumnKind.Unknown;
        }

        if (complexTypeName.Equals(Constants.ComplexTypeNames.Attachment, StringComparison.OrdinalIgnoreCase))
        {
            return ComplexColumnKind.Attachment;
        }

        // Memo + datetime "version" template — Access surfaces this via "Append Only" memos.
        // No probe-confirmed template name yet; classify by primitive-template prefix below
        // when present, otherwise fall through to MultiValue / Unknown.
        if (complexTypeName.StartsWith(Constants.ComplexTypeNames.Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return ComplexColumnKind.MultiValue;
        }

        return ComplexColumnKind.Unknown;
    }

    private List<IndexMetadata> ParseIndexMetadata(byte[] td, List<ColumnInfo> columns)
    {
        int numCols = Ru16(td, _tdNumCols);
        int numIdx = Ri32(td, _tdNumCols + 2);
        int numRealIdx = Ri32(td, _tdNumRealIdx);

        // Defensive bounds: corrupt TDEFs can report absurd counts.
        if (numIdx <= 0 || numIdx > 1000)
        {
            return [];
        }

        if (numRealIdx < 0 || numRealIdx > 1000)
        {
            numRealIdx = 0;
        }

        bool jet4 = _format != DatabaseFormat.Jet3Mdb;
        int realIdxPhysSz = jet4 ? 52 : 39;
        int logIdxEntrySz = jet4 ? 28 : 20;

        // Section walk mirrors AccessBase.ReadTableDefAsync and FormatProbe.
        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int namesStart = colStart + (numCols * _colDescSz);

        // Walk column-name length-prefix block to find where it ends.
        int pos = namesStart;
        for (int i = 0; i < numCols; i++)
        {
            if (ReadColumnName(td, ref pos, out _) < 0)
            {
                return [];
            }
        }

        int realIdxDescStart = pos;
        int logicalIdxStart = realIdxDescStart + (numRealIdx * realIdxPhysSz);
        int logicalIdxNamesStart = logicalIdxStart + (numIdx * logIdxEntrySz);

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
            if (ReadColumnName(td, ref npos, out string n) < 0)
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
            int entryStart = logicalIdxStart + (i * logIdxEntrySz);
            if (entryStart + logIdxEntrySz > td.Length)
            {
                break;
            }

            int indexNum = jet4 ? Ri32(td, entryStart + 4) : Ri32(td, entryStart + 0);
            int realIdxNum = jet4 ? Ri32(td, entryStart + 8) : Ri32(td, entryStart + 4);
            int relIdxNum = jet4 ? Ri32(td, entryStart + 13) : Ri32(td, entryStart + 9);
            int relTblPage = jet4 ? Ri32(td, entryStart + 17) : Ri32(td, entryStart + 13);
            byte cascadeUps = jet4 ? td[entryStart + 21] : td[entryStart + 17];
            byte cascadeDels = jet4 ? td[entryStart + 22] : td[entryStart + 18];
            byte indexType = jet4 ? td[entryStart + 23] : td[entryStart + 19];

            // Read the col_map for the backing real-idx entry to recover key columns.
            var keyColumns = new List<IndexColumnReference>();
            byte flags = 0x00;
            if (numRealIdx > 0 && realIdxNum >= 0 && realIdxNum < numRealIdx)
            {
                int physStart = realIdxDescStart + (realIdxNum * realIdxPhysSz);
                if (physStart + realIdxPhysSz <= td.Length)
                {
                    if (jet4)
                    {
                        // 10 col_map slots: each {col_num(2), col_order(1)}.
                        for (int slot = 0; slot < 10; slot++)
                        {
                            int so = physStart + 4 + (slot * 3);
                            int cn = Ru16(td, so);
                            byte order = td[so + 2];
                            if (cn == 0xFFFF)
                            {
                                continue;
                            }

                            keyColumns.Add(new IndexColumnReference
                            {
                                Name = colNumToName.TryGetValue(cn, out string? n) ? n : string.Empty,
                                ColumnNumber = cn,
                                IsAscending = order == 0x01,
                            });
                        }

                        flags = td[physStart + 42];
                    }
                    else
                    {
                        // Jet3 layout: HACKING.md is incomplete; emit a best-effort col_map walk
                        // limited to the documented 10-slot block at offset 4 (still 3 bytes/slot).
                        for (int slot = 0; slot < 10; slot++)
                        {
                            int so = physStart + 4 + (slot * 3);
                            if (so + 3 > physStart + realIdxPhysSz)
                            {
                                break;
                            }

                            int cn = Ru16(td, so);
                            byte order = td[so + 2];
                            if (cn == 0xFFFF)
                            {
                                continue;
                            }

                            keyColumns.Add(new IndexColumnReference
                            {
                                Name = colNumToName.TryGetValue(cn, out string? n) ? n : string.Empty,
                                ColumnNumber = cn,
                                IsAscending = order == 0x01,
                            });
                        }
                    }
                }
            }

            IndexKind kind = indexType switch
            {
                0x01 => IndexKind.PrimaryKey,
                0x02 => IndexKind.ForeignKey,
                _ => IndexKind.Normal,
            };

            result.Add(new IndexMetadata
            {
                Name = names[i],
                IndexNumber = indexNum,
                RealIndexNumber = realIdxNum,
                Kind = kind,
                IsUnique = (flags & 0x01) != 0,
                IgnoreNulls = (flags & 0x02) != 0,
                IsRequired = (flags & 0x08) != 0,
                IsForeignKey = relIdxNum != -1,
                RelatedTablePage = relIdxNum != -1 ? relTblPage : 0,
                CascadeUpdates = cascadeUps != 0,
                CascadeDeletes = cascadeDels != 0,
                Columns = keyColumns,
            });
        }

        return result;
    }

    private async ValueTask<IReadOnlyList<ComplexColumnInfo>> JoinComplexColumnsAsync(
        Dictionary<int, (string Name, byte Type)> byComplexId,
        CancellationToken cancellationToken)
    {
        long msysTdef = await FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysTdef <= 0)
        {
            return [];
        }

        TableDef? msys = await ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return [];
        }

        int idxColumnName = msys.FindColumnIndex("ColumnName");
        int idxComplexId = msys.FindColumnIndex("ComplexID");
        int idxFlatTable = msys.FindColumnIndex("FlatTableID");
        int idxConceptualTable = msys.FindColumnIndex("ConceptualTableID");
        int idxComplexType = msys.FindColumnIndex("ComplexTypeObjectID");

        if (idxComplexId < 0)
        {
            return [];
        }

        // Pre-resolve flat-table and complex-type id → name via the MSysObjects catalog
        // so each complex column row can be classified without a per-row catalog scan.
        Dictionary<long, string> objectNamesById = await BuildObjectNameLookupAsync(cancellationToken).ConfigureAwait(false);

        var result = new List<ComplexColumnInfo>(byComplexId.Count);
        await foreach (List<string> row in EnumerateRowsForTdefAsync(msysTdef, msys, cancellationToken).ConfigureAwait(false))
        {
            if (!int.TryParse(SafeGet(row, idxComplexId), out int complexId))
            {
                continue;
            }

            if (!byComplexId.TryGetValue(complexId, out var parent))
            {
                continue;
            }

            int flatId = idxFlatTable >= 0 && int.TryParse(SafeGet(row, idxFlatTable), out int fid) ? fid : 0;
            int conceptualId = idxConceptualTable >= 0 && int.TryParse(SafeGet(row, idxConceptualTable), out int cid) ? cid : 0;
            int typeObjectId = idxComplexType >= 0 && int.TryParse(SafeGet(row, idxComplexType), out int tid) ? tid : 0;

            string columnName = idxColumnName >= 0 ? SafeGet(row, idxColumnName) : parent.Name;
            string flatName = flatId != 0 && objectNamesById.TryGetValue(flatId, out string? fn) ? fn : string.Empty;
            string typeName = typeObjectId != 0 && objectNamesById.TryGetValue(typeObjectId, out string? tn) ? tn : string.Empty;

            result.Add(new ComplexColumnInfo
            {
                ColumnName = string.IsNullOrEmpty(columnName) ? parent.Name : columnName,
                ComplexId = complexId,
                Kind = ClassifyComplexKind(typeName),
                FlatTableName = flatName,
                FlatTableId = flatId,
                ConceptualTableId = conceptualId,
                ComplexTypeObjectId = typeObjectId,
                ComplexTypeName = typeName,
            });
        }

        return result;
    }

    private async ValueTask<Dictionary<long, string>> BuildObjectNameLookupAsync(CancellationToken cancellationToken)
    {
        var map = new Dictionary<long, string>();

        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return map;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxName = msys.FindColumnIndex("Name");
        if (idxId < 0 || idxName < 0)
        {
            return map;
        }

        await foreach (List<string> row in EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            if (long.TryParse(SafeGet(row, idxId), out long id))
            {
                map[id] = SafeGet(row, idxName);
            }
        }

        return map;
    }

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <returns>A list of user table names.</returns>
    public async ValueTask<List<string>> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
            LinkedTableInfo? link = await LinkedTableManager.FindLinkedTableAsync(this, tableName, cancellationToken).ConfigureAwait(false);
            if (link != null)
            {
                await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(this, link, cancellationToken).ConfigureAwait(false);
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
                _ = dt.Columns.Add(col.Name, ResolveClrType(col));
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
                    _ = dt.Rows.Add(ConvertRowToTyped(row, td.Columns, tableName, complexData, _strictParsing));
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
        Guard.ThrowIfDisposed(_disposed, this);
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        List<ColumnMetadata> meta = await GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        var headers = meta.ConvertAll(m => m.Name);
        var index = RowMapper<T>.BuildIndex(headers);
        var items = new List<T>();
        int count = 0;

        await foreach (object[] row in Rows(tableName, cancellationToken: cancellationToken).ConfigureAwait(false))
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
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
            Version = _format == DatabaseFormat.Jet3Mdb ? "Jet3" : "Jet4/ACE",
            Format = _format,
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
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
            if (_useLockFile)
            {
                LockFileManager.Delete(_path, nameof(AccessReader));
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

    /// <summary>
    /// Returns true when an MSysComplexColumns row's ConceptualTableID column refers to
    /// the user table identified by <paramref name="targetTdefPage"/> (or, as a fallback,
    /// matches <paramref name="tableName"/> when the cell holds a name rather than an ID).
    /// When <paramref name="targetTdefPage"/> is 0, no filtering is applied.
    /// </summary>
    private static bool ConceptualTableMatches(string tableIdStr, long targetTdefPage, string? tableName)
    {
        if (targetTdefPage <= 0)
        {
            return true;
        }

        if (long.TryParse(tableIdStr, out long tableId))
        {
            return (tableId & 0x00FFFFFFL) == targetTdefPage;
        }

        return tableName != null && string.Equals(tableIdStr, tableName, StringComparison.OrdinalIgnoreCase);
    }

    private static FileStream CreateStream(string path, AccessReaderOptions options)
    {
        return new FileStream(path, FileMode.Open, options.FileAccess, options.FileShare, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
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
        T_DATETIMEEXT => "Date/Time Extended",
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
        T_BINARY => typeof(byte[]),
        T_OLE => typeof(byte[]),
        T_ATTACHMENT => typeof(byte[]),
        T_COMPLEX => typeof(byte[]),
        _ => typeof(string),
    };

    /// <summary>
    /// Returns <see langword="true"/> when the column is a MEMO whose TDEF flag
    /// byte has Jackcess <c>HYPERLINK_FLAG_MASK = 0x80</c> set — Microsoft Access
    /// surfaces such columns through the Hyperlink data-format affordance.
    /// See <c>docs/design/hyperlink-format-notes.md</c>.
    /// </summary>
    internal static bool IsHyperlinkColumn(ColumnInfo col) =>
        col.Type == T_MEMO && (col.Flags & 0x80) != 0;

    private static Type ResolveClrType(ColumnInfo col) =>
        IsHyperlinkColumn(col) ? typeof(Hyperlink) : TypeCodeToClrType(col.Type);

    private static string ResolveTypeName(ColumnInfo col) =>
        IsHyperlinkColumn(col) ? "Hyperlink" : TypeCodeToName(col.Type);

    private static string ReadFixed(byte[] row, int start, ColumnInfo col, int sz)
    {
        // T_NUMERIC overflow / scale>28 is surfaced as JetLimitationException on
        // the reader path (callers rely on this contract). The shared
        // AccessBase.ReadFixedString silently returns empty in those cases, so
        // dispatch T_NUMERIC to the reader-local copy.
        return col.Type == T_NUMERIC
            ? ReadNumeric(row, start)
            : ReadFixedString(row, start, col.Type, sz);
    }

    // Magic-byte signatures for OLE-wrapped file payloads. Ordered so the
    // longest / most specific patterns are checked first when ambiguous.
    private static ReadOnlySpan<byte> MagicJpeg => [0xFF, 0xD8, 0xFF];

    private static ReadOnlySpan<byte> MagicPng => [0x89, 0x50, 0x4E, 0x47];

    private static ReadOnlySpan<byte> MagicGif => [0x47, 0x49, 0x46];

    private static ReadOnlySpan<byte> MagicBmp => [0x42, 0x4D];

    private static ReadOnlySpan<byte> MagicPdf => [0x25, 0x50, 0x44, 0x46];

    private static ReadOnlySpan<byte> MagicZip => [0x50, 0x4B, 0x03, 0x04];

    private static ReadOnlySpan<byte> MagicOleCompound => [0xD0, 0xCF, 0x11, 0xE0];

    private static ReadOnlySpan<byte> MagicRtf => [0x7B, 0x5C, 0x72, 0x74];

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
            ReadOnlySpan<byte> window = b.AsSpan(i, scanEnd - i);
            int fileLen = start + len - i;

            // ── Images ──
            if (window.StartsWith(MagicJpeg))
            {
                return "data:image/jpeg;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            if (window.StartsWith(MagicPng))
            {
                return "data:image/png;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            if (window.StartsWith(MagicGif))
            {
                return "data:image/gif;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            if (window.StartsWith(MagicBmp))
            {
                return "data:image/bmp;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // ── Documents ──
            if (window.StartsWith(MagicPdf))
            {
                return "data:application/pdf;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // ZIP (also DOCX/XLSX/PPTX). For simplicity, return generic zip MIME.
            if (window.StartsWith(MagicZip))
            {
                return "data:application/zip;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // DOC (Word 97-2003): OLE compound file.
            if (window.StartsWith(MagicOleCompound))
            {
                return "data:application/msword;base64," + Convert.ToBase64String(b, i, fileLen);
            }

            // RTF: {\rt
            if (window.StartsWith(MagicRtf))
            {
                return "data:application/rtf;base64," + Convert.ToBase64String(b, i, fileLen);
            }
        }

        return null;
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
            return new decimal((int)lo, (int)mid, (int)hi, neg, scale).ToString("G", CultureInfo.InvariantCulture);
        }
        catch (OverflowException ex)
        {
            throw new JetLimitationException(
                $"T_NUMERIC value overflow (hi=0x{hi:X8}, mid=0x{mid:X8}, lo=0x{lo:X8}, scale={scale})", ex);
        }
    }

    private static object[] ConvertRowToTyped(List<string> row, List<ColumnInfo> columns, string? tableName = null, Dictionary<int, Dictionary<int, byte[]>>? complexData = null, bool strictParsing = true)
    {
        var typedRow = new object[row.Count];
        for (int i = 0; i < row.Count && i < columns.Count; i++)
        {
            string raw = row[i];
            ColumnInfo col = columns[i];

            // Resolve complex-field attachments using preloaded complex data (keyed by col ordinal).
            // The variable slot holds a marker "__CX:<complex_id>__" encoding the FK
            // that joins this row to the hidden flat data table.
            if (col.Type == T_COMPLEX || col.Type == T_ATTACHMENT)
            {
                if (complexData != null &&
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
                }

                // Complex slot with no resolvable child data (e.g. multi-value
                // columns whose flat-table loader is not wired through, or
                // attachment slots whose ConceptualTableID has no live flat
                // rows). Surface as DBNull rather than attempting to parse the
                // "__CX:N__" marker as the column's nominal byte[] CLR type.
                typedRow[i] = DBNull.Value;
                continue;
            }

            typedRow[i] = TypedValueParser.ParseValue(raw, ResolveClrType(col), strictParsing);
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

    /// <summary>
    /// Strips the 1-byte compression flag from raw attachment FileData bytes,
    /// decompressing if the flag indicates deflate-compressed content.
    /// </summary>
    private static byte[] DecodeAttachmentFileData(byte[] raw) => raw.Length <= 1 ? raw : raw[0] switch
    {
        0x01 => DecompressAttachmentData(raw, 1),
        0x00 => raw.AsSpan(1).ToArray(),
        _ => raw,
    };

    /// <summary>
    /// Decompresses Access attachment file data using raw Deflate.
    /// Access stores attachment data with a 1-byte compression flag followed by
    /// deflate-compressed content.
    /// </summary>
    private static byte[] DecompressAttachmentData(byte[] data, int offset)
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
    private static byte[] DecodeColumnBytes(string value, byte colType)
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

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    private protected override async ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken)
    {
        if (_catalogCache != null)
        {
            return _catalogCache;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var diag = new System.Text.StringBuilder();
        _ = diag.AppendLine($"JET: {(_format == DatabaseFormat.Jet3Mdb ? "Jet3" : "Jet4/ACE")}  PageSize: {_pgSz}  TotalPages: {_stream.Length / _pgSz}");

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

        int idxId = msys.FindColumnIndex("Id");
        int idxName = msys.FindColumnIndex("Name");
        int idxType = msys.FindColumnIndex("Type");
        int idxFlags = msys.FindColumnIndex("Flags");

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
            var msg = $"File does not have a valid JET magic signature (expected 00 01 00 00, got {magic[0]:X2} {magic[1]:X2} {magic[2]:X2} {magic[3]:X2}).";
            throw new InvalidDataException(msg);
        }
    }

    /// <summary>Reads a page through the cache if enabled (PageCacheSize > 0).</summary>
    private async ValueTask<byte[]> ReadPageCachedAsync(long n, CancellationToken cancellationToken)
    {
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (PageCacheSize < 0)
        {
            return await ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        }

        if (_pageCache == null && PageCacheSize > 0)
        {
            lock (_cacheLock)
            {
                _pageCache ??= new LruCache<long, byte[]>(PageCacheSize);
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

        // Fall back to a system-table lookup (MSysObjects, MSysRelationships, etc.).
        // GetUserTablesAsync filters out rows whose Flags carry SYSTABLE_MASK, so
        // a name match against the catalog scan is needed for those.
        long sysPage = await FindSystemTablePageAsync(
            n => string.Equals(n, tableName, StringComparison.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);
        if (sysPage > 0)
        {
            TableDef? sysTd = await ReadTableDefAsync(sysPage, cancellationToken).ConfigureAwait(false);
            if (sysTd != null && sysTd.Columns.Count > 0)
            {
                return (new CatalogEntry(tableName, sysPage), sysTd);
            }
        }

        return null;
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

        // Pre-parse numCols just for the schema-evolution sanity check; the full
        // layout parse repeats this read but the cost is negligible.
        int rawNumCols = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, rowStart) : page[rowStart];
        if (rawNumCols == 0)
        {
            return null;
        }

        if (td.HasDeletedColumns && rawNumCols > td.Columns.Count)
        {
            throw new JetLimitationException(
                $"Row has {rawNumCols} columns but current schema has {td.Columns.Count} with deleted-column gaps. " +
                "This row predates schema changes and data may be misaligned. " +
                "Solution: Compact & Repair the database in Microsoft Access to rebuild all rows.");
        }

        // Tables with zero variable-length columns omit the var-length
        // metadata entirely (no varLen byte, no jump bytes, no var-offset
        // table, no EOD marker). Detect that and let the layout parser skip
        // the var-area read.
        bool hasVarCols = false;
        for (int ci = 0; ci < td.Columns.Count; ci++)
        {
            if (!td.Columns[ci].IsFixed)
            {
                hasVarCols = true;
                break;
            }
        }

        if (!TryParseRowLayout(page, rowStart, rowSize, hasVarCols, out RowLayout layout))
        {
            return null;
        }

        var result = new List<string>(td.Columns.Count);

        for (int i = 0; i < td.Columns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = td.Columns[i];
            ColumnSlice slice = ResolveColumnSlice(page, rowStart, rowSize, layout, col);

            switch (slice.Kind)
            {
                case ColumnSliceKind.Bool:
                    result.Add(slice.BoolValue ? "True" : "False");
                    break;

                case ColumnSliceKind.Null:
                case ColumnSliceKind.Empty:
                    result.Add(string.Empty);
                    break;

                case ColumnSliceKind.Fixed:
                    result.Add(ReadFixed(page, rowStart + slice.DataStart, col, slice.DataLen));
                    break;

                case ColumnSliceKind.Var:
                    string value = await ReadVarAsync(page, rowStart + slice.DataStart, slice.DataLen, col, cancellationToken).ConfigureAwait(false);
                    result.Add(value);
                    break;

                default:
                    result.Add(string.Empty);
                    break;
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
                    return _format != DatabaseFormat.Jet3Mdb ? DecodeJet4Text(row, start, len)
                                 : _ansiEncoding.GetString(row, start, len);

                case T_BINARY:
                    return ToHexStringNoSeparator(row.AsSpan(start, len));

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
                    return len >= 1 ? row[start].ToString(CultureInfo.InvariantCulture) : string.Empty;
                case T_INT:
                    return len >= 2 ? ((short)Ru16(row, start)).ToString(CultureInfo.InvariantCulture) : string.Empty;
                case T_LONG:
                    return len >= 4 ? Ri32(row, start).ToString(CultureInfo.InvariantCulture) : string.Empty;
                case T_FLOAT:
                    return len >= 4 ? AccessBase.ReadSingleLittleEndian(row.AsSpan(start, 4)).ToString(CultureInfo.InvariantCulture) : string.Empty;
                case T_DOUBLE:
                    return len >= 8 ? AccessBase.ReadDoubleLittleEndian(row.AsSpan(start, 8)).ToString(CultureInfo.InvariantCulture) : string.Empty;
                case T_DATETIME:
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

    /// <summary>
    /// Yields rows from every data page whose owning TDEF page equals <paramref name="tdefPage"/>.
    /// Centralises the common scan-all-pages-and-decode-rows pattern used by catalog/system-table readers.
    /// </summary>
    private async IAsyncEnumerable<List<string>> EnumerateRowsForTdefAsync(
        long tdefPage,
        TableDef td,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
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
                yield return row;
            }
        }
    }

    /// <summary>Loads the MSysObjects TableDef (page 2). Exposed for <see cref="Internal.LinkedTableManager"/>.</summary>
    internal ValueTask<TableDef?> GetMSysObjectsTableDefAsync(CancellationToken cancellationToken) =>
        ReadTableDefAsync(2, cancellationToken);

    /// <summary>Enumerates every row of MSysObjects. Exposed for <see cref="Internal.LinkedTableManager"/>.</summary>
    internal IAsyncEnumerable<List<string>> EnumerateMSysObjectsRowsAsync(TableDef msys, CancellationToken cancellationToken) =>
        EnumerateRowsForTdefAsync(2, msys, cancellationToken);

    /// <summary>
    /// Returns the concatenated TDEF page-chain bytes for <paramref name="tdefPage"/>,
    /// with the 8-byte page header included for the first page and stripped from
    /// continuations (matches <see cref="AccessBase.ReadTDefBytesAsync"/>). Returns
    /// <see langword="null"/> when the page is not a valid TDEF root. Diagnostic-only
    /// helper for the format-probe tool under <c>JetDatabaseWriter.FormatProbe</c>.
    /// </summary>
    internal ValueTask<byte[]?> GetRawTDefBytesAsync(long tdefPage, CancellationToken cancellationToken) =>
        ReadTDefBytesAsync(tdefPage, cancellationToken);

    /// <summary>
    /// Returns a heap-allocated copy of the raw bytes of <paramref name="pageNumber"/>
    /// (post-decryption). Diagnostic-only helper for the format-probe tool under
    /// <c>JetDatabaseWriter.FormatProbe</c>; production code should not call this.
    /// </summary>
    internal async ValueTask<byte[]> GetRawPageBytesAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] pooled = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        var copy = new byte[_pgSz];
        Buffer.BlockCopy(pooled, 0, copy, 0, _pgSz);
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
    internal async ValueTask<ColumnPropertyBlock?> ReadLvPropForTableAsync(long tdefPage, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await GetMSysObjectsTableDefAsync(cancellationToken).ConfigureAwait(false);
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

        await foreach (List<string> row in EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            if (!long.TryParse(SafeGet(row, idxId), out long id))
            {
                continue;
            }

            if ((id & 0x00FFFFFFL) != tdefPage)
            {
                continue;
            }

            byte[]? blob = TryDecodeBase64DataUrl(SafeGet(row, idxLvProp));
            return ColumnPropertyBlock.Parse(blob, _format);
        }

        return null;

        static byte[]? TryDecodeBase64DataUrl(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

            const string Prefix = "data:application/octet-stream;base64,";
            if (!value.StartsWith(Prefix, StringComparison.Ordinal))
            {
                return null;
            }

            try
            {
                return Convert.FromBase64String(value[Prefix.Length..]);
            }
            catch (FormatException)
            {
                return null;
            }
        }
    }

    private async ValueTask<Dictionary<string, string>> ReadComplexColumnSubtypesAsync(string tableName, CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            long tdefPage = await FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
            if (tdefPage <= 0)
            {
                return result;
            }

            TableDef? td = await ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            if (td == null)
            {
                return result;
            }

            int idxCol = td.FindColumnIndex("ColumnName");
            int idxConceptualTable = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ConceptualTableID", StringComparison.OrdinalIgnoreCase)
                || string.Equals(c.Name, "TableName", StringComparison.OrdinalIgnoreCase));

            if (idxCol < 0)
            {
                return result;
            }

            var resolved = await ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            long targetTdefPage = resolved is { } resolvedValue ? resolvedValue.Entry.TDefPage : 0;

            await foreach (List<string> row in EnumerateRowsForTdefAsync(tdefPage, td, cancellationToken).ConfigureAwait(false))
            {
                if (idxConceptualTable >= 0 &&
                    !ConceptualTableMatches(SafeGet(row, idxConceptualTable), targetTdefPage, tableName))
                {
                    continue;
                }

                string colName = SafeGet(row, idxCol);
                result[colName] = "Attachment";
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
    private ValueTask<long> FindSystemTablePageAsync(string name, CancellationToken cancellationToken) =>
        FindSystemTablePageAsync(
            n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase),
            cancellationToken);

    /// <summary>
    /// Finds the TDEF page for the first system table whose name satisfies <paramref name="nameMatches"/>.
    /// Shared by exact-name and suffix lookups against MSysObjects.
    /// </summary>
    private async ValueTask<long> FindSystemTablePageAsync(Predicate<string> nameMatches, CancellationToken cancellationToken)
    {
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
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

        await foreach (List<string> row in EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            string nameStr = SafeGet(row, idxName);
            if (!nameMatches(nameStr))
            {
                continue;
            }

            if (!int.TryParse(SafeGet(row, idxType), out int objType) || (objType != OBJ_TABLE && objType != Constants.SystemObjects.LinkedOdbcType))
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
            long msysTdef = await FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
            if (msysTdef <= 0)
            {
                return 0;
            }

            TableDef? td = await ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false);
            if (td == null)
            {
                return 0;
            }

            int idxCol = td.FindColumnIndex("ColumnName");
            int idxConceptualTable = td.FindColumnIndex("ConceptualTableID");
            int idxFlatTable = td.FindColumnIndex("FlatTableID");

            if (idxCol < 0 || idxFlatTable < 0)
            {
                return 0;
            }

            List<CatalogEntry> tables = await GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
            CatalogEntry entry = tables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
            long targetTdefPage = entry?.TDefPage ?? 0;

            await foreach (List<string> row in EnumerateRowsForTdefAsync(msysTdef, td, cancellationToken).ConfigureAwait(false))
            {
                string colName = SafeGet(row, idxCol);
                if (!string.Equals(colName, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (idxConceptualTable >= 0 &&
                    !ConceptualTableMatches(SafeGet(row, idxConceptualTable), targetTdefPage, tableName: null))
                {
                    continue;
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
                tdefPage = await FindSystemTablePageBySuffixAsync($"_{columnName}", cancellationToken).ConfigureAwait(false);
            }

            TableDef? td = tdefPage > 0 ? await ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false) : null;
            if (td == null)
            {
                return null;
            }

            string fkColName = $"{tableName}_{columnName}";
            int idxFk = td.FindColumnIndex(fkColName);
            if (idxFk < 0)
            {
                idxFk = td.Columns.FindIndex(c => c.Type == T_LONG && !c.Name.StartsWith("Idx", StringComparison.OrdinalIgnoreCase));
            }

            if (idxFk < 0)
            {
                return null;
            }

            int idxFileName = td.FindColumnIndex("FileName");
            int idxFileData = td.FindColumnIndex("FileData");

            var result = new Dictionary<int, byte[]>(capacity: 32);

            await foreach (var row in EnumerateRowsForTdefAsync(tdefPage, td, cancellationToken).ConfigureAwait(false))
            {
                if (!int.TryParse(SafeGet(row, idxFk), out int parentId))
                {
                    continue;
                }

                byte[] fileNameBytes = idxFileName >= 0 && SafeGet(row, idxFileName) is { Length: > 0 } fileName
                    ? Encoding.Unicode.GetBytes(fileName)
                    : [];

                byte[] fileDataBytes = idxFileData >= 0
                    ? DecodeAttachmentFileData(DecodeColumnBytes(SafeGet(row, idxFileData) ?? string.Empty, td.Columns[idxFileData].Type))
                    : [];

                if (fileNameBytes.Length == 0 && fileDataBytes.Length == 0)
                {
                    continue;
                }

                var serialized = new byte[2 + fileNameBytes.Length + fileDataBytes.Length];
                BinaryPrimitives.WriteUInt16LittleEndian(serialized, (ushort)fileNameBytes.Length);
                Buffer.BlockCopy(fileNameBytes, 0, serialized, 2, fileNameBytes.Length);
                Buffer.BlockCopy(fileDataBytes, 0, serialized, 2 + fileNameBytes.Length, fileDataBytes.Length);

                result[parentId] = serialized;
            }

            return result.Count > 0 ? result : null;
        }
        catch (Exception ex) when (ex is InvalidDataException or IndexOutOfRangeException or IOException)
        {
            return null;
        }
    }

    /// <summary>
    /// Finds the TDEF page for the first system table whose name ends with
    /// <paramref name="nameSuffix"/> (case-insensitive). Used to find
    /// <c>f_&lt;GUID&gt;_&lt;columnName&gt;</c> attachment tables.
    /// </summary>
    private ValueTask<long> FindSystemTablePageBySuffixAsync(string nameSuffix, CancellationToken cancellationToken) =>
        FindSystemTablePageAsync(
            n => n != null && n.EndsWith(nameSuffix, StringComparison.OrdinalIgnoreCase),
            cancellationToken);

    // [memo_len: 3 bytes][bitmask: 1 byte][lval_dp: 4 bytes][unknown: 4 bytes]
    // 0x80 = inline data immediately after the 12-byte header
    // 0x40 = single LVAL page:  lval_dp = (page << 8) | row_index
    // 0x00 = chained LVAL pages (not decoded; placeholder returned)

    /// <summary>
    /// Locates a single LVAL row within its data page. Shared by single-page and chained
    /// LVAL readers. lval_dp encoding: upper 24 bits = page number, lower 8 bits = row index.
    /// </summary>
    private async ValueTask<LvalRowLocation> LocateLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        int lvalPage = (int)(lvalDp >> 8);
        int lvalRow = (int)(lvalDp & 0xFF);
        if (lvalPage <= 0)
        {
            return new([], 0, 0, $"invalid page {lvalPage}");
        }

        byte[] page = await ReadPageCachedAsync(lvalPage, cancellationToken).ConfigureAwait(false);
        if (page[0] != 0x01)
        {
            return new(page, 0, 0, $"page {lvalPage} not data page");
        }

        int numRows = Ru16(page, _dpNumRows);
        if (lvalRow >= numRows)
        {
            return new(page, 0, 0, $"row {lvalRow} >= numRows {numRows}");
        }

        int rawOff = Ru16(page, _dpRowsStart + (lvalRow * 2));
        if ((rawOff & 0xC000) != 0)
        {
            return new(page, 0, 0, "deleted/overflow row");
        }

        int rowStart = rawOff & 0x1FFF;
        if (rowStart == 0 || rowStart >= _pgSz)
        {
            return new(page, 0, 0, $"invalid rowStart {rowStart}");
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

        return new(page, rowStart, rowEnd - rowStart + 1, null);
    }

    /// <summary>
    /// Reads <paramref name="maxLen"/> bytes from a single LVAL data page / row.
    /// </summary>
    private async ValueTask<byte[]?> ReadLvalBytesAsync(uint lvalDp, int maxLen, CancellationToken cancellationToken)
    {
        LvalRowLocation loc = await LocateLvalRowAsync(lvalDp, cancellationToken).ConfigureAwait(false);
        int rowSize = Math.Min(loc.Size, maxLen);
        if (loc.Failed || rowSize <= 0)
        {
            return null;
        }

        var data = new byte[rowSize];
        Buffer.BlockCopy(loc.Page, loc.Start, data, 0, rowSize);
        return data;
    }

    /// <summary>
    /// Reads multi-page LVAL chains (bitmask 0x00). Follows LVAL page links until
    /// the entire memo is reconstructed or maxLen is reached.
    /// LVAL page format (mdbtools): [next_page(4)][data_length(4)][data...].
    /// </summary>
    /// <returns>Success: concatenated data bytes from the entire LVAL chain, up to maxLen. Failure: error message.</returns>
    private async ValueTask<LvalChainResult> ReadLvalChainAsync(uint firstLvalDp, int maxLen, CancellationToken cancellationToken)
    {
        var chunks = new List<byte[]>();
        int totalLen = 0;
        uint currentDp = firstLvalDp;
        var seen = new HashSet<uint>();

        try
        {
            while (currentDp != 0 && totalLen < maxLen && seen.Add(currentDp))
            {
                cancellationToken.ThrowIfCancellationRequested();

                LvalRowLocation loc = await LocateLvalRowAsync(currentDp, cancellationToken).ConfigureAwait(false);
                if (loc.Failed)
                {
                    return LvalChainResult.Failure(loc.Error!);
                }

                if (loc.Size < 4)
                {
                    return LvalChainResult.Failure($"rowSize {loc.Size} < 4");
                }

                // LVAL chain row layout (mdbtools): [next_page_row(4)][data...].
                // The chunk length is implicit in the row size, NOT a separate
                // length field. Reading bytes 4..8 as length corrupts both the
                // payload and the length, often producing huge values that then
                // overflow when cast/added.
                currentDp = Ru32(loc.Page, loc.Start);
                int availableData = loc.Size - 4;
                int wantData = Math.Min(availableData, maxLen - totalLen);

                if (wantData > 0 && loc.Start + 4 + wantData <= _pgSz)
                {
                    var chunk = new byte[wantData];
                    Buffer.BlockCopy(loc.Page, loc.Start + 4, chunk, 0, wantData);
                    chunks.Add(chunk);
                    totalLen += wantData;
                }
            }
        }
        catch (IOException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
        catch (OverflowException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }

        return chunks.Count switch
        {
            0 => LvalChainResult.Failure("no chunks read"),
            1 => LvalChainResult.Success(chunks[0]),
            _ => LvalChainResult.Success(Concat(chunks, Math.Min(totalLen, maxLen))),
        };

        static byte[] Concat(List<byte[]> chunks, int finalLen)
        {
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

            return result;
        }
    }

    private async ValueTask<string> ReadLongValueAsync(byte[] row, int start, int len, bool isOle, CancellationToken cancellationToken)
    {
        if (len < 12)
        {
            return isOle ? "(OLE)" : "(memo)";
        }

        byte bitmask = row[start + 3];
        int memoLen = ReadUInt24LittleEndian(row.AsSpan(start, 3));

        switch (bitmask & 0xC0)
        {
            case 0x80:
                // Inline data — the memo/OLE bytes follow the 12-byte header
                // directly within this row, with no LVAL page indirection.
                int memoStart = start + 12;
                int inlineLen = Math.Min(memoLen, row.Length - memoStart);
                return inlineLen <= 0 ? string.Empty : DecodeLongValue(row, memoStart, inlineLen, isOle);

            case 0x40:
                // Single LVAL page — the header stores a pointer (page<<8 | row)
                // to one LVAL page/row that holds the entire memo/OLE payload.
                byte[]? lvalData = await ReadLvalBytesAsync(Ru32(row, start + 4), memoLen, cancellationToken).ConfigureAwait(false);
                return lvalData != null
                    ? DecodeLongValue(lvalData, 0, lvalData.Length, isOle)
                    : (isOle ? "(OLE)" : "(memo on LVAL page)");

            default:
                // Chained LVAL pages — the payload spans multiple LVAL pages
                // linked together, walked by ReadLvalChainAsync.
                LvalChainResult chain = await ReadLvalChainAsync(Ru32(row, start + 4), memoLen, cancellationToken).ConfigureAwait(false);
                return chain.Data != null
                    ? DecodeLongValue(chain.Data, 0, chain.Data.Length, isOle)
                    : (isOle ? $"(OLE chain error: {chain.Error})" : $"(memo chain error: {chain.Error})");
        }
    }

    private string DecodeLongValue(byte[] buffer, int offset, int length, bool isOle)
    {
        if (isOle)
        {
            return TryDecodeOleObject(buffer, offset, length)
                ?? "data:application/octet-stream;base64," + Convert.ToBase64String(buffer, offset, length);
        }

        return _format != DatabaseFormat.Jet3Mdb
            ? DecodeJet4Text(buffer, offset, length)
            : _ansiEncoding.GetString(buffer, offset, length);
    }

    /// <summary>
    /// Result of locating a single LVAL row within its data page.
    /// <see cref="Error"/> is non-null when the row could not be located; otherwise
    /// (<see cref="Page"/>, <see cref="Start"/>, <see cref="Size"/>) describe the row's
    /// in-page slice and are guaranteed to lie within the page buffer.
    /// </summary>
    private readonly record struct LvalRowLocation(byte[] Page, int Start, int Size, string? Error)
    {
        public bool Failed => Error is not null;
    }
}
