namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
    {
        0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
        0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
        0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
        0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
        0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
    };

    private readonly object _cacheLock = new object();
    private readonly object _catalogLock = new object();
    private volatile List<CatalogEntry>? _catalogCache;
    private volatile LruCache<long, byte[]>? _pageCache;
    private long _cacheHits;
    private long _cacheMisses;

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReader"/> class.
    /// Opens <paramref name="path"/> and detects the JET version.
    /// </summary>
    /// <param name="path">The path to the Access database file.</param>
    /// <param name="options">Options for configuring the AccessReader.</param>
    private AccessReader(string path, AccessReaderOptions options)
        : base(new FileStream(path, FileMode.Open, options.FileAccess, options.FileShare))
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotNull(options, nameof(options));

        DiagnosticsEnabled = options.DiagnosticsEnabled;
        PageCacheSize = options.PageCacheSize;
        ParallelPageReadsEnabled = options.ParallelPageReadsEnabled;

        // Offset 0x62: encryption flag — only valid for Jet4 (ver == 1, Access 2000-2003).
        // ACCDB format (ver >= 2, Access 2007+) has completely different header semantics
        // at this offset; applying the Jet4 check to ACCDB files produces false positives.
        // Truly encrypted ACCDB files are detected later when the catalog page is unreadable.
        _ = _fs.Seek(0, SeekOrigin.Begin);
        var hdr = new byte[0x80];
        _ = _fs.Read(hdr, 0, hdr.Length);
        byte ver = hdr[0x14];

        if (_jet4 && ver == 1 && hdr.Length > 0x62)
        {
            byte encFlag = hdr[0x62];

            // bit 0x01 = Office97 password, bit 0x02 = RC4 password
            if ((encFlag & 0x03) != 0)
            {
                if (string.IsNullOrEmpty(options.Password))
                {
                    throw new UnauthorizedAccessException(
                        "This database is encrypted or password-protected. " +
                        "Provide a password via AccessReaderOptions.Password, or " +
                        "remove the password in Microsoft Access (File > Info > Encrypt with Password) and try again.");
                }

                // Verify the provided password against the stored password at 0x42.
                // The stored password is XOR'd with a fixed mask + creation date bytes.
                string storedPassword = DecodeJet4Password(hdr);
                if (!string.Equals(options.Password, storedPassword, StringComparison.Ordinal))
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

        // ACCDB (ver >= 2) encryption detection.
        //
        // Access 2007+ offers two completely different protection models:
        //
        //   1. Genuine AES encryption ("Encrypt with Password" in Access 2007+):
        //      The entire .accdb file is wrapped in an OLE2 Compound File Binary (CFB)
        //      container.  The first four bytes become the CFB magic: D0 CF 11 E0.
        //      Every page is AES-encrypted; the reader cannot decode the content.
        //
        //   2. Legacy password-only (ACE CompactDatabase + ";pwd=..."):
        //      The standard ACCDB header is preserved (first bytes: 00 01 00 00).
        //      Only a password hash is stored in the header; data pages are NOT
        //      encrypted and can be read without a password.
        //
        // Previous approach — checking bit 0x02 of byte 0x62 — is unreliable.
        // Access 16 sets that bit on unencrypted ACCDB files as well as on legacy
        // password-only files, causing false-positive UnauthorizedAccessExceptions.
        // The only reliable indicator of genuine AES encryption is the CFB magic.
        if (_jet4 && ver >= 2 && hdr.Length >= 4 &&
            hdr[0] == 0xD0 && hdr[1] == 0xCF && hdr[2] == 0x11 && hdr[3] == 0xE0)
        {
            // CFB magic: the file is genuinely AES-encrypted.
            // When a password is supplied, attempt to read anyway — the caller has
            // acknowledged the encrypted format and provided credentials.
            // Without a password, throw immediately with a clear message.
            if (string.IsNullOrEmpty(options.Password))
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Access 2007+ AES encryption (Office Crypto API). " +
                    "Full AES page decryption is not yet supported by this library. " +
                    "To open the file, remove the password in Microsoft Access " +
                    "(File > Info > Decrypt Database) and try again.");
            }

            // Password provided: skip format validation (the JET magic at offset 0 was
            // replaced by the CFB magic, but data pages remain valid ACCDB format).
            return;
        }

        if (options.ValidateOnOpen)
        {
            ValidateDatabaseFormat();
        }
    }

    /// <summary>Gets or sets a value indicating whether <see cref="GetUserTables"/> logs verbose hex dumps for debugging. Default: false.</summary>
    public bool DiagnosticsEnabled { get; set; }

    /// <summary>Gets or sets the maximum number of pages to keep in cache. 0 = unlimited, -1 = disabled. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; set; } = 256;

    /// <summary>Gets or sets a value indicating whether <see cref="ReadTable(string, IProgress{int})"/> uses parallel processing for reading multiple pages. Can improve performance for large tables. Default: false.</summary>
    public bool ParallelPageReadsEnabled { get; set; }

    /// <summary>Gets diagnostic output populated after each call to <see cref="ListTables"/>.</summary>
    public string LastDiagnostics { get; private set; } = string.Empty;

    /// <summary>
    /// Opens a JET database file and returns a new AccessReader instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <returns>An AccessReader instance for the specified database.</returns>
    public static AccessReader Open(string path, AccessReaderOptions? options = null)
    {
        Guard.NotNullOrEmpty(path, nameof(path));

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        options ??= new AccessReaderOptions();

        return new AccessReader(path, options);
    }

    /// <summary>
    /// Returns the column headers and up to <paramref name="maxRows"/> rows
    /// from the first user table, plus the table name and total table count.
    /// </summary>
    /// <param name="maxRows">Maximum number of rows to read. Use with large tables to avoid long reads or out-of-memory errors.</param>
    /// <returns>A <see cref="FirstTableResult"/> containing headers, string rows, and schema information.</returns>
    public FirstTableResult ReadFirstTable(int maxRows = 100)
    {
        ThrowIfDisposed();

        var empty = new FirstTableResult
        {
            Headers = new List<string> { "Info" },
            Rows = new List<List<string>> { new List<string> { "No user tables found" } },
            Schema = new List<TableColumn>(),
            TableName = string.Empty,
            TableCount = 0,
        };

        List<CatalogEntry> tables = GetUserTables();
        if (tables.Count == 0)
        {
            return empty;
        }

        CatalogEntry entry = tables[0];
        TableDef? td = ReadTableDef(entry.TDefPage);
        if (td == null || td.Columns.Count == 0)
        {
            return new FirstTableResult
            {
                Headers = new List<string> { "Info" },
                Rows = new List<List<string>> { new List<string> { $"Cannot read TDEF for '{entry.Name}'" } },
                Schema = new List<TableColumn>(),
                TableName = entry.Name,
                TableCount = tables.Count,
            };
        }

        var headers = td.Columns.ConvertAll(c => c.Name);
        var rows = new List<List<string>>();

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            foreach (List<string> row in EnumerateRows(page, td))
            {
                rows.Add(row);
                if (rows.Count >= maxRows)
                {
                    return new FirstTableResult { Headers = headers, Rows = rows, Schema = new List<TableColumn>(), TableName = entry.Name, TableCount = tables.Count };
                }
            }
        }

        return new FirstTableResult
        {
            Headers = headers,
            Rows = rows,
            Schema = new List<TableColumn>(),
            TableName = entry.Name,
            TableCount = tables.Count,
        };
    }

    /// <summary>Returns the names of all user tables in the database.</summary>
    /// <returns>A list of table names.</returns>
    public List<string> ListTables()
    {
        ThrowIfDisposed();
        return GetUserTables().ConvertAll(e => e.Name);
    }

    /// <inheritdoc/>
    public List<LinkedTableInfo> ListLinkedTables()
    {
        ThrowIfDisposed();
        return GetLinkedTables();
    }

    /// <summary>
    /// Returns name, stored row-count, and column-count for every user table.
    /// Calling this instead of <see cref="ListTables"/> avoids a duplicate catalog scan.
    /// </summary>
    /// <returns>A list of <see cref="TableStat"/> with metadata for each user table.</returns>
    public List<TableStat> GetTableStats()
    {
        ThrowIfDisposed();
        var entries = GetUserTables();
        var result = new List<TableStat>(entries.Count);
        foreach (var e in entries)
        {
            TableDef? td = ReadTableDef(e.TDefPage);
            result.Add(new TableStat
            {
                Name = e.Name,
                RowCount = td?.RowCount ?? 0L,
                ColumnCount = td?.Columns.Count ?? 0,
            });
        }

        return result;
    }

    /// <summary>
    /// Returns table metadata as a DataTable with columns: TableName, RowCount, ColumnCount.
    /// Ideal for binding to data grids or exporting to CSV/Excel.
    /// </summary>
    /// <returns>A <see cref="DataTable"/> containing table metadata.</returns>
    public DataTable GetTablesAsDataTable()
    {
        ThrowIfDisposed();
        var dt = new DataTable("Tables");
        _ = dt.Columns.Add("TableName", typeof(string));
        _ = dt.Columns.Add("RowCount", typeof(long));
        _ = dt.Columns.Add("ColumnCount", typeof(int));

        var stats = GetTableStats();
        foreach (TableStat s in stats)
        {
            _ = dt.Rows.Add(s.Name, s.RowCount, s.ColumnCount);
        }

        return dt;
    }

    /// <summary>
    /// Scans all data pages to count live (non-deleted, non-overflow) rows for the specified table.
    /// This is slower than reading the TDEF RowCount (which may be stale), but always accurate.
    /// Use this after many deletes/imports when `Compact and Repair` hasn't been run.
    /// </summary>
    /// <param name="tableName">Name of the table to count rows for (case-insensitive).</param>
    /// <returns>Number of live rows in the specified table.</returns>
    public long GetRealRowCount(string tableName)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        CatalogEntry entry = GetCatalogEntry(tableName);
        if (entry == null)
        {
            return 0;
        }

        long count = 0;

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            int numRows = Ru16(page, _dpNumRows);
            for (int r = 0; r < numRows; r++)
            {
                int raw = Ru16(page, _dpRowsStart + (r * 2));
                if ((raw & 0xC000) != 0)
                {
                    continue; // deleted or overflow
                }

                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows from the table named
    /// <paramref name="tableName"/> (case-insensitive).
    /// Returns column headers, rows with native CLR types (int, DateTime, decimal, etc.) in <see cref="TableResult.Rows"/>, and per-column schema.
    /// Use <see cref="ReadTableAsStrings"/> when raw string values are needed instead.
    /// </summary>
    /// <param name="tableName">Name of the table to read (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read. Use with large tables to avoid long reads or out-of-memory errors.</param>
    /// <returns>A <see cref="TableResult"/> containing headers, typed rows, and schema information.</returns>
    public TableResult ReadTable(string tableName, int maxRows)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        var resolved = ResolveTable(tableName);

        if (resolved == null)
        {
            // Check if this is a linked table
            LinkedTableInfo? link = FindLinkedTable(tableName);
            if (link != null)
            {
                using var source = OpenLinkedSource(link);
                return source.ReadTable(link.ForeignName, maxRows);
            }

            return new TableResult
            {
                Headers = new List<string>(),
                Rows = new List<object[]>(),
                Schema = new List<TableColumn>(),
            };
        }

        var (entry, td) = resolved.Value;
        var headers = td.Columns.ConvertAll(c => c.Name);
        var schema = BuildSchema(td.Columns);
        var typedRows = new List<object[]>();

        // Preload complex column data for tables that have complex (attachment) columns.
        var complexData = BuildComplexColumnData(tableName, td.Columns);

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            foreach (List<string> row in EnumerateRows(page, td))
            {
                typedRows.Add(ConvertRowToTyped(row, td.Columns, tableName, complexData));
                if (typedRows.Count >= maxRows)
                {
                    return new TableResult { Headers = headers, Rows = typedRows, Schema = schema, TableName = tableName };
                }
            }
        }

        return new TableResult { Headers = headers, Rows = typedRows, Schema = schema, TableName = tableName };
    }

    /// <inheritdoc/>
    public List<T> ReadTable<T>(string tableName, int maxRows)
        where T : class, new()
    {
        TableResult result = ReadTable(tableName, maxRows);
        var index = RowMapper<T>.BuildIndex(result.Headers);
        var items = new List<T>(result.Rows.Count);

        foreach (object[] row in result.Rows)
        {
            items.Add(RowMapper<T>.Map(row, index));
        }

        return items;
    }

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows from the table named
    /// <paramref name="tableName"/> (case-insensitive) with all values as strings.
    /// Returns column headers, string rows in <see cref="StringTableResult.Rows"/>, and per-column schema.
    /// Use <see cref="ReadTable(string, int)"/> when native CLR types are preferred.
    /// </summary>
    /// <param name="tableName">Name of the table to read (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read. Use with large tables to avoid long reads or out-of-memory errors.</param>
    /// <returns>A <see cref="StringTableResult"/> containing headers, string rows, and schema information.</returns>
    public StringTableResult ReadTableAsStrings(string tableName, int maxRows)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        var resolved = ResolveTable(tableName);

        if (resolved == null)
        {
            return new StringTableResult
            {
                Headers = new List<string>(),
                Rows = new List<List<string>>(),
                Schema = new List<TableColumn>(),
            };
        }

        var (entry, td) = resolved.Value;
        var headers = td.Columns.ConvertAll(c => c.Name);
        var schema = BuildSchema(td.Columns);
        var rows = new List<List<string>>();

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            foreach (List<string> row in EnumerateRows(page, td))
            {
                rows.Add(row);
                if (rows.Count >= maxRows)
                {
                    return new StringTableResult { Headers = headers, Rows = rows, Schema = schema, TableName = tableName };
                }
            }
        }

        return new StringTableResult { Headers = headers, Rows = rows, Schema = schema, TableName = tableName };
    }

    /// <summary>
    /// Yields rows from <paramref name="tableName"/> as properly typed object arrays without collecting them all in memory.
    /// Each element in the array is the native CLR type (int, DateTime, decimal, etc.).
    /// Ideal for large tables — use foreach to process one row at a time.
    /// This is the recommended method for streaming data.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns>An enumerable of object arrays, each representing a row with typed values.</returns>
    public IEnumerable<object[]> StreamRows(string tableName, IProgress<int>? progress = null)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        return StreamRowsCore(tableName, progress!);
    }

    /// <inheritdoc/>
    public IEnumerable<T> StreamRows<T>(string tableName, IProgress<int>? progress = null)
        where T : class, new()
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));

        List<ColumnMetadata> meta = GetColumnMetadata(tableName);
        var headers = meta.ConvertAll(m => m.Name);
        var index = RowMapper<T>.BuildIndex(headers);

        foreach (object[] row in StreamRowsCore(tableName, progress!))
        {
            yield return RowMapper<T>.Map(row, index);
        }
    }

    /// <summary>
    /// Yields rows from <paramref name="tableName"/> as string arrays without collecting them all in memory.
    /// Use this for compatibility scenarios or when you need raw string data.
    /// For most use cases, prefer <see cref="StreamRows"/> which returns properly typed data.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns>An enumerable of string arrays, each representing a row with string values.</returns>
    public IEnumerable<string[]> StreamRowsAsStrings(string tableName, IProgress<int>? progress = null)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        return StreamRowsAsStringsCore(tableName, progress!);
    }

    /// <summary>
    /// Reads the entire table into a DataTable with all columns typed as strings.
    /// Use this for compatibility scenarios or when you need raw string data.
    /// For most use cases, prefer <see cref="ReadTable(string, IProgress{int})"/> which returns properly typed columns.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns>A <see cref="DataTable"/> containing all rows with columns typed as strings, or null if the table doesn't exist or has no columns.</returns>
    public DataTable? ReadTableAsStringDataTable(string? tableName = null, IProgress<int>? progress = null)
    {
        ThrowIfDisposed();

        // If no table name specified, use the first table
        if (string.IsNullOrEmpty(tableName))
        {
            var tables = GetUserTables();
            if (tables.Count == 0)
            {
                return null;
            }

            tableName = tables[0].Name;
        }

        var resolved = ResolveTable(tableName);
        if (resolved == null)
        {
            return null;
        }

        var (entry, td) = resolved.Value;
        var dt = new DataTable(tableName);
        foreach (var col in td.Columns)
        {
            _ = dt.Columns.Add(col.Name, typeof(string));
        }

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            foreach (List<string> row in EnumerateRows(page, td))
            {
                _ = dt.Rows.Add(row.ToArray());
            }

            progress?.Report(dt.Rows.Count);
        }

        return dt;
    }

    /// <summary>
    /// Returns rich metadata for all columns in the specified table.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <returns>A list of <see cref="ColumnMetadata"/> objects describing each column in the table.</returns>
    public List<ColumnMetadata> GetColumnMetadata(string tableName)
    {
        ThrowIfDisposed();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));

        var resolved = ResolveTable(tableName);
        if (resolved == null)
        {
            // Check if this is a linked table
            LinkedTableInfo? link = FindLinkedTable(tableName);
            if (link != null)
            {
                using var source = OpenLinkedSource(link);
                return source.GetColumnMetadata(link.ForeignName);
            }

            return [];
        }

        // For complex columns (T_COMPLEX = 0x12), consult MSysComplexColumns to
        // determine the actual subtype (Attachment vs. multi-value, etc.).
        var complexSubtypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        bool hasComplex = resolved.Value.Td.Columns.Any(c => c.Type == T_COMPLEX || c.Type == T_ATTACHMENT);
        if (hasComplex)
        {
            complexSubtypes = ReadComplexColumnSubtypes(tableName);
        }

        return resolved.Value.Td.Columns.Select((col, index) => new ColumnMetadata
        {
            Name = col.Name,
            TypeName = (col.Type == T_COMPLEX && complexSubtypes.TryGetValue(col.Name, out string? subtype))
                ? subtype
                : TypeCodeToName(col.Type),
            ClrType = TypeCodeToClrType(col.Type),
            MaxLength = col.Size > 0 ? (int?)col.Size : null,
            IsNullable = true,
            IsFixedLength = col.IsFixed,
            Ordinal = index,
            Size = SizeForColumn(col),
        }).ToList();
    }

    /// <summary>
    /// Returns statistical information about the database.
    /// </summary>
    /// <returns>A <see cref="DatabaseStatistics"/> object containing various statistics about the database.</returns>
    public DatabaseStatistics GetStatistics()
    {
        ThrowIfDisposed();

        var stats = new DatabaseStatistics
        {
            TotalPages = _fs.Length / _pgSz,
            DatabaseSizeBytes = _fs.Length,
            PageSize = _pgSz,
            Version = _jet4 ? "Jet4/ACE" : "Jet3",
            CodePage = _codePage,
        };

        var tables = GetUserTables();
        stats.TableCount = tables.Count;
        stats.TableRowCounts = new Dictionary<string, long>();

        foreach (var table in tables)
        {
            var td = ReadTableDef(table.TDefPage);
            if (td != null)
            {
                stats.TableRowCounts[table.Name] = td.RowCount;
                stats.TotalRows += td.RowCount;
            }
        }

        long totalAccess = _cacheHits + _cacheMisses;
        stats.PageCacheHitRate = totalAccess > 0 ? (int)(_cacheHits * 100 / totalAccess) : 0;

        return stats;
    }

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns.
    /// Each table's columns use their native CLR types (int, DateTime, decimal, etc.).
    /// This is the recommended method for bulk reading.
    /// </summary>
    /// <param name="progress">Optional progress reporter — receives table name as each table is read.</param>
    /// <returns>A <see cref="Dictionary{TKey, TValue}"/> mapping table names to their corresponding <see cref="DataTable"/> with properly typed columns.</returns>
    public Dictionary<string, DataTable> ReadAllTables(IProgress<string>? progress = null)
    {
        ThrowIfDisposed();

        var result = new Dictionary<string, DataTable>(StringComparer.OrdinalIgnoreCase);
        var tables = GetUserTables();

        foreach (var table in tables)
        {
            progress?.Report($"Reading {table.Name}...");
            result[table.Name] = ReadTable(table.Name)!;
        }

        return result;
    }

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with all columns typed as strings.
    /// Use this for compatibility scenarios.
    /// </summary>
    /// <param name="progress">Optional progress reporter — receives table name after each table is read.</param>
    /// <returns>A dictionary of DataTables with all columns typed as strings.</returns>
    public Dictionary<string, DataTable> ReadAllTablesAsStrings(IProgress<string>? progress = null)
    {
        ThrowIfDisposed();

        var result = new Dictionary<string, DataTable>(StringComparer.OrdinalIgnoreCase);
        var tables = GetUserTables();

        foreach (var table in tables)
        {
            progress?.Report($"Reading {table.Name}...");
            result[table.Name] = ReadTableAsStringDataTable(table.Name)!;
        }

        return result;
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

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// This is the recommended method for reading table data.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns.</returns>
    public DataTable? ReadTable(string? tableName = null, IProgress<int>? progress = null)
    {
        ThrowIfDisposed();

        if (string.IsNullOrEmpty(tableName))
        {
            var tables = GetUserTables();
            if (tables.Count == 0)
            {
                return null;
            }

            tableName = tables[0].Name;
        }

        var resolved = ResolveTable(tableName);
        if (resolved == null)
        {
            // Check if this is a linked table
            LinkedTableInfo? link = FindLinkedTable(tableName);
            if (link != null)
            {
                using var source = OpenLinkedSource(link);
                return source.ReadTable(link.ForeignName, progress);
            }

            return null;
        }

        var (entry, td) = resolved.Value;
        var dt = new DataTable(tableName);

        // Create columns with proper CLR types
        foreach (var col in td.Columns)
        {
            _ = dt.Columns.Add(col.Name, TypeCodeToClrType(col.Type));
        }

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            foreach (List<string> row in EnumerateRows(page, td))
            {
                _ = dt.Rows.Add(ConvertRowToTyped(row, td.Columns, tableName, BuildComplexColumnData(tableName, td.Columns)));
            }

            progress?.Report(dt.Rows.Count);
        }

        return dt;
    }

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <returns>A list of user table names.</returns>
    public Task<List<string>> ListTablesAsync()
    {
        return Task.Run(() => ListTables());
    }

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns.</returns>
    public Task<DataTable?> ReadTableAsync(string? tableName = null, IProgress<int>? progress = null)
    {
        return Task.Run(() => ReadTable(tableName, progress));
    }

    /// <summary>
    /// Async overload of <see cref="ReadTable(string, int)"/>.
    /// Reads up to <paramref name="maxRows"/> rows with native CLR types asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read.</param>
    /// <returns>A <see cref="TableResult"/> containing headers, typed rows, and schema.</returns>
    public Task<TableResult> ReadTableAsync(string tableName, int maxRows)
    {
        return Task.Run(() => ReadTable(tableName, maxRows));
    }

    /// <inheritdoc/>
    public Task<List<T>> ReadTableAsync<T>(string tableName, int maxRows)
        where T : class, new()
    {
        return Task.Run(() => ReadTable<T>(tableName, maxRows));
    }

    /// <summary>
    /// Async overload of <see cref="ReadTableAsStrings(string, int)"/>.
    /// Reads up to <paramref name="maxRows"/> rows as strings asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read.</param>
    /// <returns>A <see cref="StringTableResult"/> containing headers, string rows, and schema.</returns>
    public Task<StringTableResult> ReadTableAsStringsAsync(string tableName, int maxRows)
    {
        return Task.Run(() => ReadTableAsStrings(tableName, maxRows));
    }

    /// <summary>
    /// Returns statistical information about the database asynchronously.
    /// </summary>
    /// <returns>A <see cref="DatabaseStatistics"/> object containing various metrics about the database.</returns>
    public Task<DatabaseStatistics> GetStatisticsAsync()
    {
        return Task.Run(() => GetStatistics());
    }

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns asynchronously.
    /// Each table's columns use their native CLR types (int, DateTime, decimal, etc.).
    /// </summary>
    /// <param name="progress">Optional progress reporter for table names.</param>
    /// <returns>A dictionary mapping table names to their corresponding DataTables.</returns>
    public Task<Dictionary<string, DataTable>> ReadAllTablesAsync(IProgress<string>? progress = null)
    {
        return Task.Run(() => ReadAllTables(progress));
    }

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with all columns typed as strings asynchronously.
    /// Use this for compatibility scenarios.
    /// </summary>
    /// <param name="progress">Optional progress reporter for table names.</param>
    /// <returns>A dictionary mapping table names to their corresponding DataTables with all columns as strings.</returns>
    public Task<Dictionary<string, DataTable>> ReadAllTablesAsStringsAsync(IProgress<string>? progress = null)
    {
        return Task.Run(() => ReadAllTablesAsStrings(progress));
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            if (disposing)
            {
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
        }
        finally
        {
            base.Dispose(disposing);
        }
    }

        /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    private protected override List<CatalogEntry> GetUserTables()
    {
        if (_catalogCache != null)
        {
            return _catalogCache;
        }

        lock (_catalogLock)
        {
            if (_catalogCache != null)
            {
                return _catalogCache;
            }

            var diag = new System.Text.StringBuilder();
            _ = diag.AppendLine($"JET: {(_jet4 ? "Jet4/ACE" : "Jet3")}  PageSize: {_pgSz}  TotalPages: {_fs.Length / _pgSz}");

            // MSysObjects TDEF is hard-coded at page 2 by the Jet engine
            TableDef? msys = ReadTableDef(2);
            if (msys == null)
            {
                _ = diag.AppendLine("ERROR: Page 2 is not a valid TDEF page (null returned).");
                LastDiagnostics = diag.ToString();
                _catalogCache = [];
                return _catalogCache;
            }

            _ = diag.AppendLine($"MSysObjects cols ({msys.Columns.Count}): " +
                string.Join(", ", msys.Columns.ConvertAll(c => $"{c.Name}[0x{c.Type:X2}]")));

            // Case-insensitive column lookup — column names vary slightly across Access versions
            int idxId = msys.Columns.FindIndex(c => string.Equals(c.Name, "Id", StringComparison.OrdinalIgnoreCase));
            int idxName = msys.Columns.FindIndex(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
            int idxType = msys.Columns.FindIndex(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));
            int idxFlags = msys.Columns.FindIndex(c => string.Equals(c.Name, "Flags", StringComparison.OrdinalIgnoreCase));

            if (idxName < 0 || idxType < 0)
            {
                _ = diag.AppendLine("ERROR: Required catalog columns not found. Column name mismatch?");
                LastDiagnostics = diag.ToString();
                _catalogCache = [];
                return _catalogCache;
            }

            var result = new List<CatalogEntry>();
            long totPages = _fs.Length / _pgSz;
            int catPages = 0;
            int allRows = 0;

            for (long p = 3; p < totPages; p++)
            {
                byte[] page = ReadPageCached(p);
                if (page[0] != 0x01)
                {
                    continue;             // data pages only
                }

                if (Ri32(page, _dpTDefOff) != 2)
                {
                    continue; // must belong to MSysObjects
                }

                catPages++;

                foreach (List<string> row in EnumerateRows(page, msys))
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
                        result.Add(new CatalogEntry { Name = nameStr, TDefPage = tdefPage });
                    }
                }
            }

            _ = diag.AppendLine($"Catalog pages: {catPages}  Total rows scanned: {allRows}  User tables: {result.Count}");
            if (DiagnosticsEnabled)
            {
                foreach (var e in result)
                {
                    _ = diag.AppendLine($"  [{e.Name}] TDEF page {e.TDefPage}");
                }
            }

            LastDiagnostics = diag.ToString();
            _catalogCache = result;
            return _catalogCache;
        }
    }

    private protected override IEnumerable<byte[]> EnumerateTablePages(long tdefPage)
    {
        long total = _fs.Length / _pgSz;
        for (long p = 3; p < total; p++)
        {
            byte[] page = ReadPageCached(p);
            if (page[0] != 0x01)
            {
                continue;
            }

            if ((long)Ri32(page, _dpTDefOff) != tdefPage)
            {
                continue;
            }

            yield return page;
        }
    }

    private static string SafeGet(List<string> row, int idx) =>
        (idx >= 0 && idx < row.Count) ? row[idx] : string.Empty;

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

    private static string TypeCodeToName(byte t)
    {
        switch (t)
        {
            case T_BOOL: return "Yes/No";
            case T_BYTE: return "Byte";
            case T_INT: return "Integer";
            case T_LONG: return "Long Integer";
            case T_MONEY: return "Currency";
            case T_FLOAT: return "Single";
            case T_DOUBLE: return "Double";
            case T_DATETIME: return "Date/Time";
            case T_BINARY: return "Binary";
            case T_TEXT: return "Text";
            case T_OLE: return "OLE Object";
            case T_MEMO: return "Memo";
            case T_GUID: return "GUID";
            case T_NUMERIC: return "Decimal";
            case T_ATTACHMENT: return "Attachment";
            case T_COMPLEX: return "Complex";
            default: return $"0x{t:X2}";
        }
    }

    private static ColumnSize SizeForColumn(ColumnInfo col)
    {
        switch (col.Type)
        {
            case T_BOOL: return ColumnSize.FromBits(1);
            case T_BYTE: return ColumnSize.FromBytes(1);
            case T_INT: return ColumnSize.FromBytes(2);
            case T_LONG: return ColumnSize.FromBytes(4);
            case T_MONEY: return ColumnSize.FromBytes(8);
            case T_FLOAT: return ColumnSize.FromBytes(4);
            case T_DOUBLE: return ColumnSize.FromBytes(8);
            case T_DATETIME: return ColumnSize.FromBytes(8);
            case T_GUID: return ColumnSize.FromBytes(16);
            case T_NUMERIC: return ColumnSize.FromBytes(17);
            case T_TEXT: return ColumnSize.FromChars(col.Size > 0 ? col.Size / 2 : 255);
            case T_BINARY: return col.Size > 0 ? ColumnSize.FromBytes(col.Size) : ColumnSize.Variable;
            case T_MEMO:
            case T_OLE: return ColumnSize.Lval;
            case T_ATTACHMENT:
            case T_COMPLEX: return ColumnSize.Lval;
            default: return col.Size > 0 ? ColumnSize.FromBytes(col.Size) : ColumnSize.Variable;
        }
    }

    private static Type TypeCodeToClrType(byte typeCode)
    {
        switch (typeCode)
        {
            case T_BOOL: return typeof(bool);
            case T_BYTE: return typeof(byte);
            case T_INT: return typeof(short);
            case T_LONG: return typeof(int);
            case T_MONEY: return typeof(decimal);
            case T_FLOAT: return typeof(float);
            case T_DOUBLE: return typeof(double);
            case T_DATETIME: return typeof(DateTime);
            case T_GUID: return typeof(Guid);
            case T_NUMERIC: return typeof(decimal);
            case T_ATTACHMENT: return typeof(byte[]);
            case T_COMPLEX: return typeof(byte[]);
            default: return typeof(string);
        }
    }

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

    private static List<TableColumn> BuildSchema(List<ColumnInfo> columns)
    {
        return columns.ConvertAll(c => new TableColumn
        {
            Name = c.Name,
            Type = TypeCodeToClrType(c.Type),
            Size = SizeForColumn(c),
        });
    }

    /// <summary>
    /// Opens the source database for a linked table entry.
    /// Throws FileNotFoundException if the source database does not exist.
    /// </summary>
    private static AccessReader OpenLinkedSource(LinkedTableInfo link)
    {
        if (string.IsNullOrEmpty(link.SourceDatabasePath) || !File.Exists(link.SourceDatabasePath))
        {
            throw new FileNotFoundException(
                $"Source database for linked table '{link.Name}' not found: {link.SourceDatabasePath}",
                link.SourceDatabasePath);
        }

        return Open(link.SourceDatabasePath);
    }

    private static object[] ConvertRowToTyped(List<string> row, List<ColumnInfo> columns, string? tableName = null, Dictionary<int, Dictionary<int, byte[]>>? complexData = null)
    {
        var typedRow = new object[row.Count];
        for (int i = 0; i < row.Count && i < columns.Count; i++)
        {
            string raw = row[i];
            ColumnInfo col = columns[i];

            // Resolve complex-field attachments using preloaded complex data (keyed by col ordinal).
            // The parent row ID is the first fixed LONG column's value.
            if ((col.Type == T_COMPLEX || col.Type == T_ATTACHMENT) &&
                complexData != null &&
                complexData.TryGetValue(i, out Dictionary<int, byte[]>? colData))
            {
                // Find the parent row ID: use the first LONG fixed column value in this row.
                int parentId = ExtractParentId(row, columns);
                if (parentId > 0 && colData.TryGetValue(parentId, out byte[]? attachBytes) &&
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

    /// <summary>Returns linked table entries from the MSysObjects catalog.</summary>
    private List<LinkedTableInfo> GetLinkedTables()
    {
        // MSysObjects TDEF is hard-coded at page 2
        TableDef? msys = ReadTableDef(2);
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
        long totPages = _fs.Length / _pgSz;

        for (long p = 3; p < totPages; p++)
        {
            byte[] page = ReadPageCached(p);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            foreach (List<string> row in EnumerateRows(page, msys))
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
        if (_fs.Length < 128)
        {
            throw new InvalidDataException("File too small to be a valid JET database");
        }

        // Verify the JET magic signature at offset 0: 00 01 00 00
        _ = _fs.Seek(0, SeekOrigin.Begin);
        var magic = new byte[4];
        int read = _fs.Read(magic, 0, 4);
        if (read < 4 || magic[0] != 0x00 || magic[1] != 0x01 || magic[2] != 0x00 || magic[3] != 0x00)
        {
            throw new InvalidDataException(
                $"File does not have a valid JET magic signature " +
                $"(expected 00 01 00 00, got {magic[0]:X2} {magic[1]:X2} {magic[2]:X2} {magic[3]:X2}).");
        }
    }

    /// <summary>Reads a page through the cache if enabled (PageCacheSize > 0).</summary>
    private byte[] ReadPageCached(long n)
    {
        ThrowIfDisposed();

        if (PageCacheSize < 0)
        {
            return ReadPage(n);  // cache disabled
        }

        // Lazy-init: only one thread creates the cache; LruCache is internally thread-safe
        // so subsequent TryGetValue/Add calls need no outer lock.
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
        byte[] page = ReadPage(n);
        _pageCache?.Add(n, page);
        return page;
    }

    private (CatalogEntry Entry, TableDef Td)? ResolveTable(string tableName)
    {
        CatalogEntry entry = GetCatalogEntry(tableName);
        if (entry != null)
        {
            TableDef? td = ReadTableDef(entry.TDefPage);
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
    private LinkedTableInfo? FindLinkedTable(string tableName)
    {
        return GetLinkedTables().Find(l =>
            string.Equals(l.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    private IEnumerable<object[]> StreamRowsCore(string tableName, IProgress<int> progress)
    {
        var resolved = ResolveTable(tableName);
        if (resolved == null)
        {
            // Check if this is a linked table
            LinkedTableInfo? link = FindLinkedTable(tableName);
            if (link != null)
            {
                using var source = OpenLinkedSource(link);
                foreach (object[] row in source.StreamRows(link.ForeignName))
                {
                    yield return row;
                }

                yield break;
            }

            yield break;
        }

        var (entry, td) = resolved.Value;
        int rowCount = 0;

        // Preload complex column data for tables that have complex (attachment) columns.
        var complexData = BuildComplexColumnData(tableName, td.Columns);

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            foreach (List<string> row in EnumerateRows(page, td))
            {
                yield return ConvertRowToTyped(row, td.Columns, tableName, complexData);
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    private IEnumerable<string[]> StreamRowsAsStringsCore(string tableName, IProgress<int> progress)
    {
        var resolved = ResolveTable(tableName);
        if (resolved == null)
        {
            yield break;
        }

        var (entry, td) = resolved.Value;
        int rowCount = 0;

        foreach (byte[] page in EnumerateTablePages(entry.TDefPage))
        {
            foreach (List<string> row in EnumerateRows(page, td))
            {
                yield return row.ToArray();
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <summary>Yields decoded rows from a single data page.</summary>
    private IEnumerable<List<string>> EnumerateRows(byte[] page, TableDef td)
    {
        foreach (RowBound rb in EnumerateLiveRowBounds(page))
        {
            if (rb.RowSize < _numColsFldSz)
            {
                continue;
            }

            List<string>? values = CrackRow(page, rb.RowStart, rb.RowSize, td);
            if (values != null)
            {
                yield return values;
            }
        }
    }

    /// <summary>Decodes a single row's bytes (within <paramref name="page"/>) into string values per column.</summary>
    private List<string>? CrackRow(byte[] page, int rowStart, int rowSize, TableDef td)
    {
        if (rowSize < _numColsFldSz)
        {
            return null;
        }

        // Number of columns stored in THIS row (may be less than td.Columns.Count
        // if columns were added after this row was written)
        int numCols = _jet4 ? Ru16(page, rowStart) : page[rowStart];
        if (numCols == 0)
        {
            return null;
        }

        // Check for deleted-column schema mismatch
        // If the table has deleted columns AND this row has MORE columns than current schema,
        // it was written before the deletion and data alignment is ambiguous
        if (td.HasDeletedColumns && numCols > td.Columns.Count)
        {
            throw new JetLimitationException(
                $"Row has {numCols} columns but current schema has {td.Columns.Count} with deleted-column gaps. " +
                $"This row predates schema changes and data may be misaligned. " +
                $"Solution: Compact & Repair the database in Microsoft Access to rebuild all rows.");
        }

        int nullMaskSz = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSz;  // relative to rowStart
        if (nullMaskPos < _numColsFldSz)
        {
            return null;
        }

        // ── Tail section layout (high→low addresses, reading from end) ──
        //  Jet4: [null_mask][var_len(2)][var_table(varLen*2)][eod(2)]
        //  Jet3: [null_mask][var_len(1)][jump_table(n*1)][var_table(varLen*1)][eod(1)]

        int varLenPos = nullMaskPos - _varLenFldSz;  // relative
        if (varLenPos < _numColsFldSz)
        {
            return null;
        }

        int varLen = _jet4 ? Ru16(page, rowStart + varLenPos) : page[rowStart + varLenPos];

        // Jet3 jump table: floor(rowSize / 256) entries of 1 byte each
        int jumpSz = _jet4 ? 0 : (rowSize / 256);

        int varTableStart = varLenPos - jumpSz - (varLen * _varEntrySz);  // relative
        int eodPos = varTableStart - _eodFldSz;                  // relative
        if (eodPos < _numColsFldSz)
        {
            return null;
        }

        int eod = _jet4 ? Ru16(page, rowStart + eodPos) : page[rowStart + eodPos];

        // ── Decode each column ────────────────────────────────────────
        var result = new List<string>(td.Columns.Count);

        for (int i = 0; i < td.Columns.Count; i++)
        {
            ColumnInfo col = td.Columns[i];

            // null_mask bit index = col.ColNum (the descriptor's col_num field),
            // NOT the loop index i.  JET rows index the mask by col_num,
            // while the TDEF may store columns in a different order (e.g. alphabetically).
            bool nullBit = false;
            if (col.ColNum < numCols)
            {
                int mByte = nullMaskPos + (col.ColNum / 8);  // relative
                int mBit = col.ColNum % 8;
                if (mByte < rowSize)
                {
                    nullBit = (page[rowStart + mByte] & (1 << mBit)) != 0;
                }
            }

            // BOOL: null_mask bit IS the value; no bytes stored in the row.
            // In JET: bit SET (1) = TRUE for BOOL.
            if (col.Type == T_BOOL)
            {
                result.Add(nullBit ? "True" : "False");
                continue;
            }

            // For all other types: bit SET (1) = column HAS a value (not null).
            // bit CLEAR (0) = column IS null.
            // Column also has no value when it was added after this row was written.
            if (col.ColNum >= numCols || !nullBit)
            {
                result.Add(string.Empty);
                continue;
            }

            if (col.IsFixed)
            {
                int start = _numColsFldSz + col.FixedOff;  // relative
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
                // Variable column — look up its offset in the reversed var_table.
                // var_table is stored in reverse column order:
                //   entry for VarIdx=k  →  varTableStart + (varLen-1-k)*varEntrySz
                if (col.VarIdx >= varLen)
                {
                    result.Add(string.Empty);
                    continue;
                }

                int entryPos = varTableStart + ((varLen - 1 - col.VarIdx) * _varEntrySz);  // relative
                if (entryPos < 0 || entryPos + _varEntrySz > rowSize)
                {
                    result.Add(string.Empty);
                    continue;
                }

                int varOff = _jet4 ? Ru16(page, rowStart + entryPos) : page[rowStart + entryPos];

                // End of this variable column's data
                int varEnd;
                if (col.VarIdx + 1 < varLen)
                {
                    int nextEntry = varTableStart + ((varLen - 2 - col.VarIdx) * _varEntrySz);  // relative
                    varEnd = _jet4 ? Ru16(page, rowStart + nextEntry) : page[rowStart + nextEntry];
                }
                else
                {
                    varEnd = eod;
                }

                // var_table entries are ROW offsets (from row[0]), not data-area offsets.
                // FixedOff is a data-area offset (requires + _numColsFldSz), but var_table
                // entries already include the num_cols header bytes.
                int dataStart = varOff;          // relative to rowStart
                int dataLen = varEnd - varOff;
                if (dataLen < 0 || dataStart < 0 || dataStart + dataLen > rowSize)
                {
                    result.Add(string.Empty);
                    continue;
                }

                result.Add(ReadVar(page, rowStart + dataStart, dataLen, col));
            }
        }

        return result;
    }

    private string ReadVar(byte[] row, int start, int len, ColumnInfo col)
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
                    return ReadLongValue(row, start, len, col.Type == T_OLE);

                case T_COMPLEX:
                case T_ATTACHMENT:
                    // Variable slot holds a 4-byte complex_id pointing to rows in
                    // the MSysCM_<table>_<column> system table.  Encode as a marker
                    // so ConvertRowToTyped can resolve it to the actual attachment bytes.
                    if (len >= 4)
                    {
                        int complexId = Ri32(row, start);
                        return $"__CX:{complexId}__";
                    }

                    return string.Empty;

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
    /// Looks up MSysComplexColumns to determine the subtype name for complex (0x12) columns
    /// in the specified table. Returns a dictionary of column name → TypeName ("Attachment", etc.).
    /// </summary>
    private Dictionary<string, string> ReadComplexColumnSubtypes(string tableName)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            // Find MSysComplexColumns in the catalog (it's a system table, SYSTABLE_MASK set)
            long tdefPage = FindSystemTablePage("MSysComplexColumns");
            if (tdefPage <= 0)
            {
                return result;
            }

            TableDef? td = ReadTableDef(tdefPage);
            if (td == null)
            {
                return result;
            }

            int idxTable = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "TableName", StringComparison.OrdinalIgnoreCase));
            int idxCol = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ColumnName", StringComparison.OrdinalIgnoreCase));
            int idxType = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ComplexTypeObjectId", StringComparison.OrdinalIgnoreCase)
                || string.Equals(c.Name, "TypeObjectId", StringComparison.OrdinalIgnoreCase)
                || string.Equals(c.Name, "ComplexType", StringComparison.OrdinalIgnoreCase));

            if (idxTable < 0 || idxCol < 0)
            {
                return result;
            }

            foreach (byte[] page in EnumerateTablePages(tdefPage))
            {
                foreach (List<string> row in EnumerateRows(page, td))
                {
                    string rowTable = SafeGet(row, idxTable);
                    if (!string.Equals(rowTable, tableName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string colName = SafeGet(row, idxCol);

                    // If we find a match, the column is an Attachment type when stored
                    // via the MSysCM_ table mechanism (Access Attachment field).
                    // Without a deeper ComplexType lookup, default to "Attachment" for 0x12.
                    result[colName] = "Attachment";
                }
            }
        }
        catch (InvalidDataException)
        {
            // Best-effort: if we can't read MSysComplexColumns, fall back to "Complex".
        }
        catch (IndexOutOfRangeException)
        {
            // Best-effort fallback.
        }

        return result;
    }

    /// <summary>
    /// Finds the TDEF page number for a system table by name (case-insensitive).
    /// Unlike GetUserTables, this includes system tables (SYSTABLE_MASK set).
    /// </summary>
    private long FindSystemTablePage(string name)
    {
        TableDef? msys = ReadTableDef(2);
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

        long totPages = _fs.Length / _pgSz;
        for (long p = 3; p < totPages; p++)
        {
            byte[] page = ReadPageCached(p);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            foreach (List<string> row in EnumerateRows(page, msys))
            {
                string nameStr = SafeGet(row, idxName);
                if (!string.Equals(nameStr, name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!int.TryParse(SafeGet(row, idxType), out int objType) || objType != OBJ_TABLE)
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
    private Dictionary<int, Dictionary<int, byte[]>>? BuildComplexColumnData(
        string tableName, List<ColumnInfo> columns)
    {
        Dictionary<int, Dictionary<int, byte[]>>? result = null;

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnInfo col = columns[i];
            if (col.Type != T_COMPLEX && col.Type != T_ATTACHMENT)
            {
                continue;
            }

            Dictionary<int, byte[]>? colData = LoadAttachmentData(tableName, col.Name);
            if (colData != null && colData.Count > 0)
            {
                result ??= new Dictionary<int, Dictionary<int, byte[]>>();
                result[i] = colData;
            }
        }

        return result;
    }

    /// <summary>
    /// Loads attachment data from the hidden system table for a complex column.
    /// Access ACCDB stores attachment data in a table named <c>f_&lt;GUID&gt;_&lt;columnName&gt;</c>.
    /// The table's FK column is named <c>&lt;tableName&gt;_&lt;columnName&gt;</c> and holds the parent row ID.
    /// Returns a dictionary mapping parent row ID → serialized attachment bytes
    /// (2-byte FileName length LE, FileName UTF-16LE, FileData bytes).
    /// </summary>
    private Dictionary<int, byte[]>? LoadAttachmentData(string tableName, string columnName)
    {
        try
        {
            // The hidden attachment table name is f_<GUID>_<columnName>.
            // We find it by scanning MSysObjects for any Table entry whose name
            // ends with _<columnName> and starts with f_.
            string nameSuffix = $"_{columnName}";
            long tdefPage = FindSystemTablePageBySuffix(nameSuffix);
            if (tdefPage <= 0)
            {
                return null;
            }

            TableDef? td = ReadTableDef(tdefPage);
            if (td == null)
            {
                return null;
            }

            // FK column name = "<tableName>_<columnName>" (e.g. "Documents_Attachments")
            string fkColName = $"{tableName}_{columnName}";
            int idxFk = td.Columns.FindIndex(c =>
                string.Equals(c.Name, fkColName, StringComparison.OrdinalIgnoreCase));

            if (idxFk < 0)
            {
                // Fallback: any LONG column that looks like a FK
                idxFk = td.Columns.FindIndex(c =>
                    c.Type == T_LONG && !c.Name.StartsWith("Idx", StringComparison.OrdinalIgnoreCase));
            }

            if (idxFk < 0)
            {
                return null;
            }

            int idxFileName = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "FileName", StringComparison.OrdinalIgnoreCase));
            int idxFileData = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "FileData", StringComparison.OrdinalIgnoreCase));

            var result = new Dictionary<int, byte[]>();

            foreach (byte[] page in EnumerateTablePages(tdefPage))
            {
                foreach (List<string> row in EnumerateRows(page, td))
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
                        fileDataBytes = DecodeColumnBytes(fileDataStr, td.Columns[idxFileData].Type);
                    }

                    if (fileNameBytes.Length == 0 && fileDataBytes.Length == 0)
                    {
                        continue;
                    }

                    // Encode as: [2-byte nameLen LE][nameBytes][fileDataBytes]
                    var bytes = new byte[2 + fileNameBytes.Length + fileDataBytes.Length];
                    bytes[0] = (byte)(fileNameBytes.Length & 0xFF);
                    bytes[1] = (byte)((fileNameBytes.Length >> 8) & 0xFF);
                    Buffer.BlockCopy(fileNameBytes, 0, bytes, 2, fileNameBytes.Length);
                    Buffer.BlockCopy(fileDataBytes, 0, bytes, 2 + fileNameBytes.Length, fileDataBytes.Length);
                    result[parentId] = bytes;
                }
            }

            return result.Count > 0 ? result : null;
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
    private long FindSystemTablePageBySuffix(string nameSuffix)
    {
        TableDef? msys = ReadTableDef(2);
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

        long totPages = _fs.Length / _pgSz;
        for (long p = 3; p < totPages; p++)
        {
            byte[] page = ReadPageCached(p);
            if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            foreach (List<string> row in EnumerateRows(page, msys))
            {
                string nameStr = SafeGet(row, idxName);
                if (!nameStr.EndsWith(nameSuffix, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                // Accept any object type that includes the TABLE bit (0x01).
                // Access system tables (hidden) use type 6 = OBJ_TABLE | SYSTABLE flags.
                if (!int.TryParse(SafeGet(row, idxType), out int objType) || (objType & OBJ_TABLE) == 0)
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
    private byte[]? ReadLvalBytes(uint lvalDp, int maxLen)
    {
        try
        {
            int lvalPage = (int)(lvalDp >> 8);
            int lvalRow = (int)(lvalDp & 0xFF);
            if (lvalPage <= 0)
            {
                return null;
            }

            byte[] page = ReadPageCached(lvalPage);
            if (page[0] != 0x01)
            {
                return null;  // must be a data page
            }

            int numRows = Ru16(page, _dpNumRows);
            if (lvalRow >= numRows)
            {
                return null;
            }

            int rawOff = Ru16(page, _dpRowsStart + (lvalRow * 2));
            if ((rawOff & 0xC000) != 0)
            {
                return null;  // deleted or overflow
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
    private LvalChainResult ReadLvalChain(uint firstLvalDp, int maxLen)
    {
        try
        {
            var chunks = new List<byte[]>();
            int totalLen = 0;
            uint currentDp = firstLvalDp;
            var seen = new HashSet<uint>();

            while (currentDp != 0 && totalLen < maxLen && !seen.Contains(currentDp))
            {
                _ = seen.Add(currentDp);

                int lvalPage = (int)(currentDp >> 8);
                int lvalRow = (int)(currentDp & 0xFF);
                if (lvalPage <= 0)
                {
                    return LvalChainResult.Failure($"invalid page {lvalPage}");
                }

                byte[] page = ReadPageCached(lvalPage);
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

                // LVAL chain format: [next_dp(4)][data_len(4)][data...]
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

            // Concatenate all chunks
            int finalLen = Math.Min(totalLen, maxLen);
            var result = new byte[finalLen];
            int pos = 0;
            foreach (var chunk in chunks)
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

    private string ReadLongValue(byte[] row, int start, int len, bool isOle)
    {
        if (len < 12)
        {
            return isOle ? "(OLE)" : "(memo)";
        }

        byte bitmask = row[start + 3];
        int memoLen = row[start] | (row[start + 1] << 8) | (row[start + 2] << 16);

        if ((bitmask & 0x80) != 0)
        {
            // Inline: data follows the 12-byte header
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
                return TryDecodeOleObject(row, memoStart, memoLen) ?? "(OLE)";
            }

            return _jet4 ? DecodeJet4Text(row, memoStart, memoLen)
                         : _ansiEncoding.GetString(row, memoStart, memoLen);
        }

        if ((bitmask & 0x40) != 0)
        {
            // Single LVAL page — lval_dp = (pageNumber << 8) | rowIndex
            uint lvalDp = Ru32(row, start + 4);
            byte[]? lvalData = ReadLvalBytes(lvalDp, memoLen);

            if (lvalData != null)
            {
                if (isOle)
                {
                    return TryDecodeOleObject(lvalData, 0, lvalData.Length) ?? "(OLE)";
                }

                return _jet4 ? DecodeJet4Text(lvalData, 0, lvalData.Length)
                             : _ansiEncoding.GetString(lvalData);
            }

            return isOle ? "(OLE)" : "(memo on LVAL page)";
        }

        // Multi-page LVAL (0x00) — follow the chain
        uint chainDp = Ru32(row, start + 4);
        LvalChainResult chain = ReadLvalChain(chainDp, memoLen);

        if (chain.Data != null)
        {
            if (isOle)
            {
                return TryDecodeOleObject(chain.Data, 0, chain.Data.Length) ?? "(OLE)";
            }

            return _jet4 ? DecodeJet4Text(chain.Data, 0, chain.Data.Length)
                         : _ansiEncoding.GetString(chain.Data);
        }

        return isOle ? $"(OLE chain error: {chain.Error})" : $"(memo chain error: {chain.Error})";
    }
}
