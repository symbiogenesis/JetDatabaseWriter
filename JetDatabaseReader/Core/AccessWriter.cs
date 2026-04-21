namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CA1822 // Mark members as static
#pragma warning disable SA1202 // Keep member order stable while synchronous APIs remain private compatibility helpers
#pragma warning disable SA1648 // Private compatibility helpers still carry inherited docs from previous public API

/// <summary>
/// Pure-managed writer for Microsoft Access JET databases (.mdb / .accdb).
/// Supports creating tables, inserting, updating, and deleting rows.
/// </summary>
public sealed class AccessWriter : AccessBase, IAccessWriter
{
    private const int MaxInlineMemoBytes = 1024;
    private const int MaxInlineOleBytes = 256;
    private const int TDefRowCountOffset = 16;

    private readonly string _path;
    private readonly SecureString? _password;
    private readonly bool _useLockFile;
    private readonly bool _respectExistingLockFile;
    private readonly ReaderWriterLockSlim _stateLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

    private List<CatalogEntry>? _catalogCache;
    private long _cachedInsertTDefPage = -1;
    private long _cachedInsertPageNumber = -1;

    private AccessWriter(
        string path,
        Stream stream,
        byte[] header,
        SecureString? password,
        bool useLockFile,
        bool respectExistingLockFile)
        : base(stream, header)
    {
        _path = path;
        _password = SecureStringUtilities.CopyAsReadOnly(password);
        _useLockFile = useLockFile && !string.IsNullOrEmpty(path);
        _respectExistingLockFile = respectExistingLockFile;

        if (_useLockFile)
        {
            CreateLockFile();
        }
    }

    /// <summary>
    /// Asynchronously opens a JET database file for writing and returns a new <see cref="AccessWriter"/> instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the specified database.</returns>
    public static async ValueTask<AccessWriter> OpenAsync(string path, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        options ??= new AccessWriterOptions();
        await VerifyPasswordOnOpenAsync(path, options, cancellationToken).ConfigureAwait(false);

        FileStream fs = CreateStream(path);
        return await OpenAsync(fs, options, leaveOpen: false, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously opens a JET database from a caller-supplied <see cref="Stream"/> and returns a new <see cref="AccessWriter"/> instance.
    /// The stream must be readable, writable, and seekable. The caller retains ownership unless <paramref name="leaveOpen"/> is false (the default),
    /// in which case the stream will be disposed when the writer is disposed.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the database bytes.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="leaveOpen">If <c>true</c>, the stream is not disposed when the writer is disposed. Default is <c>false</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the database.</returns>
    public static async ValueTask<AccessWriter> OpenAsync(Stream stream, AccessWriterOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        if (!stream.CanRead)
        {
            throw new ArgumentException("Stream must be readable.", nameof(stream));
        }

        if (!stream.CanWrite)
        {
            throw new ArgumentException("Stream must be writable.", nameof(stream));
        }

        if (!stream.CanSeek)
        {
            throw new ArgumentException("Stream must be seekable.", nameof(stream));
        }

        cancellationToken.ThrowIfCancellationRequested();

        options ??= new AccessWriterOptions();
        Stream wrapped = leaveOpen ? new NonClosingStreamWrapper(stream) : stream;
        try
        {
            string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
            byte[] header = await ReadHeaderAsync(wrapped, cancellationToken).ConfigureAwait(false);
            return new AccessWriter(
                path,
                wrapped,
                header,
                options.Password,
                options.UseLockFile,
                options.RespectExistingLockFile);
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
    public async ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required", nameof(columns));
        }

        if (await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        TableDef tableDef = BuildTableDefinition(columns, _jet4);
        byte[] tdefPage = BuildTDefPage(tableDef);
        long tdefPageNumber = await AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        await InsertCatalogEntryAsync(tableName, tdefPageNumber, cancellationToken).ConfigureAwait(false);
        InvalidateCatalogCache();
    }

    /// <inheritdoc/>
    public async ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist.");
        }

        int deleted = 0;
        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (!string.Equals(row.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (row.ObjectType != OBJ_TABLE)
            {
                continue;
            }

            if ((unchecked((uint)row.Flags) & SYSTABLE_MASK) != 0)
            {
                continue;
            }

            await MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted == 0)
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist.");
        }

        InvalidateCatalogCache();
    }

    /// <inheritdoc/>
    public async ValueTask InsertRowAsync(string tableName, object[] values, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(values, nameof(values));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        await InsertRowDataAsync(entry.TDefPage, tableDef, values, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object[]> rows, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(rows, nameof(rows));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        int inserted = 0;

        foreach (object[] row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guard.NotNull(row, nameof(rows));
            await InsertRowDataAsync(entry.TDefPage, tableDef, row, cancellationToken: cancellationToken).ConfigureAwait(false);
            inserted++;
        }

        return inserted;
    }

    /// <inheritdoc/>
    public async ValueTask InsertRowAsync<T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(item, nameof(item));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        var headers = tableDef.Columns.ConvertAll(c => c.Name);
        var index = RowMapper<T>.BuildIndex(headers);
        await InsertRowDataAsync(entry.TDefPage, tableDef, RowMapper<T>.ToRow(item, index), cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<int> InsertRowsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(items, nameof(items));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        var headers = tableDef.Columns.ConvertAll(c => c.Name);
        var index = RowMapper<T>.BuildIndex(headers);
        int inserted = 0;

        foreach (T item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guard.NotNull(item, nameof(items));
            await InsertRowDataAsync(entry.TDefPage, tableDef, RowMapper<T>.ToRow(item, index), cancellationToken: cancellationToken).ConfigureAwait(false);
            inserted++;
        }

        return inserted;
    }

    /// <inheritdoc/>
    public async ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object predicateValue, IDictionary<string, object> updatedValues, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (updatedValues.Count == 0)
        {
            return 0;
        }

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        int predicateIndex = FindColumnIndex(tableDef, predicateColumn);
        if (predicateIndex < 0)
        {
            throw new ArgumentException($"Column '{predicateColumn}' was not found in table '{tableName}'.", nameof(predicateColumn));
        }

        var updateIndexes = new Dictionary<int, object>();
        foreach (KeyValuePair<string, object> kvp in updatedValues)
        {
            int columnIndex = FindColumnIndex(tableDef, kvp.Key);
            if (columnIndex < 0)
            {
                throw new ArgumentException($"Column '{kvp.Key}' was not found in table '{tableName}'.", nameof(updatedValues));
            }

            updateIndexes[columnIndex] = kvp.Value;
        }

        using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        List<RowLocation> locations = await GetLiveRowLocationsAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        int total = Math.Min(snapshot.Rows.Count, locations.Count);
        int updated = 0;

        for (int i = 0; i < total; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object currentValue = snapshot.Rows[i][predicateIndex];
            if (!ValuesEqual(currentValue, predicateValue))
            {
                continue;
            }

            object[] rowValues = snapshot.Rows[i].ItemArray;
            foreach (KeyValuePair<int, object> update in updateIndexes)
            {
                rowValues[update.Key] = update.Value ?? DBNull.Value;
            }

            await MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, cancellationToken).ConfigureAwait(false);
            await InsertRowDataAsync(entry.TDefPage, tableDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            updated++;
        }

        return updated;
    }

    /// <inheritdoc/>
    public async ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object predicateValue, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        int predicateIndex = FindColumnIndex(tableDef, predicateColumn);
        if (predicateIndex < 0)
        {
            throw new ArgumentException($"Column '{predicateColumn}' was not found in table '{tableName}'.", nameof(predicateColumn));
        }

        using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        List<RowLocation> locations = await GetLiveRowLocationsAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        int total = Math.Min(snapshot.Rows.Count, locations.Count);
        int deleted = 0;

        for (int i = 0; i < total; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object currentValue = snapshot.Rows[i][predicateIndex];
            if (!ValuesEqual(currentValue, predicateValue))
            {
                continue;
            }

            await MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted > 0)
        {
            await AdjustTDefRowCountAsync(entry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
        }

        return deleted;
    }

    /// <summary>
    /// Inserts a linked table entry (type 4) into the MSysObjects catalog.
    /// This creates a reference to a table in an external database.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDatabasePath">The path to the source database file.</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    internal void InsertLinkedTableEntry(string linkedTableName, string sourceDatabasePath, string foreignTableName)
    {
        TableDef msys = ReadRequiredTableDef(2, "MSysObjects");
        var values = new object[msys.Columns.Count];
        DateTime now = DateTime.UtcNow;

        for (int i = 0; i < msys.Columns.Count; i++)
        {
            values[i] = DBNull.Value;
        }

        SetValue(msys, values, "Id", 0);
        SetValue(msys, values, "ParentId", 0);
        SetValue(msys, values, "Name", linkedTableName);
        SetValue(msys, values, "Type", (short)OBJ_LINKED_TABLE);
        SetValue(msys, values, "DateCreate", now);
        SetValue(msys, values, "DateUpdate", now);
        SetValue(msys, values, "Flags", 0);
        SetValue(msys, values, "ForeignName", foreignTableName);
        SetValue(msys, values, "Database", sourceDatabasePath);

        InsertRowData(2, msys, values);
        InvalidateCatalogCache();
    }

    /// <summary>
    /// Asynchronously inserts a linked table entry (type 4) into the MSysObjects catalog.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDatabasePath">The path to the source database file.</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal async ValueTask InsertLinkedTableEntryAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef msys = await ReadRequiredTableDefAsync(2, "MSysObjects", cancellationToken).ConfigureAwait(false);
        var values = new object[msys.Columns.Count];
        DateTime now = DateTime.UtcNow;

        for (int i = 0; i < msys.Columns.Count; i++)
        {
            values[i] = DBNull.Value;
        }

        SetValue(msys, values, "Id", 0);
        SetValue(msys, values, "ParentId", 0);
        SetValue(msys, values, "Name", linkedTableName);
        SetValue(msys, values, "Type", (short)OBJ_LINKED_TABLE);
        SetValue(msys, values, "DateCreate", now);
        SetValue(msys, values, "DateUpdate", now);
        SetValue(msys, values, "Flags", 0);
        SetValue(msys, values, "ForeignName", foreignTableName);
        SetValue(msys, values, "Database", sourceDatabasePath);

        await InsertRowDataAsync(2, msys, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        InvalidateCatalogCache();
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        if (_useLockFile)
        {
            DeleteLockFile();
        }

        _password?.Dispose();
        _stateLock.Dispose();

        await base.DisposeAsync().ConfigureAwait(false);
    }

    private static byte[]? EncodeOleValue(object value)
    {
        byte[]? data = value as byte[];
        if (data == null)
        {
            string? stringValue = value as string;
            if (string.IsNullOrEmpty(stringValue))
            {
                return null;
            }

            data = Encoding.UTF8.GetBytes(stringValue);
        }

        if (data.Length > MaxInlineOleBytes)
        {
            throw new JetLimitationException($"OLE value is {data.Length} bytes, which exceeds the inline limit of {MaxInlineOleBytes} bytes.");
        }

        return WrapInlineLongValue(data);
    }

    private static bool ValuesEqual(object left, object right)
    {
        bool leftDbNull = left == null || left is DBNull;
        bool rightDbNull = right == null || right is DBNull;
        if (leftDbNull || rightDbNull)
        {
            return leftDbNull && rightDbNull;
        }

        return object.Equals(left, right);
    }

    private static byte TypeCodeFromDefinition(ColumnDefinition column)
    {
        Type clrType = column.ClrType;

        switch (Type.GetTypeCode(clrType))
        {
            case TypeCode.Boolean: return T_BOOL;
            case TypeCode.Byte: return T_BYTE;
            case TypeCode.Int16: return T_INT;
            case TypeCode.Int32: return T_LONG;
            case TypeCode.Single: return T_FLOAT;
            case TypeCode.Double: return T_DOUBLE;
            case TypeCode.DateTime: return T_DATETIME;
            case TypeCode.Decimal: return T_NUMERIC;
            case TypeCode.String:
                return column.MaxLength > 0 && column.MaxLength <= 255 ? T_TEXT : T_MEMO;
            default:
                if (clrType == typeof(Guid))
                {
                    return T_GUID;
                }

                if (clrType == typeof(byte[]))
                {
                    return column.MaxLength > 0 && column.MaxLength <= 255 ? T_BINARY : T_OLE;
                }

                throw new NotSupportedException($"CLR type '{clrType}' is not supported for table creation.");
        }
    }

    private static bool IsVariableType(byte type)
    {
        return type == T_TEXT || type == T_BINARY || type == T_MEMO || type == T_OLE;
    }

    private static int FindColumnIndex(TableDef tableDef, string columnName)
    {
        return tableDef.Columns.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
    }

    private static void SetValue(TableDef tableDef, object[] values, string columnName, object value)
    {
        int index = FindColumnIndex(tableDef, columnName);
        if (index >= 0)
        {
            values[index] = value;
        }
    }

    private static TableDef BuildTableDefinition(IReadOnlyList<ColumnDefinition> columns, bool jet4)
    {
        var result = new TableDef();
        int fixedOffset = 0;
        int nextVarIndex = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition definition = columns[i];
            byte type = TypeCodeFromDefinition(definition);
            bool variable = IsVariableType(type);
            int size = GetDeclaredSize(type, definition.MaxLength, jet4);

            var column = new ColumnInfo
            {
                Name = definition.Name,
                Type = type,
                ColNum = i,
                VarIdx = variable ? nextVarIndex : 0,
                FixedOff = variable ? 0 : fixedOffset,
                Size = size,
                Flags = (byte)(variable ? 0x02 : 0x03),
            };

            result.Columns.Add(column);

            if (variable)
            {
                nextVarIndex++;
            }
            else
            {
                fixedOffset += FixedSize(type, size);
            }
        }

        return result;
    }

    private static int GetDeclaredSize(byte type, int maxLength, bool jet4)
    {
        switch (type)
        {
            case T_BOOL:
                return 0;
            case T_BYTE:
                return 1;
            case T_INT:
                return 2;
            case T_LONG:
                return 4;
            case T_MONEY:
                return 8;
            case T_FLOAT:
                return 4;
            case T_DOUBLE:
                return 8;
            case T_DATETIME:
                return 8;
            case T_GUID:
                return 16;
            case T_NUMERIC:
                return 17;
            case T_TEXT:
                int charLen = maxLength > 0 ? maxLength : 255;
                return jet4 ? Math.Max(2, charLen * 2) : charLen;
            case T_BINARY:
                return maxLength > 0 ? maxLength : 255;
            default:
                return 0;
        }
    }

    private static void SetNullMaskBit(byte[] mask, int columnNumber, bool state)
    {
        if (columnNumber < 0)
        {
            return;
        }

        int byteIndex = columnNumber / 8;
        int bitIndex = columnNumber % 8;
        if (byteIndex >= mask.Length)
        {
            return;
        }

        if (state)
        {
            mask[byteIndex] = (byte)(mask[byteIndex] | (1 << bitIndex));
        }
    }

    private static byte[]? WrapInlineLongValue(byte[]? data)
    {
        if (data == null)
        {
            return null;
        }

        var buffer = new byte[12 + data.Length];
        WriteUInt24(buffer, 0, data.Length);
        buffer[3] = 0x80;
        Buffer.BlockCopy(data, 0, buffer, 12, data.Length);
        return buffer;
    }

    private static FileStream CreateStream(string path)
    {
        return new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.RandomAccess);
    }

    private static async ValueTask VerifyPasswordOnOpenAsync(string path, AccessWriterOptions options, CancellationToken cancellationToken)
    {
        var readerOptions = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            UseLockFile = false,
            Password = options.Password,
        };

        try
        {
            await using var reader = await AccessReader.OpenAsync(path, readerOptions, cancellationToken).ConfigureAwait(false);
        }
        catch (UnauthorizedAccessException ex) when (ex.Message.Contains("AccessReaderOptions.Password", StringComparison.Ordinal))
        {
            throw new UnauthorizedAccessException(
                ex.Message.Replace("AccessReaderOptions.Password", "AccessWriterOptions.Password", StringComparison.Ordinal),
                ex);
        }
    }

    private async ValueTask<DataTable> ReadTableSnapshotAsync(string tableName, CancellationToken cancellationToken)
    {
        var options = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            Password = _password,
        };

        AccessReader reader;
        if (!string.IsNullOrEmpty(_path))
        {
            reader = await AccessReader.OpenAsync(_path, options, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            _stream.Position = 0;
            reader = await AccessReader.OpenAsync(_stream, options, leaveOpen: true, cancellationToken).ConfigureAwait(false);
        }

        await using (reader)
        {
            DataTable? snapshot = await reader.ReadDataTableAsync(tableName, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (snapshot != null)
            {
                return snapshot;
            }

            return new DataTable(tableName);
        }
    }

    private protected override async ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken)
    {
        List<CatalogEntry>? cached = GetCatalogCache();
        if (cached != null)
        {
            return cached;
        }

        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            var empty = new List<CatalogEntry>();
            SetCatalogCache(empty);
            return empty;
        }

        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        var result = new List<CatalogEntry>();
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType != OBJ_TABLE)
            {
                continue;
            }

            if ((unchecked((uint)row.Flags) & SYSTABLE_MASK) != 0)
            {
                continue;
            }

            if (string.IsNullOrEmpty(row.Name) || row.TDefPage <= 0)
            {
                continue;
            }

            result.Add(new CatalogEntry(row.Name, row.TDefPage));
        }

        SetCatalogCache(result);
        return result;
    }

    private async ValueTask<CatalogEntry> GetRequiredCatalogEntryAsync(string tableName, CancellationToken cancellationToken)
    {
        CatalogEntry? entry = await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (entry == null)
        {
            throw new InvalidOperationException($"Table '{tableName}' was not found.");
        }

        return entry;
    }

    private TableDef ReadRequiredTableDef(long tdefPage, string tableName)
    {
        TableDef? tableDef = ReadTableDef(tdefPage);
        if (tableDef == null)
        {
            throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");
        }

        return tableDef;
    }

    private async ValueTask<TableDef> ReadRequiredTableDefAsync(long tdefPage, string tableName, CancellationToken cancellationToken)
    {
        TableDef? tableDef = await ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (tableDef == null)
        {
            throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");
        }

        return tableDef;
    }

    private void InsertCatalogEntry(string tableName, long tdefPageNumber)
    {
        TableDef msys = ReadRequiredTableDef(2, "MSysObjects");
        var values = new object[msys.Columns.Count];
        DateTime now = DateTime.UtcNow;

        for (int i = 0; i < msys.Columns.Count; i++)
        {
            values[i] = DBNull.Value;
        }

        SetValue(msys, values, "Id", (int)tdefPageNumber);
        SetValue(msys, values, "ParentId", 0);
        SetValue(msys, values, "Name", tableName);
        SetValue(msys, values, "Type", (short)OBJ_TABLE);
        SetValue(msys, values, "DateCreate", now);
        SetValue(msys, values, "DateUpdate", now);
        SetValue(msys, values, "Flags", 0);

        InsertRowData(2, msys, values);
    }

    private async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, CancellationToken cancellationToken)
    {
        TableDef msys = await ReadRequiredTableDefAsync(2, "MSysObjects", cancellationToken).ConfigureAwait(false);
        var values = new object[msys.Columns.Count];
        DateTime now = DateTime.UtcNow;

        for (int i = 0; i < msys.Columns.Count; i++)
        {
            values[i] = DBNull.Value;
        }

        SetValue(msys, values, "Id", (int)tdefPageNumber);
        SetValue(msys, values, "ParentId", 0);
        SetValue(msys, values, "Name", tableName);
        SetValue(msys, values, "Type", (short)OBJ_TABLE);
        SetValue(msys, values, "DateCreate", now);
        SetValue(msys, values, "DateUpdate", now);
        SetValue(msys, values, "Flags", 0);

        await InsertRowDataAsync(2, msys, values, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private byte[] BuildTDefPage(TableDef tableDef)
    {
        byte[] page = new byte[_pgSz];
        int numCols = tableDef.Columns.Count;
        int colStart = _tdBlockEnd;
        int namePos = colStart + (numCols * _colDescSz);
        int nameLenSize = _jet4 ? 2 : 1;

        page[0] = 0x02;
        page[1] = 0x01;

        // TDEF header fields: offsets are relative to _tdNumCols / _tdNumRealIdx
        // so both JET3 and JET4 layouts are covered.
        page[_tdNumCols - 5] = 0x4E;
        Wu16(page, _tdNumCols - 4, numCols);
        Wu16(page, _tdNumCols, numCols);

        int numVarCols = 0;
        for (int i = 0; i < numCols; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            int o = colStart + (i * _colDescSz);

            if (IsVariableType(col.Type))
            {
                numVarCols++;
            }

            page[o + _colTypeOff] = col.Type;
            Wu16(page, o + _colNumOff, col.ColNum);
            Wu16(page, o + _colVarOff, col.VarIdx);
            page[o + _colFlagsOff] = col.Flags;
            Wu16(page, o + _colFixedOff, col.FixedOff);
            Wu16(page, o + _colSzOff, col.Size);

            byte[] nameBytes = _jet4 ? Encoding.Unicode.GetBytes(col.Name) : _ansiEncoding.GetBytes(col.Name);
            if (namePos + nameLenSize + nameBytes.Length > page.Length)
            {
                throw new NotSupportedException("Table definition does not fit within a single TDEF page.");
            }

            if (_jet4)
            {
                Wu16(page, namePos, nameBytes.Length);
            }
            else
            {
                page[namePos] = (byte)nameBytes.Length;
            }

            namePos += nameLenSize;
            Buffer.BlockCopy(nameBytes, 0, page, namePos, nameBytes.Length);
            namePos += nameBytes.Length;
        }

        Wu16(page, _tdNumCols - 2, numVarCols);
        Wi32(page, 8, Math.Max(0, namePos - 8));
        return page;
    }

    private void InsertRowData(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true)
    {
        if (values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException(
                $"Expected {tableDef.Columns.Count} values for table row but received {values.Length}.",
                nameof(values));
        }

        byte[] rowBytes = SerializeRow(tableDef, values);
        PageInsertTarget target = FindInsertTarget(tdefPage, rowBytes.Length);
        WriteRowToPage(target.PageNumber, target.Page, rowBytes);
        ReturnPage(target.Page);

        if (updateTDefRowCount)
        {
            AdjustTDefRowCount(tdefPage, 1);
        }
    }

    private async ValueTask InsertRowDataAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default)
    {
        if (values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException(
                $"Expected {tableDef.Columns.Count} values for table row but received {values.Length}.",
                nameof(values));
        }

        byte[] rowBytes = SerializeRow(tableDef, values);
        PageInsertTarget target = await FindInsertTargetAsync(tdefPage, rowBytes.Length, cancellationToken).ConfigureAwait(false);
        try
        {
            await WriteRowToPageAsync(target.PageNumber, target.Page, rowBytes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(target.Page);
        }

        if (updateTDefRowCount)
        {
            await AdjustTDefRowCountAsync(tdefPage, 1, cancellationToken).ConfigureAwait(false);
        }
    }

    private void AdjustTDefRowCount(long tdefPage, long delta)
    {
        if (delta == 0)
        {
            return;
        }

        byte[] page = ReadPage(tdefPage);
        long updated;

        try
        {
            uint current = Ru32(page, TDefRowCountOffset);
            updated = Math.Clamp((long)current + delta, 0L, uint.MaxValue);
            Wi32(page, TDefRowCountOffset, unchecked((int)(uint)updated));
            WritePage(tdefPage, page);
        }
        finally
        {
            ReturnPage(page);
        }
    }

    private async ValueTask AdjustTDefRowCountAsync(long tdefPage, long delta, CancellationToken cancellationToken)
    {
        if (delta == 0)
        {
            return;
        }

        byte[] page = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long updated;

        try
        {
            uint current = Ru32(page, TDefRowCountOffset);
            updated = Math.Clamp((long)current + delta, 0L, uint.MaxValue);
            Wi32(page, TDefRowCountOffset, unchecked((int)(uint)updated));
            await WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(page);
        }
    }

    private PageInsertTarget FindInsertTarget(long tdefPage, int rowLength)
    {
        if (TryGetCachedInsertPageNumber(tdefPage, out long cachedPageNumber))
        {
            byte[] cached = ReadPage(cachedPageNumber);
            if (cached[0] == 0x01 && (long)Ri32(cached, _dpTDefOff) == tdefPage && CanInsertRow(cached, rowLength))
            {
                return new PageInsertTarget { PageNumber = cachedPageNumber, Page = cached };
            }

            ReturnPage(cached);
        }

        long total = _stream.Length / _pgSz;
        PageInsertTarget? candidate = null;

        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            byte[] page = ReadPage(pageNumber);
            if (page[0] != 0x01)
            {
                ReturnPage(page);
                continue;
            }

            if ((long)Ri32(page, _dpTDefOff) != tdefPage)
            {
                ReturnPage(page);
                continue;
            }

            if (CanInsertRow(page, rowLength))
            {
                if (candidate != null)
                {
                    ReturnPage(candidate.Page);
                }

                candidate = new PageInsertTarget { PageNumber = pageNumber, Page = page };
            }
            else
            {
                ReturnPage(page);
            }
        }

        if (candidate != null)
        {
            SetCachedInsertPageNumber(tdefPage, candidate.PageNumber);
            return candidate;
        }

        long newPageNumber = AppendPage(CreateEmptyDataPage(tdefPage));
        SetCachedInsertPageNumber(tdefPage, newPageNumber);
        return new PageInsertTarget { PageNumber = newPageNumber, Page = ReadPage(newPageNumber) };
    }

    private async ValueTask<PageInsertTarget> FindInsertTargetAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        if (TryGetCachedInsertPageNumber(tdefPage, out long cachedPageNumber))
        {
            byte[] cached = await ReadPageAsync(cachedPageNumber, cancellationToken).ConfigureAwait(false);
            if (cached[0] == 0x01 && (long)Ri32(cached, _dpTDefOff) == tdefPage && CanInsertRow(cached, rowLength))
            {
                return new PageInsertTarget { PageNumber = cachedPageNumber, Page = cached };
            }

            ReturnPage(cached);
        }

        long total = _stream.Length / _pgSz;
        PageInsertTarget? candidate = null;

        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01)
            {
                ReturnPage(page);
                continue;
            }

            if ((long)Ri32(page, _dpTDefOff) != tdefPage)
            {
                ReturnPage(page);
                continue;
            }

            if (CanInsertRow(page, rowLength))
            {
                if (candidate != null)
                {
                    ReturnPage(candidate.Page);
                }

                candidate = new PageInsertTarget { PageNumber = pageNumber, Page = page };
            }
            else
            {
                ReturnPage(page);
            }
        }

        if (candidate != null)
        {
            SetCachedInsertPageNumber(tdefPage, candidate.PageNumber);
            return candidate;
        }

        long newPageNumber = await AppendPageAsync(CreateEmptyDataPage(tdefPage), cancellationToken).ConfigureAwait(false);
        SetCachedInsertPageNumber(tdefPage, newPageNumber);
        return new PageInsertTarget
        {
            PageNumber = newPageNumber,
            Page = await ReadPageAsync(newPageNumber, cancellationToken).ConfigureAwait(false),
        };
    }

    private bool CanInsertRow(byte[] page, int rowLength)
    {
        int numRows = Ru16(page, _dpNumRows);
        int dataStart = GetFirstRowStart(page, numRows);
        int nextOffsetPos = _dpRowsStart + ((numRows + 1) * 2);
        return dataStart - nextOffsetPos >= rowLength;
    }

    private int GetFirstRowStart(byte[] page, int numRows)
    {
        int first = _pgSz;
        for (int i = 0; i < numRows; i++)
        {
            int raw = Ru16(page, _dpRowsStart + (i * 2));
            int start = raw & 0x1FFF;
            if (start > 0 && start < first)
            {
                first = start;
            }
        }

        return first;
    }

    private byte[] CreateEmptyDataPage(long tdefPage)
    {
        byte[] page = new byte[_pgSz];
        page[0] = 0x01;
        page[1] = 0x01;
        Wu16(page, 2, _pgSz - _dpRowsStart);
        Wi32(page, _dpTDefOff, (int)tdefPage);
        Wu16(page, _dpNumRows, 0);
        return page;
    }

    private void WriteRowToPage(long pageNumber, byte[] page, byte[] rowBytes)
    {
        int numRows = Ru16(page, _dpNumRows);
        int firstRowStart = GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = _dpRowsStart + (numRows * 2);

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, _dpNumRows, numRows + 1);

        int freeSpace = rowStart - (_dpRowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Wu16(page, 2, freeSpace);
        WritePage(pageNumber, page);
    }

    private async ValueTask WriteRowToPageAsync(long pageNumber, byte[] page, byte[] rowBytes, CancellationToken cancellationToken)
    {
        int numRows = Ru16(page, _dpNumRows);
        int firstRowStart = GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = _dpRowsStart + (numRows * 2);

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, _dpNumRows, numRows + 1);

        int freeSpace = rowStart - (_dpRowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Wu16(page, 2, freeSpace);
        await WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
    }

    private byte[] SerializeRow(TableDef tableDef, object[] values)
    {
        int numCols = 0;
        int maxFixedEnd = 0;
        int maxDefinedVarIdx = -1;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            numCols = Math.Max(numCols, col.ColNum + 1);
            if (col.IsFixed && col.Type != T_BOOL)
            {
                maxFixedEnd = Math.Max(maxFixedEnd, col.FixedOff + FixedSize(col.Type, col.Size));
            }
            else if (!col.IsFixed)
            {
                maxDefinedVarIdx = Math.Max(maxDefinedVarIdx, col.VarIdx);
            }
        }

        var nullMask = new byte[(numCols + 7) / 8];
        var fixedArea = new byte[maxFixedEnd];
        int fixedAreaSize = 0;
        var varEntries = maxDefinedVarIdx >= 0 ? new byte[maxDefinedVarIdx + 1][] : Array.Empty<byte[]>();
        int maxVarIndex = -1;
        int varPayloadSize = 0;

        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo column = tableDef.Columns[i];
            object value = values[i] ?? DBNull.Value;

            if (column.Type == T_BOOL)
            {
                if (value is not DBNull && Convert.ToBoolean(value, CultureInfo.InvariantCulture))
                {
                    SetNullMaskBit(nullMask, column.ColNum, true);
                }

                continue;
            }

            if (value is DBNull)
            {
                continue;
            }

            if (column.IsFixed)
            {
                if (!CanStoreFixedColumn(column))
                {
                    continue;
                }

                byte[]? fixedValue = EncodeFixedValue(column, value);
                if (fixedValue == null)
                {
                    continue;
                }

                Buffer.BlockCopy(fixedValue, 0, fixedArea, column.FixedOff, fixedValue.Length);
                fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + fixedValue.Length);
                SetNullMaskBit(nullMask, column.ColNum, true);
            }
            else
            {
                byte[]? variableValue = EncodeVariableValue(column, value);
                if (variableValue == null)
                {
                    continue;
                }

                varEntries[column.VarIdx] = variableValue;
                maxVarIndex = Math.Max(maxVarIndex, column.VarIdx);
                varPayloadSize += variableValue.Length;
                SetNullMaskBit(nullMask, column.ColNum, true);
            }
        }

        int varLen = maxVarIndex + 1;
        int baseRowLength = _numColsFldSz + fixedAreaSize + varPayloadSize + _eodFldSz + (varLen * _varEntrySz) + _varLenFldSz + nullMask.Length;

        // Jet3 rows include a jump table whose size depends on total row length.
        int jumpSize = _jet4 ? 0 : baseRowLength / 256;
        int rowLength = baseRowLength + jumpSize;
        int finalJump = _jet4 ? 0 : rowLength / 256;
        if (finalJump != jumpSize)
        {
            jumpSize = finalJump;
            rowLength = baseRowLength + jumpSize;
        }

        var row = new byte[rowLength];
        int pos = 0;

        WriteField(row, pos, _numColsFldSz, numCols);
        pos += _numColsFldSz;

        if (fixedAreaSize > 0)
        {
            Buffer.BlockCopy(fixedArea, 0, row, pos, fixedAreaSize);
            pos += fixedAreaSize;
        }

        int currentOffset = _numColsFldSz + fixedAreaSize;
        var variableOffsets = varLen > 0 ? new int[varLen] : Array.Empty<int>();
        for (int varIndex = 0; varIndex < varLen; varIndex++)
        {
            variableOffsets[varIndex] = currentOffset;
            byte[]? payload = varEntries[varIndex];
            if (payload != null)
            {
                Buffer.BlockCopy(payload, 0, row, pos, payload.Length);
                pos += payload.Length;
                currentOffset += payload.Length;
            }
        }

        WriteField(row, pos, _eodFldSz, currentOffset);
        pos += _eodFldSz;

        for (int varIndex = varLen - 1; varIndex >= 0; varIndex--)
        {
            WriteField(row, pos, _varEntrySz, variableOffsets[varIndex]);
            pos += _varEntrySz;
        }

        // Jet3 jump table (entries are zero for newly written rows).
        pos += jumpSize;

        WriteField(row, pos, _varLenFldSz, varLen);
        pos += _varLenFldSz;
        Buffer.BlockCopy(nullMask, 0, row, pos, nullMask.Length);

        return row;
    }

    private bool CanStoreFixedColumn(ColumnInfo column)
    {
        int size = FixedSize(column.Type, column.Size);
        return size >= 0 && column.FixedOff >= 0 && column.FixedOff + size < _pgSz;
    }

    private byte[]? EncodeFixedValue(ColumnInfo column, object value) => column.Type switch
    {
        T_BYTE => [Convert.ToByte(value, CultureInfo.InvariantCulture)],
        T_INT => BitConverter.GetBytes(Convert.ToInt16(value, CultureInfo.InvariantCulture)),
        T_LONG => BitConverter.GetBytes(Convert.ToInt32(value, CultureInfo.InvariantCulture)),
        T_FLOAT => BitConverter.GetBytes(Convert.ToSingle(value, CultureInfo.InvariantCulture)),
        T_DOUBLE => BitConverter.GetBytes(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        T_DATETIME => BitConverter.GetBytes(Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToOADate()),
        T_MONEY => BitConverter.GetBytes(decimal.ToInt64(decimal.Round(
            Convert.ToDecimal(value, CultureInfo.InvariantCulture) * 10000m, 0, MidpointRounding.AwayFromZero))),
        T_NUMERIC => EncodeNumericValue(Convert.ToDecimal(value, CultureInfo.InvariantCulture)),
        T_GUID => (value is Guid g ? g : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!)).ToByteArray(),
        _ => null,
    };

    private byte[]? EncodeVariableValue(ColumnInfo column, object value)
    {
        switch (column.Type)
        {
            case T_TEXT:
                return EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.Size);
            case T_BINARY:
                return EncodeBinaryValue(value, column.Size);
            case T_MEMO:
                return EncodeMemoValue(Convert.ToString(value, CultureInfo.InvariantCulture));
            case T_OLE:
                return EncodeOleValue(value);
            default:
                return null;
        }
    }

    private byte[]? EncodeTextValue(string? value, int maxSize)
    {
        if (value == null)
        {
            return null;
        }

        byte[] bytes = _jet4 ? Encoding.Unicode.GetBytes(value) : _ansiEncoding.GetBytes(value);
        if (maxSize > 0 && bytes.Length > maxSize)
        {
            int allowed = _jet4 ? maxSize & ~1 : maxSize;
            if (allowed <= 0)
            {
                return [];
            }

            Array.Resize(ref bytes, allowed);
        }

        return bytes;
    }

    private byte[]? EncodeBinaryValue(object value, int maxSize)
    {
        byte[]? bytes = value as byte[];
        if (bytes == null)
        {
            string? stringValue = Convert.ToString(value, CultureInfo.InvariantCulture);
            if (string.IsNullOrEmpty(stringValue))
            {
                return null;
            }

            bytes = _ansiEncoding.GetBytes(stringValue);
        }

        if (maxSize > 0 && bytes.Length > maxSize)
        {
            Array.Resize(ref bytes, maxSize);
        }

        return bytes;
    }

    private byte[]? EncodeMemoValue(string? value)
    {
        if (value == null)
        {
            return null;
        }

        byte[] data = _jet4 ? Encoding.Unicode.GetBytes(value) : _ansiEncoding.GetBytes(value);
        if (data.Length > MaxInlineMemoBytes)
        {
            throw new JetLimitationException($"MEMO value is {data.Length} bytes, which exceeds the inline limit of {MaxInlineMemoBytes} bytes.");
        }

        return WrapInlineLongValue(data);
    }

    private byte[] EncodeNumericValue(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        int flags = bits[3];
        bool negative = (flags & unchecked((int)0x80000000)) != 0;
        byte scale = (byte)((flags >> 16) & 0x7F);

        byte precision = 1;
        var mantissa = new decimal(bits[0], bits[1], bits[2], isNegative: false, scale: 0);
        while (mantissa >= 10m)
        {
            mantissa = decimal.Truncate(mantissa / 10m);
            precision++;
        }

        if (precision > 28)
        {
            precision = 28;
        }

        var buffer = new byte[17];
        buffer[0] = precision;
        buffer[1] = scale;
        buffer[2] = negative ? (byte)1 : (byte)0;

        Wi32(buffer, 4, bits[0]);
        Wi32(buffer, 8, bits[1]);
        Wi32(buffer, 12, bits[2]);
        return buffer;
    }

    private void InvalidateCatalogCache()
    {
        _stateLock.EnterWriteLock();
        try
        {
            _catalogCache = null;
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    private List<CatalogEntry>? GetCatalogCache()
    {
        _stateLock.EnterReadLock();
        try
        {
            return _catalogCache;
        }
        finally
        {
            _stateLock.ExitReadLock();
        }
    }

    private void SetCatalogCache(List<CatalogEntry> cache)
    {
        _stateLock.EnterWriteLock();
        try
        {
            _catalogCache = cache;
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    private bool TryGetCachedInsertPageNumber(long tdefPage, out long pageNumber)
    {
        _stateLock.EnterReadLock();
        try
        {
            if (_cachedInsertTDefPage == tdefPage && _cachedInsertPageNumber >= 3)
            {
                pageNumber = _cachedInsertPageNumber;
                return true;
            }

            pageNumber = -1;
            return false;
        }
        finally
        {
            _stateLock.ExitReadLock();
        }
    }

    private void SetCachedInsertPageNumber(long tdefPage, long pageNumber)
    {
        _stateLock.EnterWriteLock();
        try
        {
            _cachedInsertTDefPage = tdefPage;
            _cachedInsertPageNumber = pageNumber;
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    private IEnumerable<CatalogRow> EnumerateCatalogRows(TableDef msys)
    {
        ColumnInfo idColumn = msys.Columns.Find(c => string.Equals(c.Name, "Id", StringComparison.OrdinalIgnoreCase));
        ColumnInfo nameColumn = msys.Columns.Find(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
        ColumnInfo typeColumn = msys.Columns.Find(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));
        ColumnInfo flagsColumn = msys.Columns.Find(c => string.Equals(c.Name, "Flags", StringComparison.OrdinalIgnoreCase));
        if (nameColumn == null || typeColumn == null)
        {
            yield break;
        }

        long total = _stream.Length / _pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            byte[] page = ReadPage(pageNumber);
            if (page[0] != 0x01)
            {
                ReturnPage(page);
                continue;
            }

            if (Ri32(page, _dpTDefOff) != 2)
            {
                ReturnPage(page);
                continue;
            }

            foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
            {
                yield return new CatalogRow
                {
                    PageNumber = row.PageNumber,
                    RowIndex = row.RowIndex,
                    Name = ReadColumnValue(page, row.RowStart, row.RowSize, nameColumn),
                    ObjectType = ParseInt32(ReadColumnValue(page, row.RowStart, row.RowSize, typeColumn)),
                    Flags = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, flagsColumn)),
                    TDefPage = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, idColumn)) & 0x00FFFFFFL,
                };
            }
        }
    }

    private async ValueTask<List<CatalogRow>> GetCatalogRowsAsync(TableDef msys, CancellationToken cancellationToken)
    {
        ColumnInfo idColumn = msys.Columns.Find(c => string.Equals(c.Name, "Id", StringComparison.OrdinalIgnoreCase));
        ColumnInfo nameColumn = msys.Columns.Find(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
        ColumnInfo typeColumn = msys.Columns.Find(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));
        ColumnInfo flagsColumn = msys.Columns.Find(c => string.Equals(c.Name, "Flags", StringComparison.OrdinalIgnoreCase));
        if (nameColumn == null || typeColumn == null)
        {
            return [];
        }

        var result = new List<CatalogRow>();
        long total = _stream.Length / _pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01)
            {
                ReturnPage(page);
                continue;
            }

            if (Ri32(page, _dpTDefOff) != 2)
            {
                ReturnPage(page);
                continue;
            }

            foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
            {
                result.Add(new CatalogRow
                {
                    PageNumber = row.PageNumber,
                    RowIndex = row.RowIndex,
                    Name = ReadColumnValue(page, row.RowStart, row.RowSize, nameColumn),
                    ObjectType = ParseInt32(ReadColumnValue(page, row.RowStart, row.RowSize, typeColumn)),
                    Flags = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, flagsColumn)),
                    TDefPage = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, idColumn)) & 0x00FFFFFFL,
                });
            }

            ReturnPage(page);
        }

        return result;
    }

    private string ReadColumnValue(byte[] page, int rowStart, int rowSize, ColumnInfo column)
    {
        if (column == null || rowSize < _numColsFldSz)
        {
            return string.Empty;
        }

        int numCols = _jet4 ? Ru16(page, rowStart) : page[rowStart];
        if (numCols == 0)
        {
            return string.Empty;
        }

        int nullMaskSz = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSz;
        if (nullMaskPos < _numColsFldSz)
        {
            return string.Empty;
        }

        int varLenPos = nullMaskPos - _varLenFldSz;
        if (varLenPos < _numColsFldSz)
        {
            return string.Empty;
        }

        int varLen = _jet4 ? Ru16(page, rowStart + varLenPos) : page[rowStart + varLenPos];
        int jumpSize = _jet4 ? 0 : rowSize / 256;
        int varTableStart = varLenPos - jumpSize - (varLen * _varEntrySz);
        int eodPos = varTableStart - _eodFldSz;
        if (eodPos < _numColsFldSz)
        {
            return string.Empty;
        }

        int eod = _jet4 ? Ru16(page, rowStart + eodPos) : page[rowStart + eodPos];
        bool nullBit = false;
        if (column.ColNum < numCols)
        {
            int mByte = nullMaskPos + (column.ColNum / 8);
            int mBit = column.ColNum % 8;
            if (mByte < rowSize)
            {
                nullBit = (page[rowStart + mByte] & (1 << mBit)) != 0;
            }
        }

        if (column.Type == T_BOOL)
        {
            return nullBit ? "True" : "False";
        }

        if (column.ColNum >= numCols || !nullBit)
        {
            return string.Empty;
        }

        if (column.IsFixed)
        {
            int start = _numColsFldSz + column.FixedOff;
            int size = FixedSize(column.Type, column.Size);
            if (size == 0 || start + size > rowSize)
            {
                return string.Empty;
            }

            return ReadFixedString(page, rowStart + start, column.Type, size);
        }

        if (column.VarIdx >= varLen)
        {
            return string.Empty;
        }

        int entryPos = varTableStart + ((varLen - 1 - column.VarIdx) * _varEntrySz);
        if (entryPos < 0 || entryPos + _varEntrySz > rowSize)
        {
            return string.Empty;
        }

        int varOff = _jet4 ? Ru16(page, rowStart + entryPos) : page[rowStart + entryPos];
        int varEnd;
        if (column.VarIdx + 1 < varLen)
        {
            int nextEntry = varTableStart + ((varLen - 2 - column.VarIdx) * _varEntrySz);
            varEnd = _jet4 ? Ru16(page, rowStart + nextEntry) : page[rowStart + nextEntry];
        }
        else
        {
            varEnd = eod;
        }

        int dataStart = varOff;
        int dataLen = varEnd - varOff;
        if (dataLen <= 0 || dataStart < 0 || dataStart + dataLen > rowSize)
        {
            return string.Empty;
        }

        switch (column.Type)
        {
            case T_TEXT:
                return _jet4 ? DecodeJet4Text(page, rowStart + dataStart, dataLen) : _ansiEncoding.GetString(page, rowStart + dataStart, dataLen);
            case T_BINARY:
                return BitConverter.ToString(page, rowStart + dataStart, dataLen);
            default:
                return string.Empty;
        }
    }

    private int ParseInt32(string value)
    {
        int parsed;
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) ? parsed : 0;
    }

    private long ParseInt64(string value)
    {
        long parsed;
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) ? parsed : 0L;
    }

    private async ValueTask<List<RowLocation>> GetLiveRowLocationsAsync(long tdefPage, CancellationToken cancellationToken)
    {
        var result = new List<RowLocation>();
        long total = _stream.Length / _pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01)
            {
                ReturnPage(page);
                continue;
            }

            if ((long)Ri32(page, _dpTDefOff) != tdefPage)
            {
                ReturnPage(page);
                continue;
            }

            result.AddRange(EnumerateLiveRowLocations(pageNumber, page));
            ReturnPage(page);
        }

        return result;
    }

    private IEnumerable<RowLocation> EnumerateLiveRowLocations(long pageNumber, byte[] page)
    {
        foreach (RowBound rb in EnumerateLiveRowBounds(page))
        {
            yield return new RowLocation(pageNumber, rb.RowIndex, rb.RowStart, rb.RowSize);
        }
    }

    private async ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, CancellationToken cancellationToken)
    {
        byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        int offsetPos = _dpRowsStart + (rowIndex * 2);
        int raw = Ru16(page, offsetPos);
        if ((raw & 0x8000) != 0)
        {
            ReturnPage(page);
            return;
        }

        Wu16(page, offsetPos, raw | 0x8000);
        await WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        ReturnPage(page);
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
            if (_respectExistingLockFile && File.Exists(lockPath))
            {
                throw new IOException($"Database is already in use. A lockfile exists at: {lockPath}");
            }

            using var fs = new FileStream(lockPath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        }
        catch (IOException) when (!_respectExistingLockFile)
        {
            // Best-effort: if another process holds the lock, continue without it.
        }
        catch (UnauthorizedAccessException) when (!_respectExistingLockFile)
        {
            // Best-effort: if we lack permission, continue without it.
        }
    }

    private void DeleteLockFile()
    {
        string lockPath = GetLockFilePath();
        try
        {
            File.Delete(lockPath);
        }
        catch (IOException)
        {
            // Best-effort: file may be held by another process.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort: we may lack permission.
        }
    }

    private readonly record struct RowLocation(long PageNumber, int RowIndex, int RowStart, int RowSize)
    {
        public long PageNumber { get; } = PageNumber;

        public int RowIndex { get; } = RowIndex;

        public int RowStart { get; } = RowStart;

        public int RowSize { get; } = RowSize;
    }

    private sealed class CatalogRow
    {
        public long PageNumber { get; set; }

        public int RowIndex { get; set; }

        public string Name { get; set; } = string.Empty;

        public int ObjectType { get; set; }

        public long Flags { get; set; }

        public long TDefPage { get; set; }
    }

    private sealed class PageInsertTarget
    {
        public long PageNumber { get; set; }

        public byte[] Page { get; set; } = [];
    }
}
