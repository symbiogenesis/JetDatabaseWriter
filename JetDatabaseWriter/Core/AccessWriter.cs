namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal;

#pragma warning disable CA1822 // Mark members as static
#pragma warning disable SA1202 // Keep member order stable while synchronous APIs remain private compatibility helpers
#pragma warning disable SA1204 // Static members grouped logically alongside related instance members
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

    private readonly SecureString? _password;
    private readonly bool _useLockFile;
    private readonly bool _respectExistingLockFile;
    private readonly ReaderWriterLockSlim _stateLock = new(LockRecursionPolicy.NoRecursion);

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
        : base(stream, header, path)
    {
        _password = SecureStringUtilities.CopyAsReadOnly(password);
        _useLockFile = useLockFile && !string.IsNullOrEmpty(path);
        _respectExistingLockFile = respectExistingLockFile;

        if (_useLockFile)
        {
            LockFileManager.Create(_path, nameof(AccessWriter), _respectExistingLockFile);
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

    /// <summary>
    /// Asynchronously creates a new, empty JET database file at the specified path
    /// and returns a new <see cref="AccessWriter"/> ready for table creation and data insertion.
    /// The file must not already exist.
    /// </summary>
    /// <param name="path">Path where the new .mdb or .accdb file will be created.</param>
    /// <param name="format">The database format to use (Jet4 .mdb or ACE .accdb).</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the new database.</returns>
    public static async ValueTask<AccessWriter> CreateDatabaseAsync(string path, DatabaseFormat format, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();

        if (File.Exists(path))
        {
            throw new IOException($"Database file already exists: {path}");
        }

        byte[] dbBytes = BuildEmptyDatabase(format);

        await using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
        {
            await fs.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            return await OpenAsync(path, options, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup of the partially-created file.
            }
            catch (UnauthorizedAccessException)
            {
                // Best-effort cleanup if we lack permission.
            }

            throw;
        }
    }

    /// <summary>
    /// Asynchronously writes a new, empty JET database into the specified stream
    /// and returns a new <see cref="AccessWriter"/> ready for table creation and data insertion.
    /// The stream must be readable, writable, and seekable.
    /// </summary>
    /// <param name="stream">A writable, seekable stream to write the new database into.</param>
    /// <param name="format">The database format to use (Jet4 .mdb or ACE .accdb).</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="leaveOpen">If <c>true</c>, the stream is not disposed when the writer is disposed. Default is <c>false</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the new database.</returns>
    public static async ValueTask<AccessWriter> CreateDatabaseAsync(Stream stream, DatabaseFormat format, AccessWriterOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
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

        byte[] dbBytes = BuildEmptyDatabase(format);
        await stream.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        stream.Position = 0;

        return await OpenAsync(stream, options, leaveOpen, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required", nameof(columns));
        }

        if (await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        TableDef tableDef = BuildTableDefinition(columns, _format);
        byte[] tdefPage = BuildTDefPage(tableDef);
        long tdefPageNumber = await AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        await InsertCatalogEntryAsync(tableName, tdefPageNumber, cancellationToken).ConfigureAwait(false);
        InvalidateCatalogCache();
    }

    /// <inheritdoc/>
    public async ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.ThrowIfDisposed(_disposed, this);
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
    public ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(column, nameof(column));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        return RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                if (existing.Exists(c => string.Equals(c.Name, column.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidOperationException($"Column '{column.Name}' already exists in table '{tableName}'.");
                }

                var next = new List<ColumnDefinition>(existing);
                next.Add(column);
                return next;
            },
            (oldRow, _) =>
            {
                var next = new object[oldRow.Length + 1];
                Array.Copy(oldRow, 0, next, 0, oldRow.Length);
                next[oldRow.Length] = DBNull.Value;
                return next;
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask DropColumnAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        int dropIndex = -1;
        return RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                dropIndex = existing.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
                if (dropIndex < 0)
                {
                    throw new ArgumentException($"Column '{columnName}' was not found in table '{tableName}'.", nameof(columnName));
                }

                if (existing.Count == 1)
                {
                    throw new InvalidOperationException($"Cannot drop the last remaining column from table '{tableName}'.");
                }

                var next = new List<ColumnDefinition>(existing);
                next.RemoveAt(dropIndex);
                return next;
            },
            (oldRow, _) =>
            {
                var next = new object[oldRow.Length - 1];
                int j = 0;
                for (int i = 0; i < oldRow.Length; i++)
                {
                    if (i == dropIndex)
                    {
                        continue;
                    }

                    next[j++] = oldRow[i];
                }

                return next;
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(oldColumnName, nameof(oldColumnName));
        Guard.NotNullOrEmpty(newColumnName, nameof(newColumnName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        return RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                int idx = existing.FindIndex(c => string.Equals(c.Name, oldColumnName, StringComparison.OrdinalIgnoreCase));
                if (idx < 0)
                {
                    throw new ArgumentException($"Column '{oldColumnName}' was not found in table '{tableName}'.", nameof(oldColumnName));
                }

                if (existing.Exists(c => string.Equals(c.Name, newColumnName, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidOperationException($"Column '{newColumnName}' already exists in table '{tableName}'.");
                }

                var next = new List<ColumnDefinition>(existing);
                ColumnDefinition src = next[idx];
                next[idx] = new ColumnDefinition(newColumnName, src.ClrType, src.MaxLength);
                return next;
            },
            (oldRow, _) => oldRow,
            cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask InsertRowAsync(string tableName, object[] values, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(values, nameof(values));
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
        Guard.ThrowIfDisposed(_disposed, this);
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
    public async ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object> updatedValues, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        Guard.ThrowIfDisposed(_disposed, this);
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
    public async ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        Guard.ThrowIfDisposed(_disposed, this);
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
            LockFileManager.Delete(_path, nameof(AccessWriter));
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

    private static bool ValuesEqual(object? left, object? right)
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

    private static TableDef BuildTableDefinition(IReadOnlyList<ColumnDefinition> columns, DatabaseFormat format)
    {
        var result = new TableDef();
        int fixedOffset = 0;
        int nextVarIndex = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition definition = columns[i];
            byte type = TypeCodeFromDefinition(definition);
            bool variable = IsVariableType(type);
            int size = GetDeclaredSize(type, definition.MaxLength, format);

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

    private static int GetDeclaredSize(byte type, int maxLength, DatabaseFormat format)
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
                return format != DatabaseFormat.Jet3Mdb ? Math.Max(2, charLen * 2) : charLen;
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

    private static async ValueTask VerifyPasswordOnOpenAsync(string path, AccessWriterOptions options, CancellationToken cancellationToken = default)
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

    private async ValueTask<DataTable> ReadTableSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
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
            return await reader.ReadDataTableAsync(tableName, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    private protected override async ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken = default)
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

    private async ValueTask<CatalogEntry> GetRequiredCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
    {
        CatalogEntry? entry = await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (entry == null)
        {
            throw new InvalidOperationException($"Table '{tableName}' was not found.");
        }

        return entry;
    }

    private async ValueTask<TableDef> ReadRequiredTableDefAsync(long tdefPage, string tableName, CancellationToken cancellationToken = default)
    {
        TableDef? tableDef = await ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (tableDef == null)
        {
            throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");
        }

        return tableDef;
    }

    private async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, CancellationToken cancellationToken = default)
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

    private async ValueTask RewriteTableAsync(
        string tableName,
        Func<List<ColumnDefinition>, TableDef, List<ColumnDefinition>> projectColumns,
        Func<object[], TableDef, object[]> projectRow,
        CancellationToken cancellationToken)
    {
        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        var existingDefs = new List<ColumnDefinition>(tableDef.Columns.Count);
        foreach (ColumnInfo col in tableDef.Columns)
        {
            existingDefs.Add(BuildColumnDefinitionFromInfo(col));
        }

        List<ColumnDefinition> newDefs = projectColumns(existingDefs, tableDef);
        if (newDefs.Count == 0)
        {
            throw new InvalidOperationException($"Table '{tableName}' must retain at least one column.");
        }

        // Snapshot existing rows BEFORE we mutate the catalog so the snapshot reader
        // sees the original schema.
        using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        string tempName = $"~tmp_{Guid.NewGuid():N}".Substring(0, 18);
        await CreateTableAsync(tempName, newDefs, cancellationToken).ConfigureAwait(false);

        CatalogEntry tempEntry = await GetRequiredCatalogEntryAsync(tempName, cancellationToken).ConfigureAwait(false);
        TableDef tempDef = await ReadRequiredTableDefAsync(tempEntry.TDefPage, tempName, cancellationToken).ConfigureAwait(false);

        foreach (DataRow row in snapshot.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object?[] sourceItems = row.ItemArray;
            var sourceRow = new object[sourceItems.Length];
            for (int i = 0; i < sourceItems.Length; i++)
            {
                sourceRow[i] = sourceItems[i] ?? DBNull.Value;
            }

            object[] projected = projectRow(sourceRow, tableDef);
            await InsertRowDataAsync(tempEntry.TDefPage, tempDef, projected, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        // Drop the original table, then rename the temp catalog entry to take its place.
        await DropTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        await RenameTableInCatalogAsync(tempName, tableName, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask RenameTableInCatalogAsync(string oldName, string newName, CancellationToken cancellationToken)
    {
        TableDef msys = await ReadRequiredTableDefAsync(2, "MSysObjects", cancellationToken).ConfigureAwait(false);
        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);

        long? tdefPage = null;
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType != OBJ_TABLE)
            {
                continue;
            }

            if (!string.Equals(row.Name, oldName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            tdefPage = row.TDefPage;
            await MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            break;
        }

        if (tdefPage == null)
        {
            throw new InvalidOperationException($"Catalog row for '{oldName}' was not found during rename.");
        }

        await InsertCatalogEntryAsync(newName, tdefPage.Value, cancellationToken).ConfigureAwait(false);
        InvalidateCatalogCache();
    }

    private ColumnDefinition BuildColumnDefinitionFromInfo(ColumnInfo column)
    {
        switch (column.Type)
        {
            case T_BOOL: return new ColumnDefinition(column.Name, typeof(bool));
            case T_BYTE: return new ColumnDefinition(column.Name, typeof(byte));
            case T_INT: return new ColumnDefinition(column.Name, typeof(short));
            case T_LONG: return new ColumnDefinition(column.Name, typeof(int));
            case T_MONEY: return new ColumnDefinition(column.Name, typeof(decimal));
            case T_FLOAT: return new ColumnDefinition(column.Name, typeof(float));
            case T_DOUBLE: return new ColumnDefinition(column.Name, typeof(double));
            case T_DATETIME: return new ColumnDefinition(column.Name, typeof(DateTime));
            case T_NUMERIC: return new ColumnDefinition(column.Name, typeof(decimal));
            case T_GUID: return new ColumnDefinition(column.Name, typeof(Guid));
            case T_TEXT:
                int charLen = _format != DatabaseFormat.Jet3Mdb ? Math.Max(1, column.Size / 2) : Math.Max(1, column.Size);
                return new ColumnDefinition(column.Name, typeof(string), charLen);
            case T_MEMO: return new ColumnDefinition(column.Name, typeof(string));
            case T_BINARY: return new ColumnDefinition(column.Name, typeof(byte[]), column.Size > 0 ? column.Size : 255);
            case T_OLE: return new ColumnDefinition(column.Name, typeof(byte[]));
            case T_ATTACHMENT:
            case T_COMPLEX:
                throw new NotSupportedException($"Column '{column.Name}' has a complex type (attachment / multi-value) that cannot be rewritten by AddColumnAsync / DropColumnAsync / RenameColumnAsync.");
            default:
                throw new NotSupportedException($"Column '{column.Name}' has unsupported type code 0x{column.Type:X2}.");
        }
    }

    private byte[] BuildTDefPage(TableDef tableDef)
    {
        byte[] page = new byte[_pgSz];
        int numCols = tableDef.Columns.Count;
        int colStart = _tdBlockEnd;
        int namePos = colStart + (numCols * _colDescSz);
        int nameLenSize = _format != DatabaseFormat.Jet3Mdb ? 2 : 1;

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

            byte[] nameBytes = _format != DatabaseFormat.Jet3Mdb ? Encoding.Unicode.GetBytes(col.Name) : _ansiEncoding.GetBytes(col.Name);
            if (namePos + nameLenSize + nameBytes.Length > page.Length)
            {
                throw new NotSupportedException("Table definition does not fit within a single TDEF page.");
            }

            if (_format != DatabaseFormat.Jet3Mdb)
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

    /// <summary>
    /// Builds a minimal, empty JET database as a byte array.
    /// The database contains three pages (page size varies by format):
    /// page 0 (header), page 1 (unused placeholder), and page 2 (MSysObjects TDEF).
    /// </summary>
    private static byte[] BuildEmptyDatabase(DatabaseFormat format)
    {
        int pgSz = GetPageSize(format);
        byte[] db = new byte[pgSz * 3];

        // ── Page 0: JET header ─────────────────────────────────────
        db[0] = 0x00;
        db[1] = 0x01;
        db[2] = 0x00;
        db[3] = 0x00;

        byte[] magic = format == DatabaseFormat.AceAccdb
            ? Encoding.ASCII.GetBytes("Standard ACE DB\0")
            : Encoding.ASCII.GetBytes("Standard Jet DB\0");
        Buffer.BlockCopy(magic, 0, db, 4, magic.Length);

        // Offset 0x14: 0 = Jet3, 1 = Jet4, 2 = ACE
        db[0x14] = format switch
        {
            DatabaseFormat.Jet3Mdb => 0x00,
            DatabaseFormat.AceAccdb => 0x02,
            _ => 0x01,
        };

        // Sort order / code page left as 0x0000 → defaults to 1252.
        // Jet4/ACE at 0x3C, Jet3 at 0x3A — both are already zero.

        // ── Page 1: placeholder (left as zeros / unused page type) ──
        // Must exist so that page 2 sits at the correct file offset.

        // ── Page 2: TDEF for MSysObjects ───────────────────────────
        BuildMSysObjectsTDef(db, pgSz * 2, format);

        return db;
    }

    /// <summary>
    /// Writes a minimal MSysObjects TDEF page into <paramref name="db"/> at the given
    /// <paramref name="offset"/>. The TDEF defines nine columns: Id, ParentId, Name,
    /// Type, DateCreate, DateUpdate, Flags, ForeignName, and Database.
    /// </summary>
    private static void BuildMSysObjectsTDef(byte[] db, int offset, DatabaseFormat format)
    {
        bool isJet3 = format == DatabaseFormat.Jet3Mdb;

        // TDEF format constants (must match the values in AccessBase)
        int tdNumCols = isJet3 ? 25 : 45;
        int tdBlockEnd = isJet3 ? 43 : 63;
        int colDescSz = isJet3 ? 18 : 25;
        int colTypeOff = 0;
        int colNumOff = isJet3 ? 1 : 5;
        int colVarOff = isJet3 ? 3 : 7;
        int colFlagsOff = isJet3 ? 13 : 15;
        int colFixedOff = isJet3 ? 14 : 21;
        int colSzOff = isJet3 ? 16 : 23;
        int textColSize = isJet3 ? 255 : 510;

        // MSysObjects columns.
        // Fixed: Id(T_LONG,4), ParentId(T_LONG,4), Type(T_INT,2),
        //        DateCreate(T_DATETIME,8), DateUpdate(T_DATETIME,8), Flags(T_LONG,4).
        // Variable: Name(T_TEXT), ForeignName(T_TEXT), Database(T_TEXT).
        var columns = new (string Name, byte Type, int ColNum, int VarIdx, int FixedOff, int Size, byte Flags)[]
        {
            ("Id",          T_LONG,     0, 0, 0,  4,            0x03),
            ("ParentId",    T_LONG,     1, 0, 4,  4,            0x03),
            ("Name",        T_TEXT,     2, 0, 0,  textColSize,  0x02),
            ("Type",        T_INT,      3, 0, 8,  2,            0x03),
            ("DateCreate",  T_DATETIME, 4, 0, 10, 8,            0x03),
            ("DateUpdate",  T_DATETIME, 5, 0, 18, 8,            0x03),
            ("Flags",       T_LONG,     6, 0, 26, 4,            0x03),
            ("ForeignName", T_TEXT,     7, 1, 0,  textColSize,  0x02),
            ("Database",    T_TEXT,     8, 2, 0,  textColSize,  0x02),
        };

        int numCols = columns.Length;
        int numVarCols = 3;

        db[offset] = 0x02;
        db[offset + 1] = 0x01;

        // Next TDEF page = 0 (single page, no chain).
        Wi32(db, offset + 4, 0);

        // Header fields
        db[offset + tdNumCols - 5] = 0x4E;
        Wu16(db, offset + tdNumCols - 4, numCols);
        Wu16(db, offset + tdNumCols - 2, numVarCols);
        Wu16(db, offset + tdNumCols, numCols);

        // Column descriptors start at tdBlockEnd (no real indexes).
        int colStart = offset + tdBlockEnd;
        int namePos = colStart + (numCols * colDescSz);

        for (int i = 0; i < numCols; i++)
        {
            var col = columns[i];
            int o = colStart + (i * colDescSz);

            db[o + colTypeOff] = col.Type;
            Wu16(db, o + colNumOff, col.ColNum);
            Wu16(db, o + colVarOff, col.VarIdx);
            db[o + colFlagsOff] = col.Flags;
            Wu16(db, o + colFixedOff, col.FixedOff);
            Wu16(db, o + colSzOff, col.Size);

            byte[] nameBytes = isJet3
                ? Encoding.ASCII.GetBytes(col.Name)
                : Encoding.Unicode.GetBytes(col.Name);

            if (isJet3)
            {
                db[namePos] = (byte)nameBytes.Length;
                namePos += 1;
            }
            else
            {
                Wu16(db, namePos, nameBytes.Length);
                namePos += 2;
            }

            Buffer.BlockCopy(nameBytes, 0, db, namePos, nameBytes.Length);
            namePos += nameBytes.Length;
        }

        Wi32(db, offset + 8, Math.Max(0, namePos - offset - 8));
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
            updated = Math.Clamp(current + delta, 0L, uint.MaxValue);
            Wi32(page, TDefRowCountOffset, unchecked((int)(uint)updated));
            await WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(page);
        }
    }

    private async ValueTask<PageInsertTarget> FindInsertTargetAsync(long tdefPage, int rowLength, CancellationToken cancellationToken)
    {
        if (TryGetCachedInsertPageNumber(tdefPage, out long cachedPageNumber))
        {
            byte[] cached = await ReadPageAsync(cachedPageNumber, cancellationToken).ConfigureAwait(false);
            if (cached[0] == 0x01 && Ri32(cached, _dpTDefOff) == tdefPage && CanInsertRow(cached, rowLength))
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

            if (Ri32(page, _dpTDefOff) != tdefPage)
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
        var varEntries = maxDefinedVarIdx >= 0 ? new byte[maxDefinedVarIdx + 1][] : [];
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
        int jumpSize = _format != DatabaseFormat.Jet3Mdb ? 0 : baseRowLength / 256;
        int rowLength = baseRowLength + jumpSize;
        int finalJump = _format != DatabaseFormat.Jet3Mdb ? 0 : rowLength / 256;
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
        var variableOffsets = varLen > 0 ? new int[varLen] : [];
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

        byte[] bytes = _format != DatabaseFormat.Jet3Mdb ? Encoding.Unicode.GetBytes(value) : _ansiEncoding.GetBytes(value);
        if (maxSize > 0 && bytes.Length > maxSize)
        {
            int allowed = _format != DatabaseFormat.Jet3Mdb ? maxSize & ~1 : maxSize;
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

        byte[] data = _format != DatabaseFormat.Jet3Mdb ? Encoding.Unicode.GetBytes(value) : _ansiEncoding.GetBytes(value);
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

        int numCols = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, rowStart) : page[rowStart];
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

        int varLen = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, rowStart + varLenPos) : page[rowStart + varLenPos];
        int jumpSize = _format != DatabaseFormat.Jet3Mdb ? 0 : rowSize / 256;
        int varTableStart = varLenPos - jumpSize - (varLen * _varEntrySz);
        int eodPos = varTableStart - _eodFldSz;
        if (eodPos < _numColsFldSz)
        {
            return string.Empty;
        }

        int eod = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, rowStart + eodPos) : page[rowStart + eodPos];
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

        int varOff = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, rowStart + entryPos) : page[rowStart + entryPos];
        int varEnd;
        if (column.VarIdx + 1 < varLen)
        {
            int nextEntry = varTableStart + ((varLen - 2 - column.VarIdx) * _varEntrySz);
            varEnd = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, rowStart + nextEntry) : page[rowStart + nextEntry];
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
                return _format != DatabaseFormat.Jet3Mdb ? DecodeJet4Text(page, rowStart + dataStart, dataLen) : _ansiEncoding.GetString(page, rowStart + dataStart, dataLen);
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

            if (Ri32(page, _dpTDefOff) != tdefPage)
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
