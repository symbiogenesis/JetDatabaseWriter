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

    /// <summary>
    /// Maximum recursion depth for cascade-delete / cascade-update chains (W10).
    /// Guards against pathological self-referential cycles. Real-world Access
    /// schemas almost never exceed depth 3.
    /// </summary>
    private const int CascadeMaxDepth = 64;

    private readonly SecureString? _password;
    private readonly bool _useLockFile;
    private readonly bool _respectExistingLockFile;
    private readonly ReaderWriterLockSlim _stateLock = new(LockRecursionPolicy.NoRecursion);

    // Agile re-encryption context. When non-null, the underlying _stream is an
    // in-memory MemoryStream containing the *decrypted* inner ACCDB; on
    // DisposeAsync the bytes are re-encrypted with Office Crypto API "Agile"
    // and written back to _outerEncryptedStream (which holds the original CFB).
    private readonly Stream? _outerEncryptedStream;
    private readonly bool _outerEncryptedLeaveOpen;
    private readonly bool _isAgileEncryptedRewrap;

    /// <summary>
    /// Per-table client-side constraint registry. Populated by <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, CancellationToken)"/>
    /// and the schema-evolution helpers. Keyed by table name (case-insensitive). The list is
    /// kept positionally aligned with the table's columns and is consulted at insert time to
    /// apply default values, auto-increment, required-field, and validation rule semantics.
    /// </summary>
    private readonly Dictionary<string, List<ColumnConstraint>> _constraints =
        new(StringComparer.OrdinalIgnoreCase);

    private List<CatalogEntry>? _catalogCache;
    private long _cachedInsertTDefPage = -1;
    private long _cachedInsertPageNumber = -1;

    private AccessWriter(
        string path,
        Stream stream,
        byte[] header,
        SecureString? password,
        bool useLockFile,
        bool respectExistingLockFile,
        Stream? outerEncryptedStream = null,
        bool outerEncryptedLeaveOpen = false,
        bool isAgileEncryptedRewrap = false)
        : base(stream, header, path)
    {
        _password = SecureStringUtilities.CopyAsReadOnly(password);
        _useLockFile = useLockFile && !string.IsNullOrEmpty(path);
        _respectExistingLockFile = respectExistingLockFile;
        _outerEncryptedStream = outerEncryptedStream;
        _outerEncryptedLeaveOpen = outerEncryptedLeaveOpen;
        _isAgileEncryptedRewrap = isAgileEncryptedRewrap;

        // Real CFB-wrapped encrypted ACCDBs (Agile / Office Crypto API) can
        // only be edited via the in-memory decrypt-then-rewrap path — caller
        // must supply the outer encrypted stream. The 'header' passed here is
        // expected to be the inner Jet header, so IsCompoundFileEncrypted
        // returns false on it. Synthetic legacy AES-128 CFB-wrapped .accdb
        // files (CFB magic at byte 0 but flat per-page AES-128-ECB beneath)
        // are written in place — the existing PrepareEncryptedPageForWrite
        // pipeline re-encrypts every page we flush.
        bool isLegacyAesCfb =
            EncryptionManager.IsCompoundFileEncrypted(header) && !isAgileEncryptedRewrap;

        // Populate page-encryption keys for in-place re-encryption of writes
        // (Jet3 XOR is already configured by AccessBase; this resolves the
        // Jet4 RC4 database key and / or the legacy AES-128 page key when
        // applicable).
        (_pageKeys.Rc4DbKey, _pageKeys.AesPageKey) =
            EncryptionManager.ResolveReaderPageKeys(header, _format, isLegacyAesCfb, password);

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

            // Office Crypto API ("Agile") encrypted .accdb files are real OLE
            // compound documents (CFB) wrapping an EncryptedPackage stream.
            // We can't edit them in place: writes are buffered into an
            // in-memory MemoryStream containing the *decrypted* inner ACCDB,
            // and the whole CFB is re-emitted on DisposeAsync.
            if (EncryptionManager.IsCompoundFileEncrypted(header))
            {
                _ = wrapped.Seek(0, SeekOrigin.Begin);
                byte[]? decryptedAgile = await EncryptionManager
                    .TryDecryptAgileCompoundFileAsync(wrapped, header, options.Password, cancellationToken)
                    .ConfigureAwait(false);

                if (decryptedAgile != null)
                {
                    var inner = new MemoryStream();
                    await inner.WriteAsync(decryptedAgile.AsMemory(), cancellationToken).ConfigureAwait(false);
                    inner.Position = 0;
                    byte[] innerHeader = await ReadHeaderAsync(inner, cancellationToken).ConfigureAwait(false);

                    return new AccessWriter(
                        path,
                        inner,
                        innerHeader,
                        options.Password,
                        options.UseLockFile,
                        options.RespectExistingLockFile,
                        outerEncryptedStream: wrapped,
                        outerEncryptedLeaveOpen: leaveOpen,
                        isAgileEncryptedRewrap: true);
                }

                // CFB magic but not a real Agile compound document: treat as
                // the synthetic legacy AES-128 layout (flat per-page AES-ECB
                // beneath a CFB-magic header byte). The constructor sets up
                // the page key and writes are re-encrypted on every flush.
                _ = wrapped.Seek(0, SeekOrigin.Begin);
            }

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

        byte[] dbBytes = BuildEmptyDatabase(format, options?.WriteFullCatalogSchema ?? true);

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

        byte[] dbBytes = BuildEmptyDatabase(format, options?.WriteFullCatalogSchema ?? true);
        await stream.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        stream.Position = 0;

        return await OpenAsync(stream, options, leaveOpen, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, CancellationToken cancellationToken = default)
        => CreateTableAsync(tableName, columns, indexes: Array.Empty<IndexDefinition>(), cancellationToken);

    /// <inheritdoc/>
    public async ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        Guard.NotNull(indexes, nameof(indexes));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required", nameof(columns));
        }

        // W8: pre-process column-level IsPrimaryKey shortcut. Synthesize one
        // composite PK IndexDefinition (named "PrimaryKey") from columns
        // marked IsPrimaryKey=true, in declaration order, and force those
        // columns to IsNullable=false on the emitted TDEF. Mixing the
        // shortcut with an explicit PK IndexDefinition is rejected.
        (columns, indexes) = ApplyPrimaryKeyShortcut(columns, indexes);

        if (indexes.Count > 0 && _format == DatabaseFormat.Jet3Mdb)
        {
            throw new NotSupportedException("Index emission is only supported for Jet4 (.mdb) and ACE (.accdb) databases.");
        }

        if (await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        TableDef tableDef = BuildTableDefinition(columns, _format);
        IReadOnlyList<ResolvedIndex> resolvedIndexes = ResolveIndexes(indexes, tableDef);
        (byte[] tdefPage, int[] firstDpOffsets) = BuildTDefPageWithIndexOffsets(tableDef, resolvedIndexes);
        long tdefPageNumber = await AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        // W3: emit one empty index leaf page per real index and patch its page
        // number into the corresponding `first_dp` field of the real-idx physical
        // descriptor. The leaf is empty because CreateTableAsync starts with no
        // rows; subsequent inserts/updates/deletes do NOT maintain the leaf
        // (W5 territory), so the index goes stale until Microsoft Access rebuilds
        // it on the next Compact & Repair pass. See
        // docs/design/index-and-relationship-format-notes.md §7 W2/W3.
        if (resolvedIndexes.Count > 0)
        {
            for (int i = 0; i < resolvedIndexes.Count; i++)
            {
                byte[] leafPage = IndexLeafPageBuilder.BuildJet4LeafPage(
                    _pgSz,
                    tdefPageNumber,
                    Array.Empty<IndexLeafPageBuilder.LeafEntry>());
                long leafPageNumber = await AppendPageAsync(leafPage, cancellationToken).ConfigureAwait(false);
                Wi32(tdefPage, firstDpOffsets[i], checked((int)leafPageNumber));
            }

            // Re-flush the TDEF with the patched first_dp values.
            await WritePageAsync(tdefPageNumber, tdefPage, cancellationToken).ConfigureAwait(false);
        }

        byte[]? lvProp = JetExpressionConverter.BuildLvPropBlob(columns, _format);
        await InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, cancellationToken).ConfigureAwait(false);
        RegisterConstraints(tableName, columns);
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

        UnregisterConstraints(tableName);
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
                next[idx] = new ColumnDefinition(newColumnName, src.ClrType, src.MaxLength)
                {
                    IsNullable = src.IsNullable,
                    DefaultValue = src.DefaultValue,
                    IsAutoIncrement = src.IsAutoIncrement,
                    ValidationRule = src.ValidationRule,
                    DefaultValueExpression = src.DefaultValueExpression,
                    ValidationRuleExpression = src.ValidationRuleExpression,
                    ValidationText = src.ValidationText,
                    Description = src.Description,
                };
                return next;
            },
            (oldRow, _) => oldRow,
            cancellationToken,
            projectIndexes: (existingIndexes, newDefs) =>
            {
                var newColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (ColumnDefinition c in newDefs)
                {
                    newColumnNames.Add(c.Name);
                }

                var result = new List<IndexDefinition>(existingIndexes.Count);
                foreach (IndexMetadata idx in existingIndexes)
                {
                    // Forward Normal (single-column) and PrimaryKey (1..N column)
                    // indexes; FK forwarding is W9 territory.
                    if (idx.Kind != IndexKind.Normal && idx.Kind != IndexKind.PrimaryKey)
                    {
                        continue;
                    }

                    if (idx.Kind == IndexKind.Normal && idx.Columns.Count != 1)
                    {
                        continue;
                    }

                    var remappedCols = new List<string>(idx.Columns.Count);
                    bool allSurvive = true;
                    foreach (IndexColumnReference ic in idx.Columns)
                    {
                        string keyColumn = ic.Name;
                        string remapped = string.Equals(keyColumn, oldColumnName, StringComparison.OrdinalIgnoreCase)
                            ? newColumnName
                            : keyColumn;

                        if (string.IsNullOrEmpty(remapped) || !newColumnNames.Contains(remapped))
                        {
                            allSurvive = false;
                            break;
                        }

                        remappedCols.Add(remapped);
                    }

                    if (!allSurvive)
                    {
                        continue;
                    }

                    if (idx.Kind == IndexKind.PrimaryKey)
                    {
                        result.Add(new IndexDefinition(idx.Name, remappedCols) { IsPrimaryKey = true });
                    }
                    else
                    {
                        result.Add(new IndexDefinition(idx.Name, remappedCols[0]));
                    }
                }

                return result;
            });
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
        await ApplyConstraintsAsync(tableName, tableDef, values, cancellationToken).ConfigureAwait(false);

        IReadOnlyList<FkRelationship> rels = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtx = rels.Count > 0 ? new FkContext(rels) : null;
        if (fkCtx != null)
        {
            await EnforceFkOnInsertAsync(tableName, tableDef, values, fkCtx, cancellationToken).ConfigureAwait(false);
        }

        await InsertRowDataAsync(entry.TDefPage, tableDef, values, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (fkCtx != null)
        {
            AugmentParentSetsAfterInsert(tableName, tableDef, values, fkCtx);
        }

        await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
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
        IReadOnlyList<FkRelationship> rels = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtx = rels.Count > 0 ? new FkContext(rels) : null;
        int inserted = 0;

        foreach (object[] row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guard.NotNull(row, nameof(rows));
            await ApplyConstraintsAsync(tableName, tableDef, row, cancellationToken).ConfigureAwait(false);
            if (fkCtx != null)
            {
                await EnforceFkOnInsertAsync(tableName, tableDef, row, fkCtx, cancellationToken).ConfigureAwait(false);
            }

            await InsertRowDataAsync(entry.TDefPage, tableDef, row, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (fkCtx != null)
            {
                AugmentParentSetsAfterInsert(tableName, tableDef, row, fkCtx);
            }

            inserted++;
        }

        if (inserted > 0)
        {
            await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
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
        object[] mappedRow = RowMapper<T>.ToRow(item, index);
        await ApplyConstraintsAsync(tableName, tableDef, mappedRow, cancellationToken).ConfigureAwait(false);

        IReadOnlyList<FkRelationship> relsT = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtxT = relsT.Count > 0 ? new FkContext(relsT) : null;
        if (fkCtxT != null)
        {
            await EnforceFkOnInsertAsync(tableName, tableDef, mappedRow, fkCtxT, cancellationToken).ConfigureAwait(false);
        }

        await InsertRowDataAsync(entry.TDefPage, tableDef, mappedRow, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (fkCtxT != null)
        {
            AugmentParentSetsAfterInsert(tableName, tableDef, mappedRow, fkCtxT);
        }

        await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
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
        IReadOnlyList<FkRelationship> rels = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtx = rels.Count > 0 ? new FkContext(rels) : null;
        int inserted = 0;

        foreach (T item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guard.NotNull(item, nameof(items));
            object[] mappedRow = RowMapper<T>.ToRow(item, index);
            await ApplyConstraintsAsync(tableName, tableDef, mappedRow, cancellationToken).ConfigureAwait(false);
            if (fkCtx != null)
            {
                await EnforceFkOnInsertAsync(tableName, tableDef, mappedRow, fkCtx, cancellationToken).ConfigureAwait(false);
            }

            await InsertRowDataAsync(entry.TDefPage, tableDef, mappedRow, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (fkCtx != null)
            {
                AugmentParentSetsAfterInsert(tableName, tableDef, mappedRow, fkCtx);
            }

            inserted++;
        }

        if (inserted > 0)
        {
            await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
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

        // ── W10 — FK enforcement ────────────────────────────────────────
        // Build the list of new-row payloads up front so we can validate FK
        // constraints (FK-side parent presence, PK-side cascade-or-reject)
        // before mutating any disk page.
        IReadOnlyList<FkRelationship> rels = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtx = rels.Count > 0 ? new FkContext(rels) : null;

        var pendingNewRows = new List<(int Index, object[] NewRow)>();
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

            pendingNewRows.Add((i, rowValues));
        }

        if (fkCtx != null && pendingNewRows.Count > 0)
        {
            // FK-side: every updated row must (still) satisfy any FK constraint
            // whose foreign side is THIS table.
            foreach ((_, object[] newRow) in pendingNewRows)
            {
                await EnforceFkOnInsertAsync(tableName, tableDef, newRow, fkCtx, cancellationToken).ConfigureAwait(false);
            }

            // PK-side: if any of the updated columns belongs to a PK referenced
            // by a child table, gather (oldKey, newPkValues) pairs per affected
            // row and let EnforceFkOnPrimaryUpdateAsync cascade or reject.
            var changes = new List<(string? OldKey, object[] NewPkValues)>(pendingNewRows.Count);
            foreach (FkRelationship rel in rels)
            {
                if (!string.Equals(rel.PrimaryTable, tableName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var pkIdx = new int[rel.PrimaryColumns.Count];
                bool ok = true;
                bool anyPkUpdated = false;
                for (int i = 0; i < rel.PrimaryColumns.Count; i++)
                {
                    pkIdx[i] = FindColumnIndex(tableDef, rel.PrimaryColumns[i]);
                    if (pkIdx[i] < 0)
                    {
                        ok = false;
                        break;
                    }

                    if (updateIndexes.ContainsKey(pkIdx[i]))
                    {
                        anyPkUpdated = true;
                    }
                }

                if (!ok || !anyPkUpdated)
                {
                    continue;
                }

                changes.Clear();
                foreach ((int rowIdx, object[] newRow) in pendingNewRows)
                {
                    string? oldKey = BuildCompositeKey(snapshot.Rows[rowIdx].ItemArray, pkIdx);
                    changes.Add((oldKey, newRow));
                }

                await EnforceFkOnPrimaryUpdateAsync(tableName, changes, fkCtx, depth: 0, cancellationToken).ConfigureAwait(false);
            }
        }

        int updated = 0;
        foreach ((int i, object[] rowValues) in pendingNewRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, cancellationToken).ConfigureAwait(false);
            await InsertRowDataAsync(entry.TDefPage, tableDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            updated++;
        }

        if (updated > 0)
        {
            await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
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

        // ── W10 — FK enforcement ────────────────────────────────────────
        // Identify the rows we are about to delete; if any FK relationship
        // names this table as the primary side, capture the deleted PK
        // tuples and let EnforceFkOnPrimaryDeleteAsync cascade-delete
        // dependent child rows (or throw when cascade is disabled).
        var matchingIndices = new List<int>();
        for (int i = 0; i < total; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object currentValue = snapshot.Rows[i][predicateIndex];
            if (ValuesEqual(currentValue, predicateValue))
            {
                matchingIndices.Add(i);
            }
        }

        IReadOnlyList<FkRelationship> rels = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        if (rels.Count > 0 && matchingIndices.Count > 0)
        {
            var fkCtx = new FkContext(rels);
            foreach (FkRelationship rel in rels)
            {
                if (!string.Equals(rel.PrimaryTable, tableName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var pkIdx = new int[rel.PrimaryColumns.Count];
                bool ok = true;
                for (int i = 0; i < rel.PrimaryColumns.Count; i++)
                {
                    pkIdx[i] = FindColumnIndex(tableDef, rel.PrimaryColumns[i]);
                    if (pkIdx[i] < 0)
                    {
                        ok = false;
                        break;
                    }
                }

                if (!ok)
                {
                    continue;
                }

                var deletedKeys = new List<string?>(matchingIndices.Count);
                foreach (int rowIdx in matchingIndices)
                {
                    deletedKeys.Add(BuildCompositeKey(snapshot.Rows[rowIdx].ItemArray, pkIdx));
                }

                await EnforceFkOnPrimaryDeleteAsync(tableName, deletedKeys, fkCtx, depth: 0, cancellationToken).ConfigureAwait(false);
            }
        }

        int deleted = 0;
        foreach (int i in matchingIndices)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted > 0)
        {
            await AdjustTDefRowCountAsync(entry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
            await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        }

        return deleted;
    }

    /// <summary>
    /// Asynchronously creates a linked-table entry (MSysObjects type 4) that references
    /// a table in another Access database. No row data is stored locally; readers follow
    /// the entry to <paramref name="sourceDatabasePath"/> on demand.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDatabasePath">Path to the source Access database file (.mdb / .accdb).</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async ValueTask CreateLinkedTableAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(linkedTableName, nameof(linkedTableName));
        Guard.NotNullOrEmpty(sourceDatabasePath, nameof(sourceDatabasePath));
        Guard.NotNullOrEmpty(foreignTableName, nameof(foreignTableName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (await GetCatalogEntryAsync(linkedTableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"An object named '{linkedTableName}' already exists.");
        }

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

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 6) that references
    /// a table accessible via an ODBC connection. No row data is stored locally; readers
    /// follow the entry to <paramref name="connectionString"/> /
    /// <paramref name="foreignTableName"/> on demand.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async ValueTask CreateLinkedOdbcTableAsync(string linkedTableName, string connectionString, string foreignTableName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(linkedTableName, nameof(linkedTableName));
        Guard.NotNullOrEmpty(connectionString, nameof(connectionString));
        Guard.NotNullOrEmpty(foreignTableName, nameof(foreignTableName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (await GetCatalogEntryAsync(linkedTableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"An object named '{linkedTableName}' already exists.");
        }

        string normalizedConnect = connectionString.StartsWith("ODBC;", StringComparison.OrdinalIgnoreCase)
            ? connectionString
            : "ODBC;" + connectionString;

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
        SetValue(msys, values, "Type", (short)OBJ_LINKED_ODBC);
        SetValue(msys, values, "DateCreate", now);
        SetValue(msys, values, "DateUpdate", now);
        SetValue(msys, values, "Flags", 0);
        SetValue(msys, values, "ForeignName", foreignTableName);
        SetValue(msys, values, "Connect", normalizedConnect);

        await InsertRowDataAsync(2, msys, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        InvalidateCatalogCache();
    }

    // ════════════════════════════════════════════════════════════════
    // Foreign-key relationships (W9a — MSysRelationships row emission)
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Asynchronously creates a foreign-key relationship between two existing user
    /// tables by appending one row per FK column to the <c>MSysRelationships</c>
    /// system table. See
    /// <see cref="IAccessWriter.CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>
    /// for the full contract.
    /// </summary>
    /// <param name="relationship">The relationship to create.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// W9a (per <c>docs/design/index-and-relationship-format-notes.md</c> §7) emits
    /// only the catalog rows that the Microsoft Access Relationships designer reads.
    /// The per-TDEF FK logical-index entries (<c>index_type = 0x02</c>,
    /// <c>rel_idx_num</c>, <c>rel_tbl_page</c>) that drive runtime referential-
    /// integrity enforcement by the JET engine are not written; Microsoft Access
    /// regenerates them on the next Compact &amp; Repair pass.
    /// </remarks>
    public async ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
    {
        Guard.NotNull(relationship, nameof(relationship));
        Guard.NotNullOrEmpty(relationship.Name, "relationship.Name");
        Guard.NotNullOrEmpty(relationship.PrimaryTable, "relationship.PrimaryTable");
        Guard.NotNullOrEmpty(relationship.ForeignTable, "relationship.ForeignTable");
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // Validate referenced user tables exist.
        CatalogEntry primaryEntry = await GetRequiredCatalogEntryAsync(relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        CatalogEntry foreignEntry = await GetRequiredCatalogEntryAsync(relationship.ForeignTable, cancellationToken).ConfigureAwait(false);

        // Validate referenced columns exist on each table.
        TableDef primaryDef = await ReadRequiredTableDefAsync(primaryEntry.TDefPage, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        TableDef foreignDef = await ReadRequiredTableDefAsync(foreignEntry.TDefPage, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < relationship.PrimaryColumns.Count; i++)
        {
            if (FindColumnIndex(primaryDef, relationship.PrimaryColumns[i]) < 0)
            {
                throw new ArgumentException(
                    $"Column '{relationship.PrimaryColumns[i]}' was not found on table '{relationship.PrimaryTable}'.",
                    nameof(relationship));
            }

            if (FindColumnIndex(foreignDef, relationship.ForeignColumns[i]) < 0)
            {
                throw new ArgumentException(
                    $"Column '{relationship.ForeignColumns[i]}' was not found on table '{relationship.ForeignTable}'.",
                    nameof(relationship));
            }
        }

        // Locate MSysRelationships (system table — not in the user-table cache).
        long msysRelTdefPage = await FindSystemTableTdefPageAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table. Databases freshly created by " +
                "AccessWriter.CreateDatabaseAsync do not include this catalog table; open or copy an " +
                "Access-authored database before calling CreateRelationshipAsync.");
        }

        TableDef msysRelDef = await ReadRequiredTableDefAsync(msysRelTdefPage, "MSysRelationships", cancellationToken).ConfigureAwait(false);

        // Reject duplicate relationship names (case-insensitive).
        HashSet<string> existingNames = await ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existingNames.Contains(relationship.Name))
        {
            throw new InvalidOperationException($"A relationship named '{relationship.Name}' already exists.");
        }

        // grbit flag bits — values per Jackcess com.healthmarketscience.jackcess.impl.RelationshipImpl.
        // (These are not yet documented in the format-probe appendix; W9b empirical verification pending.)
        const uint NoRefIntegrityFlag = 0x00000002;
        const uint CascadeUpdatesFlag = 0x00000100;
        const uint CascadeDeletesFlag = 0x00001000;

        uint grbit = 0;
        if (!relationship.EnforceReferentialIntegrity)
        {
            grbit |= NoRefIntegrityFlag;
        }

        if (relationship.CascadeUpdates)
        {
            grbit |= CascadeUpdatesFlag;
        }

        if (relationship.CascadeDeletes)
        {
            grbit |= CascadeDeletesFlag;
        }

        int ccolumn = relationship.PrimaryColumns.Count;
        int grbitInt = unchecked((int)grbit);

        // One row per FK column pair (per appendix §"MSysRelationships — TDEF page 5").
        for (int i = 0; i < ccolumn; i++)
        {
            var values = new object[msysRelDef.Columns.Count];
            for (int c = 0; c < values.Length; c++)
            {
                values[c] = DBNull.Value;
            }

            SetValue(msysRelDef, values, "ccolumn", ccolumn);
            SetValue(msysRelDef, values, "grbit", grbitInt);
            SetValue(msysRelDef, values, "icolumn", i);
            SetValue(msysRelDef, values, "szColumn", relationship.ForeignColumns[i]);
            SetValue(msysRelDef, values, "szObject", relationship.ForeignTable);
            SetValue(msysRelDef, values, "szReferencedColumn", relationship.PrimaryColumns[i]);
            SetValue(msysRelDef, values, "szReferencedObject", relationship.PrimaryTable);
            SetValue(msysRelDef, values, "szRelationship", relationship.Name);

            await InsertRowDataAsync(msysRelTdefPage, msysRelDef, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        // ── W9b: per-TDEF FK logical-idx entries ────────────────────────────
        // Adds index_type=0x02 logical-idx entries on both PK-side and FK-side
        // TDEFs with cross-referenced rel_idx_num / rel_tbl_page so the JET
        // engine can locate the partner table without waiting for Microsoft
        // Access Compact & Repair to regenerate them from the MSysRelationships
        // rows above. See docs/design/index-and-relationship-format-notes.md
        // §7 W9b. Jet3 uses a different (20-byte) logical-idx layout that this
        // library does not yet exercise — skip silently to keep the W9a row
        // emission working on .mdb (Access 97) databases.
        if (_format != DatabaseFormat.Jet3Mdb)
        {
            await EmitFkPerTdefEntriesAsync(
                relationship,
                primaryEntry.TDefPage,
                primaryDef,
                foreignEntry.TDefPage,
                foreignDef,
                cancellationToken).ConfigureAwait(false);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // W9b — per-TDEF FK logical-idx entries (Jet4 / ACE only)
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Pre-computed real-idx slot information for one side of a relationship.
    /// </summary>
    private readonly record struct FkSidePlan(int RealIdxNum, bool AllocatesNewRealIdx, long NewLeafPageNumber)
    {
        // RealIdxNum:           real-idx slot index used for index_num2 on this side.
        // AllocatesNewRealIdx:  true when a new real-idx slot must be appended.
        // NewLeafPageNumber:    pre-allocated empty leaf page (set when AllocatesNewRealIdx).
        public FkSidePlan WithLeafPage(long page) => this with { NewLeafPageNumber = page };
    }

    /// <summary>
    /// Orchestrates the two-side W9b emission: pre-computes both sides' target
    /// real-idx slots (sharing where possible), allocates empty leaf pages for
    /// any newly-allocated real-idx slots, then mutates each TDEF in place to
    /// append its FK logical-idx entry. Operates on single-page TDEFs only;
    /// throws <see cref="NotSupportedException"/> if either TDEF is multi-page
    /// or would overflow a single page after growth.
    /// </summary>
    private async ValueTask EmitFkPerTdefEntriesAsync(
        RelationshipDefinition relationship,
        long primaryTdefPage,
        TableDef primaryDef,
        long foreignTdefPage,
        TableDef foreignDef,
        CancellationToken cancellationToken)
    {
        // Resolve column numbers (deleted-column gaps mean ColNum != ordinal).
        var pkColNums = new int[relationship.PrimaryColumns.Count];
        var fkColNums = new int[relationship.ForeignColumns.Count];
        for (int i = 0; i < relationship.PrimaryColumns.Count; i++)
        {
            int pkIdx = FindColumnIndex(primaryDef, relationship.PrimaryColumns[i]);
            int fkIdx = FindColumnIndex(foreignDef, relationship.ForeignColumns[i]);
            pkColNums[i] = primaryDef.Columns[pkIdx].ColNum;
            fkColNums[i] = foreignDef.Columns[fkIdx].ColNum;
        }

        // Read both TDEF pages and decide each side's real-idx slot.
        (FkSidePlan pkPlan, List<string> pkExistingNames) = await PrepareFkSideAsync(primaryTdefPage, pkColNums, cancellationToken).ConfigureAwait(false);
        (FkSidePlan fkPlan, List<string> fkExistingNames) = await PrepareFkSideAsync(foreignTdefPage, fkColNums, cancellationToken).ConfigureAwait(false);

        // Allocate empty leaf pages for any newly-allocated real-idx slots.
        // Both leaf pages are appended before any TDEF mutation so the page
        // numbers are stable for the cross-referenced first_dp values.
        if (pkPlan.AllocatesNewRealIdx)
        {
            byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(_pgSz, primaryTdefPage, Array.Empty<IndexLeafPageBuilder.LeafEntry>());
            long lp = await AppendPageAsync(leaf, cancellationToken).ConfigureAwait(false);
            pkPlan = pkPlan.WithLeafPage(lp);
        }

        if (fkPlan.AllocatesNewRealIdx)
        {
            byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(_pgSz, foreignTdefPage, Array.Empty<IndexLeafPageBuilder.LeafEntry>());
            long lp = await AppendPageAsync(leaf, cancellationToken).ConfigureAwait(false);
            fkPlan = fkPlan.WithLeafPage(lp);
        }

        byte cascadeUpsByte = (byte)(relationship.CascadeUpdates ? 1 : 0);
        byte cascadeDelsByte = (byte)(relationship.CascadeDeletes ? 1 : 0);

        // Choose unique-within-tdef logical-idx names. The PK side uses the
        // relationship name; the FK side appends "_FK" to disambiguate when
        // both endpoints land on the same table (self-referential).
        string pkName = MakeUniqueLogicalIdxName(relationship.Name, pkExistingNames);
        string fkName = MakeUniqueLogicalIdxName(
            primaryTdefPage == foreignTdefPage ? relationship.Name + "_FK" : relationship.Name,
            fkExistingNames);

        // Emit both sides. PK side carries no cascade flags (cascade is an
        // FK-side property — Access only checks them when modifying the parent
        // and looking up children).
        await EmitFkLogicalIdxAsync(
            primaryTdefPage,
            pkColNums,
            pkName,
            realIdxNumThisSide: pkPlan.RealIdxNum,
            allocateNewRealIdx: pkPlan.AllocatesNewRealIdx,
            preAllocatedLeafPage: pkPlan.NewLeafPageNumber,
            relIdxNumOtherSide: fkPlan.RealIdxNum,
            relTblPageOther: foreignTdefPage,
            cascadeUps: 0,
            cascadeDels: 0,
            cancellationToken).ConfigureAwait(false);

        await EmitFkLogicalIdxAsync(
            foreignTdefPage,
            fkColNums,
            fkName,
            realIdxNumThisSide: fkPlan.RealIdxNum,
            allocateNewRealIdx: fkPlan.AllocatesNewRealIdx,
            preAllocatedLeafPage: fkPlan.NewLeafPageNumber,
            relIdxNumOtherSide: pkPlan.RealIdxNum,
            relTblPageOther: primaryTdefPage,
            cascadeUps: cascadeUpsByte,
            cascadeDels: cascadeDelsByte,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads one side's TDEF page, walks the col-name and idx-name sections,
    /// detects any existing real-idx that already covers <paramref name="columnNumbers"/>
    /// (sharing per §3.3), and returns the resulting plan plus the existing
    /// logical-idx-name list (used to avoid name collisions on the new entry).
    /// </summary>
    private async ValueTask<(FkSidePlan Plan, List<string> ExistingNames)> PrepareFkSideAsync(
        long tdefPage,
        int[] columnNumbers,
        CancellationToken cancellationToken)
    {
        byte[] page = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != 0x02)
            {
                throw new InvalidOperationException($"W9b: page {tdefPage} is not a TDEF page.");
            }

            if (Ru32(page, 4) != 0)
            {
                throw new NotSupportedException(
                    $"W9b: TDEF at page {tdefPage} spans multiple pages; in-place FK index emission is not supported on multi-page TDEFs.");
            }

            int numCols = Ru16(page, _tdNumCols);
            int numIdx = Ri32(page, _tdNumCols + 2);
            int numRealIdx = Ri32(page, _tdNumRealIdx);
            if (numCols < 0 || numCols > 4096)
            {
                numCols = 0;
            }

            if (numIdx < 0 || numIdx > 1000)
            {
                numIdx = 0;
            }

            if (numRealIdx < 0 || numRealIdx > 1000)
            {
                numRealIdx = 0;
            }

            int realIdxDescStart = LocateRealIdxDescStart(page, numCols, numRealIdx);
            if (realIdxDescStart < 0)
            {
                throw new InvalidOperationException("W9b: unable to walk TDEF column-name section.");
            }

            int sharedSlot = FindCoveringRealIdx(page, columnNumbers, realIdxDescStart, numRealIdx);

            int logIdxStart = realIdxDescStart + (numRealIdx * 52);
            int logIdxNamesStart = logIdxStart + (numIdx * 28);
            List<string> existingNames = ReadLogicalIdxNames(page, logIdxNamesStart, numIdx);

            FkSidePlan plan = sharedSlot >= 0
                ? new FkSidePlan(sharedSlot, false, 0)
                : new FkSidePlan(numRealIdx, true, 0);

            return (plan, existingNames);
        }
        finally
        {
            ReturnPage(page);
        }
    }

    /// <summary>
    /// W9b: appends one FK logical-idx entry (and optionally a new real-idx
    /// physical descriptor) to the TDEF at <paramref name="tdefPage"/>. The
    /// TDEF must fit on a single page after the addition.
    /// </summary>
    private async ValueTask EmitFkLogicalIdxAsync(
        long tdefPage,
        int[] columnNumbers,
        string indexName,
        int realIdxNumThisSide,
        bool allocateNewRealIdx,
        long preAllocatedLeafPage,
        int relIdxNumOtherSide,
        long relTblPageOther,
        byte cascadeUps,
        byte cascadeDels,
        CancellationToken cancellationToken)
    {
        byte[] pageBytes = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] td;
        try
        {
            td = (byte[])pageBytes.Clone();
        }
        finally
        {
            ReturnPage(pageBytes);
        }

        if (td[0] != 0x02 || Ru32(td, 4) != 0)
        {
            throw new NotSupportedException(
                $"W9b: cannot mutate the TDEF at page {tdefPage} (multi-page chain or not a TDEF).");
        }

        int numCols = Ru16(td, _tdNumCols);
        int numIdx = Ri32(td, _tdNumCols + 2);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numCols < 0 || numCols > 4096
            || numIdx < 0 || numIdx > 1000
            || numRealIdx < 0 || numRealIdx > 1000)
        {
            throw new InvalidOperationException("W9b: TDEF reports out-of-range section counts.");
        }

        const int RealIdxPhysSz = 52;
        const int LogIdxEntrySz = 28;

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            throw new InvalidOperationException("W9b: failed to walk TDEF column-name section.");
        }

        int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);
        int logIdxNamesStart = logIdxStart + (numIdx * LogIdxEntrySz);
        int logIdxNamesLen = MeasureLogicalIdxNamesLength(td, logIdxNamesStart, numIdx);
        if (logIdxNamesLen < 0)
        {
            throw new InvalidOperationException("W9b: failed to walk TDEF logical-idx-name section.");
        }

        int trailingStart = logIdxNamesStart + logIdxNamesLen;

        // tdef_len at offset 8 stores (last_used_byte - 8). For Access-emitted
        // TDEFs this includes the variable-length-column trailing block; for
        // writer-emitted TDEFs there is no trailing block (trailingLen == 0).
        int storedTdefLen = Ri32(td, 8);
        int currentEnd = storedTdefLen + 8;
        if (currentEnd < trailingStart)
        {
            currentEnd = trailingStart;
        }

        int trailingLen = currentEnd - trailingStart;
        if (trailingLen < 0 || trailingStart + trailingLen > td.Length)
        {
            throw new InvalidOperationException("W9b: TDEF trailing block overruns page.");
        }

        byte[] nameBytes = Encoding.Unicode.GetBytes(indexName);
        int nameRecordSize = 2 + nameBytes.Length;

        int deltaRealIdxSkip = allocateNewRealIdx ? _realIdxEntrySz : 0;
        int deltaRealIdxPhys = allocateNewRealIdx ? RealIdxPhysSz : 0;
        int totalGrowth = deltaRealIdxSkip + deltaRealIdxPhys + LogIdxEntrySz + nameRecordSize;

        if (currentEnd + totalGrowth > _pgSz)
        {
            throw new NotSupportedException(
                "W9b: TDEF would exceed a single page after adding a foreign-key index entry. " +
                "Multi-page TDEF growth is not supported.");
        }

        // Build the rewritten page.
        var newTd = new byte[_pgSz];
        Buffer.BlockCopy(td, 0, newTd, 0, _tdBlockEnd);

        // Real-idx skip block (existing slots, unchanged content).
        int oldRealIdxSkipLen = numRealIdx * _realIdxEntrySz;
        Buffer.BlockCopy(td, _tdBlockEnd, newTd, _tdBlockEnd, oldRealIdxSkipLen);
        int newRealIdxSkipEnd = _tdBlockEnd + oldRealIdxSkipLen + deltaRealIdxSkip;

        // Column descriptors.
        int oldColStart = _tdBlockEnd + oldRealIdxSkipLen;
        int colDescBlockLen = numCols * _colDescSz;
        Buffer.BlockCopy(td, oldColStart, newTd, newRealIdxSkipEnd, colDescBlockLen);

        // Column names (variable length).
        int oldColNamesStart = oldColStart + colDescBlockLen;
        int colNamesLen = realIdxDescStart - oldColNamesStart;
        int newColNamesStart = newRealIdxSkipEnd + colDescBlockLen;
        Buffer.BlockCopy(td, oldColNamesStart, newTd, newColNamesStart, colNamesLen);

        // Real-idx physical descriptors (existing slots).
        int newRealIdxDescStart = newColNamesStart + colNamesLen;
        int oldRealIdxPhysLen = numRealIdx * RealIdxPhysSz;
        Buffer.BlockCopy(td, realIdxDescStart, newTd, newRealIdxDescStart, oldRealIdxPhysLen);

        // Append a new real-idx physical descriptor when allocating a new slot.
        if (allocateNewRealIdx)
        {
            int phys = newRealIdxDescStart + oldRealIdxPhysLen;

            // bytes 0..3   unknown(4) — Jackcess emits a per-tdef cookie; zero
            //              also round-trips through this library's reader and
            //              through Microsoft Access (probe shows the cookie is
            //              not interpreted by either parser).
            // bytes 4..33  col_map: 10 × {col_num(2), col_order(1)}
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                if (slot < columnNumbers.Length)
                {
                    Wu16(newTd, so, columnNumbers[slot]);
                    newTd[so + 2] = 0x01; // ascending
                }
                else
                {
                    Wu16(newTd, so, 0xFFFF);
                    newTd[so + 2] = 0x00;
                }
            }

            // bytes 34..37 used_pages = 0 (no usage bitmap emitted)
            // bytes 38..41 first_dp = preAllocatedLeafPage
            Wi32(newTd, phys + 38, checked((int)preAllocatedLeafPage));

            // bytes 42 flags = 0; bytes 43..51 unknown = 0
        }

        // Logical-idx entries (existing).
        int newLogIdxStart = newRealIdxDescStart + oldRealIdxPhysLen + deltaRealIdxPhys;
        int oldLogIdxLen = numIdx * LogIdxEntrySz;
        Buffer.BlockCopy(td, logIdxStart, newTd, newLogIdxStart, oldLogIdxLen);

        // Append the new FK logical-idx entry.
        // bytes 0..3   unknown(4) — Jackcess emits a per-tdef cookie; zero is
        //              consistent with this library's existing W1/W3/W8 emit.
        // bytes 24..27 trailing(4) = 0
        int newLogEntry = newLogIdxStart + oldLogIdxLen;
        Wi32(newTd, newLogEntry + 4, numIdx);                  // index_num (next sequential)
        Wi32(newTd, newLogEntry + 8, realIdxNumThisSide);      // index_num2
        newTd[newLogEntry + 12] = 0x01;                        // rel_tbl_type — empirical: 0x01 on FK entries (appendix §"Companies")
        Wi32(newTd, newLogEntry + 13, relIdxNumOtherSide);     // rel_idx_num — slot on the OTHER table
        Wi32(newTd, newLogEntry + 17, checked((int)relTblPageOther)); // rel_tbl_page
        newTd[newLogEntry + 21] = cascadeUps;
        newTd[newLogEntry + 22] = cascadeDels;
        newTd[newLogEntry + 23] = 0x02;                        // index_type = FK

        // Logical-idx names (existing).
        int newLogIdxNamesStart = newLogEntry + LogIdxEntrySz;
        Buffer.BlockCopy(td, logIdxNamesStart, newTd, newLogIdxNamesStart, logIdxNamesLen);

        // Append the new logical-idx name (UTF-16 length-prefixed).
        int newNameOffset = newLogIdxNamesStart + logIdxNamesLen;
        Wu16(newTd, newNameOffset, nameBytes.Length);
        Buffer.BlockCopy(nameBytes, 0, newTd, newNameOffset + 2, nameBytes.Length);

        // Trailing variable-length-column block (Access-emitted TDEFs only).
        int newTrailingStart = newNameOffset + nameRecordSize;
        if (trailingLen > 0)
        {
            Buffer.BlockCopy(td, trailingStart, newTd, newTrailingStart, trailingLen);
        }

        // Update header counts.
        Wi32(newTd, _tdNumCols + 2, numIdx + 1);
        if (allocateNewRealIdx)
        {
            Wi32(newTd, _tdNumRealIdx, numRealIdx + 1);
        }

        // tdef_len at offset 8 = (newEnd - 8). The page header (8 bytes) is
        // not counted in tdef_len, matching BuildTDefPageWithIndexOffsets.
        Wi32(newTd, 8, newTrailingStart + trailingLen - 8);

        await WritePageAsync(tdefPage, newTd, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Walks past column descriptors and column names to return the byte
    /// offset of the real-index physical-descriptor section start, or -1
    /// when the column-name walk fails.
    /// </summary>
    private int LocateRealIdxDescStart(byte[] td, int numCols, int numRealIdx)
    {
        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int pos = colStart + (numCols * _colDescSz);
        for (int i = 0; i < numCols; i++)
        {
            if (ReadColumnName(td, ref pos, out _) < 0)
            {
                return -1;
            }
        }

        return pos;
    }

    /// <summary>
    /// Returns the byte length of the existing logical-idx-name section, or
    /// -1 if the walk fails.
    /// </summary>
    private int MeasureLogicalIdxNamesLength(byte[] td, int logIdxNamesStart, int numIdx)
    {
        int pos = logIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (ReadColumnName(td, ref pos, out _) < 0)
            {
                return -1;
            }
        }

        return pos - logIdxNamesStart;
    }

    /// <summary>
    /// Materializes the existing logical-idx-name list (used to avoid name
    /// collisions when appending a new W9b entry).
    /// </summary>
    private List<string> ReadLogicalIdxNames(byte[] td, int logIdxNamesStart, int numIdx)
    {
        var list = new List<string>(numIdx);
        int pos = logIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (ReadColumnName(td, ref pos, out string n) < 0)
            {
                break;
            }

            list.Add(n);
        }

        return list;
    }

    /// <summary>
    /// W9b §3.3 sharing: returns the existing real-idx slot whose col_map
    /// matches <paramref name="columnNumbers"/> exactly (in declaration
    /// order); -1 when no covering real-idx exists. Jet4 col_map is fixed at
    /// 10 slots × {col_num(2), col_order(1)}.
    /// </summary>
    private int FindCoveringRealIdx(byte[] td, int[] columnNumbers, int realIdxDescStart, int numRealIdx)
    {
        const int RealIdxPhysSz = 52;
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * RealIdxPhysSz);
            if (phys + RealIdxPhysSz > td.Length)
            {
                break;
            }

            bool match = true;
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(td, so);
                if (slot < columnNumbers.Length)
                {
                    if (cn != columnNumbers[slot])
                    {
                        match = false;
                        break;
                    }
                }
                else
                {
                    if (cn != 0xFFFF)
                    {
                        match = false;
                        break;
                    }
                }
            }

            if (match)
            {
                return ri;
            }
        }

        return -1;
    }

    /// <summary>
    /// Returns <paramref name="baseName"/> if no entry in <paramref name="existing"/>
    /// already uses it (case-insensitive); otherwise appends "_1", "_2", … until
    /// an unused name is found.
    /// </summary>
    private static string MakeUniqueLogicalIdxName(string baseName, IReadOnlyList<string> existing)
    {
        var taken = new HashSet<string>(existing, StringComparer.OrdinalIgnoreCase);
        if (!taken.Contains(baseName))
        {
            return baseName;
        }

        for (int i = 1; i < int.MaxValue; i++)
        {
            string candidate = baseName + "_" + i.ToString(CultureInfo.InvariantCulture);
            if (!taken.Contains(candidate))
            {
                return candidate;
            }
        }

        return baseName;
    }

    // ════════════════════════════════════════════════════════════════
    // W10 — Foreign-key runtime enforcement on Insert / Update / Delete
    // ════════════════════════════════════════════════════════════════
    //
    // Honors the EnforceReferentialIntegrity / CascadeUpdates / CascadeDeletes
    // flags that were emitted into MSysRelationships.grbit by W9a. When a
    // relationship has EnforceReferentialIntegrity=false the row simply does
    // not enter the enforced-set returned by GetEnforcedRelationshipsAsync.
    //
    // Enforcement strategy is intentionally simple — there is no SQL engine
    // and no index seek; every check scans the relevant table snapshot once
    // per public mutation call (NOT once per row). For bulk InsertRowsAsync
    // the parent-key set is built lazily on first FK violation check and
    // reused across all rows in the same call. Self-referential inserts
    // augment the cached set after each successful insert so a row that
    // satisfies its own FK can be inserted.
    //
    // See docs/design/index-and-relationship-format-notes.md §7 W10.

    /// <summary>
    /// In-memory representation of a single enforced foreign-key relationship,
    /// aggregated from one or more <c>MSysRelationships</c> rows (one per
    /// FK column). Only relationships with the
    /// <c>NO_REFERENTIAL_INTEGRITY</c> grbit flag clear are returned.
    /// </summary>
    private sealed record FkRelationship(
        string Name,
        string PrimaryTable,
        IReadOnlyList<string> PrimaryColumns,
        string ForeignTable,
        IReadOnlyList<string> ForeignColumns,
        bool CascadeUpdates,
        bool CascadeDeletes);

    /// <summary>
    /// Per-call enforcement context: snapshot of every enforced relationship
    /// plus a lazy cache of parent-PK key sets keyed by relationship name.
    /// One context lives for the duration of a single public mutation call
    /// (Insert/Update/Delete) so the parent snapshot of a given table is
    /// loaded at most once per call even when many rows need checking.
    /// </summary>
    private sealed class FkContext
    {
        public FkContext(IReadOnlyList<FkRelationship> all)
        {
            this.All = all;
        }

        public IReadOnlyList<FkRelationship> All { get; }

        public Dictionary<string, HashSet<string>> ParentKeySets { get; }
            = new(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Loads every enforced foreign-key relationship from the
    /// <c>MSysRelationships</c> system table. Returns an empty list when the
    /// database does not contain that table or contains no enforced rows.
    /// </summary>
    private async ValueTask<IReadOnlyList<FkRelationship>> GetEnforcedRelationshipsAsync(CancellationToken cancellationToken)
    {
        long pg = await FindSystemTableTdefPageAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        if (pg == 0)
        {
            return Array.Empty<FkRelationship>();
        }

        DataTable t = await ReadTableSnapshotAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        try
        {
            if (!t.Columns.Contains("szRelationship"))
            {
                return Array.Empty<FkRelationship>();
            }

            var groups = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow r in t.Rows)
            {
                object nm = r["szRelationship"];
                if (nm == DBNull.Value)
                {
                    continue;
                }

                string name = nm.ToString() ?? string.Empty;
                if (name.Length == 0)
                {
                    continue;
                }

                if (!groups.TryGetValue(name, out List<DataRow>? list))
                {
                    list = new List<DataRow>();
                    groups[name] = list;
                }

                list.Add(r);
            }

            var result = new List<FkRelationship>(groups.Count);
            foreach (KeyValuePair<string, List<DataRow>> kvp in groups)
            {
                List<DataRow> rows = kvp.Value;
                rows.Sort((a, b) => Convert.ToInt32(a["icolumn"], CultureInfo.InvariantCulture)
                    .CompareTo(Convert.ToInt32(b["icolumn"], CultureInfo.InvariantCulture)));

                DataRow head = rows[0];
                int grbit = Convert.ToInt32(head["grbit"], CultureInfo.InvariantCulture);

                // grbit bits per W9a (Jackcess RelationshipImpl):
                //   0x00000002 NO_REFERENTIAL_INTEGRITY
                //   0x00000100 CASCADE_UPDATES
                //   0x00001000 CASCADE_DELETES
                bool enforce = (grbit & 0x00000002) == 0;
                if (!enforce)
                {
                    continue;
                }

                bool cascadeUpdates = (grbit & 0x00000100) != 0;
                bool cascadeDeletes = (grbit & 0x00001000) != 0;

                string primaryTable = head["szReferencedObject"]?.ToString() ?? string.Empty;
                string foreignTable = head["szObject"]?.ToString() ?? string.Empty;
                if (primaryTable.Length == 0 || foreignTable.Length == 0)
                {
                    continue;
                }

                var pk = new string[rows.Count];
                var fk = new string[rows.Count];
                for (int i = 0; i < rows.Count; i++)
                {
                    pk[i] = rows[i]["szReferencedColumn"]?.ToString() ?? string.Empty;
                    fk[i] = rows[i]["szColumn"]?.ToString() ?? string.Empty;
                }

                result.Add(new FkRelationship(kvp.Key, primaryTable, pk, foreignTable, fk, cascadeUpdates, cascadeDeletes));
            }

            return result;
        }
        finally
        {
            t.Dispose();
        }
    }

    /// <summary>
    /// Builds a canonical, type-tolerant string key from <paramref name="row"/>
    /// for the columns listed in <paramref name="columnIndexes"/>. Returns
    /// <see langword="null"/> when any component is <see cref="DBNull"/> /
    /// <see langword="null"/> — Access treats a partial-null FK tuple as
    /// unconstrained (the row is allowed even if no parent matches).
    /// </summary>
    private static string? BuildCompositeKey(object?[] row, int[] columnIndexes)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < columnIndexes.Length; i++)
        {
            int idx = columnIndexes[i];
            if (idx < 0 || idx >= row.Length)
            {
                return null;
            }

            object? v = row[idx];
            if (v == null || v is DBNull)
            {
                return null;
            }

            sb.Append('|');
            AppendNormalized(sb, v);
        }

        return sb.ToString();
    }

    private static void AppendNormalized(StringBuilder sb, object value)
    {
        switch (value)
        {
            case string s:
                // Access string equality is case-insensitive in JET — match that.
                sb.Append('S').Append(':').Append(s.ToUpperInvariant());
                break;
            case Guid g:
                sb.Append('G').Append(':').Append(g.ToString("N"));
                break;
            case byte[] b:
                sb.Append('B').Append(':').Append(Convert.ToBase64String(b));
                break;
            case DateTime dt:
                sb.Append('D').Append(':').Append(dt.ToUniversalTime().Ticks.ToString(CultureInfo.InvariantCulture));
                break;
            case bool bl:
                sb.Append('?').Append(':').Append(bl ? '1' : '0');
                break;
            case IConvertible c:
                // Numeric-ish: normalize through decimal for cross-width equality
                // (e.g. user passes int 5 against a long parent column).
                try
                {
                    decimal d = c.ToDecimal(CultureInfo.InvariantCulture);
                    sb.Append('N').Append(':').Append(d.ToString(CultureInfo.InvariantCulture));
                }
                catch (Exception ex) when (ex is FormatException || ex is InvalidCastException || ex is OverflowException)
                {
                    sb.Append('X').Append(':').Append(value.ToString() ?? string.Empty);
                }

                break;
            default:
                sb.Append('X').Append(':').Append(value.ToString() ?? string.Empty);
                break;
        }
    }

    /// <summary>
    /// Lazily builds (and caches inside <paramref name="ctx"/>) the set of
    /// composite-key strings for every row currently in <paramref name="rel"/>'s
    /// primary table. A relationship with a missing parent column or a missing
    /// parent table yields an empty set so all FK inserts will be rejected.
    /// </summary>
    private async ValueTask<HashSet<string>> GetParentKeySetAsync(FkRelationship rel, FkContext ctx, CancellationToken cancellationToken)
    {
        if (ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? cached))
        {
            return cached;
        }

        var set = new HashSet<string>(StringComparer.Ordinal);

        DataTable parent;
        try
        {
            parent = await ReadTableSnapshotAsync(rel.PrimaryTable, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            ctx.ParentKeySets[rel.Name] = set;
            return set;
        }

        try
        {
            var idx = new int[rel.PrimaryColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.PrimaryColumns.Count; i++)
            {
                idx[i] = parent.Columns.IndexOf(rel.PrimaryColumns[i]);
                if (idx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (ok)
            {
                foreach (DataRow row in parent.Rows)
                {
                    string? k = BuildCompositeKey(row.ItemArray, idx);
                    if (k != null)
                    {
                        _ = set.Add(k);
                    }
                }
            }
        }
        finally
        {
            parent.Dispose();
        }

        ctx.ParentKeySets[rel.Name] = set;
        return set;
    }

    /// <summary>
    /// Validates that every enforced FK whose foreign-side table is
    /// <paramref name="foreignTable"/> is satisfied by <paramref name="values"/>.
    /// Throws <see cref="InvalidOperationException"/> on the first violation.
    /// A FK column tuple containing any null component is allowed (Access
    /// permits unset FKs unless the column itself is required).
    /// </summary>
    private async ValueTask EnforceFkOnInsertAsync(string foreignTable, TableDef foreignDef, object[] values, FkContext ctx, CancellationToken cancellationToken)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.ForeignTable, foreignTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var fkIdx = new int[rel.ForeignColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.ForeignColumns.Count; i++)
            {
                fkIdx[i] = FindColumnIndex(foreignDef, rel.ForeignColumns[i]);
                if (fkIdx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            string? key = BuildCompositeKey(values, fkIdx);
            if (key == null)
            {
                continue;
            }

            HashSet<string> parentKeys = await GetParentKeySetAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (!parentKeys.Contains(key))
            {
                throw new InvalidOperationException(
                    $"INSERT into '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"no matching row in '{rel.PrimaryTable}' for the supplied {string.Join(", ", rel.ForeignColumns)} value(s).");
            }
        }
    }

    /// <summary>
    /// After a successful insert, augments any cached parent-key sets that
    /// reference <paramref name="primaryTable"/> with the row's PK tuple so
    /// subsequent inserts within the same call (self-references and bulk
    /// inserts where one row supplies the parent for the next) succeed.
    /// </summary>
    private static void AugmentParentSetsAfterInsert(string primaryTable, TableDef tableDef, object[] insertedValues, FkContext ctx)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? set))
            {
                continue;
            }

            var pkIdx = new int[rel.PrimaryColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.PrimaryColumns.Count; i++)
            {
                pkIdx[i] = FindColumnIndex(tableDef, rel.PrimaryColumns[i]);
                if (pkIdx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            string? key = BuildCompositeKey(insertedValues, pkIdx);
            if (key != null)
            {
                _ = set.Add(key);
            }
        }
    }

    /// <summary>
    /// For each enforced FK whose primary side is <paramref name="primaryTable"/>,
    /// inspects the rows currently being deleted (by their PK tuples in
    /// <paramref name="deletedKeys"/>) and either cascades the delete to the
    /// child table when <see cref="FkRelationship.CascadeDeletes"/> is set, or
    /// throws when child rows still reference one of the deleted PK tuples.
    /// </summary>
    private async ValueTask EnforceFkOnPrimaryDeleteAsync(
        string primaryTable,
        IReadOnlyList<string?> deletedKeys,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        if (depth > CascadeMaxDepth)
        {
            throw new InvalidOperationException(
                $"Foreign-key cascade depth exceeded {CascadeMaxDepth}. Possible cyclic relationship.");
        }

        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            // Find child rows referencing any of the deleted PK tuples.
            CatalogEntry childEntry = await GetRequiredCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            TableDef childDef = await ReadRequiredTableDefAsync(childEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);

            var fkIdx = new int[rel.ForeignColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.ForeignColumns.Count; i++)
            {
                fkIdx[i] = FindColumnIndex(childDef, rel.ForeignColumns[i]);
                if (fkIdx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            using DataTable childSnap = await ReadTableSnapshotAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            List<RowLocation> locations = await GetLiveRowLocationsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            int total = Math.Min(childSnap.Rows.Count, locations.Count);

            var deletedSet = new HashSet<string>(StringComparer.Ordinal);
            foreach (string? k in deletedKeys)
            {
                if (k != null)
                {
                    _ = deletedSet.Add(k);
                }
            }

            var matchingRowIndices = new List<int>();
            var matchingChildKeys = new List<string?>();
            for (int i = 0; i < total; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = BuildCompositeKey(childSnap.Rows[i].ItemArray, fkIdx);
                if (childKey == null)
                {
                    continue;
                }

                if (deletedSet.Contains(childKey))
                {
                    matchingRowIndices.Add(i);

                    // If this child is itself referenced by grandchildren, we need
                    // to recurse with the GRANDCHILD's PK tuples — i.e. the child
                    // row's own PK values (per any further relationship). Build
                    // the grandchild-side key (= this row's PK columns) on demand
                    // as we recurse below.
                    matchingChildKeys.Add(childKey);
                }
            }

            if (matchingRowIndices.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeDeletes)
            {
                throw new InvalidOperationException(
                    $"DELETE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{matchingRowIndices.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
            }

            // Capture the child rows' OWN PK tuples (per every relationship
            // whose primary side is the child table) before we delete them so
            // grandchildren can be cascaded recursively.
            var childPkSnapshots = new Dictionary<string, List<string?>>(StringComparer.OrdinalIgnoreCase);
            foreach (FkRelationship grandRel in ctx.All)
            {
                if (!string.Equals(grandRel.PrimaryTable, rel.ForeignTable, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var pkIdx = new int[grandRel.PrimaryColumns.Count];
                bool pkOk = true;
                for (int i = 0; i < grandRel.PrimaryColumns.Count; i++)
                {
                    pkIdx[i] = FindColumnIndex(childDef, grandRel.PrimaryColumns[i]);
                    if (pkIdx[i] < 0)
                    {
                        pkOk = false;
                        break;
                    }
                }

                if (!pkOk)
                {
                    continue;
                }

                var keys = new List<string?>(matchingRowIndices.Count);
                foreach (int rIdx in matchingRowIndices)
                {
                    keys.Add(BuildCompositeKey(childSnap.Rows[rIdx].ItemArray, pkIdx));
                }

                childPkSnapshots[grandRel.Name] = keys;
            }

            // Recurse for grandchildren BEFORE we delete this level so the
            // grandchild-side enforcement can still see the parent rows
            // (it uses table snapshots independent of our pending deletes,
            // but cascading bottom-up keeps the state consistent on disk).
            foreach (KeyValuePair<string, List<string?>> kvp in childPkSnapshots)
            {
                await EnforceFkOnPrimaryDeleteAsync(rel.ForeignTable, kvp.Value, ctx, depth + 1, cancellationToken).ConfigureAwait(false);
            }

            // Now delete the child rows.
            int deleted = 0;
            foreach (int rIdx in matchingRowIndices)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await MarkRowDeletedAsync(locations[rIdx].PageNumber, locations[rIdx].RowIndex, cancellationToken).ConfigureAwait(false);
                deleted++;
            }

            if (deleted > 0)
            {
                await AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
                await MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// For each enforced FK whose primary side is <paramref name="primaryTable"/>,
    /// when a row's PK tuple is changing from <c>oldKey</c> to <c>newKey</c>,
    /// either propagates the change to dependent child rows (cascade-update)
    /// or rejects the update when child rows reference the old key and
    /// cascade-update is not enabled.
    /// </summary>
    private async ValueTask EnforceFkOnPrimaryUpdateAsync(
        string primaryTable,
        IReadOnlyList<(string? OldKey, object[] NewPkValues)> changes,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        if (depth > CascadeMaxDepth)
        {
            throw new InvalidOperationException(
                $"Foreign-key cascade depth exceeded {CascadeMaxDepth}. Possible cyclic relationship.");
        }

        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            CatalogEntry childEntry = await GetRequiredCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            TableDef childDef = await ReadRequiredTableDefAsync(childEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);

            // Map this relationship's PK column ordinals on the PRIMARY-side def too
            // (we need to extract the new PK tuple from NewPkValues which is in
            // primary-table column order).
            // Caller passed NewPkValues already in primary table column order;
            // we still need to slice out just the PK columns of THIS rel.
            CatalogEntry primaryEntry = await GetRequiredCatalogEntryAsync(primaryTable, cancellationToken).ConfigureAwait(false);
            TableDef primaryDef = await ReadRequiredTableDefAsync(primaryEntry.TDefPage, primaryTable, cancellationToken).ConfigureAwait(false);
            var primaryPkIdx = new int[rel.PrimaryColumns.Count];
            var fkIdx = new int[rel.ForeignColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.PrimaryColumns.Count; i++)
            {
                primaryPkIdx[i] = FindColumnIndex(primaryDef, rel.PrimaryColumns[i]);
                fkIdx[i] = FindColumnIndex(childDef, rel.ForeignColumns[i]);
                if (primaryPkIdx[i] < 0 || fkIdx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            // Build (oldKey -> newPkObjects) map for changes whose PK actually moves.
            var movingChanges = new Dictionary<string, object[]>(StringComparer.Ordinal);
            foreach ((string? oldKey, object[] newPkValues) in changes)
            {
                if (oldKey == null)
                {
                    continue;
                }

                string? newKey = BuildCompositeKey(newPkValues, primaryPkIdx);
                if (newKey == null || string.Equals(newKey, oldKey, StringComparison.Ordinal))
                {
                    // Either new tuple is null (will fail other constraints) or
                    // unchanged — no dependents to touch.
                    continue;
                }

                var newPkSubset = new object[rel.PrimaryColumns.Count];
                for (int i = 0; i < rel.PrimaryColumns.Count; i++)
                {
                    newPkSubset[i] = newPkValues[primaryPkIdx[i]];
                }

                movingChanges[oldKey] = newPkSubset;
            }

            if (movingChanges.Count == 0)
            {
                continue;
            }

            using DataTable childSnap = await ReadTableSnapshotAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            List<RowLocation> locations = await GetLiveRowLocationsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            int total = Math.Min(childSnap.Rows.Count, locations.Count);

            // Find affected child rows.
            var affectedIndices = new List<int>();
            var affectedOldKeys = new List<string>();
            for (int i = 0; i < total; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = BuildCompositeKey(childSnap.Rows[i].ItemArray, fkIdx);
                if (childKey != null && movingChanges.ContainsKey(childKey))
                {
                    affectedIndices.Add(i);
                    affectedOldKeys.Add(childKey);
                }
            }

            if (affectedIndices.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeUpdates)
            {
                throw new InvalidOperationException(
                    $"UPDATE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{affectedIndices.Count} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
            }

            // Cascade — rewrite each affected row with the new FK values.
            for (int ai = 0; ai < affectedIndices.Count; ai++)
            {
                int rIdx = affectedIndices[ai];
                object[] newPkSubset = movingChanges[affectedOldKeys[ai]];
                object[] rowValues = childSnap.Rows[rIdx].ItemArray;

                for (int j = 0; j < rel.ForeignColumns.Count; j++)
                {
                    rowValues[fkIdx[j]] = newPkSubset[j] ?? DBNull.Value;
                }

                await MarkRowDeletedAsync(locations[rIdx].PageNumber, locations[rIdx].RowIndex, cancellationToken).ConfigureAwait(false);
                await InsertRowDataAsync(childEntry.TDefPage, childDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            await MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Locates a system or user table's TDEF page number by name (case-insensitive)
    /// by scanning every <c>MSysObjects</c> row. Returns <c>0</c> when not found.
    /// </summary>
    private async ValueTask<long> FindSystemTableTdefPageAsync(string tableName, CancellationToken cancellationToken)
    {
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == OBJ_TABLE
                && row.TDefPage > 0
                && string.Equals(row.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return row.TDefPage;
            }
        }

        return 0;
    }

    /// <summary>
    /// Reads the distinct values of <c>szRelationship</c> from every live row in the
    /// <c>MSysRelationships</c> table. Used to enforce relationship-name uniqueness.
    /// </summary>
    private async ValueTask<HashSet<string>> ReadExistingRelationshipNamesAsync(long msysRelTdefPage, TableDef msysRelDef, CancellationToken cancellationToken)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        ColumnInfo nameCol = msysRelDef.Columns.Find(c => string.Equals(c.Name, "szRelationship", StringComparison.OrdinalIgnoreCase));
        if (nameCol == null)
        {
            return names;
        }

        long total = _stream.Length / _pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (Ri32(page, _dpTDefOff) != msysRelTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    string name = ReadColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.IsNullOrEmpty(name))
                    {
                        _ = names.Add(name);
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return names;
    }

    // ════════════════════════════════════════════════════════════════
    // Encryption mutation: change password / encrypt / decrypt
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Detects the on-disk encryption format of the database at
    /// <paramref name="path"/>. Returns <see cref="AccessEncryptionFormat.None"/>
    /// when the file is unencrypted. The file is read but not modified.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static async ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
        return await DetectEncryptionFormatAsync(fs, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Detects the on-disk encryption format of the database in <paramref name="stream"/>
    /// without modifying it. The stream must be seekable.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static async ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        if (!stream.CanRead || !stream.CanSeek)
        {
            throw new ArgumentException("Stream must be readable and seekable.", nameof(stream));
        }

        cancellationToken.ThrowIfCancellationRequested();

        long origin = stream.Position;
        try
        {
            _ = stream.Seek(0, SeekOrigin.Begin);
            byte[] sniff = new byte[0x80];
            int read = 0;
            while (read < sniff.Length)
            {
                int got = await stream.ReadAsync(sniff.AsMemory(read, sniff.Length - read), cancellationToken).ConfigureAwait(false);
                if (got == 0)
                {
                    break;
                }

                read += got;
            }

            return EncryptionConverter.Detect(sniff);
        }
        finally
        {
            _ = stream.Seek(origin, SeekOrigin.Begin);
        }
    }

    /// <summary>
    /// Changes the password of an already-encrypted JET / ACE database in place,
    /// preserving the existing on-disk encryption format. Use
    /// <see cref="EncryptAsync(string, string, AccessEncryptionFormat, AccessWriterOptions?, CancellationToken)"/>
    /// to add encryption to an unencrypted database, or
    /// <see cref="DecryptAsync(string, string, AccessWriterOptions?, CancellationToken)"/>
    /// to remove it.
    /// </summary>
    /// <param name="path">Path to an existing encrypted .mdb or .accdb file.</param>
    /// <param name="oldPassword">The current password.</param>
    /// <param name="newPassword">The new password (must be non-empty).</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring; the password fields are ignored.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="UnauthorizedAccessException">The supplied <paramref name="oldPassword"/> is wrong, or the database is unencrypted.</exception>
    /// <exception cref="ArgumentException"><paramref name="newPassword"/> is null or empty.</exception>
    public static ValueTask ChangePasswordAsync(
        string path,
        string oldPassword,
        string newPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
        return ReencryptFileAsync(
            path,
            oldPassword,
            newPassword,
            targetFormat: null,
            requireSourceEncrypted: true,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Encrypts a currently-unencrypted JET / ACE database in place, applying the
    /// requested <paramref name="targetFormat"/>.
    /// </summary>
    /// <param name="path">Path to an existing unencrypted .mdb or .accdb file.</param>
    /// <param name="newPassword">The password to apply (must be non-empty).</param>
    /// <param name="targetFormat">The encryption format to use. Must be valid for the file kind (Jet4 / ACE).</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="newPassword"/> is null/empty,
    /// <paramref name="targetFormat"/> is <see cref="AccessEncryptionFormat.None"/>,
    /// or the format is not valid for the underlying file kind.
    /// </exception>
    /// <exception cref="InvalidOperationException">The file is already encrypted.</exception>
    public static ValueTask EncryptAsync(
        string path,
        string newPassword,
        AccessEncryptionFormat targetFormat,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
        if (targetFormat == AccessEncryptionFormat.None)
        {
            throw new ArgumentException(
                "Target format must not be None. Use DecryptAsync to remove encryption.",
                nameof(targetFormat));
        }

        return ReencryptFileAsync(
            path,
            oldPassword: null,
            newPassword,
            targetFormat,
            requireSourceEncrypted: false,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Removes encryption from a JET / ACE database in place, leaving an
    /// unencrypted file with no header password residue.
    /// </summary>
    /// <param name="path">Path to an existing encrypted .mdb or .accdb file.</param>
    /// <param name="oldPassword">The current password.</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="UnauthorizedAccessException">The supplied <paramref name="oldPassword"/> is wrong.</exception>
    /// <exception cref="InvalidOperationException">The file is already unencrypted.</exception>
    public static ValueTask DecryptAsync(
        string path,
        string oldPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        return ReencryptFileAsync(
            path,
            oldPassword,
            newPassword: null,
            targetFormat: AccessEncryptionFormat.None,
            requireSourceEncrypted: true,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="ChangePasswordAsync(string, string, string, AccessWriterOptions?, CancellationToken)"/>.
    /// The stream must be readable, writable, and seekable; it is rewritten
    /// in place (length may change for Agile transitions).
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the database bytes.</param>
    /// <param name="oldPassword">The current password.</param>
    /// <param name="newPassword">The new password (must be non-empty).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask ChangePasswordAsync(
        Stream stream,
        string oldPassword,
        string newPassword,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
        return ReencryptStreamAsync(
            stream,
            oldPassword,
            newPassword,
            targetFormat: null,
            requireSourceEncrypted: true,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="EncryptAsync(string, string, AccessEncryptionFormat, AccessWriterOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the unencrypted database bytes.</param>
    /// <param name="newPassword">The password to apply.</param>
    /// <param name="targetFormat">The encryption format to use.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask EncryptAsync(
        Stream stream,
        string newPassword,
        AccessEncryptionFormat targetFormat,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
        if (targetFormat == AccessEncryptionFormat.None)
        {
            throw new ArgumentException(
                "Target format must not be None. Use DecryptAsync to remove encryption.",
                nameof(targetFormat));
        }

        return ReencryptStreamAsync(
            stream,
            oldPassword: null,
            newPassword,
            targetFormat,
            requireSourceEncrypted: false,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="DecryptAsync(string, string, AccessWriterOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the encrypted database bytes.</param>
    /// <param name="oldPassword">The current password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask DecryptAsync(
        Stream stream,
        string oldPassword,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        return ReencryptStreamAsync(
            stream,
            oldPassword,
            newPassword: null,
            targetFormat: AccessEncryptionFormat.None,
            requireSourceEncrypted: true,
            cancellationToken);
    }

    private static async ValueTask ReencryptFileAsync(
        string path,
        string? oldPassword,
        string? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        AccessWriterOptions? options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        // Lockfile honouring: respect any existing lockfile while we rewrite.
        bool useLockFile = options?.UseLockFile ?? true;
        bool respectLockFile = options?.RespectExistingLockFile ?? true;
        if (useLockFile)
        {
            LockFileManager.Create(path, nameof(AccessWriter), respectLockFile);
        }

        try
        {
            byte[] sourceBytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
            using var sourceStream = new MemoryStream(sourceBytes, writable: false);

            byte[] result = await ReencryptCoreAsync(
                sourceStream,
                oldPassword,
                newPassword,
                targetFormat,
                requireSourceEncrypted,
                cancellationToken).ConfigureAwait(false);

            // Atomic-ish replace: write to a sibling temp file then move into place.
            string tempPath = path + ".reenc-" + Guid.NewGuid().ToString("N") + ".tmp";
            await using (var fs = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
            {
                await fs.WriteAsync(result.AsMemory(), cancellationToken).ConfigureAwait(false);
                await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            try
            {
                File.Replace(tempPath, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
            }
            catch (PlatformNotSupportedException)
            {
                // Some filesystems / platforms don't support File.Replace.
                File.Delete(path);
                File.Move(tempPath, path);
            }
            catch (IOException)
            {
                // Fallback for filesystems that reject Replace (e.g. across volumes).
                File.Delete(path);
                File.Move(tempPath, path);
            }
        }
        finally
        {
            if (useLockFile)
            {
                LockFileManager.Delete(path, nameof(AccessWriter));
            }
        }
    }

    private static async ValueTask ReencryptStreamAsync(
        Stream stream,
        string? oldPassword,
        string? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(stream, nameof(stream));
        if (!stream.CanRead || !stream.CanWrite || !stream.CanSeek)
        {
            throw new ArgumentException("Stream must be readable, writable, and seekable.", nameof(stream));
        }

        byte[] result = await ReencryptCoreAsync(
            stream,
            oldPassword,
            newPassword,
            targetFormat,
            requireSourceEncrypted,
            cancellationToken).ConfigureAwait(false);

        _ = stream.Seek(0, SeekOrigin.Begin);
        await stream.WriteAsync(result.AsMemory(), cancellationToken).ConfigureAwait(false);
        stream.SetLength(result.Length);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<byte[]> ReencryptCoreAsync(
        Stream source,
        string? oldPassword,
        string? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        SecureString? oldSecure = SecureStringUtilities.FromPlainText(oldPassword);
        SecureString? newSecure = SecureStringUtilities.FromPlainText(newPassword);
        try
        {
            // Peek the format up front so EncryptAsync / DecryptAsync /
            // ChangePasswordAsync produce a clear InvalidOperationException
            // (instead of an UnauthorizedAccessException from the decrypt
            // step) when the source file is in the wrong state for the
            // requested operation.
            long origPos = source.Position;
            AccessEncryptionFormat detectedFormat = await DetectEncryptionFormatAsync(source, cancellationToken).ConfigureAwait(false);
            _ = source.Seek(origPos, SeekOrigin.Begin);

            if (requireSourceEncrypted && detectedFormat == AccessEncryptionFormat.None)
            {
                throw new InvalidOperationException(
                    "The source database is not encrypted. Use EncryptAsync to add a password.");
            }

            if (!requireSourceEncrypted && detectedFormat != AccessEncryptionFormat.None)
            {
                throw new InvalidOperationException(
                    $"The source database is already encrypted ({detectedFormat}). Use ChangePasswordAsync or DecryptAsync.");
            }

            (byte[] plaintext, AccessEncryptionFormat sourceFormat) = await EncryptionConverter
                .ReadDecryptedAsync(source, oldSecure, cancellationToken)
                .ConfigureAwait(false);

            // Default target = same as source when caller didn't override.
            AccessEncryptionFormat effectiveTarget = targetFormat ?? sourceFormat;
            return EncryptionConverter.ApplyEncryption(plaintext, effectiveTarget, newSecure);
        }
        finally
        {
            oldSecure?.Dispose();
            newSecure?.Dispose();
        }
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

        // For Agile-encrypted databases the underlying _stream is an in-memory
        // copy of the *decrypted* ACCDB. Re-encrypt it before tearing down so
        // the user's outer encrypted stream/file ends up with all writes.
        if (_isAgileEncryptedRewrap && _outerEncryptedStream != null && _password != null)
        {
            try
            {
                await RewrapAgileOnDisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                if (!_outerEncryptedLeaveOpen)
                {
                    await _outerEncryptedStream.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        _password?.Dispose();
        _stateLock.Dispose();

        await base.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Re-encrypts the in-memory decrypted ACCDB (held by <c>_stream</c>) using
    /// freshly-generated Agile parameters and writes the resulting CFB document
    /// back to <see cref="_outerEncryptedStream"/>. Called from
    /// <see cref="DisposeAsync"/> when the writer was opened on an Agile-encrypted
    /// .accdb file.
    /// </summary>
    private async ValueTask RewrapAgileOnDisposeAsync()
    {
        var memory = _stream as MemoryStream
            ?? throw new InvalidOperationException("Agile-encrypted writer expected an in-memory backing stream.");

        byte[] inner = memory.ToArray();

        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoAgile.Encrypt(inner, _password!);

        byte[] cfb = CompoundFileWriter.Build(new[]
        {
            new KeyValuePair<string, byte[]>("EncryptionInfo", encryptionInfo),
            new KeyValuePair<string, byte[]>("EncryptedPackage", encryptedPackage),
        });

        _ = _outerEncryptedStream!.Seek(0, SeekOrigin.Begin);
        await _outerEncryptedStream.WriteAsync(cfb.AsMemory()).ConfigureAwait(false);
        _outerEncryptedStream.SetLength(cfb.Length);
        await _outerEncryptedStream.FlushAsync().ConfigureAwait(false);
    }

    private static ColumnConstraint ToConstraint(ColumnDefinition def)
    {
        return new ColumnConstraint
        {
            Name = def.Name,
            ClrType = def.ClrType,
            IsNullable = def.IsNullable,
            DefaultValue = def.DefaultValue,
            IsAutoIncrement = def.IsAutoIncrement,
            ValidationRule = def.ValidationRule,
        };
    }

    private static bool IsIntegralType(Type t)
    {
        return t == typeof(byte) || t == typeof(short) || t == typeof(int) || t == typeof(long);
    }

    // The return type must remain 'object' so callers can store the boxed integral
    // (byte/short/int/long) directly into a values[] array preserving the column's CLR type.
#pragma warning disable CA1859
    private static object ConvertIntegral(long value, Type targetType)
#pragma warning restore CA1859
    {
        if (targetType == typeof(byte))
        {
            return checked((byte)value);
        }

        if (targetType == typeof(short))
        {
            return checked((short)value);
        }

        if (targetType == typeof(int))
        {
            return checked((int)value);
        }

        if (targetType == typeof(long))
        {
            return value;
        }

        return value;
    }

    private void RegisterConstraints(string tableName, IReadOnlyList<ColumnDefinition> defs)
    {
        var list = new List<ColumnConstraint>(defs.Count);
        bool anyConstraint = false;
        foreach (ColumnDefinition def in defs)
        {
            ColumnConstraint c = ToConstraint(def);
            anyConstraint |= c.HasAnyConstraint;

            if (c.IsAutoIncrement && !IsIntegralType(c.ClrType))
            {
                throw new ArgumentException(
                    $"Column '{c.Name}' is marked IsAutoIncrement=true but its CLR type '{c.ClrType}' is not an integer type.",
                    nameof(defs));
            }

            list.Add(c);
        }

        if (anyConstraint)
        {
            _constraints[tableName] = list;
        }
        else
        {
            _constraints.Remove(tableName);
        }
    }

    private void UnregisterConstraints(string tableName)
    {
        _constraints.Remove(tableName);
    }

    private void RenameConstraintsTable(string oldName, string newName)
    {
        if (_constraints.TryGetValue(oldName, out List<ColumnConstraint>? list))
        {
            _constraints.Remove(oldName);
            _constraints[newName] = list;
        }
    }

    /// <summary>
    /// Rebuilds a per-column constraint list from the persisted TDEF column flags.
    /// Only the bits that JET physically stores (FLAG_NULL_ALLOWED, FLAG_AUTO_LONG)
    /// are restored — DefaultValue and ValidationRule remain client-side and are
    /// only present when the same writer instance declared them.
    /// </summary>
    private List<ColumnConstraint> HydrateConstraintsFromTableDef(string tableName, TableDef tableDef)
    {
        var list = new List<ColumnConstraint>(tableDef.Columns.Count);
        bool anyConstraint = false;
        foreach (ColumnInfo col in tableDef.Columns)
        {
            bool isNullable = (col.Flags & 0x02) != 0;
            bool isAutoIncrement = (col.Flags & 0x04) != 0;

            ColumnConstraint c = new()
            {
                Name = col.Name,
                ClrType = TdefTypeToClrType(col.Type),
                IsNullable = isNullable,
                IsAutoIncrement = isAutoIncrement,
            };

            anyConstraint |= c.HasAnyConstraint;
            list.Add(c);
        }

        if (anyConstraint)
        {
            _constraints[tableName] = list;
        }

        return list;
    }

    private static Type TdefTypeToClrType(byte type)
    {
        switch (type)
        {
            case T_BOOL: return typeof(bool);
            case T_BYTE: return typeof(byte);
            case T_INT: return typeof(short);
            case T_LONG: return typeof(int);
            case T_MONEY: return typeof(decimal);
            case T_FLOAT: return typeof(float);
            case T_DOUBLE: return typeof(double);
            case T_DATETIME: return typeof(DateTime);
            case T_NUMERIC: return typeof(decimal);
            case T_GUID: return typeof(Guid);
            case T_TEXT:
            case T_MEMO: return typeof(string);
            case T_BINARY:
            case T_OLE: return typeof(byte[]);
            default: return typeof(object);
        }
    }

    private async ValueTask ApplyConstraintsAsync(string tableName, TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        if (!_constraints.TryGetValue(tableName, out List<ColumnConstraint>? list) || list == null)
        {
            // The table may have been created by an earlier writer instance (or by Access
            // itself). Hydrate the registry from the persisted column flags so NOT NULL /
            // AutoIncrement constraints declared at CreateTableAsync time still take effect
            // after the database is closed and reopened. DefaultValue and ValidationRule are
            // not persisted in the JET TDEF and remain client-side only.
            list = HydrateConstraintsFromTableDef(tableName, tableDef);
        }

        // The constraint list is positionally aligned with the columns at registration time.
        // Add/Drop/Rename re-registers, so the count must match. Defensive bail-out otherwise.
        if (list.Count != tableDef.Columns.Count || values.Length != tableDef.Columns.Count)
        {
            return;
        }

        for (int i = 0; i < list.Count; i++)
        {
            ColumnConstraint c = list[i];
            object? value = values[i];
            bool isNull = value is null || value is DBNull;

            if (isNull && c.DefaultValue != null)
            {
                value = c.DefaultValue;
                isNull = false;
            }

            if (isNull && c.IsAutoIncrement)
            {
                long next = await GetNextAutoValueAsync(tableName, c, i, cancellationToken).ConfigureAwait(false);
                value = ConvertIntegral(next, c.ClrType);
                isNull = false;
            }

            if (isNull && !c.IsNullable)
            {
                throw new InvalidOperationException(
                    $"Column '{c.Name}' on table '{tableName}' is marked NOT NULL and no value was supplied.");
            }

            if (!isNull && c.ValidationRule != null && !c.ValidationRule(value))
            {
                throw new ArgumentException(
                    $"Validation rule for column '{c.Name}' on table '{tableName}' rejected value '{value}'.");
            }

            values[i] = value ?? DBNull.Value;
        }
    }

    private async ValueTask<long> GetNextAutoValueAsync(string tableName, ColumnConstraint c, int columnIndex, CancellationToken cancellationToken)
    {
        if (c.NextAutoValue == null)
        {
            long max = 0;
            using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (snapshot.Columns.Count > columnIndex)
            {
                foreach (DataRow row in snapshot.Rows)
                {
                    object cell = row[columnIndex];
                    if (cell is null || cell is DBNull)
                    {
                        continue;
                    }

                    try
                    {
                        long v = Convert.ToInt64(cell, CultureInfo.InvariantCulture);
                        if (v > max)
                        {
                            max = v;
                        }
                    }
                    catch (FormatException)
                    {
                    }
                    catch (InvalidCastException)
                    {
                    }
                    catch (OverflowException)
                    {
                    }
                }
            }

            c.NextAutoValue = max + 1;
        }

        long assigned = c.NextAutoValue.Value;
        c.NextAutoValue = assigned + 1;
        return assigned;
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

            // JET column-descriptor flag bits (mdbtools naming):
            //   0x01  FLAG_FIXED        — value lives in the fixed area of the row
            //   0x02  FLAG_NULL_ALLOWED — column accepts NULL (cleared = NOT NULL)
            //   0x04  FLAG_AUTO_LONG    — auto-increment (Access "AutoNumber")
            // IsNullable and IsAutoIncrement are persisted here so that constraints
            // declared on CreateTableAsync survive across writer instances and can be
            // rebuilt from the TDEF when the database is reopened.
            byte flags = 0;
            if (!variable)
            {
                flags |= 0x01;
            }

            if (definition.IsNullable)
            {
                flags |= 0x02;
            }

            if (definition.IsAutoIncrement)
            {
                flags |= 0x04;
            }

            var column = new ColumnInfo
            {
                Name = definition.Name,
                Type = type,
                ColNum = i,
                VarIdx = variable ? nextVarIndex : 0,
                FixedOff = variable ? 0 : fixedOffset,
                Size = size,
                Flags = flags,
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
        if (!string.IsNullOrEmpty(_path) && !_isAgileEncryptedRewrap)
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

    /// <summary>
    /// Opens a transient <see cref="AccessReader"/> against the same backing file/stream
    /// to enumerate <paramref name="tableName"/>'s logical indexes via the same parser
    /// that <see cref="IAccessReader.ListIndexesAsync"/> uses. Used by
    /// <see cref="RewriteTableAsync"/> to forward existing index definitions through
    /// Add/Drop/Rename column operations (W5).
    /// </summary>
    private async ValueTask<IReadOnlyList<IndexMetadata>> ReadIndexMetadataSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var options = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            Password = _password,
        };

        AccessReader reader;
        if (!string.IsNullOrEmpty(_path) && !_isAgileEncryptedRewrap)
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
            return await reader.ListIndexesAsync(tableName, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Default index-projection strategy used by <see cref="RewriteTableAsync"/>
    /// when no caller-specific projection is supplied. Forwards every existing
    /// normal or primary-key index whose key columns all still exist
    /// (case-insensitive name match) in the rebuilt schema. Non-PK indexes
    /// must be single-column (multi-column non-PK indexes are not supported).
    /// FK indexes are not forwarded today (W9 territory).
    /// </summary>
    private static List<IndexDefinition> DefaultIndexProjection(IReadOnlyList<IndexMetadata> existing, IReadOnlyList<ColumnDefinition> newDefs)
    {
        var result = new List<IndexDefinition>(existing.Count);
        var newColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnDefinition c in newDefs)
        {
            newColumnNames.Add(c.Name);
        }

        foreach (IndexMetadata idx in existing)
        {
            if (idx.Kind != IndexKind.Normal && idx.Kind != IndexKind.PrimaryKey)
            {
                continue;
            }

            if (idx.Columns.Count == 0)
            {
                continue;
            }

            if (idx.Kind == IndexKind.Normal && idx.Columns.Count != 1)
            {
                continue;
            }

            bool allSurvive = true;
            foreach (IndexColumnReference ic in idx.Columns)
            {
                if (string.IsNullOrEmpty(ic.Name) || !newColumnNames.Contains(ic.Name))
                {
                    allSurvive = false;
                    break;
                }
            }

            if (!allSurvive)
            {
                continue;
            }

            if (idx.Kind == IndexKind.PrimaryKey)
            {
                var pkCols = new string[idx.Columns.Count];
                for (int i = 0; i < idx.Columns.Count; i++)
                {
                    pkCols[i] = idx.Columns[i].Name;
                }

                result.Add(new IndexDefinition(idx.Name, pkCols) { IsPrimaryKey = true });
            }
            else
            {
                result.Add(new IndexDefinition(idx.Name, idx.Columns[0].Name));
            }
        }

        return result;
    }

    /// <summary>
    /// Opens a transient <see cref="AccessReader"/> against the same backing file/stream
    /// to read and parse the <c>MSysObjects.LvProp</c> blob for the catalog row whose
    /// <c>Id</c> low-24 bits equal <paramref name="tdefPage"/>. Returns
    /// <see langword="null"/> when the catalog has no <c>LvProp</c> column or the row
    /// has no property blob.
    /// </summary>
    private async ValueTask<ColumnPropertyBlock?> ReadLvPropBlockAsync(long tdefPage, CancellationToken cancellationToken)
    {
        var options = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            UseLockFile = false,
            Password = _password,
        };

        AccessReader reader;
        if (!string.IsNullOrEmpty(_path) && !_isAgileEncryptedRewrap)
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
            return await reader.ReadLvPropForTableAsync(tdefPage, cancellationToken).ConfigureAwait(false);
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

    private async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, CancellationToken cancellationToken = default)
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

        // LvProp is the OLE/LongBinary cell carrying per-column persisted properties
        // (DefaultValue, ValidationRule, ValidationText, Description). Only emitted on
        // the full 17-column catalog schema (the slim 9-column legacy schema lacks the
        // column entirely, so SetValue is a no-op).
        if (lvProp is not null)
        {
            SetValue(msys, values, "LvProp", lvProp);
        }

        await InsertRowDataAsync(2, msys, values, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask RewriteTableAsync(
        string tableName,
        Func<List<ColumnDefinition>, TableDef, List<ColumnDefinition>> projectColumns,
        Func<object[], TableDef, object[]> projectRow,
        CancellationToken cancellationToken,
        Func<IReadOnlyList<IndexMetadata>, IReadOnlyList<ColumnDefinition>, List<IndexDefinition>>? projectIndexes = null)
    {
        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        // Carry forward any client-side constraints registered for the original schema so
        // Add/Drop/Rename do not silently strip NotNull / Default / AutoIncrement / validation rules.
        _constraints.TryGetValue(tableName, out List<ColumnConstraint>? existingConstraints);

        // Hydrate persisted-property fields from MSysObjects.LvProp so that
        // DefaultValueExpression / ValidationRuleExpression / ValidationText / Description
        // round-trip through Add/Drop/Rename semantically. Forward-compat note: unknown
        // chunks and table-level property targets are not yet preserved by this path.
        ColumnPropertyBlock? originalProperties =
            await ReadLvPropBlockAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

        var existingDefs = new List<ColumnDefinition>(tableDef.Columns.Count);
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            ColumnDefinition baseDef = BuildColumnDefinitionFromInfo(col);
            if (existingConstraints != null && i < existingConstraints.Count
                && string.Equals(existingConstraints[i].Name, col.Name, StringComparison.OrdinalIgnoreCase))
            {
                ColumnConstraint c = existingConstraints[i];
                baseDef = baseDef with
                {
                    IsNullable = c.IsNullable,
                    DefaultValue = c.DefaultValue,
                    IsAutoIncrement = c.IsAutoIncrement,
                    ValidationRule = c.ValidationRule,
                };
            }

            ColumnPropertyTarget? target = originalProperties?.FindTarget(col.Name);
            if (target is not null)
            {
                baseDef = baseDef with
                {
                    DefaultValueExpression = target.GetTextValue(ColumnPropertyNames.DefaultValue, _format)
                        ?? baseDef.DefaultValueExpression,
                    ValidationRuleExpression = target.GetTextValue(ColumnPropertyNames.ValidationRule, _format)
                        ?? baseDef.ValidationRuleExpression,
                    ValidationText = target.GetTextValue(ColumnPropertyNames.ValidationText, _format)
                        ?? baseDef.ValidationText,
                    Description = target.GetTextValue(ColumnPropertyNames.Description, _format)
                        ?? baseDef.Description,
                };
            }

            existingDefs.Add(baseDef);
        }

        List<ColumnDefinition> newDefs = projectColumns(existingDefs, tableDef);
        if (newDefs.Count == 0)
        {
            throw new InvalidOperationException($"Table '{tableName}' must retain at least one column.");
        }

        // Snapshot existing rows AND existing indexes BEFORE we mutate the catalog,
        // so the snapshot reader sees the original schema and we can forward
        // surviving index definitions to the rebuilt table (W5).
        using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<IndexMetadata> existingIndexes = await ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        // Default index projection: keep every existing index whose single key
        // column survives in the new schema (matched by case-insensitive name).
        // AddColumn / DropColumn use this default; RenameColumn supplies a custom
        // projection that rewrites references to the renamed column.
        List<IndexDefinition> projectedIndexes = projectIndexes != null
            ? projectIndexes(existingIndexes, newDefs)
            : DefaultIndexProjection(existingIndexes, newDefs);

        string tempName = $"~tmp_{Guid.NewGuid():N}".Substring(0, 18);
        await CreateTableAsync(tempName, newDefs, projectedIndexes, cancellationToken).ConfigureAwait(false);

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

        // W5: rebuild forwarded indexes once after the bulk row copy completes,
        // so we don't pay the rebuild cost per row.
        if (projectedIndexes.Count > 0 && snapshot.Rows.Count > 0)
        {
            await MaintainIndexesAsync(tempEntry.TDefPage, tempDef, tempName, cancellationToken).ConfigureAwait(false);
        }

        // Drop the original table, then rename the temp catalog entry to take its place.
        // Pre-compute the LvProp blob from the projected columns so the catalog rename
        // re-emits the persisted properties under the user-facing table name.
        byte[]? renamedLvProp = JetExpressionConverter.BuildLvPropBlob(newDefs, _format);
        await DropTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        await RenameTableInCatalogAsync(tempName, tableName, renamedLvProp, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask RenameTableInCatalogAsync(string oldName, string newName, byte[]? lvProp, CancellationToken cancellationToken)
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

        await InsertCatalogEntryAsync(newName, tdefPage.Value, lvProp, cancellationToken).ConfigureAwait(false);
        RenameConstraintsTable(oldName, newName);
        InvalidateCatalogCache();
    }

    private ColumnDefinition BuildColumnDefinitionFromInfo(ColumnInfo column)
    {
        ColumnDefinition baseDef;
        switch (column.Type)
        {
            case T_BOOL: baseDef = new ColumnDefinition(column.Name, typeof(bool)); break;
            case T_BYTE: baseDef = new ColumnDefinition(column.Name, typeof(byte)); break;
            case T_INT: baseDef = new ColumnDefinition(column.Name, typeof(short)); break;
            case T_LONG: baseDef = new ColumnDefinition(column.Name, typeof(int)); break;
            case T_MONEY: baseDef = new ColumnDefinition(column.Name, typeof(decimal)); break;
            case T_FLOAT: baseDef = new ColumnDefinition(column.Name, typeof(float)); break;
            case T_DOUBLE: baseDef = new ColumnDefinition(column.Name, typeof(double)); break;
            case T_DATETIME: baseDef = new ColumnDefinition(column.Name, typeof(DateTime)); break;
            case T_NUMERIC: baseDef = new ColumnDefinition(column.Name, typeof(decimal)); break;
            case T_GUID: baseDef = new ColumnDefinition(column.Name, typeof(Guid)); break;
            case T_TEXT:
                int charLen = _format != DatabaseFormat.Jet3Mdb ? Math.Max(1, column.Size / 2) : Math.Max(1, column.Size);
                baseDef = new ColumnDefinition(column.Name, typeof(string), charLen);
                break;
            case T_MEMO: baseDef = new ColumnDefinition(column.Name, typeof(string)); break;
            case T_BINARY: baseDef = new ColumnDefinition(column.Name, typeof(byte[]), column.Size > 0 ? column.Size : 255); break;
            case T_OLE: baseDef = new ColumnDefinition(column.Name, typeof(byte[])); break;
            case T_ATTACHMENT:
            case T_COMPLEX:
                throw new NotSupportedException($"Column '{column.Name}' has a complex type (attachment / multi-value) that cannot be rewritten by AddColumnAsync / DropColumnAsync / RenameColumnAsync.");
            default:
                throw new NotSupportedException($"Column '{column.Name}' has unsupported type code 0x{column.Type:X2}.");
        }

        // Surface the persisted TDEF flag bits as ColumnDefinition properties so the
        // schema-rewrite path retains NOT NULL / auto-increment metadata that Access
        // wrote into the original column descriptor.
        return baseDef with
        {
            IsNullable = (column.Flags & 0x02) != 0,
            IsAutoIncrement = (column.Flags & 0x04) != 0,
        };
    }

    private byte[] BuildTDefPage(TableDef tableDef)
        => BuildTDefPageWithIndexOffsets(tableDef, Array.Empty<ResolvedIndex>()).Page;

    private byte[] BuildTDefPage(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
        => BuildTDefPageWithIndexOffsets(tableDef, indexes).Page;

    /// <summary>
    /// Builds a TDEF page and also returns, for each logical index in
    /// <paramref name="indexes"/>, the byte offset within the page of that
    /// real-index physical descriptor's <c>first_dp</c> field (§3.1). The
    /// caller uses these offsets to patch in the index leaf-page numbers
    /// after the leafs themselves have been appended (W3).
    /// </summary>
    private (byte[] Page, int[] FirstDpOffsets) BuildTDefPageWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
    {
        byte[] page = new byte[_pgSz];
        int numCols = tableDef.Columns.Count;
        int numIdx = indexes.Count;
        bool jet4 = _format != DatabaseFormat.Jet3Mdb;

        // W1 phase: one real-idx slot per logical-idx (no sharing). See
        // docs/design/index-and-relationship-format-notes.md §3.3.
        int numRealIdx = numIdx;
        int realIdxPhysSz = jet4 ? 52 : 39;
        int logIdxEntrySz = jet4 ? 28 : 20;

        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int namePos = colStart + (numCols * _colDescSz);
        int nameLenSize = jet4 ? 2 : 1;

        page[0] = 0x02;
        page[1] = 0x01;

        // TDEF header fields: offsets are relative to _tdNumCols / _tdNumRealIdx
        // so both JET3 and JET4 layouts are covered.
        page[_tdNumCols - 5] = 0x4E;
        Wu16(page, _tdNumCols - 4, numCols);
        Wu16(page, _tdNumCols, numCols);

        // num_idx (4 bytes immediately after num_cols) and num_real_idx.
        Wi32(page, _tdNumCols + 2, numIdx);
        Wi32(page, _tdNumRealIdx, numRealIdx);

        // Leading real-index entries (Jet4: 12 bytes each; Jet3: 8 bytes each).
        // Per mdbtools: unknown(4) + num_idx_rows(4) + unknown(4). Zeroed for W1.
        // Slot lives at _tdBlockEnd .. colStart and is already zero-initialised.

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

            byte[] nameBytes = jet4 ? Encoding.Unicode.GetBytes(col.Name) : _ansiEncoding.GetBytes(col.Name);
            if (namePos + nameLenSize + nameBytes.Length > page.Length)
            {
                throw new NotSupportedException("Table definition does not fit within a single TDEF page.");
            }

            if (jet4)
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

        // ── Index sections (W1+W3: only Jet4/ACE supports this code path) ─────
        int[] firstDpOffsets = numIdx > 0 ? new int[numIdx] : Array.Empty<int>();
        if (numIdx > 0)
        {
            int realIdxPhysStart = namePos;
            int logIdxStart = realIdxPhysStart + (numRealIdx * realIdxPhysSz);
            int logIdxNameStart = logIdxStart + (numIdx * logIdxEntrySz);

            // Bound check (logical-index name byte count is variable; account for it below).
            int totalIdxBytesLowerBound = logIdxNameStart - realIdxPhysStart;
            if (realIdxPhysStart + totalIdxBytesLowerBound > page.Length)
            {
                throw new NotSupportedException("Table definition (with indexes) does not fit within a single TDEF page.");
            }

            for (int i = 0; i < numIdx; i++)
            {
                ResolvedIndex ri = indexes[i];

                // ── Real-index physical descriptor (Jet4: 52 bytes) ─────────
                // §3.1: unknown(4) + col_map(30) + used_pages(4) + first_dp(4) + flags(1) + unknown(9).
                int phys = realIdxPhysStart + (i * realIdxPhysSz);

                // col_map: 10 × { col_num(2), col_order(1) }. First N slots = our
                // key columns (ascending), remaining slots filled with 0xFFFF.
                for (int slot = 0; slot < 10; slot++)
                {
                    int so = phys + 4 + (slot * 3);
                    if (slot < ri.ColumnNumbers.Count)
                    {
                        Wu16(page, so, ri.ColumnNumbers[slot]);
                        page[so + 2] = 0x01; // ascending
                    }
                    else
                    {
                        Wu16(page, so, 0xFFFF);
                        page[so + 2] = 0x00;
                    }
                }

                // used_pages (4 bytes) at phys + 34, first_dp (4 bytes) at phys + 38,
                // flags (1) at phys + 42, trailing 9 bytes at phys + 43..51 — all
                // start zero. The caller patches first_dp after appending the
                // leaf page (W3); used_pages remains 0 (no usage bitmap is
                // emitted). Per the §3.1 empirical correction, real Access
                // fixtures emit flags = 0x00 even for PK indexes — uniqueness
                // is signalled by index_type = 0x01 below, not the flag bit.
                firstDpOffsets[i] = phys + 4 + 30 + 4;

                // ── Logical-index entry (Jet4: 28 bytes) ────────────────────
                // §3.2: unknown(4) + index_num(4) + index_num2(4) + rel_tbl_type(1)
                //     + rel_idx_num(4) + rel_tbl_page(4) + cascade_ups(1) + cascade_dels(1)
                //     + index_type(1) + trailing(4).
                int log = logIdxStart + (i * logIdxEntrySz);
                Wi32(page, log + 4, i);                  // index_num
                Wi32(page, log + 8, i);                  // index_num2 (one real per logical)
                Wi32(page, log + 13, -1);                // rel_idx_num = 0xFFFFFFFF (not a FK)

                // index_type: 0x01 for PK (W8), 0x00 for normal. FK (0x02) is W9.
                page[log + 23] = (byte)(ri.IsPrimaryKey ? IndexKind.PrimaryKey : IndexKind.Normal);
            }

            // Logical-index names — same length-prefix encoding as column names.
            int npos = logIdxNameStart;
            for (int i = 0; i < numIdx; i++)
            {
                byte[] nameBytes = jet4 ? Encoding.Unicode.GetBytes(indexes[i].Name) : _ansiEncoding.GetBytes(indexes[i].Name);
                if (npos + nameLenSize + nameBytes.Length > page.Length)
                {
                    throw new NotSupportedException("Table definition (with indexes) does not fit within a single TDEF page.");
                }

                if (jet4)
                {
                    Wu16(page, npos, nameBytes.Length);
                }
                else
                {
                    page[npos] = (byte)nameBytes.Length;
                }

                npos += nameLenSize;
                Buffer.BlockCopy(nameBytes, 0, page, npos, nameBytes.Length);
                npos += nameBytes.Length;
            }

            namePos = npos;
        }

        Wi32(page, 8, Math.Max(0, namePos - 8));
        return (page, firstDpOffsets);
    }

    /// <summary>
    /// W8: synthesizes a primary-key <see cref="IndexDefinition"/> from any
    /// <see cref="ColumnDefinition.IsPrimaryKey"/> flags set on the supplied
    /// columns and forces those columns to <see cref="ColumnDefinition.IsNullable"/>
    /// = <c>false</c> on the returned column list (the JET TDEF flag bit
    /// <c>FLAG_NULL_ALLOWED 0x02</c> is cleared, matching Access semantics
    /// for PK columns). Mixing the column-level shortcut with an explicit
    /// PK <see cref="IndexDefinition"/> in the same call throws
    /// <see cref="ArgumentException"/>.
    /// </summary>
    private static (IReadOnlyList<ColumnDefinition> Columns, IReadOnlyList<IndexDefinition> Indexes) ApplyPrimaryKeyShortcut(
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<IndexDefinition> indexes)
    {
        bool anyColumnPk = false;
        foreach (ColumnDefinition c in columns)
        {
            if (c.IsPrimaryKey)
            {
                anyColumnPk = true;
                break;
            }
        }

        bool anyIndexPk = false;
        foreach (IndexDefinition idx in indexes)
        {
            if (idx.IsPrimaryKey)
            {
                anyIndexPk = true;
                break;
            }
        }

        if (anyColumnPk && anyIndexPk)
        {
            throw new ArgumentException(
                "Primary key declared both via ColumnDefinition.IsPrimaryKey and an explicit IndexDefinition.IsPrimaryKey. Use one or the other.");
        }

        // Force PK key columns (whether declared via column flag OR an explicit
        // PK IndexDefinition) to non-nullable on the emitted TDEF.
        HashSet<string>? pkColumnNames = null;
        if (anyColumnPk)
        {
            pkColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var pkColList = new List<string>();
            foreach (ColumnDefinition c in columns)
            {
                if (c.IsPrimaryKey)
                {
                    pkColumnNames.Add(c.Name);
                    pkColList.Add(c.Name);
                }
            }

            var newIndexes = new List<IndexDefinition>(indexes.Count + 1);
            newIndexes.AddRange(indexes);
            newIndexes.Add(new IndexDefinition("PrimaryKey", pkColList) { IsPrimaryKey = true });
            indexes = newIndexes;
        }
        else if (anyIndexPk)
        {
            pkColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (IndexDefinition idx in indexes)
            {
                if (idx.IsPrimaryKey)
                {
                    foreach (string col in idx.Columns)
                    {
                        pkColumnNames.Add(col);
                    }
                }
            }
        }

        if (pkColumnNames is not null)
        {
            var newCols = new ColumnDefinition[columns.Count];
            for (int i = 0; i < columns.Count; i++)
            {
                ColumnDefinition c = columns[i];
                if (pkColumnNames.Contains(c.Name) && c.IsNullable)
                {
                    c = c with { IsNullable = false };
                }

                newCols[i] = c;
            }

            columns = newCols;
        }

        return (columns, indexes);
    }

    private IReadOnlyList<ResolvedIndex> ResolveIndexes(IReadOnlyList<IndexDefinition> indexes, TableDef tableDef)
    {
        if (indexes.Count == 0)
        {
            return Array.Empty<ResolvedIndex>();
        }

        var result = new List<ResolvedIndex>(indexes.Count);
        var seenNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool sawPk = false;
        for (int i = 0; i < indexes.Count; i++)
        {
            IndexDefinition def = indexes[i];
            if (string.IsNullOrEmpty(def.Name))
            {
                throw new ArgumentException($"IndexDefinition at position {i} has an empty name.", nameof(indexes));
            }

            if (!seenNames.Add(def.Name))
            {
                throw new ArgumentException($"Duplicate index name '{def.Name}'.", nameof(indexes));
            }

            if (def.Columns.Count == 0)
            {
                throw new ArgumentException($"IndexDefinition '{def.Name}' must reference at least one column.", nameof(indexes));
            }

            // The JET col_map carries up to 10 columns per index (§3.1).
            if (def.Columns.Count > 10)
            {
                throw new NotSupportedException($"IndexDefinition '{def.Name}' has {def.Columns.Count} columns; the JET col_map supports at most 10.");
            }

            if (!def.IsPrimaryKey && def.Columns.Count != 1)
            {
                throw new NotSupportedException($"IndexDefinition '{def.Name}' must reference exactly one column (multi-column non-PK indexes are not supported).");
            }

            if (def.IsPrimaryKey)
            {
                if (sawPk)
                {
                    throw new ArgumentException("Only one primary-key index is permitted per table.", nameof(indexes));
                }

                sawPk = true;
            }

            var seenCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var colNums = new int[def.Columns.Count];
            for (int k = 0; k < def.Columns.Count; k++)
            {
                string columnName = def.Columns[k];
                if (!seenCols.Add(columnName))
                {
                    throw new ArgumentException($"IndexDefinition '{def.Name}' references column '{columnName}' more than once.", nameof(indexes));
                }

                ColumnInfo? column = tableDef.Columns.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
                if (column is null)
                {
                    throw new ArgumentException($"IndexDefinition '{def.Name}' references unknown column '{columnName}'.", nameof(indexes));
                }

                colNums[k] = column.ColNum;
            }

            result.Add(new ResolvedIndex(def.Name, colNums, def.IsPrimaryKey));
        }

        return result;
    }

    /// <summary>
    /// Builds a minimal, empty JET database as a byte array.
    /// The database contains three pages (page size varies by format):
    /// page 0 (header), page 1 (unused placeholder), and page 2 (MSysObjects TDEF).
    /// </summary>
    /// <param name="format">Target on-disk format.</param>
    /// <param name="fullCatalogSchema">
    /// When <see langword="true"/>, page 2 is bootstrapped with the real Access
    /// 17-column <c>MSysObjects</c> schema (matches files written by Microsoft
    /// Access across all Jet/ACE versions). When <see langword="false"/>, the
    /// historical 9-column slim schema is written instead.
    /// </param>
    private static byte[] BuildEmptyDatabase(DatabaseFormat format, bool fullCatalogSchema)
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
        BuildMSysObjectsTDef(db, pgSz * 2, format, fullCatalogSchema);

        return db;
    }

    /// <summary>
    /// Writes the MSysObjects TDEF page into <paramref name="db"/> at the given
    /// <paramref name="offset"/>.
    /// <para>
    /// When <paramref name="fullCatalogSchema"/> is <see langword="true"/>, emits
    /// the full 17-column Microsoft Access schema (verified empirically against
    /// Access-authored Jet3, Jet4, and ACE databases — the column list and column
    /// types are identical across all formats):
    /// <c>Id, ParentId, Name, Type, DateCreate, DateUpdate, Owner, Flags, Database,
    /// Connect, ForeignName, RmtInfoShort, RmtInfoLong, Lv, LvProp, LvModule,
    /// LvExtra</c>. The <c>LvProp</c> column is required for persisting per-column
    /// <c>DefaultValue</c> / <c>ValidationRule</c> / <c>ValidationText</c> /
    /// <c>Description</c> properties.
    /// </para>
    /// <para>
    /// When <paramref name="fullCatalogSchema"/> is <see langword="false"/>, emits
    /// the historical 9-column slim schema for backward-compatible byte layouts:
    /// <c>Id, ParentId, Name, Type, DateCreate, DateUpdate, Flags, ForeignName,
    /// Database</c>. Persisted column properties cannot be stored in this schema.
    /// </para>
    /// </summary>
    private static void BuildMSysObjectsTDef(byte[] db, int offset, DatabaseFormat format, bool fullCatalogSchema)
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

        // Flag bytes match what Microsoft Access writes (verified empirically):
        //   0x13 = fixed + nullable + system    (was 0x03 in slim schema; both work)
        //   0x12 = variable + nullable + system (was 0x02 in slim schema; both work)
        //   0x32 = Owner column — adds the auto-populated bit Access uses for SIDs.
        // Slim 9-col schema retains the historical 0x03/0x02 flags it shipped with.
        var columns = fullCatalogSchema
            ? BuildFullCatalogColumns(textColSize)
            : BuildSlimCatalogColumns(textColSize);

        int numCols = columns.Length;
        int numVarCols = 0;
        for (int i = 0; i < numCols; i++)
        {
            if (IsVariableType(columns[i].Type))
            {
                numVarCols++;
            }
        }

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

    /// <summary>
    /// Historical 9-column slim catalog. Retained for callers that hash whole-file
    /// output and depend on the legacy byte layout.
    /// </summary>
    private static (string Name, byte Type, int ColNum, int VarIdx, int FixedOff, int Size, byte Flags)[] BuildSlimCatalogColumns(int textColSize) =>
    [
        ("Id",          T_LONG,     0, 0, 0,  4,           0x03),
        ("ParentId",    T_LONG,     1, 0, 4,  4,           0x03),
        ("Name",        T_TEXT,     2, 0, 0,  textColSize, 0x02),
        ("Type",        T_INT,      3, 0, 8,  2,           0x03),
        ("DateCreate",  T_DATETIME, 4, 0, 10, 8,           0x03),
        ("DateUpdate",  T_DATETIME, 5, 0, 18, 8,           0x03),
        ("Flags",       T_LONG,     6, 0, 26, 4,           0x03),
        ("ForeignName", T_TEXT,     7, 1, 0,  textColSize, 0x02),
        ("Database",    T_TEXT,     8, 2, 0,  textColSize, 0x02),
    ];

    /// <summary>
    /// Real Microsoft Access 17-column catalog. Column order, types, sizes, fixed
    /// offsets, variable indices, and flag bytes were verified against Access-authored
    /// Jet3, Jet4, and ACE files — they are identical across all formats.
    /// </summary>
    private static (string Name, byte Type, int ColNum, int VarIdx, int FixedOff, int Size, byte Flags)[] BuildFullCatalogColumns(int textColSize) =>
    [
        ("Id",           T_LONG,     0,  0, 0,  4,           0x13),
        ("ParentId",     T_LONG,     1,  0, 4,  4,           0x13),
        ("Name",         T_TEXT,     2,  0, 0,  textColSize, 0x12),
        ("Type",         T_INT,      3,  0, 8,  2,           0x13),
        ("DateCreate",   T_DATETIME, 4,  0, 10, 8,           0x13),
        ("DateUpdate",   T_DATETIME, 5,  0, 18, 8,           0x13),
        ("Owner",        T_BINARY,   6,  1, 0,  textColSize, 0x32),
        ("Flags",        T_LONG,     7,  0, 26, 4,           0x13),
        ("Database",     T_MEMO,     8,  2, 0,  0,           0x12),
        ("Connect",      T_MEMO,     9,  3, 0,  0,           0x12),
        ("ForeignName",  T_TEXT,     10, 4, 0,  textColSize, 0x12),
        ("RmtInfoShort", T_BINARY,   11, 5, 0,  textColSize, 0x12),
        ("RmtInfoLong",  T_OLE,      12, 6, 0,  0,           0x12),
        ("Lv",           T_OLE,      13, 7, 0,  0,           0x12),
        ("LvProp",       T_OLE,      14, 8, 0,  0,           0x12),
        ("LvModule",     T_OLE,      15, 9, 0,  0,           0x12),
        ("LvExtra",      T_OLE,      16, 10, 0, 0,           0x12),
    ];

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

        byte[] bytes = _format != DatabaseFormat.Jet3Mdb ? EncodeJet4Text(value) : _ansiEncoding.GetBytes(value);
        if (maxSize > 0 && bytes.Length > maxSize)
        {
            // For Jet4 compressed text (FF FE prefix + 1 byte/char) any byte
            // boundary within the payload is a valid character boundary, so we
            // can truncate freely. For plain UCS-2 we must stay aligned to a
            // 2-byte char. Jet3 ANSI is 1 byte/char.
            bool isCompressedJet4 = _format != DatabaseFormat.Jet3Mdb && bytes.Length >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE;
            int allowed = _format != DatabaseFormat.Jet3Mdb && !isCompressedJet4 ? maxSize & ~1 : maxSize;
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

        byte[] data = _format != DatabaseFormat.Jet3Mdb ? EncodeJet4Text(value) : _ansiEncoding.GetBytes(value);
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

    /// <summary>
    /// W5: rebuild every index B-tree on <paramref name="tableName"/> from the
    /// current row data. Called at the end of each public mutation method that
    /// touches table rows so that indexes stay live instead of going stale until
    /// Microsoft Access rebuilds them on Compact &amp; Repair.
    /// <para>
    /// The implementation is a bulk rebuild: for each real index, every live row
    /// is encoded via <see cref="IndexKeyEncoder"/>, the entries are sorted by
    /// encoded key, and a fresh B-tree is built via <see cref="IndexBTreeBuilder"/>.
    /// The new root page is patched into the real-index <c>first_dp</c> field on
    /// the TDEF. Old index pages are orphaned (acceptable; Access compact-and-repair
    /// reclaims them — this library does not maintain a free-page bitmap).
    /// </para>
    /// <para>
    /// Indexes whose key column type is unsupported by the encoder (text, GUID,
    /// numeric, attachment, etc.) are skipped silently; their <c>first_dp</c> stays
    /// pointing at whatever leaf was emitted at <c>CreateTableAsync</c> time, which
    /// will go stale on first row write — same behaviour as W4 shipped with.
    /// </para>
    /// </summary>
    private async ValueTask MaintainIndexesAsync(long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
    {
        // Jet3 (.mdb Access 97) index emission is not supported by W1, so there
        // is nothing to maintain. CreateTableAsync rejects IndexDefinition entries
        // for Jet3 outright; any indexes encountered here would be foreign data
        // we cannot safely rebuild.
        if (_format == DatabaseFormat.Jet3Mdb)
        {
            return;
        }

        // Read the TDEF page bytes (single-page TDEFs produced by this writer).
        // Multi-page TDEF chains are not produced by CreateTableAsync today.
        byte[] tdefPageBytes = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] tdefBuffer;
        try
        {
            tdefBuffer = (byte[])tdefPageBytes.Clone();
        }
        finally
        {
            ReturnPage(tdefPageBytes);
        }

        int numCols = Ru16(tdefBuffer, _tdNumCols);
        int numIdx = Ri32(tdefBuffer, _tdNumCols + 2);
        int numRealIdx = Ri32(tdefBuffer, _tdNumRealIdx);
        if (numIdx <= 0 || numRealIdx <= 0 || numIdx > 1000 || numRealIdx > 1000)
        {
            return;
        }

        const int RealIdxPhysSz = 52; // Jet4/ACE only; gated above.
        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int namePos = colStart + (numCols * _colDescSz);
        for (int i = 0; i < numCols; i++)
        {
            if (ReadColumnName(tdefBuffer, ref namePos, out _) < 0)
            {
                return;
            }
        }

        int realIdxDescStart = namePos;

        // For each real-index slot referenced by a logical index, recover the key
        // column number (col_map slot 0; W1/W2 emits exactly one column for normal
        // indexes, but W8 may emit multi-column for PK — those are skipped below)
        // and the byte offset of that real-idx's first_dp field.
        var realIdxToKey = new Dictionary<int, (int ColumnNumber, int FirstDpOffset)>();
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * RealIdxPhysSz);
            if (phys + RealIdxPhysSz > tdefBuffer.Length)
            {
                break;
            }

            int keyColNum = Ru16(tdefBuffer, phys + 4);
            if (keyColNum == 0xFFFF)
            {
                continue;
            }

            // Multi-column index? slot 1's col_num is not 0xFFFF. Skip rebuild —
            // IndexKeyEncoder does not yet concatenate multi-column keys
            // (that lands with W7 / a multi-column W5). The leaf goes stale
            // until Microsoft Access rebuilds it on Compact & Repair.
            int slot1ColNum = Ru16(tdefBuffer, phys + 4 + 3);
            if (slot1ColNum != 0xFFFF)
            {
                continue;
            }

            // first_dp offset: unknown(4) + col_map(30) + used_pages(4).
            realIdxToKey[ri] = (keyColNum, phys + 4 + 30 + 4);
        }

        if (realIdxToKey.Count == 0)
        {
            return;
        }

        // Build column ordinal lookup so we can find each key column's value in the
        // snapshot row by ColNum (deleted-column gaps mean ColNum != snapshot index).
        var snapshotIndexByColNum = new Dictionary<int, int>(tableDef.Columns.Count);
        for (int c = 0; c < tableDef.Columns.Count; c++)
        {
            snapshotIndexByColNum[tableDef.Columns[c].ColNum] = c;
        }

        // Snapshot rows + locations in matching order (same page-walk semantics as
        // the existing UpdateRowsAsync/DeleteRowsAsync rely on).
        using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        List<RowLocation> locations = await GetLiveRowLocationsAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        int rowCount = Math.Min(snapshot.Rows.Count, locations.Count);

        bool tdefDirty = false;
        foreach (KeyValuePair<int, (int ColumnNumber, int FirstDpOffset)> kvp in realIdxToKey)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo? keyCol = tableDef.Columns.Find(c => c.ColNum == kvp.Value.ColumnNumber);
            if (keyCol == null)
            {
                continue;
            }

            if (!snapshotIndexByColNum.TryGetValue(kvp.Value.ColumnNumber, out int snapIdx))
            {
                continue;
            }

            var entries = new List<(byte[] Key, long Page, byte Row)>(rowCount);
            bool encoderRejected = false;
            for (int r = 0; r < rowCount; r++)
            {
                object cell = snapshot.Rows[r][snapIdx];
                object? value = cell is DBNull ? null : cell;
                byte[] encoded;
                try
                {
                    encoded = IndexKeyEncoder.EncodeEntry(keyCol.Type, value, ascending: true);
                }
                catch (NotSupportedException)
                {
                    // Unsupported key type for this column (text, GUID, numeric,
                    // attachment, etc.). Leave first_dp pointing at the existing
                    // (now-stale) leaf — same behaviour as before W5 shipped.
                    encoderRejected = true;
                    break;
                }

                entries.Add((encoded, locations[r].PageNumber, (byte)locations[r].RowIndex));
            }

            if (encoderRejected)
            {
                continue;
            }

            entries.Sort(static (a, b) => CompareKeyBytes(a.Key, b.Key));

            var leafEntries = new List<IndexLeafPageBuilder.LeafEntry>(entries.Count);
            foreach ((byte[] key, long page, byte row) in entries)
            {
                leafEntries.Add(new IndexLeafPageBuilder.LeafEntry(key, page, row));
            }

            long firstPageNumber = _stream.Length / _pgSz;
            IndexBTreeBuilder.BuildResult build = IndexBTreeBuilder.Build(_pgSz, tdefPage, leafEntries, firstPageNumber);
            foreach (byte[] page in build.Pages)
            {
                await AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
            }

            Wi32(tdefBuffer, kvp.Value.FirstDpOffset, checked((int)build.RootPageNumber));
            tdefDirty = true;
        }

        if (tdefDirty)
        {
            await WritePageAsync(tdefPage, tdefBuffer, cancellationToken).ConfigureAwait(false);
        }
    }

    private static int CompareKeyBytes(byte[] a, byte[] b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                return diff;
            }
        }

        return a.Length - b.Length;
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

    private sealed class ColumnConstraint
    {
        public string Name { get; set; } = string.Empty;

        public Type ClrType { get; set; } = typeof(object);

        public bool IsNullable { get; set; } = true;

        public object? DefaultValue { get; set; }

        public bool IsAutoIncrement { get; set; }

        public Func<object?, bool>? ValidationRule { get; set; }

        // Lazy-seeded next auto-increment value (max(existing) + 1). Null until first use.
        public long? NextAutoValue { get; set; }

        public bool HasAnyConstraint =>
            !IsNullable || DefaultValue != null || IsAutoIncrement || ValidationRule != null;
    }

    private readonly struct ResolvedIndex
    {
        public ResolvedIndex(string name, IReadOnlyList<int> columnNumbers, bool isPrimaryKey)
        {
            Name = name;
            ColumnNumbers = columnNumbers;
            IsPrimaryKey = isPrimaryKey;
        }

        public string Name { get; }

        public IReadOnlyList<int> ColumnNumbers { get; }

        public bool IsPrimaryKey { get; }
    }
}
