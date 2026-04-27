namespace JetDatabaseWriter;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
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
    /// Maximum payload size for a MEMO / OLE / Attachment value. The on-disk
    /// LVAL header dedicates a 24-bit field to the total length, so values
    /// strictly larger than 16,777,215 bytes cannot be addressed regardless
    /// of the chosen storage form (inline / single-page / chained).
    /// </summary>
    private const int MaxLvalPayloadBytes = (1 << 24) - 1;

    /// <summary>
    /// Maximum number of rows a single JET data page may hold. JET row IDs
    /// encode the per-page row index as a single byte, so a page can address
    /// at most 256 row slots; Jackcess caps at 255 and we follow suit so the
    /// <c>(byte)RowIndex</c> cast in the W5 index-rebuild path stays safe
    /// under <c>&lt;CheckForOverflowUnderflow&gt;true</c>.
    /// </summary>
    private const int MaxRowsPerDataPage = 255;

    /// <summary>
    /// Maximum recursion depth for cascade-delete / cascade-update chains (W10).
    /// Guards against pathological self-referential cycles. Real-world Access
    /// schemas almost never exceed depth 3.
    /// </summary>
    private const int CascadeMaxDepth = 64;

    private readonly ReadOnlyMemory<char> _password;
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
        ReadOnlyMemory<char> password,
        bool useLockFile,
        bool respectExistingLockFile,
        Stream? outerEncryptedStream = null,
        bool outerEncryptedLeaveOpen = false,
        bool isAgileEncryptedRewrap = false)
        : base(stream, header, path)
    {
        _password = password;
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
            AccessWriter writer = await OpenAsync(path, options, cancellationToken).ConfigureAwait(false);
            await ScaffoldSystemTablesAsync(writer, format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
            return writer;
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

        AccessWriter writer = await OpenAsync(stream, options, leaveOpen, cancellationToken).ConfigureAwait(false);
        await ScaffoldSystemTablesAsync(writer, format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
        return writer;
    }

    /// <inheritdoc/>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, CancellationToken cancellationToken = default)
        => CreateTableAsync(tableName, columns, indexes: [], cancellationToken);

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

        // W17b (2026-04-26): the Jet3 (.mdb Access 97) IndexDefinition
        // rejection has been lifted. The writer now emits the §3.1 39-byte
        // real-idx descriptor, the §3.2 20-byte logical-idx entry, and a
        // schema-only empty Jet3 leaf page (§4.2 bitmask at 0x16, first
        // entry at 0xF8). MaintainIndexesAsync still short-circuits for
        // Jet3, so the leaf goes stale on the first row mutation and Access
        // rebuilds it on the next Compact & Repair pass — the same
        // schema-only fallback model already used for unsupported Jet4 key
        // types (OLE / attachment / complex).

        if (await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        // Phase C3: complex columns (Attachment / MultiValue) declared by the user have
        // ComplexId = 0; allocate fresh per-database ComplexIDs, then emit the hidden
        // flat child table + MSysComplexColumns row per column AFTER the parent TDEF
        // is on disk. The round-trip preservation path on RewriteTableAsync supplies a
        // non-zero ComplexId from the original TDEF and is left untouched here.
        IReadOnlyList<ComplexColumnAllocation>? complexAllocs =
            await PrepareComplexColumnAllocationsAsync(columns, cancellationToken).ConfigureAwait(false);
        if (complexAllocs is { Count: > 0 })
        {
            // Rewrite the column list with the allocated ComplexIds embedded so the parent
            // TDEF's misc slot points at the soon-to-be-emitted MSysComplexColumns rows.
            var rewritten = new List<ColumnDefinition>(columns);
            for (int i = 0; i < complexAllocs.Count; i++)
            {
                ComplexColumnAllocation a = complexAllocs[i];
                rewritten[a.ColumnIndex] = rewritten[a.ColumnIndex] with { ComplexId = a.ComplexId };
            }

            columns = rewritten;
        }

        long tdefPageNumber = await CreateTableInternalAsync(tableName, columns, indexes, catalogFlags: 0, cancellationToken).ConfigureAwait(false);

        // Phase C3: emit the hidden flat child table + MSysComplexColumns row for every
        // user-declared complex column. Done after the parent table is on disk so the
        // catalog cache reflects the parent before flat-table inserts.
        if (complexAllocs is { Count: > 0 })
        {
            await EmitComplexColumnArtifactsAsync(tableName, columns, complexAllocs, cancellationToken).ConfigureAwait(false);
        }

        _ = tdefPageNumber;
    }

    /// <summary>
    /// Internal table-creation helper that drives the same TDEF + leaf + catalog-row
    /// pipeline as <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, CancellationToken)"/>
    /// but accepts an explicit <paramref name="catalogFlags"/> value so it can also
    /// emit hidden system tables (e.g. the C3 complex-column flat tables that need
    /// <c>MSysObjects.Flags = 0x800A0000</c>). Returns the new TDEF page number.
    /// </summary>
    private async ValueTask<long> CreateTableInternalAsync(
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<IndexDefinition> indexes,
        uint catalogFlags,
        CancellationToken cancellationToken)
    {
        TableDef tableDef = BuildTableDefinition(columns, _format);
        List<ResolvedIndex> resolvedIndexes = ResolveIndexes(indexes, tableDef);
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
            bool jet4 = _format != DatabaseFormat.Jet3Mdb;
            for (int i = 0; i < resolvedIndexes.Count; i++)
            {
                byte[] leafPage = jet4
                    ? IndexLeafPageBuilder.BuildJet4LeafPage(_pgSz, tdefPageNumber, [])
                    : IndexLeafPageBuilder.BuildJet3EmptyLeafPage(_pgSz, tdefPageNumber);
                long leafPageNumber = await AppendPageAsync(leafPage, cancellationToken).ConfigureAwait(false);
                Wi32(tdefPage, firstDpOffsets[i], checked((int)leafPageNumber));
            }

            // Re-flush the TDEF with the patched first_dp values.
            await WritePageAsync(tdefPageNumber, tdefPage, cancellationToken).ConfigureAwait(false);
        }

        byte[]? lvProp = JetExpressionConverter.BuildLvPropBlob(columns, _format);
        await InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, catalogFlags, cancellationToken).ConfigureAwait(false);
        RegisterConstraints(tableName, columns);
        InvalidateCatalogCache();
        return tdefPageNumber;
    }

    /// <inheritdoc/>
    public async ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        await DropTableCoreAsync(tableName, dropComplexChildren: true, cancellationToken).ConfigureAwait(false);
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

                    // Phase C9: forward complex-column flags so the rebuilt
                    // TDEF re-emits T_ATTACHMENT / T_COMPLEX with the original
                    // ComplexId in the misc slot. RewriteTableAsync uses the
                    // preserved ComplexId to update MSysComplexColumns.ColumnName.
                    IsAttachment = src.IsAttachment,
                    IsMultiValue = src.IsMultiValue,
                    MultiValueElementType = src.MultiValueElementType,
                    ComplexId = src.ComplexId,
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
                    // Forward Normal (W11: 1..N column) and PrimaryKey
                    // (1..N column) indexes; FK forwarding is W9 territory.
                    if (idx.Kind != IndexKind.Normal && idx.Kind != IndexKind.PrimaryKey)
                    {
                        continue;
                    }

                    var remappedCols = new List<string>(idx.Columns.Count);
                    var descendingCols = new List<string>();
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
                        if (!ic.IsAscending)
                        {
                            descendingCols.Add(remapped);
                        }
                    }

                    if (!allSurvive)
                    {
                        continue;
                    }

                    if (idx.Kind == IndexKind.PrimaryKey)
                    {
                        result.Add(new IndexDefinition(idx.Name, remappedCols)
                        {
                            IsPrimaryKey = true,
                            DescendingColumns = descendingCols,
                        });
                    }
                    else
                    {
                        result.Add(new IndexDefinition(idx.Name, remappedCols)
                        {
                            IsUnique = idx.IsUnique,
                            DescendingColumns = descendingCols,
                        });
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
        IReadOnlyList<FkRelationship> rels = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtx = rels.Count > 0 ? new FkContext(rels) : null;

        await InsertRowCoreAsync(tableName, entry.TDefPage, tableDef, values, fkCtx, cancellationToken).ConfigureAwait(false);
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

        // Track every row written so far + every auto-counter advance so we can
        // roll the entire batch back if the bulk MaintainIndexesAsync at the end
        // rejects it (e.g. duplicate key inside the batch).
        var batchLocations = new List<RowLocation>();
        var batchHintRows = new List<(RowLocation Loc, object[] Row)>();
        List<(ColumnConstraint Constraint, long? PreviousValue)>? batchAutoCheckpoints = null;
        int inserted = 0;

        // Materialize the batch so the W15 pre-write unique-index check sees
        // every pending row at once (it must catch intra-batch duplicates).
        // ApplyConstraintsAsync is run up front for the same reason —
        // auto-increment values must be assigned before the unique check.
        var pendingRows = new List<object[]>();
        try
        {
            foreach (object[] row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Guard.NotNull(row, nameof(rows));
                List<(ColumnConstraint Constraint, long? PreviousValue)>? rowCp =
                    await ApplyConstraintsAsync(tableName, tableDef, row, cancellationToken).ConfigureAwait(false);
                if (rowCp != null)
                {
                    (batchAutoCheckpoints ??= new List<(ColumnConstraint, long?)>()).AddRange(rowCp);
                }

                pendingRows.Add(row);
            }

            // ── W15 — pre-write unique-index enforcement ────────────────
            await CheckUniqueIndexesPreInsertAsync(entry.TDefPage, tableDef, tableName, pendingRows, cancellationToken).ConfigureAwait(false);

            foreach (object[] row in pendingRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (fkCtx != null)
                {
                    await EnforceFkOnInsertAsync(tableName, tableDef, row, fkCtx, cancellationToken).ConfigureAwait(false);
                }

                RowLocation loc = await InsertRowDataLocAsync(entry.TDefPage, tableDef, row, cancellationToken: cancellationToken).ConfigureAwait(false);
                batchLocations.Add(loc);
                batchHintRows.Add((loc, row));
                if (fkCtx != null)
                {
                    AugmentParentSetsAfterInsert(tableName, tableDef, row, fkCtx);
                }

                inserted++;
            }

            if (inserted > 0)
            {
                bool incremental = await TryMaintainIndexesIncrementalAsync(
                    entry.TDefPage,
                    tableDef,
                    batchHintRows,
                    deletedRows: null,
                    cancellationToken).ConfigureAwait(false);
                if (!incremental)
                {
                    await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch
        {
            await RollbackInsertedRowsAsync(entry.TDefPage, batchLocations, cancellationToken).ConfigureAwait(false);
            RestoreAutoCounters(batchAutoCheckpoints);
            throw;
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

        IReadOnlyList<FkRelationship> relsT = await GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtxT = relsT.Count > 0 ? new FkContext(relsT) : null;

        await InsertRowCoreAsync(tableName, entry.TDefPage, tableDef, mappedRow, fkCtxT, cancellationToken).ConfigureAwait(false);
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

        var batchLocations = new List<RowLocation>();
        var batchHintRows = new List<(RowLocation Loc, object[] Row)>();
        List<(ColumnConstraint Constraint, long? PreviousValue)>? batchAutoCheckpoints = null;
        int inserted = 0;

        // Materialize the batch (see InsertRowsAsync(object[]) above) so the
        // W15 pre-write unique check sees every pending row at once.
        var pendingRows = new List<object[]>();
        try
        {
            foreach (T item in items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Guard.NotNull(item, nameof(items));
                object[] mappedRow = RowMapper<T>.ToRow(item, index);
                List<(ColumnConstraint Constraint, long? PreviousValue)>? rowCp =
                    await ApplyConstraintsAsync(tableName, tableDef, mappedRow, cancellationToken).ConfigureAwait(false);
                if (rowCp != null)
                {
                    (batchAutoCheckpoints ??= new List<(ColumnConstraint, long?)>()).AddRange(rowCp);
                }

                pendingRows.Add(mappedRow);
            }

            // ── W15 — pre-write unique-index enforcement ────────────────
            await CheckUniqueIndexesPreInsertAsync(entry.TDefPage, tableDef, tableName, pendingRows, cancellationToken).ConfigureAwait(false);

            foreach (object[] mappedRow in pendingRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (fkCtx != null)
                {
                    await EnforceFkOnInsertAsync(tableName, tableDef, mappedRow, fkCtx, cancellationToken).ConfigureAwait(false);
                }

                RowLocation loc = await InsertRowDataLocAsync(entry.TDefPage, tableDef, mappedRow, cancellationToken: cancellationToken).ConfigureAwait(false);
                batchLocations.Add(loc);
                batchHintRows.Add((loc, mappedRow));
                if (fkCtx != null)
                {
                    AugmentParentSetsAfterInsert(tableName, tableDef, mappedRow, fkCtx);
                }

                inserted++;
            }

            if (inserted > 0)
            {
                bool incremental = await TryMaintainIndexesIncrementalAsync(
                    entry.TDefPage,
                    tableDef,
                    batchHintRows,
                    deletedRows: null,
                    cancellationToken).ConfigureAwait(false);
                if (!incremental)
                {
                    await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch
        {
            await RollbackInsertedRowsAsync(entry.TDefPage, batchLocations, cancellationToken).ConfigureAwait(false);
            RestoreAutoCounters(batchAutoCheckpoints);
            throw;
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
        int predicateIndex = tableDef.FindColumnIndex(predicateColumn);
        if (predicateIndex < 0)
        {
            throw new ArgumentException($"Column '{predicateColumn}' was not found in table '{tableName}'.", nameof(predicateColumn));
        }

        var updateIndexes = new Dictionary<int, object>();
        foreach (KeyValuePair<string, object> kvp in updatedValues)
        {
            int columnIndex = tableDef.FindColumnIndex(kvp.Key);
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
                    pkIdx[i] = tableDef.FindColumnIndex(rel.PrimaryColumns[i]);
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

        // ── W15 — pre-write unique-index enforcement ────────────────────
        // After all FK checks succeed, validate that the post-update key set
        // contains no duplicates for any unique index. The check sees the
        // snapshot with pendingNewRows substituted at their original indices.
        if (pendingNewRows.Count > 0)
        {
            await CheckUniqueIndexesPreUpdateAsync(entry.TDefPage, tableDef, tableName, snapshot, pendingNewRows, cancellationToken).ConfigureAwait(false);
        }

        int updated = 0;
        var updateInsertedHints = new List<(RowLocation Loc, object[] Row)>(pendingNewRows.Count);
        var updateDeletedHints = new List<(RowLocation Loc, object[] Row)>(pendingNewRows.Count);
        foreach ((int i, object[] rowValues) in pendingNewRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object[] oldRow = snapshot.Rows[i].ItemArray!;
            await MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, cancellationToken).ConfigureAwait(false);
            updateDeletedHints.Add((locations[i], oldRow));
            RowLocation newLoc = await InsertRowDataLocAsync(entry.TDefPage, tableDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            updateInsertedHints.Add((newLoc, rowValues));
            updated++;
        }

        if (updated > 0)
        {
            bool incremental = await TryMaintainIndexesIncrementalAsync(
                entry.TDefPage,
                tableDef,
                updateInsertedHints,
                updateDeletedHints,
                cancellationToken).ConfigureAwait(false);
            if (!incremental)
            {
                await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
            }
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
        int predicateIndex = tableDef.FindColumnIndex(predicateColumn);
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
                    pkIdx[i] = tableDef.FindColumnIndex(rel.PrimaryColumns[i]);
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

        // ── C5 — cascade flat-child rows for any complex columns on the
        // parent BEFORE we mark the parent rows deleted (we need to read
        // the parent's ConceptualTableID slots while the rows are still
        // live).
        if (matchingIndices.Count > 0)
        {
            var parentLocs = new List<RowLocation>(matchingIndices.Count);
            foreach (int i in matchingIndices)
            {
                parentLocs.Add(locations[i]);
            }

            await CascadeDeleteComplexChildrenAsync(tableDef, parentLocs, cancellationToken).ConfigureAwait(false);
        }

        int deleted = 0;
        var deleteHints = new List<(RowLocation Loc, object[] Row)>(matchingIndices.Count);
        foreach (int i in matchingIndices)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object[] oldRow = snapshot.Rows[i].ItemArray!;
            await MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, cancellationToken).ConfigureAwait(false);
            deleteHints.Add((locations[i], oldRow));
            deleted++;
        }

        if (deleted > 0)
        {
            await AdjustTDefRowCountAsync(entry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
            bool incremental = await TryMaintainIndexesIncrementalAsync(
                entry.TDefPage,
                tableDef,
                insertedRows: null,
                deleteHints,
                cancellationToken).ConfigureAwait(false);
            if (!incremental)
            {
                await MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
            }
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
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", 0);
        msys.SetValueByName(values, "ParentId", 0);
        msys.SetValueByName(values, "Name", linkedTableName);
        msys.SetValueByName(values, "Type", (short)OBJ_LINKED_TABLE);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", 0);
        msys.SetValueByName(values, "ForeignName", foreignTableName);
        msys.SetValueByName(values, "Database", sourceDatabasePath);

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
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", 0);
        msys.SetValueByName(values, "ParentId", 0);
        msys.SetValueByName(values, "Name", linkedTableName);
        msys.SetValueByName(values, "Type", (short)OBJ_LINKED_ODBC);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", 0);
        msys.SetValueByName(values, "ForeignName", foreignTableName);
        msys.SetValueByName(values, "Connect", normalizedConnect);

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
            if (primaryDef.FindColumnIndex(relationship.PrimaryColumns[i]) < 0)
            {
                throw new ArgumentException(
                    $"Column '{relationship.PrimaryColumns[i]}' was not found on table '{relationship.PrimaryTable}'.",
                    nameof(relationship));
            }

            if (foreignDef.FindColumnIndex(relationship.ForeignColumns[i]) < 0)
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
            object[] values = msysRelDef.CreateNullValueRow();

            msysRelDef.SetValueByName(values, "ccolumn", ccolumn);
            msysRelDef.SetValueByName(values, "grbit", grbitInt);
            msysRelDef.SetValueByName(values, "icolumn", i);
            msysRelDef.SetValueByName(values, "szColumn", relationship.ForeignColumns[i]);
            msysRelDef.SetValueByName(values, "szObject", relationship.ForeignTable);
            msysRelDef.SetValueByName(values, "szReferencedColumn", relationship.PrimaryColumns[i]);
            msysRelDef.SetValueByName(values, "szReferencedObject", relationship.PrimaryTable);
            msysRelDef.SetValueByName(values, "szRelationship", relationship.Name);

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

            // Populate the freshly-allocated FK index leaves so the seek-based
            // RI enforcement path (EnforceFkOnInsertAsync) sees existing parent
            // rows. EmitFkPerTdefEntriesAsync emits empty leaves; without this
            // rebuild a child INSERT immediately after CreateRelationshipAsync
            // would fail to match a parent row that was inserted before the
            // relationship existed. Re-read TDEFs because the emit mutates
            // both sides' TDEF pages in place.
            TableDef primaryDefAfter = await ReadRequiredTableDefAsync(primaryEntry.TDefPage, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
            await MaintainIndexesAsync(primaryEntry.TDefPage, primaryDefAfter, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
            if (foreignEntry.TDefPage != primaryEntry.TDefPage)
            {
                TableDef foreignDefAfter = await ReadRequiredTableDefAsync(foreignEntry.TDefPage, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
                await MaintainIndexesAsync(foreignEntry.TDefPage, foreignDefAfter, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
            }
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
            int pkIdx = primaryDef.FindColumnIndex(relationship.PrimaryColumns[i]);
            int fkIdx = foreignDef.FindColumnIndex(relationship.ForeignColumns[i]);
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
            byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(_pgSz, primaryTdefPage, []);
            long lp = await AppendPageAsync(leaf, cancellationToken).ConfigureAwait(false);
            pkPlan = pkPlan.WithLeafPage(lp);
        }

        if (fkPlan.AllocatesNewRealIdx)
        {
            byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(_pgSz, foreignTdefPage, []);
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
    // W14 — Drop / Rename relationship
    // ════════════════════════════════════════════════════════════════
    //
    // Reverses CreateRelationshipAsync (W9a + W9b):
    //   • DropRelationshipAsync removes every MSysRelationships row whose
    //     szRelationship matches and (Jet4/ACE) removes the matching FK
    //     logical-idx entry from each side's TDEF. The orphaned real-idx
    //     slot is left in place — Microsoft Access reclaims it on Compact
    //     & Repair, and ListIndexesAsync iterates by num_idx so it stops
    //     surfacing the FK immediately.
    //   • RenameRelationshipAsync rewrites the szRelationship column on
    //     every matching MSysRelationships row (read all 8 columns, mark
    //     deleted, re-insert with the new name and updateTDefRowCount=false).
    //     The TDEF logical-idx name cookies are left at the old name —
    //     Access regenerates them from the catalog row on the next Compact
    //     & Repair pass.

    /// <summary>
    /// Snapshot of one MSysRelationships row. <c>RowValues</c> mirrors the
    /// MSysRelationships column order so it can be passed directly to
    /// <see cref="InsertRowDataAsync"/> on the re-insert path used by
    /// <see cref="RenameRelationshipAsync"/>.
    /// </summary>
    private sealed record RelationshipRowSnapshot(
        RowLocation Location,
        string SzRelationship,
        string SzObject,
        string SzReferencedObject,
        string SzColumn,
        string SzReferencedColumn,
        int IColumn,
        int CColumn,
        int Grbit,
        object[] RowValues);

    /// <inheritdoc />
    public async ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(relationshipName, nameof(relationshipName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        long msysRelTdefPage = await FindSystemTableTdefPageAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to drop.");
        }

        TableDef msysRelDef = await ReadRequiredTableDefAsync(msysRelTdefPage, "MSysRelationships", cancellationToken).ConfigureAwait(false);
        List<RelationshipRowSnapshot> matches = await CollectRelationshipRowsAsync(
            msysRelTdefPage,
            msysRelDef,
            r => string.Equals(r, relationshipName, StringComparison.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);

        if (matches.Count == 0)
        {
            throw new InvalidOperationException($"No relationship named '{relationshipName}' was found.");
        }

        // Group by (PK table, FK table) pair so we can rebuild each side's
        // FK column list once. CreateRelationshipAsync emits N rows (one per
        // FK column pair) all sharing szObject / szReferencedObject; group
        // anyway so a malformed catalog with mixed pairs (which we did not
        // emit, but might exist) is handled gracefully.
        var byTablePair = new Dictionary<(string Pk, string Fk), List<RelationshipRowSnapshot>>(
            new TablePairComparer());
        foreach (RelationshipRowSnapshot row in matches)
        {
            (string Pk, string Fk) key = (row.SzReferencedObject, row.SzObject);
            if (!byTablePair.TryGetValue(key, out List<RelationshipRowSnapshot>? group))
            {
                group = new List<RelationshipRowSnapshot>();
                byTablePair[key] = group;
            }

            group.Add(row);
        }

        // Jet4/ACE only — Jet3 never received the per-TDEF FK logical-idx entries.
        if (_format != DatabaseFormat.Jet3Mdb)
        {
            foreach (KeyValuePair<(string Pk, string Fk), List<RelationshipRowSnapshot>> pair in byTablePair)
            {
                cancellationToken.ThrowIfCancellationRequested();

                CatalogEntry? pkEntry = await GetCatalogEntryAsync(pair.Key.Pk, cancellationToken).ConfigureAwait(false);
                CatalogEntry? fkEntry = await GetCatalogEntryAsync(pair.Key.Fk, cancellationToken).ConfigureAwait(false);
                if (pkEntry == null || fkEntry == null)
                {
                    // Catalog row references a missing table — skip TDEF cleanup
                    // (the catalog row is still removed below).
                    continue;
                }

                TableDef pkDef = await ReadRequiredTableDefAsync(pkEntry.TDefPage, pair.Key.Pk, cancellationToken).ConfigureAwait(false);
                TableDef fkDef = await ReadRequiredTableDefAsync(fkEntry.TDefPage, pair.Key.Fk, cancellationToken).ConfigureAwait(false);

                // Reconstruct the FK column list in icolumn order, then resolve
                // to col_num for col_map matching.
                var ordered = new List<RelationshipRowSnapshot>(pair.Value);
                ordered.Sort((a, b) => a.IColumn.CompareTo(b.IColumn));
                var pkColNames = new string[ordered.Count];
                var fkColNames = new string[ordered.Count];
                for (int i = 0; i < ordered.Count; i++)
                {
                    pkColNames[i] = ordered[i].SzReferencedColumn;
                    fkColNames[i] = ordered[i].SzColumn;
                }

                int[] pkColNums = pkDef.ResolveColNumsOrEmpty(pkColNames);
                int[] fkColNums = fkDef.ResolveColNumsOrEmpty(fkColNames);
                if (pkColNums.Length == 0 || fkColNums.Length == 0)
                {
                    continue;
                }

                // Remove the matching FK logical-idx entry from each side.
                // Self-referential relationships (PK and FK on same TDEF) need
                // both removals to target distinct entries — pass the column
                // list to disambiguate.
                _ = await TryRemoveFkLogicalIdxEntryAsync(pkEntry.TDefPage, pkColNums, fkEntry.TDefPage, cancellationToken).ConfigureAwait(false);
                _ = await TryRemoveFkLogicalIdxEntryAsync(fkEntry.TDefPage, fkColNums, pkEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            }
        }

        // Mark catalog rows deleted and adjust the row count.
        foreach (RelationshipRowSnapshot row in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await MarkRowDeletedAsync(row.Location.PageNumber, row.Location.RowIndex, cancellationToken).ConfigureAwait(false);
        }

        await AdjustTDefRowCountAsync(msysRelTdefPage, -matches.Count, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(oldName, nameof(oldName));
        Guard.NotNullOrEmpty(newName, nameof(newName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (string.Equals(oldName, newName, StringComparison.OrdinalIgnoreCase))
        {
            return; // No-op; matches Microsoft Access' designer behaviour.
        }

        long msysRelTdefPage = await FindSystemTableTdefPageAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to rename.");
        }

        TableDef msysRelDef = await ReadRequiredTableDefAsync(msysRelTdefPage, "MSysRelationships", cancellationToken).ConfigureAwait(false);

        // Reject collision with an existing name (case-insensitive).
        HashSet<string> existing = await ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existing.Contains(newName))
        {
            throw new InvalidOperationException($"A relationship named '{newName}' already exists.");
        }

        List<RelationshipRowSnapshot> matches = await CollectRelationshipRowsAsync(
            msysRelTdefPage,
            msysRelDef,
            r => string.Equals(r, oldName, StringComparison.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);

        if (matches.Count == 0)
        {
            throw new InvalidOperationException($"No relationship named '{oldName}' was found.");
        }

        int szRelIdx = msysRelDef.FindColumnIndex("szRelationship");
        if (szRelIdx < 0)
        {
            throw new InvalidOperationException("MSysRelationships does not expose a 'szRelationship' column.");
        }

        foreach (RelationshipRowSnapshot row in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object[] rowValues = (object[])row.RowValues.Clone();
            rowValues[szRelIdx] = newName;

            await MarkRowDeletedAsync(row.Location.PageNumber, row.Location.RowIndex, cancellationToken).ConfigureAwait(false);
            await InsertRowDataAsync(msysRelTdefPage, msysRelDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Walks every live row in <c>MSysRelationships</c>, materialising each
    /// row whose <c>szRelationship</c> satisfies <paramref name="namePredicate"/>
    /// into a <see cref="RelationshipRowSnapshot"/> (location + all 8 column
    /// values in MSysRelationships column order).
    /// </summary>
    private async ValueTask<List<RelationshipRowSnapshot>> CollectRelationshipRowsAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        Func<string, bool> namePredicate,
        CancellationToken cancellationToken)
    {
        var results = new List<RelationshipRowSnapshot>();
        ColumnInfo? nameCol = msysRelDef.FindColumn("szRelationship");
        ColumnInfo? objCol = msysRelDef.FindColumn("szObject");
        ColumnInfo? refObjCol = msysRelDef.FindColumn("szReferencedObject");
        ColumnInfo? colCol = msysRelDef.FindColumn("szColumn");
        ColumnInfo? refColCol = msysRelDef.FindColumn("szReferencedColumn");
        ColumnInfo? icolCol = msysRelDef.FindColumn("icolumn");
        ColumnInfo? ccolCol = msysRelDef.FindColumn("ccolumn");
        ColumnInfo? grbitCol = msysRelDef.FindColumn("grbit");
        if (nameCol == null || objCol == null || refObjCol == null || colCol == null
            || refColCol == null || icolCol == null || ccolCol == null || grbitCol == null)
        {
            return results;
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
                    if (string.IsNullOrEmpty(name) || !namePredicate(name))
                    {
                        continue;
                    }

                    var values = new object[msysRelDef.Columns.Count];
                    for (int c = 0; c < values.Length; c++)
                    {
                        ColumnInfo col = msysRelDef.Columns[c];
                        string raw = ReadColumnValue(page, row.RowStart, row.RowSize, col);
                        values[c] = string.IsNullOrEmpty(raw)
                            ? DBNull.Value
                            : col.Type switch
                            {
                                T_LONG => ParseInt32(raw),
                                T_INT => (short)ParseInt32(raw),
                                T_BYTE => (byte)ParseInt32(raw),
                                _ => raw,
                            };
                    }

                    results.Add(new RelationshipRowSnapshot(
                        row,
                        name,
                        ReadColumnValue(page, row.RowStart, row.RowSize, objCol),
                        ReadColumnValue(page, row.RowStart, row.RowSize, refObjCol),
                        ReadColumnValue(page, row.RowStart, row.RowSize, colCol),
                        ReadColumnValue(page, row.RowStart, row.RowSize, refColCol),
                        ParseInt32(ReadColumnValue(page, row.RowStart, row.RowSize, icolCol)),
                        ParseInt32(ReadColumnValue(page, row.RowStart, row.RowSize, ccolCol)),
                        ParseInt32(ReadColumnValue(page, row.RowStart, row.RowSize, grbitCol)),
                        values));
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return results;
    }

    /// <summary>
    /// Locates and removes the FK logical-idx entry on <paramref name="tdefPage"/>
    /// whose backing real-idx col_map exactly covers <paramref name="columnNumbers"/>
    /// (in declaration order) AND whose <c>rel_tbl_page</c> equals
    /// <paramref name="otherTdefPage"/>. Returns <see langword="true"/> when an
    /// entry was removed; <see langword="false"/> when no matching entry exists
    /// (already removed, or never created — Jet3 path, or out-of-band catalog).
    /// </summary>
    private async ValueTask<bool> TryRemoveFkLogicalIdxEntryAsync(
        long tdefPage,
        int[] columnNumbers,
        long otherTdefPage,
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
            // Multi-page TDEF — out of scope for in-place mutation, same
            // contract as W9b emission.
            return false;
        }

        int numCols = Ru16(td, _tdNumCols);
        int numIdx = Ri32(td, _tdNumCols + 2);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numCols < 0 || numCols > 4096
            || numIdx <= 0 || numIdx > 1000
            || numRealIdx <= 0 || numRealIdx > 1000)
        {
            return false;
        }

        const int RealIdxPhysSz = 52;
        const int LogIdxEntrySz = 28;

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return false;
        }

        int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);
        int logIdxNamesStart = logIdxStart + (numIdx * LogIdxEntrySz);
        int logIdxNamesLen = MeasureLogicalIdxNamesLength(td, logIdxNamesStart, numIdx);
        if (logIdxNamesLen < 0)
        {
            return false;
        }

        // Locate the matching logical-idx entry. Iterate in order so we can
        // also locate its name by walking the names list to the same index.
        int matchEntryIdx = -1;
        for (int li = 0; li < numIdx; li++)
        {
            int e = logIdxStart + (li * LogIdxEntrySz);
            byte indexType = td[e + 23];
            if (indexType != 0x02)
            {
                continue;
            }

            int relTblPage = Ri32(td, e + 17);
            if (relTblPage != otherTdefPage)
            {
                continue;
            }

            int realIdxNum = Ri32(td, e + 8);
            if (realIdxNum < 0 || realIdxNum >= numRealIdx)
            {
                continue;
            }

            int phys = realIdxDescStart + (realIdxNum * RealIdxPhysSz);
            if (!RealIdxColMapMatches(td, phys, columnNumbers))
            {
                continue;
            }

            matchEntryIdx = li;
            break;
        }

        if (matchEntryIdx < 0)
        {
            return false;
        }

        // Find the byte offset and length of the corresponding name (variable
        // length). Walk the names list to position matchEntryIdx.
        int namePos = logIdxNamesStart;
        int removedNameStart = -1;
        int removedNameLen = 0;
        for (int i = 0; i < numIdx; i++)
        {
            int before = namePos;
            if (ReadColumnName(td, ref namePos, out _) < 0)
            {
                return false;
            }

            if (i == matchEntryIdx)
            {
                removedNameStart = before;
                removedNameLen = namePos - before;
                break;
            }
        }

        if (removedNameStart < 0)
        {
            return false;
        }

        // Compute trailing block bounds (mirror of W9b EmitFkLogicalIdxAsync).
        int trailingStart = logIdxNamesStart + logIdxNamesLen;
        int storedTdefLen = Ri32(td, 8);
        int currentEnd = storedTdefLen + 8;
        if (currentEnd < trailingStart)
        {
            currentEnd = trailingStart;
        }

        int trailingLen = currentEnd - trailingStart;
        if (trailingLen < 0 || trailingStart + trailingLen > td.Length)
        {
            return false;
        }

        // Mutate `td` in place via two left-shifts (Buffer.BlockCopy supports
        // overlapping regions). Step 1 collapses the 28-byte logical-idx
        // entry; step 2 collapses the variable-length name. The trailing
        // variable-length-column block rides along with the second shift.
        int removedEntryStart = logIdxStart + (matchEntryIdx * LogIdxEntrySz);
        int afterEntry = removedEntryStart + LogIdxEntrySz;

        // Step 1 — drop the 28-byte logical-idx entry.
        Buffer.BlockCopy(td, afterEntry, td, removedEntryStart, currentEnd - afterEntry);
        int shiftedNameStart = removedNameStart - LogIdxEntrySz;
        int afterName = shiftedNameStart + removedNameLen;
        int endAfterStep1 = currentEnd - LogIdxEntrySz;

        // Step 2 — drop the name record.
        Buffer.BlockCopy(td, afterName, td, shiftedNameStart, endAfterStep1 - afterName);
        int finalEnd = endAfterStep1 - removedNameLen;

        // Zero the freed tail so the on-disk page matches the prior
        // fresh-buffer behavior (bytes past the new end are padding).
        Array.Clear(td, finalEnd, currentEnd - finalEnd);

        // Update header counts.
        Wi32(td, _tdNumCols + 2, numIdx - 1);
        Wi32(td, 8, finalEnd - 8);

        await WritePageAsync(tdefPage, td, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static bool RealIdxColMapMatches(byte[] td, int phys, int[] columnNumbers)
    {
        if (phys + 52 > td.Length)
        {
            return false;
        }

        for (int slot = 0; slot < 10; slot++)
        {
            int so = phys + 4 + (slot * 3);
            int cn = Ru16(td, so);
            if (slot < columnNumbers.Length)
            {
                if (cn != columnNumbers[slot])
                {
                    return false;
                }
            }
            else if (cn != 0xFFFF)
            {
                return false;
            }
        }

        return true;
    }

    private sealed class TablePairComparer : IEqualityComparer<(string Pk, string Fk)>
    {
        public bool Equals((string Pk, string Fk) x, (string Pk, string Fk) y) =>
            string.Equals(x.Pk, y.Pk, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Fk, y.Fk, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string Pk, string Fk) obj) =>
            HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Pk),
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Fk));
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

        /// <summary>
        /// Gets the per-relationship cached resolution of the parent table's
        /// PK / FK index used by <see cref="EnforceFkOnInsertAsync"/> to
        /// perform O(log N) seeks instead of a full parent snapshot scan. A
        /// value of <see langword="null"/> means "resolution attempted and
        /// the index was not usable" (Jet3 format, no covering real-idx,
        /// unsupported key type) — the enforcement loop falls back to the
        /// legacy HashSet-of-keys path in that case.
        /// </summary>
        public Dictionary<string, ParentSeekIndex?> SeekIndexes { get; }
            = new(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Resolved parent-side seek index for a single relationship. The seeker
    /// uses <see cref="RootPage"/> as the entry point and encodes the FK-side
    /// row values using <see cref="KeyColumns"/> (one entry per relationship
    /// PK column, in declaration order) plus the foreign-table column index
    /// supplying each value.
    /// </summary>
    private sealed record ParentSeekIndex(
        long RootPage,
        IReadOnlyList<ParentSeekKeyColumn> KeyColumns);

    /// <summary>One column of a parent-seek composite key.</summary>
    private readonly record struct ParentSeekKeyColumn(
        byte ColumnType,
        bool Ascending,
        int ForeignColumnIndex);

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
            return [];
        }

        DataTable t = await ReadTableSnapshotAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        try
        {
            if (!t.Columns.Contains("szRelationship"))
            {
                return [];
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
                fkIdx[i] = foreignDef.FindColumnIndex(rel.ForeignColumns[i]);
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

            // Try the O(log N) parent-seek path first: locate (and cache) the
            // parent's PK / FK index that backs this relationship and seek
            // the encoded composite key in its B-tree. Falls back to the
            // legacy HashSet path when the index is missing (Jet3, schema
            // mismatch) or its key column types are not supported by the
            // sort-key encoder.
            ParentSeekIndex? seekIdx = await ResolveParentSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (seekIdx != null)
            {
                if (!ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? pendingSet))
                {
                    pendingSet = new HashSet<string>(StringComparer.Ordinal);
                    ctx.ParentKeySets[rel.Name] = pendingSet;
                }

                if (pendingSet.Contains(key))
                {
                    continue;
                }

                byte[]? encodedKey = TryEncodeSeekKey(seekIdx, values);
                if (encodedKey != null)
                {
                    bool found = await IndexBTreeSeeker.ContainsKeyAsync(
                        (page, ct) => ReadPageOwnedAsync(page, ct),
                        _pgSz,
                        seekIdx.RootPage,
                        encodedKey,
                        cancellationToken).ConfigureAwait(false);

                    if (found)
                    {
                        continue;
                    }

                    throw new InvalidOperationException(
                        $"INSERT into '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                        $"no matching row in '{rel.PrimaryTable}' for the supplied {string.Join(", ", rel.ForeignColumns)} value(s).");
                }

                // Encoder rejected (e.g. text outside General Legacy, GUID
                // mismatch, …) — fall through to the HashSet path below.
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
    /// Page-read adapter that returns a copy of the page bytes (so the
    /// seeker can hold the buffer past the read; the writer's page pool
    /// recycles the original buffer immediately).
    /// </summary>
    private async ValueTask<byte[]> ReadPageOwnedAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])page.Clone();
        }
        finally
        {
            ReturnPage(page);
        }
    }

    /// <summary>
    /// Locates (and caches inside <paramref name="ctx"/>) the parent table's
    /// real-idx whose col_map exactly covers <paramref name="rel"/>'s
    /// PrimaryColumns in declaration order. Returns <see langword="null"/>
    /// when no covering index exists, when the format is Jet3 (no index
    /// emission), or when the parent table cannot be resolved.
    /// </summary>
    private async ValueTask<ParentSeekIndex?> ResolveParentSeekIndexAsync(
        FkRelationship rel,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        if (ctx.SeekIndexes.TryGetValue(rel.Name, out ParentSeekIndex? cached))
        {
            return cached;
        }

        ParentSeekIndex? resolved = null;
        try
        {
            if (_format == DatabaseFormat.Jet3Mdb)
            {
                return null;
            }

            CatalogEntry? primaryEntry = await GetCatalogEntryAsync(rel.PrimaryTable, cancellationToken).ConfigureAwait(false);
            if (primaryEntry == null)
            {
                return null;
            }

            TableDef primaryDef = await ReadRequiredTableDefAsync(primaryEntry.TDefPage, rel.PrimaryTable, cancellationToken).ConfigureAwait(false);
            CatalogEntry? foreignEntry = await GetCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            if (foreignEntry == null)
            {
                return null;
            }

            TableDef foreignDef = await ReadRequiredTableDefAsync(foreignEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);

            // Map relationship.PrimaryColumns → ColNum (col_map values).
            var pkColNums = new int[rel.PrimaryColumns.Count];
            var pkColTypes = new byte[rel.PrimaryColumns.Count];
            for (int i = 0; i < rel.PrimaryColumns.Count; i++)
            {
                int idx = primaryDef.FindColumnIndex(rel.PrimaryColumns[i]);
                if (idx < 0)
                {
                    return null;
                }

                pkColNums[i] = primaryDef.Columns[idx].ColNum;
                pkColTypes[i] = primaryDef.Columns[idx].Type;
            }

            // Map relationship.ForeignColumns → row index inside an
            // InsertRow values array (the same indexing GetParentKeySetAsync /
            // BuildCompositeKey already use).
            var foreignRowIdx = new int[rel.ForeignColumns.Count];
            for (int i = 0; i < rel.ForeignColumns.Count; i++)
            {
                foreignRowIdx[i] = foreignDef.FindColumnIndex(rel.ForeignColumns[i]);
                if (foreignRowIdx[i] < 0)
                {
                    return null;
                }
            }

            (long FirstDp, IReadOnlyList<bool> AscendingFlags)? hit = await TryFindCoveringRealIdxAsync(
                primaryEntry.TDefPage,
                pkColNums,
                cancellationToken).ConfigureAwait(false);
            if (hit == null)
            {
                return null;
            }

            // Probe each column type with the encoder; if any one rejects,
            // the seek path cannot serve this relationship and we fall back.
            for (int i = 0; i < pkColTypes.Length; i++)
            {
                if (!IndexKeyEncoder.IsColumnTypeSeekable(pkColTypes[i]))
                {
                    return null;
                }
            }

            var keyColumns = new ParentSeekKeyColumn[pkColNums.Length];
            for (int i = 0; i < pkColNums.Length; i++)
            {
                keyColumns[i] = new ParentSeekKeyColumn(pkColTypes[i], hit.Value.AscendingFlags[i], foreignRowIdx[i]);
            }

            resolved = new ParentSeekIndex(hit.Value.FirstDp, keyColumns);
            return resolved;
        }
        finally
        {
            ctx.SeekIndexes[rel.Name] = resolved;
        }
    }

    /// <summary>
    /// Walks the TDEF at <paramref name="tdefPage"/>, decodes every real-idx
    /// physical descriptor, and returns the first slot whose col_map exactly
    /// matches <paramref name="targetColNums"/> (in declaration order). The
    /// match is sharing-aware — a non-FK user index that happens to cover
    /// the same columns is acceptable. Returns <see langword="null"/> when
    /// no covering real-idx exists, when its <c>first_dp</c> is unset, or
    /// when the TDEF spans multiple pages.
    /// </summary>
    private async ValueTask<(long FirstDp, IReadOnlyList<bool> AscendingFlags)?> TryFindCoveringRealIdxAsync(
        long tdefPage,
        int[] targetColNums,
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
            return null;
        }

        int numCols = Ru16(td, _tdNumCols);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numCols < 0 || numCols > 4096 || numRealIdx <= 0 || numRealIdx > 1000)
        {
            return null;
        }

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return null;
        }

        const int RealIdxPhysSz = 52;
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * RealIdxPhysSz);
            if (phys + RealIdxPhysSz > td.Length)
            {
                break;
            }

            // col_map: 10 × {col_num(2), col_order(1)}; col_num=0xFFFF is a
            // padding slot. Match in declaration order against targetColNums.
            var ascending = new bool[targetColNums.Length];
            bool match = true;
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(td, so);
                if (slot < targetColNums.Length)
                {
                    if (cn != targetColNums[slot])
                    {
                        match = false;
                        break;
                    }

                    ascending[slot] = td[so + 2] != 0x02;
                }
                else if (cn != 0xFFFF)
                {
                    match = false;
                    break;
                }
            }

            if (!match)
            {
                continue;
            }

            int firstDp = Ri32(td, phys + 38);
            if (firstDp <= 0)
            {
                continue;
            }

            return (firstDp, ascending);
        }

        return null;
    }

    /// <summary>
    /// Encodes the composite seek key for a single FK-side row using the
    /// parent-side column types and the per-column ascending flags captured
    /// at index resolution. Returns <see langword="null"/> when any column
    /// is null (Access permits partial-null FK tuples — caller already
    /// short-circuited on this path) or when the encoder rejects any value.
    /// </summary>
    private static byte[]? TryEncodeSeekKey(ParentSeekIndex idx, object[] values)
    {
        var pieces = new byte[idx.KeyColumns.Count][];
        int total = 0;
        try
        {
            for (int i = 0; i < idx.KeyColumns.Count; i++)
            {
                ParentSeekKeyColumn col = idx.KeyColumns[i];
                if (col.ForeignColumnIndex < 0 || col.ForeignColumnIndex >= values.Length)
                {
                    return null;
                }

                object? v = values[col.ForeignColumnIndex];
                if (v is DBNull)
                {
                    v = null;
                }

                if (v == null)
                {
                    // BuildCompositeKey already rejected partial-null tuples,
                    // but be defensive — encoding a null key entry still
                    // produces a well-formed flag-only block.
                    pieces[i] = IndexKeyEncoder.EncodeEntry(col.ColumnType, null, col.Ascending);
                }
                else
                {
                    pieces[i] = IndexKeyEncoder.EncodeEntry(col.ColumnType, v, col.Ascending);
                }

                total += pieces[i].Length;
            }
        }
        catch (Exception ex) when (ex is NotSupportedException || ex is ArgumentException || ex is OverflowException)
        {
            return null;
        }

        byte[] composite = new byte[total];
        int offset = 0;
        for (int i = 0; i < pieces.Length; i++)
        {
            Buffer.BlockCopy(pieces[i], 0, composite, offset, pieces[i].Length);
            offset += pieces[i].Length;
        }

        return composite;
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
                pkIdx[i] = tableDef.FindColumnIndex(rel.PrimaryColumns[i]);
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
                fkIdx[i] = childDef.FindColumnIndex(rel.ForeignColumns[i]);
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
                    pkIdx[i] = childDef.FindColumnIndex(grandRel.PrimaryColumns[i]);
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

            // C5: cascade complex flat-child rows for the child rows we are
            // about to delete. Must precede MarkRowDeletedAsync so the
            // ConceptualTableID slots are still readable.
            var cascadeLocs = new List<RowLocation>(matchingRowIndices.Count);
            foreach (int rIdx in matchingRowIndices)
            {
                cascadeLocs.Add(locations[rIdx]);
            }

            await CascadeDeleteComplexChildrenAsync(childDef, cascadeLocs, cancellationToken).ConfigureAwait(false);

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
                primaryPkIdx[i] = primaryDef.FindColumnIndex(rel.PrimaryColumns[i]);
                fkIdx[i] = childDef.FindColumnIndex(rel.ForeignColumns[i]);
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
        ColumnInfo? nameCol = msysRelDef.FindColumn("szRelationship");
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
        ReadOnlyMemory<char> oldPwd = oldPassword.AsMemory();
        ReadOnlyMemory<char> newPwd = newPassword.AsMemory();

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
            .ReadDecryptedAsync(source, oldPwd, cancellationToken)
            .ConfigureAwait(false);

        // Default target = same as source when caller didn't override.
        AccessEncryptionFormat effectiveTarget = targetFormat ?? sourceFormat;
        return EncryptionConverter.ApplyEncryption(plaintext, effectiveTarget, newPwd);
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
        if (_isAgileEncryptedRewrap && _outerEncryptedStream != null && !_password.IsEmpty)
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
            OfficeCryptoAgile.Encrypt(inner, _password.Span);

        byte[] cfb = CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("EncryptionInfo", encryptionInfo),
            new KeyValuePair<string, byte[]>("EncryptedPackage", encryptedPackage),
        ]);

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

            if (c.IsAutoIncrement && (c.ClrType == typeof(byte) || c.ClrType == typeof(long)))
            {
                // Jet's FLAG_AUTO_LONG only persists Int16/Int32 counters; tinyint and BigInt
                // ("Large Number") autonumber columns require schema bits the writer does not
                // emit yet. Reject up-front so callers get a typed signal instead of a corrupt
                // schema on first insert.
                throw new NotSupportedException(
                    $"Column '{c.Name}': IsAutoIncrement is only supported for Int16 and Int32; '{c.ClrType}' is not supported.");
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
            // Complex columns (T_ATTACHMENT / T_COMPLEX) carry a magic Flags = 0x07
            // marker rather than real flag bits; do not interpret 0x02 / 0x04 here.
            bool isComplex = col.Type == T_ATTACHMENT || col.Type == T_COMPLEX;
            bool isNullable = isComplex || (col.Flags & 0x02) != 0;
            bool isAutoIncrement = !isComplex && (col.Flags & 0x04) != 0;

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

    /// <summary>
    /// Applies registered column constraints to <paramref name="values"/> and
    /// returns a list of auto-increment counter checkpoints captured for the
    /// row. Callers should pass the returned list to
    /// <see cref="RestoreAutoCounters"/> if a later step (FK enforcement,
    /// data-page write, deferred unique-index check) rejects the row, so the
    /// counter rewinds to the value the failed insert tried to consume.
    /// </summary>
    private async ValueTask<List<(ColumnConstraint Constraint, long? PreviousValue)>?> ApplyConstraintsAsync(string tableName, TableDef tableDef, object[] values, CancellationToken cancellationToken)
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
            return null;
        }

        List<(ColumnConstraint Constraint, long? PreviousValue)>? checkpoints = null;
        try
        {
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
                    long? previous = c.NextAutoValue;
                    long next = await GetNextAutoValueAsync(tableName, c, i, cancellationToken).ConfigureAwait(false);
                    (checkpoints ??= new List<(ColumnConstraint, long?)>(1)).Add((c, previous));
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
        catch
        {
            // A constraint failure after we already advanced one or more
            // auto-number counters must rewind those counters so the next
            // insert reuses the slot the rejected row would have taken.
            RestoreAutoCounters(checkpoints);
            throw;
        }

        return checkpoints;
    }

    /// <summary>
    /// Rewinds each auto-increment counter listed in <paramref name="checkpoints"/>
    /// back to the value it held before <see cref="ApplyConstraintsAsync"/>
    /// advanced it. Used by the insert paths to undo counter advances when a
    /// deferred constraint (currently the W11 post-write unique-index check
    /// in <see cref="MaintainIndexesAsync"/>) rejects the row after it has
    /// already consumed an auto-number value.
    /// </summary>
    private static void RestoreAutoCounters(List<(ColumnConstraint Constraint, long? PreviousValue)>? checkpoints)
    {
        if (checkpoints == null)
        {
            return;
        }

        foreach ((ColumnConstraint c, long? prev) in checkpoints)
        {
            c.NextAutoValue = prev;
        }
    }

    /// <summary>
    /// Single-row insert with full constraint + data-page + index-maintenance
    /// rollback on failure. Used by both <see cref="InsertRowAsync(string, object[], CancellationToken)"/>
    /// and the typed <see cref="InsertRowAsync{T}"/> overload.
    /// </summary>
    private async ValueTask InsertRowCoreAsync(
        string tableName,
        long tdefPage,
        TableDef tableDef,
        object[] values,
        FkContext? fkCtx,
        CancellationToken cancellationToken)
    {
        List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints =
            await ApplyConstraintsAsync(tableName, tableDef, values, cancellationToken).ConfigureAwait(false);

        if (fkCtx != null)
        {
            try
            {
                await EnforceFkOnInsertAsync(tableName, tableDef, values, fkCtx, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                RestoreAutoCounters(autoCheckpoints);
                throw;
            }
        }

        // ── W15 — pre-write unique-index enforcement ────────────────────
        // Reject duplicate keys before any disk page is mutated. The
        // post-write check inside MaintainIndexesAsync still runs as
        // defense-in-depth.
        try
        {
            await CheckUniqueIndexesPreInsertAsync(tdefPage, tableDef, tableName, [values], cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            RestoreAutoCounters(autoCheckpoints);
            throw;
        }

        RowLocation loc;
        try
        {
            loc = await InsertRowDataLocAsync(tdefPage, tableDef, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            RestoreAutoCounters(autoCheckpoints);
            throw;
        }

        try
        {
            if (fkCtx != null)
            {
                AugmentParentSetsAfterInsert(tableName, tableDef, values, fkCtx);
            }

            // W4-C-1 fast path: try in-place leaf splice for the inserted
            // row before falling back to a full snapshot+rebuild.
            (RowLocation Loc, object[] Row)[] hintInserts = [(loc, values)];
            bool incremental = await TryMaintainIndexesIncrementalAsync(
                tdefPage,
                tableDef,
                hintInserts,
                deletedRows: null,
                cancellationToken).ConfigureAwait(false);
            if (!incremental)
            {
                await MaintainIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            // The row hit disk but a deferred constraint (currently the W11
            // post-write unique-index check in MaintainIndexesAsync) rejected
            // it. Mark the row deleted and rewind the row count + auto-number
            // counters so the table is left exactly as it was before the call.
            await RollbackInsertedRowsAsync(tdefPage, [loc], cancellationToken).ConfigureAwait(false);
            RestoreAutoCounters(autoCheckpoints);
            throw;
        }
    }

    /// <summary>
    /// Marks every row in <paramref name="locations"/> as deleted on its data
    /// page and rewinds the owning TDEF's row count by the matching amount.
    /// Best-effort: any exception during rollback is swallowed so the original
    /// failure surfaces to the caller intact.
    /// </summary>
    private async ValueTask RollbackInsertedRowsAsync(long tdefPage, IReadOnlyList<RowLocation> locations, CancellationToken cancellationToken)
    {
        if (locations.Count == 0)
        {
            return;
        }

        try
        {
            foreach (RowLocation loc in locations)
            {
                await MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, cancellationToken).ConfigureAwait(false);
            }

            await AdjustTDefRowCountAsync(tdefPage, -locations.Count, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Best-effort rollback — never let it mask the original exception.
        catch
        {
        }
#pragma warning restore CA1031
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
        // C8: when the row pre-encode pass has already pushed an oversized
        // payload to LVAL pages, the sentinel carries the finished 12-byte
        // header and we just splice it through.
        if (value is PreEncodedLongValue pre)
        {
            return pre.HeaderBytes;
        }

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

        // Anything larger than the inline cap should have been routed through
        // the LVAL pre-encode pass already; reaching here means the caller is
        // bypassing InsertRowDataLocAsync's hook (e.g. internal system-table
        // writes), in which case the cap still applies.
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

        return Equals(left, right);
    }

    private static byte TypeCodeFromDefinition(ColumnDefinition column)
    {
        // Complex columns (Phase C2): Attachment and Multi-value have dedicated
        // type codes and override the CLR-driven mapping. The user picks one
        // explicitly via IsAttachment / IsMultiValue; declaring both is rejected
        // here so the writer never emits ambiguous descriptors.
        if (column.IsAttachment && column.IsMultiValue)
        {
            throw new ArgumentException($"Column '{column.Name}' cannot be both Attachment and MultiValue.", nameof(column));
        }

        if (column.IsAttachment)
        {
            return T_ATTACHMENT;
        }

        if (column.IsMultiValue)
        {
            return T_COMPLEX;
        }

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
            //
            // Complex columns (T_ATTACHMENT / T_COMPLEX) override these heuristics:
            // mdbtools documents the bitmask as "always exactly 0x07" regardless of
            // the underlying meaning. The 4-byte fixed-area payload carries the
            // ConceptualTableID joining the parent row to its hidden flat child rows.
            byte flags;
            bool isComplex = type == T_ATTACHMENT || type == T_COMPLEX;
            if (isComplex)
            {
                flags = 0x07;
            }
            else
            {
                flags = 0;
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
                Misc = isComplex ? definition.ComplexId : 0,
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
            case T_ATTACHMENT:
            case T_COMPLEX:
                // Complex columns store a 4-byte ConceptualTableID payload per
                // parent row (the ComplexID joins to MSysComplexColumns and the
                // hidden flat child table). See complex-columns-format-notes.md §2.1.
                return 4;
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

    // ── C8 — LVAL chain emission for oversized MEMO / OLE / Attachment payloads ──
    // The 12-byte LVAL header (read by AccessReader.ReadLongValueAsync) is:
    //   [memo_len: 3 bytes LE][bitmask: 1 byte][lval_dp: 4 bytes LE][unknown: 4 bytes]
    // bitmask values:
    //   0x80 — inline (data follows the 12-byte header in the row body)
    //   0x40 — single LVAL page (payload occupies one LVAL row at lval_dp)
    //   0x00 — chained LVAL pages ([next_lval_dp(4 LE)][chunk_bytes] per row, terminator next_lval_dp = 0)
    // lval_dp encoding: ((page_number << 8) | row_index_within_page).

    /// <summary>
    /// Sentinel produced by <see cref="PreEncodeLongValuesAsync"/>. The wrapped
    /// 12-byte LVAL header already references LVAL pages allocated earlier in
    /// the row-insert pipeline, so <see cref="EncodeOleValue"/> /
    /// <see cref="EncodeVariableValue"/> just splice <see cref="HeaderBytes"/>
    /// straight into the row body without any further encoding.
    /// </summary>
    private sealed class PreEncodedLongValue
    {
        public PreEncodedLongValue(byte[] headerBytes)
        {
            HeaderBytes = headerBytes;
        }

        public byte[] HeaderBytes { get; }
    }

    /// <summary>
    /// Pre-encode pass for <see cref="InsertRowDataLocAsync"/>: any MEMO / OLE
    /// value whose payload exceeds the inline cap is written to one or more
    /// freshly-appended LVAL data pages here, and the in-row value is replaced
    /// with a <see cref="PreEncodedLongValue"/> sentinel carrying the matching
    /// 12-byte header. Returns the same array reference when no large payloads
    /// were found and a defensively-cloned array otherwise so the caller's
    /// original <c>values</c> stays untouched.
    /// </summary>
    private async ValueTask<object[]> PreEncodeLongValuesAsync(TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        object[]? result = null;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            if (col.IsFixed || (col.Type != T_OLE && col.Type != T_MEMO))
            {
                continue;
            }

            object value = values[i];
            if (value is null or DBNull or PreEncodedLongValue)
            {
                continue;
            }

            byte[]? data;
            int inlineCap;
            if (col.Type == T_OLE)
            {
                data = value as byte[];
                if (data == null)
                {
                    continue;
                }

                inlineCap = MaxInlineOleBytes;
            }
            else
            {
                string? text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
                if (string.IsNullOrEmpty(text))
                {
                    continue;
                }

                data = _format != DatabaseFormat.Jet3Mdb ? EncodeJet4Text(text) : _ansiEncoding.GetBytes(text);
                inlineCap = MaxInlineMemoBytes;
            }

            if (data.Length <= inlineCap)
            {
                continue;
            }

            byte[] header = await EncodeAsLvalChainAsync(data, cancellationToken).ConfigureAwait(false);
            result ??= (object[])values.Clone();
            result[i] = new PreEncodedLongValue(header);
        }

        return result ?? values;
    }

    /// <summary>
    /// Allocates one (single-page LVAL, bitmask <c>0x40</c>) or many (chained
    /// LVAL pages, bitmask <c>0x00</c>) LVAL data pages for a payload that is
    /// too large for the inline form, returning the resulting 12-byte LVAL
    /// header. Pages are appended in reverse so each predecessor row can hold
    /// its successor's <c>lval_dp</c> pointer.
    /// </summary>
    private async ValueTask<byte[]> EncodeAsLvalChainAsync(byte[] data, CancellationToken cancellationToken)
    {
        if (data.Length > MaxLvalPayloadBytes)
        {
            throw new JetLimitationException(
                $"Long value is {data.Length} bytes, which exceeds the JET 24-bit LVAL length limit of {MaxLvalPayloadBytes} bytes.");
        }

        // One row per LVAL page. The row table costs 2 bytes for a single offset.
        int singleRowMax = _pgSz - _dpRowsStart - 2;
        int chainRowMax = singleRowMax - 4; // first 4 bytes of each chained row are the next-pointer

        var header = new byte[12];
        WriteUInt24(header, 0, data.Length);

        if (data.Length <= singleRowMax)
        {
            byte[] page = BuildSingleLvalPageBuffer(data);
            long pageNumber = await AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
            header[3] = 0x40;
            uint lvalDp = unchecked((uint)((pageNumber << 8) | 0));
            Wi32(header, 4, (int)lvalDp);
            return header;
        }

        // Chunk size for chained rows. Allocating in reverse means each newly
        // appended page's row carries the previously-appended page's lval_dp
        // as its [next_dp] prefix.
        int chunkCount = (data.Length + chainRowMax - 1) / chainRowMax;
        uint nextDp = 0;
        for (int i = chunkCount - 1; i >= 0; i--)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int chunkStart = i * chainRowMax;
            int chunkLen = Math.Min(chainRowMax, data.Length - chunkStart);
            byte[] page = BuildChainLvalPageBuffer(data, chunkStart, chunkLen, nextDp);
            long pageNumber = await AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
            nextDp = unchecked((uint)((pageNumber << 8) | 0));
        }

        header[3] = 0x00;
        Wi32(header, 4, (int)nextDp);
        return header;
    }

    /// <summary>
    /// Builds a single-row LVAL data page (bitmask <c>0x40</c> form): the row
    /// body is the entire payload with no next-pointer prefix.
    /// </summary>
    private byte[] BuildSingleLvalPageBuffer(byte[] payload)
    {
        byte[] page = new byte[_pgSz];
        page[0] = 0x01; // page_type = data page (the reader treats LVAL pages as type 0x01 with tdef_page = 0)
        page[1] = 0x01;
        Wi32(page, _dpTDefOff, 0);
        Wu16(page, _dpNumRows, 1);

        int rowStart = _pgSz - payload.Length;
        Buffer.BlockCopy(payload, 0, page, rowStart, payload.Length);
        Wu16(page, _dpRowsStart, rowStart);

        int freeSpace = rowStart - (_dpRowsStart + 2);
        Wu16(page, 2, freeSpace);
        return page;
    }

    /// <summary>
    /// Builds a single-row LVAL data page in chained form (bitmask <c>0x00</c>):
    /// the first 4 bytes of the row are the next-row pointer (<c>page&lt;&lt;8 | row</c>,
    /// little-endian; <c>0</c> on the terminal page) and the remainder is the chunk payload.
    /// </summary>
    private byte[] BuildChainLvalPageBuffer(byte[] data, int offset, int length, uint nextDp)
    {
        byte[] page = new byte[_pgSz];
        page[0] = 0x01;
        page[1] = 0x01;
        Wi32(page, _dpTDefOff, 0);
        Wu16(page, _dpNumRows, 1);

        int rowLen = 4 + length;
        int rowStart = _pgSz - rowLen;
        Wi32(page, rowStart, (int)nextDp);
        Buffer.BlockCopy(data, offset, page, rowStart + 4, length);
        Wu16(page, _dpRowsStart, rowStart);

        int freeSpace = rowStart - (_dpRowsStart + 2);
        Wu16(page, 2, freeSpace);
        return page;
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

            var pkCols = new string[idx.Columns.Count];
            var descendingCols = new List<string>();
            for (int i = 0; i < idx.Columns.Count; i++)
            {
                pkCols[i] = idx.Columns[i].Name;
                if (!idx.Columns[i].IsAscending)
                {
                    descendingCols.Add(idx.Columns[i].Name);
                }
            }

            if (idx.Kind == IndexKind.PrimaryKey)
            {
                result.Add(new IndexDefinition(idx.Name, pkCols)
                {
                    IsPrimaryKey = true,
                    DescendingColumns = descendingCols,
                });
            }
            else
            {
                result.Add(new IndexDefinition(idx.Name, pkCols)
                {
                    IsUnique = idx.IsUnique,
                    DescendingColumns = descendingCols,
                });
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

    private ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, CancellationToken cancellationToken = default)
        => InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, catalogFlags: 0, cancellationToken);

    private async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, uint catalogFlags, CancellationToken cancellationToken = default)
    {
        TableDef msys = await ReadRequiredTableDefAsync(2, "MSysObjects", cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", (int)tdefPageNumber);
        msys.SetValueByName(values, "ParentId", 0);
        msys.SetValueByName(values, "Name", tableName);
        msys.SetValueByName(values, "Type", (short)OBJ_TABLE);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", unchecked((int)catalogFlags));

        // LvProp is the OLE/LongBinary cell carrying per-column persisted properties
        // (DefaultValue, ValidationRule, ValidationText, Description). Only emitted on
        // the full 17-column catalog schema (the slim 9-column legacy schema lacks the
        // column entirely, so SetValue is a no-op).
        if (lvProp is not null)
        {
            msys.SetValueByName(values, "LvProp", lvProp);
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
        //
        // Phase C9: identify complex columns being dropped or renamed by this rewrite
        // BEFORE the cascade-skipping drop runs. Surviving complex columns (matched by
        // ComplexId between the existing and projected schemas) are preserved as-is —
        // their flat child tables and MSysComplexColumns rows stay attached to the
        // rebuilt parent. Dropped complex columns get their flat child + catalog row
        // removed surgically; renamed complex columns get their MSysComplexColumns row
        // rewritten with the new ColumnName.
        Dictionary<int, ColumnDefinition> newComplexById = new Dictionary<int, ColumnDefinition>();
        foreach (ColumnDefinition c in newDefs)
        {
            if ((c.IsAttachment || c.IsMultiValue) && c.ComplexId != 0)
            {
                newComplexById[c.ComplexId] = c;
            }
        }

        var droppedComplex = new List<(string Name, int ComplexId)>();
        var renamedComplex = new List<(string OldName, string NewName, int ComplexId)>();
        foreach (ColumnDefinition c in existingDefs)
        {
            if (!(c.IsAttachment || c.IsMultiValue) || c.ComplexId == 0)
            {
                continue;
            }

            if (!newComplexById.TryGetValue(c.ComplexId, out ColumnDefinition? survivor))
            {
                droppedComplex.Add((c.Name, c.ComplexId));
            }
            else if (!string.Equals(survivor.Name, c.Name, StringComparison.OrdinalIgnoreCase))
            {
                renamedComplex.Add((c.Name, survivor.Name, c.ComplexId));
            }
        }

        byte[]? renamedLvProp = JetExpressionConverter.BuildLvPropBlob(newDefs, _format);
        await DropTableCoreAsync(tableName, dropComplexChildren: false, cancellationToken).ConfigureAwait(false);
        await RenameTableInCatalogAsync(tempName, tableName, renamedLvProp, cancellationToken).ConfigureAwait(false);

        foreach ((string colName, int complexId) in droppedComplex)
        {
            await DropSingleComplexChildAsync(colName, complexId, cancellationToken).ConfigureAwait(false);
        }

        foreach ((string oldColName, string newColName, int complexId) in renamedComplex)
        {
            await RenameComplexColumnArtifactsAsync(oldColName, newColName, complexId, cancellationToken).ConfigureAwait(false);
        }
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
                // Phase C9 (2026-04-25): preserve attachment columns across
                // AddColumnAsync / DropColumnAsync / RenameColumnAsync. The parent
                // TDEF descriptor round-trips with the ComplexID intact (ColumnInfo.Misc
                // → ColumnDefinition.ComplexId → re-emitted into the rebuilt TDEF's
                // misc slot), and the existing hidden flat child table + MSysComplexColumns
                // row are kept attached because the rewrite path skips the cascade-on-drop
                // step. Per-row complex slot is null on the rebuilt parent (same as fresh
                // Insert), and the reader re-joins via the parent's auto-number primary
                // key against the flat table's `_<columnName>` FK back-reference.
                return new ColumnDefinition(column.Name, typeof(byte[]))
                {
                    IsAttachment = true,
                    ComplexId = column.Misc,
                };
            case T_COMPLEX:
                // Multi-value column. MultiValueElementType is left unset because
                // PrepareComplexColumnAllocationsAsync only validates it for
                // freshly-allocated columns (ComplexId == 0); preserved columns
                // bypass that check entirely.
                return new ColumnDefinition(column.Name, typeof(byte[]))
                {
                    IsMultiValue = true,
                    ComplexId = column.Misc,
                };
            default:
                throw new NotSupportedException($"Column '{column.Name}' has unsupported type code 0x{column.Type:X2}.");
        }

        // Surface the persisted TDEF flag bits as ColumnDefinition properties so the
        // schema-rewrite path retains NOT NULL / auto-increment metadata that Access
        // wrote into the original column descriptor. Complex columns (T_ATTACHMENT /
        // T_COMPLEX) return early above because their Flags byte is the magic 0x07
        // marker rather than real flag bits.
        return baseDef with
        {
            IsNullable = (column.Flags & 0x02) != 0,
            IsAutoIncrement = (column.Flags & 0x04) != 0,
        };
    }

    private byte[] BuildTDefPage(TableDef tableDef)
        => BuildTDefPageWithIndexOffsets(tableDef, []).Page;

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

            // Complex columns (T_ATTACHMENT / T_COMPLEX) carry the 4-byte ComplexID
            // at descriptor-relative offset _colMiscOff (=11 on Jet4/ACE). Round-tripped
            // through reader → writer so existing complex columns survive
            // RewriteTableAsync (AddColumn / DropColumn / RenameColumn). For new
            // complex columns declared via ColumnDefinition.AsAttachment() /
            // .AsMultiValue(), CreateTableAsync supplies Misc = 0 today and throws
            // before reaching disk because the corresponding flat table + MSysComplexColumns
            // row are not yet emitted (see complex-columns-format-notes.md C3).
            if (col.Type == T_ATTACHMENT || col.Type == T_COMPLEX)
            {
                Wi32(page, o + _colMiscOff, col.Misc);
            }

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
        int[] firstDpOffsets = numIdx > 0 ? new int[numIdx] : [];
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

            // Per-format field offsets within the real-idx physical descriptor
            // and logical-idx entry. The Jet3 layouts (39 / 20 bytes) drop the
            // Jet4 leading 4-byte cookie + trailing tail, so every field shifts
            // left by 4 bytes; first_dp lives where Jet4 puts used_pages and
            // there is no separate used_pages slot. See §3.1 / §3.2 (W17a).
            int physFirstDpOff = jet4 ? 38 : 34;
            int physFlagsOff = jet4 ? 42 : 38;
            int logIndexNumOff = jet4 ? 4 : 0;
            int logIndexNum2Off = jet4 ? 8 : 4;
            int logRelIdxNumOff = jet4 ? 13 : 9;
            int logIndexTypeOff = jet4 ? 23 : 19;

            for (int i = 0; i < numIdx; i++)
            {
                ResolvedIndex ri = indexes[i];

                // ── Real-index physical descriptor ─────────────────────────
                // §3.1 Jet4 (52 bytes): unknown(4) + col_map(30) + used_pages(4)
                //                     + first_dp(4) + flags(1) + unknown(9).
                // §3.1 Jet3 (39 bytes): unknown(4) + col_map(30) + first_dp(4)
                //                     + flags(1).
                int phys = realIdxPhysStart + (i * realIdxPhysSz);

                // col_map: 10 × { col_num(2), col_order(1) }. First N slots = our
                // key columns (per-column ascending/descending per W11), remaining
                // slots filled with 0xFFFF.
                for (int slot = 0; slot < 10; slot++)
                {
                    int so = phys + 4 + (slot * 3);
                    if (slot < ri.ColumnNumbers.Count)
                    {
                        Wu16(page, so, ri.ColumnNumbers[slot]);

                        // 0x01 ascending, 0x02 descending — descending byte taken
                        // from Jackcess IndexImpl; not yet probe-verified.
                        page[so + 2] = ri.Ascending[slot] ? (byte)0x01 : (byte)0x02;
                    }
                    else
                    {
                        Wu16(page, so, 0xFFFF);
                        page[so + 2] = 0x00;
                    }
                }

                // The caller patches first_dp after appending the leaf page (W3).
                // Per the §3.1 empirical correction, real Access fixtures emit
                // flags = 0x00 even for PK indexes — uniqueness is signalled by
                // index_type = 0x01 below, not the flag bit. W11: non-PK unique
                // indexes set flags bit 0x01 explicitly.
                if (ri.IsUnique && !ri.IsPrimaryKey)
                {
                    page[phys + physFlagsOff] = 0x01;
                }

                firstDpOffsets[i] = phys + physFirstDpOff;

                // ── Logical-index entry ─────────────────────────────────────
                // §3.2 Jet4 (28 bytes): unknown(4) + index_num(4) + index_num2(4)
                //   + rel_tbl_type(1) + rel_idx_num(4) + rel_tbl_page(4)
                //   + cascade_ups(1) + cascade_dels(1) + index_type(1) + trailing(4).
                // §3.2 Jet3 (20 bytes): same fields, no leading cookie / trailing tail.
                int log = logIdxStart + (i * logIdxEntrySz);
                Wi32(page, log + logIndexNumOff, i);                  // index_num
                Wi32(page, log + logIndexNum2Off, i);                 // index_num2 (one real per logical)
                Wi32(page, log + logRelIdxNumOff, -1);                // rel_idx_num = 0xFFFFFFFF (not a FK)

                // index_type: 0x01 for PK (W8), 0x00 for normal. FK (0x02) is W9.
                page[log + logIndexTypeOff] = (byte)(ri.IsPrimaryKey ? IndexKind.PrimaryKey : IndexKind.Normal);
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

    private List<ResolvedIndex> ResolveIndexes(IReadOnlyList<IndexDefinition> indexes, TableDef tableDef)
    {
        if (indexes.Count == 0)
        {
            return [];
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

            // W11 (2026-04-25): multi-column non-PK indexes are now accepted.
            // The W1-era restriction (single column when not IsPrimaryKey)
            // has been lifted; the W5 maintenance loop encodes per-column
            // and concatenates to form the composite key.
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

                ColumnInfo column = tableDef.FindColumn(columnName)
                    ?? throw new ArgumentException($"IndexDefinition '{def.Name}' references unknown column '{columnName}'.", nameof(indexes));

                colNums[k] = column.ColNum;
            }

            // W11: per-column ascending direction. DescendingColumns is a
            // case-insensitive subset of Columns; any entry that does not
            // appear in Columns is rejected.
            bool[] ascending = new bool[def.Columns.Count];
            for (int k = 0; k < ascending.Length; k++)
            {
                ascending[k] = true;
            }

            if (def.DescendingColumns is { Count: > 0 } descendingList)
            {
                foreach (string descName in descendingList)
                {
                    if (string.IsNullOrEmpty(descName))
                    {
                        throw new ArgumentException($"IndexDefinition '{def.Name}' has an empty entry in DescendingColumns.", nameof(indexes));
                    }

                    int matchIndex = -1;
                    for (int k = 0; k < def.Columns.Count; k++)
                    {
                        if (string.Equals(def.Columns[k], descName, StringComparison.OrdinalIgnoreCase))
                        {
                            matchIndex = k;
                            break;
                        }
                    }

                    if (matchIndex < 0)
                    {
                        throw new ArgumentException(
                            $"IndexDefinition '{def.Name}' lists '{descName}' in DescendingColumns but the column is not in Columns.",
                            nameof(indexes));
                    }

                    ascending[matchIndex] = false;
                }
            }

            // PKs are implicitly unique (signalled by index_type=0x01, not the
            // flag bit per §3.1). User-set IsUnique on a PK is silently subsumed.
            bool isUnique = def.IsPrimaryKey || def.IsUnique;

            result.Add(new ResolvedIndex(def.Name, colNums, ascending, def.IsPrimaryKey, isUnique));
        }

        return result;
    }

    /// <summary>
    /// Phase C1: scaffold mandatory ACCDB system tables (currently
    /// <c>MSysComplexColumns</c>) into a freshly-created database. Called
    /// from <see cref="CreateDatabaseAsync(string, DatabaseFormat, AccessWriterOptions?, CancellationToken)"/>
    /// after the bare 3-page empty database has been opened. ACCDB only —
    /// complex columns are an Access 2007+ feature absent from Jet3/Jet4
    /// <c>.mdb</c>. Skipped on the slim 9-column legacy catalog schema
    /// (<see cref="AccessWriterOptions.WriteFullCatalogSchema"/> = <c>false</c>)
    /// because that mode targets backward-compatible byte hashing and must
    /// not introduce additional pages.
    /// </summary>
    private static async ValueTask ScaffoldSystemTablesAsync(AccessWriter writer, DatabaseFormat format, bool fullCatalogSchema, CancellationToken cancellationToken)
    {
        if (format != DatabaseFormat.AceAccdb || !fullCatalogSchema)
        {
            return;
        }

        await writer.CreateMSysComplexColumnsAsync(cancellationToken).ConfigureAwait(false);
        await writer.CreateMSysComplexTypeTemplatesAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Phase C10: per-kind <c>MSysComplexType_*</c> template tables. Each entry maps
    /// the canonical Access template name to the column schema Access emits for that
    /// template (verified against <c>ComplexFields.accdb</c> in
    /// <c>docs/design/format-probe-appendix-complex.md</c> §<c>MSysComplexType_*</c>).
    /// All templates are zero-row, zero-index tables; their <c>MSysObjects.Id</c>
    /// (= TDEF page) is what <c>MSysComplexColumns.ComplexTypeObjectID</c> points at.
    /// </summary>
    private static readonly (string Name, ColumnDefinition[] Columns)[] _complexTypeTemplates =
    [
        ("MSysComplexType_UnsignedByte", new[] { new ColumnDefinition("Value", typeof(byte)) }),
        ("MSysComplexType_Short",        [new ColumnDefinition("Value", typeof(short))]),
        ("MSysComplexType_Long",         [new ColumnDefinition("Value", typeof(int))]),
        ("MSysComplexType_IEEESingle",   [new ColumnDefinition("Value", typeof(float))]),
        ("MSysComplexType_IEEEDouble",   [new ColumnDefinition("Value", typeof(double))]),
        ("MSysComplexType_GUID",         [new ColumnDefinition("Value", typeof(Guid))]),
        ("MSysComplexType_Decimal",      [new ColumnDefinition("Value", typeof(decimal))]),
        ("MSysComplexType_Text",         [new ColumnDefinition("Value", typeof(string), maxLength: 255)]),
        ("MSysComplexType_Attachment",
        [
            new ColumnDefinition("FileData",      typeof(byte[])),
            new ColumnDefinition("FileFlags",     typeof(int)),
            new ColumnDefinition("FileName",      typeof(string), maxLength: 255),
            new ColumnDefinition("FileTimeStamp", typeof(DateTime)),
            new ColumnDefinition("FileType",      typeof(string), maxLength: 255),
            new ColumnDefinition("FileURL",       typeof(string)),
        ]),
    ];

    /// <summary>
    /// Phase C10: scaffolds the nine <c>MSysComplexType_*</c> template tables
    /// (<c>UnsignedByte</c>, <c>Short</c>, <c>Long</c>, <c>IEEESingle</c>,
    /// <c>IEEEDouble</c>, <c>GUID</c>, <c>Decimal</c>, <c>Text</c>, <c>Attachment</c>)
    /// into a freshly-created ACCDB so subsequent <see cref="EmitComplexColumnArtifactsAsync"/>
    /// calls can populate <c>MSysComplexColumns.ComplexTypeObjectID</c> with a real
    /// catalog id instead of the placeholder <c>0</c>. Each catalog row carries
    /// <c>MSysObjects.Flags = 0x80030000</c> (system + the 0x30000 marker Access uses
    /// for type-template tables) so the templates are excluded from
    /// <c>ListTablesAsync</c>. Schema verified against <c>ComplexFields.accdb</c> —
    /// see <c>docs/design/format-probe-appendix-complex.md</c>.
    /// </summary>
    private async ValueTask CreateMSysComplexTypeTemplatesAsync(CancellationToken cancellationToken)
    {
        foreach ((string name, ColumnDefinition[] cols) in _complexTypeTemplates)
        {
            TableDef tableDef = BuildTableDefinition(cols, _format);
            (byte[] tdefPage, _) = BuildTDefPageWithIndexOffsets(tableDef, []);
            long tdefPageNumber = await AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

            await InsertCatalogEntryAsync(
                name,
                tdefPageNumber,
                lvProp: null,
                catalogFlags: 0x80030000U,
                cancellationToken).ConfigureAwait(false);
        }

        InvalidateCatalogCache();
    }

    /// <summary>
    /// Maps a user-declared complex column to the canonical
    /// <c>MSysComplexType_*</c> template name (Phase C10). Returns <see langword="null"/>
    /// when the column is not complex or its element type has no matching template.
    /// </summary>
    private static string? ResolveComplexTypeTemplateName(ColumnDefinition col)
    {
        if (col.IsAttachment)
        {
            return "MSysComplexType_Attachment";
        }

        if (!col.IsMultiValue)
        {
            return null;
        }

        Type? t = col.MultiValueElementType;
        if (t is null)
        {
            return null;
        }

        if (t == typeof(byte))
        {
            return "MSysComplexType_UnsignedByte";
        }

        if (t == typeof(short))
        {
            return "MSysComplexType_Short";
        }

        if (t == typeof(int))
        {
            return "MSysComplexType_Long";
        }

        if (t == typeof(float))
        {
            return "MSysComplexType_IEEESingle";
        }

        if (t == typeof(double))
        {
            return "MSysComplexType_IEEEDouble";
        }

        if (t == typeof(Guid))
        {
            return "MSysComplexType_GUID";
        }

        if (t == typeof(decimal))
        {
            return "MSysComplexType_Decimal";
        }

        if (t == typeof(string))
        {
            return "MSysComplexType_Text";
        }

        return null;
    }

    /// <summary>
    /// Creates the empty <c>MSysComplexColumns</c> system table (Phase C1).
    /// Schema verified against <c>ComplexFields.accdb</c> (see
    /// <c>docs/design/format-probe-appendix-complex.md</c> and
    /// <c>docs/design/complex-columns-format-notes.md</c> §2.2): four
    /// <c>T_LONG</c> columns (<c>ComplexTypeObjectID</c>, <c>FlatTableID</c>,
    /// <c>ConceptualTableID</c>, <c>ComplexID</c>) plus a <c>ColumnName</c>
    /// <c>T_TEXT(510)</c> variable column. The catalog row carries flag
    /// <c>0x80000000</c> (system / hidden) so the table is excluded from
    /// <c>GetUserTablesAsync</c>.
    /// </summary>
    private async ValueTask CreateMSysComplexColumnsAsync(CancellationToken cancellationToken)
    {
        var columns = new[]
        {
            new ColumnDefinition("ColumnName", typeof(string), maxLength: 255),
            new ColumnDefinition("ComplexTypeObjectID", typeof(int)),
            new ColumnDefinition("FlatTableID", typeof(int)),
            new ColumnDefinition("ConceptualTableID", typeof(int)),
            new ColumnDefinition("ComplexID", typeof(int)),
        };

        TableDef tableDef = BuildTableDefinition(columns, _format);
        (byte[] tdefPage, _) = BuildTDefPageWithIndexOffsets(tableDef, []);
        long tdefPageNumber = await AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        await InsertCatalogEntryAsync(
            "MSysComplexColumns",
            tdefPageNumber,
            lvProp: null,
            catalogFlags: 0x80000000U,
            cancellationToken).ConfigureAwait(false);

        InvalidateCatalogCache();
    }

    /// <summary>
    /// Per-column scratch state captured by <see cref="PrepareComplexColumnAllocationsAsync"/>
    /// and consumed by <see cref="EmitComplexColumnArtifactsAsync"/>.
    /// </summary>
    private readonly record struct ComplexColumnAllocation(int ColumnIndex, int ComplexId, int ConceptualTableId);

    /// <summary>
    /// Phase C3 pre-flight for <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, CancellationToken)"/>.
    /// Walks <paramref name="columns"/> for user-declared complex columns
    /// (<see cref="ColumnDefinition.IsAttachment"/> / <see cref="ColumnDefinition.IsMultiValue"/>
    /// where <c>ComplexId == 0</c>), validates the format, and allocates a
    /// fresh per-database <c>ComplexID</c> + <c>ConceptualTableID</c> for each.
    /// Returns <see langword="null"/> when no allocation is needed.
    /// </summary>
    private async ValueTask<IReadOnlyList<ComplexColumnAllocation>?> PrepareComplexColumnAllocationsAsync(
        IReadOnlyList<ColumnDefinition> columns,
        CancellationToken cancellationToken)
    {
        List<int>? indices = null;
        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition def = columns[i];
            if ((def.IsAttachment || def.IsMultiValue) && def.ComplexId == 0)
            {
                indices ??= new List<int>(2);
                indices.Add(i);

                if (def.IsMultiValue && def.MultiValueElementType is null)
                {
                    throw new ArgumentException(
                        $"Column '{def.Name}': MultiValue columns require MultiValueElementType to be set.",
                        nameof(columns));
                }
            }
        }

        if (indices is null)
        {
            return null;
        }

        if (_format != DatabaseFormat.AceAccdb)
        {
            throw new NotSupportedException(
                "Attachment and MultiValue columns are an Access 2007+ ACE feature; declare them only on .accdb databases.");
        }

        long msysComplexPg = await FindSystemTableTdefPageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        if (msysComplexPg == 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysComplexColumns' table. Create the database via " +
                "AccessWriter.CreateDatabaseAsync (which scaffolds it automatically) before declaring complex columns, " +
                "or open an Access-authored .accdb that already contains the catalog.");
        }

        int nextId = await GetNextComplexIdAsync(msysComplexPg, cancellationToken).ConfigureAwait(false);
        var allocations = new ComplexColumnAllocation[indices.Count];
        for (int i = 0; i < indices.Count; i++)
        {
            int id = nextId++;
            allocations[i] = new ComplexColumnAllocation(indices[i], id, id);
        }

        return allocations;
    }

    /// <summary>
    /// Returns one greater than the largest <c>ComplexID</c> currently stored in
    /// <c>MSysComplexColumns</c>, or <c>1</c> when the table is empty. ConceptualTableIDs
    /// are allocated from the same monotonically-increasing pool to keep the per-database
    /// space simple — Access uses two independent counters but neither value is exposed
    /// through any external API and there is no documented scenario where they must differ.
    /// </summary>
    private async ValueTask<int> GetNextComplexIdAsync(long msysComplexPg, CancellationToken cancellationToken)
    {
        TableDef msysComplex = await ReadRequiredTableDefAsync(msysComplexPg, "MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        ColumnInfo? idCol = msysComplex.FindColumn("ComplexID");
        ColumnInfo? ctIdCol = msysComplex.FindColumn("ConceptualTableID");

        int maxId = 0;
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

                if (Ri32(page, _dpTDefOff) != msysComplexPg)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    if (idCol != null)
                    {
                        string idText = ReadColumnValue(page, row.RowStart, row.RowSize, idCol);
                        if (int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) && v > maxId)
                        {
                            maxId = v;
                        }
                    }

                    if (ctIdCol != null)
                    {
                        string ctText = ReadColumnValue(page, row.RowStart, row.RowSize, ctIdCol);
                        if (int.TryParse(ctText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int cv) && cv > maxId)
                        {
                            maxId = cv;
                        }
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return maxId + 1;
    }

    /// <summary>
    /// Phase C3 post-flight: for each user-declared complex column on the parent
    /// table, build a hidden flat child table per <c>docs/design/complex-columns-format-notes.md</c>
    /// §2.3 / §2.4 and append the corresponding <c>MSysComplexColumns</c> row so
    /// readers can join parent rows to their child values.
    /// </summary>
    /// <remarks>
    /// MVP scope: emits the bare flat-table schema (FK + per-kind value columns) with no
    /// PK, no autoincrement, and no indexes. This is sufficient for round-trip through
    /// this library's reader; Microsoft Access compatibility requires a Compact &amp; Repair
    /// pass to rebuild the missing PK / FK back-reference indexes (validation gap noted in
    /// the design doc §5). <c>ComplexTypeObjectID</c> is set to <c>0</c> because the
    /// <c>MSysComplexType_*</c> template tables are not yet scaffolded; the reader's
    /// classifier falls back to <see cref="ComplexColumnKind.Unknown"/> in that case.
    /// </remarks>
    private async ValueTask EmitComplexColumnArtifactsAsync(
        string parentTableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<ComplexColumnAllocation> allocations,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < allocations.Count; i++)
        {
            ComplexColumnAllocation alloc = allocations[i];
            ColumnDefinition col = columns[alloc.ColumnIndex];

            string flatTableName = BuildFlatTableName(col.Name);
            (ColumnDefinition[] flatCols, IndexDefinition[] flatIndexes) =
                BuildFlatTableSchema(parentTableName, col);

            long flatTdefPage = await CreateTableInternalAsync(
                flatTableName,
                flatCols,
                indexes: flatIndexes,
                catalogFlags: 0x800A0000U,
                cancellationToken).ConfigureAwait(false);

            // Phase C10: resolve the matching MSysComplexType_* template id so the
            // MSysComplexColumns row points at the canonical type-template table
            // instead of carrying the placeholder 0. Templates are scaffolded by
            // CreateDatabaseAsync; for files opened in-place that lack them (e.g.
            // a slim-catalog ACCDB or a pre-C10 writer-authored file) the lookup
            // returns 0 and the row falls back to the pre-C10 placeholder value.
            string? templateName = ResolveComplexTypeTemplateName(col);
            int templateId = templateName is null
                ? 0
                : (int)await FindSystemTableTdefPageAsync(templateName, cancellationToken).ConfigureAwait(false);

            await InsertMSysComplexColumnsRowAsync(
                col.Name,
                complexId: alloc.ComplexId,
                conceptualTableId: alloc.ConceptualTableId,
                flatTableId: (int)flatTdefPage,
                complexTypeObjectId: templateId,
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Generates the canonical hidden-flat-table name <c>f_&lt;32-hex-uppercase&gt;_&lt;userColumnName&gt;</c>
    /// per the design doc §2.3 / format-probe-appendix-complex.md observations.
    /// </summary>
    private static string BuildFlatTableName(string userColumnName)
    {
        // The 32 hex chars are a GUID without dashes — Access uses a fresh GUID per
        // flat table; we do the same so the name is unique even when two columns
        // share a name across tables.
        string guid = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture).ToUpperInvariant();
        return $"f_{guid}_{userColumnName}";
    }

    /// <summary>
    /// Builds the flat-table column list and the system-managed indexes per
    /// the per-kind schemas in the design doc §2.4 / §4.2 (Phase C7).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two LONG columns participate in the back-reference plumbing:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><c>_&lt;userColumnName&gt;</c> — FK back-reference holding the parent row's <c>ConceptualTableID</c>.</description></item>
    ///   <item><description><c>&lt;parentTable&gt;_&lt;userColumnName&gt;</c> — autoincrement scalar PK used by Access internally.</description></item>
    /// </list>
    /// <para>
    /// Naming and column ordering match
    /// <c>format-probe-appendix-complex.md</c> for the attachment case
    /// (<c>f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments</c>): the
    /// kind-specific value columns first, then the autoincrement scalar PK,
    /// then the FK back-reference. Three indexes ship with the attachment
    /// table — primary key on the scalar (<c>MSysComplexPKIndex</c>), a
    /// normal index on the FK back-reference (named after the FK column),
    /// and a normal composite index on (FK, FileName) called
    /// <c>IdxFKPrimaryScalar</c> per the appendix.
    /// </para>
    /// <para>
    /// The multi-value variant has no empirical fixture; the conservative
    /// schema mirrors the attachment pattern minus the composite
    /// <c>IdxFKPrimaryScalar</c> (the value column may be a non-indexable
    /// type such as MEMO).
    /// </para>
    /// </remarks>
    private static (ColumnDefinition[] Columns, IndexDefinition[] Indexes) BuildFlatTableSchema(
        string parentTableName,
        ColumnDefinition parentColumn)
    {
        string fkName = $"_{parentColumn.Name}";
        string scalarName = $"{parentTableName}_{parentColumn.Name}";
        var fk = new ColumnDefinition(fkName, typeof(int));
        var scalar = new ColumnDefinition(scalarName, typeof(int)) { IsAutoIncrement = true };

        if (parentColumn.IsAttachment)
        {
            ColumnDefinition[] cols =
            [
                new ColumnDefinition("FileData", typeof(byte[])),
                new ColumnDefinition("FileFlags", typeof(int)),
                new ColumnDefinition("FileName", typeof(string), maxLength: 255),
                new ColumnDefinition("FileTimeStamp", typeof(DateTime)),
                new ColumnDefinition("FileType", typeof(string), maxLength: 255),
                new ColumnDefinition("FileURL", typeof(string)) /* MEMO via no maxLength */,
                scalar,
                fk,
            ];

            IndexDefinition[] indexes =
            [
                new IndexDefinition("MSysComplexPKIndex", scalarName) { IsPrimaryKey = true },
                new IndexDefinition(fkName, fkName),
                new IndexDefinition("IdxFKPrimaryScalar", [fkName, "FileName"]),
            ];

            return (cols, indexes);
        }

        // MultiValue: a single `value` column whose CLR type is the user-declared element type.
        Type elementType = parentColumn.MultiValueElementType
            ?? throw new InvalidOperationException("MultiValueElementType must be set on a multi-value column.");
        var valueCol = new ColumnDefinition("value", elementType, maxLength: parentColumn.MaxLength);
        ColumnDefinition[] mvCols = [valueCol, scalar, fk];
        IndexDefinition[] mvIndexes =
        [
            new IndexDefinition("MSysComplexPKIndex", scalarName) { IsPrimaryKey = true },
            new IndexDefinition(fkName, fkName),
        ];
        return (mvCols, mvIndexes);
    }

    /// <summary>
    /// Inserts one row into <c>MSysComplexColumns</c> linking a parent column's
    /// <see cref="ComplexColumnAllocation.ComplexId"/> to its hidden flat-table TDEF
    /// page. Schema verified in <c>format-probe-appendix-complex.md</c>.
    /// </summary>
    private async ValueTask InsertMSysComplexColumnsRowAsync(
        string parentColumnName,
        int complexId,
        int conceptualTableId,
        int flatTableId,
        int complexTypeObjectId,
        CancellationToken cancellationToken)
    {
        long pg = await FindSystemTableTdefPageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        if (pg == 0)
        {
            throw new InvalidOperationException("MSysComplexColumns table is missing.");
        }

        TableDef msysComplex = await ReadRequiredTableDefAsync(pg, "MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        object[] values = msysComplex.CreateNullValueRow();

        msysComplex.SetValueByName(values, "ColumnName", parentColumnName);
        msysComplex.SetValueByName(values, "ComplexTypeObjectID", complexTypeObjectId);
        msysComplex.SetValueByName(values, "FlatTableID", flatTableId);
        msysComplex.SetValueByName(values, "ConceptualTableID", conceptualTableId);
        msysComplex.SetValueByName(values, "ComplexID", complexId);

        await InsertRowDataAsync(pg, msysComplex, values, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    // ── C4 — Row-level APIs for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §2.1 / §2.4 / §3.

    /// <inheritdoc/>
    public ValueTask AddAttachmentAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object> parentRowKey,
        AttachmentInput attachment,
        CancellationToken cancellationToken = default)
    {
#if NET6_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(parentRowKey);
        ArgumentNullException.ThrowIfNull(attachment);
#else
        if (parentRowKey is null)
        {
            throw new ArgumentNullException(nameof(parentRowKey));
        }

        if (attachment is null)
        {
            throw new ArgumentNullException(nameof(attachment));
        }
#endif
        return AddComplexItemCoreAsync(tableName, columnName, parentRowKey, attachment, expectAttachment: true, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask AddMultiValueItemAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object> parentRowKey,
        object value,
        CancellationToken cancellationToken = default)
    {
#if NET6_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(parentRowKey);
#else
        if (parentRowKey is null)
        {
            throw new ArgumentNullException(nameof(parentRowKey));
        }
#endif
        return AddComplexItemCoreAsync(tableName, columnName, parentRowKey, value, expectAttachment: false, cancellationToken);
    }

    private async ValueTask AddComplexItemCoreAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object> parentRowKey,
        object payload,
        bool expectAttachment,
        CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (parentRowKey.Count == 0)
        {
            throw new ArgumentException("At least one key column is required.", nameof(parentRowKey));
        }

        if (_format != DatabaseFormat.AceAccdb)
        {
            throw new NotSupportedException(
                "Complex (Attachment / MultiValue) columns are an Access 2007+ ACE feature; only .accdb databases are supported.");
        }

        // Resolve parent table + complex column.
        CatalogEntry parentEntry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef parentDef = await ReadRequiredTableDefAsync(parentEntry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        ColumnInfo complexCol = parentDef.FindColumn(columnName)
            ?? throw new ArgumentException($"Column '{columnName}' was not found in table '{tableName}'.", nameof(columnName));

        bool isAttachmentCol = complexCol.Type == T_ATTACHMENT;
        bool isMultiValueCol = complexCol.Type == T_COMPLEX;
        if (!isAttachmentCol && !isMultiValueCol)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is not a complex (Attachment / MultiValue) column (type=0x{complexCol.Type:X2}).");
        }

        if (expectAttachment && !isAttachmentCol)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is a MultiValue column; call AddMultiValueItemAsync instead.");
        }

        if (!expectAttachment && !isMultiValueCol)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is an Attachment column; call AddAttachmentAsync instead.");
        }

        // Resolve the hidden flat child table via MSysComplexColumns.
        long flatTdefPage = await ResolveFlatTableTdefPageAsync(columnName, complexCol.Misc, cancellationToken).ConfigureAwait(false);
        if (flatTdefPage <= 0)
        {
            throw new InvalidOperationException(
                $"No MSysComplexColumns row was found for column '{tableName}.{columnName}'.");
        }

        TableDef flatDef = await ReadRequiredTableDefAsync(flatTdefPage, "<flat>", cancellationToken).ConfigureAwait(false);

        // Resolve predicate column ordinals + decode parent key (string-form for comparison).
        var predIndexes = new int[parentRowKey.Count];
        var predValues = new string[parentRowKey.Count];
        int pi = 0;
        foreach (KeyValuePair<string, object> kvp in parentRowKey)
        {
            int idx = parentDef.FindColumnIndex(kvp.Key);
            if (idx < 0)
            {
                throw new ArgumentException($"Column '{kvp.Key}' was not found in table '{tableName}'.", nameof(parentRowKey));
            }

            predIndexes[pi] = idx;
            predValues[pi] = kvp.Value is null or DBNull
                ? string.Empty
                : Convert.ToString(kvp.Value, CultureInfo.InvariantCulture) ?? string.Empty;
            pi++;
        }

        // Locate the unique parent row.
        (long parentPageNumber, int parentRowIndex, int parentRowStart, int parentRowSize) =
            await FindUniqueParentRowAsync(parentEntry.TDefPage, parentDef, predIndexes, predValues, tableName, cancellationToken)
                .ConfigureAwait(false);

        // Read the existing ConceptualTableID from the parent row's complex slot;
        // allocate a fresh one when the slot is null.
        int conceptualTableId = await ReadOrAllocateConceptualTableIdAsync(
            parentPageNumber,
            parentRowStart,
            parentRowSize,
            parentRowIndex,
            complexCol,
            flatTdefPage,
            flatDef,
            cancellationToken).ConfigureAwait(false);

        // Build the flat-table row values.
        object[] flatValues = expectAttachment
            ? BuildAttachmentFlatRow(flatDef, conceptualTableId, (AttachmentInput)payload)
            : BuildMultiValueFlatRow(flatDef, conceptualTableId, payload);

        // C7: the flat table now carries an autoincrement scalar PK column.
        // ApplyConstraintsAsync hydrates the constraint registry from the
        // persisted FLAG_AUTO_LONG bit and seeds the next value from the
        // existing rows so AddAttachmentAsync / AddMultiValueItemAsync stay
        // a single-call surface.
        string flatTableName = await ResolveFlatTableNameAsync(flatTdefPage, cancellationToken).ConfigureAwait(false);
        await ApplyConstraintsAsync(flatTableName, flatDef, flatValues, cancellationToken).ConfigureAwait(false);

        await InsertRowDataAsync(flatTdefPage, flatDef, flatValues, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves a hidden flat-child-table TDEF page back to its
    /// <c>MSysObjects.Name</c>. Used by <see cref="AddComplexItemCoreAsync"/>
    /// to drive <see cref="ApplyConstraintsAsync"/> for the autoincrement
    /// scalar PK column emitted by Phase C7.
    /// </summary>
    private async ValueTask<string> ResolveFlatTableNameAsync(long flatTdefPage, CancellationToken cancellationToken)
    {
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            throw new InvalidOperationException("MSysObjects catalog table is missing.");
        }

        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage == flatTdefPage)
            {
                return row.Name;
            }
        }

        throw new InvalidOperationException(
            $"No MSysObjects row was found for flat-child TDEF page {flatTdefPage}.");
    }

    /// <summary>
    /// Looks up <c>MSysComplexColumns</c> for a row matching both
    /// <paramref name="columnName"/> and <paramref name="complexId"/> and returns
    /// the lower-24-bit TDEF page number of the hidden flat child table.
    /// </summary>
    private async ValueTask<long> ResolveFlatTableTdefPageAsync(string columnName, int complexId, CancellationToken cancellationToken)
    {
        long msysPg = await FindSystemTableTdefPageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        if (msysPg == 0)
        {
            return 0;
        }

        TableDef msys = await ReadRequiredTableDefAsync(msysPg, "MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msys.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msys.FindColumn("FlatTableID");
        ColumnInfo? complexIdCol = msys.FindColumn("ComplexID");
        if (nameCol == null || flatIdCol == null || complexIdCol == null)
        {
            return 0;
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

                if (Ri32(page, _dpTDefOff) != msysPg)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = ReadColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = ReadColumnValue(page, row.RowStart, row.RowSize, complexIdCol);
                    if (complexId != 0 && (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId))
                    {
                        continue;
                    }

                    string flatText = ReadColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
                    if (long.TryParse(flatText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long flatId))
                    {
                        return flatId & 0x00FFFFFFL;
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return 0;
    }

    private async ValueTask<(long PageNumber, int RowIndex, int RowStart, int RowSize)> FindUniqueParentRowAsync(
        long parentTdefPage,
        TableDef parentDef,
        int[] predIndexes,
        string[] predValues,
        string tableName,
        CancellationToken cancellationToken)
    {
        (long, int, int, int) match = default;
        bool found = false;

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

                if (Ri32(page, _dpTDefOff) != parentTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    bool ok = true;
                    for (int p = 0; p < predIndexes.Length; p++)
                    {
                        ColumnInfo c = parentDef.Columns[predIndexes[p]];
                        string actual = ReadColumnValue(page, row.RowStart, row.RowSize, c);
                        if (!string.Equals(actual, predValues[p], StringComparison.OrdinalIgnoreCase))
                        {
                            ok = false;
                            break;
                        }
                    }

                    if (!ok)
                    {
                        continue;
                    }

                    if (found)
                    {
                        throw new InvalidOperationException(
                            $"Parent row key matches more than one row in '{tableName}'.");
                    }

                    match = (row.PageNumber, row.RowIndex, row.RowStart, row.RowSize);
                    found = true;
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        if (!found)
        {
            throw new InvalidOperationException($"No row in '{tableName}' matches the supplied parent row key.");
        }

        return match;
    }

    private async ValueTask<int> ReadOrAllocateConceptualTableIdAsync(
        long parentPageNumber,
        int parentRowStart,
        int parentRowSize,
        int parentRowIndex,
        ColumnInfo complexCol,
        long flatTdefPage,
        TableDef flatDef,
        CancellationToken cancellationToken)
    {
        // Re-read parent page to inspect the complex slot null bit + 4 bytes.
        byte[] page = await ReadPageAsync(parentPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            int numCols = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, parentRowStart) : page[parentRowStart];
            int nullMaskSz = (numCols + 7) / 8;
            int nullMaskPos = parentRowSize - nullMaskSz;
            int byteOff = nullMaskPos + (complexCol.ColNum / 8);
            int bitOff = complexCol.ColNum % 8;
            bool slotSet = byteOff < parentRowSize && (page[parentRowStart + byteOff] & (1 << bitOff)) != 0;

            int slotOff = parentRowStart + _numColsFldSz + complexCol.FixedOff;
            if (slotSet && slotOff + 4 <= parentRowStart + parentRowSize)
            {
                int existing = Ri32(page, slotOff);
                if (existing > 0)
                {
                    return existing;
                }
            }
        }
        finally
        {
            ReturnPage(page);
        }

        // Allocate a fresh ConceptualTableID by scanning the flat table for max(FK)+1.
        int allocated = await GetNextConceptualTableIdForFlatAsync(flatTdefPage, flatDef, cancellationToken).ConfigureAwait(false);

        // Patch the parent row's complex slot in place: 4 bytes + null-mask bit.
        await PatchParentComplexSlotAsync(parentPageNumber, parentRowStart, parentRowSize, complexCol, allocated, cancellationToken).ConfigureAwait(false);
        return allocated;
    }

    private async ValueTask PatchParentComplexSlotAsync(
        long pageNumber,
        int rowStart,
        int rowSize,
        ColumnInfo complexCol,
        int conceptualTableId,
        CancellationToken cancellationToken)
    {
        byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            int numCols = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, rowStart) : page[rowStart];
            int nullMaskSz = (numCols + 7) / 8;
            int nullMaskPos = rowSize - nullMaskSz;
            int slotOff = rowStart + _numColsFldSz + complexCol.FixedOff;
            if (slotOff + 4 > rowStart + rowSize)
            {
                throw new InvalidDataException("Complex column slot is out of row bounds.");
            }

            Wi32(page, slotOff, conceptualTableId);
            int byteOff = nullMaskPos + (complexCol.ColNum / 8);
            int bitOff = complexCol.ColNum % 8;
            if (byteOff < rowSize)
            {
                page[rowStart + byteOff] |= (byte)(1 << bitOff);
            }

            await WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(page);
        }
    }

    /// <summary>
    /// Returns one greater than the largest FK value currently stored in the
    /// flat table, or <c>1</c> when the table is empty. The FK column is the
    /// single <c>T_LONG</c> column whose name starts with <c>"_"</c> per
    /// C3's <see cref="BuildFlatTableSchema"/>.
    /// </summary>
    private async ValueTask<int> GetNextConceptualTableIdForFlatAsync(long flatTdefPage, TableDef flatDef, CancellationToken cancellationToken)
    {
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();

        int maxId = 0;
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

                if (Ri32(page, _dpTDefOff) != flatTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    string text = ReadColumnValue(page, row.RowStart, row.RowSize, fkCol);
                    if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) && v > maxId)
                    {
                        maxId = v;
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return maxId + 1;
    }

    private static object[] BuildAttachmentFlatRow(TableDef flatDef, int conceptualTableId, AttachmentInput input)
    {
        object[] values = flatDef.CreateNullValueRow();

        // FK back-ref: the single T_LONG column starting with "_".
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();
        values[flatDef.Columns.IndexOf(fkCol)] = conceptualTableId;

        string ext = input.FileType ?? DeriveExtension(input.FileName);
        byte[] wrapped = AttachmentWrapper.Encode(ext, input.FileData);

        flatDef.SetValueByName(values, "FileURL", (object?)input.FileURL ?? DBNull.Value);
        flatDef.SetValueByName(values, "FileName", input.FileName);
        flatDef.SetValueByName(values, "FileType", ext);
        flatDef.SetValueByName(values, "FileFlags", DBNull.Value);
        flatDef.SetValueByName(values, "FileTimeStamp", input.FileTimeStamp ?? DateTime.UtcNow);
        flatDef.SetValueByName(values, "FileData", wrapped);
        return values;
    }

    private static object[] BuildMultiValueFlatRow(TableDef flatDef, int conceptualTableId, object value)
    {
        object[] values = flatDef.CreateNullValueRow();
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();
        values[flatDef.Columns.IndexOf(fkCol)] = conceptualTableId;
        flatDef.SetValueByName(values, "value", value ?? DBNull.Value);
        return values;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase", Justification = "Access stores attachment file extensions in lowercase, matching Jackcess.")]
    private static string DeriveExtension(string fileName)
    {
        if (string.IsNullOrEmpty(fileName))
        {
            return string.Empty;
        }

        int dot = fileName.LastIndexOf('.');
        if (dot < 0 || dot == fileName.Length - 1)
        {
            return string.Empty;
        }

        return fileName.Substring(dot + 1).ToLowerInvariant();
    }

    // ── C5 — Cascade-on-delete for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §4.3 (C5).
    //
    // Whenever a parent row containing a complex column slot is deleted, the
    // associated rows in the hidden flat child table (joined via the parent's
    // 4-byte ConceptualTableID slot) must also be deleted. Without this pass
    // the flat table accumulates orphaned rows, breaks referential integrity
    // expected by Microsoft Access, and may cause Compact &amp; Repair to flag
    // the file.

    /// <summary>
    /// Cascades a pending delete of <paramref name="deletedParentLocations"/>
    /// rows in <paramref name="parentDef"/> to the hidden flat child tables
    /// of every Attachment / MultiValue column on the parent. Must be called
    /// BEFORE the parent rows are marked deleted, since the per-row
    /// <c>ConceptualTableID</c> slot value is needed to identify which flat
    /// rows to delete.
    /// </summary>
    /// <remarks>
    /// Per-flat-table cost is O(P) where P is the database page count
    /// (full sequential scan, no index seek). Multiple complex columns on
    /// the same parent perform one scan each. This matches the existing
    /// W10 cascade-delete cost profile and the C4 ConceptualTableID
    /// allocator.
    /// </remarks>
    private async ValueTask CascadeDeleteComplexChildrenAsync(
        TableDef parentDef,
        IReadOnlyList<RowLocation> deletedParentLocations,
        CancellationToken cancellationToken)
    {
        if (deletedParentLocations.Count == 0)
        {
            return;
        }

        // Identify complex columns on the parent.
        var complexCols = new List<ColumnInfo>();
        foreach (ColumnInfo col in parentDef.Columns)
        {
            if (col.Type == T_ATTACHMENT || col.Type == T_COMPLEX)
            {
                complexCols.Add(col);
            }
        }

        if (complexCols.Count == 0)
        {
            return;
        }

        // Resolve each complex column to its flat-table TDEF page (skip
        // any column whose MSysComplexColumns row is missing — same
        // tolerance as the C4 row-add path).
        var flatPagesByCol = new Dictionary<int, long>(complexCols.Count);
        foreach (ColumnInfo col in complexCols)
        {
            long flatPg = await ResolveFlatTableTdefPageAsync(col.Name, col.Misc, cancellationToken).ConfigureAwait(false);
            if (flatPg > 0)
            {
                flatPagesByCol[col.ColNum] = flatPg;
            }
        }

        if (flatPagesByCol.Count == 0)
        {
            return;
        }

        // Read each parent row to collect the live ConceptualTableID per
        // complex column. Rows whose complex slot is null contribute
        // nothing to cascade.
        var idsByCol = new Dictionary<int, HashSet<int>>(complexCols.Count);
        foreach (ColumnInfo col in complexCols)
        {
            if (flatPagesByCol.ContainsKey(col.ColNum))
            {
                idsByCol[col.ColNum] = new HashSet<int>();
            }
        }

        foreach (RowLocation loc in deletedParentLocations)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await ReadPageAsync(loc.PageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                int numCols = _format != DatabaseFormat.Jet3Mdb ? Ru16(page, loc.RowStart) : page[loc.RowStart];
                int nullMaskSz = (numCols + 7) / 8;
                int nullMaskPos = loc.RowSize - nullMaskSz;

                foreach (ColumnInfo col in complexCols)
                {
                    if (!idsByCol.TryGetValue(col.ColNum, out HashSet<int>? ids))
                    {
                        continue;
                    }

                    int byteOff = nullMaskPos + (col.ColNum / 8);
                    int bitOff = col.ColNum % 8;
                    bool slotSet = byteOff < loc.RowSize
                        && (page[loc.RowStart + byteOff] & (1 << bitOff)) != 0;
                    if (!slotSet)
                    {
                        continue;
                    }

                    int slotOff = loc.RowStart + _numColsFldSz + col.FixedOff;
                    if (slotOff + 4 > loc.RowStart + loc.RowSize)
                    {
                        continue;
                    }

                    int ctid = Ri32(page, slotOff);
                    if (ctid > 0)
                    {
                        _ = ids.Add(ctid);
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        // For each complex column with collected IDs, scan the flat
        // child table once and delete every row whose FK back-reference
        // is in the set. Adjust the flat TDEF row count once.
        foreach (ColumnInfo col in complexCols)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!flatPagesByCol.TryGetValue(col.ColNum, out long flatTdefPage))
            {
                continue;
            }

            HashSet<int> ids = idsByCol[col.ColNum];
            if (ids.Count == 0)
            {
                continue;
            }

            TableDef flatDef = await ReadRequiredTableDefAsync(flatTdefPage, "<flat>", cancellationToken).ConfigureAwait(false);
            ColumnInfo? fkCol = flatDef.Columns.Find(c => c.Type == T_LONG && c.Name.StartsWith('_'))
                ?? flatDef.Columns.Find(c => c.Type == T_LONG);
            if (fkCol == null)
            {
                continue;
            }

            int deletedFromFlat = 0;
            long total = _stream.Length / _pgSz;
            for (long pageNumber = 3; pageNumber < total; pageNumber++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var rowsToDelete = new List<int>();
                byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (page[0] != 0x01)
                    {
                        continue;
                    }

                    if (Ri32(page, _dpTDefOff) != flatTdefPage)
                    {
                        continue;
                    }

                    foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                    {
                        string fkText = ReadColumnValue(page, row.RowStart, row.RowSize, fkCol);
                        if (int.TryParse(fkText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int fk)
                            && ids.Contains(fk))
                        {
                            rowsToDelete.Add(row.RowIndex);
                        }
                    }
                }
                finally
                {
                    ReturnPage(page);
                }

                foreach (int rowIdx in rowsToDelete)
                {
                    await MarkRowDeletedAsync(pageNumber, rowIdx, cancellationToken).ConfigureAwait(false);
                    deletedFromFlat++;
                }
            }

            if (deletedFromFlat > 0)
            {
                await AdjustTDefRowCountAsync(flatTdefPage, -deletedFromFlat, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Shared implementation backing <see cref="DropTableAsync"/> and the
    /// <c>RewriteTableAsync</c> path. The <paramref name="dropComplexChildren"/>
    /// flag is set to <see langword="false"/> by the rewrite path so that the
    /// hidden flat child tables and matching <c>MSysComplexColumns</c> rows for
    /// surviving complex columns stay attached to the rebuilt parent
    /// (Phase C9 — schema evolution on tables containing complex columns).
    /// </summary>
    private async ValueTask DropTableCoreAsync(string tableName, bool dropComplexChildren, CancellationToken cancellationToken)
    {
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist.");
        }

        int deleted = 0;
        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        var droppedTdefPages = new List<long>();
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

            if (row.TDefPage > 0)
            {
                droppedTdefPages.Add(row.TDefPage);
            }

            await MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted == 0)
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist.");
        }

        if (dropComplexChildren)
        {
            foreach (long parentTdefPage in droppedTdefPages)
            {
                await DropComplexChildrenForTableAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
            }
        }

        UnregisterConstraints(tableName);
        InvalidateCatalogCache();
    }

    /// <summary>
    /// Phase C9 — surgically drops a single complex column's flat child table
    /// and its <c>MSysComplexColumns</c> row, identified by
    /// <paramref name="columnName"/> + <paramref name="complexId"/>. Used by the
    /// rewrite path when the user calls <c>DropColumnAsync</c> on an
    /// attachment / multi-value column. Returns silently if no matching row is
    /// found (idempotent).
    /// </summary>
    private async ValueTask DropSingleComplexChildAsync(string columnName, int complexId, CancellationToken cancellationToken)
    {
        long msysCxPg = await FindSystemTableTdefPageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await ReadRequiredTableDefAsync(msysCxPg, "MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msysCxDef.FindColumn("FlatTableID");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (nameCol == null || flatIdCol == null || cxIdCol == null)
        {
            return;
        }

        long flatTdefPage = 0;
        var deletedRows = new List<(long PageNumber, int RowIndex)>();

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

                if (Ri32(page, _dpTDefOff) != msysCxPg)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = ReadColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = ReadColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId)
                    {
                        continue;
                    }

                    string flatText = ReadColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
                    if (long.TryParse(flatText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long fid))
                    {
                        flatTdefPage = fid & 0x00FFFFFFL;
                    }

                    deletedRows.Add((row.PageNumber, row.RowIndex));
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        foreach ((long pg, int ri) in deletedRows)
        {
            await MarkRowDeletedAsync(pg, ri, cancellationToken).ConfigureAwait(false);
        }

        if (deletedRows.Count > 0)
        {
            await AdjustTDefRowCountAsync(msysCxPg, -deletedRows.Count, cancellationToken).ConfigureAwait(false);
        }

        if (flatTdefPage <= 0)
        {
            return;
        }

        // Drop the hidden flat-table catalog row. Same model as
        // DropComplexChildrenForTableAsync — orphaned data pages are reclaimed
        // by Access on the next Compact &amp; Repair pass.
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return;
        }

        List<CatalogRow> catalog = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in catalog)
        {
            if (row.ObjectType != OBJ_TABLE)
            {
                continue;
            }

            if (row.TDefPage == flatTdefPage)
            {
                await MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Phase C9 — when the user renames a complex column, rewrite the
    /// matching <c>MSysComplexColumns</c> row's <c>ColumnName</c> field. The
    /// hidden flat child table's catalog name (<c>f_&lt;hex&gt;_&lt;oldName&gt;</c>)
    /// is left unchanged because it is opaque to readers — they resolve the
    /// flat name via <c>FlatTableID</c> → <c>MSysObjects</c>. This mirrors the
    /// W14 <c>RenameRelationshipAsync</c> trade-off that leaves TDEF logical-idx
    /// name cookies stale until Compact &amp; Repair.
    /// </summary>
    private async ValueTask RenameComplexColumnArtifactsAsync(string oldColumnName, string newColumnName, int complexId, CancellationToken cancellationToken)
    {
        long msysCxPg = await FindSystemTableTdefPageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await ReadRequiredTableDefAsync(msysCxPg, "MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (nameCol == null || cxIdCol == null)
        {
            return;
        }

        var matched = new List<(long PageNumber, int RowIndex, object[] Values)>();
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

                if (Ri32(page, _dpTDefOff) != msysCxPg)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = ReadColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, oldColumnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = ReadColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId)
                    {
                        continue;
                    }

                    var values = new object[msysCxDef.Columns.Count];
                    for (int i = 0; i < values.Length; i++)
                    {
                        string text = ReadColumnValue(page, row.RowStart, row.RowSize, msysCxDef.Columns[i]);
                        values[i] = string.IsNullOrEmpty(text) ? DBNull.Value : text;
                    }

                    msysCxDef.SetValueByName(values, "ColumnName", newColumnName);
                    matched.Add((row.PageNumber, row.RowIndex, values));
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        foreach ((long pg, int ri, object[] _) in matched)
        {
            await MarkRowDeletedAsync(pg, ri, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long _, int _, object[] values) in matched)
        {
            _ = await InsertRowDataLocAsync(msysCxPg, msysCxDef, values, updateTDefRowCount: false, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Phase C6 — when dropping a parent table, also drop the hidden flat
    /// child tables backing each Attachment / MultiValue column on the
    /// parent and remove the corresponding rows from
    /// <c>MSysComplexColumns</c>. Tolerates missing
    /// <c>MSysComplexColumns</c> (Jet3 / Jet4 / fresh writer-created
    /// ACCDB without the system table) and missing catalog rows for a
    /// flat table (already removed) by silently skipping.
    /// </summary>
    private async ValueTask DropComplexChildrenForTableAsync(long parentTdefPage, CancellationToken cancellationToken)
    {
        TableDef? parentDef = await ReadTableDefAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
        if (parentDef == null)
        {
            return;
        }

        var complexCols = new List<ColumnInfo>();
        foreach (ColumnInfo col in parentDef.Columns)
        {
            if (col.Type == T_ATTACHMENT || col.Type == T_COMPLEX)
            {
                complexCols.Add(col);
            }
        }

        if (complexCols.Count == 0)
        {
            return;
        }

        long msysCxPg = await FindSystemTableTdefPageAsync("MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await ReadRequiredTableDefAsync(msysCxPg, "MSysComplexColumns", cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msysCxDef.FindColumn("FlatTableID");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (nameCol == null || flatIdCol == null || cxIdCol == null)
        {
            return;
        }

        var lookup = new Dictionary<string, HashSet<int>>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnInfo col in complexCols)
        {
            if (!lookup.TryGetValue(col.Name, out HashSet<int>? ids))
            {
                ids = new HashSet<int>();
                lookup[col.Name] = ids;
            }

            _ = ids.Add(col.Misc);
        }

        var flatTdefPages = new HashSet<long>();
        var cxRowsToDelete = new List<(long PageNumber, int RowIndex)>();

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

                if (Ri32(page, _dpTDefOff) != msysCxPg)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = ReadColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    string idText = ReadColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid))
                    {
                        continue;
                    }

                    if (!lookup.TryGetValue(rowName, out HashSet<int>? expectedIds) || !expectedIds.Contains(rid))
                    {
                        continue;
                    }

                    string flatText = ReadColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
                    if (long.TryParse(flatText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long flatId))
                    {
                        _ = flatTdefPages.Add(flatId & 0x00FFFFFFL);
                    }

                    cxRowsToDelete.Add((row.PageNumber, row.RowIndex));
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        foreach ((long pg, int ri) in cxRowsToDelete)
        {
            await MarkRowDeletedAsync(pg, ri, cancellationToken).ConfigureAwait(false);
        }

        if (cxRowsToDelete.Count > 0)
        {
            await AdjustTDefRowCountAsync(msysCxPg, -cxRowsToDelete.Count, cancellationToken).ConfigureAwait(false);
        }

        if (flatTdefPages.Count == 0)
        {
            return;
        }

        // Drop the hidden flat-table catalog rows (system-flag tables —
        // public DropTableAsync would skip them).
        TableDef? msys = await ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return;
        }

        List<CatalogRow> catalog = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in catalog)
        {
            if (row.ObjectType != OBJ_TABLE)
            {
                continue;
            }

            if (flatTdefPages.Contains(row.TDefPage))
            {
                await MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            }
        }
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
        _ = await InsertRowDataLocAsync(tdefPage, tableDef, values, updateTDefRowCount, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts a row and returns its (page, row-index) location so the caller
    /// can mark it deleted if a subsequent step (e.g. unique-index rebuild)
    /// fails. Mirrors <see cref="InsertRowDataAsync"/> but exposes the
    /// <see cref="RowLocation"/> of the freshly written row.
    /// </summary>
    private async ValueTask<RowLocation> InsertRowDataLocAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default)
    {
        if (values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException(
                $"Expected {tableDef.Columns.Count} values for table row but received {values.Length}.",
                nameof(values));
        }

        // C8 \u2014 push any oversized MEMO / OLE / Attachment payload to LVAL pages
        // before serializing the row. The pre-encode pass appends LVAL pages to
        // the file and rewrites the matching slot in `values` with a
        // PreEncodedLongValue sentinel carrying the finished 12-byte header.
        values = await PreEncodeLongValuesAsync(tableDef, values, cancellationToken).ConfigureAwait(false);

        byte[] rowBytes = SerializeRow(tableDef, values);
        PageInsertTarget target = await FindInsertTargetAsync(tdefPage, rowBytes.Length, cancellationToken).ConfigureAwait(false);
        int rowIndex;
        int rowStart;
        try
        {
            rowIndex = Ru16(target.Page, _dpNumRows);
            rowStart = GetFirstRowStart(target.Page, rowIndex) - rowBytes.Length;
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

        return new RowLocation(target.PageNumber, rowIndex, rowStart, rowBytes.Length);
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

        // JET row IDs encode the per-page row index as a single byte (0..255),
        // so a data page can hold at most 256 distinct row slots. Capping at
        // 255 matches Jackcess and keeps the (byte) cast in the index-rebuild
        // path safe under <CheckForOverflowUnderflow>true.
        if (numRows >= MaxRowsPerDataPage)
        {
            return false;
        }

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

                int fixedSize = FixedSize(column.Type, column.Size);
                if (fixedSize <= 0)
                {
                    continue;
                }

                int written = TryEncodeFixedValue(column, value, fixedArea.AsSpan(column.FixedOff, fixedSize));
                if (written == 0)
                {
                    continue;
                }

                fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + written);
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

    private int TryEncodeFixedValue(ColumnInfo column, object value, Span<byte> dest)
    {
        switch (column.Type)
        {
            case T_BYTE:
                dest[0] = Convert.ToByte(value, CultureInfo.InvariantCulture);
                return 1;

            case T_INT:
                BinaryPrimitives.WriteInt16LittleEndian(dest, Convert.ToInt16(value, CultureInfo.InvariantCulture));
                return 2;

            case T_LONG:
                BinaryPrimitives.WriteInt32LittleEndian(dest, Convert.ToInt32(value, CultureInfo.InvariantCulture));
                return 4;

            case T_FLOAT:
                BinaryPrimitives.WriteInt32LittleEndian(
                    dest,
                    BitConverter.SingleToInt32Bits(Convert.ToSingle(value, CultureInfo.InvariantCulture)));
                return 4;

            case T_DOUBLE:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    BitConverter.DoubleToInt64Bits(Convert.ToDouble(value, CultureInfo.InvariantCulture)));
                return 8;

            case T_DATETIME:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    BitConverter.DoubleToInt64Bits(Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToOADate()));
                return 8;

            case T_MONEY:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    decimal.ToOACurrency(Convert.ToDecimal(value, CultureInfo.InvariantCulture)));
                return 8;

            case T_NUMERIC:
                EncodeNumericValue(Convert.ToDecimal(value, CultureInfo.InvariantCulture), dest);
                return 17;

            case T_GUID:
                {
                    Guid g = value is Guid guid
                        ? guid
                        : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!);
                    if (!g.TryWriteBytes(dest))
                    {
                        return 0;
                    }

                    return 16;
                }

            default:
                return 0;
        }
    }

    private byte[]? EncodeVariableValue(ColumnInfo column, object value)
    {
        switch (column.Type)
        {
            case T_TEXT:
                return EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.Size);
            case T_BINARY:
                return EncodeBinaryValue(value, column.Size);
            case T_MEMO:
                if (value is PreEncodedLongValue preMemo)
                {
                    return preMemo.HeaderBytes;
                }

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

    private void EncodeNumericValue(decimal value, Span<byte> dest)
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

        dest[0] = precision;
        dest[1] = scale;
        dest[2] = negative ? (byte)1 : (byte)0;
        dest[3] = 0;

        BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(4, 4), bits[0]);
        BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(8, 4), bits[1]);
        BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(12, 4), bits[2]);
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
        ColumnInfo? idColumn = msys.FindColumn("Id");
        ColumnInfo? nameColumn = msys.FindColumn("Name");
        ColumnInfo? typeColumn = msys.FindColumn("Type");
        ColumnInfo? flagsColumn = msys.FindColumn("Flags");
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
                    Flags = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, flagsColumn!)),
                    TDefPage = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, idColumn!)) & 0x00FFFFFFL,
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

        if (!TryParseRowLayout(page, rowStart, rowSize, hasVarColumns: true, out RowLayout layout))
        {
            return string.Empty;
        }

        ColumnSlice slice = ResolveColumnSlice(page, rowStart, rowSize, layout, column);
        switch (slice.Kind)
        {
            case ColumnSliceKind.Bool:
                return slice.BoolValue ? "True" : "False";

            case ColumnSliceKind.Null:
            case ColumnSliceKind.Empty:
                return string.Empty;

            case ColumnSliceKind.Fixed:
                return ReadFixedString(page, rowStart + slice.DataStart, column.Type, slice.DataLen);

            case ColumnSliceKind.Var:
                if (slice.DataLen <= 0)
                {
                    return string.Empty;
                }

                switch (column.Type)
                {
                    case T_TEXT:
                        return _format != DatabaseFormat.Jet3Mdb
                            ? DecodeJet4Text(page, rowStart + slice.DataStart, slice.DataLen)
                            : _ansiEncoding.GetString(page, rowStart + slice.DataStart, slice.DataLen);
                    case T_BINARY:
                        return BitConverter.ToString(page, rowStart + slice.DataStart, slice.DataLen);
                    default:
                        return string.Empty;
                }

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
        const int LogIdxEntrySz = 28; // Jet4/ACE only.
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
        int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);

        // W11: per-real-idx, decode every populated col_map slot to recover the
        // full (column, direction) list, the first_dp byte offset, and the unique
        // flag bit (real-idx flags & 0x01). PK uniqueness is signalled by the
        // logical-idx index_type discriminator below, not the flag bit.
        var realIdxByNum = new Dictionary<int, RealIdxEntry>();
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * RealIdxPhysSz);
            if (phys + RealIdxPhysSz > tdefBuffer.Length)
            {
                break;
            }

            var keyCols = new List<(int ColNum, bool Ascending)>(10);
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(tdefBuffer, so);
                if (cn == 0xFFFF)
                {
                    continue;
                }

                byte order = tdefBuffer[so + 2];
                keyCols.Add((cn, order != 0x02));
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            byte flagsByte = tdefBuffer[phys + 42];
            realIdxByNum[ri] = new RealIdxEntry(keyCols, phys + 4 + 30 + 4, (flagsByte & 0x01) != 0);
        }

        if (realIdxByNum.Count == 0)
        {
            return;
        }

        // Walk logical-idx entries to discover whether any logical idx pointing at
        // a given real-idx is a primary key (index_type = 0x01) — that promotes
        // the real-idx to unique even when the flags byte is 0x00 (per §3.1).
        for (int li = 0; li < numIdx; li++)
        {
            int entryStart = logIdxStart + (li * LogIdxEntrySz);
            if (entryStart + LogIdxEntrySz > tdefBuffer.Length)
            {
                break;
            }

            int realIdxNum = Ri32(tdefBuffer, entryStart + 8);
            byte indexType = tdefBuffer[entryStart + 23];
            if (indexType == (byte)IndexKind.PrimaryKey
                && realIdxByNum.TryGetValue(realIdxNum, out RealIdxEntry rie))
            {
                realIdxByNum[realIdxNum] = rie with { IsUnique = true };
            }
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
        foreach (KeyValuePair<int, RealIdxEntry> kvp in realIdxByNum)
        {
            cancellationToken.ThrowIfCancellationRequested();

            RealIdxEntry rie = kvp.Value;

            // Resolve every key column up front; if any column is missing from the
            // snapshot (deleted-column gap), skip rebuild for this index.
            var keyColInfos = new List<(ColumnInfo Col, int SnapIdx, bool Ascending)>(rie.KeyColumns.Count);
            bool resolveFailed = false;
            foreach ((int colNum, bool ascending) in rie.KeyColumns)
            {
                ColumnInfo? col = tableDef.Columns.Find(c => c.ColNum == colNum);
                if (col is null || !snapshotIndexByColNum.TryGetValue(colNum, out int snapIdx))
                {
                    resolveFailed = true;
                    break;
                }

                keyColInfos.Add((col, snapIdx, ascending));
            }

            if (resolveFailed)
            {
                continue;
            }

            // W13: pre-pass to compute the per-column canonical scale for any
            // T_NUMERIC key column. All snapshot values must be encoded with
            // the same scale to be byte-comparable (Jackcess uses the column's
            // declared scale; we don't track it in ColumnInfo, so we use the
            // max natural scale present in the snapshot). Non-numeric columns
            // get -1 as a sentinel.
            int[] numericTargetScales = new int[keyColInfos.Count];
            for (int k = 0; k < keyColInfos.Count; k++)
            {
                if (keyColInfos[k].Col.Type == T_NUMERIC)
                {
                    int kSnap = keyColInfos[k].SnapIdx;
                    int max = 0;
                    for (int r = 0; r < rowCount; r++)
                    {
                        object cell = snapshot.Rows[r][kSnap];
                        if (cell is DBNull)
                        {
                            continue;
                        }

                        decimal dv = Convert.ToDecimal(cell, CultureInfo.InvariantCulture);
                        int s = (decimal.GetBits(dv)[3] >> 16) & 0x7F;
                        if (s > max)
                        {
                            max = s;
                        }
                    }

                    numericTargetScales[k] = max;
                }
                else
                {
                    numericTargetScales[k] = -1;
                }
            }

            bool legacyNumeric = _format == DatabaseFormat.Jet4Mdb;

            var entries = new List<(byte[] Key, long Page, byte Row)>(rowCount);
            bool encoderRejected = false;
            for (int r = 0; r < rowCount; r++)
            {
                byte[][] perColumn = new byte[keyColInfos.Count][];
                int totalLen = 0;
                try
                {
                    for (int k = 0; k < keyColInfos.Count; k++)
                    {
                        (ColumnInfo col, int snapIdx, bool ascending) = keyColInfos[k];
                        object cell = snapshot.Rows[r][snapIdx];
                        object? value = cell is DBNull ? null : cell;
                        perColumn[k] = col.Type == T_NUMERIC
                            ? IndexKeyEncoder.EncodeNumericEntry(value, ascending, numericTargetScales[k], legacyNumeric)
                            : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
                        totalLen += perColumn[k].Length;
                    }
                }
                catch (NotSupportedException)
                {
                    // Unsupported key type for one of the columns. Leave first_dp
                    // pointing at the existing (now-stale) leaf — same behaviour
                    // as before W5 shipped.
                    encoderRejected = true;
                    break;
                }

                byte[] composite = new byte[totalLen];
                int offset = 0;
                for (int k = 0; k < perColumn.Length; k++)
                {
                    Buffer.BlockCopy(perColumn[k], 0, composite, offset, perColumn[k].Length);
                    offset += perColumn[k].Length;
                }

                entries.Add((composite, locations[r].PageNumber, (byte)locations[r].RowIndex));
            }

            if (encoderRejected)
            {
                continue;
            }

            entries.Sort(static (a, b) => CompareKeyBytes(a.Key, b.Key));

            // W11: unique-violation detection. Note this is a post-write check —
            // the offending row has already been persisted by the time we get
            // here, so throwing leaves the table in a state where the row exists
            // but the index is stale. The caller is expected to delete the
            // duplicate row (or restore from a backup) before continuing.
            if (rie.IsUnique)
            {
                for (int e = 1; e < entries.Count; e++)
                {
                    if (CompareKeyBytes(entries[e - 1].Key, entries[e].Key) == 0)
                    {
                        throw new InvalidOperationException(
                            $"Unique index violation on table '{tableName}': duplicate key detected after row mutation. " +
                            "The duplicate row has been written but the index B-tree was not rebuilt; " +
                            "remove one of the offending rows and retry the operation.");
                    }
                }
            }

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

    /// <summary>
    /// W4-C-1 / W4-C-2 fast path: when the change since the previous index
    /// state is a small set of inserted and/or deleted rows AND every real-idx
    /// can be maintained without rereading the table snapshot, splice the
    /// change into each index in place rather than rebuilding the whole
    /// B-tree from a snapshot. Returns
    /// <see langword="true"/> when every supported real-idx was maintained
    /// incrementally; the caller MUST then NOT call
    /// <see cref="MaintainIndexesAsync"/>. Returns <see langword="false"/>
    /// when any index can't be served by the fast path — the caller must
    /// fall back to <see cref="MaintainIndexesAsync"/>, which will rebuild
    /// every index from a fresh snapshot (any incremental work this method
    /// already wrote is harmless: the orphaned pages are reclaimed by Access
    /// on Compact &amp; Repair, exactly like the bulk-rebuild path's own
    /// orphans).
    /// <para>
    /// Two flavours of fast path are attempted per real-idx:
    /// </para>
    /// <list type="bullet">
    ///   <item><b>Single-leaf splice (§7 W4-C-1 / W4-C-2).</b> Root is a leaf
    ///   (<c>page_type = 0x04</c>) with no sibling pointers AND the
    ///   post-mutation entry list still fits on one page. The leaf is
    ///   decoded, spliced, and re-emitted as a single page; <c>first_dp</c>
    ///   is patched to the new leaf.</item>
    ///   <item><b>Multi-level rebuild from existing tree (§7 W4 sub-phase D).</b>
    ///   Root is an intermediate (<c>0x03</c>) page. We descend to the
    ///   leftmost leaf, walk the leaf-sibling chain to collect every entry,
    ///   splice the change-set in, and rebuild a fresh B-tree via
    ///   <see cref="IndexBTreeBuilder"/>; <c>first_dp</c> is patched to the
    ///   new root. This avoids the bulk path's full table-snapshot read +
    ///   per-row key re-encode while still propagating leaf splits / merges
    ///   correctly through any number of intermediate levels.</item>
    /// </list>
    /// <para>
    /// Falls back when: format is Jet3 (no index emission); no indexes are
    /// declared; any index has a multi-page TDEF; any key column is
    /// <c>T_NUMERIC</c> (the W13 canonical-scale pre-pass needs a full
    /// snapshot); the encoder rejects any value (text outside General
    /// Legacy, etc.); the index page chain is malformed; or the spliced
    /// entry list cannot be repacked (e.g. a single entry exceeds the
    /// payload area).
    /// </para>
    /// <para>
    /// Pre-write unique-index enforcement is handled by W15
    /// (<c>CheckUniqueIndexesPreInsertAsync</c> /
    /// <c>CheckUniqueIndexesPreUpdateAsync</c>) before any disk page is
    /// mutated, so this fast path does not re-check uniqueness — same model
    /// as the bulk path's post-write check, which is defense-in-depth for
    /// encoder-rejected indexes that fall through anyway.
    /// </para>
    /// </summary>
    private async ValueTask<bool> TryMaintainIndexesIncrementalAsync(
        long tdefPage,
        TableDef tableDef,
        IReadOnlyList<(RowLocation Loc, object[] Row)>? insertedRows,
        IReadOnlyList<(RowLocation Loc, object[] Row)>? deletedRows,
        CancellationToken cancellationToken)
    {
        if (_format == DatabaseFormat.Jet3Mdb)
        {
            return false;
        }

        int addCount = insertedRows?.Count ?? 0;
        int delCount = deletedRows?.Count ?? 0;
        if (addCount == 0 && delCount == 0)
        {
            return true;
        }

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
        if (numIdx <= 0 || numRealIdx <= 0)
        {
            return true;
        }

        if (numIdx > 1000 || numRealIdx > 1000)
        {
            return false;
        }

        const int RealIdxPhysSz = 52;
        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int namePos = colStart + (numCols * _colDescSz);
        for (int i = 0; i < numCols; i++)
        {
            if (ReadColumnName(tdefBuffer, ref namePos, out _) < 0)
            {
                return false;
            }
        }

        int realIdxDescStart = namePos;

        // Decode every real-idx slot's key columns + first_dp offset.
        var slots = new List<RealIdxEntry>(numRealIdx);
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * RealIdxPhysSz);
            if (phys + RealIdxPhysSz > tdefBuffer.Length)
            {
                return false;
            }

            var keyCols = new List<(int ColNum, bool Ascending)>(10);
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(tdefBuffer, so);
                if (cn == 0xFFFF)
                {
                    continue;
                }

                byte order = tdefBuffer[so + 2];
                keyCols.Add((cn, order != 0x02));
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            slots.Add(new RealIdxEntry(keyCols, phys + 4 + 30 + 4, IsUnique: false));
        }

        if (slots.Count == 0)
        {
            return true;
        }

        var snapshotIndexByColNum = new Dictionary<int, int>(tableDef.Columns.Count);
        for (int c = 0; c < tableDef.Columns.Count; c++)
        {
            snapshotIndexByColNum[tableDef.Columns[c].ColNum] = c;
        }

        bool tdefDirty = false;
        foreach (RealIdxEntry rie in slots)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Resolve key columns to (ColumnInfo, snapshot index, ascending).
            var keyColInfos = new List<(ColumnInfo Col, int SnapIdx, bool Ascending)>(rie.KeyColumns.Count);
            bool resolveFailed = false;
            foreach ((int colNum, bool ascending) in rie.KeyColumns)
            {
                ColumnInfo? col = tableDef.Columns.Find(c => c.ColNum == colNum);
                if (col is null || !snapshotIndexByColNum.TryGetValue(colNum, out int snapIdx))
                {
                    resolveFailed = true;
                    break;
                }

                // T_NUMERIC needs a per-rebuild canonical scale computed
                // across the entire snapshot; the fast path can't compute it
                // without reading the whole table, which is exactly what the
                // bulk path already does. Bail.
                if (col.Type == T_NUMERIC)
                {
                    resolveFailed = true;
                    break;
                }

                keyColInfos.Add((col, snapIdx, ascending));
            }

            if (resolveFailed)
            {
                return false;
            }

            // Read the index root; require a single-leaf root.
            long firstDp = (uint)Ri32(tdefBuffer, rie.FirstDpOffset);
            if (firstDp <= 0)
            {
                return false;
            }

            byte[] rootPageBytes = await ReadPageAsync(firstDp, cancellationToken).ConfigureAwait(false);
            byte[] rootPage;
            try
            {
                rootPage = (byte[])rootPageBytes.Clone();
            }
            finally
            {
                ReturnPage(rootPageBytes);
            }

            // Encode the change-set keys for this index. Used by both the
            // single-leaf splice and the multi-level rebuild path below.
            List<(byte[] Key, long DataPage, byte DataRow)> addEntries = EncodeHintEntries(insertedRows, keyColInfos);
            if (addCount > 0 && addEntries.Count != addCount)
            {
                // Encoder rejected at least one row; bail to bulk.
                return false;
            }

            List<(long DataPage, byte DataRow)> removePtrs = new(delCount);
            if (deletedRows != null)
            {
                foreach ((RowLocation loc, _) in deletedRows)
                {
                    removePtrs.Add((loc.PageNumber, (byte)loc.RowIndex));
                }
            }

            if (!IndexLeafIncremental.IsSingleRootLeaf(rootPage))
            {
                // Multi-level tree (root is an intermediate 0x03 page) or a
                // single leaf with sibling pointers (a child of an
                // intermediate root reached transitively via first_dp would
                // not happen — first_dp always points at the root). Try the
                // multi-level path: descend to the leftmost leaf, walk the
                // leaf-sibling chain, splice the change-set into the
                // collected entry list, and rebuild a fresh tree. Bails to
                // bulk only when the encoder rejects a row or the page chain
                // is malformed. Removes the "fall back to bulk for
                // multi-level trees" branch documented in §7 W4 sub-phase D.
                if (rootPage[0] != IndexLeafIncremental.PageTypeIntermediate
                    && rootPage[0] != IndexLeafPageBuilder.PageTypeLeaf)
                {
                    return false;
                }

                long leftmostLeaf = await DescendToLeftmostLeafAsync(firstDp, cancellationToken).ConfigureAwait(false);
                if (leftmostLeaf <= 0)
                {
                    return false;
                }

                var allExisting = new List<IndexLeafIncremental.DecodedEntry>();
                long walkPage = leftmostLeaf;
                int safetyBudget = 1_000_000; // arbitrary upper bound on leaf count
                while (walkPage > 0)
                {
                    if (--safetyBudget <= 0)
                    {
                        return false;
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    byte[] leafBytes = await ReadPageAsync(walkPage, cancellationToken).ConfigureAwait(false);
                    byte[] leaf;
                    try
                    {
                        leaf = (byte[])leafBytes.Clone();
                    }
                    finally
                    {
                        ReturnPage(leafBytes);
                    }

                    if (leaf[0] != IndexLeafPageBuilder.PageTypeLeaf)
                    {
                        return false;
                    }

                    allExisting.AddRange(IndexLeafIncremental.DecodeEntries(leaf, _pgSz));
                    walkPage = IndexLeafIncremental.ReadNextLeafPage(leaf);
                }

                List<IndexLeafPageBuilder.LeafEntry>? splicedAll = IndexLeafIncremental.Splice(allExisting, addEntries, removePtrs);
                if (splicedAll is null)
                {
                    return false;
                }

                long firstNewPage = _stream.Length / _pgSz;
                IndexBTreeBuilder.BuildResult mlBuild;
                try
                {
                    mlBuild = IndexBTreeBuilder.Build(_pgSz, tdefPage, splicedAll, firstNewPage);
                }
                catch (ArgumentOutOfRangeException)
                {
                    return false;
                }

                foreach (byte[] page in mlBuild.Pages)
                {
                    await AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
                }

                Wi32(tdefBuffer, rie.FirstDpOffset, checked((int)mlBuild.RootPageNumber));
                tdefDirty = true;
                continue;
            }

            List<IndexLeafIncremental.DecodedEntry> existing = IndexLeafIncremental.DecodeEntries(rootPage, _pgSz);
            List<IndexLeafPageBuilder.LeafEntry>? spliced = IndexLeafIncremental.Splice(existing, addEntries, removePtrs);
            if (spliced is null)
            {
                return false;
            }

            byte[]? newLeaf = IndexLeafIncremental.TryRebuildLeaf(_pgSz, tdefPage, spliced);
            if (newLeaf is null)
            {
                return false;
            }

            // Append the new leaf and patch first_dp. Old leaf is orphaned —
            // same disposal model as the bulk-rebuild path.
            long newFirstDp = _stream.Length / _pgSz;
            await AppendPageAsync(newLeaf, cancellationToken).ConfigureAwait(false);
            Wi32(tdefBuffer, rie.FirstDpOffset, checked((int)newFirstDp));
            tdefDirty = true;
        }

        if (tdefDirty)
        {
            await WritePageAsync(tdefPage, tdefBuffer, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Descends an index B-tree from <paramref name="rootPage"/> through
    /// successive intermediate (<c>0x03</c>) levels by following the first
    /// child pointer of each intermediate, returning the page number of the
    /// leftmost leaf (<c>0x04</c>). Returns 0 when the chain is malformed
    /// (unknown page type, missing child pointer, or depth exceeds a sanity
    /// cap), so the caller can fall back to the bulk-rebuild path.
    /// </summary>
    private async ValueTask<long> DescendToLeftmostLeafAsync(long rootPage, CancellationToken cancellationToken)
    {
        long current = rootPage;
        for (int depth = 0; depth < 16; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] pageBytes = await ReadPageAsync(current, cancellationToken).ConfigureAwait(false);
            byte[] page;
            try
            {
                page = (byte[])pageBytes.Clone();
            }
            finally
            {
                ReturnPage(pageBytes);
            }

            if (page[0] == IndexLeafPageBuilder.PageTypeLeaf)
            {
                return current;
            }

            if (page[0] != IndexLeafIncremental.PageTypeIntermediate)
            {
                return 0;
            }

            long firstChild = IndexLeafIncremental.ReadFirstChildPointer(page, _pgSz);
            if (firstChild <= 0)
            {
                return 0;
            }

            current = firstChild;
        }

        return 0;
    }

    /// <summary>
    /// Encodes the (composite-key, page, row) tuples for the rows in
    /// <paramref name="rows"/> against the supplied key column descriptors.
    /// Returns a partially-filled list when an encoder throws — the caller
    /// detects this by comparing <c>Count</c> to the input count and bailing
    /// to the bulk-rebuild path.
    /// </summary>
    private static List<(byte[] Key, long DataPage, byte DataRow)> EncodeHintEntries(
        IReadOnlyList<(RowLocation Loc, object[] Row)>? rows,
        List<(ColumnInfo Col, int SnapIdx, bool Ascending)> keyColInfos)
    {
        var result = new List<(byte[] Key, long DataPage, byte DataRow)>(rows?.Count ?? 0);
        if (rows == null || rows.Count == 0)
        {
            return result;
        }

        foreach ((RowLocation loc, object[] row) in rows)
        {
            byte[][] perColumn = new byte[keyColInfos.Count][];
            int totalLen = 0;
            bool encoderRejected = false;
            for (int k = 0; k < keyColInfos.Count; k++)
            {
                (ColumnInfo col, int snapIdx, bool ascending) = keyColInfos[k];
                if (snapIdx >= row.Length)
                {
                    encoderRejected = true;
                    break;
                }

                object cell = row[snapIdx];
                object? value = cell is DBNull ? null : cell;
                try
                {
                    perColumn[k] = IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
                }
                catch (NotSupportedException)
                {
                    encoderRejected = true;
                    break;
                }
                catch (ArgumentException)
                {
                    encoderRejected = true;
                    break;
                }
                catch (OverflowException)
                {
                    encoderRejected = true;
                    break;
                }

                totalLen += perColumn[k].Length;
            }

            if (encoderRejected)
            {
                return result;
            }

            byte[] composite = new byte[totalLen];
            int offset = 0;
            for (int k = 0; k < perColumn.Length; k++)
            {
                Buffer.BlockCopy(perColumn[k], 0, composite, offset, perColumn[k].Length);
                offset += perColumn[k].Length;
            }

            result.Add((composite, loc.PageNumber, (byte)loc.RowIndex));
        }

        return result;
    }

    private record struct RealIdxEntry(IReadOnlyList<(int ColNum, bool Ascending)> KeyColumns, int FirstDpOffset, bool IsUnique);

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

    /// <summary>
    /// W15: parse the TDEF page and return one descriptor per <em>unique</em>
    /// real-idx slot (uniqueness is signalled either by the §3.1 real-idx
    /// <c>flags &amp; 0x01</c> bit or by an associated logical-idx entry whose
    /// <c>index_type = 0x01</c> primary-key discriminator). Returns an empty
    /// list on Jet3 (no index emission) or when the TDEF declares no indexes.
    /// </summary>
    private async ValueTask<List<UniqueIndexDescriptor>> LoadUniqueIndexDescriptorsAsync(
        long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        var result = new List<UniqueIndexDescriptor>();
        if (_format == DatabaseFormat.Jet3Mdb)
        {
            return result;
        }

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
            return result;
        }

        const int RealIdxPhysSz = 52;
        const int LogIdxEntrySz = 28;
        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int namePos = colStart + (numCols * _colDescSz);
        for (int i = 0; i < numCols; i++)
        {
            if (ReadColumnName(tdefBuffer, ref namePos, out _) < 0)
            {
                return result;
            }
        }

        int realIdxDescStart = namePos;
        int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);

        // Decode every real-idx slot's key columns + flag bit. Reuses the
        // RealIdxEntry shape from MaintainIndexesAsync; FirstDpOffset is
        // unused on this path (we don't rewrite first_dp here) so it's left
        // at 0.
        var realIdxSlots = new Dictionary<int, RealIdxEntry>();
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * RealIdxPhysSz);
            if (phys + RealIdxPhysSz > tdefBuffer.Length)
            {
                break;
            }

            var keyCols = new List<(int ColNum, bool Ascending)>(10);
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(tdefBuffer, so);
                if (cn == 0xFFFF)
                {
                    continue;
                }

                byte order = tdefBuffer[so + 2];
                keyCols.Add((cn, order != 0x02));
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            realIdxSlots[ri] = new RealIdxEntry(keyCols, FirstDpOffset: 0, IsUnique: (tdefBuffer[phys + 42] & 0x01) != 0);
        }

        // Walk the logical-idx entries to (a) promote any real-idx that backs a
        // PK logical-idx to unique, and (b) capture a best-effort name for each
        // unique real-idx (first logical-idx referencing it wins).
        int logIdxNamesStart = logIdxStart + (numIdx * LogIdxEntrySz);
        List<string> logIdxNames = ReadLogicalIdxNames(tdefBuffer, logIdxNamesStart, numIdx);

        var pkRealIdxNums = new HashSet<int>();
        var nameByRealIdx = new Dictionary<int, string>();
        for (int li = 0; li < numIdx; li++)
        {
            int entryStart = logIdxStart + (li * LogIdxEntrySz);
            if (entryStart + LogIdxEntrySz > tdefBuffer.Length)
            {
                break;
            }

            int realIdxNum = Ri32(tdefBuffer, entryStart + 8);
            byte indexType = tdefBuffer[entryStart + 23];
            if (indexType == (byte)IndexKind.PrimaryKey)
            {
                pkRealIdxNums.Add(realIdxNum);
            }

            if (li < logIdxNames.Count)
            {
                nameByRealIdx.TryAdd(realIdxNum, logIdxNames[li]);
            }
        }

        // Build column-ordinal lookup for resolving (ColNum -> snapshot index).
        var snapshotIndexByColNum = new Dictionary<int, int>(tableDef.Columns.Count);
        for (int c = 0; c < tableDef.Columns.Count; c++)
        {
            snapshotIndexByColNum[tableDef.Columns[c].ColNum] = c;
        }

        foreach ((int realIdxNum, RealIdxEntry slot) in realIdxSlots)
        {
            if (!slot.IsUnique && !pkRealIdxNums.Contains(realIdxNum))
            {
                continue;
            }

            // Resolve every key column up front; if any column is missing from
            // the snapshot (deleted-column gap), skip — same fall-through model
            // as MaintainIndexesAsync.
            var keyColInfos = new List<(ColumnInfo Col, int SnapIdx, bool Ascending)>(slot.KeyColumns.Count);
            bool resolveOk = true;
            foreach ((int colNum, bool ascending) in slot.KeyColumns)
            {
                if (!snapshotIndexByColNum.TryGetValue(colNum, out int snapIdx))
                {
                    resolveOk = false;
                    break;
                }

                keyColInfos.Add((tableDef.Columns[snapIdx], snapIdx, ascending));
            }

            if (!resolveOk)
            {
                continue;
            }

            string name = nameByRealIdx.TryGetValue(realIdxNum, out string? n) ? n : $"realidx#{realIdxNum}";
            result.Add(new UniqueIndexDescriptor(realIdxNum, name, keyColInfos));
        }

        return result;
    }

    /// <summary>
    /// W15: encode the composite index key for one row using a previously
    /// computed canonical numeric scale per key column. Returns <c>null</c>
    /// (i.e. "encoder rejected this row") only when the underlying encoder
    /// throws <see cref="NotSupportedException"/>; the caller treats that as
    /// "skip the unique check for this index" — same fall-through model as
    /// <see cref="MaintainIndexesAsync"/>.
    /// </summary>
    private byte[]? EncodeCompositeKeyForUniqueCheck(
        UniqueIndexDescriptor descriptor,
        object[] row,
        int[] numericTargetScales)
    {
        bool legacyNumeric = _format == DatabaseFormat.Jet4Mdb;
        byte[][] perColumn = new byte[descriptor.KeyColumns.Count][];
        int totalLen = 0;
        try
        {
            for (int k = 0; k < descriptor.KeyColumns.Count; k++)
            {
                (ColumnInfo col, int snapIdx, bool ascending) = descriptor.KeyColumns[k];
                object cell = snapIdx < row.Length ? row[snapIdx] : DBNull.Value;
                object? value = cell is null or DBNull ? null : cell;
                perColumn[k] = col.Type == T_NUMERIC
                    ? IndexKeyEncoder.EncodeNumericEntry(value, ascending, numericTargetScales[k], legacyNumeric)
                    : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
                totalLen += perColumn[k].Length;
            }
        }
        catch (NotSupportedException)
        {
            return null;
        }

        byte[] composite = new byte[totalLen];
        int offset = 0;
        for (int k = 0; k < perColumn.Length; k++)
        {
            Buffer.BlockCopy(perColumn[k], 0, composite, offset, perColumn[k].Length);
            offset += perColumn[k].Length;
        }

        return composite;
    }

    /// <summary>
    /// W15: pre-write unique-index validation for an insert batch. Loads the
    /// table snapshot once, encodes the existing-row keys + the pending-row
    /// keys for every unique index, and throws
    /// <see cref="InvalidOperationException"/> on the first collision (existing
    /// vs. pending or pending vs. pending). On encoder failure for a given
    /// index (unsupported key type, e.g. text outside General Legacy) the
    /// check is silently skipped for that index — the post-write check inside
    /// <see cref="MaintainIndexesAsync"/> still runs as defense-in-depth.
    /// <para>
    /// Pending rows MUST already have had <c>ApplyConstraintsAsync</c> applied
    /// (auto-increment values resolved, defaults substituted) — the encoder
    /// works against the on-disk row payload.
    /// </para>
    /// </summary>
    private async ValueTask CheckUniqueIndexesPreInsertAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        List<object[]> pendingRows,
        CancellationToken cancellationToken)
    {
        if (pendingRows.Count == 0)
        {
            return;
        }

        List<UniqueIndexDescriptor> descriptors = await LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
        if (descriptors.Count == 0)
        {
            return;
        }

        using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        CheckUniqueIndexesCore(tableName, descriptors, snapshot, pendingRows, replaceAtSnapshotIndex: null);
    }

    /// <summary>
    /// W15: pre-write unique-index validation for an update batch. The caller
    /// supplies the table snapshot it already loaded (saving a redundant
    /// scan) plus the per-row replacement payloads keyed by snapshot row
    /// index.
    /// </summary>
    private async ValueTask CheckUniqueIndexesPreUpdateAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        DataTable snapshot,
        List<(int Index, object[] NewRow)> updates,
        CancellationToken cancellationToken)
    {
        if (updates.Count == 0)
        {
            return;
        }

        List<UniqueIndexDescriptor> descriptors = await LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
        if (descriptors.Count == 0)
        {
            return;
        }

        var replaceAt = new Dictionary<int, object[]>(updates.Count);
        foreach ((int idx, object[] newRow) in updates)
        {
            replaceAt[idx] = newRow;
        }

        CheckUniqueIndexesCore(tableName, descriptors, snapshot, pendingInsertRows: [], replaceAtSnapshotIndex: replaceAt);
    }

    /// <summary>
    /// W15 core: builds the post-mutation effective row set (snapshot rows
    /// optionally replaced at <paramref name="replaceAtSnapshotIndex"/> plus
    /// <paramref name="pendingInsertRows"/> appended), encodes the composite
    /// key per unique index, and detects any collision. Throws
    /// <see cref="InvalidOperationException"/> on first violation.
    /// </summary>
    private void CheckUniqueIndexesCore(
        string tableName,
        List<UniqueIndexDescriptor> descriptors,
        DataTable snapshot,
        List<object[]> pendingInsertRows,
        Dictionary<int, object[]>? replaceAtSnapshotIndex)
    {
        int snapshotRowCount = snapshot.Rows.Count;
        int pendingCount = pendingInsertRows.Count;
        int totalRows = snapshotRowCount + pendingCount;

        foreach (UniqueIndexDescriptor descriptor in descriptors)
        {
            // Compute canonical numeric scale across (non-replaced snapshot
            // rows) + (replacement rows) + (pending insert rows). Mirrors the
            // post-write computation in MaintainIndexesAsync.
            int[] numericTargetScales = new int[descriptor.KeyColumns.Count];
            for (int k = 0; k < descriptor.KeyColumns.Count; k++)
            {
                if (descriptor.KeyColumns[k].Col.Type == T_NUMERIC)
                {
                    int kSnap = descriptor.KeyColumns[k].SnapIdx;
                    int max = 0;
                    for (int r = 0; r < snapshotRowCount; r++)
                    {
                        object cell;
                        if (replaceAtSnapshotIndex != null && replaceAtSnapshotIndex.TryGetValue(r, out object[]? rep))
                        {
                            cell = kSnap < rep.Length ? rep[kSnap] : DBNull.Value;
                        }
                        else
                        {
                            cell = snapshot.Rows[r][kSnap];
                        }

                        if (cell is null or DBNull)
                        {
                            continue;
                        }

                        decimal dv = Convert.ToDecimal(cell, CultureInfo.InvariantCulture);
                        int s = (decimal.GetBits(dv)[3] >> 16) & 0x7F;
                        if (s > max)
                        {
                            max = s;
                        }
                    }

                    foreach (object[] pendingRow in pendingInsertRows)
                    {
                        if (kSnap >= pendingRow.Length)
                        {
                            continue;
                        }

                        object cell = pendingRow[kSnap];
                        if (cell is null or DBNull)
                        {
                            continue;
                        }

                        decimal dv = Convert.ToDecimal(cell, CultureInfo.InvariantCulture);
                        int s = (decimal.GetBits(dv)[3] >> 16) & 0x7F;
                        if (s > max)
                        {
                            max = s;
                        }
                    }

                    numericTargetScales[k] = max;
                }
                else
                {
                    numericTargetScales[k] = -1;
                }
            }

            // Encode every effective row's key. Collect into a HashSet keyed
            // by composite key bytes; first collision triggers the throw.
            var seen = new HashSet<byte[]>(ByteArrayEqualityComparer.Instance);
            bool encoderRejected = false;

            for (int r = 0; r < snapshotRowCount; r++)
            {
                object[] effectiveRow;
                if (replaceAtSnapshotIndex != null && replaceAtSnapshotIndex.TryGetValue(r, out object[]? rep))
                {
                    effectiveRow = rep;
                }
                else
                {
                    effectiveRow = snapshot.Rows[r].ItemArray;
                }

                byte[]? key = EncodeCompositeKeyForUniqueCheck(descriptor, effectiveRow, numericTargetScales);
                if (key is null)
                {
                    encoderRejected = true;
                    break;
                }

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }

            if (encoderRejected)
            {
                continue;
            }

            for (int p = 0; p < pendingCount; p++)
            {
                byte[]? key = EncodeCompositeKeyForUniqueCheck(descriptor, pendingInsertRows[p], numericTargetScales);
                if (key is null)
                {
                    encoderRejected = true;
                    break;
                }

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }

            // encoderRejected on pending rows: silently skip — the post-write
            // check in MaintainIndexesAsync still runs.
            _ = totalRows;
        }
    }

    private sealed record UniqueIndexDescriptor(int RealIdxNum, string Name, IReadOnlyList<(ColumnInfo Col, int SnapIdx, bool Ascending)> KeyColumns);

    private sealed class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayEqualityComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }

            if (x is null || y is null || x.Length != y.Length)
            {
                return false;
            }

            for (int i = 0; i < x.Length; i++)
            {
                if (x[i] != y[i])
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            // FNV-1a 32-bit. Keys are typically short (< 64 bytes); this is
            // not a hot path so a SIMD/unsafe variant would be over-spec.
            unchecked
            {
                int hash = (int)2166136261u;
                for (int i = 0; i < obj.Length; i++)
                {
                    hash = (hash ^ obj[i]) * 16777619;
                }

                return hash;
            }
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
        public ResolvedIndex(string name, IReadOnlyList<int> columnNumbers, IReadOnlyList<bool> ascending, bool isPrimaryKey, bool isUnique)
        {
            Name = name;
            ColumnNumbers = columnNumbers;
            Ascending = ascending;
            IsPrimaryKey = isPrimaryKey;
            IsUnique = isUnique;
        }

        public string Name { get; }

        public IReadOnlyList<int> ColumnNumbers { get; }

        /// <summary>Gets the per-column sort direction (parallel to <see cref="ColumnNumbers"/>).</summary>
        public IReadOnlyList<bool> Ascending { get; }

        public bool IsPrimaryKey { get; }

        /// <summary>
        /// Gets a value indicating whether this index enforces uniqueness on its
        /// key columns. Primary keys are implicitly unique; for them this
        /// returns <see langword="true"/> regardless of the user-supplied
        /// <see cref="IndexDefinition.IsUnique"/>.
        /// </summary>
        public bool IsUnique { get; }
    }
}
