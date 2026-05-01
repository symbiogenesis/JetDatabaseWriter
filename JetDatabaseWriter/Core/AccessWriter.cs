namespace JetDatabaseWriter.Core;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Internal.Transactions;
using JetDatabaseWriter.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

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
    /// <c>(byte)RowIndex</c> cast in the index-rebuild path stays safe
    /// under <c>&lt;CheckForOverflowUnderflow&gt;true</c>.
    /// </summary>
    private const int MaxRowsPerDataPage = 255;

    /// <summary>
    /// Maximum recursion depth for cascade-delete / cascade-update chains.
    /// Guards against pathological self-referential cycles. Real-world Access
    /// schemas almost never exceed depth 3.
    /// </summary>
    private const int CascadeMaxDepth = 64;

    private readonly ReadOnlyMemory<char> _password;
    private readonly bool _useLockFile;
    private readonly bool _respectExistingLockFile;
    private readonly string? _lockFileUserName;
    private readonly string? _lockFileMachineName;
    private readonly bool _useByteRangeLocks;
    private readonly int _lockTimeoutMilliseconds;
    private readonly int _maxTransactionPageBudget;
    private readonly bool _useTransactionalWrites;
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

    private LockFileSlotWriter? _lockFileSlot;

    // Active explicit transaction. We dispose any in-flight transaction's
    // journal explicitly in DisposeAsync (see below); CA2213 cannot see that
    // because we mark the transaction terminated rather than calling its
    // public DisposeAsync (which would re-enter the writer).
#pragma warning disable CA2213
    private JetTransaction? _activeTransaction;
#pragma warning restore CA2213
    private long _cachedInsertTDefPage = -1;
    private long _cachedInsertPageNumber = -1;

    private AccessWriter(
        string path,
        Stream stream,
        byte[] header,
        ReadOnlyMemory<char> password,
        bool useLockFile,
        bool respectExistingLockFile,
        string? lockFileUserName,
        string? lockFileMachineName,
        bool useByteRangeLocks,
        int lockTimeoutMilliseconds,
        int maxTransactionPageBudget,
        bool useTransactionalWrites,
        Stream? outerEncryptedStream = null,
        bool outerEncryptedLeaveOpen = false,
        bool isAgileEncryptedRewrap = false)
        : base(stream, header, path)
    {
        _password = password;
        _useLockFile = useLockFile && !string.IsNullOrEmpty(path);
        _respectExistingLockFile = respectExistingLockFile;
        _lockFileUserName = lockFileUserName;
        _lockFileMachineName = lockFileMachineName;
        _useByteRangeLocks = useByteRangeLocks;
        _lockTimeoutMilliseconds = lockTimeoutMilliseconds;
        _maxTransactionPageBudget = maxTransactionPageBudget;
        _useTransactionalWrites = useTransactionalWrites;
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
            _lockFileSlot = LockFileSlotWriter.Open(
                _path,
                nameof(AccessWriter),
                respectExisting: _respectExistingLockFile,
                machineName: _lockFileMachineName,
                userName: _lockFileUserName);
        }

        _byteRangeLock = JetByteRangeLock.Create(stream, _useByteRangeLocks, _lockTimeoutMilliseconds);
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
                        options.LockFileUserName,
                        options.LockFileMachineName,
                        options.UseByteRangeLocks,
                        options.LockTimeoutMilliseconds,
                        options.MaxTransactionPageBudget,
                        options.UseTransactionalWrites,
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
                options.RespectExistingLockFile,
                options.LockFileUserName,
                options.LockFileMachineName,
                options.UseByteRangeLocks,
                options.LockTimeoutMilliseconds,
                options.MaxTransactionPageBudget,
                options.UseTransactionalWrites);
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
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => CreateTableCoreAsync(tableName, columns, indexes, cancellationToken), cancellationToken);

    private async ValueTask CreateTableCoreAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken)
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

        // Calculated columns (Access 2010+ expression columns) are recognised
        // on the read side (see ColumnMetadata.IsCalculated) but writing them
        // is not yet implemented \u2014 the on-disk format requires emitting the
        // extra-flags byte at descriptor offset 16, the Expression / ResultType
        // properties in MSysObjects.LvProp, and the 23-byte calculated-value
        // wrapper around every cached cell value (see
        // docs/design/calculated-columns-format-notes.md).
        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition col = columns[i];
            if (col.IsCalculated)
            {
                throw new NotSupportedException(
                    $"Column '{col.Name}': writing calculated columns is not yet implemented. " +
                    "See docs/design/calculated-columns-format-notes.md for the on-disk format. " +
                    "Reading calc-column metadata produced by Microsoft Access is supported via " +
                    "ColumnMetadata.IsCalculated / .CalculationExpression / .CalculatedResultType.");
            }
        }

        // Pre-process the column-level IsPrimaryKey shortcut. Synthesize one
        // composite PK IndexDefinition (named "PrimaryKey") from columns
        // marked IsPrimaryKey=true, in declaration order, and force those
        // columns to IsNullable=false on the emitted TDEF. Mixing the
        // shortcut with an explicit PK IndexDefinition is rejected.
        (columns, indexes) = IndexHelpers.ApplyPrimaryKeyShortcut(columns, indexes);

        // Unsupported Jet4 key types (OLE / Attachment / Multi-Value) are
        // rejected up-front below in ResolveIndexes.

        if (await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        // Complex columns (Attachment / MultiValue) declared by the user have
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

        // Emit the hidden flat child table + MSysComplexColumns row for every
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
    /// emit hidden system tables (e.g. complex-column flat tables that need
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
        List<ResolvedIndex> resolvedIndexes = IndexHelpers.ResolveIndexes(indexes, tableDef);
        (byte[] tdefPage, int[] firstDpOffsets) = BuildTDefPageWithIndexOffsets(tableDef, resolvedIndexes);
        long tdefPageNumber = await AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        // Emit one empty index leaf page per real index and patch its page
        // number into the corresponding `first_dp` field of the real-idx physical
        // descriptor. The leaf starts empty because CreateTableAsync inserts no
        // rows; subsequent inserts/updates/deletes maintain the B-tree via
        // MaintainIndexesAsync. See
        // docs/design/index-and-relationship-format-notes.md §7.
        if (resolvedIndexes.Count > 0)
        {
            IndexLeafPageBuilder.LeafPageLayout layout = _format == DatabaseFormat.Jet3Mdb
                ? IndexLeafPageBuilder.LeafPageLayout.Jet3
                : IndexLeafPageBuilder.LeafPageLayout.Jet4;
            for (int i = 0; i < resolvedIndexes.Count; i++)
            {
                byte[] leafPage = IndexLeafPageBuilder.BuildLeafPage(
                    layout,
                    _pgSz,
                    tdefPageNumber,
                    [],
                    prevPage: 0,
                    nextPage: 0,
                    tailPage: 0,
                    enablePrefixCompression: false);
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
    public ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => DropTableEntryAsync(tableName, cancellationToken), cancellationToken);

    private async ValueTask DropTableEntryAsync(string tableName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        await DropTableCoreAsync(tableName, dropComplexChildren: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => AddColumnCoreAsync(tableName, column, cancellationToken), cancellationToken);

    private ValueTask AddColumnCoreAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken)
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
        => RunAutoCommitAsync(_ => DropColumnCoreAsync(tableName, columnName, cancellationToken), cancellationToken);

    private ValueTask DropColumnCoreAsync(string tableName, string columnName, CancellationToken cancellationToken)
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
        => RunAutoCommitAsync(_ => RenameColumnCoreAsync(tableName, oldColumnName, newColumnName, cancellationToken), cancellationToken);

    private ValueTask RenameColumnCoreAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken)
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
                    IsHyperlink = src.IsHyperlink,
                    ValidationRule = src.ValidationRule,
                    DefaultValueExpression = src.DefaultValueExpression,
                    ValidationRuleExpression = src.ValidationRuleExpression,
                    ValidationText = src.ValidationText,
                    Description = src.Description,

                    // Forward complex-column flags so the rebuilt TDEF re-emits
                    // T_ATTACHMENT / T_COMPLEX with the original ComplexId in
                    // the misc slot. RewriteTableAsync uses the preserved
                    // ComplexId to update MSysComplexColumns.ColumnName.
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
                    // Forward Normal (1..N column) and PrimaryKey indexes;
                    // FK indexes are reconstructed from MSysRelationships.
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
    public ValueTask InsertRowAsync(string tableName, object[] values, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => InsertRowEntryAsync(tableName, values, cancellationToken), cancellationToken);

    private async ValueTask InsertRowEntryAsync(string tableName, object[] values, CancellationToken cancellationToken)
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
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object[]> rows, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync<int>(_ => InsertRowsCoreAsync(tableName, rows, cancellationToken), cancellationToken);

    private async ValueTask<int> InsertRowsCoreAsync(string tableName, IEnumerable<object[]> rows, CancellationToken cancellationToken)
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

        // Materialize the batch so the pre-write unique-index check sees
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

            // Pre-write unique-index enforcement.
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
    public ValueTask InsertRowAsync<T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new()
        => RunAutoCommitAsync(_ => InsertRowGenericCoreAsync<T>(tableName, item, cancellationToken), cancellationToken);

    private async ValueTask InsertRowGenericCoreAsync<T>(string tableName, T item, CancellationToken cancellationToken)
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
    public ValueTask<int> InsertRowsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new()
        => RunAutoCommitAsync<int>(_ => InsertRowsGenericCoreAsync<T>(tableName, items, cancellationToken), cancellationToken);

    private async ValueTask<int> InsertRowsGenericCoreAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken)
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
        // pre-write unique check sees every pending row at once.
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

            // Pre-write unique-index enforcement.
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
    public ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object> updatedValues, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync<int>(_ => UpdateRowsCoreAsync(tableName, predicateColumn, predicateValue, updatedValues, cancellationToken), cancellationToken);

    private async ValueTask<int> UpdateRowsCoreAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object> updatedValues, CancellationToken cancellationToken)
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

        // FK enforcement: build the list of new-row payloads up front so we
        // can validate FK constraints (FK-side parent presence, PK-side
        // cascade-or-reject) before mutating any disk page.
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
            var changes = new List<(string? OldKey, object?[] OldFullRow, object[] NewPkValues)>(pendingNewRows.Count);
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
                    object?[] oldFullRow = snapshot.Rows[rowIdx].ItemArray;
                    string? oldKey = IndexHelpers.BuildCompositeKey(oldFullRow, pkIdx);
                    changes.Add((oldKey, oldFullRow, newRow));
                }

                await EnforceFkOnPrimaryUpdateAsync(tableName, tableDef, changes, fkCtx, depth: 0, cancellationToken).ConfigureAwait(false);
            }
        }

        // Pre-write unique-index enforcement: after FK checks succeed,
        // validate that the post-update key set contains no duplicates for
        // any unique index. The check sees the snapshot with pendingNewRows
        // substituted at their original indices.
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
    public ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync<int>(_ => DeleteRowsCoreAsync(tableName, predicateColumn, predicateValue, cancellationToken), cancellationToken);

    private async ValueTask<int> DeleteRowsCoreAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken)
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

        // FK enforcement: identify the rows we are about to delete; if any
        // FK relationship names this table as the primary side, capture the
        // deleted PK tuples and let EnforceFkOnPrimaryDeleteAsync
        // cascade-delete dependent child rows (or throw when cascade is
        // disabled).
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

            // Snapshot the typed full row of every parent we are about to
            // delete, in primary-table column order. EnforceFkOnPrimaryDeleteAsync
            // consumes this once per relationship (slicing the relationship's
            // PrimaryColumns out for the FK seek / snapshot scan).
            var deletedParentRows = new List<object?[]>(matchingIndices.Count);
            foreach (int rowIdx in matchingIndices)
            {
                deletedParentRows.Add(snapshot.Rows[rowIdx].ItemArray);
            }

            await EnforceFkOnPrimaryDeleteAsync(
                tableName,
                tableDef,
                deletedParentRows,
                fkCtx,
                depth: 0,
                cancellationToken).ConfigureAwait(false);
        }

        // Cascade flat-child rows for any complex columns on the parent
        // BEFORE we mark the parent rows deleted (we need to read the
        // parent's ConceptualTableID slots while the rows are still live).
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
    public ValueTask CreateLinkedTableAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => CreateLinkedTableCoreAsync(linkedTableName, sourceDatabasePath, foreignTableName, cancellationToken), cancellationToken);

    private async ValueTask CreateLinkedTableCoreAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken)
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

        TableDef msys = await ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", 0);
        msys.SetValueByName(values, "ParentId", 0);
        msys.SetValueByName(values, "Name", linkedTableName);
        msys.SetValueByName(values, "Type", (short)Constants.SystemObjects.LinkedTableType);
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
    public ValueTask CreateLinkedOdbcTableAsync(string linkedTableName, string connectionString, string foreignTableName, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => CreateLinkedOdbcTableCoreAsync(linkedTableName, connectionString, foreignTableName, cancellationToken), cancellationToken);

    private async ValueTask CreateLinkedOdbcTableCoreAsync(string linkedTableName, string connectionString, string foreignTableName, CancellationToken cancellationToken)
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

        TableDef msys = await ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", 0);
        msys.SetValueByName(values, "ParentId", 0);
        msys.SetValueByName(values, "Name", linkedTableName);
        msys.SetValueByName(values, "Type", (short)Constants.SystemObjects.LinkedOdbcType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", 0);
        msys.SetValueByName(values, "ForeignName", foreignTableName);
        msys.SetValueByName(values, "Connect", normalizedConnect);

        await InsertRowDataAsync(2, msys, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        InvalidateCatalogCache();
    }

    // ════════════════════════════════════════════════════════════════
    // Foreign-key relationships — MSysRelationships row emission
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
    /// Per <c>docs/design/index-and-relationship-format-notes.md</c> §7. The
    /// MSysRelationships catalog rows are what the Microsoft Access
    /// Relationships designer reads. The per-TDEF FK logical-index entries
    /// (<c>index_type = 0x02</c>, <c>rel_idx_num</c>, <c>rel_tbl_page</c>)
    /// that drive runtime referential-integrity enforcement by the JET
    /// engine are emitted by <see cref="EmitFkPerTdefEntriesAsync"/> on
    /// Jet4 / ACE; on Jet3 they are skipped and Microsoft Access regenerates
    /// them on the next Compact &amp; Repair pass.
    /// </remarks>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => CreateRelationshipCoreAsync(relationship, cancellationToken), cancellationToken);

    private async ValueTask CreateRelationshipCoreAsync(RelationshipDefinition relationship, CancellationToken cancellationToken)
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
        long msysRelTdefPage = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table. Databases freshly created by " +
                "AccessWriter.CreateDatabaseAsync do not include this catalog table; open or copy an " +
                "Access-authored database before calling CreateRelationshipAsync.");
        }

        TableDef msysRelDef = await ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

        // Reject duplicate relationship names (case-insensitive).
        HashSet<string> existingNames = await ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existingNames.Contains(relationship.Name))
        {
            throw new InvalidOperationException($"A relationship named '{relationship.Name}' already exists.");
        }

        // grbit flag bits — values per Jackcess com.healthmarketscience.jackcess.impl.RelationshipImpl.
        // (These are not yet documented in the format-probe appendix.)
        uint grbit = 0;
        if (!relationship.EnforceReferentialIntegrity)
        {
            grbit |= Constants.RelationshipFlags.NoRefIntegrity;
        }

        if (relationship.CascadeUpdates)
        {
            grbit |= Constants.RelationshipFlags.CascadeUpdates;
        }

        if (relationship.CascadeDeletes)
        {
            grbit |= Constants.RelationshipFlags.CascadeDeletes;
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

        // Per-TDEF FK logical-idx entries: add index_type=0x02 logical-idx
        // entries on both PK-side and FK-side TDEFs with cross-referenced
        // rel_idx_num / rel_tbl_page so the JET engine can locate the partner
        // table without waiting for Microsoft Access Compact & Repair to
        // regenerate them from the MSysRelationships rows above. See
        // docs/design/index-and-relationship-format-notes.md §7. Jet3 uses a
        // different (20-byte) logical-idx layout that this library does not
        // yet exercise — skip silently to keep the catalog row emission
        // working on .mdb (Access 97) databases.
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
    // Per-TDEF FK logical-idx entries (Jet4 / ACE only)
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
    /// Orchestrates the two-side per-TDEF FK index emission: pre-computes
    /// both sides' target real-idx slots (sharing where possible), allocates
    /// empty leaf pages for any newly-allocated real-idx slots, then mutates
    /// each TDEF in place to append its FK logical-idx entry. Operates on
    /// single-page TDEFs only; throws <see cref="NotSupportedException"/> if
    /// either TDEF is multi-page or would overflow a single page after growth.
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
        string pkName = IndexHelpers.MakeUniqueLogicalIdxName(relationship.Name, pkExistingNames);
        string fkName = IndexHelpers.MakeUniqueLogicalIdxName(
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
                throw new InvalidOperationException($"page {tdefPage} is not a TDEF page.");
            }

            if (Ru32(page, 4) != 0)
            {
                throw new NotSupportedException(
                    $"TDEF at page {tdefPage} spans multiple pages; in-place FK index emission is not supported on multi-page TDEFs.");
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
                throw new InvalidOperationException("unable to walk TDEF column-name section.");
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
    /// Appends one FK logical-idx entry (and optionally a new real-idx
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
                $"cannot mutate the TDEF at page {tdefPage} (multi-page chain or not a TDEF).");
        }

        int numCols = Ru16(td, _tdNumCols);
        int numIdx = Ri32(td, _tdNumCols + 2);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numCols < 0 || numCols > 4096
            || numIdx < 0 || numIdx > 1000
            || numRealIdx < 0 || numRealIdx > 1000)
        {
            throw new InvalidOperationException("TDEF reports out-of-range section counts.");
        }

        const int RealIdxPhysSz = 52;
        const int LogIdxEntrySz = 28;

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            throw new InvalidOperationException("failed to walk TDEF column-name section.");
        }

        int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);
        int logIdxNamesStart = logIdxStart + (numIdx * LogIdxEntrySz);
        int logIdxNamesLen = MeasureLogicalIdxNamesLength(td, logIdxNamesStart, numIdx);
        if (logIdxNamesLen < 0)
        {
            throw new InvalidOperationException("failed to walk TDEF logical-idx-name section.");
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
            throw new InvalidOperationException("TDEF trailing block overruns page.");
        }

        byte[] nameBytes = Encoding.Unicode.GetBytes(indexName);
        int nameRecordSize = 2 + nameBytes.Length;

        int deltaRealIdxSkip = allocateNewRealIdx ? _realIdxEntrySz : 0;
        int deltaRealIdxPhys = allocateNewRealIdx ? RealIdxPhysSz : 0;
        int totalGrowth = deltaRealIdxSkip + deltaRealIdxPhys + LogIdxEntrySz + nameRecordSize;

        if (currentEnd + totalGrowth > _pgSz)
        {
            throw new NotSupportedException(
                "TDEF would exceed a single page after adding a foreign-key index entry. " +
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
        //              consistent with this library's existing emit.
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
    /// collisions when appending a new FK index entry).
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
    /// Real-idx sharing per §3.3: returns the existing real-idx slot whose col_map
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

    // ════════════════════════════════════════════════════════════════
    // Drop / Rename relationship
    // ════════════════════════════════════════════════════════════════
    //
    // Reverses CreateRelationshipAsync:
    //   • DropRelationshipAsync removes every MSysRelationships row whose
    //     szRelationship matches and (Jet4/ACE) removes the matching FK
    //     logical-idx entry from each side's TDEF, then conservatively
    //     reclaims any trailing real-idx physical-descriptor slots that the
    //     removal left unreferenced (common case: FK got the last slot on
    //     its TDEF and the slot is reclaimed cleanly; non-trailing orphans
    //     are still left for Compact & Repair to reclaim, since mid-array
    //     compaction would require cross-TDEF rel_idx_num renumbering on
    //     every other table that points at the slot).
    //     ListIndexesAsync iterates by num_idx so the FK stops surfacing
    //     immediately regardless of whether the real-idx slot was reclaimed.
    //   • RenameRelationshipAsync rewrites the szRelationship column on
    //     every matching MSysRelationships row (read all 8 columns, mark
    //     deleted, re-insert with the new name and updateTDefRowCount=false)
    //     and (Jet4/ACE) updates the matching FK logical-idx name cookie on
    //     each side's TDEF in place. Variable-length name records:
    //     shrink/grow shifts the trailing variable-column block; a grow
    //     that would push the TDEF past one page leaves the cookie
    //     unchanged (Access regenerates it from the catalog row on the
    //     next Compact & Repair pass).

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
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => DropRelationshipCoreAsync(relationshipName, cancellationToken), cancellationToken);

    private async ValueTask DropRelationshipCoreAsync(string relationshipName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(relationshipName, nameof(relationshipName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        long msysRelTdefPage = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to drop.");
        }

        TableDef msysRelDef = await ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
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
                int pkReleased = await TryRemoveFkLogicalIdxEntryAsync(pkEntry.TDefPage, pkColNums, fkEntry.TDefPage, cancellationToken).ConfigureAwait(false);
                int fkReleased = await TryRemoveFkLogicalIdxEntryAsync(fkEntry.TDefPage, fkColNums, pkEntry.TDefPage, cancellationToken).ConfigureAwait(false);

                // Reclaim trailing real-idx slots that are no longer
                // referenced by any logical-idx entry. PK-side typically
                // shares its real-idx slot with the existing PK logical-idx
                // (no reclaim possible), but the FK-side's real-idx is
                // usually its own and can be reclaimed cleanly.
                if (pkReleased >= 0)
                {
                    await TryReclaimTrailingRealIdxAsync(pkEntry.TDefPage, cancellationToken).ConfigureAwait(false);
                }

                if (fkReleased >= 0)
                {
                    // Self-referential: PK and FK live on the same TDEF and
                    // both removals already happened above; one reclaim pass
                    // covers both released slots.
                    if (pkEntry.TDefPage != fkEntry.TDefPage)
                    {
                        await TryReclaimTrailingRealIdxAsync(fkEntry.TDefPage, cancellationToken).ConfigureAwait(false);
                    }
                }
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
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => RenameRelationshipCoreAsync(oldName, newName, cancellationToken), cancellationToken);

    private async ValueTask RenameRelationshipCoreAsync(string oldName, string newName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(oldName, nameof(oldName));
        Guard.NotNullOrEmpty(newName, nameof(newName));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (string.Equals(oldName, newName, StringComparison.OrdinalIgnoreCase))
        {
            return; // No-op; matches Microsoft Access' designer behaviour.
        }

        long msysRelTdefPage = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to rename.");
        }

        TableDef msysRelDef = await ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

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

        // Update the TDEF logical-idx name cookies on both sides so the
        // on-disk index name matches the catalog row. Jet3 never received
        // FK logical-idx entries, so this is a no-op there.
        if (_format != DatabaseFormat.Jet3Mdb)
        {
            // Group matches by (PK table, FK table) pair so each TDEF is
            // rewritten at most once per side, mirroring DropRelationshipAsync.
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

            foreach (KeyValuePair<(string Pk, string Fk), List<RelationshipRowSnapshot>> pair in byTablePair)
            {
                cancellationToken.ThrowIfCancellationRequested();

                CatalogEntry? pkEntry = await GetCatalogEntryAsync(pair.Key.Pk, cancellationToken).ConfigureAwait(false);
                CatalogEntry? fkEntry = await GetCatalogEntryAsync(pair.Key.Fk, cancellationToken).ConfigureAwait(false);
                if (pkEntry == null || fkEntry == null)
                {
                    continue;
                }

                TableDef pkDef = await ReadRequiredTableDefAsync(pkEntry.TDefPage, pair.Key.Pk, cancellationToken).ConfigureAwait(false);
                TableDef fkDef = await ReadRequiredTableDefAsync(fkEntry.TDefPage, pair.Key.Fk, cancellationToken).ConfigureAwait(false);

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

                // Reproduce the cookie-naming convention from CreateRelationshipAsync:
                // PK side uses the relationship name; FK side appends "_FK"
                // when both endpoints land on the same TDEF (self-referential).
                string newPkBase = newName;
                string newFkBase = pkEntry.TDefPage == fkEntry.TDefPage
                    ? newName + "_FK"
                    : newName;

                string newPkName = await PickUniqueLogicalIdxNameAsync(pkEntry.TDefPage, newPkBase, cancellationToken).ConfigureAwait(false);
                _ = await TryRenameFkLogicalIdxNameAsync(pkEntry.TDefPage, pkColNums, fkEntry.TDefPage, newPkName, cancellationToken).ConfigureAwait(false);

                string newFkName = await PickUniqueLogicalIdxNameAsync(fkEntry.TDefPage, newFkBase, cancellationToken).ConfigureAwait(false);
                _ = await TryRenameFkLogicalIdxNameAsync(fkEntry.TDefPage, fkColNums, pkEntry.TDefPage, newFkName, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Reads the existing logical-idx names from the TDEF at
    /// <paramref name="tdefPage"/> and returns
    /// <paramref name="baseName"/> if it is unique, otherwise a
    /// <c>baseName_N</c> variant. Same algorithm as
    /// <see cref="IndexHelpers.MakeUniqueLogicalIdxName"/>; this overload reads the TDEF
    /// for callers that have only the page number.
    /// </summary>
    private async ValueTask<string> PickUniqueLogicalIdxNameAsync(
        long tdefPage,
        string baseName,
        CancellationToken cancellationToken)
    {
        byte[] pageBytes = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (pageBytes[0] != 0x02 || Ru32(pageBytes, 4) != 0)
            {
                return baseName;
            }

            int numCols = Ru16(pageBytes, _tdNumCols);
            int numIdx = Ri32(pageBytes, _tdNumCols + 2);
            int numRealIdx = Ri32(pageBytes, _tdNumRealIdx);
            if (numCols < 0 || numCols > 4096 || numIdx <= 0 || numIdx > 1000
                || numRealIdx < 0 || numRealIdx > 1000)
            {
                return baseName;
            }

            int realIdxDescStart = LocateRealIdxDescStart(pageBytes, numCols, numRealIdx);
            if (realIdxDescStart < 0)
            {
                return baseName;
            }

            const int RealIdxPhysSz = 52;
            const int LogIdxEntrySz = 28;
            int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);
            int logIdxNamesStart = logIdxStart + (numIdx * LogIdxEntrySz);
            List<string> existing = ReadLogicalIdxNames(pageBytes, logIdxNamesStart, numIdx);
            return IndexHelpers.MakeUniqueLogicalIdxName(baseName, existing);
        }
        finally
        {
            ReturnPage(pageBytes);
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
                    string name = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (string.IsNullOrEmpty(name) || !namePredicate(name))
                    {
                        continue;
                    }

                    var values = new object[msysRelDef.Columns.Count];
                    for (int c = 0; c < values.Length; c++)
                    {
                        ColumnInfo col = msysRelDef.Columns[c];
                        string raw = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, col);
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
                        DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, objCol),
                        DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, refObjCol),
                        DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, colCol),
                        DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, refColCol),
                        ParseInt32(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, icolCol)),
                        ParseInt32(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, ccolCol)),
                        ParseInt32(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, grbitCol)),
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
    /// <paramref name="otherTdefPage"/>. Returns the real-idx slot number that
    /// the removed FK entry referenced (so the caller can attempt
    /// <see cref="TryReclaimTrailingRealIdxAsync"/>), or <c>-1</c> when no
    /// matching entry exists (already removed, or never created — Jet3 path,
    /// or out-of-band catalog).
    /// </summary>
    private async ValueTask<int> TryRemoveFkLogicalIdxEntryAsync(
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
            // Multi-page TDEF — out of scope for in-place mutation.
            return -1;
        }

        int numCols = Ru16(td, _tdNumCols);
        int numIdx = Ri32(td, _tdNumCols + 2);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numCols < 0 || numCols > 4096
            || numIdx <= 0 || numIdx > 1000
            || numRealIdx <= 0 || numRealIdx > 1000)
        {
            return -1;
        }

        const int RealIdxPhysSz = 52;
        const int LogIdxEntrySz = 28;

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return -1;
        }

        int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);
        int logIdxNamesStart = logIdxStart + (numIdx * LogIdxEntrySz);
        int logIdxNamesLen = MeasureLogicalIdxNamesLength(td, logIdxNamesStart, numIdx);
        if (logIdxNamesLen < 0)
        {
            return -1;
        }

        // Locate the matching logical-idx entry. Iterate in order so we can
        // also locate its name by walking the names list to the same index.
        int matchEntryIdx = -1;
        int releasedRealIdxNum = -1;
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
            if (!IndexHelpers.RealIdxColMapMatches(td, phys, columnNumbers))
            {
                continue;
            }

            matchEntryIdx = li;
            releasedRealIdxNum = realIdxNum;
            break;
        }

        if (matchEntryIdx < 0)
        {
            return -1;
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
                return -1;
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
            return -1;
        }

        // Compute trailing block bounds (mirror of EmitFkLogicalIdxAsync).
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
            return -1;
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
        return releasedRealIdxNum;
    }

    /// <summary>
    /// After a FK logical-idx removal, attempts to reclaim trailing real-idx
    /// physical descriptor slots that are no longer referenced by any
    /// logical-idx entry. Conservatively reclaims only contiguous slots at
    /// the end of the real-idx array (i.e. <c>numRealIdx - 1</c> down to the
    /// first still-referenced slot) so that no still-referenced slot's index
    /// shifts. This avoids the cross-TDEF index renumbering that a generic
    /// mid-array compaction would require (the OTHER table's logical-idx
    /// entries store this TDEF's slot number in <c>rel_idx_num</c>).
    /// <para>
    /// In the common case — relationship freshly created, FK got the last
    /// slot, then dropped — this reclaims exactly one slot. After multiple
    /// drops in any order, every now-trailing orphan is reclaimed.
    /// </para>
    /// <para>
    /// Removes both the corresponding entry from the leading real-idx skip
    /// block (<c>num_real_idx × _realIdxEntrySz</c> bytes immediately after
    /// the Jet4 TDEF block) and the trailing 52-byte physical descriptor,
    /// decrements <c>num_real_idx</c>, and updates <c>tdef_len</c>.
    /// </para>
    /// <para>
    /// Jet4 / ACE only — Jet3 takes the same path because the FK logical-idx
    /// entries are not emitted on Jet3 to begin with, so this is never called
    /// against a Jet3 TDEF.
    /// </para>
    /// </summary>
    private async ValueTask TryReclaimTrailingRealIdxAsync(
        long tdefPage,
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
            return;
        }

        int numCols = Ru16(td, _tdNumCols);
        int numIdx = Ri32(td, _tdNumCols + 2);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numCols < 0 || numCols > 4096
            || numIdx < 0 || numIdx > 1000
            || numRealIdx <= 0 || numRealIdx > 1000)
        {
            return;
        }

        const int RealIdxPhysSz = 52;
        const int LogIdxEntrySz = 28;

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return;
        }

        int logIdxStart = realIdxDescStart + (numRealIdx * RealIdxPhysSz);
        int logIdxNamesStart = logIdxStart + (numIdx * LogIdxEntrySz);
        int logIdxNamesLen = MeasureLogicalIdxNamesLength(td, logIdxNamesStart, numIdx);
        if (logIdxNamesLen < 0)
        {
            return;
        }

        // Build the set of real-idx slots that are still referenced by some
        // logical-idx entry. A logical-idx points at one real-idx via
        // index_num2 (offset +8 in the 28-byte Jet4 entry).
        var referenced = new bool[numRealIdx];
        for (int li = 0; li < numIdx; li++)
        {
            int e = logIdxStart + (li * LogIdxEntrySz);
            int realIdxNum = Ri32(td, e + 8);
            if (realIdxNum >= 0 && realIdxNum < numRealIdx)
            {
                referenced[realIdxNum] = true;
            }
        }

        // Count contiguous trailing unreferenced slots.
        int reclaim = 0;
        for (int ri = numRealIdx - 1; ri >= 0 && !referenced[ri]; ri--)
        {
            reclaim++;
        }

        if (reclaim == 0)
        {
            return;
        }

        int storedTdefLen = Ri32(td, 8);
        int currentEnd = storedTdefLen + 8;
        int trailingStart = logIdxNamesStart + logIdxNamesLen;
        if (currentEnd < trailingStart)
        {
            currentEnd = trailingStart;
        }

        int trailingLen = currentEnd - trailingStart;
        if (trailingLen < 0 || trailingStart + trailingLen > td.Length)
        {
            return;
        }

        // Step 1 — drop the trailing N entries (12 bytes each on Jet4) from
        // the leading real-idx skip block. The skip block lives at
        // [_tdBlockEnd, _tdBlockEnd + numRealIdx * _realIdxEntrySz). We
        // collapse out the LAST N × _realIdxEntrySz bytes of that block by
        // left-shifting everything that follows.
        int oldSkipEnd = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int newSkipEnd = oldSkipEnd - (reclaim * _realIdxEntrySz);
        int afterSkip = oldSkipEnd;
        Buffer.BlockCopy(td, afterSkip, td, newSkipEnd, currentEnd - afterSkip);
        int endAfterStep1 = currentEnd - (reclaim * _realIdxEntrySz);

        // After step 1 the real-idx physical descriptor section starts at
        // (realIdxDescStart - reclaim * _realIdxEntrySz). We need to drop the
        // trailing N × 52 bytes of physical descriptors. Compute the new
        // boundaries.
        int newRealIdxDescStart = realIdxDescStart - (reclaim * _realIdxEntrySz);
        int newPhysEnd = newRealIdxDescStart + ((numRealIdx - reclaim) * RealIdxPhysSz);
        int oldPhysEnd = newRealIdxDescStart + (numRealIdx * RealIdxPhysSz);

        // Step 2 — drop the trailing N × 52-byte physical descriptors by
        // left-shifting the logical-idx entries + names + variable-col block.
        Buffer.BlockCopy(td, oldPhysEnd, td, newPhysEnd, endAfterStep1 - oldPhysEnd);
        int finalEnd = endAfterStep1 - (reclaim * RealIdxPhysSz);

        // Zero the freed tail so the on-disk page matches the prior
        // fresh-buffer behavior (bytes past the new end are padding).
        Array.Clear(td, finalEnd, currentEnd - finalEnd);

        // Update header counts.
        Wi32(td, _tdNumRealIdx, numRealIdx - reclaim);
        Wi32(td, 8, finalEnd - 8);

        await WritePageAsync(tdefPage, td, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Renames the FK logical-idx "name cookie" on <paramref name="tdefPage"/>
    /// for the entry whose backing real-idx col_map exactly covers
    /// <paramref name="columnNumbers"/> AND whose <c>rel_tbl_page</c> equals
    /// <paramref name="otherTdefPage"/>. Returns <see langword="true"/> when
    /// an entry was found and renamed; <see langword="false"/> otherwise
    /// (already renamed, never created — Jet3, multi-page TDEF, or out-of-band
    /// catalog). Variable-length name records: shrink/grow is handled by
    /// shifting the trailing variable-column block; growth that would push the
    /// TDEF past one page returns <see langword="false"/>.
    /// </summary>
    private async ValueTask<bool> TryRenameFkLogicalIdxNameAsync(
        long tdefPage,
        int[] columnNumbers,
        long otherTdefPage,
        string newName,
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

        // Locate the matching FK logical-idx entry.
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
            if (!IndexHelpers.RealIdxColMapMatches(td, phys, columnNumbers))
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

        // Walk the names section to find the byte offset/length of the entry
        // we want to rewrite.
        int namePos = logIdxNamesStart;
        int oldNameStart = -1;
        int oldNameLen = 0;
        for (int i = 0; i < numIdx; i++)
        {
            int before = namePos;
            if (ReadColumnName(td, ref namePos, out _) < 0)
            {
                return false;
            }

            if (i == matchEntryIdx)
            {
                oldNameStart = before;
                oldNameLen = namePos - before;
                break;
            }
        }

        if (oldNameStart < 0)
        {
            return false;
        }

        // Trailing variable-column block bounds.
        int trailingStart = logIdxNamesStart + logIdxNamesLen;
        int storedTdefLen = Ri32(td, 8);
        int currentEnd = storedTdefLen + 8;
        if (currentEnd < trailingStart)
        {
            currentEnd = trailingStart;
        }

        if (currentEnd > td.Length)
        {
            return false;
        }

        byte[] newNameBytes = Encoding.Unicode.GetBytes(newName);
        int newNameRecordSize = 2 + newNameBytes.Length;
        int delta = newNameRecordSize - oldNameLen;

        int finalEnd = currentEnd + delta;
        if (finalEnd > _pgSz || finalEnd < trailingStart)
        {
            return false;
        }

        // Shift the bytes between (oldNameStart + oldNameLen) and currentEnd
        // by delta. This covers the rest of the names section + the variable
        // -column trailing block in one move. Buffer.BlockCopy handles
        // overlapping regions.
        int afterOldName = oldNameStart + oldNameLen;
        int tailLen = currentEnd - afterOldName;
        if (tailLen > 0)
        {
            Buffer.BlockCopy(td, afterOldName, td, afterOldName + delta, tailLen);
        }

        // If we shrank, zero the freed tail bytes; if we grew, the prior
        // contents have already been overwritten by the shift.
        if (delta < 0)
        {
            Array.Clear(td, finalEnd, -delta);
        }

        // Write the new length-prefixed name into the freed slot.
        Wu16(td, oldNameStart, newNameBytes.Length);
        Buffer.BlockCopy(newNameBytes, 0, td, oldNameStart + 2, newNameBytes.Length);

        // Update tdef_len.
        Wi32(td, 8, finalEnd - 8);

        await WritePageAsync(tdefPage, td, cancellationToken).ConfigureAwait(false);
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
    // Foreign-key runtime enforcement on Insert / Update / Delete
    // ════════════════════════════════════════════════════════════════
    //
    // Honors the EnforceReferentialIntegrity / CascadeUpdates / CascadeDeletes
    // flags emitted into MSysRelationships.grbit by CreateRelationshipAsync.
    // When a relationship has EnforceReferentialIntegrity=false the row
    // simply does not enter the enforced-set returned by
    // GetEnforcedRelationshipsAsync.
    //
    // Enforcement strategy is intentionally simple — there is no SQL engine
    // and no index seek; every check scans the relevant table snapshot once
    // per public mutation call (NOT once per row). For bulk InsertRowsAsync
    // the parent-key set is built lazily on first FK violation check and
    // reused across all rows in the same call. Self-referential inserts
    // augment the cached set after each successful insert so a row that
    // satisfies its own FK can be inserted.
    //
    // See docs/design/index-and-relationship-format-notes.md §7.

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
    private sealed class FkContext(IReadOnlyList<AccessWriter.FkRelationship> all)
    {
        public IReadOnlyList<FkRelationship> All { get; } = all;

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

        /// <summary>
        /// Gets the per-relationship cached resolution of the child (FK)
        /// table's real-idx covering the relationship's foreign columns.
        /// Used by <see cref="EnforceFkOnPrimaryDeleteAsync"/> /
        /// <see cref="EnforceFkOnPrimaryUpdateAsync"/> to locate dependent
        /// child rows by parent-key seek instead of an O(N) child snapshot
        /// scan. A value of <see langword="null"/> means "resolution
        /// attempted and the index is not usable" — caller falls back to
        /// the snapshot path.
        /// </summary>
        public Dictionary<string, ChildSeekIndex?> ChildSeekIndexes { get; }
            = new(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Loads every enforced foreign-key relationship from the
    /// <c>MSysRelationships</c> system table. Returns an empty list when the
    /// database does not contain that table or contains no enforced rows.
    /// </summary>
    private async ValueTask<IReadOnlyList<FkRelationship>> GetEnforcedRelationshipsAsync(CancellationToken cancellationToken)
    {
        long pg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (pg == 0)
        {
            return [];
        }

        DataTable t = await ReadTableSnapshotAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
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

                // grbit bits (Jackcess RelationshipImpl):
                //   0x00000002 NO_REFERENTIAL_INTEGRITY
                //   0x00000100 CASCADE_UPDATES
                //   0x00001000 CASCADE_DELETES
                bool enforce = (grbit & Constants.RelationshipFlags.NoRefIntegrity) == 0;
                if (!enforce)
                {
                    continue;
                }

                bool cascadeUpdates = (grbit & Constants.RelationshipFlags.CascadeUpdates) != 0;
                bool cascadeDeletes = (grbit & Constants.RelationshipFlags.CascadeDeletes) != 0;

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
                    string? k = IndexHelpers.BuildCompositeKey(row.ItemArray, idx);
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

            string? key = IndexHelpers.BuildCompositeKey(values, fkIdx);
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

                byte[]? encodedKey = IndexHelpers.TryEncodeSeekKey(seekIdx, values);
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
    /// Locates (and caches inside <paramref name="ctx"/>) the child (FK) table's
    /// real-idx whose col_map exactly covers <paramref name="rel"/>'s
    /// ForeignColumns in declaration order. Returns <see langword="null"/>
    /// when no covering index exists, when the format is Jet3, when the
    /// child TDEF cannot be resolved, or when any FK column type is not
    /// seekable (the cascade enforcement loop falls back to the snapshot
    /// scan in that case).
    /// </summary>
    private async ValueTask<ChildSeekIndex?> ResolveChildSeekIndexAsync(
        FkRelationship rel,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        if (ctx.ChildSeekIndexes.TryGetValue(rel.Name, out ChildSeekIndex? cached))
        {
            return cached;
        }

        ChildSeekIndex? resolved = null;
        try
        {
            if (_format == DatabaseFormat.Jet3Mdb)
            {
                return null;
            }

            CatalogEntry? childEntry = await GetCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            if (childEntry == null)
            {
                return null;
            }

            TableDef childDef = await ReadRequiredTableDefAsync(childEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);

            var fkColNums = new int[rel.ForeignColumns.Count];
            var fkColTypes = new byte[rel.ForeignColumns.Count];
            for (int i = 0; i < rel.ForeignColumns.Count; i++)
            {
                int idx = childDef.FindColumnIndex(rel.ForeignColumns[i]);
                if (idx < 0)
                {
                    return null;
                }

                fkColNums[i] = childDef.Columns[idx].ColNum;
                fkColTypes[i] = childDef.Columns[idx].Type;
            }

            (long FirstDp, IReadOnlyList<bool> AscendingFlags)? hit = await TryFindCoveringRealIdxAsync(
                childEntry.TDefPage,
                fkColNums,
                cancellationToken).ConfigureAwait(false);
            if (hit == null)
            {
                return null;
            }

            for (int i = 0; i < fkColTypes.Length; i++)
            {
                if (!IndexKeyEncoder.IsColumnTypeSeekable(fkColTypes[i]))
                {
                    return null;
                }
            }

            var keyColumns = new ChildSeekKeyColumn[fkColNums.Length];
            for (int i = 0; i < fkColNums.Length; i++)
            {
                keyColumns[i] = new ChildSeekKeyColumn(fkColTypes[i], hit.Value.AscendingFlags[i]);
            }

            resolved = new ChildSeekIndex(hit.Value.FirstDp, keyColumns);
            return resolved;
        }
        finally
        {
            ctx.ChildSeekIndexes[rel.Name] = resolved;
        }
    }

    /// <summary>
    /// Resolves key column infos from index key columns, table columns, and snapshot index mapping.
    /// Returns false if any column cannot be resolved.
    /// </summary>
    private static bool TryResolveKeyColumnInfos(
        IReadOnlyList<IndexKeyColumn> indexKeyColumns,
        List<ColumnInfo> tableColumns,
        Dictionary<int, int> snapshotIndexByColNum,
        out List<KeyColumnInfo> keyColInfos)
    {
        keyColInfos = new List<KeyColumnInfo>(indexKeyColumns.Count);
        foreach (var (colNum, ascending) in indexKeyColumns)
        {
            ColumnInfo? col = tableColumns.Find(c => c.ColNum == colNum);
            if (col is null || !snapshotIndexByColNum.TryGetValue(colNum, out int snapIdx))
            {
                keyColInfos = null!;
                return false;
            }

            keyColInfos.Add(new KeyColumnInfo(tableColumns[snapIdx], snapIdx, ascending));
        }

        return true;
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

            string? key = IndexHelpers.BuildCompositeKey(insertedValues, pkIdx);
            if (key != null)
            {
                _ = set.Add(key);
            }
        }
    }

    /// <summary>
    /// For each enforced FK whose primary side is <paramref name="primaryTable"/>,
    /// inspects the parent rows currently being deleted (full typed rows in
    /// primary-table column order in <paramref name="deletedParentRows"/>)
    /// and either cascades the delete to the child table when
    /// <see cref="FkRelationship.CascadeDeletes"/> is set, or throws when
    /// child rows still reference one of the deleted PK tuples.
    /// <para>
    /// When a covering FK-side real-idx is available
    /// (<see cref="ResolveChildSeekIndexAsync"/>), each parent PK tuple is
    /// encoded once and seeked through <see cref="IndexBTreeSeeker.FindRowLocationsAsync"/>
    /// to locate dependent child rows in O(log N + K) page reads. Falls
    /// back to the legacy O(N) child-table snapshot scan when the seek
    /// path is not usable (Jet3, no covering child index, encoder-rejected
    /// key types, child row contains LVAL-only columns the single-row
    /// reader cannot decode for the recursive grandchild step).
    /// </para>
    /// </summary>
    private async ValueTask EnforceFkOnPrimaryDeleteAsync(
        string primaryTable,
        TableDef primaryDef,
        List<object?[]> deletedParentRows,
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

            var fkIdx = new int[rel.ForeignColumns.Count];
            var primaryPkIdx = new int[rel.PrimaryColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.ForeignColumns.Count; i++)
            {
                fkIdx[i] = childDef.FindColumnIndex(rel.ForeignColumns[i]);
                primaryPkIdx[i] = primaryDef.FindColumnIndex(rel.PrimaryColumns[i]);
                if (fkIdx[i] < 0 || primaryPkIdx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            // Build per-rel typed parent PK arrays (in rel.PrimaryColumns
            // declaration order) once for both the seek and the snapshot
            // fallback paths.
            var parentPkRows = new List<object?[]>(deletedParentRows.Count);
            foreach (object?[] row in deletedParentRows)
            {
                var pk = new object?[primaryPkIdx.Length];
                bool nullPart = false;
                for (int i = 0; i < primaryPkIdx.Length; i++)
                {
                    object? v = row[primaryPkIdx[i]];
                    if (v is DBNull)
                    {
                        v = null;
                    }

                    if (v == null)
                    {
                        nullPart = true;
                        break;
                    }

                    pk[i] = v;
                }

                if (!nullPart)
                {
                    parentPkRows.Add(pk);
                }
            }

            if (parentPkRows.Count == 0)
            {
                continue;
            }

            // Fast path: child-side index seek.
            ChildSeekIndex? childSeek = await ResolveChildSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (childSeek != null)
            {
                bool seekOk = await TryProcessCascadeDeleteWithSeekAsync(
                    rel,
                    childEntry,
                    childDef,
                    childSeek,
                    parentPkRows,
                    ctx,
                    depth,
                    cancellationToken).ConfigureAwait(false);
                if (seekOk)
                {
                    continue;
                }
            }

            // ── Snapshot fallback ─────────────────────────────────────
            using DataTable childSnap = await ReadTableSnapshotAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            List<RowLocation> locations = await GetLiveRowLocationsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            int total = Math.Min(childSnap.Rows.Count, locations.Count);

            // Build deletedSet from typed parent PK arrays (composite-key
            // strings keyed off rel.PrimaryColumns ordering). Equivalent to
            // the legacy BuildCompositeKey path but driven by the typed
            // parent rows we received from the caller.
            var deletedSet = new HashSet<string>(StringComparer.Ordinal);
            int[] identity = new int[primaryPkIdx.Length];
            for (int i = 0; i < identity.Length; i++)
            {
                identity[i] = i;
            }

            foreach (object?[] pk in parentPkRows)
            {
                string? k = IndexHelpers.BuildCompositeKey(pk, identity);
                if (k != null)
                {
                    _ = deletedSet.Add(k);
                }
            }

            var matchingRowIndices = new List<int>();
            for (int i = 0; i < total; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = IndexHelpers.BuildCompositeKey(childSnap.Rows[i].ItemArray, fkIdx);
                if (childKey == null)
                {
                    continue;
                }

                if (deletedSet.Contains(childKey))
                {
                    matchingRowIndices.Add(i);
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

            // Capture the child rows' OWN full typed values (in child-table
            // column order) before we delete them so any grandchild relationship
            // can be recursed via the same EnforceFkOnPrimaryDeleteAsync entry.
            var childDeletedRows = new List<object?[]>(matchingRowIndices.Count);
            foreach (int rIdx in matchingRowIndices)
            {
                childDeletedRows.Add(childSnap.Rows[rIdx].ItemArray);
            }

            await EnforceFkOnPrimaryDeleteAsync(
                rel.ForeignTable,
                childDef,
                childDeletedRows,
                ctx,
                depth + 1,
                cancellationToken).ConfigureAwait(false);

            // Cascade complex flat-child rows for the child rows we are
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
    /// Fast path for cascade-delete: locate dependent child rows by
    /// seeking the child (FK) table's covering index for each parent PK
    /// tuple. Returns <see langword="false"/> when the seek path cannot be
    /// completed (any parent key fails to encode, or any matched child row
    /// contains an LVAL column the single-row reader cannot decode for the
    /// recursive grandchild step) — caller falls back to the snapshot scan.
    /// </summary>
    private async ValueTask<bool> TryProcessCascadeDeleteWithSeekAsync(
        FkRelationship rel,
        CatalogEntry childEntry,
        TableDef childDef,
        ChildSeekIndex childSeek,
        IReadOnlyList<object?[]> parentPkRows,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        // De-duplicate matched row locations across parent keys (a child row
        // is uniquely identified by (page, rowIndex)).
        var seen = new HashSet<long>();
        var matchedLocations = new List<(long DataPage, int RowIndex)>();
        foreach (object?[] pk in parentPkRows)
        {
            byte[]? encoded = IndexHelpers.TryEncodeChildSeekKey(childSeek, pk);
            if (encoded == null)
            {
                return false;
            }

            List<(long DataPage, int RowIndex)> hits = await IndexBTreeSeeker.FindRowLocationsAsync(
                (page, ct) => ReadPageOwnedAsync(page, ct),
                _pgSz,
                childSeek.RootPage,
                encoded,
                cancellationToken).ConfigureAwait(false);

            foreach ((long dp, int ri) in hits)
            {
                long key = (dp << 16) | (uint)ri;
                if (seen.Add(key))
                {
                    matchedLocations.Add((dp, ri));
                }
            }
        }

        if (matchedLocations.Count == 0)
        {
            return true;
        }

        if (!rel.CascadeDeletes)
        {
            throw new InvalidOperationException(
                $"DELETE on '{rel.PrimaryTable}' violates foreign-key constraint '{rel.Name}': " +
                $"{matchedLocations.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
        }

        // Resolve full row bounds (RowStart / RowSize) via a one-pass live
        // location enumeration of just the pages we need. Cheaper than a
        // full snapshot read when the matched set is small. We index the
        // matched (page, rowIndex) tuples by page for the lookup.
        var matchedByPage = new Dictionary<long, HashSet<int>>();
        foreach ((long dp, int ri) in matchedLocations)
        {
            if (!matchedByPage.TryGetValue(dp, out HashSet<int>? rs))
            {
                rs = new HashSet<int>();
                matchedByPage[dp] = rs;
            }

            _ = rs.Add(ri);
        }

        var fullLocations = new List<RowLocation>(matchedLocations.Count);
        foreach (KeyValuePair<long, HashSet<int>> kvp in matchedByPage)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await ReadPageAsync(kvp.Key, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != childEntry.TDefPage)
                {
                    // The seek pointer led to an unexpected page — bail.
                    return false;
                }

                foreach (RowBound rb in EnumerateLiveRowBounds(page))
                {
                    if (kvp.Value.Contains(rb.RowIndex))
                    {
                        fullLocations.Add(new RowLocation(kvp.Key, rb.RowIndex, rb.RowStart, rb.RowSize));
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        if (fullLocations.Count != matchedLocations.Count)
        {
            // Index points at row indices the live-row enumerator could not
            // confirm — stale index pages or page-pool race. Fall back.
            return false;
        }

        // Read each matched child row's full typed values for the recursive
        // grandchild step. If any contains a column the single-row reader
        // cannot decode (LVAL: T_MEMO/T_OLE/T_COMPLEX/T_ATTACHMENT), bail.
        var allColumnOrdinals = new int[childDef.Columns.Count];
        for (int i = 0; i < childDef.Columns.Count; i++)
        {
            allColumnOrdinals[i] = i;
        }

        var childDeletedRows = new List<object?[]>(fullLocations.Count);
        foreach (RowLocation loc in fullLocations)
        {
            object?[]? values = await TryReadColumnValuesTypedAsync(loc, childDef, allColumnOrdinals, cancellationToken).ConfigureAwait(false);
            if (values == null)
            {
                return false;
            }

            // Promote bare-null entries to DBNull.Value so downstream code
            // (which uses DataRow.ItemArray semantics) sees the same shape.
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                {
                    values[i] = DBNull.Value;
                }
            }

            childDeletedRows.Add(values);
        }

        await EnforceFkOnPrimaryDeleteAsync(
            rel.ForeignTable,
            childDef,
            childDeletedRows,
            ctx,
            depth + 1,
            cancellationToken).ConfigureAwait(false);

        await CascadeDeleteComplexChildrenAsync(childDef, fullLocations, cancellationToken).ConfigureAwait(false);

        int deleted = 0;
        foreach (RowLocation loc in fullLocations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted > 0)
        {
            await AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
            await MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        }

        return true;
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
        TableDef primaryDef,
        IReadOnlyList<(string? OldKey, object?[] OldFullRow, object[] NewPkValues)> changes,
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

            // Map this relationship's PK column ordinals on the PRIMARY-side def
            // (we need to extract the new PK tuple from NewPkValues which is in
            // primary-table column order, and the old PK tuple from OldFullRow).
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

            // Build (oldKey -> (oldPkSubset, newPkSubset)) for changes whose PK
            // actually moves. movingChanges keys remain the synthetic OldKey
            // string so the snapshot fallback (which composes child-side keys
            // via BuildCompositeKey) can still match.
            var movingChanges = new Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)>(StringComparer.Ordinal);
            foreach ((string? oldKey, object?[] oldFullRow, object[] newPkValues) in changes)
            {
                if (oldKey == null)
                {
                    continue;
                }

                string? newKey = IndexHelpers.BuildCompositeKey(newPkValues, primaryPkIdx);
                if (newKey == null || string.Equals(newKey, oldKey, StringComparison.Ordinal))
                {
                    continue;
                }

                var newPkSubset = new object[rel.PrimaryColumns.Count];
                var oldPkSubset = new object?[rel.PrimaryColumns.Count];
                for (int i = 0; i < rel.PrimaryColumns.Count; i++)
                {
                    newPkSubset[i] = newPkValues[primaryPkIdx[i]];
                    oldPkSubset[i] = oldFullRow[primaryPkIdx[i]];
                }

                movingChanges[oldKey] = (oldPkSubset, newPkSubset);
            }

            if (movingChanges.Count == 0)
            {
                continue;
            }

            // Fast path: child-side index seek.
            ChildSeekIndex? childSeek = await ResolveChildSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (childSeek != null)
            {
                bool seekOk = await TryProcessCascadeUpdateWithSeekAsync(
                    rel,
                    childEntry,
                    childDef,
                    childSeek,
                    movingChanges,
                    fkIdx,
                    cancellationToken).ConfigureAwait(false);
                if (seekOk)
                {
                    continue;
                }
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
                string? childKey = IndexHelpers.BuildCompositeKey(childSnap.Rows[i].ItemArray, fkIdx);
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
                object[] newPkSubset = movingChanges[affectedOldKeys[ai]].NewPkSubset;
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
    /// Fast path for cascade-update: for each (oldKey, oldPkSubset, newPkSubset)
    /// triplet, encode the OLD PK subset as a child-index seek key, locate
    /// every dependent child row in O(log N + K) page reads, fetch each
    /// matched row's full typed values, and rewrite it in place via
    /// delete + insert with the new FK values. Returns <see langword="false"/>
    /// when any old PK fails to encode, when matched (page, rowIndex) hits
    /// cannot be confirmed against live row bounds, or when any matched
    /// child row contains an LVAL column the single-row reader cannot
    /// decode — caller falls back to the snapshot scan.
    /// </summary>
    private async ValueTask<bool> TryProcessCascadeUpdateWithSeekAsync(
        FkRelationship rel,
        CatalogEntry childEntry,
        TableDef childDef,
        ChildSeekIndex childSeek,
        Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)> movingChanges,
        int[] fkIdx,
        CancellationToken cancellationToken)
    {
        // Step 1: seek every old PK and collect (location, newPkSubset).
        // Dedupe across keys by (dataPage, rowIndex).
        var pendingByLocation = new Dictionary<long, (long DataPage, int RowIndex, object[] NewPkSubset)>();
        foreach (KeyValuePair<string, (object?[] OldPkSubset, object[] NewPkSubset)> kvp in movingChanges)
        {
            byte[]? encoded = IndexHelpers.TryEncodeChildSeekKey(childSeek, kvp.Value.OldPkSubset);
            if (encoded == null)
            {
                return false;
            }

            List<(long DataPage, int RowIndex)> hits = await IndexBTreeSeeker.FindRowLocationsAsync(
                (page, ct) => ReadPageOwnedAsync(page, ct),
                _pgSz,
                childSeek.RootPage,
                encoded,
                cancellationToken).ConfigureAwait(false);

            foreach ((long dp, int ri) in hits)
            {
                long key = (dp << 16) | (uint)ri;
                if (!pendingByLocation.ContainsKey(key))
                {
                    pendingByLocation[key] = (dp, ri, kvp.Value.NewPkSubset);
                }
            }
        }

        if (pendingByLocation.Count == 0)
        {
            return true;
        }

        if (!rel.CascadeUpdates)
        {
            throw new InvalidOperationException(
                $"UPDATE on '{rel.PrimaryTable}' violates foreign-key constraint '{rel.Name}': " +
                $"{pendingByLocation.Count} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
        }

        // Step 2: resolve full row bounds (RowStart / RowSize) via per-page
        // live-row enumeration of just the pages we need. Dedupe per-page.
        var locationsByPage = new Dictionary<long, HashSet<int>>();
        foreach ((long dp, int ri, _) in pendingByLocation.Values)
        {
            if (!locationsByPage.TryGetValue(dp, out HashSet<int>? rs))
            {
                rs = new HashSet<int>();
                locationsByPage[dp] = rs;
            }

            _ = rs.Add(ri);
        }

        var rowMeta = new List<(RowLocation Loc, object[] NewPkSubset)>(pendingByLocation.Count);
        foreach (KeyValuePair<long, HashSet<int>> kvp in locationsByPage)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await ReadPageAsync(kvp.Key, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01 || Ri32(page, _dpTDefOff) != childEntry.TDefPage)
                {
                    return false;
                }

                foreach (RowBound rb in EnumerateLiveRowBounds(page))
                {
                    if (kvp.Value.Contains(rb.RowIndex))
                    {
                        long key = (kvp.Key << 16) | (uint)rb.RowIndex;
                        if (pendingByLocation.TryGetValue(key, out (long DataPage, int RowIndex, object[] NewPkSubset) entry))
                        {
                            rowMeta.Add((new RowLocation(kvp.Key, rb.RowIndex, rb.RowStart, rb.RowSize), entry.NewPkSubset));
                        }
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        if (rowMeta.Count != pendingByLocation.Count)
        {
            return false;
        }

        // Step 3: read each matched child row's full typed values and rewrite
        // in place via delete + insert.
        var allColumnOrdinals = new int[childDef.Columns.Count];
        for (int i = 0; i < childDef.Columns.Count; i++)
        {
            allColumnOrdinals[i] = i;
        }

        foreach ((RowLocation loc, object[] newPkSubset) in rowMeta)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object?[]? values = await TryReadColumnValuesTypedAsync(loc, childDef, allColumnOrdinals, cancellationToken).ConfigureAwait(false);
            if (values == null)
            {
                return false;
            }

            object[] rowValues = new object[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                rowValues[i] = values[i] ?? DBNull.Value;
            }

            for (int j = 0; j < fkIdx.Length; j++)
            {
                rowValues[fkIdx[j]] = newPkSubset[j] ?? DBNull.Value;
            }

            await MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, cancellationToken).ConfigureAwait(false);
            await InsertRowDataAsync(childEntry.TDefPage, childDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        await MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        return true;
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
            if (row.ObjectType == Constants.SystemObjects.UserTableType
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
                    string name = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
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
        LockFileSlotWriter? lockSlot = null;
        if (useLockFile)
        {
            lockSlot = LockFileSlotWriter.Open(
                path,
                nameof(AccessWriter),
                respectExisting: respectLockFile,
                machineName: options?.LockFileMachineName,
                userName: options?.LockFileUserName);
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
            lockSlot?.Dispose();
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

    /// <summary>
    /// Begins an explicit page-buffered transaction against this writer. While
    /// the returned <see cref="JetTransaction"/> is active, every page-write
    /// performed by this writer is journaled in memory instead of flushed to
    /// the database file. <see cref="JetTransaction.CommitAsync"/> atomically
    /// replays the journal; <see cref="JetTransaction.RollbackAsync"/> (and
    /// <see cref="JetTransaction.DisposeAsync"/> on an uncommitted transaction)
    /// discards it, leaving the file in its pre-transaction state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The newly-started transaction.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a transaction is already active on this writer (only one
    /// concurrent transaction is supported per <see cref="AccessWriter"/>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown when the writer has been disposed.</exception>
    public async ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(AccessWriter));
        }

        cancellationToken.ThrowIfCancellationRequested();

        await IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_activeTransaction is not null)
            {
                throw new InvalidOperationException(
                    "A transaction is already active on this writer. Only one concurrent transaction per AccessWriter is supported.");
            }

            long baseLength = _stream.Length;
            var journal = new PageJournal(baseLength, PageSize, _maxTransactionPageBudget);
            var tx = new JetTransaction(this, journal);
            ActiveJournal = journal;
            _activeTransaction = tx;
            return tx;
        }
        finally
        {
            _ = IoGate.Release();
        }
    }

    /// <summary>
    /// If <see cref="AccessWriterOptions.UseTransactionalWrites"/> is enabled
    /// and no explicit transaction is currently active, wraps
    /// <paramref name="work"/> in a private <see cref="JetTransaction"/> so a
    /// crash mid-call leaves the database in its pre-call state. Otherwise
    /// invokes <paramref name="work"/> directly (today's flush-per-page path).
    /// </summary>
    private async ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        if (!_useTransactionalWrites || _activeTransaction is not null || _disposed)
        {
            await work(cancellationToken).ConfigureAwait(false);
            return;
        }

        JetTransaction tx = await BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await work(cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            try
            {
                if (!tx.IsTerminated)
                {
                    await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (InvalidOperationException)
            {
                // Already terminated by a concurrent commit/rollback path.
            }
            catch (IOException)
            {
                // Best-effort rollback; surface the original failure.
            }

            throw;
        }
    }

    /// <summary>
    /// Generic-result variant of <see cref="RunAutoCommitAsync(System.Func{System.Threading.CancellationToken, System.Threading.Tasks.ValueTask}, System.Threading.CancellationToken)"/>.
    /// </summary>
    private async ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
    {
        if (!_useTransactionalWrites || _activeTransaction is not null || _disposed)
        {
            return await work(cancellationToken).ConfigureAwait(false);
        }

        JetTransaction tx = await BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            TResult result = await work(cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch
        {
            try
            {
                if (!tx.IsTerminated)
                {
                    await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (InvalidOperationException)
            {
                // Already terminated.
            }
            catch (IOException)
            {
                // Best-effort rollback.
            }

            throw;
        }
    }

    /// <summary>
    /// Commits the supplied <paramref name="transaction"/>: detaches the
    /// journal from the writer and replays each buffered page (in ascending
    /// page-number order) through the normal page-write pipeline so that
    /// per-page encryption and cooperative byte-range locks are honoured.
    /// </summary>
    /// <remarks>
    /// Mirrors the JET page-shadow commit protocol: acquires the cooperative
    /// commit-lock sentinel via <see cref="JetByteRangeLock.AcquireCommitLockAsync"/>,
    /// replays the journal, increments the page-0 commit-lock byte at offset
    /// <c>0x14</c> so other openers can detect the schema/data version bump,
    /// flushes to disk (<c>FileStream.Flush(flushToDisk: true)</c> when the
    /// stream is a <see cref="FileStream"/>), and finally releases the
    /// commit-lock sentinel.
    /// </remarks>
    internal async ValueTask CommitTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(AccessWriter));
        }

        PageJournal journal;
        await IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (transaction.IsTerminated)
            {
                throw new InvalidOperationException("The transaction has already been committed or rolled back.");
            }

            if (!ReferenceEquals(_activeTransaction, transaction))
            {
                throw new InvalidOperationException("The transaction is not active on this writer.");
            }

            journal = transaction.Journal;

            // Detach the journal first so the page-write loop below routes
            // straight to disk (otherwise WritePageAsync would re-journal the
            // same bytes back into the journal we are draining).
            ActiveJournal = null;
            _activeTransaction = null;
        }
        finally
        {
            _ = IoGate.Release();
        }

        // Acquire the JET commit-lock sentinel for the entire replay window so
        // any other cooperating opener (Access, OLE DB JET / ACE, another
        // AccessWriter) blocks on its own commit attempt until our atomic
        // replay + commit-byte bump is durable on disk.
        IDisposable commitLock = _byteRangeLock is null
            ? NullDisposableInstance
            : await _byteRangeLock.AcquireCommitLockAsync(isAccdb: DatabaseFormat == DatabaseFormat.AceAccdb, cancellationToken).ConfigureAwait(false);

        try
        {
            try
            {
                foreach (KeyValuePair<long, byte[]> entry in journal.EnumerateInOrder())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await WritePageAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
                }

                // Bump the page-0 commit-lock byte at offset 0x14 so any reader
                // that participates in the protocol can detect the version
                // change and refresh its catalog cache. Done after page replay
                // and before the durability flush so a torn write that loses
                // the commit-byte update leaves the prior catalog version in
                // force.
                await BumpCommitLockByteAsync(cancellationToken).ConfigureAwait(false);

                // FlushToDisk(true) for FileStream-backed databases makes
                // every preceding write durable past an OS / process crash.
                // Other stream types (MemoryStream, the in-memory ACCDB
                // re-encryption buffer) just FlushAsync.
                await FlushDurableAsync(cancellationToken).ConfigureAwait(false);

                transaction.MarkCommitted();
            }
            catch
            {
                // Mid-commit failure: the on-disk file may now be partially
                // mutated. Mark the transaction terminated so the caller cannot
                // commit again, but propagate the original exception.
                transaction.MarkRolledBack();
                throw;
            }
        }
        finally
        {
            commitLock.Dispose();
        }
    }

    /// <summary>
    /// Increments the page-0 "commit lock byte" at header offset <c>0x14</c>
    /// (with simple wrap-around). Mirrors the JET signal that schema /
    /// catalog state has changed; other cooperating openers consult this byte
    /// to know when to refresh.
    /// </summary>
    private async ValueTask BumpCommitLockByteAsync(CancellationToken cancellationToken)
    {
        // Page 0 is always plaintext (PrepareEncryptedPageForWrite skips it),
        // so we can read-modify-write byte 0x14 without going through the
        // page-encryption pipeline.
        byte[] page0 = await ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
        try
        {
            page0[0x14] = unchecked((byte)(page0[0x14] + 1));
            await WritePageAsync(0, page0, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(page0);
        }
    }

    /// <summary>
    /// Flushes the underlying stream durably. For <see cref="FileStream"/> uses
    /// <see cref="FileStream.Flush(bool)"/> with <c>flushToDisk: true</c> so
    /// the OS write-back cache is forced past the storage device's volatile
    /// cache; for other stream types falls back to <see cref="Stream.FlushAsync(CancellationToken)"/>.
    /// </summary>
    private async ValueTask FlushDurableAsync(CancellationToken cancellationToken)
    {
        await IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_stream is FileStream fs)
            {
                // FileStream.Flush(true) is the only way to push the OS
                // write-back cache through to the storage device's volatile
                // cache; there is no async equivalent in BCL.
#pragma warning disable CA1849
                fs.Flush(flushToDisk: true);
#pragma warning restore CA1849
            }
            else
            {
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = IoGate.Release();
        }
    }

    private static readonly IDisposable NullDisposableInstance = new NullDisposableImpl();

    private sealed class NullDisposableImpl : IDisposable
    {
        public void Dispose()
        {
        }
    }

    /// <summary>
    /// Rolls back the supplied <paramref name="transaction"/>: discards the
    /// in-memory journal without touching the database file.
    /// </summary>
    internal async ValueTask RollbackTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        cancellationToken.ThrowIfCancellationRequested();

        // Allow rollback during dispose: skip the disposed-check so a
        // JetTransaction.DisposeAsync after writer-dispose is a quiet no-op
        // when the writer has already cleaned up the active transaction.
        await IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (transaction.IsTerminated)
            {
                throw new InvalidOperationException("The transaction has already been committed or rolled back.");
            }

            if (!ReferenceEquals(_activeTransaction, transaction))
            {
                throw new InvalidOperationException("The transaction is not active on this writer.");
            }

            ActiveJournal = null;
            _activeTransaction = null;
            transaction.MarkRolledBack();
        }
        finally
        {
            _ = IoGate.Release();
        }
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        // Drop any in-flight transaction so its journal does not survive
        // dispose. Nothing has been written to disk for an uncommitted
        // transaction, so this is equivalent to an implicit rollback.
        if (_activeTransaction is { } pending)
        {
            ActiveJournal = null;
            _activeTransaction = null;
            pending.MarkRolledBack();
        }

        if (_useLockFile)
        {
            _lockFileSlot?.Dispose();
            _lockFileSlot = null;
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
    /// deferred constraint (the post-write unique-index check
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

        // Pre-write unique-index enforcement: reject duplicate keys before
        // any disk page is mutated. The post-write check inside
        // MaintainIndexesAsync still runs as defense-in-depth.
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

            // fast path: try in-place leaf splice for the inserted
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
            // The row hit disk but a deferred constraint (the post-write
            // unique-index check in MaintainIndexesAsync) rejected
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
        // When the row pre-encode pass has already pushed an oversized
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
        // Complex columns: Attachment and Multi-value have dedicated
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

                if (clrType == typeof(Hyperlink))
                {
                    // Hyperlink columns are MEMO + the HYPERLINK_FLAG_MASK (0x80) bit
                    // OR'd into the TDEF column-flag byte by BuildTableDefinition.
                    return T_MEMO;
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

    /// <summary>
    /// Validates and returns the precision (1..28) declared on a
    /// <c>T_NUMERIC</c> column definition. Defaults to <c>18</c> when the
    /// caller leaves <see cref="ColumnDefinition.NumericPrecision"/> at its
    /// initial value (matches Access "Number → Decimal" UI default).
    /// </summary>
    private static byte ResolveNumericPrecision(ColumnDefinition definition)
    {
        byte p = definition.NumericPrecision == 0 ? (byte)18 : definition.NumericPrecision;
        Guard.InRange(p, 1, 28, $"Column '{definition.Name}' NumericPrecision");
        return p;
    }

    /// <summary>
    /// Validates and returns the scale (0..28, &lt;= precision) declared on a
    /// <c>T_NUMERIC</c> column definition. Defaults to <c>0</c> (Access UI
    /// default). The incremental index path uses this value as the
    /// canonical sort-key scale.
    /// </summary>
    private static byte ResolveNumericScale(ColumnDefinition definition)
    {
        byte s = definition.NumericScale;
        byte p = definition.NumericPrecision == 0 ? (byte)18 : definition.NumericPrecision;
        Guard.InRange(s, 0, 28, $"Column '{definition.Name}' NumericScale");
        Guard.InRange(s, 0, p, $"Column '{definition.Name}' NumericScale (NumericPrecision={p})");
        return s;
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

                // Microsoft Access Hyperlink columns are MEMO + the Jackcess
                // HYPERLINK_FLAG_MASK (0x80) bit. Honoured for both the explicit
                // ColumnDefinition.IsHyperlink shortcut and the implicit
                // typeof(Hyperlink) ClrType. The bit only has meaning on T_MEMO
                // columns; reject the combination on any other type so users
                // get a clear error rather than a silently-ignored flag.
                bool wantsHyperlink = definition.IsHyperlink || definition.ClrType == typeof(Hyperlink);
                if (wantsHyperlink)
                {
                    if (type != T_MEMO)
                    {
                        throw new ArgumentException(
                            $"Column '{definition.Name}' has IsHyperlink = true but resolves to JET type 0x{type:X2}; " +
                            "hyperlink columns must be MEMO (string with no MaxLength, or typeof(Hyperlink)).",
                            nameof(columns));
                    }

                    flags |= 0x80;
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
                NumericPrecision = type == T_NUMERIC ? ResolveNumericPrecision(definition) : (byte)0,
                NumericScale = type == T_NUMERIC ? ResolveNumericScale(definition) : (byte)0,
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

    // ── LVAL chain emission for oversized MEMO / OLE / Attachment payloads ──
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
    private sealed class PreEncodedLongValue(byte[] headerBytes)
    {
        public byte[] HeaderBytes { get; } = headerBytes;
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

    private static FileStream CreateStream(string path) =>
        OpenDatabaseFileStream(path, FileAccess.ReadWrite, FileShare.Read, FileOptions.Asynchronous | FileOptions.RandomAccess);

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

    /// <summary>
    /// Adds parentOps Replace and InsertAfter operations for a split leaf/intermediate.
    /// </summary>
    private static void AddParentOp(
        Dictionary<long, List<IntermediateOp>> parentOps,
        long parentPageNumber,
        int originalIndex,
        IntermediateOpType type,
        DecodedIntermediateEntry newEntry)
    {
        IndexHelpers.AddIntermediateOp(parentOps, parentPageNumber, new IntermediateOp(
            OriginalIndex: originalIndex,
            Type: type,
            NewEntry: newEntry));
    }

    private static void AddParentOpsForSplitPages(
        Dictionary<long, List<IntermediateOp>> parentOps,
        long parentPageNumber,
        int takenIndex,
        SplitPages splitPages,
        long[] pageNumbers)
    {
        if (splitPages.Count != pageNumbers.Length || splitPages.Count == 0)
        {
            throw new ArgumentException("splitPages and pageNumbers must have the same nonzero length");
        }

        // Replace op for the leftmost split page
        IndexEntry leftLast = splitPages[0][splitPages[0].Count - 1];
        AddParentOp(parentOps, parentPageNumber, takenIndex, IntermediateOpType.Replace, new(leftLast, pageNumbers[0]));

        // InsertAfter ops for the remaining split pages
        for (int p = 1; p < splitPages.Count; p++)
        {
            IndexEntry pLast = splitPages[p][splitPages[p].Count - 1];
            AddParentOp(parentOps, parentPageNumber, takenIndex, IntermediateOpType.InsertAfter, new(pLast, pageNumbers[p]));
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
    /// Add/Drop/Rename column operations.
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
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if ((unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) != 0)
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
        TableDef msys = await ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", (int)tdefPageNumber);
        msys.SetValueByName(values, "ParentId", 0);
        msys.SetValueByName(values, "Name", tableName);
        msys.SetValueByName(values, "Type", (short)Constants.SystemObjects.UserTableType);
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
                    DefaultValueExpression = target.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, _format)
                        ?? baseDef.DefaultValueExpression,
                    ValidationRuleExpression = target.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, _format)
                        ?? baseDef.ValidationRuleExpression,
                    ValidationText = target.GetTextValue(Constants.ColumnPropertyNames.ValidationText, _format)
                        ?? baseDef.ValidationText,
                    Description = target.GetTextValue(Constants.ColumnPropertyNames.Description, _format)
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
        // surviving index definitions to the rebuilt table.
        using DataTable snapshot = await ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<IndexMetadata> existingIndexes = await ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        // Default index projection: keep every existing index whose single key
        // column survives in the new schema (matched by case-insensitive name).
        // AddColumn / DropColumn use this default; RenameColumn supplies a custom
        // projection that rewrites references to the renamed column.
        List<IndexDefinition> projectedIndexes = projectIndexes != null
            ? projectIndexes(existingIndexes, newDefs)
            : IndexHelpers.DefaultIndexProjection(existingIndexes, newDefs);

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

        // rebuild forwarded indexes once after the bulk row copy completes,
        // so we don't pay the rebuild cost per row.
        if (projectedIndexes.Count > 0 && snapshot.Rows.Count > 0)
        {
            await MaintainIndexesAsync(tempEntry.TDefPage, tempDef, tempName, cancellationToken).ConfigureAwait(false);
        }

        // Drop the original table, then rename the temp catalog entry to take its place.
        // Pre-compute the LvProp blob from the projected columns so the catalog rename
        // re-emits the persisted properties under the user-facing table name.
        //
        // identify complex columns being dropped or renamed by this rewrite
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
        TableDef msys = await ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> rows = await GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);

        long? tdefPage = null;
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
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
                // preserve attachment columns across
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
        ColumnDefinition def = baseDef with
        {
            IsNullable = (column.Flags & 0x02) != 0,
            IsAutoIncrement = (column.Flags & 0x04) != 0,
            IsHyperlink = column.Type == T_MEMO && (column.Flags & 0x80) != 0,
        };

        // Preserve declared precision/scale through the schema-rewrite copy so
        // AddColumn / DropColumn / RenameColumn don't silently reset a NUMERIC
        // column to default 18/0. Access-authored files always populate these
        // descriptor bytes for T_NUMERIC columns.
        if (column.Type == T_NUMERIC)
        {
            def = def with { NumericPrecision = column.NumericPrecision, NumericScale = column.NumericScale };
        }

        return def;
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
    /// after the leafs themselves have been appended.
    /// </summary>
    private (byte[] Page, int[] FirstDpOffsets) BuildTDefPageWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
    {
        byte[] page = new byte[_pgSz];
        int numCols = tableDef.Columns.Count;
        int numIdx = indexes.Count;
        bool jet4 = _format != DatabaseFormat.Jet3Mdb;

        // One real-idx slot per logical-idx (no sharing). See
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
        // Per mdbtools: unknown(4) + num_idx_rows(4) + unknown(4). Zeroed.
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
            // row are not yet emitted (see complex-columns-format-notes.md).
            if (col.Type == T_ATTACHMENT || col.Type == T_COMPLEX)
            {
                Wi32(page, o + _colMiscOff, col.Misc);
            }
            else if (col.Type == T_NUMERIC && _format != DatabaseFormat.Jet3Mdb)
            {
                // Persist declared precision/scale at the same misc 4-byte slot
                // Access uses for Decimal columns — descriptor offset 11 =
                // precision, offset 12 = scale, bytes 13/14 stay zero (matches
                // Jackcess FixedPointColumnDescriptor parser and round-tripped
                // Access-authored fixedNumericTest fixtures). Read back by
                // AccessBase.LoadColumnInfos and consumed by index encoders as
                // the canonical index-key scale.
                page[o + _colMiscOff] = col.NumericPrecision;
                page[o + _colMiscOff + 1] = col.NumericScale;
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

        // ── Index sections ─────
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
            // there is no separate used_pages slot. See §3.1 / §3.2.
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
                // key columns (per-column ascending/descending), remaining
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

                // The caller patches first_dp after appending the leaf page.
                // Per the §3.1 empirical correction, real Access fixtures emit
                // flags = 0x00 even for PK indexes — uniqueness is signalled by
                // index_type = 0x01 below, not the flag bit. Non-PK unique
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

                // index_type: 0x01 for PK, 0x00 for normal. FK uses 0x02.
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
    /// scaffold mandatory ACCDB system tables (currently
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
    /// per-kind <c>MSysComplexType_*</c> template tables. Each entry maps
    /// the canonical Access template name to the column schema Access emits for that
    /// template (verified against <c>ComplexFields.accdb</c> in
    /// <c>docs/design/format-probe-appendix-complex.md</c> §<c>MSysComplexType_*</c>).
    /// All templates are zero-row, zero-index tables; their <c>MSysObjects.Id</c>
    /// (= TDEF page) is what <c>MSysComplexColumns.ComplexTypeObjectID</c> points at.
    /// </summary>
    private static readonly (string Name, ColumnDefinition[] Columns)[] _complexTypeTemplates =
    [
        (Constants.ComplexTypeNames.UnsignedByte, new[] { new ColumnDefinition("Value", typeof(byte)) }),
        (Constants.ComplexTypeNames.Short,        [new ColumnDefinition("Value", typeof(short))]),
        (Constants.ComplexTypeNames.Long,         [new ColumnDefinition("Value", typeof(int))]),
        (Constants.ComplexTypeNames.IEEESingle,   [new ColumnDefinition("Value", typeof(float))]),
        (Constants.ComplexTypeNames.IEEEDouble,   [new ColumnDefinition("Value", typeof(double))]),
        (Constants.ComplexTypeNames.GUID,         [new ColumnDefinition("Value", typeof(Guid))]),
        (Constants.ComplexTypeNames.Decimal,      [new ColumnDefinition("Value", typeof(decimal))]),
        (Constants.ComplexTypeNames.Text,         [new ColumnDefinition("Value", typeof(string), maxLength: 255)]),
        (Constants.ComplexTypeNames.Attachment,
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
    /// scaffolds the nine <c>MSysComplexType_*</c> template tables
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
    /// <c>MSysComplexType_*</c> template name. Returns <see langword="null"/>
    /// when the column is not complex or its element type has no matching template.
    /// </summary>
    private static string? ResolveComplexTypeTemplateName(ColumnDefinition col)
    {
        if (col.IsAttachment)
        {
            return Constants.ComplexTypeNames.Attachment;
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
            return Constants.ComplexTypeNames.UnsignedByte;
        }

        if (t == typeof(short))
        {
            return Constants.ComplexTypeNames.Short;
        }

        if (t == typeof(int))
        {
            return Constants.ComplexTypeNames.Long;
        }

        if (t == typeof(float))
        {
            return Constants.ComplexTypeNames.IEEESingle;
        }

        if (t == typeof(double))
        {
            return Constants.ComplexTypeNames.IEEEDouble;
        }

        if (t == typeof(Guid))
        {
            return Constants.ComplexTypeNames.GUID;
        }

        if (t == typeof(decimal))
        {
            return Constants.ComplexTypeNames.Decimal;
        }

        if (t == typeof(string))
        {
            return Constants.ComplexTypeNames.Text;
        }

        return null;
    }

    /// <summary>
    /// Creates the empty <c>MSysComplexColumns</c> system table.
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
            Constants.SystemTableNames.ComplexColumns,
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
    /// pre-flight for <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, CancellationToken)"/>.
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

        long msysComplexPg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
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
        TableDef msysComplex = await ReadRequiredTableDefAsync(msysComplexPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
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
                        string idText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, idCol);
                        if (int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) && v > maxId)
                        {
                            maxId = v;
                        }
                    }

                    if (ctIdCol != null)
                    {
                        string ctText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, ctIdCol);
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
    /// post-flight: for each user-declared complex column on the parent
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

            // resolve the matching MSysComplexType_* template id so the
            // MSysComplexColumns row points at the canonical type-template table
            // instead of carrying the placeholder 0. Templates are scaffolded by
            // CreateDatabaseAsync and always present in Access-authored files; the
            // lookup only falls back to 0 for slim-catalog ACCDBs
            // (WriteFullCatalogSchema = false), which intentionally skip system
            // tables for byte-hash backward compatibility.
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
    /// the per-kind schemas in the design doc §2.4 / §4.2.
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
        long pg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (pg == 0)
        {
            throw new InvalidOperationException("MSysComplexColumns table is missing.");
        }

        TableDef msysComplex = await ReadRequiredTableDefAsync(pg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        object[] values = msysComplex.CreateNullValueRow();

        msysComplex.SetValueByName(values, "ColumnName", parentColumnName);
        msysComplex.SetValueByName(values, "ComplexTypeObjectID", complexTypeObjectId);
        msysComplex.SetValueByName(values, "FlatTableID", flatTableId);
        msysComplex.SetValueByName(values, "ConceptualTableID", conceptualTableId);
        msysComplex.SetValueByName(values, "ComplexID", complexId);

        await InsertRowDataAsync(pg, msysComplex, values, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    // ── Row-level APIs for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §2.1 / §2.4 / §3.

    /// <inheritdoc/>
    public ValueTask AddAttachmentAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object> parentRowKey,
        AttachmentInput attachment,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        Guard.NotNull(attachment, nameof(attachment));
        return RunAutoCommitAsync(
            _ => AddComplexItemCoreAsync(tableName, columnName, parentRowKey, attachment, expectAttachment: true, cancellationToken),
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask AddMultiValueItemAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object> parentRowKey,
        object value,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        return RunAutoCommitAsync(
            _ => AddComplexItemCoreAsync(tableName, columnName, parentRowKey, value, expectAttachment: false, cancellationToken),
            cancellationToken);
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

        // The flat table carries an autoincrement scalar PK column.
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
    /// scalar PK column emitted by the complex-column scaffold.
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
        long msysPg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysPg == 0)
        {
            return 0;
        }

        TableDef msys = await ReadRequiredTableDefAsync(msysPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
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
                    string rowName = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, complexIdCol);
                    if (complexId != 0 && (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId))
                    {
                        continue;
                    }

                    string flatText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
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
                        string actual = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, c);
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
    /// <see cref="BuildFlatTableSchema"/>.
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
                    string text = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, fkCol);
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

    // ── Cascade-on-delete for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §4.3.
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
    /// cascade-delete cost profile and the ConceptualTableID allocator
    /// used by the row-add path.
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
        // tolerance as the row-add path).
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
                        string fkText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, fkCol);
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
    /// surviving complex columns stay attached to the rebuilt parent.
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

            if (row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if ((unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) != 0)
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
    /// surgically drops a single complex column's flat child table
    /// and its <c>MSysComplexColumns</c> row, identified by
    /// <paramref name="columnName"/> + <paramref name="complexId"/>. Used by the
    /// rewrite path when the user calls <c>DropColumnAsync</c> on an
    /// attachment / multi-value column. Returns silently if no matching row is
    /// found (idempotent).
    /// </summary>
    private async ValueTask DropSingleComplexChildAsync(string columnName, int complexId, CancellationToken cancellationToken)
    {
        long msysCxPg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
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
                    string rowName = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId)
                    {
                        continue;
                    }

                    string flatText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
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
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
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
    /// when the user renames a complex column, rewrite the
    /// matching <c>MSysComplexColumns</c> row's <c>ColumnName</c> field. The
    /// hidden flat child table's catalog name (<c>f_&lt;hex&gt;_&lt;oldName&gt;</c>)
    /// is left unchanged because it is opaque to readers — they resolve the
    /// flat name via <c>FlatTableID</c> → <c>MSysObjects</c>. This mirrors the
    /// <c>RenameRelationshipAsync</c> trade-off that leaves TDEF logical-idx
    /// name cookies stale until Compact &amp; Repair.
    /// </summary>
    private async ValueTask RenameComplexColumnArtifactsAsync(string oldColumnName, string newColumnName, int complexId, CancellationToken cancellationToken)
    {
        long msysCxPg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
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
                    string rowName = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, oldColumnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId)
                    {
                        continue;
                    }

                    var values = new object[msysCxDef.Columns.Count];
                    for (int i = 0; i < values.Length; i++)
                    {
                        string text = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, msysCxDef.Columns[i]);
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
    /// when dropping a parent table, also drop the hidden flat
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

        long msysCxPg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
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
                    string rowName = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    string idText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid))
                    {
                        continue;
                    }

                    if (!lookup.TryGetValue(rowName, out HashSet<int>? expectedIds) || !expectedIds.Contains(rid))
                    {
                        continue;
                    }

                    string flatText = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
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
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
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
    /// <see cref="AccessBase.RowLocation"/> of the freshly written row.
    /// </summary>
    private async ValueTask<RowLocation> InsertRowDataLocAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default)
    {
        if (values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException(
                $"Expected {tableDef.Columns.Count} values for table row but received {values.Length}.",
                nameof(values));
        }

        // Push any oversized MEMO / OLE / Attachment payload to LVAL pages
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
                    Name = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameColumn),
                    ObjectType = ParseInt32(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, typeColumn)),
                    Flags = ParseInt64(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flagsColumn!)),
                    TDefPage = ParseInt64(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, idColumn!)) & 0x00FFFFFFL,
                });
            }

            ReturnPage(page);
        }

        return result;
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

    /// <summary>
    /// Reads <paramref name="columnOrdinals"/>'s typed values out of a single
    /// row at <paramref name="loc"/> on a data page belonging to
    /// <paramref name="tableDef"/>. Returns <see langword="null"/> when the
    /// row layout cannot be parsed OR when any requested column points at
    /// a long-value (T_MEMO, T_OLE) or complex (T_COMPLEX, T_ATTACHMENT)
    /// column — those require LVAL chain traversal which the writer does
    /// not implement; the cascade-seek caller falls back to the snapshot
    /// path in that case. Index-key column types (the focus of this helper)
    /// only include scalar fixed and var-inline kinds — JET indexes cannot
    /// cover MEMO / OLE / Complex columns at all (rejected by
    /// <see cref="IndexHelpers.ResolveIndexes"/>) so the LVAL fall-through is safety
    /// netting, not the common path.
    /// </summary>
    private async ValueTask<object?[]?> TryReadColumnValuesTypedAsync(
        RowLocation loc,
        TableDef tableDef,
        int[] columnOrdinals,
        CancellationToken cancellationToken)
    {
        byte[] pageBytes = await ReadPageAsync(loc.PageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (pageBytes[0] != 0x01)
            {
                return null;
            }

            bool hasVarColumns = false;
            for (int i = 0; i < tableDef.Columns.Count; i++)
            {
                if (!tableDef.Columns[i].IsFixed)
                {
                    hasVarColumns = true;
                    break;
                }
            }

            if (!TryParseRowLayout(pageBytes, loc.RowStart, loc.RowSize, hasVarColumns, out RowLayout layout))
            {
                return null;
            }

            var result = new object?[columnOrdinals.Length];
            for (int i = 0; i < columnOrdinals.Length; i++)
            {
                int ord = columnOrdinals[i];
                if (ord < 0 || ord >= tableDef.Columns.Count)
                {
                    return null;
                }

                ColumnInfo col = tableDef.Columns[ord];
                ColumnSlice slice = ResolveColumnSlice(pageBytes, loc.RowStart, loc.RowSize, layout, col);

                switch (slice.Kind)
                {
                    case ColumnSliceKind.Bool:
                        result[i] = slice.BoolValue;
                        break;

                    case ColumnSliceKind.Null:
                    case ColumnSliceKind.Empty:
                        result[i] = null;
                        break;

                    case ColumnSliceKind.Fixed:
                        {
                            object? v = TryDecodeFixedTyped(pageBytes, loc.RowStart + slice.DataStart, col.Type, slice.DataLen);
                            if (v == null && col.Type != T_BOOL)
                            {
                                return null;
                            }

                            result[i] = v;
                        }

                        break;

                    case ColumnSliceKind.Var:
                        {
                            object? v = TryDecodeVarInlineTyped(pageBytes, loc.RowStart + slice.DataStart, col.Type, slice.DataLen);
                            if (v == null)
                            {
                                // T_MEMO / T_OLE / T_COMPLEX / T_ATTACHMENT
                                // — caller falls back to snapshot path.
                                return null;
                            }

                            result[i] = v;
                        }

                        break;

                    default:
                        return null;
                }
            }

            return result;
        }
        finally
        {
            ReturnPage(pageBytes);
        }
    }

    /// <summary>
    /// Decodes a fixed-area slice into the canonical CLR object the
    /// public InsertRow API accepts back. Returns <see langword="null"/>
    /// only on unsupported / malformed types — callers treat that as
    /// "fall back to snapshot path".
    /// </summary>
    private static object? TryDecodeFixedTyped(byte[] page, int start, byte type, int size)
    {
        switch (type)
        {
            case T_BYTE:
                return size >= 1 ? page[start] : null;
            case T_INT:
                return size >= 2 ? BinaryPrimitives.ReadInt16LittleEndian(page.AsSpan(start, 2)) : null;
            case T_LONG:
                return size >= 4 ? BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(start, 4)) : null;
            case T_FLOAT:
                return size >= 4 ? ReadSingleLittleEndian(page.AsSpan(start, 4)) : null;
            case T_DOUBLE:
                return size >= 8 ? ReadDoubleLittleEndian(page.AsSpan(start, 8)) : null;
            case T_DATETIME:
                {
                    if (size < 8)
                    {
                        return null;
                    }

                    double oa = ReadDoubleLittleEndian(page.AsSpan(start, 8));
                    try
                    {
                        return DateTime.FromOADate(oa);
                    }
                    catch (ArgumentException)
                    {
                        return null;
                    }
                }

            case T_MONEY:
                {
                    if (size < 8)
                    {
                        return null;
                    }

                    long raw = BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(start, 8));
                    return raw / 10000m;
                }

            case T_GUID:
                {
                    if (size < 16)
                    {
                        return null;
                    }

                    byte[] g = new byte[16];
                    Buffer.BlockCopy(page, start, g, 0, 16);
                    return new Guid(g);
                }

            case T_NUMERIC:
                {
                    // 17-byte scaled decimal (1 sign byte + 16 magnitude bytes).
                    // Cascade enforcement on T_NUMERIC keys is rare but handle it
                    // for completeness; signal failure on malformed bytes so the
                    // caller falls back to the snapshot path rather than emit a
                    // bad value.
                    if (size < 17)
                    {
                        return null;
                    }

                    return null; // canonical-scale resolution requires column metadata; defer to snapshot
                }

            default:
                return null;
        }
    }

    private object? TryDecodeVarInlineTyped(byte[] page, int start, byte type, int size)
    {
        if (size <= 0)
        {
            return null;
        }

        switch (type)
        {
            case T_TEXT:
                return _format != DatabaseFormat.Jet3Mdb
                    ? DecodeJet4Text(page, start, size)
                    : _ansiEncoding.GetString(page, start, size);

            case T_BINARY:
                {
                    byte[] b = new byte[size];
                    Buffer.BlockCopy(page, start, b, 0, size);
                    return b;
                }

            case T_BYTE:
                return size >= 1 ? page[start] : null;
            case T_INT:
                return size >= 2 ? BinaryPrimitives.ReadInt16LittleEndian(page.AsSpan(start, 2)) : null;
            case T_LONG:
                return size >= 4 ? BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(start, 4)) : null;
            case T_FLOAT:
                return size >= 4 ? ReadSingleLittleEndian(page.AsSpan(start, 4)) : null;
            case T_DOUBLE:
                return size >= 8 ? ReadDoubleLittleEndian(page.AsSpan(start, 8)) : null;
            case T_DATETIME:
                {
                    if (size < 8)
                    {
                        return null;
                    }

                    double oa = ReadDoubleLittleEndian(page.AsSpan(start, 8));
                    try
                    {
                        return DateTime.FromOADate(oa);
                    }
                    catch (ArgumentException)
                    {
                        return null;
                    }
                }

            case T_MONEY:
                if (size < 8)
                {
                    return null;
                }

                return BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(start, 8)) / 10000m;
            case T_GUID:
                if (size < 16)
                {
                    return null;
                }

                byte[] g = new byte[16];
                Buffer.BlockCopy(page, start, g, 0, 16);
                return new Guid(g);

            // T_MEMO / T_OLE / T_COMPLEX / T_ATTACHMENT / T_NUMERIC — not
            // index-keyable in any case, fall back to snapshot.
            default:
                return null;
        }
    }

    /// <summary>
    /// rebuild every index B-tree on <paramref name="tableName"/> from the
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
    /// All key column types accepted by <see cref="IndexHelpers.ResolveIndexes"/> have
    /// matching <see cref="IndexKeyEncoder"/> support, so encoder rejection
    /// is treated as an unrecoverable programmer error and propagates to
    /// the caller rather than silently leaving the leaf stale (the
    /// rejection of OLE / Attachment / Multi-Value keys at create time
    /// removed the only legitimate trigger for the prior silent-skip path).
    /// </para>
    /// </summary>
    private async ValueTask MaintainIndexesAsync(long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
    {
        // Jet3 (.mdb Access 97) live leaf maintenance is now
        // supported. The 39-byte real-idx + 20-byte logical-idx layouts
        // (§3.1 / §3.2) and the 0x16-bitmask / 0xF8-first-entry leaf layout
        // (§4.2) are pinned by the format probe and emitted by the same code
        // path Jet4/ACE uses, parameterised on `IndexLeafPageBuilder.LeafPageLayout`.

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

        bool jet3 = _format == DatabaseFormat.Jet3Mdb;
        IndexLeafPageBuilder.LeafPageLayout leafLayout = jet3
            ? IndexLeafPageBuilder.LeafPageLayout.Jet3
            : IndexLeafPageBuilder.LeafPageLayout.Jet4;

        int numCols = Ru16(tdefBuffer, _tdNumCols);
        int numIdx = Ri32(tdefBuffer, _tdNumCols + 2);
        int numRealIdx = Ri32(tdefBuffer, _tdNumRealIdx);
        if (numIdx <= 0 || numRealIdx <= 0 || numIdx > 1000 || numRealIdx > 1000)
        {
            return;
        }

        // §3.1 / §3.2 per-format sizes + offsets.
        int realIdxPhysSz = jet3 ? 39 : 52;
        int logIdxEntrySz = jet3 ? 20 : 28;
        int physFirstDpOff = jet3 ? 34 : 38;
        int physFlagsOff = jet3 ? 38 : 42;
        int logIndexNum2Off = jet3 ? 4 : 8;
        int logIndexTypeOff = jet3 ? 19 : 23;
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
        int logIdxStart = realIdxDescStart + (numRealIdx * realIdxPhysSz);

        // per-real-idx, decode every populated col_map slot to recover the
        // full (column, direction) list, the first_dp byte offset, and the unique
        // flag bit (real-idx flags & 0x01). PK uniqueness is signalled by the
        // logical-idx index_type discriminator below, not the flag bit.
        var realIdxByNum = new Dictionary<int, RealIdxEntry>();
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * realIdxPhysSz);
            if (phys + realIdxPhysSz > tdefBuffer.Length)
            {
                break;
            }

            var keyCols = new List<IndexKeyColumn>(10);
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(tdefBuffer, so);
                if (cn == 0xFFFF)
                {
                    continue;
                }

                byte order = tdefBuffer[so + 2];
                keyCols.Add(new IndexKeyColumn(cn, order != 0x02));
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            byte flagsByte = tdefBuffer[phys + physFlagsOff];
            realIdxByNum[ri] = new RealIdxEntry(keyCols, phys + physFirstDpOff, (flagsByte & 0x01) != 0);
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
            int entryStart = logIdxStart + (li * logIdxEntrySz);
            if (entryStart + logIdxEntrySz > tdefBuffer.Length)
            {
                break;
            }

            int realIdxNum = Ri32(tdefBuffer, entryStart + logIndexNum2Off);
            byte indexType = tdefBuffer[entryStart + logIndexTypeOff];
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
            if (!TryResolveKeyColumnInfos(rie.IndexKeyColumns, tableDef.Columns, snapshotIndexByColNum, out List<KeyColumnInfo> keyColInfos))
            {
                continue;
            }

            // Canonical scale for a T_NUMERIC index column is the column's
            // DECLARED scale (Access-parity — every cell is canonically stored
            // at the declared scale, so the index sorts at that scale too).
            // Non-numeric columns get -1 as a sentinel.
            int[] numericTargetScales = new int[keyColInfos.Count];
            for (int k = 0; k < keyColInfos.Count; k++)
            {
                ColumnInfo kCol = keyColInfos[k].Col;
                numericTargetScales[k] = kCol.Type == T_NUMERIC ? kCol.NumericScale : -1;
            }

            bool legacyNumeric = _format == DatabaseFormat.Jet4Mdb;

            var entries = new List<(byte[] Key, long Page, byte Row)>(rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                byte[][] perColumn = new byte[keyColInfos.Count][];
                int totalLen = 0;
                for (int k = 0; k < keyColInfos.Count; k++)
                {
                    (ColumnInfo col, int snapIdx, bool ascending) = keyColInfos[k];
                    object cell = snapshot.Rows[r][snapIdx];
                    object? value = cell is DBNull ? null : cell;
                    perColumn[k] = col.Type == T_NUMERIC
                        ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, (byte)numericTargetScales[k], legacyNumeric)
                        : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
                    totalLen += perColumn[k].Length;
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

            entries.Sort(static (a, b) => IndexHelpers.CompareKeyBytes(a.Key, b.Key));

            // unique-violation detection. Note this is a post-write check —
            // the offending row has already been persisted by the time we get
            // here, so throwing leaves the table in a state where the row exists
            // but the index is stale. The caller is expected to delete the
            // duplicate row (or restore from a backup) before continuing.
            if (rie.IsUnique)
            {
                for (int e = 1; e < entries.Count; e++)
                {
                    if (IndexHelpers.CompareKeyBytes(entries[e - 1].Key, entries[e].Key) == 0)
                    {
                        throw new InvalidOperationException(
                            $"Unique index violation on table '{tableName}': duplicate key detected after row mutation. " +
                            "The duplicate row has been written but the index B-tree was not rebuilt; " +
                            "remove one of the offending rows and retry the operation.");
                    }
                }
            }

            var leafEntries = new List<IndexEntry>(entries.Count);
            foreach ((byte[] key, long page, byte row) in entries)
            {
                leafEntries.Add(new IndexEntry(key, page, row));
            }

            long firstPageNumber = _stream.Length / _pgSz;
            IndexBTreeBuilder.BuildResult build = IndexBTreeBuilder.Build(leafLayout, _pgSz, tdefPage, leafEntries, firstPageNumber);
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
    /// Incremental fast path: when the change since the previous index
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
    ///   <item><b>Single-leaf splice.</b> Root is a leaf
    ///   (<c>page_type = 0x04</c>) with no sibling pointers AND the
    ///   post-mutation entry list still fits on one page. The leaf is
    ///   decoded, spliced, and re-emitted as a single page; <c>first_dp</c>
    ///   is patched to the new leaf.</item>
    ///   <item><b>Multi-level rebuild from existing tree.</b>
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
    /// <c>T_NUMERIC</c> (the canonical-scale pre-pass needs a full
    /// snapshot); the encoder rejects any value (text outside General
    /// Legacy, etc.); the index page chain is malformed; or the spliced
    /// entry list cannot be repacked (e.g. a single entry exceeds the
    /// payload area).
    /// </para>
    /// <para>
    /// Pre-write unique-index enforcement is handled separately
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
        // Jet3 (.mdb Access 97) participates in the
        // incremental fast paths via the per-format LeafPageLayout descriptor
        // (page size 2048, bitmask at 0x16, first entry at 0xF8) and the §3.1
        // 39-byte real-idx physical descriptor (first_dp at phys+34 instead
        // of phys+38). The change-set encode + splice + rebuild logic is
        // unchanged; only the layout-dependent byte offsets and page builder
        // calls fork on `jet3`. Same disposal model as Jet4 — old leaf /
        // intermediate pages are orphaned and reclaimed by Access on
        // Compact & Repair.
        bool jet3 = _format == DatabaseFormat.Jet3Mdb;
        IndexLeafPageBuilder.LeafPageLayout layout = jet3
            ? IndexLeafPageBuilder.LeafPageLayout.Jet3
            : IndexLeafPageBuilder.LeafPageLayout.Jet4;

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

        // §3.1 per-format real-idx physical descriptor sizes.
        // Jet4/ACE: 52 bytes — unknown(4) + col_map(30) + used_pages(4) + first_dp(4) + flags(1) + unknown(9).
        // Jet3:     39 bytes — unknown(4) + col_map(30) + first_dp(4) + flags(1) — no used_pages slot.
        int realIdxPhysSz = jet3 ? 39 : 52;
        int physFirstDpOff = jet3 ? 34 : 38;
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
            int phys = realIdxDescStart + (ri * realIdxPhysSz);
            if (phys + realIdxPhysSz > tdefBuffer.Length)
            {
                return false;
            }

            var keyCols = new List<IndexKeyColumn>(10);
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(tdefBuffer, so);
                if (cn == 0xFFFF)
                {
                    continue;
                }

                byte order = tdefBuffer[so + 2];
                keyCols.Add(new(cn, order != 0x02));
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            slots.Add(new RealIdxEntry(keyCols, phys + physFirstDpOff, IsUnique: false));
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
            if (!TryResolveKeyColumnInfos(rie.IndexKeyColumns, tableDef.Columns, snapshotIndexByColNum, out List<KeyColumnInfo> keyColInfos))
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
            var addEntries = EncodeHintEntries(insertedRows, keyColInfos);
            if (addCount > 0 && addEntries.Count != addCount)
            {
                // Encoder rejected at least one row; bail to bulk.
                return false;
            }

            // Encode the deleted rows' keys too. The single-leaf and bulk
            // paths only need the (page, row) pointers (they re-derive the
            // key from the live leaf entry); the surgical multi-level path
            // needs the keys to perform a path-capturing descent that
            // confirms every change targets the same leaf.
            var removeEntries = EncodeHintEntries(deletedRows, keyColInfos);
            if (delCount > 0 && removeEntries.Count != delCount)
            {
                return false;
            }

            List<(long DataPage, byte DataRow)> removePtrs = new(delCount);
            foreach ((_, long dpDel, byte drDel) in removeEntries)
            {
                removePtrs.Add((dpDel, drDel));
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
                // multi-level trees" branch.
                if (rootPage[0] != Constants.IndexLeafPage.PageTypeIntermediate
                    && rootPage[0] != Constants.IndexLeafPage.PageTypeLeaf)
                {
                    return false;
                }

                // Append-only tail-page fast path. When
                // the change-set is insert-only AND every new key sorts
                // strictly after the current tail-leaf max key, splice the
                // new entries into the tail leaf and rewrite that one page.
                // No descend-walk-rebuild, no sibling-chain updates, no
                // intermediate writes — the rightmost intermediate summary
                // becomes (one entry) stale, which the seeker compensates
                // for by following the intermediate's tail_page header on
                // overshoot. Falls through to the bulk rebuild on overflow,
                // deletes, out-of-order inserts, missing tail_page, or any
                // malformed page.
                if (delCount == 0 && addEntries.Count > 0)
                {
                    bool tailHandled = await TryAppendToTailLeafAsync(
                        layout,
                        tdefPage,
                        rootPage,
                        addEntries,
                        cancellationToken).ConfigureAwait(false);
                    if (tailHandled)
                    {
                        continue;
                    }
                }

                // Surgical multi-level mutation.
                // When every change in this batch lands on the SAME leaf and
                // the spliced entry list either still fits one page or splits cleanly into two pages whose
                // new summary entries fit into the parent intermediate,
                // mutate the affected leaf
                // (and possibly its right sibling + parent / ancestors) in
                // place at their existing page numbers — no orphaned pages,
                // no fresh page-range allocation. Returns true when handled,
                // false on any bail trigger (multi-leaf change-set, leaf
                // becomes empty, leaf needs 3+ pages, parent intermediate
                // overflows, descent overshoots into a tail_page chain, or
                // the encoder/IO chain hits a malformed page). The caller
                // falls through to the bulk rebuild on false. See
                // docs/design/index-and-relationship-format-notes.md §7.
                bool surgicalHandled = await TrySurgicalMultiLevelMaintainAsync(
                    layout,
                    tdefPage,
                    firstDp,
                    addEntries,
                    removeEntries,
                    cancellationToken).ConfigureAwait(false);
                if (surgicalHandled)
                {
                    continue;
                }

                // Cross-leaf surgical mutation. When
                // the change-set spans multiple leaves the single-leaf paths
                // bail; group changes by target leaf and
                // mutate each leaf in place, aggregating per-parent summary
                // updates. Bails on underflow or parent overflow,
                // in which case the bulk path below resnaps the tree.
                bool crossLeafHandled = await TrySurgicalCrossLeafMaintainAsync(
                    layout,
                    tdefPage,
                    firstDp,
                    rie.FirstDpOffset,
                    addEntries,
                    removeEntries,
                    cancellationToken).ConfigureAwait(false);
                if (crossLeafHandled)
                {
                    continue;
                }

                long leftmostLeaf = await DescendToLeftmostLeafAsync(layout, firstDp, cancellationToken).ConfigureAwait(false);
                if (leftmostLeaf <= 0)
                {
                    return false;
                }

                var allExisting = new List<IndexEntry>();
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

                    if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
                    {
                        return false;
                    }

                    allExisting.AddRange(IndexLeafIncremental.DecodeEntries(layout, leaf, _pgSz));
                    walkPage = IndexLeafIncremental.ReadNextLeafPage(leaf);
                }

                List<IndexEntry>? splicedAll = IndexLeafIncremental.Splice(allExisting, addEntries, removePtrs);
                if (splicedAll is null)
                {
                    return false;
                }

                long firstNewPage = _stream.Length / _pgSz;
                IndexBTreeBuilder.BuildResult mlBuild;
                try
                {
                    mlBuild = IndexBTreeBuilder.Build(layout, _pgSz, tdefPage, splicedAll, firstNewPage);
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

            List<IndexEntry> existing = IndexLeafIncremental.DecodeEntries(layout, rootPage, _pgSz);
            List<IndexEntry>? spliced = IndexLeafIncremental.Splice(existing, addEntries, removePtrs);
            if (spliced is null)
            {
                return false;
            }

            byte[]? newLeaf = IndexLeafIncremental.TryRebuildLeaf(layout, _pgSz, tdefPage, spliced);
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
    /// Descends an index B-tree from <paramref name="rootPage"/> through intermediate (<c>0x03</c>) levels by following the first child pointer of each.
    /// - Returns the page number of the leftmost leaf (<c>0x04</c>).
    /// - Returns 0 if the chain is malformed (unknown page type, missing child pointer, or excessive depth),
    ///   so the caller can fall back to the bulk-rebuild path.
    /// </summary>
    /// <param name="layout">Page layout descriptor (Jet3: offsets <c>0xF8</c>/<c>0x16</c>; Jet4: <c>0x1E0</c>/<c>0x1B</c>).</param>
    /// <param name="rootPage">Root page number of the index B-tree.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async ValueTask<long> DescendToLeftmostLeafAsync(IndexLeafPageBuilder.LeafPageLayout layout, long rootPage, CancellationToken cancellationToken)
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

            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                return current;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return 0;
            }

            long firstChild = IndexLeafIncremental.ReadFirstChildPointer(layout, page, _pgSz);
            if (firstChild <= 0)
            {
                return 0;
            }

            current = firstChild;
        }

        return 0;
    }

    /// <summary>
    /// append-only tail-page fast path. When every key in
    /// <paramref name="addEntries"/> sorts strictly greater than the current
    /// tail-leaf max key, splice the new entries into the tail leaf and
    /// rewrite that one page in place — preserving the leaf's
    /// <c>prev_page</c> sibling pointer and re-emitting with
    /// <c>next_page = 0</c> and <c>tail_page = 0</c> on the leaf itself.
    /// Returns <see langword="true"/> on success (the caller should
    /// <c>continue</c> to the next index slot); returns <see langword="false"/>
    /// when the fast path does not apply — missing root <c>tail_page</c>,
    /// any insert key &lt;= tail max, or the rewritten leaf overflows a
    /// single page (the caller falls through to the descend-walk-rebuild
    /// path).
    /// <para>
    /// No sibling-chain or intermediate-summary updates are performed. The
    /// rightmost intermediate's summary entry consequently becomes stale
    /// (its key is the OLD tail max, not the new one); the §4.5 design
    /// expects readers / seekers to compensate by following the
    /// intermediate's <c>tail_page</c> header on overshoot, which
    /// <see cref="IndexBTreeSeeker"/> does.
    /// </para>
    /// </summary>
    private async ValueTask<bool> TryAppendToTailLeafAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long tdefPage,
        byte[] rootPage,
        List<IndexEntry> addEntries,
        CancellationToken cancellationToken)
    {
        long tailLeafPage = IndexLeafIncremental.ReadTailPage(rootPage);
        if (tailLeafPage <= 0)
        {
            return false;
        }

        byte[] tailBytes = await ReadPageAsync(tailLeafPage, cancellationToken).ConfigureAwait(false);
        byte[] tailLeaf;
        try
        {
            tailLeaf = (byte[])tailBytes.Clone();
        }
        finally
        {
            ReturnPage(tailBytes);
        }

        if (tailLeaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        long tailPrev = IndexLeafIncremental.ReadPrevPage(tailLeaf);
        long tailNext = IndexLeafIncremental.ReadNextLeafPage(tailLeaf);
        if (tailNext != 0)
        {
            // The tail leaf must be the rightmost leaf (next_page == 0). If
            // a previous fast-path append already grew the chain and the
            // root's tail_page wasn't updated, give up — the bulk path will
            // resync the whole tree.
            return false;
        }

        List<IndexEntry> existingTail = IndexLeafIncremental.DecodeEntries(layout, tailLeaf, _pgSz);

        // Every new key must sort strictly after the current tail max.
        // Empty tail leaf trivially satisfies the predicate.
        if (existingTail.Count > 0)
        {
            byte[] tailMax = existingTail[existingTail.Count - 1].Key;
            for (int i = 0; i < addEntries.Count; i++)
            {
                if (IndexHelpers.CompareKeyBytes(addEntries[i].Key, tailMax) <= 0)
                {
                    return false;
                }
            }
        }

        // Splice (existing tail entries unchanged + new entries appended).
        // Splice() handles the (no-removes, sorted-merge) case efficiently;
        // since adds already sort > existing max, the stable merge produces
        // existing-then-new in the right order.
        List<IndexEntry>? spliced = IndexLeafIncremental.Splice(
            existingTail,
            addEntries,
            []);
        if (spliced is null)
        {
            return false;
        }

        byte[] rewritten;
        try
        {
            rewritten = IndexLeafPageBuilder.BuildLeafPage(
                layout,
                _pgSz,
                tdefPage,
                spliced,
                prevPage: tailPrev,
                nextPage: 0,
                tailPage: 0,
                enablePrefixCompression: true);
        }
        catch (ArgumentOutOfRangeException)
        {
            // Tail leaf would overflow a single page. Fall through to the
            // bulk path, which will resnap the tree (and emit a fresh tail leaf).
            return false;
        }

        await WritePageAsync(tailLeafPage, rewritten, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Surgical multi-level mutation of a
    /// JET index B-tree. Replaces the bulk fall-through "descend to leftmost
    /// leaf, walk every leaf, splice, rebuild a fresh tree on a new page
    /// range" path with an in-place mutation when:
    /// <list type="bullet">
    ///   <item>Every change in the batch lands on the SAME leaf (verified by
    ///   path-capturing descent against each change-set key).</item>
    ///   <item>The spliced entry list either still fits a single page or splits cleanly into exactly two pages.</item>
    ///   <item>Any required parent intermediate updates (max-key replacement
    ///   for the in-place case, or insertion of one new summary entry for
    ///   the split case) fit
    ///   into the existing intermediate page without overflow.</item>
    /// </list>
    /// On any bail trigger — multi-leaf change-set, leaf becomes empty,
    /// 3+ page split, parent intermediate overflow, descent overshoot into
    /// a tail-page chain, malformed page, or encoder rejection — returns
    /// <see langword="false"/>; the caller falls through to the bulk
    /// rebuild. Pages are rewritten at their existing page numbers (no
    /// orphans) on success.
    /// </summary>
    private async ValueTask<bool> TrySurgicalMultiLevelMaintainAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long tdefPage,
        long firstDp,
        List<IndexEntry> addEntries,
        List<IndexEntry> removeEntries,
        CancellationToken cancellationToken)
    {
        if (addEntries.Count == 0 && removeEntries.Count == 0)
        {
            return true;
        }

        // 1. Path-capturing descent with the FIRST change key.
        byte[] firstKey = addEntries.Count > 0 ? addEntries[0].Key : removeEntries[0].Key;
        var path = new List<DescentStep>();
        long targetLeafPage = await DescendCapturingAsync(layout, firstDp, firstKey, path, cancellationToken).ConfigureAwait(false);
        if (targetLeafPage <= 0 || path.Count == 0)
        {
            // Either descent overshot (search key > every summary, follows
            // tail_page) or the root was a leaf (single-root-leaf path
            // should have caught it). Either way: bail.
            return false;
        }

        // 2. Verify every other change targets the same leaf via fast re-walk.
        int firstAdd = addEntries.Count > 0 ? 1 : 0;
        for (int i = firstAdd; i < addEntries.Count; i++)
        {
            if (!IndexHelpers.ConfirmKeyTargetsSamePath(path, addEntries[i].Key))
            {
                return false;
            }
        }

        int rstart = addEntries.Count > 0 ? 0 : 1;
        for (int i = rstart; i < removeEntries.Count; i++)
        {
            if (!IndexHelpers.ConfirmKeyTargetsSamePath(path, removeEntries[i].Key))
            {
                return false;
            }
        }

        // 3. Read the target leaf and decode existing entries.
        byte[] leafBytes = await ReadPageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);
        byte[] leaf;
        try
        {
            leaf = (byte[])leafBytes.Clone();
        }
        finally
        {
            ReturnPage(leafBytes);
        }

        if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        List<IndexEntry> existingLeafEntries = IndexLeafIncremental.DecodeEntries(layout, leaf, _pgSz);
        if (existingLeafEntries.Count == 0)
        {
            // Empty leaf — descent shouldn't normally land here. Bail.
            return false;
        }

        // 4. Splice the change-set into the live leaf entries.
        var removePtrs = new List<(long DataPage, byte DataRow)>(removeEntries.Count);
        foreach ((_, long dp, byte dr) in removeEntries)
        {
            removePtrs.Add((dp, dr));
        }

        List<IndexEntry>? spliced = IndexLeafIncremental.Splice(existingLeafEntries, addEntries, removePtrs);
        if (spliced is null)
        {
            return false;
        }

        if (spliced.Count == 0)
        {
            // Leaf-becomes-empty underflow is out of scope for this code path.
            return false;
        }

        long leafPrev = IndexLeafIncremental.ReadPrevPage(leaf);
        long leafNext = IndexLeafIncremental.ReadNextLeafPage(leaf);
        long leafTail = IndexLeafIncremental.ReadTailPage(leaf);

        byte[] oldMaxKey = existingLeafEntries[existingLeafEntries.Count - 1].Key;

        // 5. Try to fit the spliced entries on the original leaf page.
        byte[]? rebuilt = IndexLeafIncremental.TryRebuildLeafWithSiblings(
            layout, _pgSz, tdefPage, spliced, leafPrev, leafNext, leafTail);
        if (rebuilt != null)
        {
            IndexEntry newLast = spliced[spliced.Count - 1];
            bool maxUnchanged = IndexHelpers.CompareKeyBytes(newLast.Key, oldMaxKey) == 0;

            if (maxUnchanged)
            {
                // Pure in-place leaf rewrite — no parent updates needed.
                await WritePageAsync(targetLeafPage, rebuilt, cancellationToken).ConfigureAwait(false);
                return true;
            }

            // Max key changed → walk path replacing parent's summary entry
            // for this leaf (and propagating up while the change is to the
            // last summary on each ancestor).
            var newSummary = new DecodedIntermediateEntry(new(newLast.Key, newLast.DataPage, newLast.DataRow), ChildPage: targetLeafPage);
            List<(long PageNum, byte[] Bytes)>? ancestorWrites = PrepareAncestorReplaceWrites(layout, tdefPage, path, newSummary);
            if (ancestorWrites is null)
            {
                return false;
            }

            // Commit: leaf first, then ancestors.
            await WritePageAsync(targetLeafPage, rebuilt, cancellationToken).ConfigureAwait(false);
            foreach ((long pn, byte[] bytes) in ancestorWrites)
            {
                await WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
            }

            return true;
        }

        // 6. Try an N-way leaf split (greedy left-fill).
        // Bails only if a single entry exceeds page payload area.
        SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, _pgSz, spliced);
        if (splitPages is null)
        {
            return false;
        }

        // First page reuses the original leaf page; remaining pages are
        // freshly appended at end-of-file.
        int splitCount = splitPages.Count;
        long[] pageNumbers = new long[splitCount];
        pageNumbers[0] = targetLeafPage;
        long firstFreshPage = _stream.Length / _pgSz;
        for (int p = 1; p < splitCount; p++)
        {
            pageNumbers[p] = firstFreshPage + (p - 1);
        }

        byte[][] pageBytesAll = new byte[splitCount][];
        try
        {
            for (int p = 0; p < splitCount; p++)
            {
                long thisPrev = p == 0 ? leafPrev : pageNumbers[p - 1];
                long thisNext = p == splitCount - 1 ? leafNext : pageNumbers[p + 1];
                pageBytesAll[p] = IndexLeafPageBuilder.BuildLeafPage(
                    layout,
                    _pgSz,
                    tdefPage,
                    splitPages[p],
                    prevPage: thisPrev,
                    nextPage: thisNext,
                    tailPage: 0,
                    enablePrefixCompression: true);
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }

        // Build summaries (max key per page) for parent ops.
        IndexEntry leftLast = splitPages.GetLastEntry(0);
        var leftSummary = new DecodedIntermediateEntry(leftLast, ChildPage: pageNumbers[0]);
        var rightSummaries = new DecodedIntermediateEntry[splitCount - 1];
        for (int p = 1; p < splitCount; p++)
        {
            IndexEntry last = splitPages.GetLastEntry(p);
            rightSummaries[p - 1] = new DecodedIntermediateEntry(last, ChildPage: pageNumbers[p]);
        }

        // Compute parent (and grandparent, ...) writes WITHOUT committing —
        // bail cleanly on overflow.
        List<(long PageNum, byte[] Bytes)>? splitAncestorWrites = PrepareAncestorSplitWrites(
            layout, tdefPage, path, leftSummary, rightSummaries);
        if (splitAncestorWrites is null)
        {
            return false;
        }

        // Commit order (no transactions; minimise observable half-state):
        //   (a) Append every new right page in order (no parent points at
        //       them yet, so a partial append leaves only orphans).
        //   (b) Patch leafNext.prev_page to point at the LAST new page.
        //   (c) Rewrite the original leaf in place as the new LEFT-most.
        //   (d) Rewrite parent + ancestors in place with the new summaries.
        for (int p = 1; p < splitCount; p++)
        {
            long appended = await AppendPageAsync(pageBytesAll[p], cancellationToken).ConfigureAwait(false);
            if (appended != pageNumbers[p])
            {
                // Stream extended by something else mid-flight; partial
                // appends are orphans, original tree still intact.
                return false;
            }
        }

        if (leafNext > 0)
        {
            byte[] nextBytes = await ReadPageAsync(leafNext, cancellationToken).ConfigureAwait(false);
            byte[] nextLeaf;
            try
            {
                nextLeaf = (byte[])nextBytes.Clone();
            }
            finally
            {
                ReturnPage(nextBytes);
            }

            // prev_page is at offset 8 (§4.1).
            BinaryPrimitives.WriteInt32LittleEndian(nextLeaf.AsSpan(8, 4), checked((int)pageNumbers[splitCount - 1]));
            await WritePageAsync(leafNext, nextLeaf, cancellationToken).ConfigureAwait(false);
        }

        await WritePageAsync(targetLeafPage, pageBytesAll[0], cancellationToken).ConfigureAwait(false);

        foreach ((long pn, byte[] bytes) in splitAncestorWrites)
        {
            await WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Descends an index B-tree from <paramref name="rootPage"/> using
    /// <paramref name="searchKey"/> to pick the child at every intermediate
    /// level (first summary &gt;= searchKey wins, mirroring
    /// <see cref="IndexBTreeSeeker.ContainsKeyAsync"/>). On every level
    /// pushed onto <paramref name="path"/>: the page number, raw bytes,
    /// decoded summary entries, and the index of the followed child. Returns
    /// the leaf page number reached, or 0 on any descent failure (overshoot,
    /// malformed page, or excessive depth) — surgical mutation bails on 0.
    /// </summary>
    private async ValueTask<long> DescendCapturingAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long rootPage,
        byte[] searchKey,
        List<DescentStep> path,
        CancellationToken cancellationToken)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
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

            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                return current;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return 0;
            }

            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, _pgSz);
            if (entries.Count == 0)
            {
                return 0;
            }

            int idx = IndexHelpers.SelectChildIndexFromDecoded(entries, searchKey);
            if (idx < 0)
            {
                // Search key sorts strictly above every summary on this
                // intermediate. The seeker would follow tail_page here,
                // but the surgical path needs a clean (page, taken-index)
                // pair at every level for an in-place ancestor rewrite — bail.
                return 0;
            }

            path.Add(new DescentStep(current, page, entries, idx));
            current = entries[idx].ChildPage;
            if (current <= 0)
            {
                return 0;
            }
        }

        return 0;
    }

    /// <summary>
    /// Computes the in-place rewrites required for a max-key change at the
    /// parent-of-leaf level. Replaces the entry at
    /// <c>path[^1].TakenIndex</c> with <paramref name="newSummary"/> (same
    /// child page, new key + summary row pointer). When that entry was the
    /// LAST on the parent intermediate, the parent's max key has changed
    /// too, so we walk up replacing the grandparent's entry that summarises
    /// this parent (and so on, up to the root). Returns <see langword="null"/>
    /// when any intermediate page would overflow on rebuild — caller bails
    /// to bulk rebuild without committing any partial state.
    /// </summary>
    private List<(long PageNum, byte[] Bytes)>? PrepareAncestorReplaceWrites(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long tdefPage,
        List<DescentStep> path,
        DecodedIntermediateEntry newSummary)
    {
        var writes = new List<(long PageNum, byte[] Bytes)>(path.Count);
        var current = newSummary;
        for (int level = path.Count - 1; level >= 0; level--)
        {
            DescentStep step = path[level];
            List<DecodedIntermediateEntry> entries = step.Entries;

            var newEntries = new List<DecodedIntermediateEntry>(entries.Count);
            for (int i = 0; i < entries.Count; i++)
            {
                if (i == step.TakenIndex)
                {
                    newEntries.Add(current);
                }
                else
                {
                    newEntries.Add(entries[i]);
                }
            }

            byte[] pageBytes = step.PageBytes;
            long prev = (uint)BinaryPrimitives.ReadInt32LittleEndian(pageBytes.AsSpan(8, 4));
            long next = (uint)BinaryPrimitives.ReadInt32LittleEndian(pageBytes.AsSpan(12, 4));
            long tail = (uint)BinaryPrimitives.ReadInt32LittleEndian(pageBytes.AsSpan(16, 4));

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, _pgSz, tdefPage, newEntries, prev, next, tail);
            if (rebuilt is null)
            {
                return null;
            }

            writes.Add((step.PageNumber, rebuilt));

            bool wasLast = step.TakenIndex == entries.Count - 1;
            if (!wasLast)
            {
                // Parent's max didn't change → no need to walk further up.
                return writes;
            }

            // Was last → grandparent's summary for this intermediate also
            // needs the new max key. Carry the new max upward; the
            // grandparent's entry's ChildPage is this intermediate's page.
            current = current with { ChildPage = step.PageNumber };
        }

        return writes;
    }

    /// <summary>
    /// Computes the in-place rewrites required for a leaf
    /// split. At the parent-of-leaf level, replaces the single entry at
    /// <c>path[^1].TakenIndex</c> with the <paramref name="leftSummary"/>
    /// followed by every entry in <paramref name="rightSummaries"/>
    /// (one for the 2-way case, N-1 for the N-way case).
    /// When the original entry was the LAST on the parent, the parent's max
    /// key has changed too and we propagate via
    /// <see cref="PrepareAncestorReplaceWrites"/> using the right-most new
    /// summary's key. Returns <see langword="null"/> on overflow at any
    /// captured ancestor level (recursive intermediate split lives in the
    /// cross-leaf path's <see cref="TryStageIntermediateRewritesAsync"/>;
    /// the single-leaf surgical path bails to the bulk rebuild when its parent
    /// overflows). Callers commit the writes after the leaf-side writes.
    /// </summary>
    private List<(long PageNum, byte[] Bytes)>? PrepareAncestorSplitWrites(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long tdefPage,
        List<DescentStep> path,
        DecodedIntermediateEntry leftSummary,
        DecodedIntermediateEntry[] rightSummaries)
    {
        if (rightSummaries.Length == 0)
        {
            return null;
        }

        int level = path.Count - 1;
        DescentStep step = path[level];
        List<DecodedIntermediateEntry> entries = step.Entries;

        var newEntries = new List<DecodedIntermediateEntry>(entries.Count + rightSummaries.Length);
        for (int i = 0; i < entries.Count; i++)
        {
            if (i == step.TakenIndex)
            {
                newEntries.Add(leftSummary);
                for (int r = 0; r < rightSummaries.Length; r++)
                {
                    newEntries.Add(rightSummaries[r]);
                }
            }
            else
            {
                newEntries.Add(entries[i]);
            }
        }

        byte[] parentBytes = step.PageBytes;
        long parentPrev = (uint)BinaryPrimitives.ReadInt32LittleEndian(parentBytes.AsSpan(8, 4));
        long parentNext = (uint)BinaryPrimitives.ReadInt32LittleEndian(parentBytes.AsSpan(12, 4));
        long parentTail = (uint)BinaryPrimitives.ReadInt32LittleEndian(parentBytes.AsSpan(16, 4));

        byte[]? rebuiltParent = IndexBTreeBuilder.TryBuildIntermediatePage(
            layout, _pgSz, tdefPage, newEntries, parentPrev, parentNext, parentTail);
        if (rebuiltParent is null)
        {
            // Parent overflow on insertion of the new summary entries —
            // single-leaf surgical path has no recursive parent-split
            // (that lives in the cross-leaf staging walker). Bail.
            return null;
        }

        var writes = new List<(long PageNum, byte[] Bytes)>(path.Count) { (step.PageNumber, rebuiltParent) };

        bool wasLast = step.TakenIndex == entries.Count - 1;
        if (!wasLast || level == 0)
        {
            return writes;
        }

        // The right-most new summary became this parent's new max →
        // grandparent's summary entry for this parent must carry the new
        // max key.
        var rightmost = rightSummaries[rightSummaries.Length - 1];
        var newAncestor = rightmost with { ChildPage = step.PageNumber };
        List<DescentStep> subPath = path.GetRange(0, level);
        List<(long PageNum, byte[] Bytes)>? more = PrepareAncestorReplaceWrites(layout, tdefPage, subPath, newAncestor);
        if (more is null)
        {
            return null;
        }

        writes.AddRange(more);
        return writes;
    }

    // ════════════════════════════════════════════════════════════════
    // cross-leaf surgical multi-level mutation
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Per-leaf bucket built by <see cref="GroupChangesByTargetLeafAsync"/>.
    /// Adds and removes routed to the same leaf are accumulated here; the
    /// captured intermediate path is shared across all keys that descended
    /// to this leaf (every key in the bucket picked the same child at every
    /// level above, by definition of "same target leaf").
    /// </summary>
    private sealed class LeafGroup(long leafPage, List<DescentStep> path)
    {
        /// <summary>Gets the page number of the target leaf.</summary>
        public long LeafPage { get; } = leafPage;

        /// <summary>Gets the captured path from root intermediate down to the parent-of-leaf.</summary>
        public List<DescentStep> Path { get; } = path;

        /// <summary>Gets the encoded inserts that landed on this leaf.</summary>
        public List<IndexEntry> Adds { get; } = [];

        /// <summary>Gets the row pointers whose entries should be removed from this leaf.</summary>
        public List<(long DataPage, byte DataRow)> RemovePtrs { get; } = [];
    }

    /// <summary>
    /// Cross-leaf surgical mutation. Invoked by
    /// <see cref="TryMaintainIndexesIncrementalAsync"/> AFTER the single-leaf
    /// surgical path (<see cref="TrySurgicalMultiLevelMaintainAsync"/>) has
    /// bailed. Groups every change-set key by its target leaf via
    /// path-capturing descent, applies a per-leaf splice (in-place rewrite or
    /// 2-way split), and aggregates all parent-intermediate updates into a
    /// single rewrite per intermediate page. Returns <see langword="true"/>
    /// when every leaf was mutated in place at its existing page number (with
    /// at most one new appended page per split); the caller MUST then NOT
    /// invoke <see cref="MaintainIndexesAsync"/>. Returns <see langword="false"/>
    /// on any bail trigger — caller falls through to the bulk rebuild.
    /// <para>
    /// Maximum distinct target leaves in a single cross-leaf surgical batch.
    /// Above this, the bulk path is faster (linear leaf-chain walk).
    /// The cap is held as a local constant in the method body.
    /// </para>
    /// <list type="bullet">
    ///   <item>More than 64 distinct target leaves.</item>
    ///   <item>Any per-leaf splice produces an empty leaf.</item>
    ///   <item>Any per-leaf splice would need 3+ pages.</item>
    ///   <item>Any parent intermediate would overflow on its aggregated
    ///   summary updates.</item>
    ///   <item>A leaf split's right page would need a sibling-pointer patch
    ///   on a leaf that another group is also mutating (rare; would need
    ///   merged in-place writes).</item>
    ///   <item>Any descent overshoots into a tail_page chain.</item>
    ///   <item>Any captured intermediate's last entry change requires an
    ///   ancestor rewrite that is shared with another group's update of the
    ///   same ancestor (handled — both updates are merged into one rewrite —
    ///   but only when both updates are summary replacements; mixed
    ///   replace+insert at the same position bails).</item>
    /// </list>
    /// </summary>
    private async ValueTask<bool> TrySurgicalCrossLeafMaintainAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long tdefPage,
        long firstDp,
        int firstDpOffset,
        List<IndexEntry> addEntries,
        List<IndexEntry> removeEntries,
        CancellationToken cancellationToken)
    {
        const int MaxLeafGroupCount = 64;

        if (addEntries.Count == 0 && removeEntries.Count == 0)
        {
            return true;
        }

        // ── Phase A: per-key descent → group by leaf ─────────────────
        Dictionary<long, LeafGroup>? groups = await GroupChangesByTargetLeafAsync(
            layout,
            firstDp,
            addEntries,
            removeEntries,
            MaxLeafGroupCount,
            cancellationToken).ConfigureAwait(false);
        if (groups is null)
        {
            return false;
        }

        // Single-leaf groups should have been handled by the single-leaf
        // surgical path. If we landed here with one group, that path
        // bailed (e.g. parent overflow on summary insert, leaf underflow,
        // etc.). The cross-leaf code below handles leaf-merge
        // (a one-group underflow case) too — only return false when there
        // are zero groups (no work to do, defensive).
        if (groups.Count == 0)
        {
            return true;
        }

        // ── Phase B: per-leaf splice + classify outcome ──────────────
        // Stage all writes in memory; commit only after every group's plan
        // and every aggregated intermediate rewrite validates.
        var existingPageRewrites = new Dictionary<long, byte[]>(groups.Count * 2);
        var newPageAppends = new List<byte[]>(groups.Count); // appended in order
        var leafNextPointerPatches = new Dictionary<long, long>(); // page → new prev_page (offset 8)
        var leafPrevPointerPatches = new Dictionary<long, long>(); // page → new next_page (offset 12)

        // Per-parent-intermediate aggregated operations. Key = parent page;
        // value = ordered list of ops keyed by ORIGINAL child index in the
        // parent's entry list. Two ops at the same original index (e.g.
        // ReplaceAt + InsertAfter for a split) coexist in declaration order.
        var parentOps = new Dictionary<long, List<IntermediateOp>>();

        // run-stitching map: each emptying leaf records its
        // (prev, next) sibling pointers so the post-loop boundary pass
        // can correctly patch the surviving pages of contiguous emptying
        // runs (skipping over every dead leaf in the run).
        var emptyingLeafSiblings = new Dictionary<long, (long Prev, long Next)>();

        // For ascending-up propagation when a parent's max key changes, we
        // need to know which child-index in the GRANDPARENT this parent
        // occupies. The captured DescentStep for the grandparent already
        // carries TakenIndex pointing at this parent's slot.

        long nextAllocatedPageNumber = _stream.Length / _pgSz;

        // ── Pre-pass: classify which leaves will empty out so the
        // chain-detach logic below can tolerate a contiguous run of
        // emptying leaves. Without this set the
        // `groups.ContainsKey(neighbor)` guard bails on every internal
        // group whose immediate sibling is also being emptied — which is
        // exactly the workload required to engage the recursive
        // intermediate-collapse path. With it, when both neighbours are
        // also empty-targets we simply skip patching their pointer-bytes
        // (they're being orphaned together; no surviving page needs to
        // skip them).
        var emptyingLeaves = new HashSet<long>();
        foreach (LeafGroup pre in groups.Values)
        {
            byte[] preBytes = await ReadPageAsync(pre.LeafPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (preBytes[0] != Constants.IndexLeafPage.PageTypeLeaf)
                {
                    continue;
                }

                List<IndexEntry> preExisting = IndexLeafIncremental.DecodeEntries(layout, preBytes, _pgSz);
                if (preExisting.Count == 0)
                {
                    continue;
                }

                List<IndexEntry>? preSpliced = IndexLeafIncremental.Splice(preExisting, pre.Adds, pre.RemovePtrs);
                if (preSpliced is { Count: 0 })
                {
                    emptyingLeaves.Add(pre.LeafPage);
                }
            }
            finally
            {
                ReturnPage(preBytes);
            }
        }

        foreach (LeafGroup group in groups.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] leafBytes = await ReadPageAsync(group.LeafPage, cancellationToken).ConfigureAwait(false);
            byte[] leaf;
            try
            {
                leaf = (byte[])leafBytes.Clone();
            }
            finally
            {
                ReturnPage(leafBytes);
            }

            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                return false;
            }

            List<IndexEntry> existing = IndexLeafIncremental.DecodeEntries(layout, leaf, _pgSz);
            if (existing.Count == 0)
            {
                return false;
            }

            List<IndexEntry>? spliced = IndexLeafIncremental.Splice(existing, group.Adds, group.RemovePtrs);
            if (spliced is null)
            {
                return false;
            }

            long leafPrev = IndexLeafIncremental.ReadPrevPage(leaf);
            long leafNext = IndexLeafIncremental.ReadNextLeafPage(leaf);
            long leafTail = IndexLeafIncremental.ReadTailPage(leaf);

            if (spliced.Count == 0)
            {
                // leaf-merge on underflow ───────────────────
                // Drop this leaf entirely; surviving siblings absorb the
                // logical key range. The dead-leaf-is-rightmost case is
                // supported: tail_page is
                // recomputed on the parent intermediate AND propagated up
                // every captured ancestor where the parent we mutated was
                // the rightmost child (see TryStageIntermediateRewrites).
                // Remaining caveats:
                //   - Bail when the parent has only one child (removing
                //     would empty the parent → cascade collapse, out of
                //     scope for this path).
                //   - Bail when either leaf-chain neighbour is being
                //     mutated by another group in this batch (would need
                //     coordinated pointer/content writes).
                DescentStep mergeParent = group.Path[group.Path.Count - 1];
                if (mergeParent.Entries.Count < 2)
                {
                    return false;
                }

                // a contiguous run of emptying
                // leaves is allowed; we skip the pair-wise chain-detach
                // here for any neighbour that is also being orphaned.
                // The surviving boundary pointers are patched once after
                // the per-group loop completes (see "boundary stitching"
                // pass below) so they correctly skip the entire run.
                // For surviving neighbours that are ALSO in `groups`
                // (being mutated for content) we still bail, because
                // merge has no way to coordinate a content rewrite +
                // pointer patch on the same page.
                bool prevAlsoEmptying = leafPrev > 0 && emptyingLeaves.Contains(leafPrev);
                bool nextAlsoEmptying = leafNext > 0 && emptyingLeaves.Contains(leafNext);

                if (leafPrev > 0 && groups.ContainsKey(leafPrev) && !prevAlsoEmptying)
                {
                    return false;
                }

                if (leafNext > 0 && groups.ContainsKey(leafNext) && !nextAlsoEmptying)
                {
                    return false;
                }

                // Per-group pair-wise patches happen ONLY when both
                // surviving neighbours are non-emptying (the standalone
                // dead-leaf case). Runs of two
                // or more emptying leaves are stitched together below.
                if (!prevAlsoEmptying && !nextAlsoEmptying)
                {
                    if (leafPrev > 0)
                    {
                        if (!leafPrevPointerPatches.TryAdd(leafPrev, leafNext))
                        {
                            return false;
                        }
                    }

                    if (leafNext > 0)
                    {
                        if (!leafNextPointerPatches.TryAdd(leafNext, leafPrev))
                        {
                            return false;
                        }
                    }
                }

                emptyingLeafSiblings[group.LeafPage] = (leafPrev, leafNext);

                // Stage parent Remove op. ApplyIntermediateOps drops the
                // entry at OriginalIndex; the dead leaf page is orphaned
                // (not appended to any free list — Compact & Repair sweeps
                // it, same as bulk path orphans).
                AddParentOp(parentOps, mergeParent.PageNumber, mergeParent.TakenIndex, IntermediateOpType.Remove, default!);

                continue;
            }

            byte[] oldMaxKey = existing[existing.Count - 1].Key;

            DescentStep parentStep = group.Path[group.Path.Count - 1];

            // ── Try in-place rewrite first ──
            byte[]? rebuilt = IndexLeafIncremental.TryRebuildLeafWithSiblings(
                layout, _pgSz, tdefPage, spliced, leafPrev, leafNext, leafTail);
            if (rebuilt != null)
            {
                if (existingPageRewrites.ContainsKey(group.LeafPage))
                {
                    // Two groups targeted the same leaf — shouldn't happen
                    // (groups are keyed by leaf page). Defensive bail.
                    return false;
                }

                existingPageRewrites[group.LeafPage] = rebuilt;

                IndexEntry newLast = spliced[spliced.Count - 1];
                if (IndexHelpers.CompareKeyBytes(newLast.Key, oldMaxKey) != 0)
                {
                    // Parent's summary entry for this leaf must be replaced.
                    AddParentOp(parentOps, parentStep.PageNumber, parentStep.TakenIndex, IntermediateOpType.Replace, new(newLast, group.LeafPage));
                }

                continue;
            }

            // ── N-way split ──
            // Greedy left-fill into N pages; bails only if a single entry
            // exceeds the page payload area.
            SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, _pgSz, spliced);
            if (splitPages is null)
            {
                return false;
            }

            int splitCount = splitPages.Count;

            // First page reuses group.LeafPage; remaining pages are
            // freshly allocated from the staging counter.
            long[] pageNumbers = new long[splitCount];
            pageNumbers[0] = group.LeafPage;
            for (int p = 1; p < splitCount; p++)
            {
                pageNumbers[p] = nextAllocatedPageNumber++;
            }

            byte[][] pageBytesAll = new byte[splitCount][];
            try
            {
                for (int p = 0; p < splitCount; p++)
                {
                    long thisPrev = p == 0 ? leafPrev : pageNumbers[p - 1];
                    long thisNext = p == splitCount - 1 ? leafNext : pageNumbers[p + 1];
                    pageBytesAll[p] = IndexLeafPageBuilder.BuildLeafPage(
                        layout,
                        _pgSz,
                        tdefPage,
                        splitPages[p],
                        prevPage: thisPrev,
                        nextPage: thisNext,
                        tailPage: 0,
                        enablePrefixCompression: true);
                }
            }
            catch (ArgumentOutOfRangeException)
            {
                return false;
            }

            if (existingPageRewrites.ContainsKey(group.LeafPage))
            {
                return false;
            }

            existingPageRewrites[group.LeafPage] = pageBytesAll[0];
            for (int p = 1; p < splitCount; p++)
            {
                newPageAppends.Add(pageBytesAll[p]);
            }

            // Patch leafNext.prev_page to point at the LAST new page.
            // If leafNext is itself a leaf in another group, we'd need
            // coordinated writes — bail to keep this path simple.
            if (leafNext > 0)
            {
                if (groups.ContainsKey(leafNext))
                {
                    return false;
                }

                if (!leafNextPointerPatches.TryAdd(leafNext, pageNumbers[splitCount - 1]))
                {
                    // Two splits both want to patch the same neighbour leaf.
                    // Should not happen (each leaf has one prev), but defensive.
                    return false;
                }
            }

            // Parent ops: replace existing summary with the LEFT-most's
            // summary, then insert one summary per right page (N-1 of them)
            // immediately after, in left-to-right order. ApplyIntermediateOps
            // preserves declaration order at the same OriginalIndex.
            AddParentOpsForSplitPages(parentOps, parentStep.PageNumber, parentStep.TakenIndex, splitPages, pageNumbers);
        }

        // run-boundary stitching ───────────────────────────
        // For each contiguous run of emptying leaves with at least one
        // surviving boundary on either side, patch the surviving page's
        // sibling pointer to skip OVER the entire run. Per-group patches
        // above only fire for standalone empty leaves; runs of 2+ are
        // stitched here.
        foreach ((long deadPage, (long deadPrev, long deadNext)) in emptyingLeafSiblings)
        {
            // Only act at run boundaries: this dead leaf has at least one
            // non-emptying immediate neighbour OR a chain terminus (0).
            bool prevIsLeftBoundary = deadPrev == 0 || !emptyingLeafSiblings.ContainsKey(deadPrev);
            bool nextIsRightBoundary = deadNext == 0 || !emptyingLeafSiblings.ContainsKey(deadNext);

            if (!prevIsLeftBoundary && !nextIsRightBoundary)
            {
                continue; // strictly internal to a run; nothing to do
            }

            // Walk the run rightwards from deadPage to find the first
            // non-emptying page (or 0 = chain terminus).
            long surv = deadNext;
            while (surv > 0 && emptyingLeafSiblings.ContainsKey(surv))
            {
                surv = emptyingLeafSiblings[surv].Next;
            }

            // Walk leftwards similarly.
            long survLeft = deadPrev;
            while (survLeft > 0 && emptyingLeafSiblings.ContainsKey(survLeft))
            {
                survLeft = emptyingLeafSiblings[survLeft].Prev;
            }

            // Apply the patches at run boundaries (idempotent — multiple
            // dead leaves in the same run all compute the same survLeft /
            // survRight, so TryAdd may legitimately collide; treat the
            // collision as success when the staged value matches).
            if (prevIsLeftBoundary && deadPrev > 0 && !groups.ContainsKey(deadPrev))
            {
                if (!leafPrevPointerPatches.TryAdd(deadPrev, surv) &&
                    leafPrevPointerPatches[deadPrev] != surv)
                {
                    return false;
                }
            }

            if (nextIsRightBoundary && deadNext > 0 && !groups.ContainsKey(deadNext))
            {
                if (!leafNextPointerPatches.TryAdd(deadNext, survLeft) &&
                    leafNextPointerPatches[deadNext] != survLeft)
                {
                    return false;
                }
            }
        }

        // ── Phase C: aggregate intermediate rewrites ─────────────────
        // For every parent intermediate that received ops, build a fresh
        // entry list, attempt to rebuild in place, and propagate any
        // resulting max-key changes up the captured paths.
        // When an in-place rebuild overflows AND the page is a parent-
        // of-leaf intermediate (deepest captured level), greedy-split
        // the entries 2-way and either propagate to the grandparent or
        // (if this is the root) allocate a fresh root and patch first_dp.
        // Higher-level (non-parent-of-leaf)
        // intermediates split too — the helper looks up child
        // intermediates' rightmost-leaf via either pending overrides,
        // staged rewrites, or a cache-backed read of the live page.
        var stagingState = new IntermediateStagingState
        {
            NextAllocatedPageNumber = nextAllocatedPageNumber,
        };
        bool stagingOk = await TryStageIntermediateRewritesAsync(
            layout,
            tdefPage,
            groups,
            parentOps,
            existingPageRewrites,
            stagingState,
            newPageAppends,
            cancellationToken).ConfigureAwait(false);

        if (!stagingOk)
        {
            return false;
        }

        // ── Phase D: validate + Phase E: commit ──────────────────────
        // Validation already done implicitly (every staged page has been
        // built via a try-call that returned null/false on overflow). Now
        // commit in safe order:
        //   1. Append new pages (right halves of leaf splits) so their page
        //      numbers exist before any in-place rewrite references them.
        //   2. Patch sibling pointers on any leafNext outside the touched
        //      set.
        //   3. Rewrite all in-place pages (leaves first, then intermediates,
        //      to minimise observable inconsistency for any concurrent
        //      reader between writes — though there are none in single-
        //      writer mode).

        long verifyNextPage = _stream.Length / _pgSz;
        foreach (byte[] pageBytes in newPageAppends)
        {
            long appended = await AppendPageAsync(pageBytes, cancellationToken).ConfigureAwait(false);
            if (appended != verifyNextPage)
            {
                // Stream was extended by something else mid-flight (shouldn't
                // happen in single-writer mode). Bail loudly via false; the
                // partially-appended right page is just an orphan.
                return false;
            }

            verifyNextPage++;
        }

        foreach ((long neighbourPage, long newPrevValue) in leafNextPointerPatches)
        {
            byte[] neighbourBytes = await ReadPageAsync(neighbourPage, cancellationToken).ConfigureAwait(false);
            byte[] neighbour;
            try
            {
                neighbour = (byte[])neighbourBytes.Clone();
            }
            finally
            {
                ReturnPage(neighbourBytes);
            }

            // §4.1 prev_page is at offset 8.
            BinaryPrimitives.WriteInt32LittleEndian(neighbour.AsSpan(8, 4), checked((int)newPrevValue));
            await WritePageAsync(neighbourPage, neighbour, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long neighbourPage, long newNextValue) in leafPrevPointerPatches)
        {
            byte[] neighbourBytes = await ReadPageAsync(neighbourPage, cancellationToken).ConfigureAwait(false);
            byte[] neighbour;
            try
            {
                neighbour = (byte[])neighbourBytes.Clone();
            }
            finally
            {
                ReturnPage(neighbourBytes);
            }

            // §4.1 next_page is at offset 12.
            BinaryPrimitives.WriteInt32LittleEndian(neighbour.AsSpan(12, 4), checked((int)newNextValue));
            await WritePageAsync(neighbourPage, neighbour, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long pageNum, byte[] bytes) in existingPageRewrites)
        {
            await WritePageAsync(pageNum, bytes, cancellationToken).ConfigureAwait(false);
        }

        long? newRootPage = stagingState.NewRootPage;

        // if the root intermediate split, patch the real-idx
        // first_dp slot on the TDEF page to point at the freshly-allocated
        // root. The new root page itself was already appended via
        // newPageAppends above, so the page number is stable.
        if (newRootPage.HasValue)
        {
            byte[] tdefBytesRaw = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            byte[] tdefBytes;
            try
            {
                tdefBytes = (byte[])tdefBytesRaw.Clone();
            }
            finally
            {
                ReturnPage(tdefBytesRaw);
            }

            BinaryPrimitives.WriteInt32LittleEndian(tdefBytes.AsSpan(firstDpOffset, 4), checked((int)newRootPage.Value));
            await WritePageAsync(tdefPage, tdefBytes, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Per-key path-capturing descent. Builds one <see cref="LeafGroup"/>
    /// per distinct target leaf, sharing the captured intermediate path
    /// across all keys that landed on the same leaf. Returns
    /// <see langword="null"/> on any descent failure (overshoot into
    /// tail_page chain, malformed page, encoder mismatch) or when the
    /// distinct-leaf count exceeds the cap supplied by the caller.
    /// </summary>
    private async ValueTask<Dictionary<long, LeafGroup>?> GroupChangesByTargetLeafAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long firstDp,
        List<IndexEntry> addEntries,
        List<IndexEntry> removeEntries,
        int maxLeafGroupCount,
        CancellationToken cancellationToken)
    {
        var groups = new Dictionary<long, LeafGroup>();

        for (int i = 0; i < addEntries.Count; i++)
        {
            (byte[] key, long dp, byte dr) = addEntries[i];
            LeafGroup? g = await DescendOrLookupGroupAsync(layout, firstDp, key, groups, cancellationToken).ConfigureAwait(false);
            if (g is null)
            {
                return null;
            }

            var decoded = new IndexEntry(key, dp, dr);
            g.Adds.Add(decoded);

            if (groups.Count > maxLeafGroupCount)
            {
                return null;
            }
        }

        for (int i = 0; i < removeEntries.Count; i++)
        {
            (byte[] key, long dp, byte dr) = removeEntries[i];
            LeafGroup? g = await DescendOrLookupGroupAsync(layout, firstDp, key, groups, cancellationToken).ConfigureAwait(false);
            if (g is null)
            {
                return null;
            }

            g.RemovePtrs.Add((dp, dr));
            if (groups.Count > maxLeafGroupCount)
            {
                return null;
            }
        }

        return groups;
    }

    private async ValueTask<LeafGroup?> DescendOrLookupGroupAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long firstDp,
        byte[] key,
        Dictionary<long, LeafGroup> groups,
        CancellationToken cancellationToken)
    {
        // Always descend: the page cache amortises the cost, and the
        // captured path lets us verify the key actually landed there
        // (reusing a stale path could mis-route a key that overshoots).
        var path = new List<DescentStep>();
        long leafPage = await DescendCapturingAsync(layout, firstDp, key, path, cancellationToken).ConfigureAwait(false);
        if (leafPage <= 0 || path.Count == 0)
        {
            return null;
        }

        if (groups.TryGetValue(leafPage, out LeafGroup? existing))
        {
            return existing;
        }

        var fresh = new LeafGroup(leafPage, path);
        groups[leafPage] = fresh;
        return fresh;
    }

    /// <summary>
    /// Mutable staging state shared between
    /// <see cref="TrySurgicalCrossLeafMaintainAsync"/> and
    /// <see cref="TryStageIntermediateRewritesAsync"/>. Replaces the
    /// <c>ref</c>/<c>out</c> parameters that the original synchronous helper
    /// used (async signatures cannot carry <c>ref</c>/<c>out</c>).
    /// </summary>
    private sealed class IntermediateStagingState
    {
        /// <summary>Gets or sets the next page number to allocate from the end of the file.</summary>
        public long NextAllocatedPageNumber { get; set; }

        /// <summary>Gets or sets the page number of the freshly-allocated root intermediate when the root split.</summary>
        public long? NewRootPage { get; set; }
    }

    /// <summary>
    /// helper. Returns the effective <c>tail_page</c> (rightmost
    /// leaf reachable through <paramref name="intermediatePage"/>'s subtree)
    /// taking pending mutations into account. Lookup priority:
    /// <list type="number">
    ///   <item><paramref name="overrides"/> (explicit per-page tail recorded
    ///   when an intermediate was rewritten or split earlier in the same
    ///   batch);</item>
    ///   <item><paramref name="rewrites"/> (staged in-memory rewrite of the
    ///   page \u2014 read its <c>tail_page</c> header bytes);</item>
    ///   <item>live page bytes via the page cache (untouched intermediates).</item>
    /// </list>
    /// </summary>
    private async ValueTask<long> GetEffectiveTailPageAsync(
        long intermediatePage,
        Dictionary<long, long> overrides,
        Dictionary<long, byte[]> rewrites,
        CancellationToken cancellationToken)
    {
        if (overrides.TryGetValue(intermediatePage, out long staged))
        {
            return staged;
        }

        if (rewrites.TryGetValue(intermediatePage, out byte[]? rewriteBytes))
        {
            return IndexLeafIncremental.ReadTailPage(rewriteBytes);
        }

        byte[] raw = await ReadPageAsync(intermediatePage, cancellationToken).ConfigureAwait(false);
        try
        {
            return IndexLeafIncremental.ReadTailPage(raw);
        }
        finally
        {
            ReturnPage(raw);
        }
    }

    /// <summary>
    /// Stage rewrites for every parent intermediate touched by per-leaf ops,
    /// then propagate any resulting max-key changes up each LeafGroup's
    /// captured path. Returns <see langword="false"/> on any unrecoverable
    /// shared-ancestor conflict. When an in-place rebuild
    /// overflows AND the page is a parent-of-leaf intermediate (deepest
    /// captured level whose children are leaves), greedy-split the entries
    /// 2-way and either propagate to the grandparent (Replace + InsertAfter)
    /// or, if the splitting page IS the root, allocate a new root
    /// intermediate with two summary entries pointing at the two halves and
    /// signal the caller to patch <c>first_dp</c>. Higher-level intermediates
    /// (children are themselves intermediates)
    /// also split in place — the left half's <c>tail_page</c> is computed by
    /// looking up the rightmost-child intermediate's effective tail via
    /// staged overrides, staged rewrites, or a cache-backed read of the
    /// live page. Recursive split through any number of levels (up to root
    /// reallocation) is supported; only 3+-page splits at any single level
    /// (TryGreedySplitIntermediateInTwo overflow) still bail to the bulk path.
    /// </summary>
    private async ValueTask<bool> TryStageIntermediateRewritesAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long tdefPage,
        Dictionary<long, LeafGroup> groups,
        Dictionary<long, List<IntermediateOp>> parentOps,
        Dictionary<long, byte[]> existingPageRewrites,
        IntermediateStagingState stagingState,
        List<byte[]> newPageAppends,
        CancellationToken cancellationToken)
    {
        stagingState.NewRootPage = null;

        // Track which intermediates are "parent-of-leaf" (children are
        // leaves, NOT intermediates). These are the only pages the leaf-split
        // helper is willing to split — splitting a higher-level intermediate
        // requires reading its children's tail_page values to recompute
        // the split halves' tail_page headers, handled by the recursive
        // helper below.
        var parentOfLeaf = new HashSet<long>(parentOps.Keys);

        // Build a map of every intermediate page touched, keyed by page
        // number, with a reference DescentStep (for header preservation +
        // original entries). Multiple groups may pass through the same
        // intermediate — they ALL carry the same canonical bytes by
        // construction (DescendCapturingAsync reads the same page bytes;
        // we rely on the page cache returning the same content per call,
        // which it does in single-writer mode because no mid-batch write
        // touches these pages yet).
        var intermediateRefs = new Dictionary<long, DescentStep>(parentOps.Count * 2);
        var intermediateGrandparent = new Dictionary<long, (long ParentPage, int IndexInParent)>(parentOps.Count * 2);

        // tail_page propagation. When a per-leaf
        // splice removes the parent's rightmost child entry (or a leaf
        // split appends a new rightmost child), the parent intermediate's
        // tail_page header must be recomputed to point at the NEW rightmost
        // leaf in the parent's subtree. The change cascades up: any
        // ancestor whose own rightmost child is the parent we just
        // modified inherits the new tail value. We record per-intermediate
        // tail overrides here as we process pages deepest-first so the
        // shallower intermediates' rebuild step can pick up the inherited
        // value via the lookup below.
        var intermediateTailOverrides = new Dictionary<long, long>(parentOps.Count * 2);

        // Also: remember each group's path so we can propagate max-key
        // changes upward when a parent's rewrite changes its own max.
        foreach (LeafGroup group in groups.Values)
        {
            for (int level = 0; level < group.Path.Count; level++)
            {
                DescentStep step = group.Path[level];
                if (!intermediateRefs.ContainsKey(step.PageNumber))
                {
                    intermediateRefs[step.PageNumber] = step;
                }

                if (level > 0)
                {
                    DescentStep parent = group.Path[level - 1];
                    intermediateGrandparent[step.PageNumber] = (parent.PageNumber, parent.TakenIndex);
                }
            }
        }

        // Process intermediates from deepest level up. We don't know depth
        // explicitly, but parentOps initially keys ONLY parent-of-leaf
        // intermediates. As we propagate max-key changes up, we add ops to
        // shallower intermediates. Process in passes, deepest first.

        // Compute depth of each intermediate via the captured paths.
        var depthOf = new Dictionary<long, int>(intermediateRefs.Count);
        foreach (LeafGroup group in groups.Values)
        {
            for (int level = 0; level < group.Path.Count; level++)
            {
                long pn = group.Path[level].PageNumber;
                if (!depthOf.TryGetValue(pn, out int existingDepth) || existingDepth < level)
                {
                    depthOf[pn] = level;
                }
            }
        }

        // Process pages in descending depth (deepest first).
        var pending = new List<long>(parentOps.Keys);
        while (pending.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Pick the deepest pending page.
            long deepest = pending[0];
            int deepestDepth = depthOf.TryGetValue(deepest, out int d0) ? d0 : -1;
            for (int i = 1; i < pending.Count; i++)
            {
                long candidate = pending[i];
                int cd = depthOf.TryGetValue(candidate, out int dc) ? dc : -1;
                if (cd > deepestDepth)
                {
                    deepest = candidate;
                    deepestDepth = cd;
                }
            }

            pending.Remove(deepest);

            if (!parentOps.TryGetValue(deepest, out List<IntermediateOp>? ops) || ops.Count == 0)
            {
                continue;
            }

            if (!intermediateRefs.TryGetValue(deepest, out DescentStep refStep))
            {
                // No descent passed through this page — shouldn't happen
                // because all ops were registered against pages we descended
                // through. Defensive bail.
                return false;
            }

            // Validate every op's OriginalIndex is in range.
            foreach (IntermediateOp op in ops)
            {
                if (op.OriginalIndex < 0 || op.OriginalIndex >= refStep.Entries.Count)
                {
                    return false;
                }
            }

            List<DecodedIntermediateEntry> newEntries =
                IndexHelpers.ApplyIntermediateOps(refStep.Entries, ops);

            if (newEntries.Count == 0)
            {
                // Recursive intermediate collapse on cascading
                // underflow ──────────────────────────────────────────
                // A multi-group delete batch removed every child of this
                // intermediate. Cascade the removal up: stage a Remove op
                // on the grandparent for the slot that referenced this
                // page, then re-enqueue the grandparent so the loop picks
                // up the new ops on a subsequent pass. The dead intermediate
                // page is orphaned (same disposal model as dead leaves and
                // bulk-rebuild orphans — Compact & Repair sweeps it). When
                // this collapse happens to the root (no grandparent) the
                // entire tree has emptied; we still bail because emitting
                // a fresh empty single-leaf root would require allocating
                // a leaf page and patching first_dp, which the bulk path
                // already does correctly.
                if (!intermediateGrandparent.TryGetValue(deepest, out (long ParentPage, int IndexInParent) gpCollapse))
                {
                    return false;
                }

                AddParentOp(parentOps, gpCollapse.ParentPage, gpCollapse.IndexInParent, IntermediateOpType.Remove, default!);

                if (!pending.Contains(gpCollapse.ParentPage))
                {
                    pending.Add(gpCollapse.ParentPage);
                }

                // No staged rewrite for `deepest`: it's orphaned. Skip the
                // rest of the per-page rebuild path.
                continue;
            }

            byte[] origBytes = refStep.PageBytes;
            long origPrev = (uint)BinaryPrimitives.ReadInt32LittleEndian(origBytes.AsSpan(8, 4));
            long origNext = (uint)BinaryPrimitives.ReadInt32LittleEndian(origBytes.AsSpan(12, 4));
            long origTail = (uint)BinaryPrimitives.ReadInt32LittleEndian(origBytes.AsSpan(16, 4));

            // Recompute tail_page based on the post-mutation
            // entry list. For parent-of-leaf intermediates the rightmost
            // leaf is always the LAST entry's ChildPage. For higher
            // intermediates we inherit the new tail from the rightmost
            // child intermediate — first checking the override map (set
            // when that child was rewritten or split earlier in this
            // batch), then falling back to GetEffectiveTailPageAsync
            // which reads the live or staged page header. The live-page
            // fallback matters for the recursive-collapse case:
            // when a Remove drops the previous rightmost child entry
            // entirely, the new rightmost child may be an untouched
            // intermediate whose tail is only available on disk. The
            // fix-up only applies when the page genuinely had a non-zero
            // origTail — single-leaf-root state (origTail = 0) stays
            // untouched.
            long newTail = origTail;
            if (origTail != 0)
            {
                long lastChildPage = newEntries[newEntries.Count - 1].ChildPage;
                if (parentOfLeaf.Contains(deepest))
                {
                    newTail = lastChildPage;
                }
                else
                {
                    newTail = await GetEffectiveTailPageAsync(
                        lastChildPage, intermediateTailOverrides, existingPageRewrites, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            if (newTail != origTail)
            {
                intermediateTailOverrides[deepest] = newTail;
            }

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, _pgSz, tdefPage, newEntries, origPrev, origNext, newTail);
            if (rebuilt is null)
            {
                // Intermediate overflow.
                // Greedy left-fill split into N pages; each subsequent page
                // is freshly allocated. For parent-of-leaf intermediates
                // each split page's tail_page = its rightmost child's
                // ChildPage (the leaf itself). For higher intermediates we
                // look up each split page's rightmost child's effective
                // tail_page (staged override, staged rewrite, or live page).
                // Either grandparent absorbs N new summaries (Replace +
                // (N-1) InsertAfter) and we recurse into it, OR — when this
                // page is the root — we allocate a fresh root intermediate
                // with N summary entries pointing at every split page and
                // signal the caller to patch first_dp.
                List<List<DecodedIntermediateEntry>>? splitInts =
                    IndexHelpers.TryGreedySplitIntermediateInN(layout, _pgSz, tdefPage, newEntries);
                if (splitInts is null)
                {
                    // Single entry too big for any intermediate page — bail.
                    return false;
                }

                int nSplit = splitInts.Count;

                // First split page reuses `deepest`; remaining pages are
                // freshly allocated.
                long[] intPageNumbers = new long[nSplit];
                intPageNumbers[0] = deepest;
                for (int p = 1; p < nSplit; p++)
                {
                    intPageNumbers[p] = stagingState.NextAllocatedPageNumber++;
                }

                // Compute each split page's tail_page.
                long[] intTails = new long[nSplit];
                if (parentOfLeaf.Contains(deepest))
                {
                    for (int p = 0; p < nSplit; p++)
                    {
                        var lastEntry = splitInts[p][splitInts[p].Count - 1];

                        // Last split page inherits origTail when non-zero
                        // (preserves the existing rightmost-leaf pointer
                        // semantics on the rightmost subtree); other pages
                        // get their own rightmost child as the leaf tail.
                        intTails[p] = (p == nSplit - 1 && origTail != 0) ? origTail : lastEntry.ChildPage;
                    }
                }
                else
                {
                    for (int p = 0; p < nSplit; p++)
                    {
                        var lastEntry = splitInts[p][splitInts[p].Count - 1];
                        intTails[p] = await GetEffectiveTailPageAsync(
                            lastEntry.ChildPage, intermediateTailOverrides, existingPageRewrites, cancellationToken)
                            .ConfigureAwait(false);
                    }
                }

                byte[][] intPageBytesAll = new byte[nSplit][];
                try
                {
                    for (int p = 0; p < nSplit; p++)
                    {
                        long thisPrev = p == 0 ? origPrev : intPageNumbers[p - 1];
                        long thisNext = p == nSplit - 1 ? origNext : intPageNumbers[p + 1];
                        byte[]? built = IndexBTreeBuilder.TryBuildIntermediatePage(
                            layout, _pgSz, tdefPage, splitInts[p], thisPrev, thisNext, intTails[p]);
                        if (built is null)
                        {
                            return false;
                        }

                        intPageBytesAll[p] = built;
                    }
                }
                catch (ArgumentOutOfRangeException)
                {
                    return false;
                }

                if (existingPageRewrites.ContainsKey(deepest))
                {
                    return false;
                }

                existingPageRewrites[deepest] = intPageBytesAll[0];
                for (int p = 1; p < nSplit; p++)
                {
                    newPageAppends.Add(intPageBytesAll[p]);
                }

                // Record every split page's tail so any shallower split
                // that looks up these pages picks up the post-split values
                // without re-reading the (now stale) live pages.
                for (int p = 0; p < nSplit; p++)
                {
                    intermediateTailOverrides[intPageNumbers[p]] = intTails[p];
                }

                if (intermediateGrandparent.TryGetValue(deepest, out (long ParentPage, int IndexInParent) gpSplit))
                {
                    // Grandparent absorbs: Replace the original summary at
                    // IndexInParent with the FIRST split page's summary,
                    // then InsertAfter one summary per remaining split page
                    // in left-to-right order. Recurse into grandparent in
                    // case it also overflows.
                    // Use helper for Replace + InsertAfter ops for split intermediate pages
                    AddParentOpsForSplitPages(
                        parentOps,
                        gpSplit.ParentPage,
                        gpSplit.IndexInParent,
                        new(splitInts.ConvertAll(s => s.ConvertAll(si => si.Entry))),
                        intPageNumbers);

                    if (!pending.Contains(gpSplit.ParentPage))
                    {
                        pending.Add(gpSplit.ParentPage);
                    }
                }
                else
                {
                    // No grandparent — this WAS the root intermediate.
                    // Allocate a fresh root with one summary entry per
                    // split page. tail_page of the new root = the LAST
                    // split page's tail (= rightmost leaf in the tree).
                    if (stagingState.NewRootPage.HasValue)
                    {
                        // Already split a root once in this batch (multi-
                        // group case); only one root is allowed. Bail.
                        return false;
                    }

                    long newRootPageAlloc = stagingState.NextAllocatedPageNumber++;
                    var rootEntries = new List<DecodedIntermediateEntry>(nSplit);
                    for (int p = 0; p < nSplit; p++)
                    {
                        var pLast = splitInts[p][splitInts[p].Count - 1];
                        rootEntries.Add(pLast);
                    }

                    byte[]? newRootBytes;
                    try
                    {
                        newRootBytes = IndexBTreeBuilder.TryBuildIntermediatePage(
                            layout, _pgSz, tdefPage, rootEntries, prevPage: 0, nextPage: 0, tailPage: intTails[nSplit - 1]);
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        return false;
                    }

                    if (newRootBytes is null)
                    {
                        return false;
                    }

                    newPageAppends.Add(newRootBytes);
                    stagingState.NewRootPage = newRootPageAlloc;
                }

                continue;
            }

            if (existingPageRewrites.ContainsKey(deepest))
            {
                // An intermediate page should never collide with a leaf
                // rewrite (different page-type populations). Defensive bail.
                return false;
            }

            existingPageRewrites[deepest] = rebuilt;

            // Did the page's max key change? Compare new last entry to
            // original last entry's key.
            var newMax = newEntries[newEntries.Count - 1];
            DecodedIntermediateEntry oldMax = refStep.Entries[refStep.Entries.Count - 1];
            bool maxChanged = newMax != oldMax;

            if (maxChanged && intermediateGrandparent.TryGetValue(deepest, out (long ParentPage, int IndexInParent) gp))
            {
                // Propagate: grandparent's summary entry for this
                // intermediate (at IndexInParent) needs to carry the new
                // max key (and same ChildPage = this intermediate page).
                AddParentOp(parentOps, gp.ParentPage, gp.IndexInParent, IntermediateOpType.Replace, newMax);

                if (!pending.Contains(gp.ParentPage))
                {
                    pending.Add(gp.ParentPage);
                }
            }

            // If maxChanged but no grandparent (this WAS the root) — that's
            // fine, the root's max key doesn't need propagation anywhere.
        }

        return true;
    }

    /// <summary>
    /// Encodes the (composite-key, page, row) tuples for the rows in
    /// <paramref name="rows"/> against the supplied key column descriptors.
    /// Returns a partially-filled list when an encoder throws — the caller
    /// detects this by comparing <c>Count</c> to the input count and bailing
    /// to the bulk-rebuild path.
    /// </summary>
    private List<IndexEntry> EncodeHintEntries(
        IReadOnlyList<(RowLocation Loc, object[] Row)>? rows,
        List<KeyColumnInfo> keyColInfos)
    {
        var results = new List<IndexEntry>(rows?.Count ?? 0);
        if (rows == null || rows.Count == 0)
        {
            return results;
        }

        // Jet4 .mdb uses the LegacyFixedPointColumnDescriptor
        // byte-twiddling rules for T_NUMERIC keys; ACCDB / ACE uses the
        // post-2007 form. Mirrors the bulk path's `legacyNumeric` flag.
        bool legacyNumeric = _format == DatabaseFormat.Jet4Mdb;

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
                    perColumn[k] = col.Type == T_NUMERIC
                        ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, col.NumericScale, legacyNumeric)
                        : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
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
                return results;
            }

            byte[] composite = new byte[totalLen];
            int offset = 0;
            for (int k = 0; k < perColumn.Length; k++)
            {
                Buffer.BlockCopy(perColumn[k], 0, composite, offset, perColumn[k].Length);
                offset += perColumn[k].Length;
            }

            var result = new IndexEntry(composite, loc.PageNumber, (byte)loc.RowIndex);
            results.Add(result);
        }

        return results;
    }

    private record struct IndexKeyColumn(int ColNum, bool Ascending);

    private record struct KeyColumnInfo(ColumnInfo Col, int SnapIdx, bool Ascending);

    private record struct RealIdxEntry(IReadOnlyList<IndexKeyColumn> IndexKeyColumns, int FirstDpOffset, bool IsUnique);

    private record struct UniqueIndexDescriptor(int RealIdxNum, string Name, IReadOnlyList<KeyColumnInfo> KeyColumns);

    /// <summary>
    /// parse the TDEF page and return one descriptor per <em>unique</em>
    /// real-idx slot (uniqueness is signalled either by the §3.1 real-idx
    /// <c>flags &amp; 0x01</c> bit or by an associated logical-idx entry whose
    /// <c>index_type = 0x01</c> primary-key discriminator). Returns an empty
    /// list on Jet3 (no index emission) or when the TDEF declares no indexes.
    /// </summary>
    private async ValueTask<List<UniqueIndexDescriptor>> LoadUniqueIndexDescriptorsAsync(
        long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        var result = new List<UniqueIndexDescriptor>();

        // §3.1 / §3.2 per-format sizes + offsets.
        bool jet3 = _format == DatabaseFormat.Jet3Mdb;
        int realIdxPhysSz = jet3 ? 39 : 52;
        int logIdxEntrySz = jet3 ? 20 : 28;
        int physFlagsOff = jet3 ? 38 : 42;
        int logIndexNum2Off = jet3 ? 4 : 8;
        int logIndexTypeOff = jet3 ? 19 : 23;

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
        int logIdxStart = realIdxDescStart + (numRealIdx * realIdxPhysSz);

        // Decode every real-idx slot's key columns + flag bit. Reuses the
        // RealIdxEntry shape from MaintainIndexesAsync; FirstDpOffset is
        // unused on this path (we don't rewrite first_dp here) so it's left
        // at 0.
        var realIdxSlots = new Dictionary<int, RealIdxEntry>();
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * realIdxPhysSz);
            if (phys + realIdxPhysSz > tdefBuffer.Length)
            {
                break;
            }

            var keyCols = new List<IndexKeyColumn>(10);
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                int cn = Ru16(tdefBuffer, so);
                if (cn == 0xFFFF)
                {
                    continue;
                }

                byte order = tdefBuffer[so + 2];
                keyCols.Add(new(cn, order != 0x02));
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            realIdxSlots[ri] = new RealIdxEntry(keyCols, FirstDpOffset: 0, IsUnique: (tdefBuffer[phys + physFlagsOff] & 0x01) != 0);
        }

        // Walk the logical-idx entries to (a) promote any real-idx that backs a
        // PK logical-idx to unique, and (b) capture a best-effort name for each
        // unique real-idx (first logical-idx referencing it wins).
        int logIdxNamesStart = logIdxStart + (numIdx * logIdxEntrySz);
        List<string> logIdxNames = ReadLogicalIdxNames(tdefBuffer, logIdxNamesStart, numIdx);

        var pkRealIdxNums = new HashSet<int>();
        var nameByRealIdx = new Dictionary<int, string>();
        for (int li = 0; li < numIdx; li++)
        {
            int entryStart = logIdxStart + (li * logIdxEntrySz);
            if (entryStart + logIdxEntrySz > tdefBuffer.Length)
            {
                break;
            }

            int realIdxNum = Ri32(tdefBuffer, entryStart + logIndexNum2Off);
            byte indexType = tdefBuffer[entryStart + logIndexTypeOff];
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
            if (!TryResolveKeyColumnInfos(slot.IndexKeyColumns, tableDef.Columns, snapshotIndexByColNum, out List<KeyColumnInfo> keyColInfos))
            {
                continue;
            }

            string name = nameByRealIdx.TryGetValue(realIdxNum, out string? n) ? n : $"realidx#{realIdxNum}";
            result.Add(new UniqueIndexDescriptor(realIdxNum, name, keyColInfos));
        }

        return result;
    }

    /// <summary>
    /// encode the composite index key for one row using a previously
    /// computed canonical numeric scale per key column.
    /// All key column types accepted by <see cref="IndexHelpers.ResolveIndexes"/> have
    /// matching <see cref="IndexKeyEncoder"/> support; any encoder failure
    /// propagates to the caller as an unrecoverable error.
    /// </summary>
    private byte[] EncodeCompositeKeyForUniqueCheck(
        UniqueIndexDescriptor descriptor,
        object[] row,
        int[] numericTargetScales)
    {
        bool legacyNumeric = _format == DatabaseFormat.Jet4Mdb;
        byte[][] perColumn = new byte[descriptor.KeyColumns.Count][];
        int totalLen = 0;
        for (int k = 0; k < descriptor.KeyColumns.Count; k++)
        {
            (ColumnInfo col, int snapIdx, bool ascending) = descriptor.KeyColumns[k];
            object cell = snapIdx < row.Length ? row[snapIdx] : DBNull.Value;
            object? value = cell is null or DBNull ? null : cell;
            perColumn[k] = col.Type == T_NUMERIC
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, (byte)numericTargetScales[k], legacyNumeric)
                : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
            totalLen += perColumn[k].Length;
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
    /// pre-write unique-index validation for an insert batch. Loads the
    /// table snapshot once, encodes the existing-row keys + the pending-row
    /// keys for every unique index, and throws
    /// <see cref="InvalidOperationException"/> on the first collision (existing
    /// vs. pending or pending vs. pending). All key column types accepted by
    /// <see cref="IndexHelpers.ResolveIndexes"/> have matching <see cref="IndexKeyEncoder"/>
    /// support, so encoder rejection propagates as an unrecoverable error
    /// rather than being silently skipped.
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
    /// pre-write unique-index validation for an update batch. The caller
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
    /// core: builds the post-mutation effective row set (snapshot rows
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
            // Canonical scale = column's DECLARED scale.
            int[] numericTargetScales = new int[descriptor.KeyColumns.Count];
            for (int k = 0; k < descriptor.KeyColumns.Count; k++)
            {
                ColumnInfo kCol = descriptor.KeyColumns[k].Col;
                numericTargetScales[k] = kCol.Type == T_NUMERIC ? kCol.NumericScale : -1;
            }

            // Encode every effective row's key. Collect into a HashSet keyed
            // by composite key bytes; first collision triggers the throw.
            var seen = new HashSet<byte[]>(ByteArrayEqualityComparer.Instance);

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

                byte[] key = EncodeCompositeKeyForUniqueCheck(descriptor, effectiveRow, numericTargetScales);

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }

            for (int p = 0; p < pendingCount; p++)
            {
                byte[] key = EncodeCompositeKeyForUniqueCheck(descriptor, pendingInsertRows[p], numericTargetScales);

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }

            _ = totalRows;
        }
    }

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
}
