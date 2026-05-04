namespace JetDatabaseWriter;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueEncoding;
using static JetDatabaseWriter.Constants.ColumnTypes;
using KeyColumnInfo = JetDatabaseWriter.Indexes.IndexLayout.KeyColumnInfo;
using RealIdxEntry = JetDatabaseWriter.Indexes.IndexLayout.RealIdxEntry;
using UniqueIndexDescriptor = JetDatabaseWriter.Indexes.IndexLayout.UniqueIndexDescriptor;

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

    /// <summary>
    /// Maximum recursion depth for cascade-delete / cascade-update chains.
    /// Guards against pathological self-referential cycles. Real-world Access
    /// schemas almost never exceed depth 3.
    /// </summary>
    internal const int CascadeMaxDepth = 64;

    private readonly AccessWriterOptions _options;
    private readonly LockFileCoordinator _lockFileCoordinator;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via DisposeStateLockAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    private readonly ReaderWriterLockSlim _stateLock = new(LockRecursionPolicy.NoRecursion);

    // Agile re-encryption context. When non-null, the underlying _stream is an
    // in-memory MemoryStream containing the *decrypted* inner ACCDB; on
    // DisposeAsync the bytes are re-encrypted with Office Crypto API "Agile"
    // and written back to _outerEncryptedStream (which holds the original CFB).
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via RewrapAndCloseOuterEncryptedStreamAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
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

    /// <summary>Owns the foreign-key / relationship subsystem. The bulk of
    /// FK code (catalog rows, per-TDEF logical-index entries, runtime
    /// referential-integrity enforcement) lives there; <see cref="AccessWriter"/>
    /// keeps only thin public-API forwarders.</summary>
    private readonly RelationshipManager _relationships;

    /// <summary>Gets the relationship subsystem for sibling managers (e.g. <see cref="ComplexColumnManager"/>)
    /// that need to delegate FK / system-table lookups.</summary>
    internal RelationshipManager Relationships => _relationships;

    /// <summary>The single instance owning index B-tree maintenance: bulk rebuild,
    /// incremental fast paths, and the catalog-index single-leaf splice. AccessWriter
    /// keeps only thin instance forwarders.</summary>
    private readonly IndexMaintainer _indexMaintainer;

    /// <summary>Owns the Attachment / MultiValue (complex column) subsystem:
    /// system-table scaffolding, per-table allocation of ComplexID /
    /// ConceptualTableID, hidden flat-child-table emission, the row-level
    /// Add* APIs, and cascade / drop / rename plumbing for the artifacts.
    /// AccessWriter keeps only thin public-API forwarders.</summary>
    private readonly ComplexColumnManager _complexColumns;

    /// <summary>Owns TDEF emission and empty-database bootstrap builders.
    /// AccessWriter keeps thin compatibility forwarders for existing callers.</summary>
    private readonly TDefPageBuilder _tdefPageBuilder;

    /// <summary>Gets the complex-column subsystem so sibling managers (e.g. <see cref="Relationships.RelationshipManager"/>)
    /// can delegate cascade-on-delete to the complex children.</summary>
    internal ComplexColumnManager ComplexColumns => _complexColumns;

    // Active explicit transaction. Writer disposal rolls back any in-flight
    // transaction through JetTransaction.DisposeAsync before clearing the
    // field so analyzers can see the owned resource is disposed.
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via DisposeActiveTransactionAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    private JetTransaction? _activeTransaction;
    private long _cachedInsertTDefPage = -1;
    private long _cachedInsertPageNumber = -1;

    private AccessWriter(
        string path,
        Stream stream,
        byte[] header,
        AccessWriterOptions options,
        Stream? outerEncryptedStream = null,
        bool outerEncryptedLeaveOpen = false,
        bool isAgileEncryptedRewrap = false)
        : base(stream, header, path)
    {
        _options = options;
        _lockFileCoordinator = LockFileCoordinator.ForWriter(path, options);
        _outerEncryptedStream = outerEncryptedStream;
        _outerEncryptedLeaveOpen = outerEncryptedLeaveOpen;
        _isAgileEncryptedRewrap = isAgileEncryptedRewrap;
        _relationships = new RelationshipManager(this);
        _indexMaintainer = new IndexMaintainer(this);
        _complexColumns = new ComplexColumnManager(this);
        _tdefPageBuilder = new TDefPageBuilder(this);

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
            EncryptionManager.ResolveReaderPageKeys(header, _format, isLegacyAesCfb, options.Password);

        using var lockGuard = _lockFileCoordinator.AcquireWithRollback();
        _byteRangeLock = JetByteRangeLock.Create(stream, options.UseByteRangeLocks, options.LockTimeoutMilliseconds);
        lockGuard.Commit();
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
                        options,
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
                options);
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

        byte[] dbBytes = TDefPageBuilder.BuildEmptyDatabase(format, options?.WriteFullCatalogSchema ?? true);

        await using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
        {
            await fs.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            AccessWriter writer = await OpenAsync(path, options, cancellationToken).ConfigureAwait(false);
            await writer._complexColumns.ScaffoldSystemTablesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
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

        byte[] dbBytes = TDefPageBuilder.BuildEmptyDatabase(format, options?.WriteFullCatalogSchema ?? true);
        await stream.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        stream.Position = 0;

        AccessWriter writer = await OpenAsync(stream, options, leaveOpen, cancellationToken).ConfigureAwait(false);
        await writer._complexColumns.ScaffoldSystemTablesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
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
        IReadOnlyList<ComplexColumnManager.ComplexColumnAllocation>? complexAllocs =
            await _complexColumns.PrepareComplexColumnAllocationsAsync(columns, cancellationToken).ConfigureAwait(false);
        if (complexAllocs is { Count: > 0 })
        {
            // Rewrite the column list with the allocated ComplexIds embedded so the parent
            // TDEF's misc slot points at the soon-to-be-emitted MSysComplexColumns rows.
            var rewritten = new List<ColumnDefinition>(columns);
            for (int i = 0; i < complexAllocs.Count; i++)
            {
                ComplexColumnManager.ComplexColumnAllocation a = complexAllocs[i];
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
            await _complexColumns.EmitComplexColumnArtifactsAsync(tableName, columns, complexAllocs, cancellationToken).ConfigureAwait(false);
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
    internal async ValueTask<long> CreateTableInternalAsync(
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<IndexDefinition> indexes,
        uint catalogFlags,
        CancellationToken cancellationToken)
    {
        TableDef tableDef = TDefPageBuilder.BuildTableDefinition(columns, _format);
        List<ResolvedIndex> resolvedIndexes = IndexHelpers.ResolveIndexes(indexes, tableDef);
        (byte[][] tdefPages, int[] firstDpLogicalOffsets, int[] usedPagesLogicalOffsets) = BuildTDefPagesWithIndexOffsets(tableDef, resolvedIndexes);

        // Append all TDEF pages first (sequential page numbers). The first
        // page's number is the table's catalog ID; subsequent pages are
        // chained via the next-page pointer at offset 4 of each non-last
        // page. Leaf pages and the usage-map page are appended AFTER, so
        // they don't interleave with the TDEF chain and the page numbers
        // stay contiguous (tdefPages[i] lives at file page tdefPageNumber + i).
        long tdefPageNumber = await AppendPageAsync(tdefPages[0], cancellationToken).ConfigureAwait(false);
        for (int p = 1; p < tdefPages.Length; p++)
        {
            _ = await AppendPageAsync(tdefPages[p], cancellationToken).ConfigureAwait(false);
        }

        // Stamp the next-page pointer at offset 4 of every non-last TDEF page.
        for (int p = 0; p < tdefPages.Length - 1; p++)
        {
            Wi32(tdefPages[p], 4, checked((int)(tdefPageNumber + p + 1)));
        }

        bool tdefDirty = tdefPages.Length > 1;

        // Emit one empty index leaf page per real index and patch its page
        // number into the corresponding `first_dp` field of the real-idx physical
        // descriptor. The leaf starts empty because CreateTableAsync inserts no
        // rows; subsequent inserts/updates/deletes maintain the B-tree via
        // MaintainIndexesAsync. See
        // docs/design/index-and-relationship-format-notes.md §7.
        if (resolvedIndexes.Count > 0)
        {
            var layout = IndexLeafPageBuilder.GetLayout(_format);
            var leafPageNumbers = new long[resolvedIndexes.Count];

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
                leafPageNumbers[i] = leafPageNumber;
                WriteLogicalTDefI32(tdefPages, firstDpLogicalOffsets[i], checked((int)leafPageNumber));
            }

            if (_format != DatabaseFormat.Jet3Mdb)
            {
                // DAO-authored TDEFs point each real index's used_pages field at a
                // dedicated data page row whose bitmap marks that index's first leaf.
                // Emit the same row+page structure here so CompactDatabase can walk
                // the index allocation chain instead of seeing an empty descriptor.
                long indexUsageMapPageNumber = await AppendIndexUsageMapPageAsync(leafPageNumbers, cancellationToken).ConfigureAwait(false);
                for (int i = 0; i < usedPagesLogicalOffsets.Length; i++)
                {
                    int usedPagesOffset = usedPagesLogicalOffsets[i];
                    tdefPages[usedPagesOffset / _pgSz][usedPagesOffset % _pgSz] = checked((byte)(i + 2));
                    WriteLogicalTDefUInt24(tdefPages, usedPagesOffset + 1, checked((int)indexUsageMapPageNumber));
                }
            }

            tdefDirty = true;
        }

        // Allocate a per-table usage-map data page and patch the TDEF
        // `used_pages` / `free_pages` pointers (Jet4/ACE only). DAO Compact
        // & Repair walks every catalog row and dereferences `used_pages` to
        // enumerate the table's data pages; a zero pointer here aborts the
        // walk with "could not find object 'MSysDb'". The companion
        // `autonum_flag` byte at TDEF offset 0x18 is also patched to 0x01
        // when any column carries the autonumber flag — Access checks this
        // before consulting the autonum-next counter at 0x14. See
        // docs/design/round-trip-test-failures.md.
        if (_format != DatabaseFormat.Jet3Mdb)
        {
            long usageMapPageNumber = await AppendUsageMapPageAsync(cancellationToken).ConfigureAwait(false);

            // PatchUsageMapPointers / PatchAutoNumFlag write only into the
            // TDEF header (offsets 0x18, 0x37..0x3F), which always live on
            // the first physical page.
            PatchUsageMapPointers(tdefPages[0], checked((int)usageMapPageNumber));
            PatchAutoNumFlag(tdefPages[0], tableDef);
            tdefDirty = true;
        }

        if (tdefDirty)
        {
            // Re-flush every TDEF page with the patched first_dp / usage-map /
            // autonum bytes and (for multi-page chains) the next-page pointers.
            for (int p = 0; p < tdefPages.Length; p++)
            {
                await WritePageAsync(tdefPageNumber + p, tdefPages[p], cancellationToken).ConfigureAwait(false);
            }
        }

        byte[]? lvProp = JetExpressionConverter.BuildLvPropBlob(columns, _format);
        await InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, catalogFlags, cancellationToken).ConfigureAwait(false);

        // DAO Compact & Repair requires every user table to have ACE
        // (Access Control Entry) rows in MSysACEs. Without them DAO's
        // security-descriptor pass aborts with err 3011 "MSysDb".
        if (catalogFlags == 0)
        {
            await InsertAceRowsForTableAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        }

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

                return [.. existing, column];
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
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await _relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        RelationshipManager.FkContext? fkCtx = rels.Count > 0 ? new RelationshipManager.FkContext(rels) : null;

        await InsertRowCoreAsync(tableName, entry.TDefPage, tableDef, values, fkCtx, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object[]> rows, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => InsertRowsCoreAsync(tableName, rows, cancellationToken), cancellationToken);

    private async ValueTask<int> InsertRowsCoreAsync(string tableName, IEnumerable<object[]> rows, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(rows, nameof(rows));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await _relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        RelationshipManager.FkContext? fkCtx = rels.Count > 0 ? new RelationshipManager.FkContext(rels) : null;

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
                    (batchAutoCheckpoints ??= []).AddRange(rowCp);
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
                    await _relationships.EnforceFkOnInsertAsync(tableName, tableDef, row, fkCtx, cancellationToken).ConfigureAwait(false);
                }

                RowLocation loc = await InsertRowDataLocAsync(entry.TDefPage, tableDef, row, cancellationToken: cancellationToken).ConfigureAwait(false);
                batchLocations.Add(loc);
                batchHintRows.Add((loc, row));
                if (fkCtx != null)
                {
                    RelationshipManager.AugmentParentSetsAfterInsert(tableName, tableDef, row, fkCtx);
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
        => RunAutoCommitAsync(_ => InsertRowGenericCoreAsync(tableName, item, cancellationToken), cancellationToken);

    private async ValueTask InsertRowGenericCoreAsync<T>(string tableName, T item, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(item, nameof(item));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        object[] mappedRow = RowMapper<T>.ToRow(tableDef, item);

        IReadOnlyList<RelationshipManager.FkRelationship> relsT = await _relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        RelationshipManager.FkContext? fkCtxT = relsT.Count > 0 ? new RelationshipManager.FkContext(relsT) : null;

        await InsertRowCoreAsync(tableName, entry.TDefPage, tableDef, mappedRow, fkCtxT, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new()
        => RunAutoCommitAsync(_ => InsertRowsGenericCoreAsync(tableName, items, cancellationToken), cancellationToken);

    private async ValueTask<int> InsertRowsGenericCoreAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(items, nameof(items));
        Guard.ThrowIfDisposed(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        CatalogEntry entry = await GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await _relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        RelationshipManager.FkContext? fkCtx = rels.Count > 0 ? new RelationshipManager.FkContext(rels) : null;

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
                object[] mappedRow = RowMapper<T>.ToRow(tableDef, item);
                List<(ColumnConstraint Constraint, long? PreviousValue)>? rowCp =
                    await ApplyConstraintsAsync(tableName, tableDef, mappedRow, cancellationToken).ConfigureAwait(false);
                if (rowCp != null)
                {
                    (batchAutoCheckpoints ??= []).AddRange(rowCp);
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
                    await _relationships.EnforceFkOnInsertAsync(tableName, tableDef, mappedRow, fkCtx, cancellationToken).ConfigureAwait(false);
                }

                RowLocation loc = await InsertRowDataLocAsync(entry.TDefPage, tableDef, mappedRow, cancellationToken: cancellationToken).ConfigureAwait(false);
                batchLocations.Add(loc);
                batchHintRows.Add((loc, mappedRow));
                if (fkCtx != null)
                {
                    RelationshipManager.AugmentParentSetsAfterInsert(tableName, tableDef, mappedRow, fkCtx);
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
        => RunAutoCommitAsync(_ => UpdateRowsCoreAsync(tableName, predicateColumn, predicateValue, updatedValues, cancellationToken), cancellationToken);

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
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await _relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        RelationshipManager.FkContext? fkCtx = rels.Count > 0 ? new RelationshipManager.FkContext(rels) : null;

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
                await _relationships.EnforceFkOnInsertAsync(tableName, tableDef, newRow, fkCtx, cancellationToken).ConfigureAwait(false);
            }

            // PK-side: if any of the updated columns belongs to a PK referenced
            // by a child table, gather (oldKey, newPkValues) pairs per affected
            // row and let EnforceFkOnPrimaryUpdateAsync cascade or reject.
            var changes = new List<(string? OldKey, object?[] OldFullRow, object[] NewPkValues)>(pendingNewRows.Count);
            foreach (RelationshipManager.FkRelationship rel in rels)
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

                await _relationships.EnforceFkOnPrimaryUpdateAsync(tableName, tableDef, changes, fkCtx, depth: 0, cancellationToken).ConfigureAwait(false);
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
        => RunAutoCommitAsync(_ => DeleteRowsCoreAsync(tableName, predicateColumn, predicateValue, cancellationToken), cancellationToken);

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

        IReadOnlyList<RelationshipManager.FkRelationship> rels = await _relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        if (rels.Count > 0 && matchingIndices.Count > 0)
        {
            var fkCtx = new RelationshipManager.FkContext(rels);

            // Snapshot the typed full row of every parent we are about to
            // delete, in primary-table column order. EnforceFkOnPrimaryDeleteAsync
            // consumes this once per relationship (slicing the relationship's
            // PrimaryColumns out for the FK seek / snapshot scan).
            var deletedParentRows = new List<object?[]>(matchingIndices.Count);
            foreach (int rowIdx in matchingIndices)
            {
                deletedParentRows.Add(snapshot.Rows[rowIdx].ItemArray);
            }

            await _relationships.EnforceFkOnPrimaryDeleteAsync(
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

            await _complexColumns.CascadeDeleteComplexChildrenAsync(tableDef, parentLocs, cancellationToken).ConfigureAwait(false);
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
        msys.SetValueByName(values, "ParentId", Constants.SystemObjects.TablesParentId);
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
        msys.SetValueByName(values, "ParentId", Constants.SystemObjects.TablesParentId);
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
    // Foreign-key relationships — thin forwarders to RelationshipManager
    // ════════════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => _relationships.CreateRelationshipAsync(relationship, cancellationToken);

    /// <inheritdoc/>
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
        => _relationships.DropRelationshipAsync(relationshipName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
        => _relationships.RenameRelationshipAsync(oldName, newName, cancellationToken);

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
    public static ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        string path,
        CancellationToken cancellationToken = default)
        => EncryptionManager.DetectEncryptionFormatAsync(path, cancellationToken);

    /// <summary>
    /// Detects the on-disk encryption format of the database in <paramref name="stream"/>
    /// without modifying it. The stream must be seekable.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
        => EncryptionManager.DetectEncryptionFormatAsync(stream, cancellationToken);

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
        => EncryptionManager.ChangePasswordAsync(path, oldPassword, newPassword, options, cancellationToken);

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
        => EncryptionManager.EncryptAsync(path, newPassword, targetFormat, options, cancellationToken);

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
        => EncryptionManager.DecryptAsync(path, oldPassword, options, cancellationToken);

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
        => EncryptionManager.ChangePasswordAsync(stream, oldPassword, newPassword, cancellationToken);

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
        => EncryptionManager.EncryptAsync(stream, newPassword, targetFormat, cancellationToken);

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
        => EncryptionManager.DecryptAsync(stream, oldPassword, cancellationToken);

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
            var journal = new PageJournal(baseLength, PageSize, _options.MaxTransactionPageBudget);
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
    internal async ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        if (!_options.UseTransactionalWrites || _activeTransaction is not null || _disposed)
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
    /// Generic-result variant of <see cref="RunAutoCommitAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/>.
    /// </summary>
    internal async ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
    {
        if (!_options.UseTransactionalWrites || _activeTransaction is not null || _disposed)
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
        IDisposable commitLock = await _byteRangeLock.AcquireCommitLockAsync(isAccdb: DatabaseFormat == DatabaseFormat.AceAccdb, cancellationToken).ConfigureAwait(false);

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
    [SuppressMessage("Usage", "CA2215:Dispose methods should call base class dispose", Justification = "base.DisposeAsync is passed as the final step to LockFileCoordinator.DisposeAfterAsync.")]
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        // The coordinator drains every step in order, captures the first
        // failure, and unconditionally releases the .ldb / .laccdb slot last.
        // Lock-file release runs after the agile re-wrap so the lock-file
        // accurately reflects "database still in use" while we re-encrypt.
        await _lockFileCoordinator.DisposeAfterAsync(
            DisposeActiveTransactionAsync,
            RewrapAndCloseOuterEncryptedStreamAsync,
            DisposeStateLockAsync,
            () => base.DisposeAsync()).ConfigureAwait(false);
    }

    private async ValueTask DisposeActiveTransactionAsync()
    {
        // Drop any in-flight transaction so its journal does not survive
        // dispose. Nothing has been written to disk for an uncommitted
        // transaction, so this is equivalent to an implicit rollback.
        if (_activeTransaction is null)
        {
            return;
        }

        try
        {
            await _activeTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            ActiveJournal = null;
            _activeTransaction = null;
        }
    }

    private async ValueTask RewrapAndCloseOuterEncryptedStreamAsync()
    {
        // For Agile-encrypted databases the underlying _stream is an in-memory
        // copy of the *decrypted* ACCDB. Re-encrypt it before tearing down so
        // the user's outer encrypted stream/file ends up with all writes.
        if (!_isAgileEncryptedRewrap || _outerEncryptedStream is null || _options.Password.IsEmpty)
        {
            return;
        }

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

    private ValueTask DisposeStateLockAsync()
    {
        _stateLock.Dispose();
        return default;
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
            OfficeCryptoAgile.Encrypt(inner, _options.Password.Span);

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
    internal async ValueTask ApplyConstraintsForComplexAsync(string tableName, TableDef tableDef, object[] values, CancellationToken cancellationToken)
        => _ = await ApplyConstraintsAsync(tableName, tableDef, values, cancellationToken).ConfigureAwait(false);

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
        RelationshipManager.FkContext? fkCtx,
        CancellationToken cancellationToken)
    {
        List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints =
            await ApplyConstraintsAsync(tableName, tableDef, values, cancellationToken).ConfigureAwait(false);

        if (fkCtx != null)
        {
            try
            {
                await _relationships.EnforceFkOnInsertAsync(tableName, tableDef, values, fkCtx, cancellationToken).ConfigureAwait(false);
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
                RelationshipManager.AugmentParentSetsAfterInsert(tableName, tableDef, values, fkCtx);
            }

            // fast path: try in-place leaf splice for the inserted
            // row before falling back to a full snapshot+rebuild.
            List<(RowLocation Loc, object[] Row)> hintInserts = [(loc, values)];
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
    private async ValueTask RollbackInsertedRowsAsync(long tdefPage, List<RowLocation> locations, CancellationToken cancellationToken)
    {
        if (locations.Count == 0)
        {
            return;
        }

        foreach (RowLocation loc in locations)
        {
            await MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, cancellationToken).ConfigureAwait(false);
        }

        await AdjustTDefRowCountAsync(tdefPage, -locations.Count, cancellationToken).ConfigureAwait(false);
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

    internal static byte TypeCodeFromDefinition(ColumnDefinition column)
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

    internal static bool IsVariableType(byte type)
    {
        return type == T_TEXT || type == T_BINARY || type == T_MEMO || type == T_OLE;
    }

    /// <summary>
    /// Validates and returns the precision (1..28) declared on a
    /// <c>T_NUMERIC</c> column definition. Defaults to <c>18</c> when the
    /// caller leaves <see cref="ColumnDefinition.NumericPrecision"/> at its
    /// initial value (matches Access "Number → Decimal" UI default).
    /// </summary>
    internal static byte ResolveNumericPrecision(ColumnDefinition definition)
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
    internal static byte ResolveNumericScale(ColumnDefinition definition)
    {
        byte s = definition.NumericScale;
        byte p = definition.NumericPrecision == 0 ? (byte)18 : definition.NumericPrecision;
        Guard.InRange(s, 0, 28, $"Column '{definition.Name}' NumericScale");
        Guard.InRange(s, 0, p, $"Column '{definition.Name}' NumericScale (NumericPrecision={p})");
        return s;
    }

    internal static TableDef BuildTableDefinition(IReadOnlyList<ColumnDefinition> columns, DatabaseFormat format)
        => TDefPageBuilder.BuildTableDefinition(columns, format);

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
        if (data.Length > Constants.LongValue.MaxPayloadBytes)
        {
            throw new JetLimitationException(
                $"Long value is {data.Length} bytes, which exceeds the JET 24-bit LVAL length limit of {Constants.LongValue.MaxPayloadBytes} bytes.");
        }

        // One row per LVAL page. The row table costs 2 bytes for a single offset.
        int singleRowMax = _pgSz - _dataPage.RowsStart - 2;
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
        Wi32(page, _dataPage.TDefOff, 0);
        Wu16(page, _dataPage.NumRows, 1);

        int rowStart = _pgSz - payload.Length;
        Buffer.BlockCopy(payload, 0, page, rowStart, payload.Length);
        Wu16(page, _dataPage.RowsStart, rowStart);

        int freeSpace = rowStart - (_dataPage.RowsStart + 2);
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
        Wi32(page, _dataPage.TDefOff, 0);
        Wu16(page, _dataPage.NumRows, 1);

        int rowLen = 4 + length;
        int rowStart = _pgSz - rowLen;
        Wi32(page, rowStart, (int)nextDp);
        Buffer.BlockCopy(data, offset, page, rowStart + 4, length);
        Wu16(page, _dataPage.RowsStart, rowStart);

        int freeSpace = rowStart - (_dataPage.RowsStart + 2);
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

    internal async ValueTask<DataTable> ReadTableSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var options = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            Password = _options.Password,
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
            Password = _options.Password,
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
            Password = _options.Password,
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

    internal async ValueTask<CatalogEntry> GetRequiredCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
    {
        CatalogEntry? entry = await GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (entry == null)
        {
            throw new InvalidOperationException($"Table '{tableName}' was not found.");
        }

        return entry;
    }

    internal async ValueTask<TableDef> ReadRequiredTableDefAsync(long tdefPage, string tableName, CancellationToken cancellationToken = default)
    {
        TableDef? tableDef = await ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (tableDef == null)
        {
            throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");
        }

        return tableDef;
    }

    internal ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, CancellationToken cancellationToken = default)
        => InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, catalogFlags: 0, cancellationToken);

    internal async ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, uint catalogFlags, CancellationToken cancellationToken = default)
    {
        TableDef msys = await ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        object[] values = msys.CreateNullValueRow();
        DateTime now = DateTime.UtcNow;

        msys.SetValueByName(values, "Id", (int)tdefPageNumber);
        msys.SetValueByName(values, "ParentId", Constants.SystemObjects.TablesParentId);
        msys.SetValueByName(values, "Name", tableName);
        msys.SetValueByName(values, "Type", (short)Constants.SystemObjects.UserTableType);
        msys.SetValueByName(values, "DateCreate", now);
        msys.SetValueByName(values, "DateUpdate", now);
        msys.SetValueByName(values, "Flags", unchecked((int)catalogFlags));

        // Microsoft Access stamps MSysObjects.Owner with a per-file 2-byte
        // token that varies across databases (verified across ~80 Jet3 / Jet4 /
        // ACE fixtures: e.g. NorthwindTraders.accdb uses 0x71 0x10, nwind.mdb
        // 0x03 0x01, ComplexFields.accdb 0xC8 0xB1, AdventureLT2008.mdb 0xEC
        // 0xC7). The exact bytes appear to be derived from file/owner identity
        // and are opaque to DAO Compact & Repair, which only requires the
        // column to be non-null on user-authored Type=1 / Type=8 catalog rows
        // (Access itself leaves the 10 hidden system-managed Type=1 rows with
        // Owner=NULL on every modern .accdb). We stamp a fixed 2-byte sentinel
        // -- DefaultOwnerBlob (0x71 0x10, the value Access uses in
        // NorthwindTraders.accdb) -- which satisfies the non-null check on
        // every JET / ACE format we support, with no version gate required.
        msys.SetValueByName(values, "Owner", Constants.SystemObjects.DefaultOwnerBlob);

        // LvProp is the OLE/LongBinary cell carrying per-column persisted properties
        // (DefaultValue, ValidationRule, ValidationText, Description). Only emitted on
        // the full 17-column catalog schema (the slim 9-column legacy schema lacks the
        // column entirely, so SetValue is a no-op). DAO Compact & Repair requires
        // LvProp to be NOT NULL on every user-authored Type=1 catalog row -- when no
        // per-column properties are declared the writer falls back to a 12-byte
        // placeholder (DefaultLvPropPlaceholder) so the null-mask bit is set and the
        // row's var-offset table mirrors what DAO emits. See
        // docs/design/round-trip-test-failures.md.
        msys.SetValueByName(values, "LvProp", lvProp ?? Constants.SystemObjects.DefaultLvPropPlaceholder);

        // Insert the new MSysObjects row, then splice its index entry into
        // the rightmost leaf of every real-idx slot WITHOUT re-encoding any
        // pre-existing entries. This keeps Microsoft Access's PK Id index
        // pointing at the new TDEF row so DAO Compact &amp; Repair can locate
        // it, while preserving the byte-for-byte content of Access-authored
        // catalog rows the writer cannot losslessly re-encode (e.g. the
        // special "Databases" properties row's LvProp blob). See
        // docs/design/catalog-index-maintenance-notes.md.
        RowLocation loc = await InsertRowDataLocAsync(2, msys, values, updateTDefRowCount: true, cancellationToken).ConfigureAwait(false);
        _ = await TrySpliceCatalogIndexEntryAsync(2, msys, loc, values, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts 3 ACE (Access Control Entry) rows into <c>MSysACEs</c> for a
    /// newly-created user table. DAO Compact &amp; Repair's security-descriptor
    /// pass requires every user table to have owner / admins / users entries;
    /// without them DAO aborts with err 3011 "could not find 'MSysDb'".
    /// </summary>
    private async ValueTask InsertAceRowsForTableAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        long acesTdefPage = await _relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        if (acesTdefPage <= 0)
        {
            return;
        }

        TableDef acesDef = await ReadRequiredTableDefAsync(acesTdefPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);

        // Harvest the Admins group SID from an existing ACE row (the long
        // ~102-byte blob that is the same on every row in the database).
        byte[]? adminsSid = await HarvestAdminsSidAsync(acesTdefPage, acesDef, cancellationToken).ConfigureAwait(false);

        byte[][] sids = adminsSid != null
            ? [Constants.Aces.OwnerSid, adminsSid, Constants.Aces.UsersSid]
            : [Constants.Aces.OwnerSid, Constants.Aces.UsersSid];

        foreach (byte[] sid in sids)
        {
            object[] row = acesDef.CreateNullValueRow();
            acesDef.SetValueByName(row, "ObjectId", (int)tdefPageNumber);
            acesDef.SetValueByName(row, "ACM", Constants.Aces.DefaultAcm);
            acesDef.SetValueByName(row, "FInheritable", true);
            acesDef.SetValueByName(row, "SID", sid);
            await InsertSystemRowAndMaintainAsync(acesTdefPage, acesDef, Constants.SystemTableNames.Aces, row, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads an existing ACE row from <c>MSysACEs</c> and extracts the
    /// Admins-group SID (the long ~102-byte blob). All ACE rows in a database
    /// share the same Admins SID; we harvest it from any row whose SID is
    /// longer than the 2-byte owner/users sentinels.
    /// </summary>
    private async ValueTask<byte[]?> HarvestAdminsSidAsync(long acesTdefPage, TableDef acesDef, CancellationToken cancellationToken)
    {
        ColumnInfo? sidCol = acesDef.FindColumn("SID");
        if (sidCol == null)
        {
            return null;
        }

        long total = _stream.Length / _pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01 || Ri32(page, _dataPage.TDefOff) != acesTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
                {
                    string hex = DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, sidCol);

                    // Longer than 4 hex chars means > 2 bytes (not owner/users short SIDs).
                    if (hex.Length > 4)
                    {
                        return ParseHexBytes(hex);
                    }
                }
            }
            finally
            {
                ReturnPage(page);
            }
        }

        return null;
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
        Dictionary<int, ColumnDefinition> newComplexById = [];
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
            await _complexColumns.DropSingleComplexChildAsync(colName, complexId, cancellationToken).ConfigureAwait(false);
        }

        foreach ((string oldColName, string newColName, int complexId) in renamedComplex)
        {
            await _complexColumns.RenameComplexColumnArtifactsAsync(oldColName, newColName, complexId, cancellationToken).ConfigureAwait(false);
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
            case T_TEXT:
                int charLen = _format != DatabaseFormat.Jet3Mdb ? Math.Max(1, column.Size / 2) : Math.Max(1, column.Size);
                baseDef = new ColumnDefinition(column.Name, typeof(string), charLen);
                break;
            case T_BINARY:
                baseDef = new ColumnDefinition(column.Name, typeof(byte[]), column.Size > 0 ? column.Size : 255);
                break;
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
                Type? clrType = JetTypeInfo.GetClrType(column.Type)
                    ?? throw new NotSupportedException($"Column '{column.Name}' has unsupported type code 0x{column.Type:X2}.");
                baseDef = new ColumnDefinition(column.Name, clrType);
                break;
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
        => _tdefPageBuilder.BuildTDefPage(tableDef);

    private byte[] BuildTDefPage(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
        => _tdefPageBuilder.BuildTDefPage(tableDef, indexes);

    /// <summary>
    /// Single-page convenience wrapper for callers (system-table emission,
    /// complex-type templates) that emit small fixed schemas which always
    /// fit in one TDEF page. Throws if the table definition would actually
    /// require a multi-page chain — those callers don't use this method.
    /// User-facing <see cref="CreateTableInternalAsync"/> goes through
    /// <see cref="BuildTDefPagesWithIndexOffsets"/> instead, which fully
    /// supports the multi-page TDEF chain that the reader's
    /// <c>ReadTDefBytesAsync</c> already stitches.
    /// </summary>
    internal (byte[] Page, int[] FirstDpOffsets) BuildTDefPageWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
        => _tdefPageBuilder.BuildTDefPageWithIndexOffsets(tableDef, indexes);

    /// <summary>
    /// Builds a (possibly multi-page) TDEF chain and also returns, for each
    /// logical index in <paramref name="indexes"/>, the LOGICAL byte offset
    /// of that real-index physical descriptor's <c>first_dp</c> field (§3.1).
    /// Logical offsets address the stitched buffer the reader produces in
    /// <see cref="AccessBase.ReadTDefBytesAsync"/>: the first physical page
    /// (full <c>_pgSz</c> bytes) followed by every continuation page's body
    /// from offset 8 onward. Use <see cref="LogicalToPhysicalTDefOffset"/>
    /// to translate to a (page-index, page-offset) pair before patching.
    /// <para>
    /// Continuation pages each carry an 8-byte page-chain header
    /// (<c>0x02 0x01 00 00</c> + 4-byte next-page pointer) which is NOT part
    /// of the logical TDEF body. The caller is responsible for writing the
    /// next-page pointer at offset 4 of each non-last page after appending,
    /// since page numbers are not known until then.
    /// </para>
    /// </summary>
    internal (byte[][] Pages, int[] FirstDpLogicalOffsets, int[] UsedPagesLogicalOffsets) BuildTDefPagesWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
        => _tdefPageBuilder.BuildTDefPagesWithIndexOffsets(tableDef, indexes);

    /// <summary>
    /// Splits the linear logical TDEF buffer produced by
    /// <see cref="BuildTDefPagesWithIndexOffsets"/> into one or more physical
    /// pages. The first page is taken verbatim from <c>logical[0.._pgSz]</c>
    /// (its 8-byte page-chain header is already populated by the build path).
    /// Continuation pages get a fresh 8-byte header (<c>0x02 0x01 00 00</c> +
    /// 4-byte next-page placeholder) followed by a slice of the logical
    /// body. The next-page pointer at offset 4 of each non-last page is left
    /// at 0 — the caller stamps the real page number after appending.
    /// </summary>
    private (int PageIndex, int PageOffset) LogicalToPhysicalTDefOffset(int logicalOffset)
        => _tdefPageBuilder.LogicalToPhysicalTDefOffset(logicalOffset);

    /// <summary>
    /// Writes a 4-byte little-endian integer at the given LOGICAL TDEF
    /// offset, dispatching the bytes across the physical page boundary
    /// when the field straddles two pages. Used to patch <c>first_dp</c>
    /// values after their leaf pages are appended.
    /// </summary>
    private void WriteLogicalTDefI32(byte[][] pages, int logicalOffset, int value)
    {
        for (int i = 0; i < 4; i++)
        {
            (int pageIdx, int pageOff) = LogicalToPhysicalTDefOffset(logicalOffset + i);
            pages[pageIdx][pageOff] = (byte)((value >> (i * 8)) & 0xFF);
        }
    }

    private void WriteLogicalTDefUInt24(byte[][] pages, int logicalOffset, int value)
    {
        for (int i = 0; i < 3; i++)
        {
            (int pageIdx, int pageOff) = LogicalToPhysicalTDefOffset(logicalOffset + i);
            pages[pageIdx][pageOff] = (byte)((value >> (i * 8)) & 0xFF);
        }
    }

    // Matches the DAO-observed shape for a real-index usage-map page: two leading
    // zero rows, then one 69-byte usage-map row per real index. Each usage-map row
    // stores a page-aligned base in bytes 1..3 and a bitmap starting at byte 4.
    private async ValueTask<long> AppendIndexUsageMapPageAsync(long[] leafPageNumbers, CancellationToken cancellationToken)
    {
        byte[] page = new byte[_pgSz];
        page[0] = 0x01;
        page[1] = 0x01;

        const int rowSize = 69;
        int rowCount = leafPageNumbers.Length + 2;
        int rowStart = _pgSz;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            rowStart -= rowSize;
            Wu16(page, _dataPage.RowsStart + (rowIndex * 2), rowStart);

            if (rowIndex < 2)
            {
                continue;
            }

            long leafPageNumber = leafPageNumbers[rowIndex - 2];
            int basePageNumber = checked((int)((leafPageNumber / 8) * 8));
            int bitIndex = checked((int)(leafPageNumber - basePageNumber));

            page[rowStart] = 0x00;
            WriteUInt24(page, rowStart + 1, basePageNumber);
            page[rowStart + 4 + (bitIndex / 8)] |= (byte)(1 << (bitIndex % 8));
        }

        Wi32(page, _dataPage.TDefOff, 0);
        Wu16(page, _dataPage.NumRows, rowCount);
        int freeSpace = rowStart - (_dataPage.RowsStart + (rowCount * 2));
        Wu16(page, 2, freeSpace);

        return await AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
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
            _ => _complexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, attachment, expectAttachment: true, cancellationToken),
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
            _ => _complexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, value, expectAttachment: false, cancellationToken),
            cancellationToken);
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
                await _complexColumns.DropComplexChildrenForTableAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
            }
        }

        UnregisterConstraints(tableName);
        InvalidateCatalogCache();
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
        => TDefPageBuilder.BuildEmptyDatabase(format, fullCatalogSchema);

    internal async ValueTask InsertRowDataAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default)
    {
        _ = await InsertRowDataLocAsync(tdefPage, tableDef, values, updateTDefRowCount, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts one row into a system table (MSysObjects, MSysRelationships,
    /// MSysComplexColumns, …) and refreshes that table's indexes so external
    /// readers (Microsoft Access / DAO Compact &amp; Repair) can locate the
    /// new row through the catalog indexes. Bare <see cref="InsertRowDataAsync"/>
    /// only writes the data row; index leaves are not maintained, so DAO
    /// walking via <c>ParentIdName</c> / <c>Id</c> never sees the row and the
    /// catalog appears empty from outside.
    /// </summary>
    /// <remarks>
    /// User-row inserts are batched by <see cref="InsertRowsAsync(string, IEnumerable{object[]}, CancellationToken)"/>
    /// for performance; system-table inserts are infrequent and can afford to
    /// pay the per-call index-maintenance cost.
    /// </remarks>
    internal async ValueTask InsertSystemRowAndMaintainAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        object[] values,
        bool updateTDefRowCount = true,
        CancellationToken cancellationToken = default)
    {
        RowLocation loc = await InsertRowDataLocAsync(tdefPage, tableDef, values, updateTDefRowCount, cancellationToken).ConfigureAwait(false);

        // Skip maintenance when the system-table TDEF has no allocated index
        // leaves yet (writer-created blank databases bootstrap MSysObjects
        // with degenerate / placeholder real-idx slots whose first_dp values
        // are not valid page numbers; calling MaintainIndexesAsync against
        // those would attempt to read non-existent pages). Access-authored
        // databases (Northwind, etc.) always have valid first_dp values, so
        // the maintenance path runs as expected and external readers / DAO
        // Compact &amp; Repair see the new system-table rows through the
        // catalog indexes.
        if (!await SystemTableHasMaintainableIndexesAsync(tdefPage, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        var hint = new List<(RowLocation Loc, object[] Row)>(1) { (loc, values) };
        try
        {
            bool incremental = await TryMaintainIndexesIncrementalAsync(
                tdefPage,
                tableDef,
                hint,
                deletedRows: null,
                cancellationToken).ConfigureAwait(false);
            if (!incremental)
            {
                await MaintainIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            // Defensive: writer-bootstrapped system tables may carry index
            // descriptors that point at unallocated / out-of-range pages
            // (the writer's blank-database template is intentionally minimal
            // and never exercises catalog-index reads on its own). Swallow
            // the page-read failure rather than corrupting an otherwise
            // healthy mutation — readers that walk data pages directly
            // (this library's AccessReader) still see the new row.
        }
        catch (InvalidOperationException)
        {
            // Same rationale: malformed real-idx / leaf chain on a
            // bootstrapped catalog table is not a reason to fail an insert.
        }
    }

    /// <summary>
    /// Returns <c>true</c> when every real-idx slot on <paramref name="tdefPage"/>
    /// references a valid in-range data page through its <c>first_dp</c>
    /// pointer. Used by <see cref="InsertSystemRowAndMaintainAsync"/> to
    /// avoid index maintenance on writer-bootstrapped system tables whose
    /// real-idx descriptors point at unallocated pages.
    /// </summary>
    private async ValueTask<bool> SystemTableHasMaintainableIndexesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        byte[] page = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != 0x02 || Ru32(page, 4) != 0)
            {
                return false;
            }

            int numCols = Ru16(page, _tdef.NumCols);
            int numRealIdx = Ri32(page, _tdef.NumRealIdx);
            if (numCols < 0 || numCols > 4096 || numRealIdx <= 0 || numRealIdx > 1000)
            {
                return false;
            }

            int realIdxDescStart = _relationships.LocateRealIdxDescStart(page, numCols, numRealIdx);
            if (realIdxDescStart < 0)
            {
                return false;
            }

            long totalPages = _stream.Length / _pgSz;
            for (int ri = 0; ri < numRealIdx; ri++)
            {
                if (!_indexLayout.TryReadRealIdxSlot(page, realIdxDescStart, ri, out IndexLayout.RealIdxSlot slot))
                {
                    return false;
                }

                long firstDp = (uint)Ri32(page, slot.FirstDpOffset);
                if (firstDp <= 0 || firstDp >= totalPages)
                {
                    return false;
                }
            }

            return true;
        }
        finally
        {
            ReturnPage(page);
        }
    }

    /// <summary>
    /// Inserts a row and returns its (page, row-index) location so the caller
    /// can mark it deleted if a subsequent step (e.g. unique-index rebuild)
    /// fails. Mirrors <see cref="InsertRowDataAsync"/> but exposes the
    /// <see cref="RowLocation"/> of the freshly written row.
    /// </summary>
    internal async ValueTask<RowLocation> InsertRowDataLocAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default)
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
            rowIndex = Ru16(target.Page, _dataPage.NumRows);
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

    internal async ValueTask AdjustTDefRowCountAsync(long tdefPage, long delta, CancellationToken cancellationToken)
    {
        if (delta == 0)
        {
            return;
        }

        byte[] page = await ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long updated;

        try
        {
            uint current = Ru32(page, Constants.TableDefinition.RowCountOffset);
            updated = Math.Clamp(current + delta, 0L, uint.MaxValue);
            Wi32(page, Constants.TableDefinition.RowCountOffset, unchecked((int)(uint)updated));

            // Mirror the change into the per-real-idx `num_idx_rows` counter
            // (offset +4 of each 12-byte/8-byte slot in the leading real-idx
            // skip block at [_tdef.BlockEnd, _tdef.BlockEnd + numRealIdx *
            // _tdef.RealIdxEntrySz)). Per mdbtools HACKING.md the slot is laid
            // out as `unknown(4) + num_idx_rows(4) + unknown(4)`. DAO compares
            // num_idx_rows against the leaf-level row count when walking
            // MSysObjects; if they disagree it aborts compact with
            // "could not find the object 'MSysDb'" — see
            // docs/design/round-trip-test-failures.md.
            int numRealIdx = Ri32(page, _tdef.NumRealIdx);
            if (numRealIdx > 0 && numRealIdx <= 1000)
            {
                int slotEnd = _tdef.BlockEnd + (numRealIdx * _tdef.RealIdxEntrySz);
                if (slotEnd <= page.Length)
                {
                    for (int i = 0; i < numRealIdx; i++)
                    {
                        int countOff = _tdef.BlockEnd + (i * _tdef.RealIdxEntrySz) + 4;
                        uint cur = Ru32(page, countOff);
                        long next = Math.Clamp(cur + delta, 0L, uint.MaxValue);
                        Wi32(page, countOff, unchecked((int)(uint)next));
                    }
                }
            }

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
            if (cached[0] == 0x01 && Ri32(cached, _dataPage.TDefOff) == tdefPage && CanInsertRow(cached, rowLength))
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

            if (Ri32(page, _dataPage.TDefOff) != tdefPage)
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
        int numRows = Ru16(page, _dataPage.NumRows);

        // JET row IDs encode the per-page row index as a single byte (0..255),
        // so a data page can hold at most 256 distinct row slots. Capping at
        // 255 matches Jackcess and keeps the (byte) cast in the index-rebuild
        // path safe under <CheckForOverflowUnderflow>true.
        if (numRows >= Constants.DataPage.MaxRowsPerPage)
        {
            return false;
        }

        int dataStart = GetFirstRowStart(page, numRows);
        int nextOffsetPos = _dataPage.RowsStart + ((numRows + 1) * 2);
        return dataStart - nextOffsetPos >= rowLength;
    }

    private int GetFirstRowStart(byte[] page, int numRows)
    {
        int first = _pgSz;
        for (int i = 0; i < numRows; i++)
        {
            int raw = Ru16(page, _dataPage.RowsStart + (i * 2));
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
        Wu16(page, 2, _pgSz - _dataPage.RowsStart);
        Wi32(page, _dataPage.TDefOff, (int)tdefPage);
        Wu16(page, _dataPage.NumRows, 0);
        return page;
    }

    /// <summary>
    /// Allocates and appends a per-table usage-map data page (page_type 0x01)
    /// containing two empty inline-bitmap rows. Row 0 backs the table's
    /// <c>used_pages</c> map; row 1 backs the <c>free_pages</c> map. Both
    /// rows consist of a single <c>0x00</c> byte (Access "inline" usage-map
    /// marker followed by a zero-length bitmap), so the table is reported as
    /// owning no data pages until subsequent inserts populate the map. The
    /// data-page back-pointer at offset <c>_dataPage.TDefOff</c> is left at 0
    /// to match the layout of Access-authored usage-map data pages
    /// (e.g. page 6 in <c>NorthwindTraders.accdb</c>).
    /// </summary>
    /// <remarks>
    /// Used by <see cref="CreateTableInternalAsync"/> to satisfy DAO
    /// Compact &amp; Repair, which dereferences each catalog row's
    /// <c>used_pages</c> pointer and aborts the catalog walk when it is
    /// zero. See docs/design/round-trip-test-failures.md.
    /// </remarks>
    private async ValueTask<long> AppendUsageMapPageAsync(CancellationToken cancellationToken)
    {
        byte[] page = new byte[_pgSz];
        page[0] = 0x01;
        page[1] = 0x01;

        // Each row is 69 bytes: 1-byte type-0 marker (0x00) + 68 bytes of bitmap.
        // 68 bytes = 544 bits covers pages 0..543, the minimum bitmap width
        // Access reserves on Access-authored usage-map pages (verified against
        // NorthwindTraders.accdb page 24, the usage-map data page for the
        // single-column TDEF on page 23). DAO refuses to walk a 1-byte row.
        // For an empty table the bitmap is all zeros (no pages owned / free).
        // Row 0 = used_pages, row 1 = free_pages, both packed at the page tail.
        const int rowSize = 69;
        int row0Off = _pgSz - rowSize;
        int row1Off = row0Off - rowSize;

        // Bitmap bytes are zero by virtue of `new byte[]`; no explicit writes needed.
        Wi32(page, _dataPage.TDefOff, 0);
        Wu16(page, _dataPage.NumRows, 2);
        Wu16(page, _dataPage.RowsStart, row0Off);
        Wu16(page, _dataPage.RowsStart + 2, row1Off);

        int freeSpace = row1Off - (_dataPage.RowsStart + 4);
        Wu16(page, 2, freeSpace);

        return await AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Patches the new TDEF's <c>used_pages</c> (offset 0x37..0x3A) and
    /// <c>free_pages</c> (offset 0x3B..0x3E) pointers to reference the
    /// freshly-allocated usage-map data page built by
    /// <see cref="AppendUsageMapPageAsync"/>. Each pointer is encoded as
    /// 1-byte row index followed by 3-byte LE page number — used_pages
    /// references row 0, free_pages references row 1 (matches the row
    /// ordering in Access-authored usage-map data pages, e.g. MSysObjects'
    /// page 6 in <c>NorthwindTraders.accdb</c>). Jet4/ACE only; Jet3
    /// stores per-table page-usage pointers in a different location
    /// (see docs/design/index-and-relationship-format-notes.md §3.1).
    /// </summary>
    private static void PatchUsageMapPointers(byte[] tdefPage, int usageMapPageNumber)
    {
        // used_pages: row 0 (1 byte) + page (3 bytes LE)
        tdefPage[0x37] = 0x00;
        WriteUInt24(tdefPage, 0x38, usageMapPageNumber);

        // free_pages: row 1 (1 byte) + page (3 bytes LE)
        tdefPage[0x3B] = 0x01;
        WriteUInt24(tdefPage, 0x3C, usageMapPageNumber);
    }

    /// <summary>
    /// Sets the TDEF's <c>autonum_flag</c> byte (offset 0x18) to <c>0x01</c>
    /// when any column in <paramref name="tableDef"/> carries the autonumber
    /// flag bit (<c>0x04</c>). Access checks this byte before consulting the
    /// autonum-next counter at offset 0x14, and DAO Compact &amp; Repair
    /// rejects a TDEF whose autonumber column count disagrees with this
    /// flag. Jet4/ACE only.
    /// </summary>
    private static void PatchAutoNumFlag(byte[] tdefPage, TableDef tableDef)
    {
        bool hasAutoNumber = false;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            if ((tableDef.Columns[i].Flags & 0x04) != 0)
            {
                hasAutoNumber = true;
                break;
            }
        }

        tdefPage[0x18] = hasAutoNumber ? (byte)0x01 : (byte)0x00;
    }

    private void WriteRowToPage(long pageNumber, byte[] page, byte[] rowBytes)
    {
        int numRows = Ru16(page, _dataPage.NumRows);
        int firstRowStart = GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = _dataPage.RowsStart + (numRows * 2);

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, _dataPage.NumRows, numRows + 1);

        int freeSpace = rowStart - (_dataPage.RowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Wu16(page, 2, freeSpace);
        WritePage(pageNumber, page);
    }

    private async ValueTask WriteRowToPageAsync(long pageNumber, byte[] page, byte[] rowBytes, CancellationToken cancellationToken)
    {
        int numRows = Ru16(page, _dataPage.NumRows);
        int firstRowStart = GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = _dataPage.RowsStart + (numRows * 2);

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, _dataPage.NumRows, numRows + 1);

        int freeSpace = rowStart - (_dataPage.RowsStart + ((numRows + 1) * 2));
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
                maxFixedEnd = Math.Max(maxFixedEnd, col.FixedOff + JetTypeInfo.GetFixedSize(col.Type));
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
                // Complex columns (T_ATTACHMENT / T_COMPLEX) store a 4-byte
                // ConceptualTableID in the fixed area. The ID is initially
                // null and patched in-place later by PatchParentComplexSlotAsync
                // when the first attachment or multi-value item is added.
                // The fixed area must always reserve space for these slots so
                // the in-place patch doesn't land outside the row bounds.
                if (column.IsFixed && (column.Type == T_ATTACHMENT || column.Type == T_COMPLEX))
                {
                    fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + JetTypeInfo.GetFixedSize(column.Type));
                }

                continue;
            }

            if (column.IsFixed)
            {
                if (!CanStoreFixedColumn(column))
                {
                    continue;
                }

                int fixedSize = JetTypeInfo.GetFixedSize(column.Type);
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
                varPayloadSize += variableValue.Length;
                SetNullMaskBit(nullMask, column.ColNum, true);
            }
        }

        // Emit the full variable-column slot count declared by the TableDef
        // schema, not just up through the highest non-null var column. Microsoft
        // Access stamps every catalog/system-table row with the schema's full
        // varLen and writes zero-length entries (offset == EOD) for trailing
        // null vars; DAO Compact & Repair's catalog walk rejects rows whose
        // var-offset table is shorter than the schema's variable-column count.
        // See docs/design/round-trip-test-failures.md (hypothesis #6).
        int varLen = maxDefinedVarIdx + 1;
        int baseRowLength = _rowSz.NumCols + fixedAreaSize + varPayloadSize + _rowSz.Eod + (varLen * _rowSz.VarEntry) + _rowSz.VarLen + nullMask.Length;

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

        WriteField(row, pos, _rowSz.NumCols, numCols);
        pos += _rowSz.NumCols;

        if (fixedAreaSize > 0)
        {
            Buffer.BlockCopy(fixedArea, 0, row, pos, fixedAreaSize);
            pos += fixedAreaSize;
        }

        int currentOffset = _rowSz.NumCols + fixedAreaSize;
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

        WriteField(row, pos, _rowSz.Eod, currentOffset);
        pos += _rowSz.Eod;

        for (int varIndex = varLen - 1; varIndex >= 0; varIndex--)
        {
            WriteField(row, pos, _rowSz.VarEntry, variableOffsets[varIndex]);
            pos += _rowSz.VarEntry;
        }

        // Jet3 jump table (entries are zero for newly written rows).
        pos += jumpSize;

        WriteField(row, pos, _rowSz.VarLen, varLen);
        pos += _rowSz.VarLen;
        Buffer.BlockCopy(nullMask, 0, row, pos, nullMask.Length);

        return row;
    }

    private bool CanStoreFixedColumn(ColumnInfo column)
    {
        int size = JetTypeInfo.GetFixedSize(column.Type);
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
        Span<byte> mantissa = dest.Slice(4, 12);
        NumericEncoder.Decompose(value, mantissa, out bool negative, out int scale);

        dest[0] = NumericEncoder.ComputePrecision(mantissa);
        dest[1] = (byte)scale;
        dest[2] = negative ? (byte)1 : (byte)0;
        dest[3] = 0;
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

    internal async ValueTask<List<CatalogRow>> GetCatalogRowsAsync(TableDef msys, CancellationToken cancellationToken)
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

            if (Ri32(page, _dataPage.TDefOff) != 2)
            {
                ReturnPage(page);
                continue;
            }

            foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
            {
                result.Add(new CatalogRow(
                    PageNumber: row.PageNumber,
                    RowIndex: row.RowIndex,
                    Name: DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameColumn),
                    ObjectType: ParseInt32(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, typeColumn)),
                    Flags: ParseInt64(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flagsColumn!)),
                    TDefPage: ParseInt64(DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, idColumn!)) & 0x00FFFFFFL));
            }

            ReturnPage(page);
        }

        return result;
    }

    internal int ParseInt32(string value)
    {
        int parsed;
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) ? parsed : 0;
    }

    private long ParseInt64(string value)
    {
        long parsed;
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) ? parsed : 0L;
    }

    private static byte[] ParseHexBytes(string hex)
    {
#if NET5_0_OR_GREATER
        return Convert.FromHexString(hex);
#else
        byte[] bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = byte.Parse(hex.AsSpan(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return bytes;
#endif
    }

    internal async ValueTask<List<RowLocation>> GetLiveRowLocationsAsync(long tdefPage, CancellationToken cancellationToken)
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

            if (Ri32(page, _dataPage.TDefOff) != tdefPage)
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
    internal async ValueTask<object?[]?> TryReadColumnValuesTypedAsync(
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
            foreach (var column in tableDef.Columns)
            {
                if (!column.IsFixed)
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

                    // Fixed and Var share the decoder; types it can't decode (T_NUMERIC, T_MEMO/OLE/COMPLEX/ATTACHMENT)
                    // return null and force the caller to the snapshot path.
                    case ColumnSliceKind.Fixed:
                    case ColumnSliceKind.Var:
                        result[i] = TryDecodeColumnSlice(pageBytes, loc.RowStart + slice.DataStart, col.Type, slice.DataLen);
                        if (result[i] is null)
                        {
                            return null;
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
    /// Decodes a fixed-area or var-inline column slice into the canonical
    /// CLR object the public InsertRow API accepts back. Returns
    /// <see langword="null"/> on unsupported / malformed types so callers
    /// fall back to the snapshot path. T_NUMERIC always returns null here
    /// (canonical-scale resolution requires column metadata); T_MEMO / T_OLE /
    /// T_COMPLEX / T_ATTACHMENT also return null since they require LVAL
    /// chain traversal.
    /// </summary>
    private object? TryDecodeColumnSlice(byte[] page, int start, byte type, int size)
    {
        if (size <= 0)
        {
            return null;
        }

        switch (type)
        {
            case T_BYTE:
                return page[start];
            case T_INT:
                return size >= 2 ? BinaryPrimitives.ReadInt16LittleEndian(page.AsSpan(start, 2)) : null;
            case T_LONG:
                return size >= 4 ? BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(start, 4)) : null;
            case T_FLOAT:
                return size >= 4 ? JetTypeInfo.ReadSingleLittleEndian(page.AsSpan(start, 4)) : null;
            case T_DOUBLE:
                return size >= 8 ? JetTypeInfo.ReadDoubleLittleEndian(page.AsSpan(start, 8)) : null;
            case T_MONEY:
                return size >= 8 ? BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(start, 8)) / 10000m : null;

            case T_DATETIME:
                if (size < 8)
                {
                    return null;
                }

                try
                {
                    return DateTime.FromOADate(JetTypeInfo.ReadDoubleLittleEndian(page.AsSpan(start, 8)));
                }
                catch (ArgumentException)
                {
                    return null;
                }

            case T_GUID:
                if (size < 16)
                {
                    return null;
                }

                return new Guid(page.AsSpan(start, 16));

            case T_TEXT:
                return _format != DatabaseFormat.Jet3Mdb
                    ? DecodeJet4Text(page, start, size)
                    : _ansiEncoding.GetString(page, start, size);

            case T_BINARY:
                return page.AsSpan(start, size).ToArray();

            // T_MEMO / T_OLE / T_COMPLEX / T_ATTACHMENT / T_NUMERIC — not
            // index-keyable in any case, fall back to snapshot.
            default:
                return null;
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Index-maintenance forwarders. The actual implementations live on
    // <see cref="IndexMaintainer"/>; AccessWriter exposes thin instance
    // forwarders so the many existing call sites (and the public
    // RelationshipManager → writer.MaintainIndexesAsync calls) keep
    // working.
    // ════════════════════════════════════════════════════════════════

    internal ValueTask MaintainIndexesAsync(long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
        => _indexMaintainer.MaintainIndexesAsync(tdefPage, tableDef, tableName, cancellationToken);

    private ValueTask<bool> TryMaintainIndexesIncrementalAsync(
        long tdefPage,
        TableDef tableDef,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        CancellationToken cancellationToken)
        => _indexMaintainer.TryMaintainIndexesIncrementalAsync(tdefPage, tableDef, insertedRows, deletedRows, cancellationToken);

    private ValueTask<bool> TrySpliceCatalogIndexEntryAsync(
        long tdefPage,
        TableDef tableDef,
        RowLocation newRowLoc,
        object[] newRowValues,
        CancellationToken cancellationToken)
        => _indexMaintainer.TrySpliceCatalogIndexEntryAsync(tdefPage, tableDef, newRowLoc, newRowValues, cancellationToken);

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

        int numCols = Ru16(tdefBuffer, _tdef.NumCols);
        int numIdx = Ri32(tdefBuffer, _tdef.NumCols + 2);
        int numRealIdx = Ri32(tdefBuffer, _tdef.NumRealIdx);
        if (numIdx <= 0 || numRealIdx <= 0 || numIdx > 1000 || numRealIdx > 1000)
        {
            return result;
        }

        int colStart = _tdef.BlockEnd + (numRealIdx * _tdef.RealIdxEntrySz);
        int namePos = colStart + (numCols * _colDesc.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (ReadColumnName(tdefBuffer, ref namePos, out _) < 0)
            {
                return result;
            }
        }

        int realIdxDescStart = namePos;
        var anchors = _indexLayout.GetIndexSection(realIdxDescStart, numRealIdx, numIdx);
        List<string> logIdxNames = _relationships.ReadLogicalIdxNames(tdefBuffer, anchors.LogIdxNamesStart, numIdx);

        // Decode the index catalog (with per-real-idx names so the
        // unique-violation error message can quote the originating
        // logical-idx) and pre-resolve each slot's key columns against the
        // table snapshot in one shot.
        IndexCatalogReader.ResolvedIndexCatalog catalog = IndexCatalogReader.ReadResolved(
            tdefBuffer, _indexLayout, anchors, tableDef.Columns, logIdxNames);

        foreach ((int realIdxNum, RealIdxEntry slot) in catalog.RealIdxByNum)
        {
            if (!catalog.Catalog.IsUniqueOrPk(realIdxNum))
            {
                continue;
            }

            // Skip indexes whose key columns failed to resolve against the
            // snapshot (deleted-column gap) — same fall-through model as
            // MaintainIndexesAsync.
            if (!catalog.TryGetKeyColumnInfos(realIdxNum, out List<KeyColumnInfo> keyColInfos))
            {
                continue;
            }

            result.Add(new UniqueIndexDescriptor(realIdxNum, catalog.Catalog.GetNameOrFallback(realIdxNum), keyColInfos));
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

    internal async ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, CancellationToken cancellationToken)
    {
        byte[] page = await ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        int offsetPos = _dataPage.RowsStart + (rowIndex * 2);
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
