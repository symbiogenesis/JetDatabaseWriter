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
using JetDatabaseWriter.Catalog;
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
using JetDatabaseWriter.ValueEncoding.Models;
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
public sealed class AccessWriter : AccessBase, IAccessWriter, IAccessSchema
{
    internal const int MaxInlineMemoBytes = 1024;
    internal const int MaxInlineOleBytes = 256;

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

    /// <summary>The single instance owning index B-tree maintenance: bulk rebuild,
    /// incremental fast paths, and the catalog-index single-leaf splice. AccessWriter
    /// keeps only thin instance forwarders.</summary>
    private readonly IndexMaintainer _indexMaintainer;

    /// <summary>Owns TDEF emission and empty-database bootstrap builders.
    /// AccessWriter keeps thin compatibility forwarders for existing callers.</summary>
    private readonly TDefPageBuilder _tdefPageBuilder;

    /// <summary>Owns LVAL chain encoding: pre-encode oversized MEMO/OLE/Attachment payloads.</summary>
    private readonly LongValueEncoder _longValueEncoder;

    /// <summary>Owns pre-write unique-index violation checks.</summary>
    private readonly UniqueIndexChecker _uniqueIndexChecker;

    /// <summary>Owns transaction lifecycle: begin, commit, rollback, auto-commit wrapping.</summary>
    private readonly TransactionLifecycle _transactionLifecycle;

    /// <summary>Owns catalog (MSysObjects) write operations: insert, rename, ACE rows.</summary>
    private readonly CatalogWriter _catalogWriter;

    /// <summary>Encodes value arrays into on-disk row byte layouts.</summary>
    private readonly RowEncoder _rowEncoder;

    /// <summary>Owns data-page allocation and row insertion mechanics.</summary>
    private readonly DataPageInserter _dataPageInserter;

    /// <summary>Gets the foreign-key / relationship subsystem. The bulk of
    /// FK code (catalog rows, per-TDEF logical-index entries, runtime
    /// referential-integrity enforcement) lives there; <see cref="AccessWriter"/>
    /// keeps only thin public-API forwarders. Exposed for sibling managers
    /// (e.g. <see cref="ComplexColumnManager"/>) that need to delegate FK /
    /// system-table lookups.</summary>
    internal RelationshipManager Relationships { get; }

    /// <summary>Gets the Attachment / MultiValue (complex column) subsystem:
    /// system-table scaffolding, per-table allocation of ComplexID /
    /// ConceptualTableID, hidden flat-child-table emission, the row-level
    /// Add* APIs, and cascade / drop / rename plumbing for the artifacts.
    /// <see cref="AccessWriter"/> keeps only thin public-API forwarders.
    /// Exposed for sibling managers (e.g. <see cref="Relationships.RelationshipManager"/>)
    /// that can delegate cascade-on-delete to the complex children.</summary>
    internal ComplexColumnManager ComplexColumns { get; }

    /// <summary>Gets the per-table client-side constraint registry. Populated by
    /// <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, CancellationToken)"/>
    /// and the schema-evolution helpers. Keyed by table name (case-insensitive). The list is
    /// kept positionally aligned with the table's columns and is consulted at insert time to
    /// apply default values, auto-increment, required-field, and validation rule semantics.
    /// Exposed for sibling managers (e.g. <see cref="ComplexColumns.ComplexColumnManager"/>)
    /// that apply constraints directly without a pass-through forwarder.</summary>
    internal ConstraintRegistry Constraints { get; }

    /// <summary>Gets the writer options.</summary>
    internal AccessWriterOptions Options => _options;

    /// <summary>Gets or sets the active explicit transaction.</summary>
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via DisposeActiveTransactionAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    internal JetTransaction? ActiveTransaction { get; set; }

    /// <summary>Gets the cooperative JET byte-range lock helper.</summary>
    internal JetByteRangeLock ByteRangeLock => _byteRangeLock;

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
        Relationships = new RelationshipManager(this);
        _indexMaintainer = new IndexMaintainer(this);
        ComplexColumns = new ComplexColumnManager(this);
        _tdefPageBuilder = new TDefPageBuilder(this);
        _longValueEncoder = new LongValueEncoder(this);
        _uniqueIndexChecker = new UniqueIndexChecker(this);
        _transactionLifecycle = new TransactionLifecycle(this);
        _catalogWriter = new CatalogWriter(this);
        _rowEncoder = new RowEncoder(this);
        _dataPageInserter = new DataPageInserter(this);
        Constraints = new ConstraintRegistry(
            async (tableName, ct) => await ReadTableSnapshotAsync(tableName, ct).ConfigureAwait(false));

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
            await writer.ComplexColumns.ScaffoldSystemTablesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
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
        try
        {
            await writer.ComplexColumns.ScaffoldSystemTablesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
            return writer;
        }
        catch
        {
            await writer.DisposeAsync().ConfigureAwait(false);
            throw;
        }
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
            await ComplexColumns.PrepareComplexColumnAllocationsAsync(columns, cancellationToken).ConfigureAwait(false);
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
            await ComplexColumns.EmitComplexColumnArtifactsAsync(tableName, columns, complexAllocs, cancellationToken).ConfigureAwait(false);
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
            long usageMapPageNumber = await _dataPageInserter.AppendUsageMapPageAsync(cancellationToken).ConfigureAwait(false);

            // PatchUsageMapPointers / PatchAutoNumFlag write only into the
            // TDEF header (offsets 0x18, 0x37..0x3F), which always live on
            // the first physical page.
            DataPageInserter.PatchUsageMapPointers(tdefPages[0], checked((int)usageMapPageNumber));
            DataPageInserter.PatchAutoNumFlag(tdefPages[0], tableDef);
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
            await _catalogWriter.InsertAceRowsForTableAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        }

        Constraints.Register(tableName, columns);
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
                            IgnoreNulls = idx.IgnoreNulls,
                        });
                    }
                    else
                    {
                        result.Add(new IndexDefinition(idx.Name, remappedCols)
                        {
                            IsUnique = idx.IsUnique,
                            DescendingColumns = descendingCols,
                            IgnoreNulls = idx.IgnoreNulls,
                            IsRequired = idx.IsRequired,
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
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await Relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
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
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await Relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
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
                    await Constraints.ApplyAsync(tableName, tableDef, row, cancellationToken).ConfigureAwait(false);
                if (rowCp != null)
                {
                    (batchAutoCheckpoints ??= []).AddRange(rowCp);
                }

                pendingRows.Add(row);
            }

            // Pre-write unique-index enforcement.
            await _uniqueIndexChecker.CheckUniqueIndexesPreInsertAsync(entry.TDefPage, tableDef, tableName, pendingRows, cancellationToken).ConfigureAwait(false);

            foreach (object[] row in pendingRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (fkCtx != null)
                {
                    await Relationships.EnforceFkOnInsertAsync(tableName, tableDef, row, fkCtx, cancellationToken).ConfigureAwait(false);
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
                bool incremental = await _indexMaintainer.TryMaintainIndexesIncrementalAsync(
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
            ConstraintRegistry.RestoreAutoCounters(batchAutoCheckpoints);
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

        IReadOnlyList<RelationshipManager.FkRelationship> relsT = await Relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
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
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await Relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
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
                    await Constraints.ApplyAsync(tableName, tableDef, mappedRow, cancellationToken).ConfigureAwait(false);
                if (rowCp != null)
                {
                    (batchAutoCheckpoints ??= []).AddRange(rowCp);
                }

                pendingRows.Add(mappedRow);
            }

            // Pre-write unique-index enforcement.
            await _uniqueIndexChecker.CheckUniqueIndexesPreInsertAsync(entry.TDefPage, tableDef, tableName, pendingRows, cancellationToken).ConfigureAwait(false);

            foreach (object[] mappedRow in pendingRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (fkCtx != null)
                {
                    await Relationships.EnforceFkOnInsertAsync(tableName, tableDef, mappedRow, fkCtx, cancellationToken).ConfigureAwait(false);
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
                bool incremental = await _indexMaintainer.TryMaintainIndexesIncrementalAsync(
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
            ConstraintRegistry.RestoreAutoCounters(batchAutoCheckpoints);
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
        IReadOnlyList<RelationshipManager.FkRelationship> rels = await Relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
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
                await Relationships.EnforceFkOnInsertAsync(tableName, tableDef, newRow, fkCtx, cancellationToken).ConfigureAwait(false);
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

                await Relationships.EnforceFkOnPrimaryUpdateAsync(tableName, tableDef, changes, fkCtx, depth: 0, cancellationToken).ConfigureAwait(false);
            }
        }

        // Pre-write unique-index enforcement: after FK checks succeed,
        // validate that the post-update key set contains no duplicates for
        // any unique index. The check sees the snapshot with pendingNewRows
        // substituted at their original indices.
        if (pendingNewRows.Count > 0)
        {
            await _uniqueIndexChecker.CheckUniqueIndexesPreUpdateAsync(entry.TDefPage, tableDef, tableName, snapshot, pendingNewRows, cancellationToken).ConfigureAwait(false);
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
            bool incremental = await _indexMaintainer.TryMaintainIndexesIncrementalAsync(
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

        IReadOnlyList<RelationshipManager.FkRelationship> rels = await Relationships.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
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

            await Relationships.EnforceFkOnPrimaryDeleteAsync(
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

            await ComplexColumns.CascadeDeleteComplexChildrenAsync(tableDef, parentLocs, cancellationToken).ConfigureAwait(false);
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
            bool incremental = await _indexMaintainer.TryMaintainIndexesIncrementalAsync(
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

    /// <summary>
    /// Asynchronously creates a linked-text/CSV table entry (MSysObjects type 6) that references
    /// a text or CSV file in a directory. The entry stores both a <c>Database</c> path (the
    /// directory containing the file) and a <c>Connect</c> string (e.g.
    /// <c>"Text;HDR=YES;FMT=Delimited"</c>). No row data is stored locally.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDirectoryPath">Path to the directory containing the text/CSV source file.</param>
    /// <param name="foreignFileName">The filename of the text/CSV source (e.g. <c>"data.csv"</c>).</param>
    /// <param name="connectString">The text-driver connect string (e.g. <c>"Text;HDR=YES;FMT=Delimited"</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask CreateLinkedTextTableAsync(string linkedTableName, string sourceDirectoryPath, string foreignFileName, string connectString, CancellationToken cancellationToken = default)
        => RunAutoCommitAsync(_ => CreateLinkedTextTableCoreAsync(linkedTableName, sourceDirectoryPath, foreignFileName, connectString, cancellationToken), cancellationToken);

    private async ValueTask CreateLinkedTextTableCoreAsync(string linkedTableName, string sourceDirectoryPath, string foreignFileName, string connectString, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(linkedTableName, nameof(linkedTableName));
        Guard.NotNullOrEmpty(sourceDirectoryPath, nameof(sourceDirectoryPath));
        Guard.NotNullOrEmpty(foreignFileName, nameof(foreignFileName));
        Guard.NotNullOrEmpty(connectString, nameof(connectString));
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
        msys.SetValueByName(values, "ForeignName", foreignFileName);
        msys.SetValueByName(values, "Database", sourceDirectoryPath);
        msys.SetValueByName(values, "Connect", connectString);

        await InsertRowDataAsync(2, msys, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        InvalidateCatalogCache();
    }

    // ════════════════════════════════════════════════════════════════
    // Foreign-key relationships — thin forwarders to RelationshipManager
    // ════════════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => Relationships.CreateRelationshipAsync(relationship, cancellationToken);

    /// <inheritdoc/>
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
        => Relationships.DropRelationshipAsync(relationshipName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
        => Relationships.RenameRelationshipAsync(oldName, newName, cancellationToken);

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
    public ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        => _transactionLifecycle.BeginTransactionAsync(cancellationToken);

    /// <summary>
    /// If <see cref="AccessWriterOptions.UseTransactionalWrites"/> is enabled
    /// and no explicit transaction is currently active, wraps
    /// <paramref name="work"/> in a private <see cref="JetTransaction"/> so a
    /// crash mid-call leaves the database in its pre-call state. Otherwise
    /// invokes <paramref name="work"/> directly (today's flush-per-page path).
    /// </summary>
    internal ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
        => _transactionLifecycle.RunAutoCommitAsync(work, cancellationToken);

    /// <summary>
    /// Generic-result variant of <see cref="RunAutoCommitAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/>.
    /// </summary>
    internal ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
        => _transactionLifecycle.RunAutoCommitAsync(work, cancellationToken);

    /// <summary>
    /// Commits the supplied <paramref name="transaction"/>: detaches the
    /// journal from the writer and replays each buffered page (in ascending
    /// page-number order) through the normal page-write pipeline so that
    /// per-page encryption and cooperative byte-range locks are honoured.
    /// </summary>
    internal ValueTask CommitTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
        => _transactionLifecycle.CommitTransactionAsync(transaction, cancellationToken);

    /// <summary>
    /// Rolls back the supplied <paramref name="transaction"/>: discards the
    /// in-memory journal without touching the database file.
    /// </summary>
    internal ValueTask RollbackTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
        => _transactionLifecycle.RollbackTransactionAsync(transaction, cancellationToken);

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
        if (ActiveTransaction is null)
        {
            return;
        }

        try
        {
            await ActiveTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            ActiveJournal = null;
            ActiveTransaction = null;
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
            await Constraints.ApplyAsync(tableName, tableDef, values, cancellationToken).ConfigureAwait(false);

        if (fkCtx != null)
        {
            try
            {
                await Relationships.EnforceFkOnInsertAsync(tableName, tableDef, values, fkCtx, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
                throw;
            }
        }

        // Pre-write unique-index enforcement: reject duplicate keys before
        // any disk page is mutated. The post-write check inside
        // MaintainIndexesAsync still runs as defense-in-depth.
        try
        {
            await _uniqueIndexChecker.CheckUniqueIndexesPreInsertAsync(tdefPage, tableDef, tableName, [values], cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
            throw;
        }

        RowLocation loc;
        try
        {
            loc = await InsertRowDataLocAsync(tdefPage, tableDef, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
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
            bool incremental = await _indexMaintainer.TryMaintainIndexesIncrementalAsync(
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
            ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
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
        => _catalogWriter.InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, cancellationToken);

    internal ValueTask InsertCatalogEntryAsync(string tableName, long tdefPageNumber, byte[]? lvProp, uint catalogFlags, CancellationToken cancellationToken = default)
        => _catalogWriter.InsertCatalogEntryAsync(tableName, tdefPageNumber, lvProp, catalogFlags, cancellationToken);

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
        Constraints.TryGet(tableName, out List<ColumnConstraint>? existingConstraints);

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
        await _catalogWriter.RenameTableInCatalogAsync(tempName, tableName, renamedLvProp, cancellationToken).ConfigureAwait(false);

        foreach ((string colName, int complexId) in droppedComplex)
        {
            await ComplexColumns.DropSingleComplexChildAsync(colName, complexId, cancellationToken).ConfigureAwait(false);
        }

        foreach ((string oldColName, string newColName, int complexId) in renamedComplex)
        {
            await ComplexColumns.RenameComplexColumnArtifactsAsync(oldColName, newColName, complexId, cancellationToken).ConfigureAwait(false);
        }
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
            // See AccessReader IsNullable comment: 0x08 is the writer-private NOT NULL marker.
            IsNullable = (column.Flags & 0x08) == 0,
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
    /// from offset 8 onward. Use <see cref="TDefPageBuilder.LogicalToPhysicalTDefOffset"/>
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
    /// Writes a 4-byte little-endian integer at the given LOGICAL TDEF
    /// offset, dispatching the bytes across the physical page boundary
    /// when the field straddles two pages. Used to patch <c>first_dp</c>
    /// values after their leaf pages are appended.
    /// </summary>
    private void WriteLogicalTDefI32(byte[][] pages, int logicalOffset, int value)
    {
        for (int i = 0; i < 4; i++)
        {
            (int pageIdx, int pageOff) = _tdefPageBuilder.LogicalToPhysicalTDefOffset(logicalOffset + i);
            pages[pageIdx][pageOff] = (byte)((value >> (i * 8)) & 0xFF);
        }
    }

    private void WriteLogicalTDefUInt24(byte[][] pages, int logicalOffset, int value)
    {
        for (int i = 0; i < 3; i++)
        {
            (int pageIdx, int pageOff) = _tdefPageBuilder.LogicalToPhysicalTDefOffset(logicalOffset + i);
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
            _ => ComplexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, attachment, expectAttachment: true, cancellationToken),
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
            _ => ComplexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, value, expectAttachment: false, cancellationToken),
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
                await ComplexColumns.DropComplexChildrenForTableAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
            }
        }

        Constraints.Unregister(tableName);
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
            bool incremental = await _indexMaintainer.TryMaintainIndexesIncrementalAsync(
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

            int realIdxDescStart = Relationships.LocateRealIdxDescStart(page, numCols, numRealIdx);
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
        values = await _longValueEncoder.PreEncodeLongValuesAsync(tableDef, values, cancellationToken).ConfigureAwait(false);

        byte[] rowBytes = _rowEncoder.SerializeRow(tableDef, values);
        PageInsertTarget target = await _dataPageInserter.FindInsertTargetAsync(tdefPage, rowBytes.Length, cancellationToken).ConfigureAwait(false);
        int rowIndex;
        int rowStart;
        try
        {
            rowIndex = Ru16(target.Page, _dataPage.NumRows);
            rowStart = _dataPageInserter.GetFirstRowStart(target.Page, rowIndex) - rowBytes.Length;
            await _dataPageInserter.WriteRowToPageAsync(target.PageNumber, target.Page, rowBytes, cancellationToken).ConfigureAwait(false);
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

    internal bool TryGetCachedInsertPageNumber(long tdefPage, out long pageNumber)
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

    internal void SetCachedInsertPageNumber(long tdefPage, long pageNumber)
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

    internal ValueTask<List<CatalogRow>> GetCatalogRowsAsync(TableDef msys, CancellationToken cancellationToken)
        => _catalogWriter.GetCatalogRowsAsync(msys, cancellationToken);

    internal int ParseInt32(string value)
    {
        int parsed;
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) ? parsed : 0;
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

    internal ValueTask<bool> TrySpliceCatalogIndexEntryAsync(
        long tdefPage,
        TableDef tableDef,
        RowLocation newRowLoc,
        object[] newRowValues,
        CancellationToken cancellationToken)
        => _indexMaintainer.TrySpliceCatalogIndexEntryAsync(tdefPage, tableDef, newRowLoc, newRowValues, cancellationToken);

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
}
