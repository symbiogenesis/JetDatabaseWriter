namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueEncoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

#pragma warning disable SA1202 // Keep member order stable while synchronous APIs remain private compatibility helpers
#pragma warning disable SA1204 // Static members grouped logically alongside related instance members

/// <summary>
/// Pure-managed writer for Microsoft Access JET databases (.mdb / .accdb).
/// Supports creating tables, inserting, updating, and deleting rows.
/// </summary>
public sealed class AccessWriter : AccessBase, IAccessWriter, IAccessSchema
{
    /// <summary>
    /// Maximum recursion depth for cascade-delete / cascade-update chains.
    /// Guards against pathological self-referential cycles. Real-world Access
    /// schemas almost never exceed depth 3.
    /// </summary>
    internal const int CascadeMaxDepth = 64;
    private readonly LockFileCoordinator lockFileCoordinator;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via DisposeStateLockAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    private readonly ReaderWriterLockSlim stateLock = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// Office Crypto re-encryption context. When non-null, the underlying _stream is an
    /// in-memory MemoryStream containing the *decrypted* inner ACCDB; on
    /// DisposeAsync the bytes are re-encrypted with the original Office Crypto format
    /// and written back to _outerEncryptedStream (which holds the original CFB).
    /// </summary>
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via RewrapAndCloseOuterEncryptedStreamAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    private readonly Stream? outerEncryptedStream;
    private readonly bool outerEncryptedLeaveOpen;
    private readonly AccessEncryptionFormat outerEncryptedFormat;
    private readonly bool isAgileEncryptedRewrap;

    /// <summary>The single instance owning index B-tree maintenance: bulk rebuild,
    /// incremental fast paths, and the catalog-index splice.</summary>
    private readonly IndexMaintainer indexMaintainer;
    private readonly HashSet<long> ownedMapWritableTdefs = [];

    /// <summary>Builds table-definition pages for writer-created schemas.</summary>
    private readonly TDefPageBuilder tdefPageBuilder;

    /// <summary>Advances the per-table AutoNumber high-water counter after inserts.</summary>
    private readonly AutoNumberMaintainer autoNumberMaintainer;

    /// <summary>Pre-encodes oversized MEMO/OLE/Attachment payloads into LVAL chains.</summary>
    private readonly LongValueEncoder longValueEncoder;

    /// <summary>Runs pre-write unique-index violation checks.</summary>
    private readonly UniqueIndexChecker uniqueIndexChecker;

    /// <summary>Coordinates transaction lifecycle: begin, commit, rollback, auto-commit wrapping.</summary>
    private readonly TransactionLifecycle transactionLifecycle;

    /// <summary>Writes catalog (MSysObjects) entries, renames, and ACE rows.</summary>
    private readonly CatalogWriter catalogWriter;

    /// <summary>Encodes value arrays into on-disk row byte layouts.</summary>
    private readonly RowEncoder rowEncoder;

    /// <summary>Handles data-page allocation and row insertion mechanics.</summary>
    private readonly DataPageInserter dataPageInserter;

    /// <summary>Maintains the global page free-list allocator.</summary>
    private readonly PageAllocator pageAllocator;

    /// <summary>Gets the foreign-key / relationship subsystem. Relationship
    /// lifecycle and TDEF mutation are coordinated there, while catalog rows
    /// and runtime referential-integrity enforcement are delegated to smaller
    /// relationship collaborators. <see cref="AccessWriter"/> keeps only thin
    /// public-API forwarders. Exposed for sibling managers (e.g.
    /// <see cref="ComplexColumnManager"/>) that need to delegate FK /
    /// system-table lookups.</summary>
    internal RelationshipManager Relationships { get; }

    /// <summary>Gets the most recent system-table index-maintenance path.</summary>
    internal SystemTableIndexMaintenancePath LastSystemTableIndexMaintenancePath => this.indexMaintainer.LastSystemTableIndexMaintenancePath;

    /// <summary>Gets the Attachment / MultiValue (complex column) subsystem:
    /// ACCDB system-table scaffolding, per-column ComplexID allocation, per-row
    /// complex-reference allocation, hidden flat-child-table emission, the row-level
    /// Add* APIs, and cascade / drop / rename plumbing for the artifacts.
    /// <see cref="AccessWriter"/> keeps only thin public-API forwarders.
    /// Exposed for sibling managers (e.g. <see cref="RelationshipManager"/>)
    /// that can delegate cascade-on-delete to the complex children.</summary>
    internal ComplexColumnManager ComplexColumns { get; }

    /// <summary>Gets the per-table client-side constraint registry. Populated by
    /// <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, CancellationToken)"/>
    /// and the schema-evolution helpers. Keyed by table name (case-insensitive). The list is
    /// kept positionally aligned with the table's columns and is consulted at insert time to
    /// apply default values, auto-increment, required-field, and validation rule semantics.
    /// Exposed for sibling managers (e.g. <see cref="ComplexColumnManager"/>)
    /// that apply constraints directly without a pass-through forwarder.</summary>
    internal ConstraintRegistry Constraints { get; }

    /// <summary>Gets the writer options.</summary>
    internal AccessWriterOptions Options { get; }

    private protected override bool CanCacheOwnedDataPages => false;

    /// <summary>Gets or sets the active explicit transaction.</summary>
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via DisposeActiveTransactionAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    internal JetTransaction? ActiveTransaction { get; set; }

    /// <summary>Gets the cooperative JET byte-range lock helper.</summary>
    internal JetByteRangeLock ByteRangeLock => this.ByteRangeLockCore;

    private long cachedInsertTDefPage = -1;
    private long cachedInsertPageNumber = -1;

    private AccessWriter(
        string path,
        Stream stream,
        byte[] header,
        AccessWriterOptions options,
        Stream? outerEncryptedStream = null,
        bool outerEncryptedLeaveOpen = false,
        AccessEncryptionFormat outerEncryptedFormat = AccessEncryptionFormat.None,
        bool leaveOpen = false)
        : base(
            stream,
            header,
            options.Password,
            path,
            leaveOpen)
    {
        this.Options = options;
        this.lockFileCoordinator = LockFileCoordinator.ForWriter(path, options);
        this.outerEncryptedStream = outerEncryptedStream;
        this.outerEncryptedLeaveOpen = outerEncryptedLeaveOpen;
        this.outerEncryptedFormat = outerEncryptedFormat;
        this.isAgileEncryptedRewrap = outerEncryptedFormat != AccessEncryptionFormat.None;
        this.pageAllocator = new PageAllocator(this);
        this.indexMaintainer = new IndexMaintainer(this, this.pageAllocator);
        this.Relationships = new RelationshipManager(this, this.indexMaintainer, this.pageAllocator);
        this.ComplexColumns = new ComplexColumnManager(this, this.indexMaintainer);
        this.tdefPageBuilder = new TDefPageBuilder(this);
        this.autoNumberMaintainer = new AutoNumberMaintainer(this);
        this.longValueEncoder = new LongValueEncoder(this, this.pageAllocator);
        this.uniqueIndexChecker = new UniqueIndexChecker(this);
        this.transactionLifecycle = new TransactionLifecycle(this);
        this.catalogWriter = new CatalogWriter(this, this.indexMaintainer, this.longValueEncoder);
        this.rowEncoder = new RowEncoder(this);
        this.dataPageInserter = new DataPageInserter(this, this.pageAllocator);
        this.Constraints = new ConstraintRegistry(
            this.ReadTableSnapshotAsync,
            async (tableName, ct) =>
            {
                CatalogEntry? entry = await this.GetCatalogEntryAsync(tableName, ct).ConfigureAwait(false);
                if (entry is null)
                {
                    return null;
                }

                return await this.ReadLvPropBlockAsync(entry.TDefPage, ct).ConfigureAwait(false);
            });

        this.lockFileCoordinator.Acquire();
        try
        {
            this.ByteRangeLockCore = JetByteRangeLock.Create(stream, options.UseByteRangeLocks, options.LockTimeoutMilliseconds);
        }
        catch
        {
            this.lockFileCoordinator.Dispose();
            throw;
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
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

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
        Guard.RequireReadWriteSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        options ??= new AccessWriterOptions();
        try
        {
            string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
            byte[] header = await ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);

            // Office Crypto API ("Agile") encrypted .accdb files are real OLE
            // compound documents (CFB) wrapping an EncryptedPackage stream.
            // We can't edit them in place: writes are buffered into an
            // in-memory MemoryStream containing the *decrypted* inner ACCDB,
            // and the whole CFB is re-emitted on DisposeAsync.
            if (EncryptionManager.IsCompoundFileEncrypted(header))
            {
                _ = stream.Seek(0, SeekOrigin.Begin);
                (byte[]? decryptedPackage, AccessEncryptionFormat outerFormat) = await EncryptionManager
                    .TryDecryptCompoundFileWithFormatAsync(stream, header, options.Password, cancellationToken)
                    .ConfigureAwait(false);

                if (decryptedPackage != null)
                {
                    var inner = new MemoryStream();
                    await inner.WriteAsync(decryptedPackage.AsMemory(), cancellationToken).ConfigureAwait(false);
                    inner.Position = 0;
                    byte[] innerHeader = await ReadHeaderAsync(inner, cancellationToken).ConfigureAwait(false);

                    return new AccessWriter(
                        path,
                        inner,
                        innerHeader,
                        options,
                        outerEncryptedStream: stream,
                        outerEncryptedLeaveOpen: leaveOpen,
                        outerEncryptedFormat: outerFormat);
                }

                // CFB magic but not a real Agile compound document: treat as
                // the synthetic legacy AES-128 layout (flat per-page AES-ECB
                // beneath a CFB-magic header byte). The constructor sets up
                // the page key and writes are re-encrypted on every flush.
                _ = stream.Seek(0, SeekOrigin.Begin);
            }

            return new AccessWriter(
                path,
                stream,
                header,
                options,
                leaveOpen: leaveOpen);
        }
        catch
        {
            if (!leaveOpen)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
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
    /// <exception cref="IOException">Thrown when a database file already exists at <paramref name="path"/>.</exception>
    public static async ValueTask<AccessWriter> CreateDatabaseAsync(string path, DatabaseFormat format, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();

        if (File.Exists(path))
        {
            throw new IOException($"Database file already exists: {path}");
        }

        byte[] dbBytes = TDefPageBuilder.BuildEmptyDatabase(format, options?.WriteFullCatalogSchema ?? true);

        await using (FileStream fs = FileStreamFactory.Open(
            path,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            FileOptions.Asynchronous,
            preallocationSize: dbBytes.Length))
        {
            await fs.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            AccessWriter writer = await OpenAsync(path, options, cancellationToken).ConfigureAwait(false);
            long coreSystemTableStartPage = await writer.ReserveFreshCoreSystemTablePagesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
            await writer.InitializeFreshCatalogIndexesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
            await writer.ComplexColumns.ScaffoldSystemTablesAsync(format, options?.WriteFullCatalogSchema ?? true, coreSystemTableStartPage, cancellationToken).ConfigureAwait(false);
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
        Guard.RequireReadWriteSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        byte[] dbBytes = TDefPageBuilder.BuildEmptyDatabase(format, options?.WriteFullCatalogSchema ?? true);
        await stream.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        stream.Position = 0;

        AccessWriter writer = await OpenAsync(stream, options, leaveOpen, cancellationToken).ConfigureAwait(false);
        try
        {
            long coreSystemTableStartPage = await writer.ReserveFreshCoreSystemTablePagesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
            await writer.InitializeFreshCatalogIndexesAsync(format, options?.WriteFullCatalogSchema ?? true, cancellationToken).ConfigureAwait(false);
            await writer.ComplexColumns.ScaffoldSystemTablesAsync(format, options?.WriteFullCatalogSchema ?? true, coreSystemTableStartPage, cancellationToken).ConfigureAwait(false);
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
        => this.CreateTableAsync(tableName, columns, indexes: [], cancellationToken);

    /// <inheritdoc/>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.CreateTableCoreAsync(tableName, columns, indexes, cancellationToken), cancellationToken);

    private async ValueTask CreateTableCoreAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        Guard.NotNull(indexes, nameof(indexes));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required", nameof(columns));
        }

        // Pre-process the column-level IsPrimaryKey shortcut. Synthesize one
        // composite PK IndexDefinition (named "PrimaryKey") from columns
        // marked IsPrimaryKey=true, in declaration order, and force those
        // columns to IsNullable=false on the emitted TDEF. Mixing the
        // shortcut with an explicit PK IndexDefinition is rejected.
        (columns, indexes) = IndexHelpers.ApplyPrimaryKeyShortcut(columns, indexes);

        // Unsupported Jet4 key types (OLE / Attachment / Multi-Value) are
        // rejected up-front below in ResolveIndexes.

        if (await this.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        // Complex columns (Attachment / MultiValue) declared by the user have
        // ComplexId = 0; allocate fresh per-database ComplexIDs, then emit the hidden
        // flat child table + MSysComplexColumns row per column AFTER the parent TDEF
        // is on disk. The round-trip preservation path on RewriteTableAsync supplies a
        // non-zero ComplexId from the original TDEF and is left untouched here.
        IReadOnlyList<ComplexColumnAllocation>? complexAllocs =
            await this.ComplexColumns.PrepareComplexColumnAllocationsAsync(columns, cancellationToken).ConfigureAwait(false);
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

        uint catalogFlags = 0;
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].IsAttachment || columns[i].IsMultiValue)
            {
                catalogFlags = 0x00040000U;
                break;
            }
        }

        var tableArtifact = new CatalogTableArtifact(tableName, columns, indexes, catalogFlags);
        long tdefPageNumber = await this.CreateTableInternalAsync(tableArtifact, cancellationToken).ConfigureAwait(false);

        // Emit the hidden flat child table + MSysComplexColumns row for every
        // user-declared complex column. Done after the parent table is on disk so the
        // catalog cache reflects the parent before flat-table inserts.
        if (complexAllocs is { Count: > 0 })
        {
            await this.ComplexColumns.EmitComplexColumnArtifactsAsync(tableName, tdefPageNumber, columns, complexAllocs, cancellationToken).ConfigureAwait(false);
        }

        _ = tdefPageNumber;
    }

    private async ValueTask InitializeFreshCatalogIndexesAsync(DatabaseFormat format, bool fullCatalogSchema, CancellationToken cancellationToken)
    {
        if (format == DatabaseFormat.Jet3Mdb || !fullCatalogSchema)
        {
            return;
        }

        IReadOnlyList<ColumnDefinition> columns = BuildFullCatalogColumnDefinitions();
        TableDef tableDef = TDefPageBuilder.BuildTableDefinition(columns, this.Format);
        var indexes = new IndexDefinition[]
        {
            new("Id", "Id") { IsPrimaryKey = true },
            new("ParentIdName", ["ParentId", "Name"]) { IsUnique = true },
        };

        List<ResolvedIndex> resolvedIndexes = IndexHelpers.ResolveIndexes(indexes, tableDef);
        (byte[][] tdefPages, int[] firstDpLogicalOffsets, int[] usedPagesLogicalOffsets) = this.tdefPageBuilder.BuildTDefPagesWithIndexOffsets(tableDef, resolvedIndexes);
        if (tdefPages.Length != 1)
        {
            throw new InvalidDataException("Fresh MSysObjects bootstrap unexpectedly produced a multi-page TDEF.");
        }

        tdefPages[0][this.TDef.NumCols - 5] = 0x53;
        var layout = IndexPageLayout.ForFormat(this.Format);
        long[] leafPageNumbers = new long[resolvedIndexes.Count];
        for (int i = 0; i < resolvedIndexes.Count; i++)
        {
            byte[] leafPage = IndexPageCodec.BuildLeafPage(
                layout,
                this.PageSizeBytes,
                parentTdefPage: 2,
                entries: [],
                enablePrefixCompression: false);
            long leafPageNumber = await this.pageAllocator.AllocatePageAsync(leafPage, cancellationToken).ConfigureAwait(false);
            leafPageNumbers[i] = leafPageNumber;
            this.tdefPageBuilder.WriteLogicalTDefI32(tdefPages, firstDpLogicalOffsets[i], checked((int)leafPageNumber));
        }

        long usageMapPageNumber = await this.dataPageInserter.AppendUsageMapPageAsync(cancellationToken).ConfigureAwait(false);
        await this.UpdateTableIndexUsageMapRowsAsync(
            usageMapPageNumber,
            DataPageInserter.ToSinglePageGroups(leafPageNumbers),
            cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < usedPagesLogicalOffsets.Length; i++)
        {
            int usedPagesOffset = usedPagesLogicalOffsets[i];
            tdefPages[usedPagesOffset / this.PageSizeBytes][usedPagesOffset % this.PageSizeBytes] = checked((byte)(i + 2));
            this.tdefPageBuilder.WriteLogicalTDefUInt24(tdefPages, usedPagesOffset + 1, checked((int)usageMapPageNumber));
        }

        DataPageInserter.PatchUsageMapPointers(tdefPages[0], checked((int)usageMapPageNumber));
        DataPageInserter.PatchAutoNumFlag(tdefPages[0], tableDef);
        await this.WritePageAsync(2, tdefPages[0], cancellationToken).ConfigureAwait(false);
        this.RegisterOwnedMapWritableTdef(2);
        this.InvalidateCatalogCache();
    }

    private ValueTask<long> ReserveFreshCoreSystemTablePagesAsync(DatabaseFormat format, bool fullCatalogSchema, CancellationToken cancellationToken)
        => format == DatabaseFormat.AceAccdb && fullCatalogSchema
            ? this.pageAllocator.ReserveContiguousPagesAsync(3, cancellationToken)
            : new ValueTask<long>(0L);

    private static IReadOnlyList<ColumnDefinition> BuildFullCatalogColumnDefinitions()
        =>
        [
            new("Id", typeof(int)) { IsNullable = false, DescriptorFlagsOverride = 0x13 },
            new("ParentId", typeof(int)) { IsNullable = false, DescriptorFlagsOverride = 0x13 },
            new("Name", typeof(string), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
            new("Type", typeof(short)) { IsNullable = false, DescriptorFlagsOverride = 0x13 },
            new("DateCreate", typeof(DateTime)) { DescriptorFlagsOverride = 0x13 },
            new("DateUpdate", typeof(DateTime)) { DescriptorFlagsOverride = 0x13 },
            new("Owner", typeof(byte[]), maxLength: 255) { DescriptorFlagsOverride = 0x32 },
            new("Flags", typeof(int)) { DescriptorFlagsOverride = 0x13 },
            new("Database", typeof(string)) { DescriptorFlagsOverride = 0x12, IsCompressedUnicode = false },
            new("Connect", typeof(string)) { DescriptorFlagsOverride = 0x12, IsCompressedUnicode = false },
            new("ForeignName", typeof(string), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
            new("RmtInfoShort", typeof(byte[]), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
            new("RmtInfoLong", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("Lv", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("LvProp", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("LvModule", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("LvExtra", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
        ];

    /// <summary>
    /// Internal table-creation helper that drives the same TDEF + leaf + catalog-row
    /// pipeline as <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, CancellationToken)"/>
    /// but accepts a pre-built <see cref="CatalogTableArtifact"/> so it can also
    /// emit hidden system tables (e.g. complex-column flat tables that need
    /// <c>MSysObjects.Flags = 0x800A0000</c>). Returns the new TDEF page number.
    /// </summary>
    /// <param name="tableArtifact">The catalog table artifact describing the table to create.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">Thrown when a reserved TDEF page is requested for a multi-page table definition.</exception>
    internal async ValueTask<long> CreateTableInternalAsync(
        CatalogTableArtifact tableArtifact,
        CancellationToken cancellationToken)
    {
        long[] tablePages = await this.ExecuteCatalogArtifactPlanAsync(
            new CatalogArtifactPlan([tableArtifact], []),
            cancellationToken).ConfigureAwait(false);

        return tablePages[0];
    }

    internal async ValueTask<long[]> ExecuteCatalogArtifactPlanAsync(
        CatalogArtifactPlan plan,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(plan, nameof(plan));

        long[] tablePages = new long[plan.TableArtifacts.Count];
        for (int artifactIndex = 0; artifactIndex < plan.TableArtifacts.Count; artifactIndex++)
        {
            CatalogTableArtifact tableArtifact = plan.TableArtifacts[artifactIndex];
            tablePages[artifactIndex] = await this.CreateCatalogTableArtifactAsync(tableArtifact, cancellationToken).ConfigureAwait(false);
        }

        for (int artifactIndex = 0; artifactIndex < plan.CatalogObjects.Count; artifactIndex++)
        {
            CatalogObjectArtifact catalogObject = plan.CatalogObjects[artifactIndex];
            _ = await this.catalogWriter.InsertCatalogObjectAsync(catalogObject, cancellationToken).ConfigureAwait(false);
        }

        for (int artifactIndex = 0; artifactIndex < plan.CatalogReplacements.Count; artifactIndex++)
        {
            UserTableCatalogReplacementArtifact replacement = plan.CatalogReplacements[artifactIndex];
            _ = await this.catalogWriter.ReplaceUserTableCatalogEntryAsync(
                replacement.ExistingName,
                replacement.ReplacementName,
                replacement.TDefPage,
                replacement.LvProp,
                replacement.IncludeSystemTables,
                replacement.Operation ?? $"replacing catalog row for '{replacement.ExistingName}'",
                replacement.MissingMessage,
                cancellationToken).ConfigureAwait(false);
        }

        for (int artifactIndex = 0; artifactIndex < plan.CatalogDeletions.Count; artifactIndex++)
        {
            UserTableCatalogDeletionArtifact deletion = plan.CatalogDeletions[artifactIndex];
            _ = await this.catalogWriter.DeleteUserTableCatalogRowsAsync(
                deletion.TableName,
                deletion.TDefPage,
                deletion.IncludeSystemTables,
                deletion.ThrowIfNotFound,
                deletion.Operation ?? $"deleting catalog row for '{deletion.TableName}'",
                deletion.MissingMessage,
                cancellationToken).ConfigureAwait(false);
        }

        if (plan.CatalogObjects.Count > 0 || plan.CatalogReplacements.Count > 0 || plan.CatalogDeletions.Count > 0)
        {
            this.InvalidateCatalogCache();
        }

        return tablePages;
    }

    private async ValueTask<long> CreateCatalogTableArtifactAsync(CatalogTableArtifact tableArtifact, CancellationToken cancellationToken)
    {
        TableDef tableDef = TDefPageBuilder.BuildTableDefinition(tableArtifact.Columns, this.Format);
        List<ResolvedIndex> resolvedIndexes = IndexHelpers.ResolveIndexes(tableArtifact.Indexes, tableDef);
        (byte[][] tdefPages, int[] firstDpLogicalOffsets, int[] usedPagesLogicalOffsets) = this.tdefPageBuilder.BuildTDefPagesWithIndexOffsets(tableDef, resolvedIndexes);
        if (tableArtifact.ReservedTdefPageNumber > 0 && tdefPages.Length != 1)
        {
            throw new InvalidDataException("Reserved fresh system-table TDEF slots support only single-page TDEFs.");
        }

        if (tableArtifact.MarkSystemTableTdef && IsSystemCatalogFlags(tableArtifact.CatalogFlags))
        {
            tdefPages[0][this.TDef.NumCols - 5] = 0x53;
        }

        // Reserve all TDEF pages first (sequential page numbers). The first
        // page's number is the table's catalog ID; subsequent pages are
        // chained via the next-page pointer at offset 4 of each non-last
        // page. Leaf pages and the usage-map page are allocated AFTER, so
        // they don't interleave with the TDEF chain and the page numbers
        // stay contiguous (tdefPages[i] lives at file page tdefPageNumber + i).
        long tdefPageNumber = tableArtifact.ReservedTdefPageNumber > 0
            ? tableArtifact.ReservedTdefPageNumber
            : await this.pageAllocator.ReserveContiguousPagesAsync(tdefPages.Length, cancellationToken).ConfigureAwait(false);

        // Stamp the next-page pointer at offset 4 of every non-last TDEF page.
        for (int pageIndex = 0; pageIndex < tdefPages.Length - 1; pageIndex++)
        {
            Wi32(tdefPages[pageIndex], 4, checked((int)(tdefPageNumber + pageIndex + 1)));
        }

        for (int pageIndex = 0; pageIndex < tdefPages.Length; pageIndex++)
        {
            await this.WritePageAsync(tdefPageNumber + pageIndex, tdefPages[pageIndex], cancellationToken).ConfigureAwait(false);
        }

        bool tdefDirty = false;

        long[]? leafPageNumbers = null;

        // Emit one empty index leaf page per real index and patch its page
        // number into the corresponding `first_dp` field of the real-idx physical
        // descriptor. The leaf starts empty because CreateTableAsync inserts no
        // rows; subsequent inserts/updates/deletes maintain the B-tree via
        // MaintainIndexesAsync. See
        // docs/design/index-and-relationship-format-notes.md §7.
        if (resolvedIndexes.Count > 0)
        {
            var layout = IndexPageLayout.ForFormat(this.Format);
            leafPageNumbers = new long[resolvedIndexes.Count];

            for (int indexIndex = 0; indexIndex < resolvedIndexes.Count; indexIndex++)
            {
                byte[] leafPage = IndexPageCodec.BuildLeafPage(
                    layout,
                    this.PageSizeBytes,
                    tdefPageNumber,
                    [],
                    enablePrefixCompression: false);
                long leafPageNumber = await this.pageAllocator.AllocatePageAsync(leafPage, cancellationToken).ConfigureAwait(false);
                leafPageNumbers[indexIndex] = leafPageNumber;
                this.tdefPageBuilder.WriteLogicalTDefI32(tdefPages, firstDpLogicalOffsets[indexIndex], checked((int)leafPageNumber));
            }

            tdefDirty = true;
        }

        // Allocate a per-table usage-map data page and patch the TDEF
        // `used_pages` / `free_pages` pointers (Jet4/ACE only). DAO Compact
        // & Repair walks every catalog row and dereferences `used_pages` to
        // enumerate the table's data pages; a zero pointer here aborts the
        // walk with "could not find object 'MSysDb'". The companion
        // `autonum_flag` byte at TDEF offset 0x18 is patched unconditionally
        // to 0x01: per Jackcess and verified empirically against
        // NorthwindTraders.accdb (every user table has byte 0x18 == 0x01,
        // including ones without an autonumber column). See
        // docs/design/round-trip-openrecordset-hypothesis.md.
        if (tableArtifact.EmitUsageMap && this.Format != DatabaseFormat.Jet3Mdb)
        {
            long usageMapPageNumber = await this.dataPageInserter.AppendUsageMapPageAsync(cancellationToken).ConfigureAwait(false);

            if (leafPageNumbers is not null)
            {
                await this.UpdateTableIndexUsageMapRowsAsync(
                    usageMapPageNumber,
                    DataPageInserter.ToSinglePageGroups(leafPageNumbers),
                    cancellationToken).ConfigureAwait(false);

                for (int usedPagesIndex = 0; usedPagesIndex < usedPagesLogicalOffsets.Length; usedPagesIndex++)
                {
                    int usedPagesOffset = usedPagesLogicalOffsets[usedPagesIndex];
                    tdefPages[usedPagesOffset / this.PageSizeBytes][usedPagesOffset % this.PageSizeBytes] = checked((byte)(usedPagesIndex + 2));
                    this.tdefPageBuilder.WriteLogicalTDefUInt24(tdefPages, usedPagesOffset + 1, checked((int)usageMapPageNumber));
                }
            }

            // PatchUsageMapPointers / PatchAutoNumFlag write only into the
            // TDEF header (offsets 0x18, 0x37..0x3F), which always live on
            // the first physical page.
            DataPageInserter.PatchUsageMapPointers(tdefPages[0], checked((int)usageMapPageNumber));
            DataPageInserter.PatchAutoNumFlag(tdefPages[0], tableDef);
            this.RegisterOwnedMapWritableTdef(tdefPageNumber);
            tdefDirty = true;
        }

        if (tdefDirty)
        {
            // Re-flush every TDEF page with the patched first_dp / usage-map /
            // autonum bytes and (for multi-page chains) the next-page pointers.
            for (int pageIndex = 0; pageIndex < tdefPages.Length; pageIndex++)
            {
                await this.WritePageAsync(tdefPageNumber + pageIndex, tdefPages[pageIndex], cancellationToken).ConfigureAwait(false);
            }
        }

        byte[]? lvProp = tableArtifact.EmitLvProp ? JetExpressionConverter.BuildLvPropBlob(tableArtifact.Columns, this.Format) : null;
        await this.catalogWriter.InsertCatalogEntryAsync(
            tableArtifact.TableName,
            tdefPageNumber,
            lvProp,
            tableArtifact.CatalogFlags,
            cancellationToken).ConfigureAwait(false);

        // DAO Compact & Repair requires every user table to have ACE
        // (Access Control Entry) rows in MSysACEs. Without them DAO's
        // security-descriptor pass aborts with err 3011 "MSysDb".
        if (ShouldEmitAceRows(tableArtifact))
        {
            await this.catalogWriter.InsertAceRowsForTableAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        }

        if (tableArtifact.RegisterConstraints)
        {
            this.Constraints.Register(tableArtifact.TableName, tableArtifact.Columns);
        }

        this.InvalidateCatalogCache();
        return tdefPageNumber;
    }

    private static bool ShouldEmitAceRows(CatalogTableArtifact tableArtifact)
        => tableArtifact.EmitAceRows ?? !IsSystemCatalogFlags(tableArtifact.CatalogFlags);

    private static bool IsSystemCatalogFlags(uint catalogFlags)
        => (catalogFlags & Constants.SystemObjects.SystemTableMask) != 0;

    /// <inheritdoc/>
    public ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.DropTableEntryAsync(tableName, cancellationToken), cancellationToken);

    /// <summary>
    /// Overwrites every page currently marked free in the Access global page
    /// allocation map. This is a maintenance operation; it does not move live
    /// pages or change table contents.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of free pages scrubbed.</returns>
    public ValueTask<int> ScrubFreePagesAsync(CancellationToken cancellationToken = default)
    {
        this.ThrowIfDisposedOrCancelled(cancellationToken);
        return this.pageAllocator.ScrubFreePagesAsync(cancellationToken);
    }

    /// <summary>
    /// Truncates globally-free pages from the physical end of the database file.
    /// This is a tail shrinker, not a full Microsoft Access Compact &amp; Repair
    /// rebuild: live pages keep their existing page numbers.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of pages removed from the end of the file.</returns>
    public ValueTask<long> ShrinkDatabaseAsync(CancellationToken cancellationToken = default)
    {
        this.ThrowIfDisposedOrCancelled(cancellationToken);
        return this.pageAllocator.ShrinkDatabaseAsync(cancellationToken);
    }

    private async ValueTask DropTableEntryAsync(string tableName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        await this.DropTableCoreAsync(tableName, dropComplexChildren: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.AddColumnCoreAsync(tableName, column, cancellationToken), cancellationToken);

    private ValueTask AddColumnCoreAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(column, nameof(column));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        return this.RewriteTableAsync(
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
                object[] next = new object[oldRow.Length + 1];
                Array.Copy(oldRow, 0, next, 0, oldRow.Length);
                next[oldRow.Length] = DBNull.Value;
                return next;
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask DropColumnAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.DropColumnCoreAsync(tableName, columnName, cancellationToken), cancellationToken);

    private ValueTask DropColumnCoreAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        int dropIndex = -1;
        return this.RewriteTableAsync(
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
                object[] next = new object[oldRow.Length - 1];
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
        => this.RunAutoCommitAsync(_ => this.RenameColumnCoreAsync(tableName, oldColumnName, newColumnName, cancellationToken), cancellationToken);

    private ValueTask RenameColumnCoreAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(oldColumnName, nameof(oldColumnName));
        Guard.NotNullOrEmpty(newColumnName, nameof(newColumnName));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        return this.RewriteTableAsync(
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
                    IsDateTimeExtended = src.IsDateTimeExtended,
                    ValidationRule = src.ValidationRule,
                    DefaultValueExpression = src.DefaultValueExpression,
                    ValidationRuleExpression = src.ValidationRuleExpression,
                    ValidationText = src.ValidationText,
                    Description = src.Description,

                    // Forward complex-column flags so the rebuilt TDEF re-emits
                    // a complex descriptor with the original ComplexId in the
                    // misc slot. RewriteTableAsync uses the preserved ComplexId
                    // to update MSysComplexColumns.ColumnName.
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
                    if (idx.Kind is not IndexKind.Normal and not IndexKind.PrimaryKey)
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
                            IsUnique = idx.HasUniqueFlag,
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
    public ValueTask InsertRowAsync(string tableName, object?[] values, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.InsertRowEntryAsync(tableName, NormalizePublicRow(values, nameof(values)), cancellationToken), cancellationToken);

    private async ValueTask InsertRowEntryAsync(string tableName, object[] values, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(values, nameof(values));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        _ = await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            SingleItem(values),
            static (_, row) => row,
            nameof(values),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object?[]> rows, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.InsertRowsCoreAsync(tableName, rows, cancellationToken), cancellationToken);

    private async ValueTask<int> InsertRowsCoreAsync(string tableName, IEnumerable<object?[]> rows, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(rows, nameof(rows));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            rows,
            static (_, row) => NormalizePublicRow(row, nameof(rows)),
            nameof(rows),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask InsertRowAsync<T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new()
        => this.RunAutoCommitAsync(_ => this.InsertRowGenericCoreAsync(tableName, item, cancellationToken), cancellationToken);

    private async ValueTask InsertRowGenericCoreAsync<T>(string tableName, T item, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(item, nameof(item));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        _ = await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            SingleItem(item),
            static (tableDef, row) => RowMapper<T>.ToRow(tableDef, row),
            nameof(item),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new()
        => this.RunAutoCommitAsync(_ => this.InsertRowsGenericCoreAsync(tableName, items, cancellationToken), cancellationToken);

    private async ValueTask<int> InsertRowsGenericCoreAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(items, nameof(items));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            items,
            static (tableDef, item) => RowMapper<T>.ToRow(tableDef, item),
            nameof(items),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask InsertRowAsync(string tableName, RowValues row, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.InsertNamedRowEntryAsync(tableName, row, cancellationToken), cancellationToken);

    private async ValueTask InsertNamedRowEntryAsync(string tableName, RowValues row, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(row, nameof(row));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        _ = await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            SingleItem(row),
            (tableDef, named) => ResolveNamedRow(tableDef, tableName, named, nameof(row)),
            nameof(row),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<RowValues> rows, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.InsertNamedRowsCoreAsync(tableName, rows, cancellationToken), cancellationToken);

    private async ValueTask<int> InsertNamedRowsCoreAsync(string tableName, IEnumerable<RowValues> rows, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(rows, nameof(rows));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        return await this.InsertMappedRowsAfterValidationAsync(
            tableName,
            rows,
            (tableDef, named) => ResolveNamedRow(tableDef, tableName, named, nameof(rows)),
            nameof(rows),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<int> UpdateRowsAsync(string tableName, RowCriteria criteria, RowValues updatedValues, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.UpdateRowsCoreAsync(tableName, criteria, updatedValues, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object?> updatedValues, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        return this.RunAutoCommitAsync(
            _ => this.UpdateRowsCoreAsync(
                tableName,
                RowCriteria.Where(predicateColumn, predicateValue),
                new RowValues(updatedValues),
                cancellationToken),
            cancellationToken);
    }

    private async ValueTask<int> UpdateRowsCoreAsync(string tableName, RowCriteria criteria, RowValues updatedValues, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        if (updatedValues.Count == 0)
        {
            return 0;
        }

        CatalogEntry entry = await this.GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await this.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        var predicate = RowCriteriaEvaluator.Compile(criteria, tableDef, tableName, nameof(criteria));

        var updateIndexes = new Dictionary<int, object?>();
        foreach (KeyValuePair<string, object?> kvp in updatedValues)
        {
            int columnIndex = tableDef.FindColumnIndex(kvp.Key);
            if (columnIndex < 0)
            {
                throw new ArgumentException($"Column '{kvp.Key}' was not found in table '{tableName}'.", nameof(updatedValues));
            }

            updateIndexes[columnIndex] = kvp.Value;
        }

        using DataTable snapshot = await this.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        List<RowLocation> locations = await this.GetLiveRowLocationsAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        int total = Math.Min(snapshot.Rows.Count, locations.Count);

        // FK enforcement: build the list of new-row payloads up front so we
        // can validate FK constraints (FK-side parent presence, PK-side
        // cascade-or-reject) before mutating any disk page.
        IReadOnlyList<FkRelationship> rels = await this.Relationships.Enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkCtx = rels.Count > 0 ? new FkContext(rels) : null;

        var pendingNewRows = new List<(int Index, object[] NewRow)>();
        for (int i = 0; i < total; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object[] rowValues = GetDbNullNormalizedItemArray(snapshot.Rows[i]);
            if (!predicate.Matches(rowValues))
            {
                continue;
            }

            foreach (KeyValuePair<int, object?> update in updateIndexes)
            {
                rowValues[update.Key] = update.Value ?? DBNull.Value;
            }

            await this.Constraints.ApplyCalculatedAsync(tableName, tableDef, rowValues, force: true, cancellationToken).ConfigureAwait(false);

            pendingNewRows.Add((i, rowValues));
        }

        if (fkCtx != null && pendingNewRows.Count > 0)
        {
            // FK-side: every updated row must (still) satisfy any FK constraint
            // whose foreign side is THIS table.
            foreach ((_, object[] newRow) in pendingNewRows)
            {
                await this.Relationships.Enforcer.EnforceFkOnInsertAsync(tableName, tableDef, newRow, fkCtx, cancellationToken).ConfigureAwait(false);
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

                int[] pkIdx = new int[rel.PrimaryColumns.Count];
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
                    string? oldKey = RelationshipKeyBuilder.Build(oldFullRow, pkIdx);
                    changes.Add((oldKey, oldFullRow, newRow));
                }

                await this.Relationships.Enforcer.EnforceFkOnPrimaryUpdateAsync(tableName, tableDef, changes, fkCtx, depth: 0, cancellationToken).ConfigureAwait(false);
            }
        }

        // Pre-write unique-index enforcement: after FK checks succeed,
        // validate that the post-update key set contains no duplicates for
        // any unique index. The check sees the snapshot with pendingNewRows
        // substituted at their original indices.
        if (pendingNewRows.Count > 0)
        {
            await this.uniqueIndexChecker.CheckUniqueIndexesPreUpdateAsync(entry.TDefPage, tableDef, tableName, snapshot, pendingNewRows, cancellationToken).ConfigureAwait(false);
        }

        int updated = 0;
        var updateInsertedHints = new List<(RowLocation Loc, object[] Row)>(pendingNewRows.Count);
        var updateDeletedHints = new List<(RowLocation Loc, object[] Row)>(pendingNewRows.Count);
        foreach ((int i, object[] rowValues) in pendingNewRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object[] oldRow = GetDbNullNormalizedItemArray(snapshot.Rows[i]);
            await this.MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, tableDef, cancellationToken).ConfigureAwait(false);
            updateDeletedHints.Add((locations[i], oldRow));
            RowLocation newLoc = await this.InsertRowDataLocAsync(entry.TDefPage, tableDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            updateInsertedHints.Add((newLoc, rowValues));
            updated++;
        }

        if (updated > 0)
        {
            bool incremental = await this.indexMaintainer.TryMaintainIndexesIncrementalAsync(
                entry.TDefPage,
                tableDef,
                updateInsertedHints,
                updateDeletedHints,
                cancellationToken).ConfigureAwait(false);
            if (!incremental)
            {
                await this.indexMaintainer.MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
            }
        }

        return updated;
    }

    /// <inheritdoc/>
    public ValueTask<int> DeleteRowsAsync(string tableName, RowCriteria criteria, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.DeleteRowsCoreAsync(tableName, criteria, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        return this.RunAutoCommitAsync(
            _ => this.DeleteRowsCoreAsync(tableName, RowCriteria.Where(predicateColumn, predicateValue), cancellationToken),
            cancellationToken);
    }

    private async ValueTask<int> DeleteRowsCoreAsync(string tableName, RowCriteria criteria, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(criteria, nameof(criteria));
        this.ThrowIfDisposedOrCancelled(cancellationToken);

        CatalogEntry entry = await this.GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await this.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        var predicate = RowCriteriaEvaluator.Compile(criteria, tableDef, tableName, nameof(criteria));

        using DataTable snapshot = await this.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        List<RowLocation> locations = await this.GetLiveRowLocationsAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
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
            object[] rowValues = GetDbNullNormalizedItemArray(snapshot.Rows[i]);
            if (predicate.Matches(rowValues))
            {
                matchingIndices.Add(i);
            }
        }

        IReadOnlyList<FkRelationship> rels = await this.Relationships.Enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
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

            await this.Relationships.Enforcer.EnforceFkOnPrimaryDeleteAsync(
                tableName,
                tableDef,
                deletedParentRows,
                fkCtx,
                depth: 0,
                cancellationToken).ConfigureAwait(false);
        }

        // Cascade flat-child rows for any complex columns on the parent
        // BEFORE we mark the parent rows deleted (we need to read the
        // parent's per-row complex-reference slots while the rows are still live).
        if (matchingIndices.Count > 0)
        {
            var parentLocs = new List<RowLocation>(matchingIndices.Count);
            foreach (int i in matchingIndices)
            {
                parentLocs.Add(locations[i]);
            }

            await this.ComplexColumns.CascadeDeleteComplexChildrenAsync(tableDef, parentLocs, cancellationToken).ConfigureAwait(false);
        }

        int deleted = 0;
        var deleteHints = new List<(RowLocation Loc, object[] Row)>(matchingIndices.Count);
        foreach (int i in matchingIndices)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object[] oldRow = GetDbNullNormalizedItemArray(snapshot.Rows[i]);
            await this.MarkRowDeletedAsync(locations[i].PageNumber, locations[i].RowIndex, tableDef, cancellationToken).ConfigureAwait(false);
            deleteHints.Add((locations[i], oldRow));
            deleted++;
        }

        if (deleted > 0)
        {
            await this.AdjustTDefRowCountAsync(entry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
            bool incremental = await this.indexMaintainer.TryMaintainIndexesIncrementalAsync(
                entry.TDefPage,
                tableDef,
                insertedRows: null,
                deleteHints,
                cancellationToken).ConfigureAwait(false);
            if (!incremental)
            {
                await this.indexMaintainer.MaintainIndexesAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
            }
        }

        return deleted;
    }

    /// <summary>
    /// Asynchronously creates a linked-table entry (MSysObjects type 6) that references
    /// a table in another Access database. No row data is stored locally; readers follow
    /// the entry to <paramref name="sourceDatabasePath"/> on demand.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDatabasePath">Path to the source Access database file (.mdb / .accdb).</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask CreateLinkedTableAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken = default)
        => LinkedTableManager.CreateLinkedTableAsync(this, linkedTableName, sourceDatabasePath, foreignTableName, cancellationToken);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) that references
    /// a table accessible via an ODBC connection. No row data is stored locally; managed
    /// readers expose the catalog metadata but do not open the ODBC source. Because this
    /// overload receives no source columns, it writes a table-level <c>LvProp</c>
    /// property block but cannot cache the remote column schema. Use the source-column
    /// overload for generated column-level metadata, or the <c>cachedSchemaLvProp</c>
    /// overload when byte-for-byte Access/DAO-authored metadata is required.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask CreateLinkedOdbcTableAsync(string linkedTableName, string connectionString, string foreignTableName, CancellationToken cancellationToken = default)
        => LinkedTableManager.CreateLinkedOdbcTableAsync(this, linkedTableName, connectionString, foreignTableName, sourceColumns: null, cancellationToken);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) and
    /// generates a cached-schema <c>MSysObjects.LvProp</c> property block from
    /// the supplied remote column definitions.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="sourceColumns">Column definitions for the remote source table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask CreateLinkedOdbcTableAsync(
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        IReadOnlyList<ColumnDefinition> sourceColumns,
        CancellationToken cancellationToken = default)
        => LinkedTableManager.CreateLinkedOdbcTableAsync(this, linkedTableName, connectionString, foreignTableName, sourceColumns, cancellationToken);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) using
    /// a caller-supplied Access/DAO cached-schema payload for <c>MSysObjects.LvProp</c>.
    /// The payload must come from an Access-compatible ODBC link to the same source
    /// schema; the writer validates that it is a non-empty <c>MR2\0</c> / <c>KKD\0</c>
    /// property block and stores it verbatim.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cachedSchemaLvProp">Access/DAO-authored cached linked-schema payload for <c>MSysObjects.LvProp</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask CreateLinkedOdbcTableAsync(
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        ReadOnlyMemory<byte> cachedSchemaLvProp,
        CancellationToken cancellationToken = default)
        => LinkedTableManager.CreateLinkedOdbcTableAsync(this, linkedTableName, connectionString, foreignTableName, cachedSchemaLvProp, cancellationToken);

    /// <summary>
    /// Asynchronously creates a linked-text/CSV table entry (MSysObjects type 6) that references
    /// a text or CSV file in a directory. The entry stores both a <c>Database</c> path (the
    /// directory containing the file) and a <c>Connect</c> string (e.g.
    /// <c>"Text;HDR=YES;FMT=Delimited"</c>). No row data is stored locally; managed
    /// readers parse supported delimited text sources on demand through the
    /// linked-source path policy and expose fields as strings.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDirectoryPath">Path to the directory containing the text/CSV source file.</param>
    /// <param name="foreignFileName">The filename of the text/CSV source (e.g. <c>"data.csv"</c>).</param>
    /// <param name="connectString">The text-driver connect string (e.g. <c>"Text;HDR=YES;FMT=Delimited"</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask CreateLinkedTextTableAsync(string linkedTableName, string sourceDirectoryPath, string foreignFileName, string connectString, CancellationToken cancellationToken = default)
        => LinkedTableManager.CreateLinkedTextTableAsync(this, linkedTableName, sourceDirectoryPath, foreignFileName, connectString, cancellationToken);

    // ════════════════════════════════════════════════════════════════
    // Foreign-key relationships — thin forwarders to RelationshipManager
    // ════════════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => this.Relationships.CreateRelationshipAsync(relationship, cancellationToken);

    /// <inheritdoc/>
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
        => this.Relationships.DropRelationshipAsync(relationshipName, cancellationToken);

    /// <inheritdoc/>
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
        => this.Relationships.RenameRelationshipAsync(oldName, newName, cancellationToken);

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
    /// <see cref="EncryptAsync(string, ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, CancellationToken)"/>
    /// to add encryption to an unencrypted database, or
    /// <see cref="DecryptAsync(string, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>
    /// to remove it.
    /// </summary>
    /// <param name="path">Path to an existing encrypted .mdb or .accdb file.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="newPassword">The new password (must be non-empty). Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring; the password fields are ignored.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="UnauthorizedAccessException">The supplied <paramref name="oldPassword"/> is wrong, or the database is unencrypted.</exception>
    /// <exception cref="ArgumentException"><paramref name="newPassword"/> is empty.</exception>
    public static ValueTask ChangePasswordAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.ChangePasswordAsync(path, oldPassword, newPassword, options, cancellationToken);

    /// <summary>
    /// Encrypts a currently-unencrypted JET / ACE database in place, applying
    /// <paramref name="targetFormat"/> when supplied or the best supported
    /// password encryption for the database format when omitted.
    /// </summary>
    /// <param name="path">Path to an existing unencrypted .mdb or .accdb file.</param>
    /// <param name="newPassword">The password to apply (must be non-empty). Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="targetFormat">The encryption format to use. When <see langword="null"/>, Jet4 <c>.mdb</c> files use <see cref="AccessEncryptionFormat.Jet4Rc4"/> and ACE <c>.accdb</c> files use <see cref="AccessEncryptionFormat.AccdbAgile"/>.</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="newPassword"/> is empty,
    /// <paramref name="targetFormat"/> is <see cref="AccessEncryptionFormat.None"/>,
    /// or the format is not valid for the underlying file kind.
    /// </exception>
    /// <exception cref="InvalidOperationException">The file is already encrypted.</exception>
    public static ValueTask EncryptAsync(
        string path,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.EncryptAsync(path, newPassword, targetFormat, options, cancellationToken);

    /// <summary>
    /// Removes encryption from a JET / ACE database in place, leaving an
    /// unencrypted file with no header password residue.
    /// </summary>
    /// <param name="path">Path to an existing encrypted .mdb or .accdb file.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="UnauthorizedAccessException">The supplied <paramref name="oldPassword"/> is wrong.</exception>
    /// <exception cref="InvalidOperationException">The file is already unencrypted.</exception>
    public static ValueTask DecryptAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.DecryptAsync(path, oldPassword, options, cancellationToken);

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="ChangePasswordAsync(string, ReadOnlyMemory{char}, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>.
    /// The stream must be readable, writable, and seekable; it is rewritten
    /// in place (length may change for Agile transitions).
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the database bytes.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="newPassword">The new password (must be non-empty). Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask ChangePasswordAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        CancellationToken cancellationToken = default)
        => EncryptionManager.ChangePasswordAsync(stream, oldPassword, newPassword, cancellationToken);

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="EncryptAsync(string, ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the unencrypted database bytes.</param>
    /// <param name="newPassword">The password to apply. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="targetFormat">The encryption format to use. When <see langword="null"/>, Jet4 <c>.mdb</c> files use <see cref="AccessEncryptionFormat.Jet4Rc4"/> and ACE <c>.accdb</c> files use <see cref="AccessEncryptionFormat.AccdbAgile"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask EncryptAsync(
        Stream stream,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.EncryptAsync(stream, newPassword, targetFormat, cancellationToken);

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="DecryptAsync(string, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the encrypted database bytes.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask DecryptAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
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
        => this.transactionLifecycle.BeginTransactionAsync(cancellationToken);

    /// <summary>
    /// If <see cref="AccessWriterOptions.UseTransactionalWrites"/> is enabled
    /// and no explicit transaction is currently active, wraps
    /// <paramref name="work"/> in a private <see cref="JetTransaction"/> so a
    /// crash mid-call leaves the database in its pre-call state. Otherwise
    /// invokes <paramref name="work"/> directly using the flush-per-page path.
    /// </summary>
    /// <param name="work">The work to execute.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
        => this.transactionLifecycle.RunAutoCommitAsync(work, cancellationToken);

    /// <summary>
    /// Generic-result variant of <see cref="RunAutoCommitAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="TResult">The result type produced by <paramref name="work"/>.</typeparam>
    /// <param name="work">The work to execute.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
        => this.transactionLifecycle.RunAutoCommitAsync(work, cancellationToken);

    /// <summary>
    /// Commits the supplied <paramref name="transaction"/>: detaches the
    /// journal from the writer and replays each buffered page (in ascending
    /// page-number order) through the normal page-write pipeline so that
    /// per-page encryption and cooperative byte-range locks are honoured.
    /// </summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask CommitTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
        => this.transactionLifecycle.CommitTransactionAsync(transaction, cancellationToken);

    /// <summary>
    /// Rolls back the supplied <paramref name="transaction"/>: discards the
    /// in-memory journal without touching the database file.
    /// </summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask RollbackTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
        => this.transactionLifecycle.RollbackTransactionAsync(transaction, cancellationToken);

    /// <inheritdoc/>
    [SuppressMessage("Usage", "CA2215:Dispose methods should call base class dispose", Justification = "base.DisposeAsync is passed as the final step to LockFileCoordinator.DisposeAfterAsync.")]
    public override async ValueTask DisposeAsync()
    {
        if (this.IsDisposed)
        {
            return;
        }

        // The coordinator drains every step in order, aggregates failures,
        // and unconditionally releases the .ldb / .laccdb slot last.
        // Lock-file release runs after the agile re-wrap so the lock-file
        // accurately reflects "database still in use" while we re-encrypt.
        await this.lockFileCoordinator.DisposeAfterAsync(
            this.DisposeActiveTransactionAsync,
            this.RewrapAndCloseOuterEncryptedStreamAsync,
            this.DisposeStateLockAsync,
            base.DisposeAsync).ConfigureAwait(false);
    }

    private async ValueTask DisposeActiveTransactionAsync()
    {
        // Drop any in-flight transaction so its journal does not survive
        // dispose. Nothing has been written to disk for an uncommitted
        // transaction, so this is equivalent to an implicit rollback.
        if (this.ActiveTransaction is null)
        {
            return;
        }

        try
        {
            await this.ActiveTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            this.ActiveJournal = null;
            this.ActiveTransaction = null;
        }
    }

    private async ValueTask RewrapAndCloseOuterEncryptedStreamAsync()
    {
        // For Agile-encrypted databases the underlying _stream is an in-memory
        // copy of the *decrypted* ACCDB. Re-encrypt it before tearing down so
        // the user's outer encrypted stream/file ends up with all writes.
        if (!this.isAgileEncryptedRewrap || this.outerEncryptedStream is null || this.Options.Password.IsEmpty)
        {
            return;
        }

        try
        {
            await this.RewrapAgileOnDisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            if (!this.outerEncryptedLeaveOpen)
            {
                await this.outerEncryptedStream.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private ValueTask DisposeStateLockAsync()
    {
        this.stateLock.Dispose();
        return default;
    }

    /// <summary>
    /// Re-encrypts the in-memory decrypted ACCDB (held by <c>_stream</c>) using
    /// freshly-generated Office Crypto parameters and writes the resulting CFB
    /// document back to <see cref="outerEncryptedStream"/>. Called from
    /// <see cref="DisposeAsync"/> when the writer was opened on an Office Crypto
    /// .accdb file.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the encrypted writer does not have the required in-memory backing stream.</exception>
    private async ValueTask RewrapAgileOnDisposeAsync()
    {
        MemoryStream memory = this.DatabaseStream as MemoryStream
            ?? throw new InvalidOperationException("Agile-encrypted writer expected an in-memory backing stream.");

        byte[] inner = memory.ToArray();

        OfficeEncryptedPackage package = this.outerEncryptedFormat == AccessEncryptionFormat.AccdbStandard
            ? OfficeCryptoStandard.Encrypt(inner, this.Options.Password.Span)
            : OfficeCryptoAgile.Encrypt(inner, this.Options.Password.Span);

        byte[] cfb = EncryptionConverter.BuildOfficeCryptoCompoundFile(package);

        _ = this.outerEncryptedStream!.Seek(0, SeekOrigin.Begin);
        await this.outerEncryptedStream.WriteAsync(cfb.AsMemory()).ConfigureAwait(false);
        this.outerEncryptedStream.SetLength(cfb.Length);
        await this.outerEncryptedStream.FlushAsync().ConfigureAwait(false);
    }

    private static object[] NormalizePublicRow(object?[] values, string paramName)
    {
        Guard.NotNull(values, paramName);

        object[] normalized = new object[values.Length];
        Array.Copy(values, normalized, values.Length);
        NormalizeRowInPlace(normalized);
        return normalized;
    }

    /// <summary>
    /// Projects a named-column <see cref="RowValues"/> onto a positional
    /// <c>object[]</c> in table-column order. Columns not named in the row are
    /// left as <see cref="DBNull.Value"/> so AutoNumber columns generate and any
    /// other omitted column stores database null. Unknown column names throw.
    /// </summary>
    /// <param name="tableDef">The target table definition.</param>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="row">The named-column values.</param>
    /// <param name="paramName">The public parameter name, for <see cref="ArgumentException"/>.</param>
    /// <returns>The positional row values.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="row"/> names a column not in the table.</exception>
    private static object[] ResolveNamedRow(TableDef tableDef, string tableName, RowValues row, string paramName)
    {
        Guard.NotNull(row, paramName);

        object[] values = new object[tableDef.Columns.Count];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = DBNull.Value;
        }

        foreach (KeyValuePair<string, object?> pair in row)
        {
            int columnIndex = tableDef.FindColumnIndex(pair.Key);
            if (columnIndex < 0)
            {
                throw new ArgumentException(
                    $"Column '{pair.Key}' was not found in table '{tableName}'.",
                    paramName);
            }

            values[columnIndex] = pair.Value ?? DBNull.Value;
        }

        return values;
    }

    internal static object[] GetDbNullNormalizedItemArray(DataRow row)
    {
        Guard.NotNull(row, nameof(row));

        object?[] values = row.ItemArray;
        NormalizeRowInPlace(values);
        return (object[])values;
    }

    private static void NormalizeRowInPlace(object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            values[i] ??= DBNull.Value;
        }
    }

    private static IEnumerable<TItem> SingleItem<TItem>(TItem item)
        where TItem : class
    {
        yield return item;
    }

    private async ValueTask<int> InsertMappedRowsAfterValidationAsync<TItem>(
        string tableName,
        IEnumerable<TItem> items,
        Func<TableDef, TItem, object[]> mapRow,
        string itemParamName,
        CancellationToken cancellationToken)
        where TItem : class
    {
        CatalogEntry entry = await this.GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await this.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<FkRelationship> relationships = await this.Relationships.Enforcer.GetEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        FkContext? fkContext = relationships.Count > 0 ? new FkContext(relationships) : null;

        (List<object[]> pendingRows, List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints) =
            await this.PrepareInsertBatchAsync(
                tableName,
                tableDef,
                items,
                mapRow,
                itemParamName,
                cancellationToken).ConfigureAwait(false);

        return await this.InsertPreparedBatchAsync(
            tableName,
            entry.TDefPage,
            tableDef,
            pendingRows,
            autoCheckpoints,
            fkContext,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<(List<object[]> PendingRows, List<(ColumnConstraint Constraint, long? PreviousValue)>? AutoCheckpoints)> PrepareInsertBatchAsync<TItem>(
        string tableName,
        TableDef tableDef,
        IEnumerable<TItem> items,
        Func<TableDef, TItem, object[]> mapRow,
        string itemParamName,
        CancellationToken cancellationToken)
        where TItem : class
    {
        var pendingRows = new List<object[]>();
        List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints = null;

        try
        {
            foreach (TItem item in items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Guard.NotNull(item, itemParamName);

                object[] row = mapRow(tableDef, item);
                List<(ColumnConstraint Constraint, long? PreviousValue)>? rowCheckpoints =
                    await this.Constraints.ApplyAsync(tableName, tableDef, row, cancellationToken).ConfigureAwait(false);
                if (rowCheckpoints != null)
                {
                    (autoCheckpoints ??= []).AddRange(rowCheckpoints);
                }

                pendingRows.Add(row);
            }
        }
        catch
        {
            ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
            throw;
        }

        return (pendingRows, autoCheckpoints);
    }

    private async ValueTask<int> InsertPreparedBatchAsync(
        string tableName,
        long tdefPage,
        TableDef tableDef,
        List<object[]> pendingRows,
        List<(ColumnConstraint Constraint, long? PreviousValue)>? autoCheckpoints,
        FkContext? fkContext,
        CancellationToken cancellationToken)
    {
        var batchLocations = new List<RowLocation>();
        var batchHintRows = new List<(RowLocation Loc, object[] Row)>();
        int inserted = 0;

        try
        {
            // AutoNumber values are already assigned, so the unique check sees
            // the exact keys the row writer and index maintainer will encode.
            await this.uniqueIndexChecker.CheckUniqueIndexesPreInsertAsync(
                tdefPage,
                tableDef,
                tableName,
                pendingRows,
                cancellationToken).ConfigureAwait(false);

            foreach (object[] row in pendingRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (fkContext != null)
                {
                    await this.Relationships.Enforcer.EnforceFkOnInsertAsync(tableName, tableDef, row, fkContext, cancellationToken).ConfigureAwait(false);
                }

                RowLocation location = await this.InsertRowDataLocAsync(tdefPage, tableDef, row, cancellationToken: cancellationToken).ConfigureAwait(false);
                batchLocations.Add(location);
                batchHintRows.Add((location, row));

                if (fkContext != null)
                {
                    RelationshipEnforcer.AugmentParentSetsAfterInsert(tableName, tableDef, row, fkContext);
                }

                inserted++;
            }

            if (inserted > 0)
            {
                bool incremental = await this.indexMaintainer.TryMaintainIndexesIncrementalAsync(
                    tdefPage,
                    tableDef,
                    batchHintRows,
                    deletedRows: null,
                    cancellationToken).ConfigureAwait(false);
                if (!incremental)
                {
                    await this.indexMaintainer.MaintainIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
                }

                await this.autoNumberMaintainer.UpdateHighWaterAsync(tdefPage, tableDef, pendingRows, cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            await this.RollbackInsertedRowsAsync(tdefPage, batchLocations, cancellationToken).ConfigureAwait(false);
            ConstraintRegistry.RestoreAutoCounters(autoCheckpoints);
            throw;
        }

        return inserted;
    }

    /// <summary>
    /// Marks every row in <paramref name="locations"/> as deleted on its data
    /// page and rewinds the owning TDEF's row count by the matching amount.
    /// Best-effort: any exception during rollback is swallowed so the original
    /// failure surfaces to the caller intact.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="locations">The locations.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask RollbackInsertedRowsAsync(long tdefPage, List<RowLocation> locations, CancellationToken cancellationToken)
    {
        if (locations.Count == 0)
        {
            return;
        }

        foreach (RowLocation loc in locations)
        {
            await this.MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, cancellationToken).ConfigureAwait(false);
        }

        await this.AdjustTDefRowCountAsync(tdefPage, -locations.Count, cancellationToken).ConfigureAwait(false);
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
            await using AccessReader reader = await AccessReader.OpenAsync(path, readerOptions, cancellationToken).ConfigureAwait(false);
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
            PageCacheSize = -1,
            Password = this.Options.Password,
        };

        AccessReader reader;
        if (!string.IsNullOrEmpty(this.DatabasePath) && !this.isAgileEncryptedRewrap)
        {
            reader = await AccessReader.OpenUncachedAsync(this.DatabasePath, options, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            this.DatabaseStream.Position = 0;
            reader = await AccessReader.OpenUncachedAsync(this.DatabaseStream, options, leaveOpen: true, cancellationToken).ConfigureAwait(false);
        }

        await using (reader)
        {
            return await reader.ReadDataTableForSchemaRewriteAsync(tableName, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Opens a transient <see cref="AccessReader"/> against the same backing file/stream
    /// to enumerate <paramref name="tableName"/>'s logical indexes via the same parser
    /// that <see cref="IAccessReader.ListIndexesAsync"/> uses. Used by
    /// <see cref="RewriteTableAsync"/> to forward existing index definitions through
    /// Add/Drop/Rename column operations.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<IReadOnlyList<IndexMetadata>> ReadIndexMetadataSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var options = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            PageCacheSize = -1,
            Password = this.Options.Password,
        };

        AccessReader reader;
        if (!string.IsNullOrEmpty(this.DatabasePath) && !this.isAgileEncryptedRewrap)
        {
            reader = await AccessReader.OpenUncachedAsync(this.DatabasePath, options, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            this.DatabaseStream.Position = 0;
            reader = await AccessReader.OpenUncachedAsync(this.DatabaseStream, options, leaveOpen: true, cancellationToken).ConfigureAwait(false);
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
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<ColumnPropertyBlock?> ReadLvPropBlockAsync(long tdefPage, CancellationToken cancellationToken)
    {
        var options = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            PageCacheSize = -1,
            UseLockFile = false,
            Password = this.Options.Password,
        };

        AccessReader reader;
        if (!string.IsNullOrEmpty(this.DatabasePath) && !this.isAgileEncryptedRewrap)
        {
            reader = await AccessReader.OpenUncachedAsync(this.DatabasePath, options, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            this.DatabaseStream.Position = 0;
            reader = await AccessReader.OpenUncachedAsync(this.DatabaseStream, options, leaveOpen: true, cancellationToken).ConfigureAwait(false);
        }

        await using (reader)
        {
            return await reader.ReadLvPropForTableAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        }
    }

    private protected override async ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken = default)
    {
        List<CatalogEntry>? cached = this.GetCatalogCache();
        if (cached != null)
        {
            return cached;
        }

        TableDef? msys = await this.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            var empty = new List<CatalogEntry>();
            this.SetCatalogCache(empty);
            return empty;
        }

        List<CatalogRow> rows = await this.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
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

        this.SetCatalogCache(result);
        return result;
    }

    internal async ValueTask<CatalogEntry> GetRequiredCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
        => await this.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"Table '{tableName}' was not found.");

    internal async ValueTask<TableDef> ReadRequiredTableDefAsync(long tdefPage, string tableName, CancellationToken cancellationToken = default)
        => await this.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");

    private async ValueTask RewriteTableAsync(
        string tableName,
        Func<List<ColumnDefinition>, TableDef, List<ColumnDefinition>> projectColumns,
        Func<object[], TableDef, object[]> projectRow,
        CancellationToken cancellationToken,
        Func<IReadOnlyList<IndexMetadata>, IReadOnlyList<ColumnDefinition>, List<IndexDefinition>>? projectIndexes = null)
    {
        CatalogEntry entry = await this.GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await this.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        // Carry forward any client-side constraints registered for the original schema so
        // Add/Drop/Rename do not silently strip NotNull / Default / AutoIncrement / validation rules.
        this.Constraints.TryGet(tableName, out List<ColumnConstraint>? existingConstraints);

        // Hydrate persisted-property fields from MSysObjects.LvProp so that
        // DefaultValueExpression / ValidationRuleExpression / ValidationText / Description
        // round-trip through Add/Drop/Rename semantically. Forward-compat note: unknown
        // chunks and table-level property targets are intentionally not preserved by this path.
        ColumnPropertyBlock? originalProperties =
            await this.ReadLvPropBlockAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

        var existingDefs = new List<ColumnDefinition>(tableDef.Columns.Count);
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            ColumnDefinition baseDef = this.BuildColumnDefinitionFromInfo(col, originalProperties);
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
                    DefaultValueExpression = target.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, this.Format)
                        ?? baseDef.DefaultValueExpression,
                    ValidationRuleExpression = target.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, this.Format)
                        ?? baseDef.ValidationRuleExpression,
                    ValidationText = target.GetTextValue(Constants.ColumnPropertyNames.ValidationText, this.Format)
                        ?? baseDef.ValidationText,
                    Description = target.GetTextValue(Constants.ColumnPropertyNames.Description, this.Format)
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
        using DataTable snapshot = await this.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<IndexMetadata> existingIndexes = await this.ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        // Default index projection: keep every existing index whose single key
        // column survives in the new schema (matched by case-insensitive name).
        // AddColumn / DropColumn use this default; RenameColumn supplies a custom
        // projection that rewrites references to the renamed column.
        List<IndexDefinition> projectedIndexes = projectIndexes != null
            ? projectIndexes(existingIndexes, newDefs)
            : IndexHelpers.DefaultIndexProjection(existingIndexes, newDefs);

        string tempName = $"~tmp_{Guid.NewGuid():N}"[..18];
        await this.CreateTableAsync(tempName, newDefs, projectedIndexes, cancellationToken).ConfigureAwait(false);

        CatalogEntry tempEntry = await this.GetRequiredCatalogEntryAsync(tempName, cancellationToken).ConfigureAwait(false);
        TableDef tempDef = await this.ReadRequiredTableDefAsync(tempEntry.TDefPage, tempName, cancellationToken).ConfigureAwait(false);

        foreach (DataRow row in snapshot.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object?[] sourceItems = row.ItemArray;
            object[] sourceRow = new object[sourceItems.Length];
            for (int i = 0; i < sourceItems.Length; i++)
            {
                sourceRow[i] = sourceItems[i] ?? DBNull.Value;
            }

            object[] projected = projectRow(sourceRow, tableDef);
            await this.InsertRowDataAsync(tempEntry.TDefPage, tempDef, projected, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        // rebuild forwarded indexes once after the bulk row copy completes,
        // so we don't pay the rebuild cost per row.
        if (projectedIndexes.Count > 0 && snapshot.Rows.Count > 0)
        {
            await this.indexMaintainer.MaintainIndexesAsync(tempEntry.TDefPage, tempDef, tempName, cancellationToken).ConfigureAwait(false);
        }

        // Drop the original table, then rename the temp catalog entry to take its place.
        // Pre-compute the LvProp blob from the projected columns so the catalog rename
        // re-emits the persisted properties under the user-facing table name.
        //
        // identify complex columns being dropped or renamed by this rewrite
        // BEFORE the cascade-skipping drop runs. Surviving complex columns (matched by
        // ComplexId between the existing and projected schemas) are preserved as-is —
        // their flat child tables and MSysComplexColumns rows stay attached to the
        // rebuilt parent. If no complex column is dropped or renamed, the temp table
        // is transplanted onto the original TDEF page so MSysComplexColumns keeps the
        // same parent object id; otherwise surviving rows are patched to the temp
        // TDEF page after the copy/swap. Dropped complex columns get their flat child
        // + catalog row removed surgically; renamed complex columns get their
        // MSysComplexColumns row rewritten with the new ColumnName.
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

        byte[]? renamedLvProp = JetExpressionConverter.BuildLvPropBlob(newDefs, this.Format);
        if (newComplexById.Count > 0 && droppedComplex.Count == 0 && renamedComplex.Count == 0)
        {
            await this.TransplantTempTableToOriginalAsync(
                tableName,
                entry.TDefPage,
                tempName,
                tempEntry.TDefPage,
                renamedLvProp,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        await this.DropTableCoreAsync(tableName, dropComplexChildren: false, cancellationToken).ConfigureAwait(false);
        await this.catalogWriter.RenameTableInCatalogAsync(tempName, tableName, renamedLvProp, cancellationToken).ConfigureAwait(false);

        foreach (ColumnDefinition survivor in newComplexById.Values)
        {
            await this.ComplexColumns.UpdateComplexColumnParentTableIdAsync(
                survivor.ComplexId,
                checked((int)tempEntry.TDefPage),
                cancellationToken).ConfigureAwait(false);
        }

        foreach ((string colName, int complexId) in droppedComplex)
        {
            await this.ComplexColumns.DropSingleComplexChildAsync(colName, complexId, cancellationToken).ConfigureAwait(false);
        }

        foreach ((string oldColName, string newColName, int complexId) in renamedComplex)
        {
            await this.ComplexColumns.RenameComplexColumnArtifactsAsync(oldColName, newColName, complexId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask TransplantTempTableToOriginalAsync(
        string tableName,
        long originalTdefPage,
        string tempName,
        long tempTdefPage,
        byte[]? lvProp,
        CancellationToken cancellationToken)
    {
        byte[] tempTdef = await this.ReadPageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (tempTdef[0] != Constants.PageTypes.TableDefinition || Ri32(tempTdef, 4) != 0)
            {
                throw new NotSupportedException("Complex table schema rewrite currently requires a single-page rebuilt TDEF.");
            }

            await this.ReclaimTableStoragePagesAsync(originalTdefPage, includeTDefRoot: false, cancellationToken).ConfigureAwait(false);
            await this.PatchTablePageOwnersAsync(tempTdefPage, originalTdefPage, cancellationToken).ConfigureAwait(false);
            await this.WritePageAsync(originalTdefPage, tempTdef, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(tempTdef);
        }

        await this.ExecuteCatalogArtifactPlanAsync(
            new CatalogArtifactPlan([], [])
            {
                CatalogReplacements =
                [
                    new UserTableCatalogReplacementArtifact(
                        tableName,
                        tableName,
                        originalTdefPage,
                        lvProp,
                        Operation: $"replacing catalog row for '{tableName}'",
                        MissingMessage: $"Catalog row for '{tableName}' was not found during schema rewrite."),
                ],
                CatalogDeletions =
                [
                    new UserTableCatalogDeletionArtifact(
                        tempName,
                        tempTdefPage,
                        Operation: $"deleting catalog row for '{tempName}'"),
                ],
            },
            cancellationToken).ConfigureAwait(false);
        await this.catalogWriter.DeleteAceRowsForObjectIdsAsync([tempTdefPage], cancellationToken).ConfigureAwait(false);
        await this.pageAllocator.DeallocatePageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask PatchTablePageOwnersAsync(long fromTdefPage, long toTdefPage, CancellationToken cancellationToken)
    {
        long totalPages = this.DatabaseStream.Length / this.PageSizeBytes;
        for (long pageNumber = 3; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                bool patchDataPage = page[0] == Constants.PageTypes.Data && Ri32(page, this.DataPage.TDefOff) == fromTdefPage;
                bool patchIndexPage = page[0] is Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf && Ri32(page, 4) == fromTdefPage;
                if (!patchDataPage && !patchIndexPage)
                {
                    continue;
                }

                int ownerOffset = patchDataPage ? this.DataPage.TDefOff : 4;
                Wi32(page, ownerOffset, checked((int)toTdefPage));
                await this.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ReturnPage(page);
            }
        }
    }

    private ColumnDefinition BuildColumnDefinitionFromInfo(ColumnInfo column, ColumnPropertyBlock? properties = null)
    {
        ColumnDefinition baseDef;
        switch (column.Type)
        {
            case TextType:
                int textSize = column.IsCalculated
                    ? Math.Max(0, column.Size - Constants.CalculatedColumn.ExtraDataLen)
                    : column.Size;
                int charLen = this.Format != DatabaseFormat.Jet3Mdb ? Math.Max(1, textSize / 2) : Math.Max(1, textSize);
                baseDef = new ColumnDefinition(column.Name, typeof(string), charLen);
                break;
            case BinaryType:
                int binarySize = column.IsCalculated
                    ? Math.Max(0, column.Size - Constants.CalculatedColumn.ExtraDataLen)
                    : column.Size;
                baseDef = new ColumnDefinition(column.Name, typeof(byte[]), binarySize > 0 ? binarySize : 255);
                break;
            case AttachmentType:
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
            case ComplexType:
                // Access stores all complex parent descriptors as the generic
                // 0x12 type; the subtype lives in MSysComplexColumns. This
                // rewrite path only needs a generic complex marker and the
                // preserved ComplexId, so the existing flat child table remains
                // attached without allocating a new one.
                return new ColumnDefinition(column.Name, typeof(byte[]))
                {
                    IsMultiValue = true,
                    ComplexId = column.Misc,
                };
            case BooleanType:
            case ByteType:
            case IntegerType:
            case LongIntegerType:
            case MoneyType:
            case FloatType:
            case DoubleType:
            case DateTimeType:
            case OleType:
            case MemoType:
            case GuidType:
            case NumericType:
            case BigIntType:
                Type clrType = GetClrType(column.Type)
                    ?? throw new NotSupportedException($"Column '{column.Name}' has unsupported type {GetTypeDisplayName(column.Type)}.");
                baseDef = new ColumnDefinition(column.Name, clrType)
                {
                    ColumnTypeOverride = column.Type,
                };
                break;
            case DateTimeExtendedType:
                baseDef = new ColumnDefinition(column.Name, typeof(DateTime))
                {
                    IsDateTimeExtended = true,
                    ColumnTypeOverride = column.Type,
                };
                break;
            default:
                throw new InvalidOperationException($"Column '{column.Name}' has unknown type {GetTypeDisplayName(column.Type)}.");
        }

        // Surface the persisted TDEF flag bits as ColumnDefinition properties so the
        // schema-rewrite path retains NOT NULL / auto-increment metadata that Access
        // wrote into the original column descriptor. Complex columns (Attachment /
        // Complex) return early above because their Flags byte is the magic 0x07
        // marker rather than real flag bits.
        bool isAutoIncrement = (column.Flags & Constants.ColumnDescriptorFlags.AutoNumber) != 0;
        bool? requiredFromLvProp = properties?.FindTarget(column.Name)?
            .GetBooleanValue(Constants.ColumnPropertyNames.Required);
        bool isNullable = !isAutoIncrement && (requiredFromLvProp is bool req
                ? !req
                : (column.Flags & Constants.ColumnDescriptorFlags.LegacyNotNull) == 0);

        ColumnDefinition def = baseDef with
        {
            IsNullable = isNullable,
            IsAutoIncrement = isAutoIncrement,
            IsHyperlink = column.Type == MemoType && (column.Flags & Constants.ColumnDescriptorFlags.Hyperlink) != 0,
            IsDateTimeExtended = column.Type == DateTimeExtendedType,
            IsCompressedUnicode = column.IsCompressedUnicode,
        };

        // Preserve declared precision/scale through the schema-rewrite copy so
        // AddColumn / DropColumn / RenameColumn don't silently reset a NUMERIC
        // column to default 18/0. Access-authored files always populate these
        // descriptor bytes for Numeric columns.
        if (column.Type == NumericType)
        {
            def = def with { NumericPrecision = column.NumericPrecision, NumericScale = column.NumericScale };
        }

        if (column.IsCalculated)
        {
            ColumnPropertyTarget? target = properties?.FindTarget(column.Name);
            byte resultType = (byte)column.Type;
            ColumnPropertyEntry? resultTypeEntry = target?.Find(Constants.ColumnPropertyNames.ResultType);
            if (resultTypeEntry?.Value.Length >= 1)
            {
                resultType = resultTypeEntry.Value[0];
            }

            def = def with
            {
                IsCalculated = true,
                CalculationExpression = target?.GetTextValue(Constants.ColumnPropertyNames.Expression, this.Format),
                CalculatedResultType = resultType,
                IsCompressedUnicode = false,
            };
        }

        return def;
    }

    /// <summary>
    /// Forwards real-index usage-map row maintenance to <see cref="DataPageInserter"/>,
    /// which owns usage-map page construction. Retained so the index maintainer can
    /// drive usage-map updates through the writer.
    /// </summary>
    /// <param name="usageMapPageNumber">The usage-map page number.</param>
    /// <param name="indexPageGroups">The per-index leaf-page groups.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask UpdateTableIndexUsageMapRowsAsync(long usageMapPageNumber, IReadOnlyList<long[]> indexPageGroups, CancellationToken cancellationToken)
        => this.dataPageInserter.UpdateTableIndexUsageMapRowsAsync(usageMapPageNumber, indexPageGroups, cancellationToken);

    // ── Row-level APIs for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §2.1 / §2.4 / §3.

    /// <inheritdoc/>
    public ValueTask AddAttachmentAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?> parentRowKey,
        AttachmentInput attachment,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        Guard.NotNull(attachment, nameof(attachment));
        return this.RunAutoCommitAsync(
            _ => this.ComplexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, attachment, expectAttachment: true, cancellationToken),
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask AddMultiValueItemAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?> parentRowKey,
        object? value,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        return this.RunAutoCommitAsync(
            _ => this.ComplexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, value, expectAttachment: false, cancellationToken),
            cancellationToken);
    }

    /// <summary>
    /// Shared implementation backing <see cref="DropTableAsync"/> and the
    /// <c>RewriteTableAsync</c> path. The <paramref name="dropComplexChildren"/>
    /// flag is set to <see langword="false"/> by the rewrite path so that the
    /// hidden flat child tables and matching <c>MSysComplexColumns</c> rows for
    /// surviving complex columns stay attached to the rebuilt parent.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="dropComplexChildren">The drop complex children.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <c>MSysObjects</c> is missing or no matching user table exists.</exception>
    private async ValueTask DropTableCoreAsync(string tableName, bool dropComplexChildren, CancellationToken cancellationToken)
    {
        UserTableCatalogDeletionResult deleted = await this.catalogWriter.DeleteUserTableCatalogRowsAsync(
            tableName,
            tdefPage: null,
            includeSystemTables: false,
            throwIfNotFound: true,
            operation: $"dropping table '{tableName}'",
            missingMessage: $"Table '{tableName}' does not exist.",
            cancellationToken).ConfigureAwait(false);

        if (dropComplexChildren)
        {
            foreach (long parentTdefPage in deleted.TDefPages)
            {
                await this.ComplexColumns.DropComplexChildrenForTableAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
            }
        }

        await this.catalogWriter.DeleteAceRowsForObjectIdsAsync(deleted.TDefPages, cancellationToken).ConfigureAwait(false);

        foreach (long tdefPage in deleted.TDefPages)
        {
            await this.ReclaimDroppedTablePagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        }

        this.Constraints.Unregister(tableName);
        this.InvalidateCatalogCache();
    }

    private ValueTask ReclaimDroppedTablePagesAsync(long tdefPage, CancellationToken cancellationToken)
        => this.ReclaimTableStoragePagesAsync(tdefPage, includeTDefRoot: true, cancellationToken);

    private async ValueTask ReclaimTableStoragePagesAsync(long tdefPage, bool includeTDefRoot, CancellationToken cancellationToken)
    {
        var pagesToFree = new SortedSet<long>();
        var longValueRoots = new List<LongValueDescriptor>();

        TableDef? tableDef = await this.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long totalPages = this.DatabaseStream.Length / this.PageSizeBytes;
        if (tableDef is not null)
        {
            await this.ForEachOwnedDataPageAsync(
                tdefPage,
                (pageNumber, page, _) =>
                {
                    pagesToFree.Add(pageNumber);
                    foreach (RowBound rowBound in this.EnumerateLiveRowBounds(page))
                    {
                        longValueRoots.AddRange(this.longValueEncoder.CollectLongValueRoots(page, rowBound, tableDef));
                    }

                    return new ValueTask<bool>(true);
                },
                cancellationToken).ConfigureAwait(false);
        }

        byte[]? firstTdefPage = null;
        var seenTdefPages = new HashSet<long>();
        long currentTdefPage = tdefPage;
        while (currentTdefPage > 0 && currentTdefPage < totalPages && seenTdefPages.Add(currentTdefPage))
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await this.ReadPageAsync(currentTdefPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.TableDefinition)
                {
                    break;
                }

                if (includeTDefRoot || currentTdefPage != tdefPage)
                {
                    _ = pagesToFree.Add(currentTdefPage);
                }

                firstTdefPage ??= (byte[])page.Clone();
                currentTdefPage = Ri32(page, 4);
            }
            finally
            {
                ReturnPage(page);
            }
        }

        if (firstTdefPage is not null && this.Format != DatabaseFormat.Jet3Mdb)
        {
            int usageMapPage = UsageMap.ReadUInt24(firstTdefPage, Constants.TableDefinition.OwnedPagesPageOffset);
            if (usageMapPage > 0)
            {
                _ = pagesToFree.Add(usageMapPage);
                await this.CollectIndexPagesFromUsageMapAsync(usageMapPage, pagesToFree, cancellationToken).ConfigureAwait(false);
            }
        }

        foreach (LongValueDescriptor root in longValueRoots)
        {
            await this.longValueEncoder.DeallocateLongValueAsync(root, cancellationToken).ConfigureAwait(false);
        }

        foreach (long pageNumber in pagesToFree)
        {
            if (pageNumber > 2)
            {
                await this.pageAllocator.DeallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask CollectIndexPagesFromUsageMapAsync(long usageMapPageNumber, SortedSet<long> pagesToFree, CancellationToken cancellationToken)
    {
        long totalPages = this.DatabaseStream.Length / this.PageSizeBytes;
        if (usageMapPageNumber <= 0 || usageMapPageNumber >= totalPages)
        {
            return;
        }

        byte[] page = await this.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.Data)
            {
                return;
            }

            foreach (RowBound rowBound in this.EnumerateLiveRowBounds(page))
            {
                if (rowBound.RowIndex < 2)
                {
                    continue;
                }

                var indexPages = new List<long>();
                if (!await UsageMap.TryEnumeratePagesAsync(
                    page,
                    rowBound,
                    this.PageSizeBytes,
                    totalPages,
                    minimumPageNumber: 3,
                    strict: false,
                    this.ReadPageAsync,
                    ReturnPage,
                    indexPages,
                    cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                foreach (long pageNumber in indexPages)
                {
                    _ = pagesToFree.Add(pageNumber);
                }
            }
        }
        finally
        {
            ReturnPage(page);
        }
    }

    internal async ValueTask InsertRowDataAsync(long tdefPage, TableDef tableDef, object[] values, bool updateTDefRowCount = true, CancellationToken cancellationToken = default) => _ = await this.InsertRowDataLocAsync(tdefPage, tableDef, values, updateTDefRowCount, cancellationToken).ConfigureAwait(false);

    internal async ValueTask InsertSystemRowAndMaintainAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        object[] values,
        bool updateTDefRowCount = true,
        CancellationToken cancellationToken = default)
        => await this.indexMaintainer.InsertSystemRowAndMaintainAsync(tdefPage, tableDef, tableName, values, updateTDefRowCount, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Inserts a row and returns its (page, row-index) location so the caller
    /// can mark it deleted if a subsequent step (e.g. unique-index rebuild)
    /// fails. Mirrors <see cref="InsertRowDataAsync"/> but exposes the
    /// <see cref="RowLocation"/> of the freshly written row.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values.</param>
    /// <param name="updateTDefRowCount">Whether to update the table row count in the TDEF.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="values"/> does not contain one value per column in <paramref name="tableDef"/>.</exception>
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
        values = await this.longValueEncoder.PreEncodeLongValuesAsync(tdefPage, tableDef, values, cancellationToken).ConfigureAwait(false);

        byte[] rowBytes = this.rowEncoder.SerializeRow(tableDef, values);
        PageInsertTarget target = await this.dataPageInserter.FindInsertTargetAsync(tdefPage, rowBytes.Length, cancellationToken).ConfigureAwait(false);
        int rowIndex;
        int rowStart;
        try
        {
            rowIndex = Ru16(target.Page, this.DataPage.NumRows);
            rowStart = this.dataPageInserter.GetFirstRowStart(target.Page, rowIndex) - rowBytes.Length;
            await this.dataPageInserter.WriteRowToPageAsync(target.PageNumber, target.Page, rowBytes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(target.Page);
        }

        if (updateTDefRowCount)
        {
            await this.AdjustTDefRowCountAsync(tdefPage, 1, cancellationToken).ConfigureAwait(false);
        }

        return new RowLocation(target.PageNumber, rowIndex, rowStart, rowBytes.Length);
    }

    /// <summary>
    /// Adjusts the persisted row count of the table at <paramref name="tdefPage"/>
    /// by <paramref name="delta"/>. Delegates to <see cref="TDefPageBuilder"/>,
    /// which owns TDEF row-count and <c>num_idx_rows</c> byte layout.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="delta">The signed row-count delta.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask AdjustTDefRowCountAsync(long tdefPage, long delta, CancellationToken cancellationToken)
        => this.tdefPageBuilder.AdjustTDefRowCountAsync(tdefPage, delta, cancellationToken);

    /// <summary>
    /// Rewrites all data pages for a small system table with the supplied live rows.
    /// Used when tombstones themselves are not DAO-compatible, such as
    /// <c>MSysRelationships</c> rename/drop mutations.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rows">The row collection.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">Thrown when the system table has rows to rewrite but no data pages are present.</exception>
    internal async ValueTask RewriteSystemTableRowsAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        IReadOnlyList<object[]> rows,
        CancellationToken cancellationToken)
    {
        var dataPages = new List<long>();
        await this.ForEachOwnedDataPageAsync(
            tdefPage,
            (pageNumber, _, _) =>
            {
                dataPages.Add(pageNumber);
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        if (dataPages.Count == 0 && rows.Count > 0)
        {
            throw new InvalidDataException($"System table '{tableName}' has no data pages to rewrite.");
        }

        foreach (long pageNumber in dataPages)
        {
            await this.WritePageAsync(pageNumber, this.dataPageInserter.CreateEmptyDataPage(tdefPage), cancellationToken).ConfigureAwait(false);
        }

        if (dataPages.Count > 0)
        {
            this.SetCachedInsertPageNumber(tdefPage, dataPages[0]);
        }

        foreach (object[] row in rows)
        {
            object[] rowValues = (object[])row.Clone();
            await this.InsertRowDataLocAsync(tdefPage, tableDef, rowValues, updateTDefRowCount: false, cancellationToken).ConfigureAwait(false);
        }

        await this.AdjustTDefRowCountAsync(tdefPage, rows.Count - tableDef.RowCount, cancellationToken).ConfigureAwait(false);
        tableDef.RowCount = rows.Count;
        await this.indexMaintainer.MaintainIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
    }

    internal bool TryGetCachedInsertPageNumber(long tdefPage, out long pageNumber)
    {
        this.stateLock.EnterReadLock();
        try
        {
            if (this.cachedInsertTDefPage == tdefPage && this.cachedInsertPageNumber >= 3)
            {
                pageNumber = this.cachedInsertPageNumber;
                return true;
            }

            pageNumber = -1;
            return false;
        }
        finally
        {
            this.stateLock.ExitReadLock();
        }
    }

    internal void SetCachedInsertPageNumber(long tdefPage, long pageNumber)
    {
        this.stateLock.EnterWriteLock();
        try
        {
            this.cachedInsertTDefPage = tdefPage;
            this.cachedInsertPageNumber = pageNumber;
        }
        finally
        {
            this.stateLock.ExitWriteLock();
        }
    }

    internal ValueTask<List<CatalogRow>> GetCatalogRowsAsync(TableDef msys, CancellationToken cancellationToken)
        => this.catalogWriter.GetCatalogRowsAsync(msys, cancellationToken);

    /// <summary>
    /// Reads <paramref name="columnOrdinals"/>'s typed values out of a single
    /// row at <paramref name="loc"/> on a data page belonging to
    /// <paramref name="tableDef"/>. Returns <see langword="null"/> when the
    /// row layout cannot be parsed OR when any requested column needs
    /// long-value (Memo, Ole) or complex (Complex, Attachment)
    /// traversal outside this inline reader; the cascade-seek caller falls back to the snapshot
    /// path in that case. Index-key column types (the focus of this helper)
    /// usually include scalar fixed and var-inline kinds. Memo is indexable
    /// but routes through the snapshot path when pre-write uniqueness checks
    /// need existing-row values; OLE / Attachment / Complex columns are
    /// rejected by <see cref="IndexHelpers.ResolveIndexes"/>.
    /// </summary>
    /// <param name="loc">The row location.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="columnOrdinals">The column ordinals.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<object?[]?> TryReadColumnValuesTypedAsync(
        RowLocation loc,
        TableDef tableDef,
        int[] columnOrdinals,
        CancellationToken cancellationToken)
    {
        byte[] pageBytes = await this.ReadPageAsync(loc.PageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (pageBytes[0] != Constants.PageTypes.Data)
            {
                return null;
            }

            var decodePlan = RowDecodePlan.CreatePartial(tableDef, columnOrdinals);
            object?[] result = new object?[columnOrdinals.Length];
            return decodePlan.TryDecodePartialColumns(this, pageBytes, loc.RowStart, loc.RowSize, result)
                ? result
                : null;
        }
        finally
        {
            ReturnPage(pageBytes);
        }
    }

    private bool IsOwnedMapWritableTdef(long tdefPageNumber) => this.ownedMapWritableTdefs.Contains(tdefPageNumber);

    internal async ValueTask<bool> CanMaintainOwnedMapAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        if (this.Format == DatabaseFormat.Jet3Mdb || tdefPageNumber <= 0)
        {
            return false;
        }

        if (this.IsOwnedMapWritableTdef(tdefPageNumber))
        {
            return true;
        }

        if (tdefPageNumber == 2)
        {
            return false;
        }

        TableDef? msys = await this.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys is null)
        {
            return false;
        }

        List<CatalogRow> rows = await this.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage != tdefPageNumber || row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if (string.IsNullOrEmpty(row.Name) || row.Name.StartsWith("MSys", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            this.RegisterOwnedMapWritableTdef(tdefPageNumber);
            return true;
        }

        return false;
    }

    private void RegisterOwnedMapWritableTdef(long tdefPageNumber) => this.ownedMapWritableTdefs.Add(tdefPageNumber);

    internal async ValueTask RequireMsysObjectsIndexMaintenanceAsync(
        TableDef msys,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        string operation,
        CancellationToken cancellationToken)
    {
        bool incremental = await this.indexMaintainer.TryMaintainIndexesIncrementalAsync(
            2,
            msys,
            insertedRows,
            deletedRows,
            cancellationToken).ConfigureAwait(false);
        if (incremental)
        {
            return;
        }

        if (this.DatabaseFormat != DatabaseFormat.Jet3Mdb)
        {
            throw new InvalidOperationException($"Could not maintain MSysObjects catalog indexes while {operation}.");
        }

        await this.indexMaintainer.MaintainIndexesAsync(2, msys, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
    }

    internal ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, CancellationToken cancellationToken)
        => this.MarkRowDeletedAsync(pageNumber, rowIndex, tableDef: null, DeletedRowDataMode.Default, cancellationToken);

    internal ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, DeletedRowDataMode dataMode, CancellationToken cancellationToken)
        => this.MarkRowDeletedAsync(pageNumber, rowIndex, tableDef: null, dataMode, cancellationToken);

    internal async ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, TableDef? tableDef, CancellationToken cancellationToken)
        => await this.MarkRowDeletedAsync(pageNumber, rowIndex, tableDef, DeletedRowDataMode.Default, cancellationToken).ConfigureAwait(false);

    private async ValueTask MarkRowDeletedAsync(long pageNumber, int rowIndex, TableDef? tableDef, DeletedRowDataMode dataMode, CancellationToken cancellationToken)
    {
        byte[] page = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        List<LongValueDescriptor>? longValueRoots = null;
        int offsetPos = this.DataPage.RowsStart + (rowIndex * 2);
        int raw = Ru16(page, offsetPos);
        if ((raw & Constants.DataPage.NonLiveRowFlags) != 0)
        {
            ReturnPage(page);
            return;
        }

        if (dataMode == DeletedRowDataMode.Clear || this.Options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages)
        {
            foreach (RowBound rowBound in this.EnumerateLiveRowBounds(page))
            {
                if (rowBound.RowIndex != rowIndex)
                {
                    continue;
                }

                if (tableDef is not null && this.Options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages)
                {
                    longValueRoots = this.longValueEncoder.CollectLongValueRoots(page, rowBound, tableDef);
                }

                Array.Clear(page, rowBound.RowStart, rowBound.RowSize);
                break;
            }
        }

        Wu16(page, offsetPos, raw | Constants.DataPage.DeletedRowFlag);
        await this.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        ReturnPage(page);

        if (longValueRoots is null)
        {
            return;
        }

        foreach (LongValueDescriptor root in longValueRoots)
        {
            await this.longValueEncoder.DeallocateLongValueAsync(root, cancellationToken).ConfigureAwait(false);
        }
    }
}
