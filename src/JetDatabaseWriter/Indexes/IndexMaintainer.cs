namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Index B-tree maintenance for <see cref="AccessWriter"/>: bulk rebuild
/// (<see cref="MaintainIndexesAsync"/>), incremental fast-path
/// (<see cref="TryMaintainIndexesIncrementalAsync"/>), the catalog-index
/// splice (<see cref="TrySpliceCatalogIndexEntryAsync"/>), and the system-table
/// index policy used by prepared row mutations
/// (<see cref="MaintainSystemTableIndexesIncrementallyAsync"/>).
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="pager">The writer's page file, through which index and data pages are written.</param>
/// <param name="tdefWriter">Writes index roots and usage-map pointers back into TDEF chains.</param>
/// <param name="pageAllocator">Reserves and frees index pages.</param>
/// <param name="dataPages">Owns usage-map rows and empty data pages.</param>
/// <param name="snapshots">Reads decoded table rows for full index rebuilds.</param>
/// <param name="catalogRows">Discovers surviving table definitions when proving usage-map ownership.</param>
internal sealed class IndexMaintainer(
    JetFormat format,
    TableDefReader tableDefs,
    Pager pager,
    TDefWriter tdefWriter,
    PageAllocator pageAllocator,
    DataPageInserter dataPages,
    TableSnapshotReader snapshots,
    CatalogRowReader catalogRows)
{
    private readonly IndexBTreeEditor btreeEditor = new(format, pager, tdefWriter, pageAllocator);

    /// <summary>Refuses a text column before creating an index over its unsupported order.</summary>
    /// <param name="column">The column descriptor.</param>
    /// <param name="tableName">The table name.</param>
    /// <exception cref="JetLimitationException">The text sort order is unsupported.</exception>
    internal static void ThrowIfTextCollationUnsupported(ColumnInfo column, string tableName)
    {
        if (column.Type is TextType or MemoType && !column.TextSortOrder.IsSupported)
        {
            throw new JetLimitationException(JetErrorCode.UnsupportedTextCollation, $"The indexes of table '{tableName}' cannot be maintained: column '{column.Name}' uses unsupported text sort order 0x{column.TextSortOrder.Value:X4}, version {column.TextSortOrder.Version}.", errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = column.Name });
        }
    }

    /// <summary>
    /// Gets the most recent reason
    /// <see cref="TryMaintainIndexesIncrementalAsync"/> returned false.
    /// Diagnostic-only; not part of the public contract.
    /// </summary>
    public string? LastIncrementalBail { get; private set; }

    internal async ValueTask<SystemTableIndexMaintenancePath> MaintainSystemTableIndexesIncrementallyAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        CancellationToken cancellationToken)
    {
        if (!await this.SystemTableHasMaintainableIndexesAsync(tdefPage, cancellationToken).ConfigureAwait(false))
        {
            return SystemTableIndexMaintenancePath.SkippedNoMaintainableIndexes;
        }

        try
        {
            bool incremental = await this.TryMaintainIndexesIncrementalAsync(
                tdefPage,
                tableDef,
                insertedRows,
                deletedRows,
                cancellationToken).ConfigureAwait(false);
            if (incremental)
            {
                return SystemTableIndexMaintenancePath.Incremental;
            }
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw this.CreateSystemTableIndexMaintenanceException(tableName, ex);
        }
        catch (InvalidOperationException ex)
        {
            throw this.CreateSystemTableIndexMaintenanceException(tableName, ex);
        }

        throw this.CreateSystemTableIndexMaintenanceException(tableName);
    }

    private static bool HasAnyIndexPageGroup(long[][] groups)
    {
        for (int i = 0; i < groups.Length; i++)
        {
            if (groups[i].Length > 0)
            {
                return true;
            }
        }

        return false;
    }

    private static void WriteIndexUsageMapPointer(byte[] tdefBuffer, int usedPagesOffset, int rowIndex, long usageMapPage)
        => UsageMap.WritePointer(tdefBuffer, usedPagesOffset, rowIndex, usageMapPage);

    /// <summary>
    /// Builds the exception the write preflight and the bulk path throw when a
    /// table's indexes cannot be maintained. Index maintenance is never
    /// skipped silently: a table whose indexes cannot be rebuilt refuses the
    /// mutation instead of leaving stale B-trees behind, before it changes
    /// anything when the preflight runs first.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="reason">Why the indexes cannot be maintained.</param>
    private static JetLimitationException CreateUnmaintainableIndexesException(string tableName, string reason)
        => new(JetErrorCode.IndexesUnmaintainable, $"The indexes of table '{tableName}' cannot be maintained: {reason}.", new JetErrorInfo { TableName = tableName, Reason = reason });

    private int ReadTableUsageMapPage(byte[] tdefBuffer)
        => UsageMap.ReadUInt24(tdefBuffer, format.TDef.UsedPagesPage);

    private JetOperationException CreateSystemTableIndexMaintenanceException(string tableName, Exception? inner = null)
    {
        string message = $"Could not maintain {tableName} system-table indexes incrementally; full rebuild fallback is disabled.";
        if (!string.IsNullOrWhiteSpace(this.LastIncrementalBail))
        {
            message += $" Bail: {this.LastIncrementalBail}.";
        }

        return new JetOperationException(JetErrorCode.SystemIndexMaintenanceFailed, message, new JetErrorInfo { TableName = tableName, Reason = this.LastIncrementalBail }, inner);
    }

    /// <summary>
    /// Returns <c>true</c> when every real-idx slot of the TDEF chain rooted at
    /// <paramref name="tdefPage"/> references a valid in-range data page
    /// through its <c>first_dp</c> pointer. Used by
    /// <see cref="MaintainSystemTableIndexesIncrementallyAsync"/> to avoid index
    /// maintenance on writer-bootstrapped system tables whose real-idx
    /// descriptors point at unallocated pages.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> SystemTableHasMaintainableIndexesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        byte[]? td = await tableDefs.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (td is null)
        {
            return false;
        }

        TDefCounts header = TDefCodec.ReadCounts(format, td);
        int numCols = header.ColumnCount;
        int numRealIdx = header.RealIndexCount;
        if (numCols < 0 || numCols > Constants.TableDefinition.MaxColumns || numRealIdx <= 0 || numRealIdx > Constants.TableDefinition.MaxIndexes)
        {
            return false;
        }

        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(format, td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return false;
        }

        long totalPages = pager.PageCount;
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            if (!format.Index.TryReadRealIdxSlot(td, realIdxDescStart, ri, out RealIdxSlot slot))
            {
                return false;
            }

            long firstDp = (uint)Ri32(td, slot.FirstDpOffset);
            if (firstDp <= 0 || firstDp >= totalPages)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Parsed snapshot of the per-table TDEF bytes needed by the index
    /// maintenance paths: the logical TDEF chain (every page of a wide
    /// table's definition, stitched into one buffer that remembers its
    /// physical pages), decoded column / index counts, and the logical offset
    /// at which the real-idx descriptor block begins (i.e. just past the
    /// column-name table). Index <c>first_dp</c> and <c>used_pages</c>
    /// offsets taken from it are logical too, so patches go into
    /// <see cref="Buffer"/> and back to disk through
    /// <see cref="TDefWriter.WriteChainInPlaceAsync"/>.
    /// </summary>
    /// <param name="Chain">The logical TDEF chain.</param>
    /// <param name="NumCols">The number of cols.</param>
    /// <param name="NumIdx">The number of index.</param>
    /// <param name="NumRealIdx">The number of real index.</param>
    /// <param name="RealIdxDescStart">The real index desc start.</param>
    private readonly record struct TdefPreamble(
        LogicalTDefChain Chain,
        int NumCols,
        int NumIdx,
        int NumRealIdx,
        int RealIdxDescStart)
    {
        /// <summary>Gets the logical TDEF bytes; patches to it are written back with the chain.</summary>
        /// <summary>Gets the current image bytes.</summary>
        public byte[] Buffer => this.Chain.Bytes;
    }

    /// <summary>
    /// Reads the whole TDEF chain, decodes <c>numCols</c> / <c>numIdx</c> /
    /// <c>numRealIdx</c>, walks the column-name table, and returns the logical
    /// offset at which the real-idx descriptor block starts. Each caller maps
    /// the returned <see cref="TdefPreambleStatus"/> to its own bail policy
    /// (an exception for the bulk path, <c>LastIncrementalBail</c> string for
    /// the incremental and catalog-splice paths).
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<(TdefPreambleStatus Status, TdefPreamble Preamble)> ReadTdefPreambleAsync(
        long tdefPage,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] buffer = chain.Bytes;

        TDefCounts header = TDefCodec.ReadCounts(format, buffer);
        int numCols = header.ColumnCount;
        int numIdx = header.LogicalIndexCount;
        int numRealIdx = header.RealIndexCount;

        if (numIdx <= 0 || numRealIdx <= 0)
        {
            return (TdefPreambleStatus.Empty, new TdefPreamble(chain, numCols, numIdx, numRealIdx, 0));
        }

        if (numIdx > Constants.TableDefinition.MaxIndexes || numRealIdx > Constants.TableDefinition.MaxIndexes)
        {
            return (TdefPreambleStatus.TooMany, new TdefPreamble(chain, numCols, numIdx, numRealIdx, 0));
        }

        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(format, buffer, numCols, numRealIdx);
        return realIdxDescStart < 0
            ? (TdefPreambleStatus.ColumnNameWalkFailed, new TdefPreamble(chain, numCols, numIdx, numRealIdx, 0))
            : (TdefPreambleStatus.Ok, new TdefPreamble(chain, numCols, numIdx, numRealIdx, realIdxDescStart));
    }

    /// <summary>
    /// The one check of whether a table's indexes can be maintained, shared by
    /// the write preflight (<see cref="ThrowIfIndexesUnmaintainableAsync"/>)
    /// and the bulk rebuild (<see cref="RebuildIndexesAsync"/>), so both
    /// refuse the same tables with the same message. Reads the whole TDEF
    /// chain, checks that every real-index descriptor, logical index entry and
    /// logical index name lies within it, decodes the index catalog with its
    /// logical index names, and resolves every real index's key columns
    /// against <paramref name="tableDef"/>.
    /// </summary>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="tableDef">The table's definition.</param>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>
    /// The TDEF preamble, and the resolved catalog, in which every real index
    /// has its key columns; the catalog is <see langword="null"/> when the
    /// table definition declares no index.
    /// </returns>
    /// <exception cref="JetLimitationException">The index section cannot be parsed or runs past the end of the table definition, or an index names a column the table does not have.</exception>
    /// <exception cref="JetCorruptDataException">Logical-index names or physical-index references are invalid.</exception>
    private async ValueTask<(TdefPreamble Preamble, IndexCatalogReader.ResolvedIndexCatalog? Catalog)> ResolveMaintainableIndexesAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        CancellationToken cancellationToken)
    {
        (TdefPreambleStatus status, TdefPreamble preamble) = await this.ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (preamble.NumIdx > 0 && preamble.NumRealIdx <= 0)
        {
            const string reason = "The table definition declares logical indexes without any physical index";
            throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"Table '{tableName}' declares logical indexes without any physical index.", new JetErrorInfo { TableName = tableName, PageNumber = tdefPage, Reason = reason });
        }

        if (status == TdefPreambleStatus.Empty)
        {
            return (preamble, null);
        }

        if (status != TdefPreambleStatus.Ok)
        {
            string reason = status == TdefPreambleStatus.TooMany
                ? $"its table definition declares {preamble.NumIdx} logical and {preamble.NumRealIdx} real indexes"
                : "the column-name section of its table definition could not be walked";
            throw CreateUnmaintainableIndexesException(tableName, reason);
        }

        IndexSectionAnchors anchors = format.Index.GetIndexSection(preamble.RealIdxDescStart, preamble.NumRealIdx, preamble.NumIdx);
        List<string> logicalIndexNames = IndexCatalogReader.ReadLogicalIdxNames(format, preamble.Buffer, anchors.LogIdxNamesStart, preamble.NumIdx);
        this.ThrowIfIndexSectionCutShort(tableName, preamble, anchors, logicalIndexNames.Count);
        var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int logicalIndex = 0; logicalIndex < preamble.NumIdx; logicalIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string indexName = logicalIndexNames[logicalIndex];
            if (!uniqueNames.Add(indexName))
            {
                const string reason = "The table definition contains duplicate logical-index names";
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"Table '{tableName}' has duplicate logical index name '{indexName}'.", new JetErrorInfo { TableName = tableName, IndexName = indexName, PageNumber = tdefPage, Reason = reason });
            }

            if (!format.Index.TryReadLogicalEntry(preamble.Buffer, anchors.LogIdxStart, logicalIndex, out LogicalIdxEntry entry)
                || entry.IndexNum2 < 0 || entry.IndexNum2 >= preamble.NumRealIdx)
            {
                const string reason = "The logical index references a real index outside its table definition";
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"Index '{indexName}' of table '{tableName}' has an invalid real-index reference.", new JetErrorInfo { TableName = tableName, IndexName = indexName, PageNumber = tdefPage, Reason = reason });
            }
        }

        IndexCatalogReader.ResolvedIndexCatalog catalog = IndexCatalogReader.ReadResolved(
            preamble.Buffer,
            format.Index,
            anchors,
            tableDef.Columns,
            logicalIndexNames);

        // Key columns are resolved by col_num, so deleted-column gaps are
        // fine. An index naming a column the table does not have cannot be
        // rebuilt, and is refused rather than left stale or skipped.
        for (int realIdxNum = 0; realIdxNum < preamble.NumRealIdx; realIdxNum++)
        {
            if (catalog.RealIdxByNum.ContainsKey(realIdxNum) && !catalog.TryGetKeyColumnInfos(realIdxNum, out _))
            {
                throw CreateUnmaintainableIndexesException(
                    tableName,
                    $"index '{catalog.Catalog.GetNameOrFallback(realIdxNum)}' names a column the table does not have");
            }
        }

        foreach (int realIdxNum in catalog.RealIdxByNum.Keys)
        {
            if (catalog.TryGetKeyColumnInfos(realIdxNum, out List<KeyColumnInfo>? keyColumns))
            {
                foreach (KeyColumnInfo keyColumn in keyColumns)
                {
                    ColumnInfo column = keyColumn.Col;
                    if (column.Type == BigBinaryType)
                    {
                        throw CreateUnmaintainableIndexesException(tableName, $"index '{catalog.Catalog.GetNameOrFallback(realIdxNum)}' requires unsupported native BIGBINARY encoding");
                    }

                    ThrowIfTextCollationUnsupported(column, tableName);
                }
            }
        }

        return (preamble, catalog);
    }

    /// <summary>
    /// Refuses a table whose index section runs past the end of its table
    /// definition as read. The chain read stops at a page whose type byte is
    /// not a TDEF page's, including a corrupted continuation-page header. Catalog
    /// decode stops at the first descriptor or entry it cannot read, so the
    /// indexes past that point would be neither checked nor maintained, and
    /// a schema rewrite would not carry them over.
    /// </summary>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="preamble">The TDEF preamble, with the chain as read.</param>
    /// <param name="anchors">The index section's anchors.</param>
    /// <param name="logicalIndexNameCount">How many logical index names could be read.</param>
    /// <exception cref="JetLimitationException">A real-index descriptor, a logical index entry or a logical index name lies past the end of the table definition.</exception>
    private void ThrowIfIndexSectionCutShort(string tableName, TdefPreamble preamble, IndexSectionAnchors anchors, int logicalIndexNameCount)
    {
        IndexLayout layout = format.Index;
        int length = preamble.Buffer.Length;
        for (int realIdxNum = 0; realIdxNum < preamble.NumRealIdx; realIdxNum++)
        {
            if (!layout.TryGetRealIdxPhysOffset(anchors.RealIdxDescStart, realIdxNum, length, out _))
            {
                throw CreateUnmaintainableIndexesException(
                    tableName,
                    $"its table definition ends before the descriptor of real index {realIdxNum}");
            }
        }

        for (int logicalIdxNum = 0; logicalIdxNum < preamble.NumIdx; logicalIdxNum++)
        {
            if (!layout.TryGetLogicalIdxFieldsOffset(anchors.LogIdxStart, logicalIdxNum, length, out _))
            {
                throw CreateUnmaintainableIndexesException(
                    tableName,
                    $"its table definition ends before the entry of logical index {logicalIdxNum}");
            }
        }

        if (logicalIndexNameCount < preamble.NumIdx)
        {
            throw CreateUnmaintainableIndexesException(
                tableName,
                $"its table definition ends before the name of logical index {logicalIndexNameCount}");
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
    /// the TDEF. The pages of the tree it replaces then go back to the global
    /// usage map, except on system tables, complex flat tables and tables with
    /// long or complex storage columns. Shared or uncertain Jet3 map storage
    /// is retained, and its index gets a private map row before refreshing.
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
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetOperationException">Thrown when a unique index violation is detected after a row mutation.</exception>
    /// <exception cref="JetLimitationException">Thrown when the TDEF's index section cannot be parsed or an index names a column the table does not have, so the indexes cannot be rebuilt.</exception>
    public ValueTask MaintainIndexesAsync(long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
        => this.RebuildIndexesAsync(tdefPage, tableDef, tableName, writtenRows: null, cancellationToken);

    /// <summary>
    /// The bulk rebuild behind <see cref="MaintainIndexesAsync"/>. When
    /// <paramref name="writtenRows"/> is supplied, the indexes are built from
    /// those rows and locations instead of decoding the table again. The schema
    /// rewrite passes the rows it has just copied into the rebuilt table,
    /// with the location each insert returned.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writtenRows">Every live row of the table, in <paramref name="tableDef"/> column order, with its location; or <see langword="null"/> to decode every live row of the table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetOperationException">Thrown when a unique index violation is detected after a row mutation.</exception>
    /// <exception cref="JetLimitationException">Thrown when the TDEF's index section cannot be parsed or an index names a column the table does not have, so the indexes cannot be rebuilt.</exception>
    /// <exception cref="JetConstraintException">The operation is refused with a structured <see cref="JetConstraintException"/>.</exception>
    internal async ValueTask RebuildIndexesAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        IReadOnlyList<LocatedRow>? writtenRows,
        CancellationToken cancellationToken)
    {
        // Jet3 (.mdb Access 97) live leaf maintenance is now
        // supported. The 39-byte real-idx + 20-byte logical-idx layouts
        // (§3.1 / §3.2) and the 0x16-bitmask / 0xF8-first-entry leaf layout
        // (§4.2) are pinned by the format probe and emitted by the same code
        // path Jet4/ACE uses, parameterised on `IndexPageLayout`.

        // Read the whole TDEF chain. Wide schemas (>32 col / >16 idx on Jet3,
        // ≫50 col on Jet4 / ACE) span several pages, and their real-idx
        // descriptors (with the first_dp / used_pages fields patched below)
        // sit on a continuation page. The resolver throws, before any tree is
        // written, when an index cannot be rebuilt, exactly as the write
        // preflight (ThrowIfIndexesUnmaintainableAsync) does.
        (TdefPreamble preamble, IndexCatalogReader.ResolvedIndexCatalog? catalog) =
            await this.ResolveMaintainableIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        if (catalog is null)
        {
            return;
        }

        byte[] tdefBuffer = preamble.Buffer;
        int numRealIdx = preamble.NumRealIdx;

        IndexPageLayout leafLayout = format.IndexPage;

        // Every populated real-idx slot, with IsUnique already promoted for
        // any slot backing a PK logical-idx, and its key columns resolved.
        Dictionary<int, RealIdxEntry> realIdxByNum = catalog.RealIdxByNum;
        var indexPointers = new UsageMapPointer[numRealIdx];
        foreach ((int realIdxNum, RealIdxEntry entry) in realIdxByNum)
        {
            _ = UsageMap.TryReadPointer(tdefBuffer, entry.FirstDpOffset - 4, out indexPointers[realIdxNum]);
        }

        if (realIdxByNum.Count == 0)
        {
            return;
        }

        // Every live row, decoded from the location its index entry points at,
        // unless the caller supplied the rows it wrote with their locations.
        IReadOnlyList<LocatedRow> rows = writtenRows
            ?? await snapshots.ReadRowsAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        bool tdefDirty = false;
        bool jet3 = format.IsJet3;
        long[][] rebuiltIndexPageGroups = CreateEmptyPageGroups(numRealIdx);

        // The trees this rebuild replaces go back to the global usage map
        // after the TDEF write. System tables, complex flat tables and tables
        // with long or complex storage columns stay conservative and keep
        // them. Jet4 / ACE find each old tree through its index usage-map row.
        // Jet3 also walks current roots and proves ownership before reclaiming
        // original map members that a previous writer left outside the tree.
        bool freeReplacedTrees = !HasLongOrComplexStorageColumns(tableDef)
            && !IsGeneratedComplexFlatTableName(tableName)
            && !tableName.StartsWith("MSys", StringComparison.OrdinalIgnoreCase);
        long[][]? oldIndexPageGroups = null;
        if (freeReplacedTrees)
        {
            oldIndexPageGroups = jet3
                ? await this.CollectJet3ReplacedTreePagesAsync(tdefPage, tdefBuffer, leafLayout, realIdxByNum, numRealIdx, cancellationToken).ConfigureAwait(false)
                : await this.ReadIndexPageGroupsFromUsageMapAsync(
                    indexPointers,
                    cancellationToken).ConfigureAwait(false);
        }

        // Each index's new tree is reserved and written in turn, but nothing
        // links it until the usage-map rows and the TDEF are written after
        // the loop. If an index throws first (a unique violation, an encoder
        // rejection, an I/O failure), the trees already written go back to
        // the global usage map.
        var runs = new ReservedPageRuns(pageAllocator);
        try
        {
            foreach ((int rieKey, RealIdxEntry rie) in realIdxByNum)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Key columns are resolved by col_num, so deleted-column gaps are
                // fine; the resolver has already refused an index naming a
                // column the table does not have.
                List<KeyColumnInfo> keyColInfos = catalog.KeyColumnInfosByRealIdx[rieKey];

                List<IndexEntry> entries = new(rows.Count);
                object?[] cells = new object?[keyColInfos.Count];
                byte[]? allowedNullKey = !rie.IsRequired && !catalog.Catalog.PkRealIdxNums.Contains(rieKey)
                    ? this.EncodeCompositeKey(keyColInfos, cells)
                    : null;
                foreach ((RowLocation location, object[] values) in rows)
                {
                    if (rie.IgnoreNulls && IndexHelpers.CountNullKeyColumns(keyColInfos, values) == keyColInfos.Count)
                    {
                        continue;
                    }

                    for (int k = 0; k < keyColInfos.Count; k++)
                    {
                        object cell = values[keyColInfos[k].SnapIdx];
                        cells[k] = cell is DBNull ? null : cell;
                    }

                    byte[] composite = this.EncodeCompositeKey(keyColInfos, cells);
                    entries.Add(new IndexEntry(composite, location.PageNumber, (byte)location.RowIndex));
                }

                entries.Sort(static (a, b) => IndexHelpers.CompareKeyBytes(a.Key, b.Key));

                // Unique-violation detection. This is a post-write defense-in-depth
                // check; public mutations normally run pre-write uniqueness checks
                // before reaching this bulk rebuild path. Callers that own a pending
                // mutation must roll it back before surfacing this failure.
                if (rie.IsUnique)
                {
                    for (int e = 1; e < entries.Count; e++)
                    {
                        if (IndexHelpers.CompareKeyBytes(entries[e - 1].Key, entries[e].Key) == 0
                            && (allowedNullKey is null || IndexHelpers.CompareKeyBytes(entries[e].Key, allowedNullKey) != 0))
                        {
                            throw new JetConstraintException(JetErrorCode.UniqueViolation, $"Unique index violation on table '{tableName}': duplicate key detected after row mutation. The duplicate row has been written but the index B-tree was not rebuilt; remove one of the offending rows and retry the operation.", errorInfo: new JetErrorInfo { TableName = tableName, IndexName = catalog.Catalog.GetNameOrFallback(rieKey) });
                        }
                    }
                }

                IndexBTreeBuildResult build = IndexBTreeBuilder.Build(leafLayout, format.PageSize, tdefPage, entries, pager.PageCount);
                long rootPageNumber;
                long[] pageNumbers;

                int oldRootPageNumber = Ri32(tdefBuffer, rie.FirstDpOffset);
                if (build.Pages.Count == 1 && await this.CanReuseSingleLeafPageAsync(oldRootPageNumber, tdefPage, cancellationToken).ConfigureAwait(false))
                {
                    await pager.WritePageAsync(oldRootPageNumber, build.Pages[0], cancellationToken).ConfigureAwait(false);
                    rootPageNumber = oldRootPageNumber;
                    pageNumbers = [oldRootPageNumber];
                }
                else
                {
                    IndexBTreeBuildResult? placed = await this.btreeEditor.PlaceBuiltTreeAsync(
                        build,
                        firstPage => IndexBTreeBuilder.TryBuild(leafLayout, format.PageSize, tdefPage, entries, firstPage),
                        runs,
                        cancellationToken).ConfigureAwait(false);
                    build = placed ?? throw new JetLimitationException(
                        JetErrorCode.IndexEntryTooLarge,
                        $"The rebuilt B-tree of real index {rieKey} on table '{tableName}' no longer fits at the pages the allocator reserved for it.",
                        new JetErrorInfo { TableName = tableName, PageNumber = tdefPage });

                    rootPageNumber = build.RootPageNumber;
                    pageNumbers = new long[build.Pages.Count];
                    for (int i = 0; i < pageNumbers.Length; i++)
                    {
                        pageNumbers[i] = build.FirstPageNumber + i;
                    }
                }

                Wi32(tdefBuffer, rie.FirstDpOffset, checked((int)rootPageNumber));
                rebuiltIndexPageGroups[rieKey] = pageNumbers;

                tdefDirty = true;
            }

            // The usage-map rows and the TDEF write below link every tree
            // rebuilt above.
            runs.MarkLinked();
        }
        catch
        {
            await runs.ReleaseAsync().ConfigureAwait(false);
            throw;
        }

        long[][] mapPageGroups = await this.FilterUnchangedIndexMapsAsync(indexPointers, rebuiltIndexPageGroups, cancellationToken).ConfigureAwait(false);
        if (HasAnyIndexPageGroup(mapPageGroups))
        {
            var changedIndexes = realIdxByNum.Where(entry => mapPageGroups[entry.Key].Length > 0).ToDictionary(entry => entry.Key, entry => entry.Value);
            await this.DetachAliasedIndexMapsAsync(tdefPage, changedIndexes, indexPointers, cancellationToken).ConfigureAwait(false);
            long usageMapPage = this.ReadTableUsageMapPage(tdefBuffer);
            indexPointers = await dataPages.UpdateTableIndexUsageMapRowsAsync(usageMapPage, indexPointers, mapPageGroups, cancellationToken).ConfigureAwait(false);
            for (int realIdxNum = 0; realIdxNum < mapPageGroups.Length; realIdxNum++)
            {
                if (mapPageGroups[realIdxNum].Length == 0)
                {
                    continue;
                }

                if (!realIdxByNum.TryGetValue(realIdxNum, out RealIdxEntry rebuiltEntry))
                {
                    continue;
                }

                UsageMapPointer pointer = indexPointers[realIdxNum];
                WriteIndexUsageMapPointer(tdefBuffer, rebuiltEntry.FirstDpOffset - 4, pointer.RowIndex, pointer.PageNumber);
            }

            tdefDirty = true;
        }

        if (tdefDirty)
        {
            await tdefWriter.WriteChainInPlaceAsync(preamble.Chain, cancellationToken).ConfigureAwait(false);
        }

        if (oldIndexPageGroups is not null)
        {
            await this.DeallocateReplacedIndexPagesAsync(tdefPage, oldIndexPageGroups, rebuiltIndexPageGroups, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Checks, before a write changes anything, that every index of the table
    /// can be maintained: the index section of its table definition can be
    /// parsed and lies wholly within the TDEF chain, and every real index
    /// names only columns the table has. The wide tables of builds before
    /// 4.0.0 fail it: a stray <c>used_pages</c> byte in a <c>col_map</c>
    /// names a missing column, and one on a continuation page's type byte
    /// cuts the chain short. Either would make the index rebuild throw, or
    /// skip the indexes it cannot read, after the write had already changed
    /// rows. Insert, update and delete in <see cref="TableDataWriter"/> and
    /// the schema rewrite in <see cref="TableSchemaEditor"/> call this first,
    /// so each of those writes to such a table is refused and leaves the file
    /// byte for byte unchanged. Writes that reach the table another way do
    /// not call it yet and still change the file before the rebuild throws:
    /// a cascade from a related table, <c>CreateRelationshipAsync</c>, and a
    /// complex item added to a row whose complex slot is null. It runs the
    /// same resolver as <see cref="RebuildIndexesAsync"/>, which throws the
    /// same exception. For <c>MSysObjects</c>, it also proves that every
    /// reachable catalog index leaf has a complete payload and matching
    /// end sentinel, before a temporary table can be inserted.
    /// </summary>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="tableDef">The table's definition.</param>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the check has passed.</returns>
    /// <exception cref="JetLimitationException">The index section cannot be parsed or runs past the end of the table definition, or an index names a column the table does not have; the message names the table, and the index when its name can be read.</exception>
    /// <exception cref="JetCorruptDataException">A catalog index page is malformed; the error identifies the catalog and page.</exception>
    internal async ValueTask ThrowIfIndexesUnmaintainableAsync(long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
    {
        (TdefPreamble preamble, IndexCatalogReader.ResolvedIndexCatalog? catalog) = await this.ResolveMaintainableIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        if (tdefPage == 2 && catalog is not null)
        {
            await this.ValidateCatalogIndexPagesAsync(tdefPage, preamble, catalog, cancellationToken).ConfigureAwait(false);
        }
    }

    private static List<IndexEntry> DecodeRequiredCatalogLeaf(long pageNumber, byte[] page, JetFormat profile)
    {
        if (!IndexPageCodec.TryDecodeLeafEntries(profile.IndexPage, page, profile.PageSize, out List<IndexEntry> entries))
        {
            throw CreateCorruptCatalogIndexException(pageNumber, indexName: null, "The leaf payload cannot be decoded completely.");
        }

        return entries;
    }

    private static JetCorruptDataException CreateCorruptCatalogIndexException(long pageNumber, string? indexName, string reason)
        => new(JetErrorCode.CorruptIndex, $"The MSysObjects catalog index is corrupt at page {pageNumber}.", new JetErrorInfo
        {
            TableName = Constants.SystemTableNames.Objects,
            IndexName = indexName,
            PageNumber = pageNumber,
            Reason = reason,
        });

    private static long[][] CreateEmptyPageGroups(int numRealIdx)
    {
        long[][] groups = new long[numRealIdx][];
        for (int i = 0; i < groups.Length; i++)
        {
            groups[i] = [];
        }

        return groups;
    }

    private static bool HasLongOrComplexStorageColumns(TableDef tableDef)
    {
        foreach (ColumnInfo column in tableDef.Columns)
        {
            if (column.Type is MemoType or OleType or ComplexType)
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsGeneratedComplexFlatTableName(string tableName)
    {
        if (tableName.Length <= 35 || tableName[0] != 'f' || tableName[1] != '_' || tableName[34] != '_')
        {
            return false;
        }

        for (int i = 2; i < 34; i++)
        {
            char ch = tableName[i];
            if (ch is not ((>= '0' and <= '9') or (>= 'A' and <= 'F')))
            {
                return false;
            }
        }

        return true;
    }

    private async ValueTask ValidateCatalogIndexPagesAsync(long tdefPage, TdefPreamble preamble, IndexCatalogReader.ResolvedIndexCatalog catalog, CancellationToken cancellationToken)
    {
        foreach ((int indexNumber, RealIdxEntry index) in catalog.RealIdxByNum)
        {
            long root = (uint)Ri32(preamble.Buffer, index.FirstDpOffset);
            var pending = new Stack<long>();
            var seen = new HashSet<long>();
            pending.Push(root);
            while (pending.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                long pageNumber = pending.Pop();
                if (pageNumber <= 2 || pageNumber >= pager.PageCount)
                {
                    throw CreateCorruptCatalogIndexException(pageNumber, catalog.Catalog.GetNameOrFallback(indexNumber), "An index pointer lies outside the database.");
                }

                if (!seen.Add(pageNumber))
                {
                    continue;
                }

                byte[] page = await pager.ReadPageCopyAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                if (Ri32(page, 4) != tdefPage)
                {
                    throw CreateCorruptCatalogIndexException(pageNumber, catalog.Catalog.GetNameOrFallback(indexNumber), "The index page belongs to another table.");
                }

                if (IndexPageCodec.IsLeaf(page))
                {
                    _ = DecodeRequiredCatalogLeaf(pageNumber, page, format);
                    long next = IndexPageCodec.ReadNextPage(format.IndexPage, page);
                    if (next != 0)
                    {
                        pending.Push(next);
                    }
                }
                else if (IndexPageCodec.TryDecodeIntermediateEntries(format.IndexPage, page, format.PageSize, out List<DecodedIntermediateEntry> entries))
                {
                    foreach (DecodedIntermediateEntry entry in entries)
                    {
                        pending.Push(entry.ChildPage);
                    }

                    long tail = IndexPageCodec.ReadTailPage(format.IndexPage, page);
                    if (tail != 0)
                    {
                        pending.Push(tail);
                    }

                    if (entries.Count == 0 && tail == 0)
                    {
                        throw CreateCorruptCatalogIndexException(pageNumber, catalog.Catalog.GetNameOrFallback(indexNumber), "An intermediate index page has no children.");
                    }
                }
                else
                {
                    throw CreateCorruptCatalogIndexException(pageNumber, catalog.Catalog.GetNameOrFallback(indexNumber), "The index page payload cannot be decoded completely.");
                }
            }
        }
    }

    private async ValueTask<long[][]?> ReadIndexPageGroupsFromUsageMapAsync(
        UsageMapPointer[] indexPointers,
        CancellationToken cancellationToken)
    {
        var mapPages = new Dictionary<int, byte[]>();
        try
        {
            long[][] result = CreateEmptyPageGroups(indexPointers.Length);
            for (int realIdxNum = 0; realIdxNum < indexPointers.Length; realIdxNum++)
            {
                UsageMapPointer pointer = indexPointers[realIdxNum];
                if (pointer.PageNumber <= 0 || pointer.PageNumber >= pager.PageCount)
                {
                    continue;
                }

                if (!mapPages.TryGetValue(pointer.PageNumber, out byte[]? page))
                {
                    page = await pager.ReadPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
                    mapPages.Add(pointer.PageNumber, page);
                }

                if (page[0] != Constants.PageTypes.Data
                    || !UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound rowBound))
                {
                    continue;
                }

                var pageNumbers = new List<long>();
                if (!await UsageMap.TryEnumeratePagesAsync(
                    page,
                    rowBound,
                    format.PageSize,
                    pager.PageCount,
                    minimumPageNumber: 0,
                    strict: true,
                    pager.ReadPageAsync,
                    PageBuffers.Return,
                    pageNumbers,
                    cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                result[realIdxNum] = pageNumbers.ToArray();
            }

            return result;
        }
        finally
        {
            foreach (byte[] page in mapPages.Values)
            {
                PageBuffers.Return(page);
            }
        }
    }

    /// <summary>
    /// Collects every page of the current tree of each real index a Jet3 bulk
    /// rebuild is about to replace, and the original members of exclusively
    /// owned map rows. Shared rows cannot contribute extra map-only candidates;
    /// surviving references protect traversed tree pages after publication. An
    /// unreadable map or tree prevents reclamation rather than guessing ownership.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tdefBuffer">The logical TDEF bytes, before the rebuild patches them.</param>
    /// <param name="layout">The index page layout.</param>
    /// <param name="realIdxByNum">The real indexes the rebuild replaces, by real-index number.</param>
    /// <param name="numRealIdx">The number of real indexes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The pages of each collected tree by real-index number, and an empty group for every other index.</returns>
    private async ValueTask<long[][]> CollectJet3ReplacedTreePagesAsync(
        long tdefPage,
        byte[] tdefBuffer,
        IndexPageLayout layout,
        Dictionary<int, RealIdxEntry> realIdxByNum,
        int numRealIdx,
        CancellationToken cancellationToken)
    {
        int numCols = TDefCodec.ReadCounts(format, tdefBuffer).ColumnCount;
        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(format, tdefBuffer, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return CreateEmptyPageGroups(numRealIdx);
        }

        var pointers = new UsageMapPointer[numRealIdx];
        bool[] hasMap = new bool[numRealIdx];
        for (int realIdxNum = 0; realIdxNum < numRealIdx; realIdxNum++)
        {
            if (!format.Index.TryReadRealIdxSlot(tdefBuffer, realIdxDescStart, realIdxNum, out RealIdxSlot slot)
                || !UsageMap.TryReadPointer(tdefBuffer, slot.FirstDpOffset - 4, out pointers[realIdxNum]))
            {
                return CreateEmptyPageGroups(numRealIdx);
            }

            hasMap[realIdxNum] = Ri32(tdefBuffer, slot.FirstDpOffset - 4) != 0;
        }

        long[][] mappedGroups = await this.ReadIndexPageGroupsFromUsageMapAsync(pointers, cancellationToken).ConfigureAwait(false) ?? CreateEmptyPageGroups(numRealIdx);
        for (int realIdxNum = 0; realIdxNum < mappedGroups.Length; realIdxNum++)
        {
            // Even an unmaintained descriptor may own pages shared with a
            // replaced tree. Every nonzero native map must be understood.
            if (hasMap[realIdxNum] && mappedGroups[realIdxNum].Length == 0)
            {
                return CreateEmptyPageGroups(numRealIdx);
            }
        }

        long[][] groups = CreateEmptyPageGroups(numRealIdx);
        foreach ((int realIdxNum, RealIdxEntry entry) in realIdxByNum)
        {
            long rootPage = (uint)Ri32(tdefBuffer, entry.FirstDpOffset);
            if (realIdxNum < 0 || realIdxNum >= numRealIdx || rootPage == 0
                || (Ri32(tdefBuffer, entry.FirstDpOffset - 4) != 0 && mappedGroups[realIdxNum].Length == 0))
            {
                continue;
            }

            long[] treePages = await this.TryCollectIndexTreePagesAsync(layout, tdefPage, rootPage, cancellationToken).ConfigureAwait(false) ?? [];
            var reclaimable = new HashSet<long>(treePages);
            if (pointers[realIdxNum] != default && mappedGroups[realIdxNum].Any(page => !reclaimable.Contains(page)))
            {
                UsageMapOwnership.Proof proof;
                try
                {
                    proof = await UsageMapOwnership.InspectAsync(format, tableDefs, catalogRows, pager, tdefPage, entry.FirstDpOffset - 4, cancellationToken).ConfigureAwait(false);
                }
                catch (JetCorruptDataException)
                {
                    return CreateEmptyPageGroups(numRealIdx);
                }

                if (proof.IsExclusiveRow)
                {
                    reclaimable.UnionWith(mappedGroups[realIdxNum]);
                }

                // Keep the traversed old tree even when its original map
                // row was shared. All aliases may detach in this rebuild;
                // the postpublication walk protects any surviving reference.
            }

            groups[realIdxNum] = [.. reclaimable];
        }

        return groups;
    }

    private async ValueTask DeallocateReplacedIndexPagesAsync(
        long tdefPage,
        long[][] oldIndexPageGroups,
        long[][] rebuiltIndexPageGroups,
        CancellationToken cancellationToken)
    {
        var newPages = new HashSet<long>();
        for (int realIdxNum = 0; realIdxNum < rebuiltIndexPageGroups.Length; realIdxNum++)
        {
            foreach (long pageNumber in rebuiltIndexPageGroups[realIdxNum])
            {
                _ = newPages.Add(pageNumber);
            }
        }

        bool hasReplacedPages = false;
        int groupCount = Math.Min(oldIndexPageGroups.Length, rebuiltIndexPageGroups.Length);
        for (int realIdxNum = 0; realIdxNum < groupCount && !hasReplacedPages; realIdxNum++)
        {
            hasReplacedPages = rebuiltIndexPageGroups[realIdxNum].Length > 0
                && oldIndexPageGroups[realIdxNum].Any(page => page > 2 && !newPages.Contains(page));
        }

        if (!hasReplacedPages)
        {
            return;
        }

        // Include every current descriptor, even one not maintained in this
        // pass. A page shared with another live tree must stay allocated.
        (TdefPreambleStatus status, TdefPreamble current) = await this.ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (status != TdefPreambleStatus.Ok)
        {
            return;
        }

        for (int realIdxNum = 0; realIdxNum < current.NumRealIdx; realIdxNum++)
        {
            if (!format.Index.TryReadRealIdxSlot(current.Buffer, current.RealIdxDescStart, realIdxNum, out RealIdxSlot slot))
            {
                return;
            }

            long rootPage = (uint)Ri32(current.Buffer, slot.FirstDpOffset);
            if (rootPage == 0)
            {
                continue;
            }

            long[]? reachable = await this.TryCollectIndexTreePagesAsync(format.IndexPage, tdefPage, rootPage, cancellationToken).ConfigureAwait(false);
            if (reachable is null)
            {
                return;
            }

            newPages.UnionWith(reachable);
        }

        HashSet<long>? mappedPages = await this.TryReadCurrentIndexMapProtectionAsync(tdefPage, current, cancellationToken).ConfigureAwait(false);
        if (mappedPages is null)
        {
            return;
        }

        newPages.UnionWith(mappedPages);
        var deallocatedPages = new HashSet<long>();
        for (int realIdxNum = 0; realIdxNum < groupCount; realIdxNum++)
        {
            if (rebuiltIndexPageGroups[realIdxNum].Length == 0 || oldIndexPageGroups[realIdxNum].Length == 0)
            {
                continue;
            }

            foreach (long oldPageNumber in oldIndexPageGroups[realIdxNum])
            {
                if (oldPageNumber > 2
                    && !newPages.Contains(oldPageNumber)
                    && deallocatedPages.Add(oldPageNumber)
                    && await this.IsReplacedIndexPageAsync(oldPageNumber, tdefPage, cancellationToken).ConfigureAwait(false))
                {
                    await pageAllocator.DeallocatePageAsync(oldPageNumber, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    private async ValueTask<bool> IsReplacedIndexPageAsync(long pageNumber, long tdefPage, CancellationToken cancellationToken)
    {
        if (pageNumber <= 0 || pageNumber >= pager.PageCount)
        {
            return false;
        }

        byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return page[0] is Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf
                && Ri32(page, 4) == tdefPage;
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private async ValueTask<bool> CanReuseSingleLeafPageAsync(int pageNumber, long tdefPage, CancellationToken cancellationToken)
    {
        if (pageNumber <= 0 || pageNumber >= pager.PageCount)
        {
            return false;
        }

        byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return page[0] == Constants.PageTypes.IndexLeaf && Ri32(page, 4) == tdefPage;
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    /// <summary>
    /// Collects, for each maintained real index, every page its tree now
    /// spans. Reads pages without changing <paramref name="tdefBuffer"/> or
    /// writing pages, so the caller can still bail and release what it reserved.
    /// Returns an empty array when no slot was maintained, and <see langword="null"/> when a
    /// tree cannot be walked or the table has no usage-map page.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tdefBuffer">The logical TDEF bytes naming the maintained indexes.</param>
    /// <param name="layout">The index page layout.</param>
    /// <param name="slots">The maintained real-index slots.</param>
    /// <param name="numRealIdx">The number of real indexes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<long[][]?> TryCollectIncrementalIndexPageGroupsAsync(
        long tdefPage,
        byte[] tdefBuffer,
        IndexPageLayout layout,
        List<(int RealIdxNum, RealIdxEntry Entry)> slots,
        int numRealIdx,
        CancellationToken cancellationToken)
    {
        if (slots.Count == 0)
        {
            return [];
        }

        long usageMapPage = this.ReadTableUsageMapPage(tdefBuffer);
        if (!format.IsJet3 && (usageMapPage <= 0 || usageMapPage >= pager.PageCount))
        {
            return null;
        }

        long[][] indexPageGroups = new long[numRealIdx][];
        for (int i = 0; i < indexPageGroups.Length; i++)
        {
            indexPageGroups[i] = [];
        }

        foreach ((int realIdxNum, RealIdxEntry entry) in slots)
        {
            if (realIdxNum < 0 || realIdxNum >= numRealIdx)
            {
                return null;
            }

            long rootPage = (uint)Ri32(tdefBuffer, entry.FirstDpOffset);
            if (rootPage <= 0)
            {
                continue;
            }

            long[]? pageGroup = await this.TryCollectIndexTreePagesAsync(layout, tdefPage, rootPage, cancellationToken).ConfigureAwait(false);
            if (pageGroup is null)
            {
                return null;
            }

            indexPageGroups[realIdxNum] = pageGroup;
        }

        return indexPageGroups;
    }

    /// <summary>
    /// Writes the index usage-map rows collected by
    /// <see cref="TryCollectIncrementalIndexPageGroupsAsync"/>, as REFERENCE
    /// rows where a tree spans more than one INLINE window. Aliased
    /// storage is detached before the refreshed map is written.
    /// </summary>
    /// <param name="tdefPage">The owning table definition.</param>
    /// <param name="tdefBuffer">The logical TDEF bytes, which name the table's usage-map page.</param>
    /// <param name="slots">The maintained real-index descriptors.</param>
    /// <param name="indexPageGroups">The pages of each real index's tree, by real-index number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask WriteIncrementalIndexUsageMapsAsync(long tdefPage, byte[] tdefBuffer, List<(int RealIdxNum, RealIdxEntry Entry)> slots, long[][] indexPageGroups, CancellationToken cancellationToken)
    {
        if (indexPageGroups.Length == 0)
        {
            return;
        }

        var indexPointers = new UsageMapPointer[indexPageGroups.Length];
        foreach ((int realIdxNum, RealIdxEntry entry) in slots)
        {
            _ = UsageMap.TryReadPointer(tdefBuffer, entry.FirstDpOffset - 4, out indexPointers[realIdxNum]);
        }

        long[][] mapPageGroups = await this.FilterUnchangedIndexMapsAsync(indexPointers, indexPageGroups, cancellationToken).ConfigureAwait(false);
        if (!HasAnyIndexPageGroup(mapPageGroups))
        {
            return;
        }

        var entriesByNumber = new Dictionary<int, RealIdxEntry>();
        foreach ((int realIdxNum, RealIdxEntry entry) in slots)
        {
            if (mapPageGroups[realIdxNum].Length > 0)
            {
                entriesByNumber.Add(realIdxNum, entry);
            }
        }

        await this.DetachAliasedIndexMapsAsync(tdefPage, entriesByNumber, indexPointers, cancellationToken).ConfigureAwait(false);
        indexPointers = await dataPages.UpdateTableIndexUsageMapRowsAsync(
            this.ReadTableUsageMapPage(tdefBuffer), indexPointers, mapPageGroups, cancellationToken).ConfigureAwait(false);
        foreach ((int realIdxNum, RealIdxEntry entry) in slots)
        {
            if (mapPageGroups[realIdxNum].Length > 0)
            {
                UsageMapPointer pointer = indexPointers[realIdxNum];
                WriteIndexUsageMapPointer(tdefBuffer, entry.FirstDpOffset - 4, pointer.RowIndex, pointer.PageNumber);
            }
        }
    }

    /// <summary>Elides map writes only when strict membership already equals the current tree.</summary>
    /// <param name="pointers">The current physical map pointers.</param>
    /// <param name="treePageGroups">The complete current trees, retained unchanged for reclamation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Write groups with empty entries for maps that need no edit.</returns>
    private async ValueTask<long[][]> FilterUnchangedIndexMapsAsync(UsageMapPointer[] pointers, long[][] treePageGroups, CancellationToken cancellationToken)
    {
        long[][] writes = (long[][])treePageGroups.Clone();
        var mapPages = new Dictionary<int, byte[]>();
        try
        {
            for (int index = 0; index < treePageGroups.Length; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (treePageGroups[index].Length == 0)
                {
                    continue;
                }

                List<long>? members = await this.TryReadStrictIndexMapMembersAsync(pointers[index], mapPages, cancellationToken).ConfigureAwait(false);
                if (members is null || members.Count != treePageGroups[index].Length)
                {
                    continue;
                }

                var uniqueMembers = new HashSet<long>(members);
                if (uniqueMembers.Count == members.Count && uniqueMembers.SetEquals(treePageGroups[index]))
                {
                    // Shared storage is safe to retain because neither
                    // its row nor any bitmap will be written in this pass.
                    writes[index] = [];
                }
            }

            return writes;
        }
        finally
        {
            foreach (byte[] page in mapPages.Values)
            {
                PageBuffers.Return(page);
            }
        }
    }

    private async ValueTask<List<long>?> TryReadStrictIndexMapMembersAsync(UsageMapPointer pointer, Dictionary<int, byte[]> mapPages, CancellationToken cancellationToken)
    {
        if (pointer.PageNumber <= 1 || pointer.PageNumber >= pager.PageCount)
        {
            return null;
        }

        try
        {
            if (!mapPages.TryGetValue(pointer.PageNumber, out byte[]? page))
            {
                page = await pager.ReadPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
                mapPages.Add(pointer.PageNumber, page);
            }

            if (page[0] != Constants.PageTypes.Data || page[1] != 1 || Ri32(page, format.DataPage.TDefOff) != 0
                || !UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row)
                || row.RowSize < 6
                || (Ru16(page, format.DataPage.RowsStart + (pointer.RowIndex * 2)) & ~Constants.DataPage.RowOffsetMask) != 0)
            {
                return null;
            }

            var members = new List<long>();
            return await UsageMap.TryEnumeratePagesAsync(page, row, format.PageSize, pager.PageCount, 1, true, pager.ReadPageAsync, PageBuffers.Return, members, cancellationToken).ConfigureAwait(false)
                ? members
                : null;
        }
        catch (JetCorruptDataException)
        {
            // A no-change decision requires a completely readable target.
            return null;
        }
    }

    private async ValueTask<HashSet<long>?> TryReadCurrentIndexMapProtectionAsync(long tdefPage, TdefPreamble current, CancellationToken cancellationToken)
    {
        var mapPages = new Dictionary<int, byte[]>();
        var mappedPages = new HashSet<long>();
        int proofOffset = -1;
        try
        {
            for (int realIdxNum = 0; realIdxNum < current.NumRealIdx; realIdxNum++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!format.Index.TryReadRealIdxSlot(current.Buffer, current.RealIdxDescStart, realIdxNum, out RealIdxSlot slot))
                {
                    return null;
                }

                int offset = slot.FirstDpOffset - 4;
                if (Ri32(current.Buffer, offset) == 0)
                {
                    continue;
                }

                if (!UsageMap.TryReadPointer(current.Buffer, offset, out UsageMapPointer pointer))
                {
                    return null;
                }

                List<long>? members = await this.TryReadStrictIndexMapMembersAsync(pointer, mapPages, cancellationToken).ConfigureAwait(false);
                if (members is null)
                {
                    return null;
                }

                mappedPages.UnionWith(members);
                if (proofOffset < 0)
                {
                    proofOffset = offset;
                }
            }
        }
        finally
        {
            foreach (byte[] page in mapPages.Values)
            {
                PageBuffers.Return(page);
            }
        }

        if (proofOffset < 0)
        {
            return null;
        }

        try
        {
            // One full graph inspection includes all other map memberships.
            // Every current descriptor was also validated above, because an
            // invalid row can be omitted from that conservative graph scan.
            UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, tableDefs, catalogRows, pager, tdefPage, proofOffset, cancellationToken).ConfigureAwait(false);
            mappedPages.UnionWith(proof.Pages);
            mappedPages.UnionWith(proof.OtherMappedPages);
            return mappedPages;
        }
        catch (JetCorruptDataException)
        {
            return null;
        }
    }

    /// <summary>Gives a maintained index a private map row when another definition or bitmap aliases its storage.</summary>
    /// <param name="tdefPage">The owning table definition.</param>
    /// <param name="entries">The maintained index descriptors.</param>
    /// <param name="pointers">Map pointers to retain or replace with a fresh row.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async ValueTask DetachAliasedIndexMapsAsync(long tdefPage, Dictionary<int, RealIdxEntry> entries, UsageMapPointer[] pointers, CancellationToken cancellationToken)
    {
        KeyValuePair<int, RealIdxEntry>[] targets = entries.Where(entry => pointers[entry.Key] != default).ToArray();
        if (targets.Length == 0)
        {
            return;
        }

        UsageMapOwnership.Proof[] proofs;
        try
        {
            int[] offsets = targets.Select(target => target.Value.FirstDpOffset - 4).ToArray();
            proofs = await UsageMapOwnership.InspectForMutationBatchAsync(format, tableDefs, catalogRows, pager, tdefPage, offsets, cancellationToken).ConfigureAwait(false);
        }
        catch (JetCorruptDataException)
        {
            // Uncertain metadata requires private rows for this entire read-only
            // phase. Reclamation retains every uncertain original page.
            foreach (KeyValuePair<int, RealIdxEntry> target in targets)
            {
                pointers[target.Key] = default;
            }

            return;
        }

        for (int index = 0; index < targets.Length; index++)
        {
            if (!proofs[index].IsExclusiveRow || !proofs[index].HasExclusiveBitmapPages)
            {
                pointers[targets[index].Key] = default;
            }
        }
    }

    /// <summary>Collects every current index page of a private schema-rewrite replacement.</summary>
    /// <param name="tdefPage">The replacement table's first TDEF page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The distinct pages reachable from all physical index roots and maps, in page order.</returns>
    /// <exception cref="JetCorruptDataException">An index descriptor, map or reachable tree cannot be traversed safely.</exception>
    internal async ValueTask<long[]> CollectRewriteIndexPagesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        (TdefPreambleStatus status, TdefPreamble preamble) = await this.ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (preamble.NumIdx == 0 && preamble.NumRealIdx == 0)
        {
            return [];
        }

        if (status != TdefPreambleStatus.Ok)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement table at page {tdefPage} has unreadable index definitions.", new JetErrorInfo { PageNumber = tdefPage });
        }

        // Older writer output can retain original map members outside the
        // current tree; schema rewrites must collect those pages as well.
        var pages = new SortedSet<long>();
        for (int realIdxNum = 0; realIdxNum < preamble.NumRealIdx; realIdxNum++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!format.Index.TryReadRealIdxSlot(preamble.Buffer, preamble.RealIdxDescStart, realIdxNum, out RealIdxSlot slot))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement table at page {tdefPage} has an unreadable physical index descriptor {realIdxNum}.", new JetErrorInfo { PageNumber = tdefPage });
            }

            long rootPage = Ru32(preamble.Buffer, slot.FirstDpOffset);
            long[] treePages = await this.TryCollectIndexTreePagesAsync(format.IndexPage, tdefPage, rootPage, cancellationToken).ConfigureAwait(false)
                ?? throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement table's index {realIdxNum} rooted at page {rootPage} cannot be traversed safely.", new JetErrorInfo { PageNumber = rootPage });

            pages.UnionWith(treePages);
            if (!UsageMap.TryReadPointer(preamble.Buffer, slot.FirstDpOffset - 4, out UsageMapPointer pointer))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement table at page {tdefPage} has an unreadable index map pointer {realIdxNum}.", new JetErrorInfo { PageNumber = tdefPage });
            }

            // Fresh FK leaves acquire their map during the bulk rebuild.
            if (pointer.PageNumber == 0 && pointer.RowIndex == 0)
            {
                continue;
            }

            List<long> mappedPages = await this.ReadRequiredRewriteIndexMapAsync(pointer, cancellationToken).ConfigureAwait(false);
            foreach (long pageNumber in mappedPages)
            {
                if (pages.Contains(pageNumber))
                {
                    continue;
                }

                if (!await this.IsReplacedIndexPageAsync(pageNumber, tdefPage, cancellationToken).ConfigureAwait(false))
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement table's index {realIdxNum} map names page {pageNumber} with an invalid type or owner.", new JetErrorInfo { PageNumber = pageNumber });
                }

                pages.Add(pageNumber);
            }
        }

        return [.. pages];
    }

    private async ValueTask<List<long>> ReadRequiredRewriteIndexMapAsync(UsageMapPointer pointer, CancellationToken cancellationToken)
    {
        if (pointer.PageNumber <= 2 || pointer.PageNumber >= pager.PageCount)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement index map page {pointer.PageNumber} lies outside the database.", new JetErrorInfo { PageNumber = pointer.PageNumber });
        }

        byte[] page = await pager.ReadPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.Data
                || !UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound rowBound))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement index map at page {pointer.PageNumber}, row {pointer.RowIndex} cannot be read.", new JetErrorInfo { PageNumber = pointer.PageNumber });
            }

            var pageNumbers = new List<long>();
            bool recognized = await UsageMap.TryEnumeratePagesAsync(
                page,
                rowBound,
                format.PageSize,
                pager.PageCount,
                minimumPageNumber: 3,
                strict: true,
                pager.ReadPageAsync,
                PageBuffers.Return,
                pageNumbers,
                cancellationToken).ConfigureAwait(false);
            if (!recognized || pageNumbers.Count == 0)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The replacement index map at page {pointer.PageNumber}, row {pointer.RowIndex} has no safely traversable index pages.", new JetErrorInfo { PageNumber = pointer.PageNumber });
            }

            return pageNumbers;
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private async ValueTask<long[]?> TryCollectIndexTreePagesAsync(
        IndexPageLayout layout,
        long tdefPage,
        long rootPage,
        CancellationToken cancellationToken)
    {
        long pageCount = pager.PageCount;
        var pages = new List<long>();
        var seen = new HashSet<long>();
        var stack = new Stack<long>();
        stack.Push(rootPage);

        while (stack.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long pageNumber = stack.Pop();
            if (pageNumber <= 2 || pageNumber >= pageCount)
            {
                return null;
            }

            if (!seen.Add(pageNumber))
            {
                continue;
            }

            byte[] page = await pager.ReadPageCopyAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (Ri32(page, 4) != tdefPage)
            {
                return null;
            }

            pages.Add(pageNumber);
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                continue;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return null;
            }

            if (!IndexPageCodec.TryDecodeIntermediateEntries(layout, page, format.PageSize, out List<DecodedIntermediateEntry> entries))
            {
                return null;
            }

            long tailPage = IndexPageCodec.ReadTailPage(layout, page);
            if (tailPage != 0)
            {
                stack.Push(tailPage);
            }

            if (entries.Count == 0 && tailPage == 0)
            {
                return null;
            }

            for (int i = entries.Count - 1; i >= 0; i--)
            {
                stack.Push(entries[i].ChildPage);
            }
        }

        pages.Sort();
        return pages.ToArray();
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
    /// every index from a fresh snapshot. Any incremental work this method
    /// already wrote is harmless: a rebuilt tree it reserved and wrote but
    /// had not yet linked through the TDEF is given back to the global usage
    /// map before it returns, and a tree it had already linked (or replaced)
    /// is retained if the call bails. Successful maintenance releases replaced
    /// pages after protecting current roots and surviving usage maps.
    /// <para>
    /// Two flavours of fast path are attempted per real-idx:
    /// </para>
    /// <list type="bullet">
    ///   <item><b>Single-leaf splice.</b> Root is a leaf
    ///   (<c>page_type = 0x04</c>) with no sibling pointers. The leaf is
    ///   decoded and spliced; when the post-mutation entry list still fits on
    ///   one page it is rewritten in place, and when it no longer does, the
    ///   entries are built into a fresh multi-level tree (as below) and
    ///   <c>first_dp</c> is patched to its root.</item>
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
    /// Falls back when: the TDEF's index section cannot be parsed; the table
    /// carries a foreign-key logical index; the encoder rejects any value
    /// (text outside General Legacy, oversized numeric mantissa, etc.); the
    /// index page chain is malformed; or the spliced entry list cannot be
    /// repacked (e.g. a single entry exceeds the payload area). Wide tables
    /// whose TDEF spans several pages take the fast path like any other: the
    /// whole chain is read, and <c>first_dp</c> / <c>used_pages</c> patches
    /// are written back to whichever page holds them.
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
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="insertedRows">The inserted rows.</param>
    /// <param name="deletedRows">The deleted rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<bool> TryMaintainIndexesIncrementalAsync(
        long tdefPage,
        TableDef tableDef,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        CancellationToken cancellationToken)
    {
        this.LastIncrementalBail = null;
        if (tdefPage == 2)
        {
            await this.ThrowIfIndexesUnmaintainableAsync(tdefPage, tableDef, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        }

        int addCount = insertedRows?.Count ?? 0;
        int delCount = deletedRows?.Count ?? 0;
        if (addCount == 0 && delCount == 0)
        {
            return true;
        }

        // Rebuilt trees are reserved and written as each index is processed,
        // but only linked by a TDEF write (mid-loop or at the end). Whatever
        // is still unlinked when the call bails or throws goes back to the
        // global usage map.
        var runs = new ReservedPageRuns(pageAllocator);
        try
        {
            return await this.TryMaintainIndexesIncrementalCoreAsync(
                tdefPage,
                tableDef,
                insertedRows,
                deletedRows,
                runs,
                preserveStoredKeys: false,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (!runs.IsEmpty)
            {
                await runs.ReleaseAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Moves a catalog row's index pointers while preserving its unchanged native keys.</summary>
    /// <param name="tableDef">The catalog definition.</param>
    /// <param name="original">The original row.</param>
    /// <param name="replacement">The replacement row at its new location.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether every catalog index was maintained.</returns>
    internal async ValueTask<bool> TryRelocateCatalogRowAsync(TableDef tableDef, LocatedRow original, LocatedRow replacement, CancellationToken cancellationToken)
    {
        this.LastIncrementalBail = null;
        await this.ThrowIfIndexesUnmaintainableAsync(2, tableDef, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        var runs = new ReservedPageRuns(pageAllocator);
        try
        {
            return await this.TryMaintainIndexesIncrementalCoreAsync(
                2,
                tableDef,
                [(replacement.Location, replacement.Values)],
                [(original.Location, original.Values)],
                runs,
                preserveStoredKeys: true,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (!runs.IsEmpty)
            {
                await runs.ReleaseAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The body of <see cref="TryMaintainIndexesIncrementalAsync"/>. Every run
    /// it reserves is recorded in <paramref name="runs"/>, and it calls
    /// <see cref="ReservedPageRuns.MarkLinked"/> before each write that links
    /// them, so the caller can release whatever is left when it returns.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="insertedRows">The inserted rows.</param>
    /// <param name="deletedRows">The deleted rows.</param>
    /// <param name="runs">The reserved runs not yet linked through the TDEF.</param>
    /// <param name="preserveStoredKeys">Whether only one unchanged catalog row pointer moves.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> TryMaintainIndexesIncrementalCoreAsync(
        long tdefPage,
        TableDef tableDef,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        ReservedPageRuns runs,
        bool preserveStoredKeys,
        CancellationToken cancellationToken)
    {
        // Jet3 (.mdb Access 97) participates in the
        // incremental fast paths via the per-format IndexPageLayout descriptor
        // (page size 2048, bitmask at 0x16, first entry at 0xF8) and the §3.1
        // 39-byte real-idx physical descriptor (first_dp at phys+34 instead
        // of phys+38). The change-set encode + splice + rebuild logic is
        // unchanged; only the layout-dependent byte offsets and page builder
        // calls fork on `jet3`. Replaced pages are reclaimed only after the
        // maintained roots are linked, preserving native map-owned pages.
        IndexLayout idxLayout = format.Index;
        IndexPageLayout layout = format.IndexPage;

        int addCount = insertedRows?.Count ?? 0;
        int delCount = deletedRows?.Count ?? 0;

        (TdefPreambleStatus preStatus, TdefPreamble preamble) = await this.ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        switch (preStatus)
        {
            case TdefPreambleStatus.Ok:
                break;
            case TdefPreambleStatus.Empty:
                return true;
            case TdefPreambleStatus.TooMany:
                this.LastIncrementalBail = $"NumIdx_TooMany numIdx={preamble.NumIdx} numRealIdx={preamble.NumRealIdx}";
                return false;
            case TdefPreambleStatus.ColumnNameWalkFailed:
                this.LastIncrementalBail = $"C0 col-name walk failed numCols={preamble.NumCols}";
                return false;
            default:
                return false;
        }

        var metadata = new IncrementalMetadataState(preamble.Chain);
        int numIdx = preamble.NumIdx;
        int numRealIdx = preamble.NumRealIdx;
        int realIdxDescStart = preamble.RealIdxDescStart;
        int logIdxStart = idxLayout.LogicalIdxStart(realIdxDescStart, numRealIdx);

        // Access Compact & Repair has rejected incrementally maintained
        // relationship-backed indexes in probe validation; keep those tables
        // on the bulk rebuild path until the FK incremental layout is proven
        // against Access-authored repair output.
        for (int li = 0; li < numIdx; li++)
        {
            if (!idxLayout.TryReadLogicalEntry(metadata.Buffer, logIdxStart, li, out LogicalIdxEntry entry))
            {
                this.LastIncrementalBail = $"C1b li={li} logIdxStart={logIdxStart} bufLen={metadata.Buffer.Length}";
                return false;
            }

            if (entry.IndexType == IndexKind.ForeignKey)
            {
                this.LastIncrementalBail = "C1c foreign-key logical index present";
                return false;
            }
        }

        // Decode every real-idx slot's key columns + first_dp offset.
        var slots = new List<(int RealIdxNum, RealIdxEntry Entry)>(numRealIdx);
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            if (!idxLayout.TryReadRealIdxSlotWithKeyColumns(metadata.Buffer, realIdxDescStart, ri, out RealIdxSlot slot, out List<KeyColumn>? keyCols))
            {
                this.LastIncrementalBail = $"C1 ri={ri} realIdxDescStart={realIdxDescStart} bufLen={metadata.Buffer.Length}";
                return false;
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            slots.Add((ri, slot.ToEntry(keyCols, overrideUnique: false)));
        }

        if (slots.Count == 0)
        {
            this.LastIncrementalBail = $"C1d no usable real-idx slots numIdx={numIdx} numRealIdx={numRealIdx}";
            return false;
        }

        if (format.IsJet3)
        {
            var entriesByNumber = new Dictionary<int, RealIdxEntry>();
            foreach ((int realIdxNum, RealIdxEntry entry) in slots)
            {
                entriesByNumber.Add(realIdxNum, entry);
            }

            metadata.ReplacedPageGroups = await this.CollectJet3ReplacedTreePagesAsync(tdefPage, metadata.Buffer, layout, entriesByNumber, numRealIdx, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            metadata.ReplacedPageGroups = await this.TryCollectIncrementalIndexPageGroupsAsync(tdefPage, metadata.Buffer, layout, slots, numRealIdx, cancellationToken).ConfigureAwait(false);
        }

        Dictionary<int, int> snapshotIndexByColNum = IndexCatalogReader.BuildColumnNumberToSnapshotIndex(tableDef.Columns);

        foreach ((_, RealIdxEntry rie) in slots)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Resolve key columns to (ColumnInfo, snapshot index, ascending).
            if (!IndexLayout.TryResolveKeyColumnInfos(rie.IndexKeyColumns, tableDef.Columns, snapshotIndexByColNum, out List<KeyColumnInfo>? keyColInfos))
            {
                this.LastIncrementalBail = "C2 resolveFailed";
                return false;
            }

            // Read the index root; require a single-leaf root.
            long firstDp = (uint)Ri32(metadata.Buffer, rie.FirstDpOffset);
            if (firstDp <= 0)
            {
                this.LastIncrementalBail = $"C3 firstDp={firstDp}";
                return false;
            }

            byte[] rootPage = await pager.ReadPageCopyAsync(firstDp, cancellationToken).ConfigureAwait(false);

            if (preserveStoredKeys)
            {
                foreach (KeyColumnInfo key in keyColInfos)
                {
                    if (!Equals(deletedRows![0].Row[key.SnapIdx], insertedRows![0].Row[key.SnapIdx]))
                    {
                        this.LastIncrementalBail = "A preserved catalog index value was assigned.";
                        return false;
                    }
                }

                List<IndexEntry> stored = await this.ReadStoredIndexEntriesAsync(layout, firstDp, rootPage, cancellationToken).ConfigureAwait(false);
                for (int entryIndex = 1; entryIndex < stored.Count; entryIndex++)
                {
                    if (IndexPageCodec.CompareKeyBytes(stored[entryIndex - 1].Key, stored[entryIndex].Key) >= 0)
                    {
                        this.LastIncrementalBail = "Preserved native catalog keys must be unique and ordered.";
                        return false;
                    }
                }

                RowLocation oldLocation = deletedRows![0].Loc;
                RowLocation newLocation = insertedRows![0].Loc;
                int match = stored.FindIndex(candidate => candidate.DataPage == oldLocation.PageNumber && candidate.DataRow == oldLocation.RowIndex);
                if (match < 0)
                {
                    if (rie.IgnoreNulls && IndexHelpers.CountNullKeyColumns(keyColInfos, deletedRows[0].Row) == keyColInfos.Count)
                    {
                        continue;
                    }

                    this.LastIncrementalBail = "The original catalog row has no stored index entry.";
                    return false;
                }

                if (stored.FindIndex(match + 1, candidate => candidate.DataPage == oldLocation.PageNumber && candidate.DataRow == oldLocation.RowIndex) >= 0)
                {
                    this.LastIncrementalBail = "The original catalog row has duplicate stored index entries.";
                    return false;
                }

                stored[match] = stored[match] with { DataPage = newLocation.PageNumber, DataRow = checked((byte)newLocation.RowIndex) };
                byte[]? relocatedLeaf = IndexPageCodec.IsSingleRootLeaf(layout, rootPage)
                    ? IndexPageCodec.TryBuildLeafPage(layout, format.PageSize, tdefPage, stored)
                    : null;
                if (relocatedLeaf is not null)
                {
                    await pager.WritePageAsync(firstDp, relocatedLeaf, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    IndexBTreeBuildResult? relocated = await this.btreeEditor.TryPlaceTreeAsync(layout, tdefPage, stored, runs, cancellationToken).ConfigureAwait(false);
                    if (relocated is not { } tree)
                    {
                        this.LastIncrementalBail = "The preserved catalog index tree could not be rebuilt.";
                        return false;
                    }

                    Wi32(metadata.Buffer, rie.FirstDpOffset, checked((int)tree.RootPageNumber));
                    metadata.Dirty = true;
                }

                continue;
            }

            // Encode the change-set keys for this index. Used by both the
            // single-leaf splice and the multi-level rebuild path below.
            List<IndexEntry>? addEntries = this.EncodeHintEntries(insertedRows, keyColInfos, rie.IgnoreNulls);
            if (addEntries is null)
            {
                // Encoder rejected at least one row; bail to bulk.
                this.LastIncrementalBail = "C4 key encoding failed";
                return false;
            }

            // Encode the deleted rows' keys too. The single-leaf and bulk
            // paths only need the (page, row) pointers (they re-derive the
            // key from the live leaf entry); the surgical multi-level path
            // needs the keys to perform a path-capturing descent that
            // confirms every change targets the same leaf.
            List<IndexEntry>? removeEntries = this.EncodeHintEntries(deletedRows, keyColInfos, rie.IgnoreNulls);
            if (removeEntries is null)
            {
                this.LastIncrementalBail = "C5";
                return false;
            }

            List<(long DataPage, byte DataRow)> removePtrs = new(delCount);
            foreach ((_, long dpDel, byte drDel) in removeEntries)
            {
                removePtrs.Add((dpDel, drDel));
            }

            if (!IndexPageCodec.IsSingleRootLeaf(layout, rootPage))
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
                if (rootPage[0] is not Constants.IndexLeafPage.PageTypeIntermediate
                    and not Constants.IndexLeafPage.PageTypeLeaf)
                {
                    this.LastIncrementalBail = $"C6 rootPage[0]={rootPage[0]:X2}";
                    return false;
                }

                // Append-only tail-page fast path. When
                // the change-set is insert-only AND every new key sorts
                // strictly after the current tail-leaf max key, splice the
                // new entries into the tail leaf and rewrite that one page.
                // No descend-walk-rebuild, no sibling-chain updates, no
                // intermediate writes — the rightmost intermediate summary
                // becomes (one entry) stale, which the cursor compensates
                // for by following the intermediate's tail_page header on
                // overshoot. Falls through to the bulk rebuild on overflow,
                // deletes, out-of-order inserts, missing tail_page, or any
                // malformed page.
                if (delCount == 0 && addEntries.Count > 0)
                {
                    bool tailHandled = await this.btreeEditor.TryAppendToTailLeafAsync(
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

                // Link any earlier rebuilt index before the editor can install a new root.
                if (metadata.Dirty)
                {
                    runs.MarkLinked();
                    await tdefWriter.WriteChainInPlaceAsync(metadata.Chain, cancellationToken).ConfigureAwait(false);
                    metadata.Dirty = false;
                }

                (bool handled, bool rootChanged) = await this.btreeEditor.TrySurgicalMultiLevelMaintainAsync(
                    layout, tdefPage, firstDp, rie.FirstDpOffset, addEntries, removeEntries, cancellationToken).ConfigureAwait(false);
                if (handled)
                {
                    if (rootChanged)
                    {
                        metadata.Chain = await tableDefs.ReadTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
                    }

                    continue;
                }

                long leftmostLeaf = await this.btreeEditor.DescendToLeftmostLeafAsync(layout, firstDp, cancellationToken).ConfigureAwait(false);
                if (leftmostLeaf <= 0)
                {
                    this.LastIncrementalBail = $"C7 firstDp={firstDp}";
                    return false;
                }

                var allExisting = new List<IndexEntry>();
                long walkPage = leftmostLeaf;
                int safetyBudget = 1_000_000; // arbitrary upper bound on leaf count
                while (walkPage > 0)
                {
                    if (--safetyBudget <= 0)
                    {
                        this.LastIncrementalBail = "C8 safetyBudget";
                        return false;
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    byte[] leaf = await pager.ReadPageCopyAsync(walkPage, cancellationToken).ConfigureAwait(false);

                    if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
                    {
                        this.LastIncrementalBail = $"C9 walkPage={walkPage} leaf[0]={leaf[0]:X2}";
                        return false;
                    }

                    allExisting.AddRange(tdefPage == 2 ? DecodeRequiredCatalogLeaf(walkPage, leaf, format) : IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize));
                    walkPage = IndexPageCodec.ReadNextPage(layout, leaf);
                }

                List<IndexEntry>? splicedAll = IndexEntrySplicer.Splice(allExisting, addEntries, removePtrs);
                if (splicedAll is null)
                {
                    this.LastIncrementalBail = $"C10 allExisting={allExisting.Count}";
                    return false;
                }

                // Build at the provisional end of file, reserve that many
                // pages, rebuild when the allocator hands back a reused free
                // run or a different end of file (the child and sibling
                // pointers inside the new tree must name the pages it lands
                // on), and write the pages. The run stays in `runs` until a
                // TDEF write links it.
                IndexBTreeBuildResult? mlBuild = await this.btreeEditor.TryPlaceTreeAsync(layout, tdefPage, splicedAll, runs, cancellationToken).ConfigureAwait(false);
                if (mlBuild is not { } placedTree)
                {
                    this.LastIncrementalBail = $"C11 rebuilt tree does not fit spliced={splicedAll.Count}";
                    return false;
                }

                Wi32(metadata.Buffer, rie.FirstDpOffset, checked((int)placedTree.RootPageNumber));
                metadata.Dirty = true;
                continue;
            }

            List<IndexEntry> existing = tdefPage == 2 ? DecodeRequiredCatalogLeaf(firstDp, rootPage, format) : IndexPageCodec.DecodeLeafEntries(layout, rootPage, format.PageSize);
            List<IndexEntry>? spliced = IndexEntrySplicer.Splice(existing, addEntries, removePtrs);
            if (spliced is null)
            {
                this.LastIncrementalBail = $"C12 existing={existing.Count}";
                return false;
            }

            byte[]? newLeaf = IndexPageCodec.TryBuildLeafPage(layout, format.PageSize, tdefPage, spliced);
            if (newLeaf is null)
            {
                // The root leaf overflows: grow the index into a multi-level
                // tree built from the leaf's own entries plus the change set,
                // as the multi-level path does, and point first_dp at its
                // root. Nothing is re-encoded from the table's rows, so system
                // tables, which have no bulk fallback, grow too. The old root
                // leaf is reclaimed after the metadata commit when safe.
                IndexBTreeBuildResult? grown = await this.btreeEditor.TryPlaceTreeAsync(layout, tdefPage, spliced, runs, cancellationToken).ConfigureAwait(false);
                if (grown is not { } grownTree)
                {
                    this.LastIncrementalBail = $"C13 spliced={spliced.Count}";
                    return false;
                }

                Wi32(metadata.Buffer, rie.FirstDpOffset, checked((int)grownTree.RootPageNumber));
                metadata.Dirty = true;
                continue;
            }

            await pager.WritePageAsync(firstDp, newLeaf, cancellationToken).ConfigureAwait(false);
        }

        return await this.CommitIncrementalMetadataAsync(tdefPage, layout, slots, numRealIdx, metadata, runs, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<List<IndexEntry>> ReadStoredIndexEntriesAsync(IndexPageLayout layout, long root, byte[] rootPage, CancellationToken cancellationToken)
    {
        if (IndexPageCodec.IsSingleRootLeaf(layout, rootPage))
        {
            return DecodeRequiredCatalogLeaf(root, rootPage, format);
        }

        long pageNumber = await this.btreeEditor.DescendToLeftmostLeafAsync(layout, root, cancellationToken).ConfigureAwait(false);
        var seen = new HashSet<long>();
        var entries = new List<IndexEntry>();
        while (pageNumber > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (pageNumber >= pager.PageCount || !seen.Add(pageNumber))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The preserved catalog index contains an invalid leaf chain.", new JetErrorInfo { PageNumber = pageNumber });
            }

            byte[] leaf = await pager.ReadPageCopyAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The preserved catalog index chain contains a non-leaf page.", new JetErrorInfo { PageNumber = pageNumber });
            }

            entries.AddRange(DecodeRequiredCatalogLeaf(pageNumber, leaf, format));
            pageNumber = IndexPageCodec.ReadNextPage(layout, leaf);
        }

        return entries;
    }

    /// <summary>
    /// Encodes a single composite index key by per-column-encoding then
    /// concatenating. Honours Jet4's legacy fixed-point byte-twiddling for
    /// <c>Numeric</c> columns (<see cref="JetFormat.LegacyNumericIndexKeys"/>). Throws
    /// whatever <see cref="IndexKeyEncoder"/> throws on encoder rejection
    /// (<see cref="JetNotSupportedException"/> / <see cref="ArgumentException"/>
    /// / <see cref="OverflowException"/>); callers that want soft-fail
    /// behaviour should use <see cref="TryEncodeCompositeKey"/>.
    /// </summary>
    /// <param name="keyColInfos">The key col infos.</param>
    /// <param name="cells">The cells.</param>
    private byte[] EncodeCompositeKey(List<KeyColumnInfo> keyColInfos, object?[] cells)
    {
        var builder = new IndexKeyEncoder.CompositeKeyBuilder(format, keyColInfos.Count);
        for (int k = 0; k < keyColInfos.Count; k++)
        {
            (ColumnInfo? col, int _, bool ascending) = keyColInfos[k];
            builder.Add(k, col, cells[k], ascending);
        }

        return builder.Build();
    }

    /// <summary>
    /// Candidate encoder for the composite key: gathers the
    /// per-column cells for <paramref name="row"/> against
    /// <paramref name="keyColInfos"/>'s snapshot indices and returns
    /// <see langword="null"/> when the row is too short or a value exceeds
    /// the declared key type or capacity.
    /// Used by the incremental + catalog-splice paths to bail to bulk on any
    /// encoder rejection.
    /// </summary>
    /// <param name="keyColInfos">The key col infos.</param>
    /// <param name="row">The row values or row bytes.</param>
    private byte[]? TryEncodeCompositeKey(List<KeyColumnInfo> keyColInfos, object[] row)
    {
        var builder = new IndexKeyEncoder.CompositeKeyBuilder(format, keyColInfos.Count);
        for (int k = 0; k < keyColInfos.Count; k++)
        {
            int snapIdx = keyColInfos[k].SnapIdx;
            if (snapIdx < 0 || snapIdx >= row.Length)
            {
                return null;
            }

            KeyColumnInfo key = keyColInfos[k];
            if (!builder.TryAdd(k, key.Col, row[snapIdx], key.Ascending))
            {
                return null;
            }
        }

        return builder.Build();
    }

    /// <summary>
    /// Encodes the (composite-key, page, row) tuples for the rows in
    /// <paramref name="rows"/> against the supplied key column descriptors.
    /// Returns null when an encoder refuses a row so the caller can rebuild.
    /// All-Null keys are omitted when the index ignores them.
    /// </summary>
    /// <param name="rows">The row collection.</param>
    /// <param name="keyColInfos">The key col infos.</param>
    /// <param name="ignoreNulls">Whether all-Null keys are omitted.</param>
    private List<IndexEntry>? EncodeHintEntries(
        List<(RowLocation Loc, object[] Row)>? rows,
        List<KeyColumnInfo> keyColInfos,
        bool ignoreNulls)
    {
        var results = new List<IndexEntry>(rows?.Count ?? 0);
        if (rows == null || rows.Count == 0)
        {
            return results;
        }

        foreach ((RowLocation loc, object[] row) in rows)
        {
            if (ignoreNulls && IndexHelpers.CountNullKeyColumns(keyColInfos, row) == keyColInfos.Count)
            {
                continue;
            }

            byte[]? composite = this.TryEncodeCompositeKey(keyColInfos, row);
            if (composite is null)
            {
                return null;
            }

            results.Add(new IndexEntry(composite, loc.PageNumber, (byte)loc.RowIndex));
        }

        return results;
    }

    /// <summary>
    /// Splices a single new catalog row's index entry into every real-idx
    /// slot on a system table's index B-tree without re-encoding any
    /// pre-existing entries.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="newRowLoc">The new row loc.</param>
    /// <param name="newRowValues">The new row values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <remarks>
    /// <para>
    /// Used by <c>InsertCatalogEntryAsync</c> for MSysObjects to keep
    /// Microsoft Access's catalog indexes consistent with new table,
    /// relationship, and linked-table rows, while preserving the byte-for-byte
    /// content of existing catalog row payloads the writer cannot losslessly
    /// re-encode. See
    /// <see href="docs/design/catalog-index-maintenance-notes.md" />.
    /// </para>
    /// <para>
    /// Current catalog-splice scope:
    /// <list type="bullet">
    ///   <item>Descends by the encoded catalog key and, when an Access-authored
    ///   tree has stale right-edge summaries, can follow the rightward sibling
    ///   chain to the insertion leaf.</item>
    ///   <item>Rewrites the target leaf in place when the spliced entry set
    ///   still fits on one page, preserving the existing prefix-length cap.</item>
    ///   <item>On leaf overflow, greedily splits the leaf, appends right-hand
    ///   pages, patches sibling pointers, and rewrites ancestor summaries when
    ///   the descent captured a clean path.</item>
    ///   <item>When a clean ancestor rewrite is not available or would overflow,
    ///   rebuilds only the affected catalog index from existing index entries
    ///   plus the inserted row pointer, then patches the real-index root.
    ///   Existing catalog rows are not re-encoded.</item>
    ///   <item>Returns <see langword="false"/> only when the existing index
    ///   tree or the new catalog key cannot be trusted or staged safely, such
    ///   as malformed pages, impossible key encoding, a single entry too large
    ///   to pack, or an unexpected append-position mismatch.</item>
    /// </list>
    /// On <see langword="false"/>, catalog callers must fail the
    /// surrounding mutation so the transaction rolls back instead of leaving
    /// unmaintained catalog indexes that DAO Compact &amp; Repair could later
    /// report as JET <c>-1601</c>.
    /// </para>
    /// </remarks>
    /// <exception cref="JetOperationException">Thrown when an unexpected TdefPreambleStatus is encountered.</exception>
    public async ValueTask<bool> TrySpliceCatalogIndexEntryAsync(
        long tdefPage,
        TableDef tableDef,
        RowLocation newRowLoc,
        object[] newRowValues,
        CancellationToken cancellationToken)
    {
        IndexPageLayout layout = format.IndexPage;

        this.LastIncrementalBail = null;

        (TdefPreambleStatus preStatus, TdefPreamble preamble) = await this.ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        switch (preStatus)
        {
            case TdefPreambleStatus.Ok:
                break;
            case TdefPreambleStatus.Empty:
                this.LastIncrementalBail = $"S0 numIdx={preamble.NumIdx} numRealIdx={preamble.NumRealIdx}";
                return true;
            case TdefPreambleStatus.TooMany:
                this.LastIncrementalBail = "S1 too many idx";
                return false;
            case TdefPreambleStatus.ColumnNameWalkFailed:
                return false;
            default:
                throw new JetOperationException(JetErrorCode.InvalidOperationState, $"Unexpected TdefPreambleStatus {preStatus}", new JetErrorInfo { PageNumber = tdefPage, Reason = "Unknown index preamble status" });
        }

        byte[] tdefBuf = preamble.Buffer;
        int realIdxDescStart = preamble.RealIdxDescStart;
        int numIdx = preamble.NumIdx;
        int numRealIdx = preamble.NumRealIdx;

        // Decode the index catalog once, with key columns pre-resolved
        // against the snapshot. PK promotion is harmless here (this path
        // doesn't gate on IsUnique); names are unused so we skip them.
        IndexCatalogReader.ResolvedIndexCatalog catalog = IndexCatalogReader.ReadResolved(
            tdefBuf,
            format.Index,
            format.Index.GetIndexSection(realIdxDescStart, numRealIdx, numIdx),
            tableDef.Columns);

        await this.ValidateCatalogIndexPagesAsync(tdefPage, preamble, catalog, cancellationToken).ConfigureAwait(false);

        foreach ((int ri, RealIdxEntry rie) in catalog.RealIdxByNum)
        {
            long firstDp = (uint)Ri32(tdefBuf, rie.FirstDpOffset);
            if (firstDp <= 0)
            {
                this.LastIncrementalBail = $"S2 ri={ri} firstDp=0";
                continue;
            }

            // Resolve key columns to TDEF ColumnInfos.
            if (!catalog.TryGetKeyColumnInfos(ri, out List<KeyColumnInfo>? keyColInfos))
            {
                this.LastIncrementalBail = $"S3 ri={ri} resolveFailed";
                return false;
            }

            if (rie.IgnoreNulls && IndexHelpers.CountNullKeyColumns(keyColInfos, newRowValues) == keyColInfos.Count)
            {
                continue;
            }

            // Encode the composite key for the new row.
            byte[]? composite = this.TryEncodeCompositeKey(keyColInfos, newRowValues);
            if (composite is null)
            {
                this.LastIncrementalBail = $"S4 ri={ri} encErr";
                return false;
            }

            if (!await this.btreeEditor.TryInsertCatalogEntryAsync(layout, tdefPage, firstDp, rie.FirstDpOffset, composite, newRowLoc, cancellationToken).ConfigureAwait(false))
            {
                this.LastIncrementalBail = $"S5 ri={ri} tree insertion refused";
                return false;
            }
        }

        return true;
    }

    /// <summary>Links staged trees and refreshes usage maps from the latest TDEF image.</summary>
    /// <param name="tdefPage">The tdefPage for this phase.</param>
    /// <param name="layout">The layout for this phase.</param>
    /// <param name="slots">The slots for this phase.</param>
    /// <param name="numRealIdx">The numRealIdx for this phase.</param>
    /// <param name="metadata">The metadata for this phase.</param>
    /// <param name="runs">The runs for this phase.</param>
    /// <param name="cancellationToken">The cancellationToken for this phase.</param>
    private async ValueTask<bool> CommitIncrementalMetadataAsync(
        long tdefPage,
        IndexPageLayout layout,
        List<(int RealIdxNum, RealIdxEntry Entry)> slots,
        int numRealIdx,
        IncrementalMetadataState metadata,
        ReservedPageRuns runs,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long[][]? indexPageGroups = await this.TryCollectIncrementalIndexPageGroupsAsync(tdefPage, metadata.Buffer, layout, slots, numRealIdx, cancellationToken).ConfigureAwait(false);
        if (indexPageGroups is null)
        {
            this.LastIncrementalBail = "C14 usage-map refresh failed";
            return false;
        }

        // The usage-map rows and the TDEF write below link every tree
        // rebuilt since the last TDEF write.
        runs.MarkLinked();
        await this.WriteIncrementalIndexUsageMapsAsync(tdefPage, metadata.Buffer, slots, indexPageGroups, cancellationToken).ConfigureAwait(false);

        metadata.Dirty = true;

        if (metadata.Dirty)
        {
            await tdefWriter.WriteChainInPlaceAsync(metadata.Chain, cancellationToken).ConfigureAwait(false);
        }

        if (metadata.ReplacedPageGroups is not null)
        {
            await this.DeallocateReplacedIndexPagesAsync(tdefPage, metadata.ReplacedPageGroups, indexPageGroups, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>Owns pending TDEF changes across tree edits that can replace the on-disk root.</summary>
    /// <param name="chain">The chain for this phase.</param>
    private sealed class IncrementalMetadataState(LogicalTDefChain chain)
    {
        /// <summary>Gets or sets the current logical TDEF image.</summary>
        public LogicalTDefChain Chain { get; set; } = chain;

        /// <summary>Gets the current image bytes.</summary>
        public byte[] Buffer => this.Chain.Bytes;

        /// <summary>Gets or sets a value indicating whether pending roots need linking.</summary>
        public bool Dirty { get; set; }

        /// <summary>Gets or sets the traversable trees captured before editing.</summary>
        public long[][]? ReplacedPageGroups { get; set; }
    }
}
