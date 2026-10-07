namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Index B-tree maintenance for <see cref="AccessWriter"/>: bulk rebuild
/// (<see cref="MaintainIndexesAsync"/>), incremental fast-path
/// (<see cref="TryMaintainIndexesIncrementalAsync"/>), the catalog-index
/// splice (<see cref="TrySpliceCatalogIndexEntryAsync"/>), and the system-table
/// row writes that must keep their indexes current
/// (<see cref="InsertSystemRowAndMaintainAsync"/>,
/// <see cref="RewriteSystemTableRowsAsync"/>).
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="ownedPages">The database's owned-page discovery and row walks.</param>
/// <param name="pager">The writer's page file, through which index and data pages are written.</param>
/// <param name="tdefWriter">Writes index roots and usage-map pointers back into TDEF chains.</param>
/// <param name="pageAllocator">Reserves and frees index pages.</param>
/// <param name="tableRows">Writes system-table rows ahead of their index splice.</param>
/// <param name="dataPages">Owns usage-map rows and empty data pages.</param>
/// <param name="snapshots">Reads decoded table rows for full index rebuilds.</param>
internal sealed class IndexMaintainer(
    JetFormat format,
    TableDefReader tableDefs,
    OwnedDataPages ownedPages,
    Pager pager,
    TDefWriter tdefWriter,
    PageAllocator pageAllocator,
    TableRowStore tableRows,
    DataPageInserter dataPages,
    TableSnapshotReader snapshots)
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

    /// <summary>Gets the most recent system-table index-maintenance path.</summary>
    internal SystemTableIndexMaintenancePath LastSystemTableIndexMaintenancePath { get; private set; }

    /// <summary>
    /// Inserts one row into a system table (MSysObjects, MSysRelationships,
    /// MSysComplexColumns, …) and refreshes that table's indexes so external
    /// readers (Microsoft Access / DAO Compact &amp; Repair) can locate the
    /// new row through the catalog indexes. Bare <see cref="TableRowStore.InsertRowDataAsync"/>
    /// only writes the data row; index leaves are not maintained, so DAO
    /// walking via <c>ParentIdName</c> / <c>Id</c> never sees the row and the
    /// catalog appears empty from outside.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="values">The values.</param>
    /// <param name="updateTDefRowCount">Whether to update the table row count in the TDEF.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <remarks>
    /// User-row inserts are batched by <see cref="AccessWriter.InsertRowsAsync(string, IEnumerable{object[]}, CancellationToken)"/>
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
        this.LastSystemTableIndexMaintenancePath = SystemTableIndexMaintenancePath.None;
        RowLocation loc = await tableRows.InsertRowDataLocAsync(tdefPage, tableDef, values, updateTDefRowCount, cancellationToken).ConfigureAwait(false);

        var hint = new List<(RowLocation Loc, object[] Row)>(1) { (loc, values) };
        this.LastSystemTableIndexMaintenancePath = await this.MaintainSystemTableIndexesIncrementallyAsync(
            tdefPage,
            tableDef,
            tableName,
            hint,
            deletedRows: null,
            cancellationToken).ConfigureAwait(false);
    }

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
        foreach (object[] row in rows)
        {
            UnreadableLongValue.ThrowIfAny(row, tableName);
        }

        var dataPageNumbers = new List<long>();
        await ownedPages.ForEachOwnedDataPageAsync(
            tdefPage,
            (pageNumber, _, _) =>
            {
                dataPageNumbers.Add(pageNumber);
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        if (dataPageNumbers.Count == 0 && rows.Count > 0)
        {
            throw new InvalidDataException($"System table '{tableName}' has no data pages to rewrite.");
        }

        foreach (long pageNumber in dataPageNumbers)
        {
            await pager.WritePageAsync(pageNumber, dataPages.CreateEmptyDataPage(tdefPage), cancellationToken).ConfigureAwait(false);
        }

        if (dataPageNumbers.Count > 0)
        {
            dataPages.SetCachedInsertPageNumber(tdefPage, dataPageNumbers[0]);
        }

        foreach (object[] row in rows)
        {
            object[] rowValues = (object[])row.Clone();
            await tableRows.InsertRowDataLocAsync(tdefPage, tableDef, rowValues, updateTDefRowCount: false, cancellationToken).ConfigureAwait(false);
        }

        await tableRows.SetTDefRowCountAsync(tdefPage, checked((uint)rows.Count), cancellationToken).ConfigureAwait(false);
        await this.MaintainIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
    }

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

    private InvalidOperationException CreateSystemTableIndexMaintenanceException(string tableName, Exception? inner = null)
    {
        string message = $"Could not maintain {tableName} system-table indexes incrementally; full rebuild fallback is disabled.";
        if (!string.IsNullOrWhiteSpace(this.LastIncrementalBail))
        {
            message += $" Bail: {this.LastIncrementalBail}.";
        }

        return inner is null ? new InvalidOperationException(message) : new InvalidOperationException(message, inner);
    }

    /// <summary>
    /// Returns <c>true</c> when every real-idx slot of the TDEF chain rooted at
    /// <paramref name="tdefPage"/> references a valid in-range data page
    /// through its <c>first_dp</c> pointer. Used by
    /// <see cref="InsertSystemRowAndMaintainAsync"/> to avoid index
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
    /// Reads <paramref name="pageNumber"/> through the page cache and returns
    /// a freshly cloned, caller-owned copy of the bytes. The cache buffer is
    /// returned to the pool before this method returns, so callers must not
    /// retain any reference to the original buffer.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<byte[]> ReadAndClonePageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] pageBytes = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])pageBytes.Clone();
        }
        finally
        {
            PageBuffers.Return(pageBytes);
        }
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
    private async ValueTask<(TdefPreamble Preamble, IndexCatalogReader.ResolvedIndexCatalog? Catalog)> ResolveMaintainableIndexesAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        CancellationToken cancellationToken)
    {
        (TdefPreambleStatus status, TdefPreamble preamble) = await this.ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
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
    /// long or complex storage columns, and on Jet3 for an Access-authored
    /// index, whose <c>used_pages</c> row still lists them; those stay
    /// unreferenced until Access reclaims them on Compact &amp; Repair.
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
    /// <exception cref="InvalidOperationException">Thrown when a unique index violation is detected after a row mutation.</exception>
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
    /// <exception cref="InvalidOperationException">Thrown when a unique index violation is detected after a row mutation.</exception>
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
        // them. Jet4 / ACE find each old tree through its index usage-map row;
        // Jet3 keeps none the writer can read back, so each old tree is walked
        // from its first_dp, before anything is written.
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
                            throw JetErrors.Constraint(JetErrorCode.UniqueViolation, $"Unique index violation on table '{tableName}': duplicate key detected after row mutation. The duplicate row has been written but the index B-tree was not rebuilt; remove one of the offending rows and retry the operation.", errorInfo: new JetErrorInfo { TableName = tableName, IndexName = catalog.Catalog.GetNameOrFallback(rieKey) });
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
                        firstPage => IndexBTreeBuilder.Build(leafLayout, format.PageSize, tdefPage, entries, firstPage),
                        runs,
                        cancellationToken).ConfigureAwait(false);
                    build = placed ?? throw new ArgumentOutOfRangeException(
                        nameof(tdefPage),
                        $"The rebuilt B-tree of real index {rieKey} on table '{tableName}' no longer fits at the pages the allocator reserved for it.");

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

        if (!jet3 && HasAnyIndexPageGroup(rebuiltIndexPageGroups))
        {
            long usageMapPage = this.ReadTableUsageMapPage(tdefBuffer);
            indexPointers = await dataPages.UpdateTableIndexUsageMapRowsAsync(usageMapPage, indexPointers, rebuiltIndexPageGroups, cancellationToken).ConfigureAwait(false);
            for (int realIdxNum = 0; realIdxNum < rebuiltIndexPageGroups.Length; realIdxNum++)
            {
                if (rebuiltIndexPageGroups[realIdxNum].Length == 0)
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
    /// same exception.
    /// </summary>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="tableDef">The table's definition.</param>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the check has passed.</returns>
    /// <exception cref="JetLimitationException">The index section cannot be parsed or runs past the end of the table definition, or an index names a column the table does not have; the message names the table, and the index when its name can be read.</exception>
    internal async ValueTask ThrowIfIndexesUnmaintainableAsync(long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
        => _ = await this.ResolveMaintainableIndexesAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);

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
                    strict: false,
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
    /// rebuild is about to replace, by walking it from its <c>first_dp</c>:
    /// Jet3 keeps no index usage map the writer can read back. Only an index
    /// whose <c>used_pages</c> is 0, one the writer created, is collected. An
    /// Access-authored index keeps a usage-map row that still names its old
    /// pages, so its tree is left for Compact &amp; Repair rather than freed
    /// while that row lists it. A tree that cannot be walked is not collected.
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
        long[][] groups = CreateEmptyPageGroups(numRealIdx);
        foreach ((int realIdxNum, RealIdxEntry entry) in realIdxByNum)
        {
            long rootPage = (uint)Ri32(tdefBuffer, entry.FirstDpOffset);
            if (realIdxNum < 0 || realIdxNum >= numRealIdx || rootPage == 0 || Ri32(tdefBuffer, entry.FirstDpOffset - 4) != 0)
            {
                continue;
            }

            groups[realIdxNum] = await this.TryCollectIndexTreePagesAsync(layout, tdefPage, rootPage, cancellationToken).ConfigureAwait(false) ?? [];
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

        var deallocatedPages = new HashSet<long>();
        int groupCount = Math.Min(oldIndexPageGroups.Length, rebuiltIndexPageGroups.Length);
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
    /// Returns an empty array when the format keeps no index usage maps
    /// (Jet3) or no slot was maintained, and <see langword="null"/> when a
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
        if (format.IsJet3 || slots.Count == 0)
        {
            return [];
        }

        long usageMapPage = this.ReadTableUsageMapPage(tdefBuffer);
        if (usageMapPage <= 0 || usageMapPage >= pager.PageCount)
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
    /// rows where a tree spans more than one INLINE window. Does nothing for
    /// an empty array.
    /// </summary>
    /// <param name="tdefBuffer">The logical TDEF bytes, which name the table's usage-map page.</param>
    /// <param name="slots">The maintained real-index descriptors.</param>
    /// <param name="indexPageGroups">The pages of each real index's tree, by real-index number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask WriteIncrementalIndexUsageMapsAsync(byte[] tdefBuffer, List<(int RealIdxNum, RealIdxEntry Entry)> slots, long[][] indexPageGroups, CancellationToken cancellationToken)
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

        indexPointers = await dataPages.UpdateTableIndexUsageMapRowsAsync(
            this.ReadTableUsageMapPage(tdefBuffer), indexPointers, indexPageGroups, cancellationToken).ConfigureAwait(false);
        foreach ((int realIdxNum, RealIdxEntry entry) in slots)
        {
            if (indexPageGroups[realIdxNum].Length > 0)
            {
                UsageMapPointer pointer = indexPointers[realIdxNum];
                WriteIndexUsageMapPointer(tdefBuffer, entry.FirstDpOffset - 4, pointer.RowIndex, pointer.PageNumber);
            }
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

            byte[] page = await this.ReadAndClonePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
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

            List<DecodedIntermediateEntry> entries = IndexPageCodec.DecodeIntermediateEntries(layout, page, format.PageSize);
            if (entries.Count == 0)
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
    /// is left orphaned for Access to reclaim on Compact &amp; Repair, exactly
    /// like the bulk-rebuild path's own orphans.
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> TryMaintainIndexesIncrementalCoreAsync(
        long tdefPage,
        TableDef tableDef,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        ReservedPageRuns runs,
        CancellationToken cancellationToken)
    {
        // Jet3 (.mdb Access 97) participates in the
        // incremental fast paths via the per-format IndexPageLayout descriptor
        // (page size 2048, bitmask at 0x16, first entry at 0xF8) and the §3.1
        // 39-byte real-idx physical descriptor (first_dp at phys+34 instead
        // of phys+38). The change-set encode + splice + rebuild logic is
        // unchanged; only the layout-dependent byte offsets and page builder
        // calls fork on `jet3`. Same disposal model as Jet4 — old leaf /
        // intermediate pages are orphaned and reclaimed by Access on
        // Compact & Repair.
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

        LogicalTDefChain tdefChain = preamble.Chain;
        byte[] tdefBuffer = tdefChain.Bytes;
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
            if (!idxLayout.TryReadLogicalEntry(tdefBuffer, logIdxStart, li, out LogicalIdxEntry entry))
            {
                this.LastIncrementalBail = $"C1b li={li} logIdxStart={logIdxStart} bufLen={tdefBuffer.Length}";
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
            if (!idxLayout.TryReadRealIdxSlotWithKeyColumns(tdefBuffer, realIdxDescStart, ri, out RealIdxSlot slot, out List<KeyColumn>? keyCols))
            {
                this.LastIncrementalBail = $"C1 ri={ri} realIdxDescStart={realIdxDescStart} bufLen={tdefBuffer.Length}";
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

        Dictionary<int, int> snapshotIndexByColNum = IndexCatalogReader.BuildColumnNumberToSnapshotIndex(tableDef.Columns);

        bool tdefDirty = false;
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
            long firstDp = (uint)Ri32(tdefBuffer, rie.FirstDpOffset);
            if (firstDp <= 0)
            {
                this.LastIncrementalBail = $"C3 firstDp={firstDp}";
                return false;
            }

            byte[] rootPage = await this.ReadAndClonePageAsync(firstDp, cancellationToken).ConfigureAwait(false);

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
                bool surgicalHandled = await this.btreeEditor.TrySurgicalMultiLevelMaintainAsync(
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
                if (tdefDirty)
                {
                    // This write links every tree rebuilt so far.
                    runs.MarkLinked();
                    await tdefWriter.WriteChainInPlaceAsync(tdefChain, cancellationToken).ConfigureAwait(false);
                    tdefDirty = false;
                }

                bool crossLeafHandled = await this.btreeEditor.TrySurgicalCrossLeafMaintainAsync(
                    layout,
                    tdefPage,
                    firstDp,
                    rie.FirstDpOffset,
                    addEntries,
                    removeEntries,
                    cancellationToken).ConfigureAwait(false);
                if (crossLeafHandled)
                {
                    // The editor may have patched this index's first_dp on
                    // disk; pick up the chain it wrote.
                    tdefChain = await tableDefs.ReadTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
                    tdefBuffer = tdefChain.Bytes;
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
                    byte[] leaf = await this.ReadAndClonePageAsync(walkPage, cancellationToken).ConfigureAwait(false);

                    if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
                    {
                        this.LastIncrementalBail = $"C9 walkPage={walkPage} leaf[0]={leaf[0]:X2}";
                        return false;
                    }

                    allExisting.AddRange(IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize));
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

                Wi32(tdefBuffer, rie.FirstDpOffset, checked((int)placedTree.RootPageNumber));
                tdefDirty = true;
                continue;
            }

            List<IndexEntry> existing = IndexPageCodec.DecodeLeafEntries(layout, rootPage, format.PageSize);
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
                // leaf is left orphaned, like a replaced multi-level tree.
                IndexBTreeBuildResult? grown = await this.btreeEditor.TryPlaceTreeAsync(layout, tdefPage, spliced, runs, cancellationToken).ConfigureAwait(false);
                if (grown is not { } grownTree)
                {
                    this.LastIncrementalBail = $"C13 spliced={spliced.Count}";
                    return false;
                }

                Wi32(tdefBuffer, rie.FirstDpOffset, checked((int)grownTree.RootPageNumber));
                tdefDirty = true;
                continue;
            }

            await pager.WritePageAsync(firstDp, newLeaf, cancellationToken).ConfigureAwait(false);
        }

        long[][]? indexPageGroups = await this.TryCollectIncrementalIndexPageGroupsAsync(tdefPage, tdefBuffer, layout, slots, numRealIdx, cancellationToken).ConfigureAwait(false);
        if (indexPageGroups is null)
        {
            this.LastIncrementalBail = "C14 usage-map refresh failed";
            return false;
        }

        // The usage-map rows and the TDEF write below link every tree
        // rebuilt since the last TDEF write.
        runs.MarkLinked();
        await this.WriteIncrementalIndexUsageMapsAsync(tdefBuffer, slots, indexPageGroups, cancellationToken).ConfigureAwait(false);

        if (!format.IsJet3)
        {
            tdefDirty = true;
        }

        if (tdefDirty)
        {
            await tdefWriter.WriteChainInPlaceAsync(tdefChain, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Encodes a single composite index key by per-column-encoding then
    /// concatenating. Honours Jet4's legacy fixed-point byte-twiddling for
    /// <c>Numeric</c> columns (<see cref="JetFormat.LegacyNumericIndexKeys"/>). Throws
    /// whatever <see cref="IndexKeyEncoder"/> throws on encoder rejection
    /// (<see cref="NotSupportedException"/> / <see cref="ArgumentException"/>
    /// / <see cref="OverflowException"/>); callers that want soft-fail
    /// behaviour should use <see cref="TryEncodeCompositeKey"/>.
    /// </summary>
    /// <param name="keyColInfos">The key col infos.</param>
    /// <param name="cells">The cells.</param>
    private byte[] EncodeCompositeKey(List<KeyColumnInfo> keyColInfos, object?[] cells)
    {
        bool legacyNumeric = format.LegacyNumericIndexKeys;

        byte[][] perColumn = new byte[keyColInfos.Count][];
        int totalLen = 0;
        for (int k = 0; k < keyColInfos.Count; k++)
        {
            (ColumnInfo? col, int _, bool ascending) = keyColInfos[k];
            object? value = cells[k];
            perColumn[k] = col.Type == NumericType
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, col.NumericScale, legacyNumeric)
                : IndexKeyEncoder.EncodeColumnEntry(format, col, value, ascending);
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
    /// Soft-fail wrapper over <see cref="EncodeCompositeKey"/>: gathers the
    /// per-column cells for <paramref name="row"/> against
    /// <paramref name="keyColInfos"/>'s snapshot indices and returns
    /// <see langword="null"/> when the row is too short or any encoder
    /// rejects (<see cref="NotSupportedException"/>,
    /// <see cref="ArgumentException"/>, or <see cref="OverflowException"/>).
    /// Used by the incremental + catalog-splice paths to bail to bulk on any
    /// encoder rejection.
    /// </summary>
    /// <param name="keyColInfos">The key col infos.</param>
    /// <param name="row">The row values or row bytes.</param>
    private byte[]? TryEncodeCompositeKey(List<KeyColumnInfo> keyColInfos, object[] row)
    {
        object?[] cells = new object?[keyColInfos.Count];
        for (int k = 0; k < keyColInfos.Count; k++)
        {
            int snapIdx = keyColInfos[k].SnapIdx;
            if (snapIdx >= row.Length)
            {
                return null;
            }

            object cell = row[snapIdx];
            cells[k] = cell is DBNull ? null : cell;
        }

        try
        {
            return this.EncodeCompositeKey(keyColInfos, cells);
        }
        catch (NotSupportedException)
        {
            return null;
        }
        catch (ArgumentException)
        {
            return null;
        }
        catch (OverflowException)
        {
            return null;
        }
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
    /// <exception cref="InvalidOperationException">Thrown when an unexpected TdefPreambleStatus is encountered.</exception>
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
                throw new InvalidOperationException($"Unexpected TdefPreambleStatus {preStatus}");
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

            // Descend by binary-searching child summaries. First try
            // without tail overshoot so we capture a clean path for
            // ancestor updates (needed when the leaf splits). Fall back
            // to allowTailOvershoot when the key overshoots every summary
            // on an intermediate — in that case the chain walk below still
            // finds the correct leaf and we accept that ancestor updates
            // won't be possible (but a split can still chain-append).
            var descentPath = new List<DescentStep>();
            bool hasCleanPath = true;
            long targetLeafPage = await this.btreeEditor.DescendCapturingAsync(
                layout, firstDp, composite, descentPath, cancellationToken, allowTailOvershoot: false).ConfigureAwait(false);
            if (targetLeafPage <= 0)
            {
                // Overshoot — retry with tail following. Path will be
                // incomplete but the chain walk handles placement.
                descentPath.Clear();
                hasCleanPath = false;
                targetLeafPage = await this.btreeEditor.DescendCapturingAsync(
                    layout, firstDp, composite, descentPath, cancellationToken, allowTailOvershoot: true).ConfigureAwait(false);
                if (targetLeafPage <= 0)
                {
                    this.LastIncrementalBail = $"S5 ri={ri} descent failed firstDp={firstDp}";
                    return false;
                }
            }

            byte[] leaf = await this.ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                this.LastIncrementalBail = $"S8 ri={ri} targetLeafPage={targetLeafPage} type=0x{leaf[0]:X2}";
                return false;
            }

            // If the descent landed before the true tail of a sibling
            // chain (Access can store mostly-monotonic data with stale
            // intermediate summaries plus a rightward chain), walk
            // next_page while every existing entry on the current leaf
            // is < composite. That way we still find the correct
            // insertion leaf.
            int chainBudget = 1_000_000;
            while (true)
            {
                long nextLeaf = IndexPageCodec.ReadNextPage(layout, leaf);
                if (nextLeaf <= 0)
                {
                    break;
                }

                List<IndexEntry> probe = IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize);
                if (probe.Count == 0 || IndexHelpers.CompareKeyBytes(composite, probe[^1].Key) <= 0)
                {
                    // composite belongs in this leaf (or earlier).
                    break;
                }

                if (--chainBudget <= 0)
                {
                    this.LastIncrementalBail = $"S8b ri={ri} chainBudget exhausted";
                    return false;
                }

                targetLeafPage = nextLeaf;
                leaf = await this.ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

                if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
                {
                    this.LastIncrementalBail = $"S8c ri={ri} walkedTo={targetLeafPage} type=0x{leaf[0]:X2}";
                    return false;
                }
            }

            long leafPrev = IndexPageCodec.ReadPrevPage(layout, leaf);
            long leafNext = IndexPageCodec.ReadNextPage(layout, leaf);
            long leafTail = IndexPageCodec.ReadTailPage(layout, leaf);
            int originalPrefLen = IndexPageCodec.ReadPrefixLength(layout, leaf);

            List<IndexEntry> existing = IndexPageCodec.DecodeLeafEntries(layout, leaf, format.PageSize);

            var addEntries = new List<IndexEntry>(1)
            {
                new(composite, newRowLoc.PageNumber, (byte)newRowLoc.RowIndex),
            };

            List<IndexEntry>? spliced = IndexEntrySplicer.Splice(
                existing,
                addEntries,
                []);
            if (spliced is null)
            {
                this.LastIncrementalBail = $"S11 ri={ri} splice null";
                return false;
            }

            byte[] rewritten;
            try
            {
                rewritten = IndexPageCodec.BuildLeafPage(
                    layout,
                    format.PageSize,
                    tdefPage,
                    spliced,
                    prevPage: leafPrev,
                    nextPage: leafNext,
                    tailPage: leafTail,
                    enablePrefixCompression: true,
                    maxPrefixLength: originalPrefLen);
            }
            catch (ArgumentOutOfRangeException)
            {
                // Leaf overflow → N-way split.
                SplitPages? splitPages = this.btreeEditor.TryBalancedTwoWayLeafSplit(layout, spliced, originalPrefLen)
                    ?? IndexHelpers.TryGreedySplitLeafInN(layout, format.PageSize, spliced);
                if (splitPages is null)
                {
                    this.LastIncrementalBail = $"S12 ri={ri} split failed";
                    return false;
                }

                // Plan the split at the provisional end of file and reserve
                // its new pages only once an in-place split is certain: the
                // rebuild fallbacks below reserve their own pages, and a run
                // reserved first would be left behind, unwritten and marked
                // used.
                int splitCount = splitPages.Count;
                bool canSplitInPlace = hasCleanPath && descentPath.Count > 0;
                CatalogLeafSplitPlan? plan = this.TryPlanCatalogLeafSplit(
                    layout, tdefPage, splitPages, targetLeafPage, pager.PageCount, leafPrev, leafNext, originalPrefLen, canSplitInPlace ? descentPath : null);
                if (plan is null)
                {
                    this.LastIncrementalBail = $"S12b ri={ri} split build failed";
                    return false;
                }

                var runs = new ReservedPageRuns(pageAllocator);
                try
                {
                    if (plan.AncestorWrites is not null)
                    {
                        long firstFreshPage = await runs.ReserveAsync(splitCount - 1, cancellationToken).ConfigureAwait(false);
                        if (firstFreshPage != plan.PageNumbers[1])
                        {
                            plan = this.TryPlanCatalogLeafSplit(
                                layout, tdefPage, splitPages, targetLeafPage, firstFreshPage, leafPrev, leafNext, originalPrefLen, descentPath);
                        }
                    }

                    if (plan?.AncestorWrites is not { } ancestorWrites)
                    {
                        // No clean ancestor path, or the ancestor summaries
                        // overflow: rebuild just this index from its entries.
                        await runs.ReleaseAsync().ConfigureAwait(false);
                        bool rebuilt = await this.btreeEditor.TryRebuildCatalogIndexTreeAsync(
                            layout,
                            tdefPage,
                            firstDp,
                            rie.FirstDpOffset,
                            addEntries,
                            cancellationToken).ConfigureAwait(false);
                        if (!rebuilt)
                        {
                            this.LastIncrementalBail = canSplitInPlace
                                ? $"S12c ri={ri} ancestor overflow"
                                : $"S12c ri={ri} no clean ancestor path";
                            return false;
                        }

                        continue;
                    }

                    // Commit: write the new pages, then patch next-leaf's prev
                    // pointer, the original leaf and the ancestors, which link
                    // them.
                    long[] pageNumbers = plan.PageNumbers;
                    for (int p = 1; p < splitCount; p++)
                    {
                        await pager.WritePageAsync(pageNumbers[p], plan.Pages[p], cancellationToken).ConfigureAwait(false);
                    }

                    runs.MarkLinked();
                    if (leafNext > 0)
                    {
                        byte[] nextLeafBuf = await this.ReadAndClonePageAsync(leafNext, cancellationToken).ConfigureAwait(false);
                        IndexPageCodec.WritePrevPage(layout, nextLeafBuf, pageNumbers[splitCount - 1]);
                        await pager.WritePageAsync(leafNext, nextLeafBuf, cancellationToken).ConfigureAwait(false);
                    }

                    await pager.WritePageAsync(targetLeafPage, plan.Pages[0], cancellationToken).ConfigureAwait(false);

                    foreach ((long pn, byte[] bytes) in ancestorWrites)
                    {
                        await pager.WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    if (!runs.IsEmpty)
                    {
                        await runs.ReleaseAsync().ConfigureAwait(false);
                    }
                }

                continue;
            }

            await pager.WritePageAsync(targetLeafPage, rewritten, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Builds the pages of a catalog leaf split whose new right-hand pages
    /// start at <paramref name="firstNewPage"/>, and, when
    /// <paramref name="descentPath"/> is supplied, the ancestor rewrites that
    /// link them. Writes and reserves nothing, so the splice can plan at the
    /// provisional end of file, reserve only when the in-place split is
    /// certain, and plan again if the reservation lands elsewhere.
    /// </summary>
    /// <param name="layout">The index page layout.</param>
    /// <param name="tdefPage">The catalog TDEF page.</param>
    /// <param name="splitPages">The entries of each split page, left to right.</param>
    /// <param name="targetLeafPage">The leaf being split; it stays the left-most page.</param>
    /// <param name="firstNewPage">The page number of the first new right-hand page.</param>
    /// <param name="leafPrev">The split leaf's prev_page.</param>
    /// <param name="leafNext">The split leaf's next_page.</param>
    /// <param name="maxPrefixLength">The split leaf's prefix-length cap.</param>
    /// <param name="descentPath">The clean root-to-leaf path, or <see langword="null"/> when there is none.</param>
    /// <returns>The plan, or <see langword="null"/> when a split page cannot be built.</returns>
    private CatalogLeafSplitPlan? TryPlanCatalogLeafSplit(
        IndexPageLayout layout,
        long tdefPage,
        SplitPages splitPages,
        long targetLeafPage,
        long firstNewPage,
        long leafPrev,
        long leafNext,
        int maxPrefixLength,
        List<DescentStep>? descentPath)
    {
        long[] pageNumbers = IndexBTreeEditor.AllocateSplitPageNumbers(targetLeafPage, splitPages.Count, firstNewPage);
        byte[][]? pages = this.btreeEditor.TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, maxPrefixLength);
        if (pages is null)
        {
            return null;
        }

        List<(long PageNum, byte[] Bytes)>? ancestorWrites = null;
        if (descentPath is not null)
        {
            DecodedIntermediateEntry[] summaries = IndexBTreeEditor.BuildSplitSummaries(splitPages, pageNumbers);
            ancestorWrites = this.btreeEditor.PrepareAncestorSplitWrites(layout, tdefPage, descentPath, summaries);
        }

        return new CatalogLeafSplitPlan(pageNumbers, pages, ancestorWrites);
    }

    /// <summary>A planned in-place catalog leaf split.</summary>
    /// <param name="PageNumbers">The page number of each split page; <c>[0]</c> is the original leaf.</param>
    /// <param name="Pages">The bytes of each split page, parallel to <paramref name="PageNumbers"/>.</param>
    /// <param name="AncestorWrites">The ancestor rewrites that link the new pages, or <see langword="null"/> when the split cannot be linked in place.</param>
    private sealed record CatalogLeafSplitPlan(
        long[] PageNumbers,
        byte[][] Pages,
        List<(long PageNum, byte[] Bytes)>? AncestorWrites);
}
