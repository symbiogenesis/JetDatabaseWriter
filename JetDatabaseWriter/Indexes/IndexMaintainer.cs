namespace JetDatabaseWriter.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;
using KeyColumnInfo = JetDatabaseWriter.Indexes.IndexLayout.KeyColumnInfo;
using RealIdxEntry = JetDatabaseWriter.Indexes.IndexLayout.RealIdxEntry;

#pragma warning disable CA1822 // Mark members as static
#pragma warning disable SA1202
#pragma warning disable SA1204

/// <summary>
/// Index B-tree maintenance for <see cref="AccessWriter"/>: bulk rebuild
/// (<see cref="MaintainIndexesAsync"/>), incremental fast-path
/// (<see cref="TryMaintainIndexesIncrementalAsync"/>), and the
/// catalog-index single-leaf splice (<see cref="TrySpliceCatalogIndexEntryAsync"/>).
/// Owned by an <see cref="AccessWriter"/> via a private field; the writer
/// exposes thin instance forwarders.
/// </summary>
internal sealed class IndexMaintainer(AccessWriter writer)
{
    /// <summary>
    /// Gets the most recent reason
    /// <see cref="TryMaintainIndexesIncrementalAsync"/> returned false.
    /// Diagnostic-only; not part of the public contract.
    /// </summary>
    public string? LastIncrementalBail { get; private set; }

    /// <summary>
    /// Reads <paramref name="pageNumber"/> through the page cache and returns
    /// a freshly cloned, caller-owned copy of the bytes. The cache buffer is
    /// returned to the pool before this method returns, so callers must not
    /// retain any reference to the original buffer.
    /// </summary>
    private async ValueTask<byte[]> ReadAndClonePageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] pageBytes = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])pageBytes.Clone();
        }
        finally
        {
            AccessBase.ReturnPage(pageBytes);
        }
    }

    /// <summary>
    /// Parsed snapshot of the per-table TDEF header bytes needed by the index
    /// maintenance paths: the cloned page buffer, decoded column / index
    /// counts, and the byte offset at which the real-idx descriptor block
    /// begins (i.e. just past the column-name table).
    /// </summary>
    private readonly record struct TdefPreamble(
        byte[] Buffer,
        int NumCols,
        int NumIdx,
        int NumRealIdx,
        int RealIdxDescStart,
        int FailedColumnIndex,
        int FailedColumnNamePos);

    /// <summary>
    /// Reads + clones the TDEF page, decodes <c>numCols</c> / <c>numIdx</c> /
    /// <c>numRealIdx</c>, walks the column-name table, and returns the byte
    /// offset at which the real-idx descriptor block starts. Each caller maps
    /// the returned <see cref="TdefPreambleStatus"/> to its own bail policy
    /// (silent return for the bulk path, <c>LastIncrementalBail</c> string for
    /// the incremental and catalog-splice paths).
    /// </summary>
    private async ValueTask<(TdefPreambleStatus Status, TdefPreamble Preamble)> ReadTdefPreambleAsync(
        long tdefPage,
        CancellationToken cancellationToken)
    {
        byte[] buffer = await ReadAndClonePageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        int numCols = AccessBase.Ru16(buffer, writer._tdef.NumCols);
        int numIdx = AccessBase.Ri32(buffer, writer._tdef.NumCols + 2);
        int numRealIdx = AccessBase.Ri32(buffer, writer._tdef.NumRealIdx);

        if (numIdx <= 0 || numRealIdx <= 0)
        {
            return (TdefPreambleStatus.Empty, new TdefPreamble(buffer, numCols, numIdx, numRealIdx, 0, -1, 0));
        }

        if (numIdx > 1000 || numRealIdx > 1000)
        {
            return (TdefPreambleStatus.TooMany, new TdefPreamble(buffer, numCols, numIdx, numRealIdx, 0, -1, 0));
        }

        int colStart = writer._tdef.BlockEnd + (numRealIdx * writer._tdef.RealIdxEntrySz);
        int namePos = colStart + (numCols * writer._colDesc.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (writer.ReadColumnName(buffer, ref namePos, out _) < 0)
            {
                return (TdefPreambleStatus.ColumnNameWalkFailed, new TdefPreamble(buffer, numCols, numIdx, numRealIdx, 0, i, namePos));
            }
        }

        return (TdefPreambleStatus.Ok, new TdefPreamble(buffer, numCols, numIdx, numRealIdx, namePos, -1, 0));
    }

    /// <summary>
    /// Allocates the page-number array for an N-way leaf split. The first
    /// page reuses <paramref name="originalPage"/>; pages 1..N-1 are
    /// consecutive starting at <paramref name="firstNewPage"/>. Used by
    /// both surgical split paths so the (file-end / staging-counter)
    /// allocation source is the only thing the caller varies.
    /// </summary>
    private static long[] AllocateSplitPageNumbers(long originalPage, int count, long firstNewPage)
    {
        long[] pageNumbers = new long[count];
        pageNumbers[0] = originalPage;
        for (int p = 1; p < count; p++)
        {
            pageNumbers[p] = firstNewPage + (p - 1);
        }

        return pageNumbers;
    }

    /// <summary>
    /// Builds every page of an N-way leaf split into a fresh
    /// <c>byte[][]</c>. Each page's prev/next sibling pointers stitch
    /// the new pages into the existing chain (page 0's prev =
    /// <paramref name="leafPrev"/>, page N-1's next =
    /// <paramref name="leafNext"/>; interior pages point at their
    /// neighbours via <paramref name="pageNumbers"/>). Returns
    /// <see langword="null"/> on any single-entry overflow
    /// (<see cref="ArgumentOutOfRangeException"/> from the page builder).
    /// </summary>
    private byte[][]? TryBuildSplitLeafPages(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long tdefPage,
        SplitPages splitPages,
        long[] pageNumbers,
        long leafPrev,
        long leafNext,
        int maxPrefixLength)
    {
        int splitCount = splitPages.Count;
        byte[][] pageBytesAll = new byte[splitCount][];
        try
        {
            for (int p = 0; p < splitCount; p++)
            {
                long thisPrev = p == 0 ? leafPrev : pageNumbers[p - 1];
                long thisNext = p == splitCount - 1 ? leafNext : pageNumbers[p + 1];
                pageBytesAll[p] = IndexLeafPageBuilder.BuildLeafPage(
                    layout,
                    writer._pgSz,
                    tdefPage,
                    splitPages[p],
                    prevPage: thisPrev,
                    nextPage: thisNext,
                    tailPage: 0,
                    enablePrefixCompression: true,
                    maxPrefixLength: maxPrefixLength);
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }

        return pageBytesAll;
    }

    /// <summary>
    /// Adds a parent-intermediate op for a split leaf/intermediate.
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

        IndexEntry leftLast = splitPages[0][splitPages[0].Count - 1];
        AddParentOp(parentOps, parentPageNumber, takenIndex, IntermediateOpType.Replace, new(leftLast, pageNumbers[0]));

        for (int p = 1; p < splitPages.Count; p++)
        {
            IndexEntry pLast = splitPages[p][splitPages[p].Count - 1];
            AddParentOp(parentOps, parentPageNumber, takenIndex, IntermediateOpType.InsertAfter, new(pLast, pageNumbers[p]));
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
    public async ValueTask MaintainIndexesAsync(long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
    {
        // Jet3 (.mdb Access 97) live leaf maintenance is now
        // supported. The 39-byte real-idx + 20-byte logical-idx layouts
        // (§3.1 / §3.2) and the 0x16-bitmask / 0xF8-first-entry leaf layout
        // (§4.2) are pinned by the format probe and emitted by the same code
        // path Jet4/ACE uses, parameterised on `IndexLeafPageBuilder.LeafPageLayout`.

        // Read the TDEF page bytes. CreateTableAsync may now emit multi-page
        // TDEF chains for wide schemas (>32 col / >16 idx on Jet3, ≫50 col on
        // Jet4 / ACE). The single-page in-place mutation path used here will
        // bail (TdefPreambleStatus != Ok or a downstream layout check) on
        // those tables; that is the same fall-back trigger documented in
        // §7.9 of docs/design/index-and-relationship-format-notes.md.
        var (status, preamble) = await ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (status != TdefPreambleStatus.Ok)
        {
            // Bulk path is silent on every bail (Empty / TooMany /
            // ColumnNameWalkFailed) — caller treats the table as having
            // no maintainable indexes.
            return;
        }

        byte[] tdefBuffer = preamble.Buffer;
        int numCols = preamble.NumCols;
        int numIdx = preamble.NumIdx;
        int numRealIdx = preamble.NumRealIdx;
        int realIdxDescStart = preamble.RealIdxDescStart;

        IndexLeafPageBuilder.LeafPageLayout leafLayout = IndexLeafPageBuilder.GetLayout(writer._format);

        // Decode the index catalog: every populated real-idx slot (with
        // IsUnique already promoted for any slot backing a PK logical-idx),
        // along with the snapshot-index map and pre-resolved key columns.
        IndexCatalogReader.ResolvedIndexCatalog catalog = IndexCatalogReader.ReadResolved(
            tdefBuffer,
            writer._indexLayout,
            writer._indexLayout.GetIndexSection(realIdxDescStart, numRealIdx, numIdx),
            tableDef.Columns);
        Dictionary<int, RealIdxEntry> realIdxByNum = catalog.RealIdxByNum;

        if (realIdxByNum.Count == 0)
        {
            return;
        }

        // Snapshot rows + locations in matching order (same page-walk semantics as
        // the existing UpdateRowsAsync/DeleteRowsAsync rely on).
        using DataTable snapshot = await writer.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        List<RowLocation> locations = await writer.GetLiveRowLocationsAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        int rowCount = Math.Min(snapshot.Rows.Count, locations.Count);

        bool tdefDirty = false;
        long[][]? rebuiltIndexPageGroups = writer._format == DatabaseFormat.Jet3Mdb ? null : new long[numRealIdx][];
        if (rebuiltIndexPageGroups is not null)
        {
            for (int i = 0; i < rebuiltIndexPageGroups.Length; i++)
            {
                rebuiltIndexPageGroups[i] = Array.Empty<long>();
            }
        }

        foreach (var (rieKey, rie) in realIdxByNum)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Skip indexes whose key columns failed to resolve against the
            // snapshot (deleted-column gap).
            if (!catalog.TryGetKeyColumnInfos(rieKey, out List<KeyColumnInfo> keyColInfos))
            {
                continue;
            }

            var entries = new List<(byte[] Key, long Page, byte Row)>(rowCount);
            object?[] cells = new object?[keyColInfos.Count];
            for (int r = 0; r < rowCount; r++)
            {
                for (int k = 0; k < keyColInfos.Count; k++)
                {
                    object cell = snapshot.Rows[r][keyColInfos[k].SnapIdx];
                    cells[k] = cell is DBNull ? null : cell;
                }

                byte[] composite = EncodeCompositeKey(keyColInfos, cells);
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

            long firstPageNumber = writer._stream.Length / writer._pgSz;
            IndexBTreeBuilder.BuildResult build = IndexBTreeBuilder.Build(leafLayout, writer._pgSz, tdefPage, leafEntries, firstPageNumber);
            long rootPageNumber = build.RootPageNumber;
            long[] pageNumbers;

            int oldRootPageNumber = AccessBase.Ri32(tdefBuffer, rie.FirstDpOffset);
            if (build.Pages.Count == 1 && await CanReuseSingleLeafPageAsync(oldRootPageNumber, tdefPage, cancellationToken).ConfigureAwait(false))
            {
                await writer.WritePageAsync(oldRootPageNumber, build.Pages[0], cancellationToken).ConfigureAwait(false);
                rootPageNumber = oldRootPageNumber;
                pageNumbers = [oldRootPageNumber];
            }
            else
            {
                pageNumbers = new long[build.Pages.Count];
                for (int i = 0; i < build.Pages.Count; i++)
                {
                    await writer.AppendPageAsync(build.Pages[i], cancellationToken).ConfigureAwait(false);
                    pageNumbers[i] = firstPageNumber + i;
                }
            }

            AccessBase.Wi32(tdefBuffer, rie.FirstDpOffset, checked((int)rootPageNumber));
            if (rebuiltIndexPageGroups is not null)
            {
                rebuiltIndexPageGroups[rieKey] = pageNumbers;
            }

            tdefDirty = true;
        }

        if (rebuiltIndexPageGroups is not null && HasAnyIndexPageGroup(rebuiltIndexPageGroups))
        {
            long usageMapPage = ReadTableUsageMapPage(tdefBuffer);
            await writer.UpdateTableIndexUsageMapRowsAsync(usageMapPage, rebuiltIndexPageGroups, cancellationToken).ConfigureAwait(false);
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

                WriteIndexUsageMapPointer(tdefBuffer, rebuiltEntry.FirstDpOffset - 4, realIdxNum + 2, usageMapPage);
            }

            tdefDirty = true;
        }

        if (tdefDirty)
        {
            await writer.WritePageAsync(tdefPage, tdefBuffer, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<bool> CanReuseSingleLeafPageAsync(int pageNumber, long tdefPage, CancellationToken cancellationToken)
    {
        if (pageNumber <= 0 || pageNumber >= writer._stream.Length / writer._pgSz)
        {
            return false;
        }

        byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return page[0] == 0x04 && AccessBase.Ri32(page, 4) == tdefPage;
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }
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

    private static int ReadTableUsageMapPage(byte[] tdefBuffer) =>
        tdefBuffer[0x38] | (tdefBuffer[0x39] << 8) | (tdefBuffer[0x3A] << 16);

    private static void WriteIndexUsageMapPointer(byte[] tdefBuffer, int usedPagesOffset, int rowIndex, long usageMapPage)
    {
        int pageNumber = checked((int)usageMapPage);
        tdefBuffer[usedPagesOffset] = checked((byte)rowIndex);
        tdefBuffer[usedPagesOffset + 1] = (byte)(pageNumber & 0xFF);
        tdefBuffer[usedPagesOffset + 2] = (byte)((pageNumber >> 8) & 0xFF);
        tdefBuffer[usedPagesOffset + 3] = (byte)((pageNumber >> 16) & 0xFF);
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
    public async ValueTask<bool> TryMaintainIndexesIncrementalAsync(
        long tdefPage,
        TableDef tableDef,
        List<(RowLocation Loc, object[] Row)>? insertedRows,
        List<(RowLocation Loc, object[] Row)>? deletedRows,
        CancellationToken cancellationToken)
    {
        LastIncrementalBail = null;

        // Jet3 (.mdb Access 97) participates in the
        // incremental fast paths via the per-format LeafPageLayout descriptor
        // (page size 2048, bitmask at 0x16, first entry at 0xF8) and the §3.1
        // 39-byte real-idx physical descriptor (first_dp at phys+34 instead
        // of phys+38). The change-set encode + splice + rebuild logic is
        // unchanged; only the layout-dependent byte offsets and page builder
        // calls fork on `jet3`. Same disposal model as Jet4 — old leaf /
        // intermediate pages are orphaned and reclaimed by Access on
        // Compact & Repair.
        IndexLayout idxLayout = writer._indexLayout;
        var layout = IndexLeafPageBuilder.GetLayout(writer._format);

        int addCount = insertedRows?.Count ?? 0;
        int delCount = deletedRows?.Count ?? 0;
        if (addCount == 0 && delCount == 0)
        {
            return true;
        }

        var (preStatus, preamble) = await ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        switch (preStatus)
        {
            case TdefPreambleStatus.Ok:
                break;
            case TdefPreambleStatus.Empty:
                return true;
            case TdefPreambleStatus.TooMany:
                LastIncrementalBail = $"NumIdx_TooMany numIdx={preamble.NumIdx} numRealIdx={preamble.NumRealIdx}";
                return false;
            case TdefPreambleStatus.ColumnNameWalkFailed:
                LastIncrementalBail = $"C0 col-name walk i={preamble.FailedColumnIndex} namePos={preamble.FailedColumnNamePos}";
                return false;
            default:
                return false;
        }

        byte[] tdefBuffer = preamble.Buffer;
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
            if (!idxLayout.TryReadLogicalEntry(tdefBuffer, logIdxStart, li, out IndexLayout.LogicalIdxEntry entry))
            {
                LastIncrementalBail = $"C1b li={li} logIdxStart={logIdxStart} bufLen={tdefBuffer.Length}";
                return false;
            }

            if (entry.IndexType == IndexKind.ForeignKey)
            {
                LastIncrementalBail = "C1c foreign-key logical index present";
                return false;
            }
        }

        // Decode every real-idx slot's key columns + first_dp offset.
        var slots = new List<RealIdxEntry>(numRealIdx);
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            if (!idxLayout.TryReadRealIdxSlotWithKeyColumns(tdefBuffer, realIdxDescStart, ri, out IndexLayout.RealIdxSlot slot, out List<IndexLayout.KeyColumn> keyCols))
            {
                LastIncrementalBail = $"C1 ri={ri} realIdxDescStart={realIdxDescStart} bufLen={tdefBuffer.Length}";
                return false;
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            slots.Add(slot.ToEntry(keyCols, overrideUnique: false));
        }

        if (slots.Count == 0)
        {
            return true;
        }

        Dictionary<int, int> snapshotIndexByColNum = IndexCatalogReader.BuildColumnNumberToSnapshotIndex(tableDef.Columns);

        bool tdefDirty = false;
        foreach (RealIdxEntry rie in slots)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Resolve key columns to (ColumnInfo, snapshot index, ascending).
            if (!IndexLayout.TryResolveKeyColumnInfos(rie.IndexKeyColumns, tableDef.Columns, snapshotIndexByColNum, out List<KeyColumnInfo> keyColInfos))
            {
                LastIncrementalBail = "C2 resolveFailed";
                return false;
            }

            // Read the index root; require a single-leaf root.
            long firstDp = (uint)AccessBase.Ri32(tdefBuffer, rie.FirstDpOffset);
            if (firstDp <= 0)
            {
                LastIncrementalBail = $"C3 firstDp={firstDp}";
                return false;
            }

            byte[] rootPage = await ReadAndClonePageAsync(firstDp, cancellationToken).ConfigureAwait(false);

            // Encode the change-set keys for this index. Used by both the
            // single-leaf splice and the multi-level rebuild path below.
            var addEntries = EncodeHintEntries(insertedRows, keyColInfos);
            if (addCount > 0 && addEntries.Count != addCount)
            {
                // Encoder rejected at least one row; bail to bulk.
                LastIncrementalBail = $"C4 addEntries.Count={addEntries.Count} addCount={addCount}";
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
                LastIncrementalBail = "C5";
                return false;
            }

            List<(long DataPage, byte DataRow)> removePtrs = new(delCount);
            foreach ((_, long dpDel, byte drDel) in removeEntries)
            {
                removePtrs.Add((dpDel, drDel));
            }

            if (!IndexLeafIncremental.IsSingleRootLeaf(layout, rootPage))
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
                    LastIncrementalBail = $"C6 rootPage[0]={rootPage[0]:X2}";
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
                    LastIncrementalBail = $"C7 firstDp={firstDp}";
                    return false;
                }

                var allExisting = new List<IndexEntry>();
                long walkPage = leftmostLeaf;
                int safetyBudget = 1_000_000; // arbitrary upper bound on leaf count
                while (walkPage > 0)
                {
                    if (--safetyBudget <= 0)
                    {
                        LastIncrementalBail = "C8 safetyBudget";
                        return false;
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    byte[] leaf = await ReadAndClonePageAsync(walkPage, cancellationToken).ConfigureAwait(false);

                    if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
                    {
                        LastIncrementalBail = $"C9 walkPage={walkPage} leaf[0]={leaf[0]:X2}";
                        return false;
                    }

                    allExisting.AddRange(IndexLeafIncremental.DecodeEntries(layout, leaf, writer._pgSz));
                    walkPage = IndexLeafIncremental.ReadNextLeafPage(layout, leaf);
                }

                List<IndexEntry>? splicedAll = IndexLeafIncremental.Splice(allExisting, addEntries, removePtrs);
                if (splicedAll is null)
                {
                    LastIncrementalBail = $"C10 allExisting={allExisting.Count}";
                    return false;
                }

                long firstNewPage = writer._stream.Length / writer._pgSz;
                IndexBTreeBuilder.BuildResult mlBuild;
                try
                {
                    mlBuild = IndexBTreeBuilder.Build(layout, writer._pgSz, tdefPage, splicedAll, firstNewPage);
                }
                catch (ArgumentOutOfRangeException ex)
                {
                    LastIncrementalBail = $"C11 {ex.Message}";
                    return false;
                }

                foreach (byte[] page in mlBuild.Pages)
                {
                    await writer.AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
                }

                AccessBase.Wi32(tdefBuffer, rie.FirstDpOffset, checked((int)mlBuild.RootPageNumber));
                tdefDirty = true;
                continue;
            }

            List<IndexEntry> existing = IndexLeafIncremental.DecodeEntries(layout, rootPage, writer._pgSz);
            List<IndexEntry>? spliced = IndexLeafIncremental.Splice(existing, addEntries, removePtrs);
            if (spliced is null)
            {
                LastIncrementalBail = $"C12 existing={existing.Count}";
                return false;
            }

            byte[]? newLeaf = IndexLeafIncremental.TryRebuildLeaf(layout, writer._pgSz, tdefPage, spliced);
            if (newLeaf is null)
            {
                LastIncrementalBail = $"C13 spliced={spliced.Count}";
                return false;
            }

            await writer.WritePageAsync(firstDp, newLeaf, cancellationToken).ConfigureAwait(false);
        }

        if (tdefDirty)
        {
            await writer.WritePageAsync(tdefPage, tdefBuffer, cancellationToken).ConfigureAwait(false);
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
            byte[] page = await ReadAndClonePageAsync(current, cancellationToken).ConfigureAwait(false);

            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                return current;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return 0;
            }

            long firstChild = IndexLeafIncremental.ReadFirstChildPointer(layout, page, writer._pgSz);
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
        long tailLeafPage = IndexLeafIncremental.ReadTailPage(layout, rootPage);
        if (tailLeafPage <= 0)
        {
            return false;
        }

        byte[] tailLeaf = await ReadAndClonePageAsync(tailLeafPage, cancellationToken).ConfigureAwait(false);

        if (tailLeaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        long tailPrev = IndexLeafIncremental.ReadPrevPage(layout, tailLeaf);
        long tailNext = IndexLeafIncremental.ReadNextLeafPage(layout, tailLeaf);
        if (tailNext != 0)
        {
            // The tail leaf must be the rightmost leaf (next_page == 0). If
            // a previous fast-path append already grew the chain and the
            // root's tail_page wasn't updated, give up — the bulk path will
            // resync the whole tree.
            return false;
        }

        int originalTailPrefLen = BinaryPrimitives.ReadUInt16LittleEndian(tailLeaf.AsSpan(layout.PrefLenOffset, 2));

        List<IndexEntry> existingTail = IndexLeafIncremental.DecodeEntries(layout, tailLeaf, writer._pgSz);

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
                writer._pgSz,
                tdefPage,
                spliced,
                prevPage: tailPrev,
                nextPage: 0,
                tailPage: 0,
                enablePrefixCompression: true,
                maxPrefixLength: originalTailPrefLen);
        }
        catch (ArgumentOutOfRangeException)
        {
            // Tail leaf would overflow a single page. Fall through to the
            // bulk path, which will resnap the tree (and emit a fresh tail leaf).
            return false;
        }

        await writer.WritePageAsync(tailLeafPage, rewritten, cancellationToken).ConfigureAwait(false);
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
        byte[] leaf = await ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

        if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
        {
            return false;
        }

        List<IndexEntry> existingLeafEntries = IndexLeafIncremental.DecodeEntries(layout, leaf, writer._pgSz);
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

        long leafPrev = IndexLeafIncremental.ReadPrevPage(layout, leaf);
        long leafNext = IndexLeafIncremental.ReadNextLeafPage(layout, leaf);
        long leafTail = IndexLeafIncremental.ReadTailPage(layout, leaf);
        int originalPrefLen = BinaryPrimitives.ReadUInt16LittleEndian(leaf.AsSpan(layout.PrefLenOffset, 2));

        byte[] oldMaxKey = existingLeafEntries[existingLeafEntries.Count - 1].Key;

        // 5. Try to fit the spliced entries on the original leaf page.
        byte[]? rebuilt = IndexLeafIncremental.TryRebuildLeafWithSiblings(
            layout, writer._pgSz, tdefPage, spliced, leafPrev, leafNext, leafTail);
        if (rebuilt != null)
        {
            IndexEntry newLast = spliced[spliced.Count - 1];
            bool maxUnchanged = IndexHelpers.CompareKeyBytes(newLast.Key, oldMaxKey) == 0;

            if (maxUnchanged)
            {
                // Pure in-place leaf rewrite — no parent updates needed.
                await writer.WritePageAsync(targetLeafPage, rebuilt, cancellationToken).ConfigureAwait(false);
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
            await writer.WritePageAsync(targetLeafPage, rebuilt, cancellationToken).ConfigureAwait(false);
            foreach ((long pn, byte[] bytes) in ancestorWrites)
            {
                await writer.WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
            }

            return true;
        }

        // 6. Try an N-way leaf split (greedy left-fill).
        // Bails only if a single entry exceeds page payload area.
        SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, writer._pgSz, spliced);
        if (splitPages is null)
        {
            return false;
        }

        // First page reuses the original leaf page; remaining pages are
        // freshly appended at end-of-file.
        int splitCount = splitPages.Count;
        long firstFreshPage = writer._stream.Length / writer._pgSz;
        long[] pageNumbers = AllocateSplitPageNumbers(targetLeafPage, splitCount, firstFreshPage);

        byte[][]? pageBytesAll = TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, originalPrefLen);
        if (pageBytesAll is null)
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
            long appended = await writer.AppendPageAsync(pageBytesAll[p], cancellationToken).ConfigureAwait(false);
            if (appended != pageNumbers[p])
            {
                // Stream extended by something else mid-flight; partial
                // appends are orphans, original tree still intact.
                return false;
            }
        }

        if (leafNext > 0)
        {
            byte[] nextLeaf = await ReadAndClonePageAsync(leafNext, cancellationToken).ConfigureAwait(false);

            // prev_page is per layout (§4.1).
            BinaryPrimitives.WriteInt32LittleEndian(nextLeaf.AsSpan(layout.PrevPageOffset, 4), checked((int)pageNumbers[splitCount - 1]));
            await writer.WritePageAsync(leafNext, nextLeaf, cancellationToken).ConfigureAwait(false);
        }

        await writer.WritePageAsync(targetLeafPage, pageBytesAll[0], cancellationToken).ConfigureAwait(false);

        foreach ((long pn, byte[] bytes) in splitAncestorWrites)
        {
            await writer.WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
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
    /// <para>
    /// When <paramref name="allowTailOvershoot"/> is <see langword="true"/>,
    /// an overshoot (search key sorts strictly above every summary on the
    /// current intermediate) is tolerated by following <c>tail_page</c> (or
    /// the last child pointer as fallback) without recording the step on
    /// <paramref name="path"/>. Used by the catalog-splice path, which
    /// doesn't need a clean (page, taken-index) pair at every level.
    /// </para>
    /// </summary>
    private async ValueTask<long> DescendCapturingAsync(
        IndexLeafPageBuilder.LeafPageLayout layout,
        long rootPage,
        byte[] searchKey,
        List<DescentStep> path,
        CancellationToken cancellationToken,
        bool allowTailOvershoot = false)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await ReadAndClonePageAsync(current, cancellationToken).ConfigureAwait(false);

            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                return current;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return 0;
            }

            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, writer._pgSz);
            if (entries.Count == 0)
            {
                return 0;
            }

            int idx = IndexHelpers.SelectChildIndexFromDecoded(entries, searchKey);
            if (idx < 0)
            {
                if (!allowTailOvershoot)
                {
                    // Search key sorts strictly above every summary on this
                    // intermediate. The seeker would follow tail_page here,
                    // but the surgical path needs a clean (page, taken-index)
                    // pair at every level for an in-place ancestor rewrite — bail.
                    return 0;
                }

                long tail = IndexLeafIncremental.ReadTailPage(layout, page);
                long nextChild = tail > 0 ? tail : ReadLastChildPointer(page, writer._pgSz, layout);
                if (nextChild <= 0)
                {
                    return 0;
                }

                current = nextChild;
                continue;
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
            var (prev, next, tail) = IndexLeafIncremental.ReadSiblingPointers(layout, pageBytes);

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, writer._pgSz, tdefPage, newEntries, prev, next, tail);
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
        var (parentPrev, parentNext, parentTail) = IndexLeafIncremental.ReadSiblingPointers(layout, parentBytes);

        byte[]? rebuiltParent = IndexBTreeBuilder.TryBuildIntermediatePage(
            layout, writer._pgSz, tdefPage, newEntries, parentPrev, parentNext, parentTail);
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

        long nextAllocatedPageNumber = writer._stream.Length / writer._pgSz;

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
            byte[] preBytes = await writer.ReadPageAsync(pre.LeafPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (preBytes[0] != Constants.IndexLeafPage.PageTypeLeaf)
                {
                    continue;
                }

                List<IndexEntry> preExisting = IndexLeafIncremental.DecodeEntries(layout, preBytes, writer._pgSz);
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
                AccessBase.ReturnPage(preBytes);
            }
        }

        foreach (LeafGroup group in groups.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] leaf = await ReadAndClonePageAsync(group.LeafPage, cancellationToken).ConfigureAwait(false);

            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                return false;
            }

            List<IndexEntry> existing = IndexLeafIncremental.DecodeEntries(layout, leaf, writer._pgSz);
            if (existing.Count == 0)
            {
                return false;
            }

            List<IndexEntry>? spliced = IndexLeafIncremental.Splice(existing, group.Adds, group.RemovePtrs);
            if (spliced is null)
            {
                return false;
            }

            long leafPrev = IndexLeafIncremental.ReadPrevPage(layout, leaf);
            long leafNext = IndexLeafIncremental.ReadNextLeafPage(layout, leaf);
            long leafTail = IndexLeafIncremental.ReadTailPage(layout, leaf);
            int originalPrefLen = BinaryPrimitives.ReadUInt16LittleEndian(leaf.AsSpan(layout.PrefLenOffset, 2));

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
                layout, writer._pgSz, tdefPage, spliced, leafPrev, leafNext, leafTail);
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
            SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, writer._pgSz, spliced);
            if (splitPages is null)
            {
                return false;
            }

            int splitCount = splitPages.Count;

            // First page reuses group.LeafPage; remaining pages are
            // freshly allocated from the staging counter.
            long[] pageNumbers = AllocateSplitPageNumbers(group.LeafPage, splitCount, nextAllocatedPageNumber);
            nextAllocatedPageNumber += splitCount - 1;

            byte[][]? pageBytesAll = TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, originalPrefLen);
            if (pageBytesAll is null)
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

        long verifyNextPage = writer._stream.Length / writer._pgSz;
        foreach (byte[] pageBytes in newPageAppends)
        {
            long appended = await writer.AppendPageAsync(pageBytes, cancellationToken).ConfigureAwait(false);
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
            byte[] neighbour = await ReadAndClonePageAsync(neighbourPage, cancellationToken).ConfigureAwait(false);

            // §4.1 prev_page (per layout).
            BinaryPrimitives.WriteInt32LittleEndian(neighbour.AsSpan(layout.PrevPageOffset, 4), checked((int)newPrevValue));
            await writer.WritePageAsync(neighbourPage, neighbour, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long neighbourPage, long newNextValue) in leafPrevPointerPatches)
        {
            byte[] neighbour = await ReadAndClonePageAsync(neighbourPage, cancellationToken).ConfigureAwait(false);

            // §4.1 next_page (per layout).
            BinaryPrimitives.WriteInt32LittleEndian(neighbour.AsSpan(layout.NextPageOffset, 4), checked((int)newNextValue));
            await writer.WritePageAsync(neighbourPage, neighbour, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long pageNum, byte[] bytes) in existingPageRewrites)
        {
            await writer.WritePageAsync(pageNum, bytes, cancellationToken).ConfigureAwait(false);
        }

        long? newRootPage = stagingState.NewRootPage;

        // if the root intermediate split, patch the real-idx
        // first_dp slot on the TDEF page to point at the freshly-allocated
        // root. The new root page itself was already appended via
        // newPageAppends above, so the page number is stable.
        if (newRootPage.HasValue)
        {
            byte[] tdefBytes = await ReadAndClonePageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

            BinaryPrimitives.WriteInt32LittleEndian(tdefBytes.AsSpan(firstDpOffset, 4), checked((int)newRootPage.Value));
            await writer.WritePageAsync(tdefPage, tdefBytes, cancellationToken).ConfigureAwait(false);
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
        IndexLeafPageBuilder.LeafPageLayout layout,
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
            return IndexLeafIncremental.ReadTailPage(layout, rewriteBytes);
        }

        byte[] raw = await writer.ReadPageAsync(intermediatePage, cancellationToken).ConfigureAwait(false);
        try
        {
            return IndexLeafIncremental.ReadTailPage(layout, raw);
        }
        finally
        {
            AccessBase.ReturnPage(raw);
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
            var (origPrev, origNext, origTail) = IndexLeafIncremental.ReadSiblingPointers(layout, origBytes);

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
                        layout, lastChildPage, intermediateTailOverrides, existingPageRewrites, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            if (newTail != origTail)
            {
                intermediateTailOverrides[deepest] = newTail;
            }

            byte[]? rebuilt = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, writer._pgSz, tdefPage, newEntries, origPrev, origNext, newTail);
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
                    IndexHelpers.TryGreedySplitIntermediateInN(layout, writer._pgSz, tdefPage, newEntries);
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
                            layout, lastEntry.ChildPage, intermediateTailOverrides, existingPageRewrites, cancellationToken)
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
                            layout, writer._pgSz, tdefPage, splitInts[p], thisPrev, thisNext, intTails[p]);
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
                        [.. splitInts.ConvertAll(s => s.ConvertAll(si => si.Entry))],
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
                            layout, writer._pgSz, tdefPage, rootEntries, prevPage: 0, nextPage: 0, tailPage: intTails[nSplit - 1]);
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
    /// Encodes a single composite index key by per-column-encoding then
    /// concatenating. Honours <see cref="DatabaseFormat.Jet4Mdb"/>'s legacy
    /// fixed-point byte-twiddling for <c>T_NUMERIC</c> columns. Throws
    /// whatever <see cref="IndexKeyEncoder"/> throws on encoder rejection
    /// (<see cref="NotSupportedException"/> / <see cref="ArgumentException"/>
    /// / <see cref="OverflowException"/>); callers that want soft-fail
    /// behaviour should use <see cref="TryEncodeCompositeKey"/>.
    /// </summary>
    private byte[] EncodeCompositeKey(List<KeyColumnInfo> keyColInfos, object?[] cells)
    {
        bool legacyNumeric = writer._format == DatabaseFormat.Jet4Mdb;

        byte[][] perColumn = new byte[keyColInfos.Count][];
        int totalLen = 0;
        for (int k = 0; k < keyColInfos.Count; k++)
        {
            (ColumnInfo col, int _, bool ascending) = keyColInfos[k];
            object? value = cells[k];
            perColumn[k] = col.Type == T_NUMERIC
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, col.NumericScale, legacyNumeric)
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
    /// Soft-fail wrapper over <see cref="EncodeCompositeKey"/>: gathers the
    /// per-column cells for <paramref name="row"/> against
    /// <paramref name="keyColInfos"/>'s snapshot indices and returns
    /// <see langword="null"/> when the row is too short or any encoder
    /// rejects (<see cref="NotSupportedException"/>,
    /// <see cref="ArgumentException"/>, or <see cref="OverflowException"/>).
    /// Used by the incremental + catalog-splice paths to bail to bulk on any
    /// encoder rejection.
    /// </summary>
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
            return EncodeCompositeKey(keyColInfos, cells);
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
    /// Returns a partially-filled list when an encoder throws — the caller
    /// detects this by comparing <c>Count</c> to the input count and bailing
    /// to the bulk-rebuild path.
    /// </summary>
    private List<IndexEntry> EncodeHintEntries(
        List<(RowLocation Loc, object[] Row)>? rows,
        List<KeyColumnInfo> keyColInfos)
    {
        var results = new List<IndexEntry>(rows?.Count ?? 0);
        if (rows == null || rows.Count == 0)
        {
            return results;
        }

        foreach ((RowLocation loc, object[] row) in rows)
        {
            byte[]? composite = TryEncodeCompositeKey(keyColInfos, row);
            if (composite is null)
            {
                return results;
            }

            results.Add(new IndexEntry(composite, loc.PageNumber, (byte)loc.RowIndex));
        }

        return results;
    }

    /// <summary>
    /// Reads the 4-byte big-endian child-page pointer at the END of the LAST
    /// entry on an intermediate (<c>0x03</c>) page. Each intermediate entry
    /// trails with <c>[3 B BE data page][1 B data row][4 B BE child page]</c>;
    /// the bitmask-driven entry layout means the last entry ends exactly at
    /// <c>payloadEnd</c>, so the child pointer occupies
    /// <c>[payloadEnd-4, payloadEnd)</c>.
    /// </summary>
    private static long ReadLastChildPointer(byte[] page, int pageSize, IndexLeafPageBuilder.LeafPageLayout layout)
    {
        if (page == null || page.Length < pageSize)
        {
            return 0;
        }

        int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
        int payloadEnd = pageSize - freeSpace;
        if (payloadEnd < layout.FirstEntryOffset + 8)
        {
            return 0;
        }

        return IndexLeafIncremental.DecodeIntermediateChildPointer(page, payloadEnd - 4);
    }

    /// <summary>
    /// Splices a single new catalog row's index entry into the rightmost
    /// (tail) leaf of every real-idx slot on a system table's index B-tree
    /// without re-encoding any pre-existing entries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Used by <c>InsertCatalogEntryAsync</c> for MSysObjects to keep
    /// Microsoft Access's PK Id index consistent with the new TDEF row, while
    /// preserving the byte-for-byte content of every other leaf page on the
    /// index — including Access-authored leaves that hold special rows
    /// (e.g. the <c>Databases</c> properties row) whose Lv/LvProp blobs the
    /// writer cannot losslessly re-encode. See
    /// <c>docs/design/catalog-index-maintenance-notes.md</c>.
    /// </para>
    /// <para>
    /// Phase C1 scope (tail-leaf-only):
    /// <list type="bullet">
    ///   <item>Descends to the rightmost leaf by following each
    ///   intermediate's LAST child pointer.</item>
    ///   <item>Splices when the new key sorts strictly greater than every
    ///   existing entry on the tail leaf and the rewritten leaf still fits
    ///   on one page.</item>
    ///   <item>Returns <see langword="false"/> on any unsupported case
    ///   (non-Jet4 format, malformed page, key not greater than tail max,
    ///   tail leaf overflow, or descent encountering a non-tail leaf).</item>
    /// </list>
    /// On <see langword="false"/> the caller should treat the catalog index
    /// as un-maintained for this row (the row is still present on disk;
    /// downstream Compact &amp; Repair may report JET <c>-1601</c>).
    /// </para>
    /// </remarks>
    public async ValueTask<bool> TrySpliceCatalogIndexEntryAsync(
        long tdefPage,
        TableDef tableDef,
        RowLocation newRowLoc,
        object[] newRowValues,
        CancellationToken cancellationToken)
    {
        // Phase C1 targets ACCDB / Jet4 only. Jet3 catalog index format
        // differs (39-byte real-idx descriptor, different sort-key encoding)
        // and is left to a future phase.
        if (writer._format == DatabaseFormat.Jet3Mdb)
        {
            return false;
        }

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.LeafPageLayout.Jet4;

        LastIncrementalBail = null;

        var (preStatus, preamble) = await ReadTdefPreambleAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        switch (preStatus)
        {
            case TdefPreambleStatus.Ok:
                break;
            case TdefPreambleStatus.Empty:
                LastIncrementalBail = $"S0 numIdx={preamble.NumIdx} numRealIdx={preamble.NumRealIdx}";
                return true;
            case TdefPreambleStatus.TooMany:
                LastIncrementalBail = "S1 too many idx";
                return false;
            case TdefPreambleStatus.ColumnNameWalkFailed:
                return false;
            default:
                return false;
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
            writer._indexLayout,
            writer._indexLayout.GetIndexSection(realIdxDescStart, numRealIdx, numIdx),
            tableDef.Columns);

        foreach ((int ri, RealIdxEntry rie) in catalog.RealIdxByNum)
        {
            long firstDp = (uint)AccessBase.Ri32(tdefBuf, rie.FirstDpOffset);
            if (firstDp <= 0)
            {
                LastIncrementalBail = $"S2 ri={ri} firstDp=0";
                continue;
            }

            // Resolve key columns to TDEF ColumnInfos.
            if (!catalog.TryGetKeyColumnInfos(ri, out List<KeyColumnInfo> keyColInfos))
            {
                LastIncrementalBail = $"S3 ri={ri} resolveFailed";
                return false;
            }

            // Encode the composite key for the new row.
            byte[]? composite = TryEncodeCompositeKey(keyColInfos, newRowValues);
            if (composite is null)
            {
                LastIncrementalBail = $"S4 ri={ri} encErr";
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
            long targetLeafPage = await DescendCapturingAsync(
                layout, firstDp, composite, descentPath, cancellationToken, allowTailOvershoot: false).ConfigureAwait(false);
            if (targetLeafPage <= 0)
            {
                // Overshoot — retry with tail following. Path will be
                // incomplete but the chain walk handles placement.
                descentPath.Clear();
                hasCleanPath = false;
                targetLeafPage = await DescendCapturingAsync(
                    layout, firstDp, composite, descentPath, cancellationToken, allowTailOvershoot: true).ConfigureAwait(false);
                if (targetLeafPage <= 0)
                {
                    LastIncrementalBail = $"S5 ri={ri} descent failed firstDp={firstDp}";
                    return false;
                }
            }

            byte[] leaf = await ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

            if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                LastIncrementalBail = $"S8 ri={ri} targetLeafPage={targetLeafPage} type=0x{leaf[0]:X2}";
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
                long nextLeaf = IndexLeafIncremental.ReadNextLeafPage(layout, leaf);
                if (nextLeaf <= 0)
                {
                    break;
                }

                List<IndexEntry> probe = IndexLeafIncremental.DecodeEntries(layout, leaf, writer._pgSz);
                if (probe.Count == 0 || IndexHelpers.CompareKeyBytes(composite, probe[probe.Count - 1].Key) <= 0)
                {
                    // composite belongs in this leaf (or earlier).
                    break;
                }

                if (--chainBudget <= 0)
                {
                    LastIncrementalBail = $"S8b ri={ri} chainBudget exhausted";
                    return false;
                }

                targetLeafPage = nextLeaf;
                leaf = await ReadAndClonePageAsync(targetLeafPage, cancellationToken).ConfigureAwait(false);

                if (leaf[0] != Constants.IndexLeafPage.PageTypeLeaf)
                {
                    LastIncrementalBail = $"S8c ri={ri} walkedTo={targetLeafPage} type=0x{leaf[0]:X2}";
                    return false;
                }
            }

            long leafPrev = IndexLeafIncremental.ReadPrevPage(layout, leaf);
            long leafNext = IndexLeafIncremental.ReadNextLeafPage(layout, leaf);
            long leafTail = IndexLeafIncremental.ReadTailPage(layout, leaf);
            int originalPrefLen = BinaryPrimitives.ReadUInt16LittleEndian(leaf.AsSpan(layout.PrefLenOffset, 2));

            List<IndexEntry> existing = IndexLeafIncremental.DecodeEntries(layout, leaf, writer._pgSz);

            var addEntries = new List<IndexEntry>(1)
            {
                new IndexEntry(composite, newRowLoc.PageNumber, (byte)newRowLoc.RowIndex),
            };

            List<IndexEntry>? spliced = IndexLeafIncremental.Splice(
                existing,
                addEntries,
                Array.Empty<(long DataPage, byte DataRow)>());
            if (spliced is null)
            {
                LastIncrementalBail = $"S11 ri={ri} splice null";
                return false;
            }

            byte[] rewritten;
            try
            {
                rewritten = IndexLeafPageBuilder.BuildLeafPage(
                    layout,
                    writer._pgSz,
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
                SplitPages? splitPages = IndexHelpers.TryGreedySplitLeafInN(layout, writer._pgSz, spliced);
                if (splitPages is null)
                {
                    LastIncrementalBail = $"S12 ri={ri} split failed";
                    return false;
                }

                int splitCount = splitPages.Count;
                long firstFreshPage = writer._stream.Length / writer._pgSz;
                long[] pageNumbers = AllocateSplitPageNumbers(targetLeafPage, splitCount, firstFreshPage);

                byte[][]? pageBytesAll = TryBuildSplitLeafPages(layout, tdefPage, splitPages, pageNumbers, leafPrev, leafNext, originalPrefLen);
                if (pageBytesAll is null)
                {
                    LastIncrementalBail = $"S12b ri={ri} split build failed";
                    return false;
                }

                // Compute ancestor writes if we have a clean descent path.
                List<(long PageNum, byte[] Bytes)>? ancestorWrites = null;
                if (hasCleanPath && descentPath.Count > 0)
                {
                    IndexEntry leftLast = splitPages.GetLastEntry(0);
                    var leftSummary = new DecodedIntermediateEntry(leftLast, ChildPage: pageNumbers[0]);
                    var rightSummaries = new DecodedIntermediateEntry[splitCount - 1];
                    for (int p = 1; p < splitCount; p++)
                    {
                        IndexEntry last = splitPages.GetLastEntry(p);
                        rightSummaries[p - 1] = new DecodedIntermediateEntry(last, ChildPage: pageNumbers[p]);
                    }

                    ancestorWrites = PrepareAncestorSplitWrites(layout, tdefPage, descentPath, leftSummary, rightSummaries);
                    if (ancestorWrites is null)
                    {
                        LastIncrementalBail = $"S12c ri={ri} ancestor overflow";
                        return false;
                    }
                }

                // Commit: append new pages, patch next-leaf's prev pointer,
                // rewrite original leaf, then ancestors.
                for (int p = 1; p < splitCount; p++)
                {
                    long appended = await writer.AppendPageAsync(pageBytesAll[p], cancellationToken).ConfigureAwait(false);
                    if (appended != pageNumbers[p])
                    {
                        LastIncrementalBail = $"S12d ri={ri} append mismatch expected={pageNumbers[p]} got={appended}";
                        return false;
                    }
                }

                if (leafNext > 0)
                {
                    byte[] nextLeafBuf = await ReadAndClonePageAsync(leafNext, cancellationToken).ConfigureAwait(false);
                    BinaryPrimitives.WriteInt32LittleEndian(nextLeafBuf.AsSpan(layout.PrevPageOffset, 4), checked((int)pageNumbers[splitCount - 1]));
                    await writer.WritePageAsync(leafNext, nextLeafBuf, cancellationToken).ConfigureAwait(false);
                }

                await writer.WritePageAsync(targetLeafPage, pageBytesAll[0], cancellationToken).ConfigureAwait(false);

                if (ancestorWrites is not null)
                {
                    foreach ((long pn, byte[] bytes) in ancestorWrites)
                    {
                        await writer.WritePageAsync(pn, bytes, cancellationToken).ConfigureAwait(false);
                    }
                }

                continue;
            }

            await writer.WritePageAsync(targetLeafPage, rewritten, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }
}
