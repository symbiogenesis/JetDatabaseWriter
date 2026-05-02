namespace JetDatabaseWriter.Internal;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal.Models;

/// <summary>
/// Single-pass decoder of a TDEF page's index catalog: combines the real-idx
/// physical-descriptor walk (§3.1) and the logical-idx entry walk (§3.2) into
/// one call so that <see cref="JetDatabaseWriter.Core.AccessWriter"/>'s
/// catalog-touching methods (<c>MaintainIndexesAsync</c>,
/// <c>LoadUniqueIndexDescriptorsAsync</c>, <c>TrySpliceCatalogIndexEntryAsync</c>)
/// no longer re-implement the same ~50-line decode each.
/// <para>
/// Caller is responsible for advancing past the column-name block to compute
/// <c>realIdxDescStart</c> (that walk depends on the writer's per-format
/// column-name encoding and is not duplicated across the catalog callers).
/// Pass <c>logIdxNames</c> when the caller needs best-effort
/// logical-idx names per real-idx slot; pass <see langword="null"/> when only
/// the real-idx → key-list map and PK-promotion set are required.
/// </para>
/// </summary>
internal static class IndexCatalogReader
{
    /// <summary>
    /// Reads every populated real-idx slot, then walks logical-idx entries to
    /// (a) collect the set of real-idx slots backing a primary-key
    /// (<c>index_type = 0x01</c>) logical-idx — those slots are also marked
    /// unique on the returned <see cref="IndexLayout.RealIdxEntry"/> values
    /// even when their physical <c>flags &amp; 0x01</c> bit is clear — and
    /// (b) when <paramref name="logIdxNames"/> is supplied, capture a
    /// best-effort name per real-idx (first logical-idx referencing that
    /// real-idx wins).
    /// </summary>
    /// <param name="tdefBuffer">Full decoded TDEF buffer.</param>
    /// <param name="layout">Per-format real-idx / logical-idx layout descriptor.</param>
    /// <param name="anchors">Index-section anchors + slot counts (typically obtained via <see cref="IndexLayout.GetIndexSection"/> after the caller has walked the column-name block to compute <see cref="IndexLayout.IndexSectionAnchors.RealIdxDescStart"/>).</param>
    /// <param name="logIdxNames">Optional pre-decoded logical-idx names list (one per logical entry, in order); pass <see langword="null"/> to skip name capture.</param>
    public static IndexCatalog Read(
        byte[] tdefBuffer,
        IndexLayout layout,
        IndexLayout.IndexSectionAnchors anchors,
        IReadOnlyList<string>? logIdxNames = null)
    {
        var realIdxByNum = new Dictionary<int, IndexLayout.RealIdxEntry>(anchors.NumRealIdx);
        for (int ri = 0; ri < anchors.NumRealIdx; ri++)
        {
            if (!layout.TryReadRealIdxSlotWithKeyColumns(
                    tdefBuffer,
                    anchors.RealIdxDescStart,
                    ri,
                    out IndexLayout.RealIdxSlot slot,
                    out List<IndexLayout.KeyColumn> keyCols))
            {
                break;
            }

            if (keyCols.Count == 0)
            {
                continue;
            }

            realIdxByNum[ri] = slot.ToEntry(keyCols);
        }

        var pkRealIdxNums = new HashSet<int>();
        var nameByRealIdx = new Dictionary<int, string>();
        for (int li = 0; li < anchors.NumIdx; li++)
        {
            if (!layout.TryReadLogicalEntry(tdefBuffer, anchors.LogIdxStart, li, out IndexLayout.LogicalIdxEntry entry))
            {
                break;
            }

            int realIdxNum = entry.IndexNum2;
            if (entry.IndexType == IndexKind.PrimaryKey)
            {
                pkRealIdxNums.Add(realIdxNum);
                if (realIdxByNum.TryGetValue(realIdxNum, out IndexLayout.RealIdxEntry rie))
                {
                    realIdxByNum[realIdxNum] = rie with { IsUnique = true };
                }
            }

            if (logIdxNames is not null && li < logIdxNames.Count)
            {
                nameByRealIdx.TryAdd(realIdxNum, logIdxNames[li]);
            }
        }

        return new IndexCatalog(realIdxByNum, pkRealIdxNums, nameByRealIdx);
    }

    /// <summary>
    /// Builds the <c>ColNum → snapshot row index</c> lookup that every
    /// catalog-using path needs in order to translate a real-idx key column's
    /// <c>col_num</c> (which can outrun the snapshot index when columns have
    /// been deleted) into the matching slot in a row's value array. Equivalent
    /// to <see cref="IndexLayout.TryResolveKeyColumnInfos"/>'s expected
    /// <c>snapshotIndexByColNum</c> argument.
    /// </summary>
    public static Dictionary<int, int> BuildColumnNumberToSnapshotIndex(IReadOnlyList<ColumnInfo> tableColumns)
    {
        var map = new Dictionary<int, int>(tableColumns.Count);
        for (int c = 0; c < tableColumns.Count; c++)
        {
            map[tableColumns[c].ColNum] = c;
        }

        return map;
    }

    /// <summary>
    /// Convenience overload of <see cref="Read"/> that additionally builds
    /// the <c>ColNum → snapshot index</c> lookup and pre-resolves each
    /// real-idx slot's key columns against <paramref name="tableColumns"/>.
    /// Real-idx slots whose key columns can't be resolved (deleted-column
    /// gaps) are still present in the underlying <see cref="IndexCatalog"/>
    /// but absent from <see cref="ResolvedIndexCatalog.KeyColumnInfosByRealIdx"/>;
    /// callers that need to bail vs. skip on resolve failure can branch on
    /// the lookup result via <see cref="ResolvedIndexCatalog.TryGetKeyColumnInfos"/>.
    /// Collapses the catalog-touching prelude shared by every catalog-using
    /// path in <see cref="JetDatabaseWriter.Core.AccessWriter"/>.
    /// </summary>
    public static ResolvedIndexCatalog ReadResolved(
        byte[] tdefBuffer,
        IndexLayout layout,
        IndexLayout.IndexSectionAnchors anchors,
        IReadOnlyList<ColumnInfo> tableColumns,
        IReadOnlyList<string>? logIdxNames = null)
    {
        IndexCatalog catalog = Read(tdefBuffer, layout, anchors, logIdxNames);
        Dictionary<int, int> snapshotIndexByColNum = BuildColumnNumberToSnapshotIndex(tableColumns);
        var keyColInfosByRealIdx = new Dictionary<int, List<IndexLayout.KeyColumnInfo>>(catalog.RealIdxByNum.Count);
        foreach ((int realIdxNum, IndexLayout.RealIdxEntry rie) in catalog.RealIdxByNum)
        {
            if (IndexLayout.TryResolveKeyColumnInfos(
                    rie.IndexKeyColumns,
                    tableColumns,
                    snapshotIndexByColNum,
                    out List<IndexLayout.KeyColumnInfo> infos))
            {
                keyColInfosByRealIdx[realIdxNum] = infos;
            }
        }

        return new ResolvedIndexCatalog(catalog, snapshotIndexByColNum, keyColInfosByRealIdx);
    }

    /// <summary>
    /// Decoded TDEF index catalog returned by <see cref="Read"/>.
    /// </summary>
    /// <param name="RealIdxByNum">Real-idx slot number → decoded entry. <see cref="IndexLayout.RealIdxEntry.IsUnique"/> reflects the physical <c>flags &amp; 0x01</c> bit OR a PK promotion (any logical-idx with <c>index_type = 0x01</c> referencing this slot via <c>index_num2</c>).</param>
    /// <param name="PkRealIdxNums">Set of real-idx slot numbers backing a primary-key logical-idx.</param>
    /// <param name="NameByRealIdx">Best-effort logical-idx name per real-idx slot (first logical-idx referencing that slot wins). Empty when <c>logIdxNames</c> was not supplied to <see cref="Read"/>.</param>
    public sealed record IndexCatalog(
        Dictionary<int, IndexLayout.RealIdxEntry> RealIdxByNum,
        HashSet<int> PkRealIdxNums,
        Dictionary<int, string> NameByRealIdx)
    {
        /// <summary>
        /// Returns the best-effort logical-idx name for <paramref name="realIdxNum"/>,
        /// or the synthetic <c>realidx#N</c> fallback when no logical-idx
        /// references this real-idx (or when names were not captured).
        /// </summary>
        public string GetNameOrFallback(int realIdxNum)
            => NameByRealIdx.TryGetValue(realIdxNum, out string? n) ? n : $"realidx#{realIdxNum}";

        /// <summary>
        /// Returns <see langword="true"/> when <paramref name="realIdxNum"/>
        /// is unique either by its physical <c>flags &amp; 0x01</c> bit or by
        /// PK promotion (any logical-idx with <c>index_type = 0x01</c>
        /// references this slot).
        /// </summary>
        public bool IsUniqueOrPk(int realIdxNum)
            => (RealIdxByNum.TryGetValue(realIdxNum, out IndexLayout.RealIdxEntry rie) && rie.IsUnique)
                || PkRealIdxNums.Contains(realIdxNum);
    }

    /// <summary>
    /// Result of <see cref="ReadResolved"/>: bundles the decoded
    /// <see cref="IndexCatalog"/> with the <c>ColNum → snapshot index</c>
    /// lookup and the per-real-idx pre-resolved key columns. Catalog-touching
    /// paths in <see cref="JetDatabaseWriter.Core.AccessWriter"/> can iterate
    /// <see cref="IndexCatalog.RealIdxByNum"/> and call
    /// <see cref="TryGetKeyColumnInfos"/> directly rather than re-running the
    /// snapshot-map build + per-slot resolve loop.
    /// </summary>
    /// <param name="Catalog">Decoded catalog (real-idx slots, PK promotion, optional names).</param>
    /// <param name="SnapshotIndexByColNum">ColNum → snapshot row index lookup over the same <c>tableColumns</c> passed to <see cref="ReadResolved"/>.</param>
    /// <param name="KeyColumnInfosByRealIdx">Pre-resolved key columns per real-idx slot. A real-idx present in <see cref="IndexCatalog.RealIdxByNum"/> but absent here failed resolution (deleted-column gap); callers decide whether that's a skip or a bail.</param>
    public sealed record ResolvedIndexCatalog(
        IndexCatalog Catalog,
        Dictionary<int, int> SnapshotIndexByColNum,
        Dictionary<int, List<IndexLayout.KeyColumnInfo>> KeyColumnInfosByRealIdx)
    {
        /// <summary>Gets the decoded real-idx slots; shortcut for <c>Catalog.RealIdxByNum</c>.</summary>
        public Dictionary<int, IndexLayout.RealIdxEntry> RealIdxByNum => Catalog.RealIdxByNum;

        /// <summary>
        /// Returns the pre-resolved key columns for <paramref name="realIdxNum"/>,
        /// or <see langword="false"/> when the slot's columns could not be
        /// resolved against the table snapshot (deleted-column gap).
        /// </summary>
        public bool TryGetKeyColumnInfos(int realIdxNum, out List<IndexLayout.KeyColumnInfo> keyColInfos)
        {
            if (KeyColumnInfosByRealIdx.TryGetValue(realIdxNum, out List<IndexLayout.KeyColumnInfo>? infos))
            {
                keyColInfos = infos;
                return true;
            }

            keyColInfos = [];
            return false;
        }
    }
}
