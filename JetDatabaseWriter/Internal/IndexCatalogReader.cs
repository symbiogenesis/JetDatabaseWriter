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
    }
}
