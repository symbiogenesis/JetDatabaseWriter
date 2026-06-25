namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Single-pass decoder of a TDEF page's index catalog: combines the real-idx
/// physical-descriptor walk (§3.1) and the logical-idx entry walk (§3.2) into
/// one call so that <see cref="AccessWriter"/>'s
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
    /// unique on the returned <see cref="RealIdxEntry"/> values
    /// even when their physical <c>flags &amp; 0x01</c> bit is clear — and
    /// (b) when <paramref name="logIdxNames"/> is supplied, capture a
    /// best-effort name per real-idx (first logical-idx referencing that
    /// real-idx wins).
    /// </summary>
    /// <param name="tdefBuffer">Full decoded TDEF buffer.</param>
    /// <param name="layout">Per-format real-idx / logical-idx layout descriptor.</param>
    /// <param name="anchors">Index-section anchors + slot counts (typically obtained via <see cref="IndexLayout.GetIndexSection"/> after the caller has walked the column-name block to compute <see cref="IndexSectionAnchors.RealIdxDescStart"/>).</param>
    /// <param name="logIdxNames">Optional pre-decoded logical-idx names list (one per logical entry, in order); pass <see langword="null"/> to skip name capture.</param>
    public static IndexCatalog Read(
        byte[] tdefBuffer,
        IndexLayout layout,
        IndexSectionAnchors anchors,
        IReadOnlyList<string>? logIdxNames = null)
    {
        var realIdxByNum = new Dictionary<int, RealIdxEntry>(anchors.NumRealIdx);
        for (int ri = 0; ri < anchors.NumRealIdx; ri++)
        {
            if (!layout.TryReadRealIdxSlotWithKeyColumns(
                    tdefBuffer,
                    anchors.RealIdxDescStart,
                    ri,
                    out RealIdxSlot slot,
                    out List<KeyColumn>? keyCols))
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
            if (!layout.TryReadLogicalEntry(tdefBuffer, anchors.LogIdxStart, li, out LogicalIdxEntry entry))
            {
                break;
            }

            int realIdxNum = entry.IndexNum2;
            if (entry.IndexType == IndexKind.PrimaryKey)
            {
                pkRealIdxNums.Add(realIdxNum);
                if (realIdxByNum.TryGetValue(realIdxNum, out RealIdxEntry rie))
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
    /// Walks the real-idx + logical-idx sections of <paramref name="td"/> and
    /// returns one public <see cref="IndexMetadata"/> per logical index, as
    /// consumed by <see cref="AccessReader.ListIndexesAsync"/> and the
    /// index-seek path. Returns an empty list when the TDEF reports no indexes
    /// or a malformed column-name / index-section layout.
    /// <para>
    /// Distinct from <see cref="Read"/>: that overload returns the writer-side
    /// real-idx-centric <see cref="IndexCatalog"/> (which folds PK promotion
    /// into a single <c>IsUnique</c> bit and drops the raw <c>flags</c> byte),
    /// whereas this projection walks the logical-idx entries one-to-one and
    /// keeps the physical <c>flags</c> byte so <see cref="IndexMetadata"/> can
    /// surface <see cref="IndexMetadata.HasUniqueFlag"/>,
    /// <see cref="IndexMetadata.IgnoreNulls"/>, and
    /// <see cref="IndexMetadata.IsRequired"/> independently. It additionally
    /// owns the format-specific prelude (counts, column-name walk, section
    /// anchors, index-name walk) via <paramref name="db"/>, so callers pass
    /// only the raw TDEF bytes and the table's columns.
    /// </para>
    /// </summary>
    /// <param name="db">Format context supplying the per-format TDEF, column-descriptor, and index layouts plus the column-name decoder.</param>
    /// <param name="td">The concatenated TDEF page-chain bytes.</param>
    /// <param name="columns">The table's parsed columns, used to resolve key-column names (honouring deleted-column gaps).</param>
    public static List<IndexMetadata> ReadMetadata(AccessBase db, byte[] td, IReadOnlyList<ColumnInfo> columns)
    {
        int numCols = Ru16(td, db.TDef.NumCols);
        int numIdx = Ri32(td, db.TDef.NumCols + 2);
        int numRealIdx = Ri32(td, db.TDef.NumRealIdx);

        // Defensive bounds: corrupt TDEFs can report absurd counts.
        if (numIdx is <= 0 or > Constants.TableDefinition.MaxIndexes)
        {
            return [];
        }

        if (numRealIdx is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            numRealIdx = 0;
        }

        // Section walk mirrors AccessBase.ReadTableDefAsync and FormatProbe.
        int colStart = db.TDef.BlockEnd + (numRealIdx * db.TDef.RealIdxEntrySz);

        // Walk column-name length-prefix block to find where it ends.
        int pos = colStart + (numCols * db.ColumnDescriptor.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (db.ReadColumnName(td, ref pos, out _) < 0)
            {
                return [];
            }
        }

        int realIdxDescStart = pos;
        IndexSectionAnchors anchors = db.IndexLayoutInfo.GetIndexSection(realIdxDescStart, numRealIdx, numIdx);

        if (anchors.LogIdxNamesStart > td.Length)
        {
            return [];
        }

        // Build a col_num → name lookup honouring deleted-column gaps.
        var colNumToName = new Dictionary<int, string>(columns.Count);
        foreach (ColumnInfo c in columns)
        {
            colNumToName[c.ColNum] = c.Name;
        }

        // Pre-walk index names so we can pair each logical-idx entry with its name.
        string[] names = new string[numIdx];
        int npos = anchors.LogIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (db.ReadColumnName(td, ref npos, out string n) < 0)
            {
                names[i] = string.Empty;
            }
            else
            {
                names[i] = n;
            }
        }

        var result = new List<IndexMetadata>(numIdx);
        for (int i = 0; i < numIdx; i++)
        {
            if (!db.IndexLayoutInfo.TryReadLogicalEntry(td, anchors.LogIdxStart, i, out LogicalIdxEntry entry))
            {
                break;
            }

            (int _, int indexNum, int realIdxNum, int relIdxNum, int relTblPage, byte cascadeUps, byte cascadeDels, IndexKind indexType) = entry;

            // Read the col_map for the backing real-idx entry to recover key columns.
            var keyColumns = new List<IndexColumnReference>();
            byte flags = 0x00;
            int firstDp = 0;
            if (numRealIdx > 0 && realIdxNum >= 0 && realIdxNum < numRealIdx
                && db.IndexLayoutInfo.TryReadRealIdxSlotWithKeyColumns(td, realIdxDescStart, realIdxNum, out RealIdxSlot slot, out List<KeyColumn>? kcs))
            {
                foreach ((int cn, bool ascending) in kcs)
                {
                    keyColumns.Add(new IndexColumnReference
                    {
                        Name = colNumToName.TryGetValue(cn, out string? n) ? n : string.Empty,
                        ColumnNumber = cn,
                        IsAscending = ascending,
                    });
                }

                flags = slot.Flags;
                if (slot.FirstDpOffset >= 0 && slot.FirstDpOffset + 4 <= td.Length)
                {
                    firstDp = Ri32(td, slot.FirstDpOffset);
                }
            }

            // Access often leaves the real-index unique flag clear on primary
            // keys; their semantic uniqueness is conveyed by index_type=0x01.
            bool hasUniqueFlag = (flags & Constants.TableDefinition.UniqueIndexFlag) != 0;

            result.Add(new IndexMetadata
            {
                Name = names[i],
                IndexNumber = indexNum,
                RealIndexNumber = realIdxNum,
                Kind = indexType,
                HasUniqueFlag = hasUniqueFlag,
                IgnoreNulls = (flags & Constants.TableDefinition.IgnoreNullsIndexFlag) != 0,
                IsRequired = (flags & Constants.TableDefinition.RequiredIndexFlag) != 0,
                IsForeignKey = relIdxNum != -1,
                RelatedTablePage = relIdxNum != -1 ? relTblPage : 0,

                // Per Jackcess IndexImpl: only bit 0x01 (CASCADE_DELETES_FLAG /
                // CASCADE_UPDATES_FLAG) signals "cascade enabled". DAO/Access stamps
                // a non-zero default (0x04 = CASCADE_SET_DEFAULT_FLAG) into these
                // bytes for every index — including PK and standalone indexes — so
                // a bare `!= 0` check would surface false positives. Mask to bit 0x01.
                CascadeUpdates = (cascadeUps & 0x01) != 0,
                CascadeDeletes = (cascadeDels & 0x01) != 0,
                Columns = keyColumns,
                FirstDp = firstDp,
            });
        }

        return result;
    }

    /// <summary>
    /// Builds the <c>ColNum → snapshot row index</c> lookup that every
    /// catalog-using path needs in order to translate a real-idx key column's
    /// <c>col_num</c> (which can outrun the snapshot index when columns have
    /// been deleted) into the matching slot in a row's value array. Equivalent
    /// to <see cref="IndexLayout.TryResolveKeyColumnInfos"/>'s expected
    /// <c>snapshotIndexByColNum</c> argument.
    /// </summary>
    /// <param name="tableColumns">The table columns.</param>
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
    /// path in <see cref="AccessWriter"/>.
    /// </summary>
    /// <param name="tdefBuffer">The TDEF buffer.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="anchors">The anchors.</param>
    /// <param name="tableColumns">The table columns.</param>
    /// <param name="logIdxNames">The log index names.</param>
    public static ResolvedIndexCatalog ReadResolved(
        byte[] tdefBuffer,
        IndexLayout layout,
        IndexSectionAnchors anchors,
        IReadOnlyList<ColumnInfo> tableColumns,
        IReadOnlyList<string>? logIdxNames = null)
    {
        IndexCatalog catalog = Read(tdefBuffer, layout, anchors, logIdxNames);
        Dictionary<int, int> snapshotIndexByColNum = BuildColumnNumberToSnapshotIndex(tableColumns);
        var keyColInfosByRealIdx = new Dictionary<int, List<KeyColumnInfo>>(catalog.RealIdxByNum.Count);
        foreach ((int realIdxNum, RealIdxEntry rie) in catalog.RealIdxByNum)
        {
            if (IndexLayout.TryResolveKeyColumnInfos(
                    rie.IndexKeyColumns,
                    tableColumns,
                    snapshotIndexByColNum,
                    out List<KeyColumnInfo>? infos))
            {
                keyColInfosByRealIdx[realIdxNum] = infos;
            }
        }

        return new ResolvedIndexCatalog(catalog, snapshotIndexByColNum, keyColInfosByRealIdx);
    }

    /// <summary>
    /// Decoded TDEF index catalog returned by <see cref="Read"/>.
    /// </summary>
    /// <param name="RealIdxByNum">Real-idx slot number → decoded entry. <see cref="RealIdxEntry.IsUnique"/> reflects the physical <c>flags &amp; 0x01</c> bit OR a PK promotion (any logical-idx with <c>index_type = 0x01</c> referencing this slot via <c>index_num2</c>).</param>
    /// <param name="PkRealIdxNums">Set of real-idx slot numbers backing a primary-key logical-idx.</param>
    /// <param name="NameByRealIdx">Best-effort logical-idx name per real-idx slot (first logical-idx referencing that slot wins). Empty when <c>logIdxNames</c> was not supplied to <see cref="Read"/>.</param>
    public sealed record IndexCatalog(
        Dictionary<int, RealIdxEntry> RealIdxByNum,
        HashSet<int> PkRealIdxNums,
        Dictionary<int, string> NameByRealIdx)
    {
        /// <summary>
        /// Returns the best-effort logical-idx name for <paramref name="realIdxNum"/>,
        /// or the synthetic <c>realidx#N</c> fallback when no logical-idx
        /// references this real-idx (or when names were not captured).
        /// </summary>
        /// <param name="realIdxNum">The real index number of.</param>
        public string GetNameOrFallback(int realIdxNum)
            => this.NameByRealIdx.TryGetValue(realIdxNum, out string? n) ? n : $"realidx#{realIdxNum}";

        /// <summary>
        /// Returns <see langword="true"/> when <paramref name="realIdxNum"/>
        /// is unique either by its physical <c>flags &amp; 0x01</c> bit or by
        /// PK promotion (any logical-idx with <c>index_type = 0x01</c>
        /// references this slot).
        /// </summary>
        /// <param name="realIdxNum">The real index number of.</param>
        public bool IsUniqueOrPk(int realIdxNum)
            => (this.RealIdxByNum.TryGetValue(realIdxNum, out RealIdxEntry rie) && rie.IsUnique)
                || this.PkRealIdxNums.Contains(realIdxNum);
    }

    /// <summary>
    /// Result of <see cref="ReadResolved"/>: bundles the decoded
    /// <see cref="IndexCatalog"/> with the <c>ColNum → snapshot index</c>
    /// lookup and the per-real-idx pre-resolved key columns. Catalog-touching
    /// paths in <see cref="AccessWriter"/> can iterate
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
        Dictionary<int, List<KeyColumnInfo>> KeyColumnInfosByRealIdx)
    {
        /// <summary>Gets the decoded real-idx slots; shortcut for <c>Catalog.RealIdxByNum</c>.</summary>
        public Dictionary<int, RealIdxEntry> RealIdxByNum => this.Catalog.RealIdxByNum;

        /// <summary>
        /// Returns the pre-resolved key columns for <paramref name="realIdxNum"/>,
        /// or <see langword="false"/> when the slot's columns could not be
        /// resolved against the table snapshot (deleted-column gap).
        /// </summary>
        /// <param name="realIdxNum">The real index number of.</param>
        /// <param name="keyColInfos">The key col infos.</param>
        public bool TryGetKeyColumnInfos(int realIdxNum, out List<KeyColumnInfo> keyColInfos)
        {
            if (this.KeyColumnInfosByRealIdx.TryGetValue(realIdxNum, out List<KeyColumnInfo>? infos))
            {
                keyColInfos = infos;
                return true;
            }

            keyColInfos = [];
            return false;
        }
    }
}
