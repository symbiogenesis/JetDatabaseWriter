namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

#pragma warning disable SA1204

/// <summary>
/// Owns physical foreign-key metadata: logical/real index descriptors, TDEF chains,
/// leaf reservations and partner links. Relationship policy and catalog lifecycle
/// remain with RelationshipManager. The caller owns the transaction and cache
/// invalidation; this editor releases unlinked reservations on failed emission.
/// </summary>
/// <param name="format">The database format and byte layouts.</param>
/// <param name="tableDefs">The bounded table-definition reader.</param>
/// <param name="pager">The caller's transactional page store.</param>
/// <param name="pageAllocator">Allocates and releases leaf and TDEF continuation pages.</param>
internal sealed class ForeignKeyMetadataEditor(
    JetFormat format,
    TableDefReader tableDefs,
    Pager pager,
    PageAllocator pageAllocator)
{
    private readonly JetFormat format = format;
    private readonly TableDefReader tableDefs = tableDefs;
    private readonly Pager pager = pager;
    private readonly PageAllocator pageAllocator = pageAllocator;

    // ════════════════════════════════════════════════════════════════
    // Per-TDEF FK logical-idx entries
    // ════════════════════════════════════════════════════════════════
    //
    // Jet4 / ACE follow the DAO baseline (§3.4): the FK entry is prepended,
    // the parent shares the first covering real index, its cascade bytes
    // stay 0, and a new real index carries the 0x80 flag. Jet3 follows the
    // only Access 97 evidence, indexTestV1997.mdb: entries and names stay in
    // case-insensitive name order, the parent shares a unique (primary-key)
    // covering real index, both sides carry the cascade bytes, and a new real
    // index has flags 0x00. used_pages stays 0 on Jet3, where the writer keeps
    // no index usage maps.

    /// <summary>
    /// Pre-computed real-idx slot information for one side of a relationship.
    /// </summary>
    /// <param name="RealIdxNum">The real index number of.</param>
    /// <param name="LogicalIdxNum">The logical index number of.</param>
    /// <param name="AllocatesNewRealIdx">The allocates new real index.</param>
    /// <param name="NewLeafPageNumber">The new leaf page number.</param>
    private readonly record struct FkSidePlan(int RealIdxNum, int LogicalIdxNum, bool AllocatesNewRealIdx, long NewLeafPageNumber)
    {
        /// <summary>
        /// RealIdxNum:           real-idx slot index used for index_num2 on this side.
        /// LogicalIdxNum:        logical-idx number written as index_num for this side.
        /// AllocatesNewRealIdx:  true when a new real-idx slot must be appended.
        /// NewLeafPageNumber:    pre-allocated empty leaf page (set when AllocatesNewRealIdx).
        /// </summary>
        /// <param name="page">The page number of the newly allocated leaf page for this side, if AllocatesNewRealIdx is true.</param>
        /// <returns>The new plan with the given leaf page.</returns>
        public FkSidePlan WithLeafPage(long page) => this with { NewLeafPageNumber = page };
    }

    /// <summary>
    /// Orchestrates the two-side per-TDEF FK index emission: pre-computes
    /// both sides' target real-idx slots (sharing where possible), allocates
    /// empty leaf pages for any newly-allocated real-idx slots, then mutates
    /// each TDEF to append its FK logical-idx entry. Either TDEF may be a
    /// multi-page chain: each side is edited as one logical buffer and
    /// rewritten over its chain, which grows by continuation pages as needed.
    /// </summary>
    /// <param name="relationship">The relationship.</param>
    /// <param name="primaryTdefPage">The primary TDEF page.</param>
    /// <param name="primaryDef">The primary def.</param>
    /// <param name="foreignTdefPage">The foreign TDEF page.</param>
    /// <param name="foreignDef">The foreign def.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask EmitFkPerTdefEntriesAsync(
        RelationshipDefinition relationship,
        long primaryTdefPage,
        TableDef primaryDef,
        long foreignTdefPage,
        TableDef foreignDef,
        CancellationToken cancellationToken)
    {
        // Resolve column numbers (deleted-column gaps mean ColNum != ordinal).
        int[] pkColNums = new int[relationship.PrimaryColumns.Count];
        int[] fkColNums = new int[relationship.ForeignColumns.Count];
        for (int i = 0; i < relationship.PrimaryColumns.Count; i++)
        {
            int pkIdx = primaryDef.FindColumnIndex(relationship.PrimaryColumns[i]);
            int fkIdx = foreignDef.FindColumnIndex(relationship.ForeignColumns[i]);
            pkColNums[i] = primaryDef.Columns[pkIdx].ColNum;
            fkColNums[i] = foreignDef.Columns[fkIdx].ColNum;
        }

        // Read both TDEF pages and decide each side's real-idx slot and new
        // logical-idx number. rel_idx_num cross-references the partner
        // logical-idx number, not the partner physical real-idx slot. On Jet3
        // the parent shares a unique covering real index, as Access 97 does.
        bool jet3 = this.format.IsJet3;
        FkSidePlan pkPlan;
        FkSidePlan fkPlan;
        List<string> pkExistingNames;
        List<string> fkExistingNames;
        if (primaryTdefPage == foreignTdefPage)
        {
            (pkPlan, fkPlan, pkExistingNames) = await this.PrepareSelfReferentialFkSidesAsync(
                primaryTdefPage,
                pkColNums,
                fkColNums,
                cancellationToken).ConfigureAwait(false);
            fkExistingNames = pkExistingNames;
        }
        else
        {
            (pkPlan, pkExistingNames) = await this.PrepareFkSideAsync(primaryTdefPage, pkColNums, preferUnique: jet3, cancellationToken).ConfigureAwait(false);
            (fkPlan, fkExistingNames) = await this.PrepareFkSideAsync(foreignTdefPage, fkColNums, preferUnique: false, cancellationToken).ConfigureAwait(false);
        }

        // Allocate empty leaf pages for any newly-allocated real-idx slots.
        // Both leaf pages are appended before any TDEF mutation so the page
        // numbers are stable for the cross-referenced first_dp values. Each
        // side's TDEF write links its leaf; a leaf whose write never happens
        // goes back to the global usage map.
        var pkRuns = new ReservedPageRuns(this.pageAllocator);
        var fkRuns = new ReservedPageRuns(this.pageAllocator);
        try
        {
            if (pkPlan.AllocatesNewRealIdx)
            {
                pkPlan = pkPlan.WithLeafPage(await this.AllocateEmptyFkLeafAsync(primaryTdefPage, pkRuns, cancellationToken).ConfigureAwait(false));
            }

            if (fkPlan.AllocatesNewRealIdx)
            {
                fkPlan = fkPlan.WithLeafPage(await this.AllocateEmptyFkLeafAsync(foreignTdefPage, fkRuns, cancellationToken).ConfigureAwait(false));
            }

            byte cascadeUpsByte = (byte)(relationship.CascadeUpdates ? 1 : 0);
            byte cascadeDelsByte = (byte)(relationship.CascadeDeletes ? 1 : 0);

            // Choose unique-within-tdef logical-idx names. DAO uses a hidden .rB/.rC
            // style logical name on the parent side and the public relationship name
            // on the child side.
            string pkName = MakeUniqueParentRelationshipLogicalName(pkExistingNames);
            string fkName = IndexHelpers.MakeUniqueLogicalIdxName(
                primaryTdefPage == foreignTdefPage ? relationship.Name + "_FK" : relationship.Name,
                fkExistingNames);

            // Emit both sides. On Jet4 / ACE the PK side carries no cascade
            // flags, matching the DAO baseline; Access 97 sets them on both.
            await this.EmitFkLogicalIdxAsync(
                primaryTdefPage,
                pkColNums,
                pkName,
                pkPlan,
                relTblTypeThisSide: Constants.TableDefinition.ParentRelationshipTableType,
                relIdxNumOtherSide: fkPlan.LogicalIdxNum,
                relTblPageOther: foreignTdefPage,
                cascadeUps: jet3 ? cascadeUpsByte : (byte)0,
                cascadeDels: jet3 ? cascadeDelsByte : (byte)0,
                pkRuns,
                cancellationToken).ConfigureAwait(false);

            await this.EmitFkLogicalIdxAsync(
                foreignTdefPage,
                fkColNums,
                fkName,
                fkPlan,
                relTblTypeThisSide: Constants.TableDefinition.ChildRelationshipTableType,
                relIdxNumOtherSide: pkPlan.LogicalIdxNum,
                relTblPageOther: primaryTdefPage,
                cascadeUps: cascadeUpsByte,
                cascadeDels: cascadeDelsByte,
                fkRuns,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await pkRuns.ReleaseAsync().ConfigureAwait(false);
            await fkRuns.ReleaseAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Allocates and writes the empty leaf a new FK real index starts with,
    /// in the database format's leaf layout, and records it in
    /// <paramref name="runs"/> until the TDEF write that links it.
    /// </summary>
    /// <param name="tdefPage">The owning table's TDEF page.</param>
    /// <param name="runs">The runs to record the leaf in.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The leaf page number.</returns>
    private async ValueTask<long> AllocateEmptyFkLeafAsync(long tdefPage, ReservedPageRuns runs, CancellationToken cancellationToken)
    {
        byte[] leaf = IndexPageCodec.BuildLeafPage(
            this.format.IndexPage,
            this.format.PageSize,
            tdefPage,
            [],
            enablePrefixCompression: false);
        long page = await this.pageAllocator.AllocatePageAsync(leaf, cancellationToken).ConfigureAwait(false);
        runs.Add(page, 1);
        return page;
    }

    /// <summary>
    /// Reads one side's TDEF page, walks the col-name and idx-name sections,
    /// detects any existing real-idx that already covers <paramref name="columnNumbers"/>
    /// (sharing per §3.3), and returns the resulting plan plus the existing
    /// logical-idx-name list (used to avoid name collisions on the new entry).
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="preferUnique">Whether to share a unique covering real index in preference to the first one (see <see cref="FindCoveringRealIdx"/>).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="NotSupportedException">Thrown when the TDEF cannot be mutated because its layout is malformed or not a TDEF.</exception>
    private async ValueTask<(FkSidePlan Plan, List<string> ExistingNames)> PrepareFkSideAsync(
        long tdefPage,
        int[] columnNumbers,
        bool preferUnique,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] page = chain.Bytes;
        if (!this.TryParseFkTDefLayout(page, out FkTDefLayout layout))
        {
            throw new NotSupportedException(
                $"TDEF at page {tdefPage} cannot be mutated in place (malformed counts or not a TDEF).");
        }

        int sharedSlot = FindCoveringRealIdx(this.format.Index, page, columnNumbers, in layout, preferUnique);
        List<string> existingNames = IndexCatalogReader.ReadLogicalIdxNames(this.format, page, layout.LogIdxNamesStart, layout.NumIdx);

        int logicalIdxNum = NextLogicalIdxNumber(this.format.Index, page, in layout);
        FkSidePlan plan = sharedSlot >= 0
            ? new FkSidePlan(sharedSlot, logicalIdxNum, false, 0)
            : new FkSidePlan(layout.NumRealIdx, logicalIdxNum, true, 0);

        return (plan, existingNames);
    }

    /// <summary>
    /// Plans both sides of a self-referential relationship from one original
    /// TDEF snapshot. When both sides need new real-idx descriptors, the
    /// second side must reserve the slot after the first side's pending slot;
    /// preparing each side independently would make both claim the same slot.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="pkColumnNumbers">The primary key column numbers.</param>
    /// <param name="fkColumnNumbers">The foreign key column numbers.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="NotSupportedException">Thrown when the TDEF cannot be mutated because its layout is malformed or not a TDEF.</exception>
    private async ValueTask<(FkSidePlan PkPlan, FkSidePlan FkPlan, List<string> ExistingNames)> PrepareSelfReferentialFkSidesAsync(
        long tdefPage,
        int[] pkColumnNumbers,
        int[] fkColumnNumbers,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] page = chain.Bytes;
        if (!this.TryParseFkTDefLayout(page, out FkTDefLayout layout))
        {
            throw new NotSupportedException(
                $"TDEF at page {tdefPage} cannot be mutated in place (malformed counts or not a TDEF).");
        }

        int pkSharedSlot = FindCoveringRealIdx(this.format.Index, page, pkColumnNumbers, in layout, preferUnique: this.format.IsJet3);
        int fkSharedSlot = FindCoveringRealIdx(this.format.Index, page, fkColumnNumbers, in layout, preferUnique: false);
        int nextRealIdxNum = layout.NumRealIdx;

        bool pkAllocates = pkSharedSlot < 0;
        int pkRealIdxNum = pkAllocates ? nextRealIdxNum++ : pkSharedSlot;

        bool fkAllocates;
        int fkRealIdxNum;
        if (fkSharedSlot >= 0)
        {
            fkAllocates = false;
            fkRealIdxNum = fkSharedSlot;
        }
        else if (pkAllocates && ColumnNumbersEqual(pkColumnNumbers, fkColumnNumbers))
        {
            fkAllocates = false;
            fkRealIdxNum = pkRealIdxNum;
        }
        else
        {
            fkAllocates = true;
            fkRealIdxNum = nextRealIdxNum;
        }

        int pkLogicalIdxNum = NextLogicalIdxNumber(this.format.Index, page, in layout);
        int fkLogicalIdxNum = pkLogicalIdxNum + 1;
        List<string> existingNames = IndexCatalogReader.ReadLogicalIdxNames(this.format, page, layout.LogIdxNamesStart, layout.NumIdx);

        return (
            new FkSidePlan(pkRealIdxNum, pkLogicalIdxNum, pkAllocates, 0),
            new FkSidePlan(fkRealIdxNum, fkLogicalIdxNum, fkAllocates, 0),
            existingNames);
    }

    /// <summary>
    /// Adds one FK logical-idx entry and its name (and optionally appends a
    /// new real-idx physical descriptor) to the TDEF chain at
    /// <paramref name="tdefPage"/>, editing the stitched logical buffer and
    /// rewriting the chain, which gains continuation pages when the addition
    /// needs them.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="indexName">The index name.</param>
    /// <param name="sidePlan">The pre-computed real/logical index plan for this side.</param>
    /// <param name="relTblTypeThisSide">The relationship table type this side.</param>
    /// <param name="relIdxNumOtherSide">The relationship index number of other side.</param>
    /// <param name="relTblPageOther">The relationship table page other.</param>
    /// <param name="cascadeUps">The cascade ups.</param>
    /// <param name="cascadeDels">The cascade dels.</param>
    /// <param name="newLeafRuns">Holds the new real index's empty leaf until the TDEF write links it.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="NotSupportedException">Thrown when the target TDEF cannot be mutated because its layout is malformed or not a TDEF.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="indexName"/> is too long for a TDEF name record (more than 255 ANSI bytes on Jet3).</exception>
    private async ValueTask EmitFkLogicalIdxAsync(
        long tdefPage,
        int[] columnNumbers,
        string indexName,
        FkSidePlan sidePlan,
        byte relTblTypeThisSide,
        int relIdxNumOtherSide,
        long relTblPageOther,
        byte cascadeUps,
        byte cascadeDels,
        ReservedPageRuns newLeafRuns,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] td = chain.Bytes;

        if (!this.TryParseFkTDefLayout(td, out FkTDefLayout layout))
        {
            throw new NotSupportedException(
                $"cannot mutate the TDEF at page {tdefPage} (malformed counts or not a TDEF).");
        }

        IndexLayout lay = this.format.Index;
        int numCols = layout.NumCols;
        int numIdx = layout.NumIdx;
        int numRealIdx = layout.NumRealIdx;
        int realIdxDescStart = layout.RealIdxDescStart;
        int logIdxStart = layout.LogIdxStart;
        int logIdxNamesStart = layout.LogIdxNamesStart;
        int logIdxNamesLen = layout.LogIdxNamesLen;
        int trailingStart = layout.TrailingStart;
        int currentEnd = layout.CurrentEnd;
        int trailingLen = layout.TrailingLen;

        // The new entry and its name go in at logical position `insertAt`;
        // entries and names before it keep their offsets.
        List<string> existingNames = IndexCatalogReader.ReadLogicalIdxNames(this.format, td, logIdxNamesStart, numIdx);
        int insertAt = this.FkEntryInsertPosition(existingNames, indexName);
        byte[] nameRecord = this.format.EncodeTDefNameRecord(indexName);
        int namesBeforeLen = 0;
        if (insertAt > 0)
        {
            if (!this.TryGetLogicalIdxNameRange(td, in layout, insertAt - 1, out int lastNameStart, out int lastNameLen))
            {
                throw new NotSupportedException(
                    $"cannot mutate the TDEF at page {tdefPage} (its index names cannot be walked).");
            }

            namesBeforeLen = lastNameStart + lastNameLen - logIdxNamesStart;
        }

        int entrySize = lay.LogicalEntrySize;
        int deltaRealIdxSkip = sidePlan.AllocatesNewRealIdx ? this.format.TDef.RealIdxEntrySz : 0;
        int deltaRealIdxPhys = sidePlan.AllocatesNewRealIdx ? lay.RealIdxPhysSize : 0;
        int totalGrowth = deltaRealIdxSkip + deltaRealIdxPhys + entrySize + nameRecord.Length;

        // Build the rewritten page.
        byte[] newTd = new byte[LogicalTDefChain.GetLogicalCapacity(this.format.PageSize, currentEnd + totalGrowth)];
        Buffer.BlockCopy(td, 0, newTd, 0, this.format.TDef.BlockEnd);

        // Real-idx skip block (existing slots, unchanged content).
        int oldRealIdxSkipLen = numRealIdx * this.format.TDef.RealIdxEntrySz;
        Buffer.BlockCopy(td, this.format.TDef.BlockEnd, newTd, this.format.TDef.BlockEnd, oldRealIdxSkipLen);
        int newRealIdxSkipEnd = this.format.TDef.BlockEnd + oldRealIdxSkipLen + deltaRealIdxSkip;

        // Column descriptors.
        int oldColStart = this.format.TDef.BlockEnd + oldRealIdxSkipLen;
        int colDescBlockLen = numCols * this.format.ColumnDescriptor.Size;
        Buffer.BlockCopy(td, oldColStart, newTd, newRealIdxSkipEnd, colDescBlockLen);

        // Column names (variable length).
        int oldColNamesStart = oldColStart + colDescBlockLen;
        int colNamesLen = realIdxDescStart - oldColNamesStart;
        int newColNamesStart = newRealIdxSkipEnd + colDescBlockLen;
        Buffer.BlockCopy(td, oldColNamesStart, newTd, newColNamesStart, colNamesLen);

        // Real-idx physical descriptors (existing slots).
        int newRealIdxDescStart = newColNamesStart + colNamesLen;
        int oldRealIdxPhysLen = numRealIdx * lay.RealIdxPhysSize;
        Buffer.BlockCopy(td, realIdxDescStart, newTd, newRealIdxDescStart, oldRealIdxPhysLen);

        // Append a new real-idx physical descriptor when allocating a new slot.
        // On Jet4/ACE it starts with the 0x00000783 leading magic, distinct
        // from the format-wide 0x00000659 cookie; DAO validates it during
        // CompactDatabase / OpenRecordset on tables with FK indexes. flags
        // carries the 0x80 bit Access sets on every Jet4 index; Access 97
        // writes 0x00 for an FK index. used_pages starts at 0;
        // MaintainIndexesAsync patches the DAO-shaped index usage-map pointer
        // after rebuilding on Jet4/ACE. Its statistics slot in the skip block
        // stays zero.
        if (sidePlan.AllocatesNewRealIdx)
        {
            lay.WriteRealIdxDescriptor(
                newTd,
                newRealIdxDescStart + oldRealIdxPhysLen,
                columnNumbers,
                this.format.IsJet3 ? (byte)0 : Constants.TableDefinition.UnknownIndexFlag,
                sidePlan.NewLeafPageNumber);
        }

        // Logical-idx entries, with the new FK entry at `insertAt`. DAO
        // prepends relationship logical entries before the existing
        // PrimaryKey entry on Jet4/ACE; CompactDatabase preserves FK tables
        // only when the entry/name ordering follows that shape. On Jet4/ACE
        // the entry starts with the 0x00000659 cookie DAO checks during
        // CompactDatabase.
        int newLogIdxStart = newRealIdxDescStart + oldRealIdxPhysLen + deltaRealIdxPhys;
        int oldLogIdxLen = numIdx * entrySize;
        int entriesBeforeLen = insertAt * entrySize;
        Buffer.BlockCopy(td, logIdxStart, newTd, newLogIdxStart, entriesBeforeLen);
        lay.WriteLogicalEntry(
            newTd,
            newLogIdxStart + entriesBeforeLen,
            sidePlan.LogicalIdxNum,
            sidePlan.RealIdxNum,
            relTblTypeThisSide,
            relIdxNumOtherSide,
            relTblPageOther,
            cascadeUps,
            cascadeDels,
            IndexKind.ForeignKey);
        Buffer.BlockCopy(
            td,
            logIdxStart + entriesBeforeLen,
            newTd,
            newLogIdxStart + entriesBeforeLen + entrySize,
            oldLogIdxLen - entriesBeforeLen);

        // Logical-idx names follow the same order as their entries.
        int newNamesStart = newLogIdxStart + oldLogIdxLen + entrySize;
        Buffer.BlockCopy(td, logIdxNamesStart, newTd, newNamesStart, namesBeforeLen);
        Buffer.BlockCopy(nameRecord, 0, newTd, newNamesStart + namesBeforeLen, nameRecord.Length);
        Buffer.BlockCopy(
            td,
            logIdxNamesStart + namesBeforeLen,
            newTd,
            newNamesStart + namesBeforeLen + nameRecord.Length,
            logIdxNamesLen - namesBeforeLen);

        // Trailing variable-length-column block (Access-emitted TDEFs only).
        int newTrailingStart = newNamesStart + logIdxNamesLen + nameRecord.Length;
        if (trailingLen > 0)
        {
            Buffer.BlockCopy(td, trailingStart, newTd, newTrailingStart, trailingLen);
        }

        // Update header counts.
        Wi32(newTd, this.format.TDef.NumIdx, numIdx + 1);
        if (sidePlan.AllocatesNewRealIdx)
        {
            Wi32(newTd, this.format.TDef.NumRealIdx, numRealIdx + 1);
        }

        // tdef_len at offset 8 = (newEnd - 8). The page header (8 bytes) is
        // not counted in tdef_len, matching BuildTDefPageWithIndexOffsets.
        Wi32(newTd, 8, newTrailingStart + trailingLen - 8);

        // This write links the new real index's leaf.
        newLeafRuns.MarkLinked();
        await this.WriteLogicalTDefChainAsync(
            chain,
            newTd,
            newTrailingStart + trailingLen,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the byte length of the existing logical-idx-name section, or
    /// -1 if the walk fails.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="logIdxNamesStart">The log index names start.</param>
    /// <param name="numIdx">The number of index.</param>
    private int MeasureLogicalIdxNamesLength(byte[] td, int logIdxNamesStart, int numIdx)
    {
        int pos = logIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (this.format.ReadColumnName(td, ref pos, out _) < 0)
            {
                return -1;
            }
        }

        return pos - logIdxNamesStart;
    }

    /// <summary>
    /// Returns the logical position at which a new FK entry named
    /// <paramref name="indexName"/> goes. On Jet4 / ACE it is 0: DAO prepends
    /// relationship entries before the existing <c>PrimaryKey</c> entry, and
    /// CompactDatabase preserves FK tables only when the entry and name order
    /// follows that shape. On Jet3 it is before the first existing entry whose
    /// name sorts after <paramref name="indexName"/>, ignoring case, as in the
    /// TDEFs Access 97 wrote (<c>.rC</c>, <c>id</c>, <c>PrimaryKey</c>,
    /// <c>Table2Table1</c>); the existing entries are not re-sorted.
    /// </summary>
    /// <param name="existingNames">The TDEF's logical-index names, in entry order.</param>
    /// <param name="indexName">The new entry's name.</param>
    /// <returns>The position, from 0 to <c>existingNames.Count</c>.</returns>
    private int FkEntryInsertPosition(List<string> existingNames, string indexName)
    {
        if (!this.format.IsJet3)
        {
            return 0;
        }

        for (int i = 0; i < existingNames.Count; i++)
        {
            if (StringComparer.OrdinalIgnoreCase.Compare(existingNames[i], indexName) > 0)
            {
                return i;
            }
        }

        return existingNames.Count;
    }

    private static string MakeUniqueParentRelationshipLogicalName(IReadOnlyList<string> existing)
    {
        var taken = new HashSet<string>(existing, StringComparer.OrdinalIgnoreCase);
        for (char suffix = 'B'; suffix <= 'Z'; suffix++)
        {
            string candidate = ".r" + suffix;
            if (!taken.Contains(candidate))
            {
                return candidate;
            }
        }

        for (int i = 1; i < int.MaxValue; i++)
        {
            string candidate = ".r" + i.ToString(CultureInfo.InvariantCulture);
            if (!taken.Contains(candidate))
            {
                return candidate;
            }
        }

        return ".r";
    }

    /// <summary>
    /// Real-idx sharing per §3.3: returns the existing real-idx slot whose col_map
    /// matches <paramref name="columnNumbers"/> exactly (in declaration
    /// order); -1 when no covering real-idx exists. The col_map is fixed at
    /// 10 slots × {col_num(2), col_order(1)} on every format. With
    /// <paramref name="preferUnique"/>, a covering slot flagged unique or
    /// backing the primary-key logical index wins over an earlier non-unique
    /// one: Access 97 shared Table2's <c>PrimaryKey</c> real index, not its
    /// <c>id</c> index on the same column, for the parent side of
    /// 'Table2Table1' in indexTestV1997.mdb.
    /// </summary>
    /// <param name="lay">The format's TDEF index layout.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="layout">The parsed TDEF layout.</param>
    /// <param name="preferUnique">Whether a unique covering slot wins over the first covering slot.</param>
    private static int FindCoveringRealIdx(IndexLayout lay, byte[] td, int[] columnNumbers, in FkTDefLayout layout, bool preferUnique)
    {
        int first = -1;
        for (int ri = 0; ri < layout.NumRealIdx; ri++)
        {
            int phys = lay.RealIdxPhysOffset(layout.RealIdxDescStart, ri);
            if (!IndexHelpers.RealIdxColMapMatches(lay, td, phys, columnNumbers))
            {
                continue;
            }

            if (!preferUnique)
            {
                return ri;
            }

            if ((td[lay.FlagsAbsoluteOffset(phys)] & Constants.TableDefinition.UniqueIndexFlag) != 0
                || BacksPrimaryKey(lay, td, in layout, ri))
            {
                return ri;
            }

            if (first < 0)
            {
                first = ri;
            }
        }

        return first;
    }

    private static bool BacksPrimaryKey(IndexLayout lay, byte[] td, in FkTDefLayout layout, int realIdxNum)
    {
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int f = lay.LogicalIdxFieldsOffset(layout.LogIdxStart, li);
            if (td[f + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset] == (byte)IndexKind.PrimaryKey
                && Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset) == realIdxNum)
            {
                return true;
            }
        }

        return false;
    }

    private static bool ColumnNumbersEqual(int[] left, int[] right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        for (int i = 0; i < left.Length; i++)
        {
            if (left[i] != right[i])
            {
                return false;
            }
        }

        return true;
    }

    private static int NextLogicalIdxNumber(IndexLayout lay, byte[] td, in FkTDefLayout layout)
    {
        int max = -1;
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int indexNum = Ri32(td, lay.LogicalIdxFieldsOffset(layout.LogIdxStart, li) + Constants.TableDefinition.Jet3.LogicalIdx.IndexNumOffset);
            if (indexNum > max)
            {
                max = indexNum;
            }
        }

        return max + 1;
    }

    /// <summary>
    /// Re-emits the FK logical-idx entries captured in <paramref name="state"/>
    /// on <paramref name="targetTdefPage"/>, the rebuilt copy of the table, with
    /// their names, sides and cascade flags. Each entry gets a new logical-idx
    /// number; an entry that points back at the same table (a self-referencing
    /// relationship) is pointed at <paramref name="finalTdefPage"/> and its
    /// partner's new number. Partner tables are not touched here; see
    /// <see cref="RelationshipManager.CompleteRewriteAsync"/>. The new FK leaves are empty until the
    /// caller rebuilds the copy's indexes.
    /// </summary>
    /// <param name="state">The state captured before the rewrite.</param>
    /// <param name="targetTdefPage">The TDEF page of the rebuilt copy.</param>
    /// <param name="targetDef">The rebuilt copy's table definition.</param>
    /// <param name="finalTdefPage">The TDEF page the table occupies once the copy has replaced it.</param>
    /// <param name="mapColumnName">Maps each captured column name to its name in the copy.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The map from each entry's old <c>index_num</c> to its new one.</returns>
    /// <exception cref="InvalidOperationException">Thrown when a key column is missing from the copy.</exception>
    /// <exception cref="NotSupportedException">Thrown when the copy's TDEF cannot be mutated because its layout is malformed or not a TDEF.</exception>
    internal async ValueTask<IReadOnlyDictionary<int, int>> EmitFkEntriesForRewriteAsync(
        RelationshipRewriteState state,
        long targetTdefPage,
        TableDef targetDef,
        long finalTdefPage,
        Func<string, string?> mapColumnName,
        CancellationToken cancellationToken)
    {
        var newIndexNumbers = new Dictionary<int, int>();
        if (state.FkEntries.Count == 0)
        {
            return newIndexNumbers;
        }

        int[][] columnNumbers = new int[state.FkEntries.Count][];
        for (int i = 0; i < state.FkEntries.Count; i++)
        {
            FkLogicalIndexSnapshot entry = state.FkEntries[i];
            columnNumbers[i] = new int[entry.ColumnNames.Count];
            for (int k = 0; k < entry.ColumnNames.Count; k++)
            {
                string? mapped = mapColumnName(entry.ColumnNames[k]);
                int columnIndex = mapped is null ? -1 : targetDef.FindColumnIndex(mapped);
                if (columnIndex < 0)
                {
                    throw new InvalidOperationException(
                        $"Column '{entry.ColumnNames[k]}' of foreign-key index '{entry.Name}' is missing from the rebuilt table '{state.TableName}'.");
                }

                columnNumbers[i][k] = targetDef.Columns[columnIndex].ColNum;
            }
        }

        // On Jet4/ACE each emitted entry is prepended to the logical-idx list,
        // so emitting last-to-first keeps the original entry order; on Jet3
        // each goes in at its name's sorted position. Each one also takes the
        // next free logical-idx number, which lets a self-referencing pair
        // learn its partner's number before either side is written.
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(targetTdefPage, cancellationToken).ConfigureAwait(false);
        if (!this.TryParseFkTDefLayout(chain.Bytes, out FkTDefLayout layout))
        {
            throw new NotSupportedException(
                $"TDEF at page {targetTdefPage} cannot be mutated in place (malformed counts or not a TDEF).");
        }

        int nextIndexNumber = NextLogicalIdxNumber(this.format.Index, chain.Bytes, in layout);
        for (int i = state.FkEntries.Count - 1; i >= 0; i--)
        {
            newIndexNumbers[state.FkEntries[i].IndexNumber] = nextIndexNumber++;
        }

        for (int i = state.FkEntries.Count - 1; i >= 0; i--)
        {
            FkLogicalIndexSnapshot entry = state.FkEntries[i];
            bool selfReferencing = entry.RelTblPage == state.TDefPage;
            int relIdxNum = selfReferencing && newIndexNumbers.TryGetValue(entry.RelIdxNum, out int partnerNumber)
                ? partnerNumber
                : entry.RelIdxNum;

            // A parent-side entry on Jet3 shares a unique covering real index,
            // as in CreateRelationshipAsync.
            bool preferUnique = this.format.IsJet3
                && entry.RelTblType == Constants.TableDefinition.ParentRelationshipTableType;
            (FkSidePlan plan, List<string> existingNames) = await this.PrepareFkSideAsync(targetTdefPage, columnNumbers[i], preferUnique, cancellationToken).ConfigureAwait(false);
            plan = plan with { LogicalIdxNum = newIndexNumbers[entry.IndexNumber] };
            var runs = new ReservedPageRuns(this.pageAllocator);
            try
            {
                if (plan.AllocatesNewRealIdx)
                {
                    plan = plan.WithLeafPage(await this.AllocateEmptyFkLeafAsync(targetTdefPage, runs, cancellationToken).ConfigureAwait(false));
                }

                await this.EmitFkLogicalIdxAsync(
                    targetTdefPage,
                    columnNumbers[i],
                    IndexHelpers.MakeUniqueLogicalIdxName(entry.Name, existingNames),
                    plan,
                    relTblTypeThisSide: entry.RelTblType,
                    relIdxNumOtherSide: relIdxNum,
                    relTblPageOther: selfReferencing ? finalTdefPage : entry.RelTblPage,
                    cascadeUps: entry.CascadeUps,
                    cascadeDels: entry.CascadeDels,
                    runs,
                    cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await runs.ReleaseAsync().ConfigureAwait(false);
                throw;
            }
        }

        return newIndexNumbers;
    }

    /// <summary>
    /// Reads every FK logical-idx entry (<c>index_type = 0x02</c>) on the TDEF at
    /// <paramref name="tdefPage"/>. Entries whose key columns cannot be resolved
    /// against <paramref name="tableDef"/> are skipped.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table definition, used to name the key columns.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<FkLogicalIndexSnapshot>> ReadFkLogicalIndexesAsync(
        long tdefPage,
        TableDef tableDef,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] td = chain.Bytes;
        if (!this.TryParseFkTDefLayout(td, out FkTDefLayout layout))
        {
            return [];
        }

        var columnNames = new Dictionary<int, string>(tableDef.Columns.Count);
        foreach (ColumnInfo column in tableDef.Columns)
        {
            columnNames[column.ColNum] = column.Name;
        }

        List<string> names = IndexCatalogReader.ReadLogicalIdxNames(this.format, td, layout.LogIdxNamesStart, layout.NumIdx);
        var result = new List<FkLogicalIndexSnapshot>();
        for (int li = 0; li < layout.NumIdx && li < names.Count; li++)
        {
            int f = this.format.Index.LogicalIdxFieldsOffset(layout.LogIdxStart, li);
            if (td[f + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset] != (byte)IndexKind.ForeignKey)
            {
                continue;
            }

            int realIdxNum = Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset);
            if (realIdxNum < 0 || realIdxNum >= layout.NumRealIdx
                || !this.format.Index.TryReadRealIdxSlotWithKeyColumns(td, layout.RealIdxDescStart, realIdxNum, out _, out List<KeyColumn> keyColumns)
                || keyColumns.Count == 0)
            {
                continue;
            }

            var keyColumnNames = new List<string>(keyColumns.Count);
            foreach (KeyColumn keyColumn in keyColumns)
            {
                if (!columnNames.TryGetValue(keyColumn.ColNum, out string? columnName))
                {
                    break;
                }

                keyColumnNames.Add(columnName);
            }

            if (keyColumnNames.Count != keyColumns.Count)
            {
                continue;
            }

            result.Add(new FkLogicalIndexSnapshot(
                names[li],
                Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.IndexNumOffset),
                keyColumnNames,
                td[f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblTypeOffset],
                Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset),
                Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset),
                td[f + Constants.TableDefinition.Jet3.LogicalIdx.CascadeUpsOffset],
                td[f + Constants.TableDefinition.Jet3.LogicalIdx.CascadeDelsOffset]));
        }

        return result;
    }

    /// <summary>
    /// Returns whether the FK logical-idx entry numbered
    /// <paramref name="indexNumber"/> on <paramref name="tdefPage"/> has a
    /// partner that links back: another FK entry, numbered
    /// <paramref name="partnerIndexNumber"/> on the TDEF at
    /// <paramref name="partnerTdefPage"/>, whose <c>rel_tbl_page</c> is
    /// <paramref name="tdefPage"/> and whose <c>rel_idx_num</c> is
    /// <paramref name="indexNumber"/>, as on both sides of every writer-created
    /// relationship and every relationship in the Access fixtures. A partner
    /// with <c>rel_idx_num = -1</c> also links back when its table page matches;
    /// the rewrite restores its missing entry number. For a
    /// self-referencing entry both pages are the same TDEF, and the partner
    /// must be a different entry.
    /// Returns <see langword="false"/> for a dangling entry: its partner page
    /// is out of range, freed, holds no parseable TDEF or holds a table
    /// without that entry, the partner names a different entry, or the entry
    /// names itself.
    /// </summary>
    /// <param name="partnerTdefPage">The page the entry names (<c>rel_tbl_page</c>).</param>
    /// <param name="partnerIndexNumber">The partner entry the entry names (<c>rel_idx_num</c>).</param>
    /// <param name="tdefPage">The TDEF page that holds the entry.</param>
    /// <param name="indexNumber">The entry's own <c>index_num</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>Whether the partner entry exists and points back at the entry.</returns>
    internal async ValueTask<bool> PartnerLinksBackAsync(
        long partnerTdefPage,
        int partnerIndexNumber,
        long tdefPage,
        int indexNumber,
        CancellationToken cancellationToken)
    {
        if (!this.IsTDefPageCandidate(partnerTdefPage)
            || (partnerTdefPage == tdefPage && partnerIndexNumber == indexNumber))
        {
            return false;
        }

        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
            partnerTdefPage,
            this.format.PageSize,
            this.pager.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: false,
            cancellationToken,
            this.tableDefs.MaxLogicalBytes).ConfigureAwait(false);
        if (chain is null || !this.TryParseFkTDefLayout(chain.Bytes, out FkTDefLayout layout))
        {
            return false;
        }

        byte[] td = chain.Bytes;
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int f = this.format.Index.LogicalIdxFieldsOffset(layout.LogIdxStart, li);
            if (td[f + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset] == (byte)IndexKind.ForeignKey
                && Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.IndexNumOffset) == partnerIndexNumber
                && Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset) == tdefPage
                && (Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset) == indexNumber
                    || Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset) == -1))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// On the partner TDEF at <paramref name="partnerTdefPage"/>, finds the FK
    /// logical-idx entry numbered <paramref name="partnerIndexNumber"/> that
    /// points at <paramref name="oldTdefPage"/>. Points it at
    /// <paramref name="newTdefPage"/> and <paramref name="newRelIdxNum"/>, or
    /// removes it (with its name) when <paramref name="newRelIdxNum"/> is
    /// <see langword="null"/> because the rewritten side has no matching entry.
    /// Does nothing when the partner TDEF or entry is not found.
    /// </summary>
    /// <param name="partnerTdefPage">The partner table's TDEF page.</param>
    /// <param name="partnerIndexNumber">The partner entry's <c>index_num</c>.</param>
    /// <param name="oldTdefPage">The rewritten table's former TDEF page.</param>
    /// <param name="newTdefPage">The rewritten table's TDEF page.</param>
    /// <param name="newRelIdxNum">The re-emitted entry's logical-idx number, or <see langword="null"/> to remove the partner entry.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask UpdatePartnerFkEntryAsync(
        long partnerTdefPage,
        int partnerIndexNumber,
        long oldTdefPage,
        long newTdefPage,
        int? newRelIdxNum,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
            partnerTdefPage,
            this.format.PageSize,
            this.pager.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: true,
            cancellationToken,
            this.tableDefs.MaxLogicalBytes).ConfigureAwait(false);
        if (chain is null || !this.TryParseFkTDefLayout(chain.Bytes, out FkTDefLayout layout))
        {
            return;
        }

        byte[] td = chain.Bytes;
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int f = this.format.Index.LogicalIdxFieldsOffset(layout.LogIdxStart, li);
            if (td[f + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset] != (byte)IndexKind.ForeignKey
                || Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.IndexNumOffset) != partnerIndexNumber
                || Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset) != oldTdefPage)
            {
                continue;
            }

            if (newRelIdxNum is int relIdxNum)
            {
                Wi32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset, checked((int)newTdefPage));
                Wi32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset, relIdxNum);
                await this.WriteLogicalTDefChainAsync(chain, td, layout.CurrentEnd, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _ = await this.RemoveLogicalIdxEntryAsync(chain, layout, li, cancellationToken).ConfigureAwait(false);
            }

            return;
        }
    }

    /// <summary>
    /// Removes every FK logical-idx entry on the TDEF at
    /// <paramref name="partnerTdefPage"/> whose <c>rel_tbl_page</c> is
    /// <paramref name="targetTdefPage"/>, one at a time, then reclaims the
    /// trailing real-idx slots left unreferenced. Stops early when the TDEF
    /// cannot be read or its name section cannot be walked.
    /// </summary>
    /// <param name="partnerTdefPage">The TDEF page to edit.</param>
    /// <param name="targetTdefPage">The TDEF page the removed entries name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask RemoveFkEntriesNamingAsync(long partnerTdefPage, long targetTdefPage, CancellationToken cancellationToken)
    {
        bool removed = false;
        while (true)
        {
            LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
                partnerTdefPage,
                this.format.PageSize,
                this.pager.ReadPageAsync,
                PageBuffers.Return,
                retainPageNumbers: true,
                cancellationToken,
                this.tableDefs.MaxLogicalBytes).ConfigureAwait(false);
            if (chain is null || !this.TryParseFkTDefLayout(chain.Bytes, out FkTDefLayout layout))
            {
                break;
            }

            int entryIndex = FindFkLogicalIdxEntryNaming(this.format.Index, chain.Bytes, in layout, targetTdefPage);
            if (entryIndex < 0 || !await this.RemoveLogicalIdxEntryAsync(chain, layout, entryIndex, cancellationToken).ConfigureAwait(false))
            {
                break;
            }

            removed = true;
        }

        if (removed)
        {
            await this.TryReclaimTrailingRealIdxAsync(partnerTdefPage, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns the position of the first FK logical-idx entry whose
    /// <c>rel_tbl_page</c> is <paramref name="relTblPage"/>, or <c>-1</c>.
    /// </summary>
    /// <param name="lay">The format's TDEF index layout.</param>
    /// <param name="td">The stitched TDEF bytes.</param>
    /// <param name="layout">The parsed layout of <paramref name="td"/>.</param>
    /// <param name="relTblPage">The partner TDEF page to look for.</param>
    private static int FindFkLogicalIdxEntryNaming(IndexLayout lay, byte[] td, in FkTDefLayout layout, long relTblPage)
    {
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int f = lay.LogicalIdxFieldsOffset(layout.LogIdxStart, li);
            if (td[f + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset] == (byte)IndexKind.ForeignKey
                && Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset) == relTblPage)
            {
                return li;
            }
        }

        return -1;
    }

    /// <summary>
    /// Returns whether <paramref name="page"/> can be a user table's TDEF
    /// page: past the header (page 0), the global usage map (page 1) and
    /// <c>MSysObjects</c> (page 2), and inside the file, journal-appended pages
    /// included.
    /// </summary>
    /// <param name="page">The page number read from a <c>rel_tbl_page</c> field.</param>
    internal bool IsTDefPageCandidate(long page)
        => page > 2 && page < this.pager.PageCount;

    /// <summary>
    /// Reads the existing logical-idx names from the TDEF at
    /// <paramref name="tdefPage"/> and returns
    /// <paramref name="baseName"/> if it is unique, otherwise a
    /// <c>baseName_N</c> variant. Same algorithm as
    /// <see cref="IndexHelpers.MakeUniqueLogicalIdxName"/>; this overload reads the TDEF
    /// for callers that have only the page number.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="baseName">The base name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<string> PickUniqueLogicalIdxNameAsync(
        long tdefPage,
        string baseName,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] pageBytes = chain.Bytes;
        if (!this.TryParseFkTDefLayout(pageBytes, out FkTDefLayout layout) || layout.NumIdx <= 0)
        {
            return IndexHelpers.MakeUniqueLogicalIdxName(baseName, []);
        }

        List<string> existing = IndexCatalogReader.ReadLogicalIdxNames(this.format, pageBytes, layout.LogIdxNamesStart, layout.NumIdx);
        return IndexHelpers.MakeUniqueLogicalIdxName(baseName, existing);
    }

    /// <summary>
    /// Locates and removes the FK logical-idx entry on <paramref name="tdefPage"/>
    /// whose backing real-idx col_map exactly covers <paramref name="columnNumbers"/>
    /// (in declaration order) AND whose <c>rel_tbl_page</c> equals
    /// <paramref name="otherTdefPage"/>. Returns the real-idx slot number that
    /// the removed FK entry referenced (so the caller can attempt
    /// <see cref="TryReclaimTrailingRealIdxAsync"/>), or <c>-1</c> when no
    /// matching entry exists (already removed, never created, or an
    /// out-of-band catalog).
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="otherTdefPage">The other TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<int> TryRemoveFkLogicalIdxEntryAsync(
        long tdefPage,
        int[] columnNumbers,
        long otherTdefPage,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] td = chain.Bytes;
        if (!this.TryParseFkTDefLayout(td, out FkTDefLayout layout) || layout.NumIdx <= 0 || layout.NumRealIdx <= 0)
        {
            return -1;
        }

        // Locate the matching logical-idx entry, then walk the names list to
        // the same index to find its variable-length name record.
        int matchEntryIdx = FindFkLogicalIdxEntry(this.format.Index, td, in layout, columnNumbers, otherTdefPage, out int releasedRealIdxNum);
        if (matchEntryIdx < 0)
        {
            return -1;
        }

        return await this.RemoveLogicalIdxEntryAsync(chain, layout, matchEntryIdx, cancellationToken).ConfigureAwait(false)
            ? releasedRealIdxNum
            : -1;
    }

    /// <summary>
    /// Removes the <paramref name="entryIndex"/>-th logical-idx entry and its
    /// name record from the TDEF held in <paramref name="chain"/>, decrements
    /// <c>num_idx</c>, and writes the chain back. The backing real-idx slot is
    /// left in place. Works on every format's entry size. Returns
    /// <see langword="false"/> when the name section cannot be walked.
    /// </summary>
    /// <param name="chain">The TDEF chain read from disk.</param>
    /// <param name="layout">The parsed layout of <paramref name="chain"/>.</param>
    /// <param name="entryIndex">The zero-based position of the entry to remove.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> RemoveLogicalIdxEntryAsync(
        LogicalTDefChain chain,
        FkTDefLayout layout,
        int entryIndex,
        CancellationToken cancellationToken)
    {
        byte[] td = chain.Bytes;
        if (!this.TryGetLogicalIdxNameRange(td, in layout, entryIndex, out int removedNameStart, out int removedNameLen))
        {
            return false;
        }

        // Mutate `td` in place via two left-shifts (Buffer.BlockCopy supports
        // overlapping regions). Step 1 collapses the logical-idx entry; step 2
        // collapses the variable-length name. The trailing variable-length-
        // column block rides along with the second shift.
        int entrySize = this.format.Index.LogicalEntrySize;
        int removedEntryStart = this.format.Index.LogicalIdxEntryOffset(layout.LogIdxStart, entryIndex);
        int afterEntry = removedEntryStart + entrySize;

        // Step 1 — drop the logical-idx entry.
        Buffer.BlockCopy(td, afterEntry, td, removedEntryStart, layout.CurrentEnd - afterEntry);
        int shiftedNameStart = removedNameStart - entrySize;
        int afterName = shiftedNameStart + removedNameLen;
        int endAfterStep1 = layout.CurrentEnd - entrySize;

        // Step 2 — drop the name record.
        Buffer.BlockCopy(td, afterName, td, shiftedNameStart, endAfterStep1 - afterName);
        int finalEnd = endAfterStep1 - removedNameLen;

        // Zero the freed tail so the on-disk page matches the prior
        // fresh-buffer behavior (bytes past the new end are padding).
        Array.Clear(td, finalEnd, layout.CurrentEnd - finalEnd);

        // Update header counts.
        Wi32(td, this.format.TDef.NumIdx, layout.NumIdx - 1);
        Wi32(td, 8, finalEnd - 8);

        await this.WriteLogicalTDefChainAsync(chain, td, finalEnd, cancellationToken).ConfigureAwait(false);
        return true;
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
    /// block (<c>num_real_idx × RealIdxEntrySz</c> bytes immediately after
    /// the TDEF block: 12 bytes each on Jet4 / ACE, 8 on Jet3) and the
    /// trailing physical descriptor (52 or 39 bytes), decrements
    /// <c>num_real_idx</c>, and updates <c>tdef_len</c>.
    /// </para>
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask TryReclaimTrailingRealIdxAsync(
        long tdefPage,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] td = chain.Bytes;
        if (!this.TryParseFkTDefLayout(td, out FkTDefLayout layout) || layout.NumRealIdx <= 0)
        {
            return;
        }

        // Build the set of real-idx slots that are still referenced by some
        // logical-idx entry. A logical-idx points at one real-idx via
        // index_num2.
        IndexLayout lay = this.format.Index;
        bool[] referenced = new bool[layout.NumRealIdx];
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int realIdxNum = Ri32(td, lay.LogicalIdxFieldsOffset(layout.LogIdxStart, li) + Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset);
            if (realIdxNum >= 0 && realIdxNum < layout.NumRealIdx)
            {
                referenced[realIdxNum] = true;
            }
        }

        // Count contiguous trailing unreferenced slots.
        int reclaim = 0;
        for (int ri = layout.NumRealIdx - 1; ri >= 0 && !referenced[ri]; ri--)
        {
            reclaim++;
        }

        if (reclaim == 0)
        {
            return;
        }

        // Step 1 — drop the trailing N entries (12 bytes each on Jet4, 8 on
        // Jet3) from the leading real-idx skip block. The skip block lives at
        // [TDef.BlockEnd, TDef.BlockEnd + numRealIdx * TDef.RealIdxEntrySz).
        // We collapse out the LAST N × RealIdxEntrySz bytes of that block by
        // left-shifting everything that follows.
        int oldSkipEnd = this.format.TDef.BlockEnd + (layout.NumRealIdx * this.format.TDef.RealIdxEntrySz);
        int newSkipEnd = oldSkipEnd - (reclaim * this.format.TDef.RealIdxEntrySz);
        Buffer.BlockCopy(td, oldSkipEnd, td, newSkipEnd, layout.CurrentEnd - oldSkipEnd);
        int endAfterStep1 = layout.CurrentEnd - (reclaim * this.format.TDef.RealIdxEntrySz);

        // After step 1 the real-idx physical descriptor section starts at
        // (realIdxDescStart - reclaim * RealIdxEntrySz). We need to drop the
        // trailing N physical descriptors (52 bytes each on Jet4/ACE, 39 on
        // Jet3). Compute the new boundaries.
        int newRealIdxDescStart = layout.RealIdxDescStart - (reclaim * this.format.TDef.RealIdxEntrySz);
        int newPhysEnd = lay.RealIdxPhysOffset(newRealIdxDescStart, layout.NumRealIdx - reclaim);
        int oldPhysEnd = lay.RealIdxPhysOffset(newRealIdxDescStart, layout.NumRealIdx);

        // Step 2 — drop the trailing N physical descriptors by left-shifting
        // the logical-idx entries + names + variable-col block.
        Buffer.BlockCopy(td, oldPhysEnd, td, newPhysEnd, endAfterStep1 - oldPhysEnd);
        int finalEnd = endAfterStep1 - (reclaim * lay.RealIdxPhysSize);

        // Zero the freed tail so the on-disk page matches the prior
        // fresh-buffer behavior (bytes past the new end are padding).
        Array.Clear(td, finalEnd, layout.CurrentEnd - finalEnd);

        // Update header counts.
        Wi32(td, this.format.TDef.NumRealIdx, layout.NumRealIdx - reclaim);
        Wi32(td, 8, finalEnd - 8);

        await this.WriteLogicalTDefChainAsync(chain, td, finalEnd, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Renames the FK logical-idx "name cookie" on <paramref name="tdefPage"/>
    /// for the entry whose backing real-idx col_map exactly covers
    /// <paramref name="columnNumbers"/> AND whose <c>rel_tbl_page</c> equals
    /// <paramref name="otherTdefPage"/>. Returns <see langword="true"/> when
    /// an entry was found and renamed; <see langword="false"/> otherwise
    /// (already renamed, never created, or an out-of-band catalog).
    /// Variable-length name records: shrink/grow is handled by shifting the
    /// trailing variable-column block; growth can spill into a continuation page
    /// through the logical TDEF-chain writer. On Jet3 the entry then moves to
    /// its new name's sorted position.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="otherTdefPage">The other TDEF page.</param>
    /// <param name="newName">The new name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<bool> TryRenameFkLogicalIdxNameAsync(
        long tdefPage,
        int[] columnNumbers,
        long otherTdefPage,
        string newName,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] td = chain.Bytes;
        if (!this.TryParseFkTDefLayout(td, out FkTDefLayout layout) || layout.NumIdx <= 0 || layout.NumRealIdx <= 0)
        {
            return false;
        }

        int matchEntryIdx = FindFkLogicalIdxEntry(this.format.Index, td, in layout, columnNumbers, otherTdefPage, out _);
        if (matchEntryIdx < 0)
        {
            return false;
        }

        if (!this.TryGetLogicalIdxNameRange(td, in layout, matchEntryIdx, out int oldNameStart, out int oldNameLen))
        {
            return false;
        }

        if (layout.CurrentEnd > td.Length)
        {
            return false;
        }

        byte[] newNameRecord = this.format.EncodeTDefNameRecord(newName);
        int delta = newNameRecord.Length - oldNameLen;

        int finalEnd = layout.CurrentEnd + delta;
        if (finalEnd < layout.TrailingStart)
        {
            return false;
        }

        td = chain.EnsureCapacity(finalEnd);

        // Shift the bytes between (oldNameStart + oldNameLen) and currentEnd
        // by delta. This covers the rest of the names section + the variable
        // -column trailing block in one move. Buffer.BlockCopy handles
        // overlapping regions.
        int afterOldName = oldNameStart + oldNameLen;
        int tailLen = layout.CurrentEnd - afterOldName;
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
        Buffer.BlockCopy(newNameRecord, 0, td, oldNameStart, newNameRecord.Length);

        // Jet3 keeps entries and names in name order, so the renamed entry
        // moves to its new name's position.
        if (this.format.IsJet3
            && !this.TryMoveLogicalIdxEntryToNameOrder(td, in layout, matchEntryIdx))
        {
            return false;
        }

        // Update tdef_len.
        Wi32(td, 8, finalEnd - 8);

        await this.WriteLogicalTDefChainAsync(chain, td, finalEnd, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Moves the logical-idx entry at <paramref name="entryIndex"/>, and its
    /// name record, to the position <see cref="FkEntryInsertPosition"/> gives
    /// its name among the others, shifting the entries and names between. The
    /// entry keeps its <c>index_num</c>, which is what partner entries name,
    /// and the sections keep their sizes. Returns <see langword="false"/>
    /// when the name section cannot be walked.
    /// </summary>
    /// <param name="td">The logical TDEF bytes, edited in place.</param>
    /// <param name="layout">The parsed layout; the name records may have changed length since.</param>
    /// <param name="entryIndex">The entry to move.</param>
    private bool TryMoveLogicalIdxEntryToNameOrder(byte[] td, in FkTDefLayout layout, int entryIndex)
    {
        IndexLayout lay = this.format.Index;
        int entrySize = lay.LogicalEntrySize;
        var entries = new List<byte[]>(layout.NumIdx);
        var nameRecords = new List<byte[]>(layout.NumIdx);
        var names = new List<string>(layout.NumIdx);
        int pos = layout.LogIdxNamesStart;
        for (int i = 0; i < layout.NumIdx; i++)
        {
            entries.Add(td.AsSpan(lay.LogicalIdxEntryOffset(layout.LogIdxStart, i), entrySize).ToArray());
            int start = pos;
            if (this.format.ReadColumnName(td, ref pos, out string name) < 0)
            {
                return false;
            }

            nameRecords.Add(td.AsSpan(start, pos - start).ToArray());
            names.Add(name);
        }

        byte[] entry = entries[entryIndex];
        byte[] nameRecord = nameRecords[entryIndex];
        string movedName = names[entryIndex];
        entries.RemoveAt(entryIndex);
        nameRecords.RemoveAt(entryIndex);
        names.RemoveAt(entryIndex);
        int target = this.FkEntryInsertPosition(names, movedName);
        entries.Insert(target, entry);
        nameRecords.Insert(target, nameRecord);

        int write = layout.LogIdxStart;
        foreach (byte[] bytes in entries)
        {
            Buffer.BlockCopy(bytes, 0, td, write, bytes.Length);
            write += bytes.Length;
        }

        write = layout.LogIdxNamesStart;
        foreach (byte[] bytes in nameRecords)
        {
            Buffer.BlockCopy(bytes, 0, td, write, bytes.Length);
            write += bytes.Length;
        }

        return true;
    }

    private ValueTask<LogicalTDefChain> ReadRequiredLogicalTDefChainAsync(
        long startPage,
        CancellationToken cancellationToken)
        => LogicalTDefChain.ReadRequiredAsync(
            startPage,
            this.format.PageSize,
            this.pager.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: true,
            cancellationToken,
            this.tableDefs.MaxLogicalBytes);

    private ValueTask WriteLogicalTDefChainAsync(
        LogicalTDefChain chain,
        byte[] logicalBytes,
        int usedLength,
        CancellationToken cancellationToken)
        => chain.WriteAsync(
            logicalBytes,
            usedLength,
            this.pageAllocator.AllocatePageAsync,
            this.pager.WritePageAsync,
            this.pageAllocator.DeallocatePageAsync,
            writeFreeSpace: this.format.WritesTDefFreeSpace,
            cancellationToken);

    /// <summary>
    /// Parsed layout of a stitched TDEF of any format, used by the FK
    /// logical-idx mutation helpers (rename / remove / reclaim) to share the
    /// header validation and offset-computation boilerplate.
    /// </summary>
    /// <param name="NumCols">The number of cols.</param>
    /// <param name="NumIdx">The number of index.</param>
    /// <param name="NumRealIdx">The number of real index.</param>
    /// <param name="RealIdxDescStart">The real index desc start.</param>
    /// <param name="LogIdxStart">The log index start.</param>
    /// <param name="LogIdxNamesStart">The log index names start.</param>
    /// <param name="LogIdxNamesLen">The log index names len.</param>
    /// <param name="TrailingStart">The trailing start.</param>
    /// <param name="CurrentEnd">The current end.</param>
    /// <param name="TrailingLen">The trailing len.</param>
    private readonly record struct FkTDefLayout(
        int NumCols,
        int NumIdx,
        int NumRealIdx,
        int RealIdxDescStart,
        int LogIdxStart,
        int LogIdxNamesStart,
        int LogIdxNamesLen,
        int TrailingStart,
        int CurrentEnd,
        int TrailingLen);

    /// <summary>
    /// Validates that <paramref name="td"/> is a stitched TDEF
    /// with sane counts and computes every offset required by the FK
    /// mutation helpers in one pass. Returns <see langword="false"/> when
    /// the buffer is not a TDEF, has out-of-range counts, or the column-name
    /// / idx-name walk fails.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="layout">The layout.</param>
    private bool TryParseFkTDefLayout(byte[] td, out FkTDefLayout layout)
    {
        layout = default;
        if (td.Length < this.format.TDef.BlockEnd || td[0] != Constants.PageTypes.TableDefinition)
        {
            return false;
        }

        TDefCounts header = TDefCodec.ReadCounts(this.format, td);
        int numCols = header.ColumnCount;
        int numIdx = header.LogicalIndexCount;
        int numRealIdx = header.RealIndexCount;
        if (numCols < 0 || numCols > Constants.TableDefinition.MaxColumns
            || numIdx < 0 || numIdx > Constants.TableDefinition.MaxIndexes
            || numRealIdx < 0 || numRealIdx > Constants.TableDefinition.MaxIndexes)
        {
            return false;
        }

        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(this.format, td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return false;
        }

        int logIdxStart = this.format.Index.LogicalIdxStart(realIdxDescStart, numRealIdx);
        int logIdxNamesStart = this.format.Index.LogicalIdxNamesStart(logIdxStart, numIdx);
        int logIdxNamesLen = this.MeasureLogicalIdxNamesLength(td, logIdxNamesStart, numIdx);
        if (logIdxNamesLen < 0)
        {
            return false;
        }

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
            return false;
        }

        layout = new FkTDefLayout(
            numCols,
            numIdx,
            numRealIdx,
            realIdxDescStart,
            logIdxStart,
            logIdxNamesStart,
            logIdxNamesLen,
            trailingStart,
            currentEnd,
            trailingLen);
        return true;
    }

    /// <summary>
    /// Walks the logical-idx entries and returns the index of the first FK
    /// entry (<c>index_type == 0x02</c>) whose <c>rel_tbl_page</c> matches
    /// <paramref name="otherTdefPage"/> and whose backing real-idx col_map
    /// exactly covers <paramref name="columnNumbers"/> in declaration order.
    /// Returns <c>-1</c> when no entry matches; on success
    /// <paramref name="realIdxNum"/> is the matched real-idx slot.
    /// </summary>
    /// <param name="lay">The format's TDEF index layout.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="otherTdefPage">The other TDEF page.</param>
    /// <param name="realIdxNum">The real index number of.</param>
    private static int FindFkLogicalIdxEntry(
        IndexLayout lay,
        byte[] td,
        in FkTDefLayout layout,
        int[] columnNumbers,
        long otherTdefPage,
        out int realIdxNum)
    {
        realIdxNum = -1;
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int f = lay.LogicalIdxFieldsOffset(layout.LogIdxStart, li);
            byte indexType = td[f + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset];
            if (indexType != (byte)IndexKind.ForeignKey)
            {
                continue;
            }

            int relTblPage = Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset);
            if (relTblPage != otherTdefPage)
            {
                continue;
            }

            int rin = Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset);
            if (rin < 0 || rin >= layout.NumRealIdx)
            {
                continue;
            }

            if (!IndexHelpers.RealIdxColMapMatches(lay, td, lay.RealIdxPhysOffset(layout.RealIdxDescStart, rin), columnNumbers))
            {
                continue;
            }

            realIdxNum = rin;
            return li;
        }

        return -1;
    }

    /// <summary>
    /// Walks the variable-length idx-name section to position
    /// <paramref name="matchEntryIdx"/> and returns the byte offset and
    /// length of that entry's name record. Returns <see langword="false"/>
    /// when the walk fails before reaching the requested index.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="matchEntryIdx">The match entry index.</param>
    /// <param name="nameStart">The name start.</param>
    /// <param name="nameLen">The name len.</param>
    private bool TryGetLogicalIdxNameRange(
        byte[] td,
        in FkTDefLayout layout,
        int matchEntryIdx,
        out int nameStart,
        out int nameLen)
    {
        int namePos = layout.LogIdxNamesStart;
        for (int i = 0; i <= matchEntryIdx; i++)
        {
            int before = namePos;
            if (this.format.ReadColumnName(td, ref namePos, out _) < 0)
            {
                nameStart = -1;
                nameLen = 0;
                return false;
            }

            if (i == matchEntryIdx)
            {
                nameStart = before;
                nameLen = namePos - before;
                return true;
            }
        }

        nameStart = -1;
        nameLen = 0;
        return false;
    }

    /// <summary>Reads the physical partner pages, or null when the source is not a TDEF.</summary>
    /// <param name="tdefPage">The source table's intact definition page.</param>
    /// <param name="cancellationToken">Cancels the metadata read.</param>
    internal async ValueTask<SortedSet<long>?> ReadPartnerPagesAsync(long tdefPage, CancellationToken cancellationToken)
    {
        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
            tdefPage,
            this.format.PageSize,
            this.pager.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: false,
            cancellationToken,
            this.tableDefs.MaxLogicalBytes).ConfigureAwait(false);
        if (chain is null || !this.TryParseFkTDefLayout(chain.Bytes, out FkTDefLayout layout))
        {
            return null;
        }

        byte[] td = chain.Bytes;
        var partners = new SortedSet<long>();
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int f = this.format.Index.LogicalIdxFieldsOffset(layout.LogIdxStart, li);
            if (td[f + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset] != (byte)IndexKind.ForeignKey)
            {
                continue;
            }

            long partnerPage = Ri32(td, f + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset);
            if (partnerPage != tdefPage && this.IsTDefPageCandidate(partnerPage))
            {
                _ = partners.Add(partnerPage);
            }
        }

        return partners;
    }
}
