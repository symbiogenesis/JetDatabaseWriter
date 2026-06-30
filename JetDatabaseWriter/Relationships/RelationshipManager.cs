namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Schema.JetTypeInfo;

#pragma warning disable SA1204

/// <summary>
/// Foreign-key relationship management for <see cref="AccessWriter"/>:
/// create/drop/rename workflow and per-TDEF FK logical-index mutation.
/// MSysRelationships row operations live in <see cref="RelationshipCatalogStore"/>,
/// and runtime referential-integrity enforcement lives in <see cref="RelationshipEnforcer"/>.
/// Owned by an <see cref="AccessWriter"/> instance via a private field;
/// AccessWriter exposes thin forwarders for the public CRUD entry points.
/// </summary>
internal sealed class RelationshipManager
{
    private readonly AccessWriter writer;
    private readonly IndexMaintainer indexes;
    private readonly PageAllocator pageAllocator;
    private readonly RelationshipCatalogStore catalog;

    public RelationshipManager(AccessWriter writer, IndexMaintainer indexes, PageAllocator pageAllocator)
    {
        this.writer = writer;
        this.indexes = indexes;
        this.pageAllocator = pageAllocator;
        this.catalog = new RelationshipCatalogStore(writer);
        this.Enforcer = new RelationshipEnforcer(writer, indexes, this.catalog);
    }

    internal RelationshipEnforcer Enforcer { get; }

    // ════════════════════════════════════════════════════════════════
    // Foreign-key relationships — lifecycle orchestration
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Asynchronously creates a foreign-key relationship between two existing user
    /// tables by appending one row per FK column to the <c>MSysRelationships</c>
    /// system table. See
    /// <see cref="IAccessSchema.CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>
    /// for the full contract.
    /// </summary>
    /// <param name="relationship">The relationship to create.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Per <see href="docs/design/index-and-relationship-format-notes.md" /> §7. The
    /// MSysRelationships catalog rows are what the Microsoft Access
    /// Relationships designer reads. The per-TDEF FK logical-index entries
    /// (<c>index_type = 0x02</c>, <c>rel_idx_num</c>, <c>rel_tbl_page</c>)
    /// that drive runtime referential-integrity enforcement by the JET
    /// engine are emitted by <see cref="EmitFkPerTdefEntriesAsync"/> on
    /// Jet4 / ACE; on Jet3 they are skipped and Microsoft Access regenerates
    /// them on the next Compact &amp; Repair pass.
    /// </remarks>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => this.writer.RunAutoCommitAsync(_ => this.CreateRelationshipCoreAsync(relationship, cancellationToken), cancellationToken);

    private async ValueTask CreateRelationshipCoreAsync(RelationshipDefinition relationship, CancellationToken cancellationToken)
    {
        Guard.NotNull(relationship, nameof(relationship));
        Guard.NotNullOrEmpty(relationship.Name, "relationship.Name");
        Guard.NotNullOrEmpty(relationship.PrimaryTable, "relationship.PrimaryTable");
        Guard.NotNullOrEmpty(relationship.ForeignTable, "relationship.ForeignTable");
        Guard.ThrowIfDisposed(this.writer.IsDisposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // Validate referenced user tables exist and load their definitions.
        ResolvedTable primaryTable = await this.writer.ResolveRequiredTableAsync(relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        ResolvedTable foreignTable = await this.writer.ResolveRequiredTableAsync(relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
        CatalogEntry primaryEntry = primaryTable.Entry;
        CatalogEntry foreignEntry = foreignTable.Entry;
        TableDef primaryDef = primaryTable.Definition;
        TableDef foreignDef = foreignTable.Definition;

        for (int i = 0; i < relationship.PrimaryColumns.Count; i++)
        {
            if (primaryDef.FindColumnIndex(relationship.PrimaryColumns[i]) < 0)
            {
                throw new ArgumentException(
                    $"Column '{relationship.PrimaryColumns[i]}' was not found on table '{relationship.PrimaryTable}'.",
                    nameof(relationship));
            }

            if (foreignDef.FindColumnIndex(relationship.ForeignColumns[i]) < 0)
            {
                throw new ArgumentException(
                    $"Column '{relationship.ForeignColumns[i]}' was not found on table '{relationship.ForeignTable}'.",
                    nameof(relationship));
            }
        }

        // Locate MSysRelationships (system table — not in the user-table cache).
        long msysRelTdefPage = await this.catalog.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table. Full-catalog ACCDB databases " +
                "created by AccessWriter.CreateDatabaseAsync include it, but Jet/MDB outputs and slim " +
                "catalog databases may require an Access-authored source before calling CreateRelationshipAsync.");
        }

        TableDef msysRelDef = await this.writer.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

        // Reject duplicate relationship names (case-insensitive).
        HashSet<string> existingNames = await this.catalog.ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existingNames.Contains(relationship.Name))
        {
            throw new InvalidOperationException($"A relationship named '{relationship.Name}' already exists.");
        }

        await this.catalog.AppendRelationshipRowsAsync(msysRelTdefPage, msysRelDef, relationship, cancellationToken).ConfigureAwait(false);

        await this.writer.ExecuteCatalogArtifactPlanAsync(
            new CatalogArtifactPlan(
                [],
                [CatalogObjectArtifact.Relationship(relationship.Name)]),
            cancellationToken).ConfigureAwait(false);

        // Per-TDEF FK logical-idx entries: add index_type=0x02 logical-idx
        // entries on both PK-side and FK-side TDEFs with cross-referenced
        // rel_idx_num / rel_tbl_page so the JET engine can locate the partner
        // table without waiting for Microsoft Access Compact & Repair to
        // regenerate them from the MSysRelationships rows above. See
        // docs/design/index-and-relationship-format-notes.md §7. Jet3 uses a
        // different (20-byte) logical-idx layout that this library does not
        // yet exercise — skip silently to keep the catalog row emission
        // working on .mdb (Access 97) databases.
        if (this.writer.Format != DatabaseFormat.Jet3Mdb)
        {
            await this.EmitFkPerTdefEntriesAsync(
                relationship,
                primaryEntry.TDefPage,
                primaryDef,
                foreignEntry.TDefPage,
                foreignDef,
                cancellationToken).ConfigureAwait(false);

            // Populate the freshly-allocated FK index leaves so the seek-based
            // RI enforcement path (EnforceFkOnInsertAsync) sees existing parent
            // rows. EmitFkPerTdefEntriesAsync emits empty leaves; without this
            // rebuild a child INSERT immediately after CreateRelationshipAsync
            // would fail to match a parent row that was inserted before the
            // relationship existed. Re-read TDEFs because the emit mutates
            // both sides' TDEF pages in place.
            TableDef primaryDefAfter = await this.writer.ReadRequiredTableDefAsync(primaryEntry.TDefPage, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
            await this.indexes.MaintainIndexesAsync(primaryEntry.TDefPage, primaryDefAfter, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
            if (foreignEntry.TDefPage != primaryEntry.TDefPage)
            {
                TableDef foreignDefAfter = await this.writer.ReadRequiredTableDefAsync(foreignEntry.TDefPage, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
                await this.indexes.MaintainIndexesAsync(foreignEntry.TDefPage, foreignDefAfter, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Per-TDEF FK logical-idx entries (Jet4 / ACE only)
    // ════════════════════════════════════════════════════════════════

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
    /// each TDEF in place to append its FK logical-idx entry. Operates on
    /// single-page TDEFs only; throws <see cref="NotSupportedException"/> if
    /// either TDEF is multi-page or would overflow a single page after growth.
    /// </summary>
    /// <param name="relationship">The relationship.</param>
    /// <param name="primaryTdefPage">The primary TDEF page.</param>
    /// <param name="primaryDef">The primary def.</param>
    /// <param name="foreignTdefPage">The foreign TDEF page.</param>
    /// <param name="foreignDef">The foreign def.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask EmitFkPerTdefEntriesAsync(
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
        // logical-idx number, not the partner physical real-idx slot.
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
            (pkPlan, pkExistingNames) = await this.PrepareFkSideAsync(primaryTdefPage, pkColNums, cancellationToken).ConfigureAwait(false);
            (fkPlan, fkExistingNames) = await this.PrepareFkSideAsync(foreignTdefPage, fkColNums, cancellationToken).ConfigureAwait(false);
        }

        // Allocate empty leaf pages for any newly-allocated real-idx slots.
        // Both leaf pages are appended before any TDEF mutation so the page
        // numbers are stable for the cross-referenced first_dp values.
        if (pkPlan.AllocatesNewRealIdx)
        {
            byte[] leaf = IndexPageCodec.BuildLeafPage(
                IndexPageLayout.Jet4,
                this.writer.PageSizeBytes,
                primaryTdefPage,
                [],
                enablePrefixCompression: false);
            long lp = await this.pageAllocator.AllocatePageAsync(leaf, cancellationToken).ConfigureAwait(false);
            pkPlan = pkPlan.WithLeafPage(lp);
        }

        if (fkPlan.AllocatesNewRealIdx)
        {
            byte[] leaf = IndexPageCodec.BuildLeafPage(
                IndexPageLayout.Jet4,
                this.writer.PageSizeBytes,
                foreignTdefPage,
                [],
                enablePrefixCompression: false);
            long lp = await this.pageAllocator.AllocatePageAsync(leaf, cancellationToken).ConfigureAwait(false);
            fkPlan = fkPlan.WithLeafPage(lp);
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

        // Emit both sides. PK side carries no cascade flags (cascade is an
        // FK-side property — Access only checks them when modifying the parent
        // and looking up children).
        await this.EmitFkLogicalIdxAsync(
            primaryTdefPage,
            pkColNums,
            pkName,
            pkPlan,
            relTblTypeThisSide: Constants.TableDefinition.ParentRelationshipTableType,
            relIdxNumOtherSide: fkPlan.LogicalIdxNum,
            relTblPageOther: foreignTdefPage,
            cascadeUps: 0,
            cascadeDels: 0,
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
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads one side's TDEF page, walks the col-name and idx-name sections,
    /// detects any existing real-idx that already covers <paramref name="columnNumbers"/>
    /// (sharing per §3.3), and returns the resulting plan plus the existing
    /// logical-idx-name list (used to avoid name collisions on the new entry).
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="NotSupportedException">Thrown when the TDEF cannot be mutated because its layout is malformed or not a TDEF.</exception>
    private async ValueTask<(FkSidePlan Plan, List<string> ExistingNames)> PrepareFkSideAsync(
        long tdefPage,
        int[] columnNumbers,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] page = chain.Bytes;
        if (!this.TryParseFkTDefLayout(page, out FkTDefLayout layout))
        {
            throw new NotSupportedException(
                $"TDEF at page {tdefPage} cannot be mutated in place (malformed counts or not a TDEF).");
        }

        int sharedSlot = FindCoveringRealIdx(page, columnNumbers, layout.RealIdxDescStart, layout.NumRealIdx);
        List<string> existingNames = this.ReadLogicalIdxNames(page, layout.LogIdxNamesStart, layout.NumIdx);

        int logicalIdxNum = NextLogicalIdxNumber(page, in layout);
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

        int pkSharedSlot = FindCoveringRealIdx(page, pkColumnNumbers, layout.RealIdxDescStart, layout.NumRealIdx);
        int fkSharedSlot = FindCoveringRealIdx(page, fkColumnNumbers, layout.RealIdxDescStart, layout.NumRealIdx);
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

        int pkLogicalIdxNum = NextLogicalIdxNumber(page, in layout);
        int fkLogicalIdxNum = pkLogicalIdxNum + 1;
        List<string> existingNames = this.ReadLogicalIdxNames(page, layout.LogIdxNamesStart, layout.NumIdx);

        return (
            new FkSidePlan(pkRealIdxNum, pkLogicalIdxNum, pkAllocates, 0),
            new FkSidePlan(fkRealIdxNum, fkLogicalIdxNum, fkAllocates, 0),
            existingNames);
    }

    /// <summary>
    /// Appends one FK logical-idx entry (and optionally a new real-idx
    /// physical descriptor) to the TDEF at <paramref name="tdefPage"/>. The
    /// TDEF must fit on a single page after the addition.
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
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="NotSupportedException">Thrown when the target TDEF cannot be mutated because its layout is malformed or not a TDEF.</exception>
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
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] td = chain.Bytes;

        if (!this.TryParseFkTDefLayout(td, out FkTDefLayout layout))
        {
            throw new NotSupportedException(
                $"cannot mutate the TDEF at page {tdefPage} (malformed counts or not a TDEF).");
        }

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

        byte[] nameBytes = Encoding.Unicode.GetBytes(indexName);
        int nameRecordSize = 2 + nameBytes.Length;

        int deltaRealIdxSkip = sidePlan.AllocatesNewRealIdx ? this.writer.TDef.RealIdxEntrySz : 0;
        int deltaRealIdxPhys = sidePlan.AllocatesNewRealIdx ? Constants.TableDefinition.Jet4.RealIdx.PhysSize : 0;
        int totalGrowth = deltaRealIdxSkip + deltaRealIdxPhys + Constants.TableDefinition.Jet4.LogicalIdx.EntrySize + nameRecordSize;

        // Build the rewritten page.
        byte[] newTd = new byte[LogicalTDefChain.GetLogicalCapacity(this.writer.PageSizeBytes, currentEnd + totalGrowth)];
        Buffer.BlockCopy(td, 0, newTd, 0, this.writer.TDef.BlockEnd);

        // Real-idx skip block (existing slots, unchanged content).
        int oldRealIdxSkipLen = numRealIdx * this.writer.TDef.RealIdxEntrySz;
        Buffer.BlockCopy(td, this.writer.TDef.BlockEnd, newTd, this.writer.TDef.BlockEnd, oldRealIdxSkipLen);
        int newRealIdxSkipEnd = this.writer.TDef.BlockEnd + oldRealIdxSkipLen + deltaRealIdxSkip;

        // Column descriptors.
        int oldColStart = this.writer.TDef.BlockEnd + oldRealIdxSkipLen;
        int colDescBlockLen = numCols * this.writer.ColumnDescriptor.Size;
        Buffer.BlockCopy(td, oldColStart, newTd, newRealIdxSkipEnd, colDescBlockLen);

        // Column names (variable length).
        int oldColNamesStart = oldColStart + colDescBlockLen;
        int colNamesLen = realIdxDescStart - oldColNamesStart;
        int newColNamesStart = newRealIdxSkipEnd + colDescBlockLen;
        Buffer.BlockCopy(td, oldColNamesStart, newTd, newColNamesStart, colNamesLen);

        // Real-idx physical descriptors (existing slots).
        int newRealIdxDescStart = newColNamesStart + colNamesLen;
        int oldRealIdxPhysLen = numRealIdx * Constants.TableDefinition.Jet4.RealIdx.PhysSize;
        Buffer.BlockCopy(td, realIdxDescStart, newTd, newRealIdxDescStart, oldRealIdxPhysLen);

        // Append a new real-idx physical descriptor when allocating a new slot.
        if (sidePlan.AllocatesNewRealIdx)
        {
            int phys = newRealIdxDescStart + oldRealIdxPhysLen;

            // bytes 0..3   Jet4/ACE real-idx physical-descriptor leading magic
            //              (0x00000783). Distinct from the format-wide TDEF
            //              magic (0x00000659) used in column / logical-idx
            //              descriptors — see Constants.TableDefinition.Jet4.RealIdx.LeadingMagic
            //              and BuildTDefPagesWithIndexOffsets. DAO validates
            //              this during CompactDatabase / OpenRecordset on
            //              tables with FK indexes; leaving the wrong magic
            //              here surfaces as AssertTdefMagicStampsAsync
            //              ("real-idx[1] magic = 0x00000659, expected 0x00000783").
            Wi32(newTd, phys, Constants.TableDefinition.Jet4.RealIdx.LeadingMagic);

            // bytes 4..33  col_map: 10 × {col_num(2), col_order(1)}
            for (int slot = 0; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
            {
                int so = phys + Constants.TableDefinition.Jet4.RealIdx.ColMapOffset
                    + (slot * Constants.TableDefinition.ColMapSlotSize);
                if (slot < columnNumbers.Length)
                {
                    Wu16(newTd, so, columnNumbers[slot]);
                    newTd[so + 2] = Constants.TableDefinition.ColMapAscendingFlag;
                }
                else
                {
                    Wu16(newTd, so, Constants.TableDefinition.ColMapPaddingSlot);
                    newTd[so + 2] = Constants.TableDefinition.ColMapDescendingFlag;
                }
            }

            // bytes 34..37 used_pages = 0 initially; MaintainIndexesAsync
            // patches the DAO-shaped index usage-map pointer after rebuilding.
            // bytes 38..41 first_dp = sidePlan.NewLeafPageNumber
            Wi32(newTd, phys + Constants.TableDefinition.Jet4.RealIdx.FirstDpOffset, checked((int)sidePlan.NewLeafPageNumber));

            // bytes 42..45 unknown(4) = 0
            // byte  46     flags: 0x80 (unknown-flag bit always set per Jackcess)
            // bytes 47..51 unknown(5) = 0
            newTd[phys + Constants.TableDefinition.Jet4.RealIdx.FlagsOffset] =
                Constants.TableDefinition.UnknownIndexFlag;
        }

        // Logical-idx entries. DAO prepends relationship logical entries before
        // the existing PrimaryKey entry; CompactDatabase preserves FK tables only
        // when the entry/name ordering follows that shape.
        int newLogIdxStart = newRealIdxDescStart + oldRealIdxPhysLen + deltaRealIdxPhys;
        int oldLogIdxLen = numIdx * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize;

        // Write the new FK logical-idx entry first.
        // bytes 0..3   Jet4/ACE format magic cookie (0x00000659). DAO checks
        //              this during CompactDatabase.
        // bytes 24..27 trailing(4) = 0
        int newLogEntry = newLogIdxStart;
        Wi32(newTd, newLogEntry, Constants.TableDefinition.Jet4.FormatMagic);
        Wi32(newTd, newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.IndexNumOffset, sidePlan.LogicalIdxNum);
        Wi32(newTd, newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.IndexNum2Offset, sidePlan.RealIdxNum);
        newTd[newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.RelTblTypeOffset] = relTblTypeThisSide;
        Wi32(newTd, newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.RelIdxNumOffset, relIdxNumOtherSide);
        Wi32(newTd, newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.RelTblPageOffset, checked((int)relTblPageOther));
        newTd[newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.CascadeUpsOffset] = cascadeUps;
        newTd[newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.CascadeDelsOffset] = cascadeDels;
        newTd[newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.IndexTypeOffset] = (byte)IndexKind.ForeignKey;

        int existingLogIdxStart = newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.EntrySize;
        Buffer.BlockCopy(td, logIdxStart, newTd, existingLogIdxStart, oldLogIdxLen);

        // Logical-idx names follow the same order as their entries.
        int newNameOffset = existingLogIdxStart + oldLogIdxLen;
        Wu16(newTd, newNameOffset, nameBytes.Length);
        Buffer.BlockCopy(nameBytes, 0, newTd, newNameOffset + 2, nameBytes.Length);

        int existingNamesOffset = newNameOffset + nameRecordSize;
        Buffer.BlockCopy(td, logIdxNamesStart, newTd, existingNamesOffset, logIdxNamesLen);

        // Trailing variable-length-column block (Access-emitted TDEFs only).
        int newTrailingStart = existingNamesOffset + logIdxNamesLen;
        if (trailingLen > 0)
        {
            Buffer.BlockCopy(td, trailingStart, newTd, newTrailingStart, trailingLen);
        }

        // Update header counts.
        Wi32(newTd, this.writer.TDef.NumCols + 2, numIdx + 1);
        if (sidePlan.AllocatesNewRealIdx)
        {
            Wi32(newTd, this.writer.TDef.NumRealIdx, numRealIdx + 1);
        }

        // tdef_len at offset 8 = (newEnd - 8). The page header (8 bytes) is
        // not counted in tdef_len, matching BuildTDefPageWithIndexOffsets.
        Wi32(newTd, 8, newTrailingStart + trailingLen - 8);

        await this.WriteLogicalTDefChainAsync(
            chain,
            newTd,
            newTrailingStart + trailingLen,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Walks past column descriptors and column names to return the byte
    /// offset of the real-index physical-descriptor section start, or -1
    /// when the column-name walk fails.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="numCols">The number of cols.</param>
    /// <param name="numRealIdx">The number of real index.</param>
    internal int LocateRealIdxDescStart(byte[] td, int numCols, int numRealIdx)
    {
        int colStart = this.writer.TDef.BlockEnd + (numRealIdx * this.writer.TDef.RealIdxEntrySz);
        int pos = colStart + (numCols * this.writer.ColumnDescriptor.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (this.writer.ReadColumnName(td, ref pos, out _) < 0)
            {
                return -1;
            }
        }

        return pos;
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
            if (this.writer.ReadColumnName(td, ref pos, out _) < 0)
            {
                return -1;
            }
        }

        return pos - logIdxNamesStart;
    }

    /// <summary>
    /// Materializes the existing logical-idx-name list (used to avoid name
    /// collisions when appending a new FK index entry).
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="logIdxNamesStart">The log index names start.</param>
    /// <param name="numIdx">The number of index.</param>
    internal List<string> ReadLogicalIdxNames(byte[] td, int logIdxNamesStart, int numIdx)
    {
        var list = new List<string>(numIdx);
        int pos = logIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (this.writer.ReadColumnName(td, ref pos, out string n) < 0)
            {
                break;
            }

            list.Add(n);
        }

        return list;
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
    /// order); -1 when no covering real-idx exists. Jet4 col_map is fixed at
    /// 10 slots × {col_num(2), col_order(1)}.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="realIdxDescStart">The real index desc start.</param>
    /// <param name="numRealIdx">The number of real index.</param>
    private static int FindCoveringRealIdx(byte[] td, int[] columnNumbers, int realIdxDescStart, int numRealIdx)
    {
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * Constants.TableDefinition.Jet4.RealIdx.PhysSize);
            if (IndexHelpers.RealIdxColMapMatches(td, phys, columnNumbers))
            {
                return ri;
            }
        }

        return -1;
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

    private static int NextLogicalIdxNumber(byte[] td, in FkTDefLayout layout)
    {
        int max = -1;
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int entry = layout.LogIdxStart + (li * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
            int indexNum = Ri32(td, entry + Constants.TableDefinition.Jet4.LogicalIdx.IndexNumOffset);
            if (indexNum > max)
            {
                max = indexNum;
            }
        }

        return max + 1;
    }

    // ════════════════════════════════════════════════════════════════
    // Drop / Rename relationship
    // ════════════════════════════════════════════════════════════════
    //
    // Reverses CreateRelationshipAsync:
    //   • DropRelationshipAsync rewrites MSysRelationships as the remaining
    //     live rows, excluding rows whose szRelationship matches, and
    //     (Jet4/ACE) removes the matching FK logical-idx entry from each
    //     side's TDEF, then conservatively
    //     reclaims any trailing real-idx physical-descriptor slots that the
    //     removal left unreferenced (common case: FK got the last slot on
    //     its TDEF and the slot is reclaimed cleanly; non-trailing orphans
    //     are still left for Compact & Repair to reclaim, since mid-array
    //     compaction would require cross-TDEF rel_idx_num renumbering on
    //     every other table that points at the slot).
    //     ListIndexesAsync iterates by num_idx so the FK stops surfacing
    //     immediately regardless of whether the real-idx slot was reclaimed.
    //   • RenameRelationshipAsync rewrites MSysRelationships as live rows
    //     with szRelationship replaced on every match and (Jet4/ACE) updates
    //     the matching FK logical-idx name cookie on each side's TDEF through
    //     the logical-chain writer. Relationship Type=8 MSysObjects rows are
    //     deliberately not renamed or deleted here; DAO Compact & Repair
    //     normalizes them from MSysRelationships, while manual mutation of
    //     those rows has proven less compact-safe.

    /// <summary>
    /// Asynchronously deletes a foreign-key relationship and its Jet4 / ACE
    /// per-TDEF logical-index entries inside an auto-commit operation.
    /// </summary>
    /// <param name="relationshipName">The case-insensitive relationship name to delete.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
        => this.writer.RunAutoCommitAsync(_ => this.DropRelationshipCoreAsync(relationshipName, cancellationToken), cancellationToken);

    private async ValueTask DropRelationshipCoreAsync(string relationshipName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(relationshipName, nameof(relationshipName));
        Guard.ThrowIfDisposed(this.writer.IsDisposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        long msysRelTdefPage = await this.catalog.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to drop.");
        }

        TableDef msysRelDef = await this.writer.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        List<RelationshipRowSnapshot> allRows = await this.catalog.CollectRowsAsync(
            msysRelTdefPage,
            msysRelDef,
            _ => true,
            cancellationToken).ConfigureAwait(false);

        var matches = new List<RelationshipRowSnapshot>();
        foreach (RelationshipRowSnapshot row in allRows)
        {
            if (string.Equals(row.SzRelationship, relationshipName, StringComparison.OrdinalIgnoreCase))
            {
                matches.Add(row);
            }
        }

        if (matches.Count == 0)
        {
            throw new InvalidOperationException($"No relationship named '{relationshipName}' was found.");
        }

        // Jet4/ACE only — Jet3 never received the per-TDEF FK logical-idx entries.
        if (this.writer.Format != DatabaseFormat.Jet3Mdb)
        {
            await this.ForEachRelationshipFkPairAsync(
                matches,
                async (ctx, ct) =>
                {
                    // Remove the matching FK logical-idx entry from each side.
                    // Self-referential relationships (PK and FK on same TDEF) need
                    // both removals to target distinct entries — pass the column
                    // list to disambiguate.
                    int pkReleased = await this.TryRemoveFkLogicalIdxEntryAsync(ctx.PkEntry.TDefPage, ctx.PkColNums, ctx.FkEntry.TDefPage, ct).ConfigureAwait(false);
                    int fkReleased = await this.TryRemoveFkLogicalIdxEntryAsync(ctx.FkEntry.TDefPage, ctx.FkColNums, ctx.PkEntry.TDefPage, ct).ConfigureAwait(false);

                    // Reclaim trailing real-idx slots that are no longer
                    // referenced by any logical-idx entry. PK-side typically
                    // shares its real-idx slot with the existing PK logical-idx
                    // (no reclaim possible), but the FK-side's real-idx is
                    // usually its own and can be reclaimed cleanly. Self-
                    // referential: PK and FK live on the same TDEF and both
                    // removals already happened above; one reclaim pass covers
                    // both released slots.
                    if (pkReleased >= 0)
                    {
                        await this.TryReclaimTrailingRealIdxAsync(ctx.PkEntry.TDefPage, ct).ConfigureAwait(false);
                    }

                    if (fkReleased >= 0 && ctx.PkEntry.TDefPage != ctx.FkEntry.TDefPage)
                    {
                        await this.TryReclaimTrailingRealIdxAsync(ctx.FkEntry.TDefPage, ct).ConfigureAwait(false);
                    }
                },
                cancellationToken).ConfigureAwait(false);
        }

        var remainingRows = new List<object[]>(allRows.Count - matches.Count);
        foreach (RelationshipRowSnapshot row in allRows)
        {
            if (!string.Equals(row.SzRelationship, relationshipName, StringComparison.OrdinalIgnoreCase))
            {
                remainingRows.Add(row.RowValues);
            }
        }

        await this.catalog.RewriteRowsAsync(msysRelTdefPage, msysRelDef, remainingRows, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously renames a foreign-key relationship and its Jet4 / ACE
    /// per-TDEF logical-index name cookies inside an auto-commit operation.
    /// </summary>
    /// <param name="oldName">The case-insensitive existing relationship name.</param>
    /// <param name="newName">The new relationship name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
        => this.writer.RunAutoCommitAsync(_ => this.RenameRelationshipCoreAsync(oldName, newName, cancellationToken), cancellationToken);

    private async ValueTask RenameRelationshipCoreAsync(string oldName, string newName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(oldName, nameof(oldName));
        Guard.NotNullOrEmpty(newName, nameof(newName));
        Guard.ThrowIfDisposed(this.writer.IsDisposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (string.Equals(oldName, newName, StringComparison.OrdinalIgnoreCase))
        {
            return; // No-op; matches Microsoft Access' designer behaviour.
        }

        long msysRelTdefPage = await this.catalog.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to rename.");
        }

        TableDef msysRelDef = await this.writer.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

        // Reject collision with an existing name (case-insensitive).
        HashSet<string> existing = await this.catalog.ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existing.Contains(newName))
        {
            throw new InvalidOperationException($"A relationship named '{newName}' already exists.");
        }

        List<RelationshipRowSnapshot> allRows = await this.catalog.CollectRowsAsync(
            msysRelTdefPage,
            msysRelDef,
            _ => true,
            cancellationToken).ConfigureAwait(false);

        var matches = new List<RelationshipRowSnapshot>();
        foreach (RelationshipRowSnapshot row in allRows)
        {
            if (string.Equals(row.SzRelationship, oldName, StringComparison.OrdinalIgnoreCase))
            {
                matches.Add(row);
            }
        }

        if (matches.Count == 0)
        {
            throw new InvalidOperationException($"No relationship named '{oldName}' was found.");
        }

        int szRelIdx = msysRelDef.FindColumnIndex("szRelationship");
        if (szRelIdx < 0)
        {
            throw new InvalidOperationException("MSysRelationships does not expose a 'szRelationship' column.");
        }

        var replacementRows = new List<object[]>(allRows.Count);
        foreach (RelationshipRowSnapshot row in allRows)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object[] rowValues = (object[])row.RowValues.Clone();
            if (string.Equals(row.SzRelationship, oldName, StringComparison.OrdinalIgnoreCase))
            {
                rowValues[szRelIdx] = newName;
            }

            replacementRows.Add(rowValues);
        }

        await this.catalog.RewriteRowsAsync(msysRelTdefPage, msysRelDef, replacementRows, cancellationToken).ConfigureAwait(false);

        // Update the TDEF logical-idx name cookies on both sides so the
        // on-disk index name matches the catalog row. Jet3 never received
        // FK logical-idx entries, so this is a no-op there.
        if (this.writer.Format != DatabaseFormat.Jet3Mdb)
        {
            await this.ForEachRelationshipFkPairAsync(
                matches,
                async (ctx, ct) =>
                {
                    // Reproduce the cookie-naming convention from CreateRelationshipAsync:
                    // PK side uses the relationship name; FK side appends "_FK"
                    // when both endpoints land on the same TDEF (self-referential).
                    string newPkBase = newName;
                    string newFkBase = ctx.PkEntry.TDefPage == ctx.FkEntry.TDefPage
                        ? newName + "_FK"
                        : newName;

                    string newPkName = await this.PickUniqueLogicalIdxNameAsync(ctx.PkEntry.TDefPage, newPkBase, ct).ConfigureAwait(false);
                    _ = await this.TryRenameFkLogicalIdxNameAsync(ctx.PkEntry.TDefPage, ctx.PkColNums, ctx.FkEntry.TDefPage, newPkName, ct).ConfigureAwait(false);

                    string newFkName = await this.PickUniqueLogicalIdxNameAsync(ctx.FkEntry.TDefPage, newFkBase, ct).ConfigureAwait(false);
                    _ = await this.TryRenameFkLogicalIdxNameAsync(ctx.FkEntry.TDefPage, ctx.FkColNums, ctx.PkEntry.TDefPage, newFkName, ct).ConfigureAwait(false);
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

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
    private async ValueTask<string> PickUniqueLogicalIdxNameAsync(
        long tdefPage,
        string baseName,
        CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await this.ReadRequiredLogicalTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] pageBytes = chain.Bytes;
        if (!this.TryParseFkTDefLayout(pageBytes, out FkTDefLayout layout) || layout.NumIdx <= 0)
        {
            return baseName;
        }

        List<string> existing = this.ReadLogicalIdxNames(pageBytes, layout.LogIdxNamesStart, layout.NumIdx);
        return IndexHelpers.MakeUniqueLogicalIdxName(baseName, existing);
    }

    /// <summary>
    /// Locates and removes the FK logical-idx entry on <paramref name="tdefPage"/>
    /// whose backing real-idx col_map exactly covers <paramref name="columnNumbers"/>
    /// (in declaration order) AND whose <c>rel_tbl_page</c> equals
    /// <paramref name="otherTdefPage"/>. Returns the real-idx slot number that
    /// the removed FK entry referenced (so the caller can attempt
    /// <see cref="TryReclaimTrailingRealIdxAsync"/>), or <c>-1</c> when no
    /// matching entry exists (already removed, or never created — Jet3 path,
    /// or out-of-band catalog).
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="otherTdefPage">The other TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<int> TryRemoveFkLogicalIdxEntryAsync(
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
        int matchEntryIdx = FindFkLogicalIdxEntry(td, in layout, columnNumbers, otherTdefPage, out int releasedRealIdxNum);
        if (matchEntryIdx < 0)
        {
            return -1;
        }

        if (!this.TryGetLogicalIdxNameRange(td, in layout, matchEntryIdx, out int removedNameStart, out int removedNameLen))
        {
            return -1;
        }

        // Mutate `td` in place via two left-shifts (Buffer.BlockCopy supports
        // overlapping regions). Step 1 collapses the 28-byte logical-idx
        // entry; step 2 collapses the variable-length name. The trailing
        // variable-length-column block rides along with the second shift.
        int removedEntryStart = layout.LogIdxStart + (matchEntryIdx * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
        int afterEntry = removedEntryStart + Constants.TableDefinition.Jet4.LogicalIdx.EntrySize;

        // Step 1 — drop the 28-byte logical-idx entry.
        Buffer.BlockCopy(td, afterEntry, td, removedEntryStart, layout.CurrentEnd - afterEntry);
        int shiftedNameStart = removedNameStart - Constants.TableDefinition.Jet4.LogicalIdx.EntrySize;
        int afterName = shiftedNameStart + removedNameLen;
        int endAfterStep1 = layout.CurrentEnd - Constants.TableDefinition.Jet4.LogicalIdx.EntrySize;

        // Step 2 — drop the name record.
        Buffer.BlockCopy(td, afterName, td, shiftedNameStart, endAfterStep1 - afterName);
        int finalEnd = endAfterStep1 - removedNameLen;

        // Zero the freed tail so the on-disk page matches the prior
        // fresh-buffer behavior (bytes past the new end are padding).
        Array.Clear(td, finalEnd, layout.CurrentEnd - finalEnd);

        // Update header counts.
        Wi32(td, this.writer.TDef.NumCols + 2, layout.NumIdx - 1);
        Wi32(td, 8, finalEnd - 8);

        await this.WriteLogicalTDefChainAsync(chain, td, finalEnd, cancellationToken).ConfigureAwait(false);
        return releasedRealIdxNum;
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
    /// block (<c>num_real_idx × _writer._tdef.RealIdxEntrySz</c> bytes immediately after
    /// the Jet4 TDEF block) and the trailing 52-byte physical descriptor,
    /// decrements <c>num_real_idx</c>, and updates <c>tdef_len</c>.
    /// </para>
    /// <para>
    /// Jet4 / ACE only — Jet3 takes the same path because the FK logical-idx
    /// entries are not emitted on Jet3 to begin with, so this is never called
    /// against a Jet3 TDEF.
    /// </para>
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask TryReclaimTrailingRealIdxAsync(
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
        // index_num2 (offset +8 in the 28-byte Jet4 entry).
        bool[] referenced = new bool[layout.NumRealIdx];
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int e = layout.LogIdxStart + (li * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
            int realIdxNum = Ri32(td, e + Constants.TableDefinition.Jet4.LogicalIdx.IndexNum2Offset);
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

        // Step 1 — drop the trailing N entries (12 bytes each on Jet4) from
        // the leading real-idx skip block. The skip block lives at
        // [_writer._tdef.BlockEnd, _writer._tdef.BlockEnd + numRealIdx * _writer._tdef.RealIdxEntrySz). We
        // collapse out the LAST N × _writer._tdef.RealIdxEntrySz bytes of that block by
        // left-shifting everything that follows.
        int oldSkipEnd = this.writer.TDef.BlockEnd + (layout.NumRealIdx * this.writer.TDef.RealIdxEntrySz);
        int newSkipEnd = oldSkipEnd - (reclaim * this.writer.TDef.RealIdxEntrySz);
        Buffer.BlockCopy(td, oldSkipEnd, td, newSkipEnd, layout.CurrentEnd - oldSkipEnd);
        int endAfterStep1 = layout.CurrentEnd - (reclaim * this.writer.TDef.RealIdxEntrySz);

        // After step 1 the real-idx physical descriptor section starts at
        // (realIdxDescStart - reclaim * _writer._tdef.RealIdxEntrySz). We need to drop the
        // trailing N × 52 bytes of physical descriptors. Compute the new
        // boundaries.
        int newRealIdxDescStart = layout.RealIdxDescStart - (reclaim * this.writer.TDef.RealIdxEntrySz);
        int newPhysEnd = newRealIdxDescStart + ((layout.NumRealIdx - reclaim) * Constants.TableDefinition.Jet4.RealIdx.PhysSize);
        int oldPhysEnd = newRealIdxDescStart + (layout.NumRealIdx * Constants.TableDefinition.Jet4.RealIdx.PhysSize);

        // Step 2 — drop the trailing N × 52-byte physical descriptors by
        // left-shifting the logical-idx entries + names + variable-col block.
        Buffer.BlockCopy(td, oldPhysEnd, td, newPhysEnd, endAfterStep1 - oldPhysEnd);
        int finalEnd = endAfterStep1 - (reclaim * Constants.TableDefinition.Jet4.RealIdx.PhysSize);

        // Zero the freed tail so the on-disk page matches the prior
        // fresh-buffer behavior (bytes past the new end are padding).
        Array.Clear(td, finalEnd, layout.CurrentEnd - finalEnd);

        // Update header counts.
        Wi32(td, this.writer.TDef.NumRealIdx, layout.NumRealIdx - reclaim);
        Wi32(td, 8, finalEnd - 8);

        await this.WriteLogicalTDefChainAsync(chain, td, finalEnd, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Renames the FK logical-idx "name cookie" on <paramref name="tdefPage"/>
    /// for the entry whose backing real-idx col_map exactly covers
    /// <paramref name="columnNumbers"/> AND whose <c>rel_tbl_page</c> equals
    /// <paramref name="otherTdefPage"/>. Returns <see langword="true"/> when
    /// an entry was found and renamed; <see langword="false"/> otherwise
    /// (already renamed, never created — Jet3 or out-of-band catalog).
    /// Variable-length name records: shrink/grow is handled by shifting the
    /// trailing variable-column block; growth can spill into a continuation page
    /// through the logical TDEF-chain writer.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="otherTdefPage">The other TDEF page.</param>
    /// <param name="newName">The new name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> TryRenameFkLogicalIdxNameAsync(
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

        int matchEntryIdx = FindFkLogicalIdxEntry(td, in layout, columnNumbers, otherTdefPage, out _);
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

        byte[] newNameBytes = Encoding.Unicode.GetBytes(newName);
        int newNameRecordSize = 2 + newNameBytes.Length;
        int delta = newNameRecordSize - oldNameLen;

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
        Wu16(td, oldNameStart, newNameBytes.Length);
        Buffer.BlockCopy(newNameBytes, 0, td, oldNameStart + 2, newNameBytes.Length);

        // Update tdef_len.
        Wi32(td, 8, finalEnd - 8);

        await this.WriteLogicalTDefChainAsync(chain, td, finalEnd, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private ValueTask<LogicalTDefChain> ReadRequiredLogicalTDefChainAsync(
        long startPage,
        CancellationToken cancellationToken)
        => LogicalTDefChain.ReadRequiredAsync(
            startPage,
            this.writer.PageSizeBytes,
            this.writer.ReadPageAsync,
            AccessBase.ReturnPage,
            retainPageNumbers: true,
            cancellationToken);

    private ValueTask WriteLogicalTDefChainAsync(
        LogicalTDefChain chain,
        byte[] logicalBytes,
        int usedLength,
        CancellationToken cancellationToken)
        => chain.WriteAsync(
            logicalBytes,
            usedLength,
            this.pageAllocator.AllocatePageAsync,
            this.writer.WritePageAsync,
            this.pageAllocator.DeallocatePageAsync,
            cancellationToken);

    /// <summary>
    /// Parsed layout of a stitched Jet4/ACE TDEF, used by the FK
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
    /// Validates that <paramref name="td"/> is a stitched Jet4/ACE TDEF
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
        if (td.Length < this.writer.TDef.BlockEnd || td[0] != Constants.PageTypes.TableDefinition)
        {
            return false;
        }

        int numCols = Ru16(td, this.writer.TDef.NumCols);
        int numIdx = Ri32(td, this.writer.TDef.NumCols + 2);
        int numRealIdx = Ri32(td, this.writer.TDef.NumRealIdx);
        if (numCols < 0 || numCols > Constants.TableDefinition.MaxColumns
            || numIdx < 0 || numIdx > Constants.TableDefinition.MaxIndexes
            || numRealIdx < 0 || numRealIdx > Constants.TableDefinition.MaxIndexes)
        {
            return false;
        }

        int realIdxDescStart = this.LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return false;
        }

        int logIdxStart = realIdxDescStart + (numRealIdx * Constants.TableDefinition.Jet4.RealIdx.PhysSize);
        int logIdxNamesStart = logIdxStart + (numIdx * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
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
    /// <param name="td">Parsed table definition.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="columnNumbers">The column numbers.</param>
    /// <param name="otherTdefPage">The other TDEF page.</param>
    /// <param name="realIdxNum">The real index number of.</param>
    private static int FindFkLogicalIdxEntry(
        byte[] td,
        in FkTDefLayout layout,
        int[] columnNumbers,
        long otherTdefPage,
        out int realIdxNum)
    {
        realIdxNum = -1;
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int e = layout.LogIdxStart + (li * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
            byte indexType = td[e + Constants.TableDefinition.Jet4.LogicalIdx.IndexTypeOffset];
            if (indexType != (byte)IndexKind.ForeignKey)
            {
                continue;
            }

            int relTblPage = Ri32(td, e + Constants.TableDefinition.Jet4.LogicalIdx.RelTblPageOffset);
            if (relTblPage != otherTdefPage)
            {
                continue;
            }

            int rin = Ri32(td, e + Constants.TableDefinition.Jet4.LogicalIdx.IndexNum2Offset);
            if (rin < 0 || rin >= layout.NumRealIdx)
            {
                continue;
            }

            int phys = layout.RealIdxDescStart + (rin * Constants.TableDefinition.Jet4.RealIdx.PhysSize);
            if (!IndexHelpers.RealIdxColMapMatches(td, phys, columnNumbers))
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
            if (this.writer.ReadColumnName(td, ref namePos, out _) < 0)
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

    /// <summary>
    /// Per-pair context resolved by <see cref="ForEachRelationshipFkPairAsync"/>:
    /// catalog entries, table definitions, and column-number arrays for both
    /// sides of one (PK table, FK table) pair, with the FK column list in
    /// <c>icolumn</c> order.
    /// </summary>
    /// <param name="PkTableName">The primary key table name.</param>
    /// <param name="PkEntry">The primary key entry.</param>
    /// <param name="PkDef">The primary key def.</param>
    /// <param name="PkColNums">The primary key col nums.</param>
    /// <param name="FkTableName">The foreign key table name.</param>
    /// <param name="FkEntry">The foreign key entry.</param>
    /// <param name="FkDef">The foreign key def.</param>
    /// <param name="FkColNums">The foreign key col nums.</param>
    private readonly record struct FkPairContext(
        string PkTableName,
        CatalogEntry PkEntry,
        TableDef PkDef,
        int[] PkColNums,
        string FkTableName,
        CatalogEntry FkEntry,
        TableDef FkDef,
        int[] FkColNums);

    /// <summary>
    /// Groups <paramref name="matches"/> by (PK table, FK table) pair —
    /// <see cref="CreateRelationshipAsync"/> emits N rows (one per FK column
    /// pair) sharing szObject / szReferencedObject; group anyway so a
    /// malformed catalog with mixed pairs is handled gracefully — and for
    /// each pair resolves the catalog entries, reads both TDEFs, sorts the
    /// rows by <c>icolumn</c>, resolves PK/FK column names to col_num, and
    /// invokes <paramref name="action"/>. Pairs whose tables or columns
    /// cannot be resolved are silently skipped (the caller still removes
    /// the catalog rows).
    /// </summary>
    /// <param name="matches">The matches.</param>
    /// <param name="action">The action.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask ForEachRelationshipFkPairAsync(
        List<RelationshipRowSnapshot> matches,
        Func<FkPairContext, CancellationToken, ValueTask> action,
        CancellationToken cancellationToken)
    {
        var byTablePair = new Dictionary<(string Pk, string Fk), List<RelationshipRowSnapshot>>(
            new TablePairComparer());
        foreach (RelationshipRowSnapshot row in matches)
        {
            (string Pk, string Fk) key = (row.SzReferencedObject, row.SzObject);
            if (!byTablePair.TryGetValue(key, out List<RelationshipRowSnapshot>? group))
            {
                group = [];
                byTablePair[key] = group;
            }

            group.Add(row);
        }

        foreach (KeyValuePair<(string Pk, string Fk), List<RelationshipRowSnapshot>> pair in byTablePair)
        {
            cancellationToken.ThrowIfCancellationRequested();

            CatalogEntry? pkEntry = await this.writer.GetCatalogEntryAsync(pair.Key.Pk, cancellationToken).ConfigureAwait(false);
            CatalogEntry? fkEntry = await this.writer.GetCatalogEntryAsync(pair.Key.Fk, cancellationToken).ConfigureAwait(false);
            if (pkEntry == null || fkEntry == null)
            {
                // Catalog row references a missing table — skip TDEF work.
                continue;
            }

            TableDef pkDef = await this.writer.ReadRequiredTableDefAsync(pkEntry.TDefPage, pair.Key.Pk, cancellationToken).ConfigureAwait(false);
            TableDef fkDef = await this.writer.ReadRequiredTableDefAsync(fkEntry.TDefPage, pair.Key.Fk, cancellationToken).ConfigureAwait(false);

            // Reconstruct the FK column list in icolumn order, then resolve
            // to col_num for col_map matching.
            var ordered = new List<RelationshipRowSnapshot>(pair.Value);
            ordered.Sort((a, b) => a.IColumn.CompareTo(b.IColumn));
            string[] pkColNames = new string[ordered.Count];
            string[] fkColNames = new string[ordered.Count];
            for (int i = 0; i < ordered.Count; i++)
            {
                pkColNames[i] = ordered[i].SzReferencedColumn;
                fkColNames[i] = ordered[i].SzColumn;
            }

            int[] pkColNums = pkDef.ResolveColNumsOrEmpty(pkColNames);
            int[] fkColNums = fkDef.ResolveColNumsOrEmpty(fkColNames);
            if (pkColNums.Length == 0 || fkColNums.Length == 0)
            {
                continue;
            }

            await action(
                new FkPairContext(pair.Key.Pk, pkEntry, pkDef, pkColNums, pair.Key.Fk, fkEntry, fkDef, fkColNums),
                cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class TablePairComparer : IEqualityComparer<(string Pk, string Fk)>
    {
        public bool Equals((string Pk, string Fk) x, (string Pk, string Fk) y) =>
            string.Equals(x.Pk, y.Pk, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Fk, y.Fk, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string Pk, string Fk) obj) =>
            HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Pk),
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Fk));
    }

    /// <summary>
    /// Locates a system or user table's TDEF page number by name (case-insensitive)
    /// by scanning every <c>MSysObjects</c> row. Returns <c>0</c> when not found.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<long> FindSystemTableTdefPageAsync(string tableName, CancellationToken cancellationToken)
        => this.catalog.FindSystemTableTdefPageAsync(tableName, cancellationToken);
}
