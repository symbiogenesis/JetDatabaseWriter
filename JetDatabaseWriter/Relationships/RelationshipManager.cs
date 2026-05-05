namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

#pragma warning disable CA1822 // Mark members as static
#pragma warning disable SA1202
#pragma warning disable SA1204
#pragma warning disable SA1648

/// <summary>
/// Foreign-key relationship management for <see cref="AccessWriter"/>:
/// MSysRelationships catalog rows, per-TDEF FK logical-index entries,
/// and runtime referential-integrity enforcement (insert/update/delete).
/// Owned by an <see cref="AccessWriter"/> instance via a private field;
/// AccessWriter exposes thin forwarders for the public CRUD entry points.
/// </summary>
internal sealed class RelationshipManager(AccessWriter writer)
{
    // ════════════════════════════════════════════════════════════════
    // Foreign-key relationships — MSysRelationships row emission
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
    /// Per <c>docs/design/index-and-relationship-format-notes.md</c> §7. The
    /// MSysRelationships catalog rows are what the Microsoft Access
    /// Relationships designer reads. The per-TDEF FK logical-index entries
    /// (<c>index_type = 0x02</c>, <c>rel_idx_num</c>, <c>rel_tbl_page</c>)
    /// that drive runtime referential-integrity enforcement by the JET
    /// engine are emitted by <see cref="EmitFkPerTdefEntriesAsync"/> on
    /// Jet4 / ACE; on Jet3 they are skipped and Microsoft Access regenerates
    /// them on the next Compact &amp; Repair pass.
    /// </remarks>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => writer.RunAutoCommitAsync(_ => CreateRelationshipCoreAsync(relationship, cancellationToken), cancellationToken);

    private async ValueTask CreateRelationshipCoreAsync(RelationshipDefinition relationship, CancellationToken cancellationToken)
    {
        Guard.NotNull(relationship, nameof(relationship));
        Guard.NotNullOrEmpty(relationship.Name, "relationship.Name");
        Guard.NotNullOrEmpty(relationship.PrimaryTable, "relationship.PrimaryTable");
        Guard.NotNullOrEmpty(relationship.ForeignTable, "relationship.ForeignTable");
        Guard.ThrowIfDisposed(writer._disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // Validate referenced user tables exist.
        CatalogEntry primaryEntry = await writer.GetRequiredCatalogEntryAsync(relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        CatalogEntry foreignEntry = await writer.GetRequiredCatalogEntryAsync(relationship.ForeignTable, cancellationToken).ConfigureAwait(false);

        // Validate referenced columns exist on each table.
        TableDef primaryDef = await writer.ReadRequiredTableDefAsync(primaryEntry.TDefPage, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        TableDef foreignDef = await writer.ReadRequiredTableDefAsync(foreignEntry.TDefPage, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);

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
        long msysRelTdefPage = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table. Databases freshly created by " +
                "AccessWriter.CreateDatabaseAsync do not include this catalog table; open or copy an " +
                "Access-authored database before calling CreateRelationshipAsync.");
        }

        TableDef msysRelDef = await writer.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

        // Reject duplicate relationship names (case-insensitive).
        HashSet<string> existingNames = await ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existingNames.Contains(relationship.Name))
        {
            throw new InvalidOperationException($"A relationship named '{relationship.Name}' already exists.");
        }

        // grbit flag bits — values per Jackcess com.healthmarketscience.jackcess.impl.RelationshipImpl.
        // (These are not yet documented in the format-probe appendix.)
        uint grbit = 0;
        if (!relationship.EnforceReferentialIntegrity)
        {
            grbit |= Constants.RelationshipFlags.NoRefIntegrity;
        }

        if (relationship.CascadeUpdates)
        {
            grbit |= Constants.RelationshipFlags.CascadeUpdates;
        }

        if (relationship.CascadeDeletes)
        {
            grbit |= Constants.RelationshipFlags.CascadeDeletes;
        }

        int ccolumn = relationship.PrimaryColumns.Count;
        int grbitInt = unchecked((int)grbit);

        // One row per FK column pair (per appendix §"MSysRelationships — TDEF page 5").
        for (int i = 0; i < ccolumn; i++)
        {
            object[] values = msysRelDef.CreateNullValueRow();

            msysRelDef.SetValueByName(values, "ccolumn", ccolumn);
            msysRelDef.SetValueByName(values, "grbit", grbitInt);
            msysRelDef.SetValueByName(values, "icolumn", i);
            msysRelDef.SetValueByName(values, "szColumn", relationship.ForeignColumns[i]);
            msysRelDef.SetValueByName(values, "szObject", relationship.ForeignTable);
            msysRelDef.SetValueByName(values, "szReferencedColumn", relationship.PrimaryColumns[i]);
            msysRelDef.SetValueByName(values, "szReferencedObject", relationship.PrimaryTable);
            msysRelDef.SetValueByName(values, "szRelationship", relationship.Name);

            await writer.InsertSystemRowAndMaintainAsync(msysRelTdefPage, msysRelDef, Constants.SystemTableNames.Relationships, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        // Per-TDEF FK logical-idx entries: add index_type=0x02 logical-idx
        // entries on both PK-side and FK-side TDEFs with cross-referenced
        // rel_idx_num / rel_tbl_page so the JET engine can locate the partner
        // table without waiting for Microsoft Access Compact & Repair to
        // regenerate them from the MSysRelationships rows above. See
        // docs/design/index-and-relationship-format-notes.md §7. Jet3 uses a
        // different (20-byte) logical-idx layout that this library does not
        // yet exercise — skip silently to keep the catalog row emission
        // working on .mdb (Access 97) databases.
        if (writer._format != DatabaseFormat.Jet3Mdb)
        {
            await EmitFkPerTdefEntriesAsync(
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
            TableDef primaryDefAfter = await writer.ReadRequiredTableDefAsync(primaryEntry.TDefPage, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
            await writer.MaintainIndexesAsync(primaryEntry.TDefPage, primaryDefAfter, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
            if (foreignEntry.TDefPage != primaryEntry.TDefPage)
            {
                TableDef foreignDefAfter = await writer.ReadRequiredTableDefAsync(foreignEntry.TDefPage, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
                await writer.MaintainIndexesAsync(foreignEntry.TDefPage, foreignDefAfter, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Per-TDEF FK logical-idx entries (Jet4 / ACE only)
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Pre-computed real-idx slot information for one side of a relationship.
    /// </summary>
    private readonly record struct FkSidePlan(int RealIdxNum, bool AllocatesNewRealIdx, long NewLeafPageNumber)
    {
        // RealIdxNum:           real-idx slot index used for index_num2 on this side.
        // AllocatesNewRealIdx:  true when a new real-idx slot must be appended.
        // NewLeafPageNumber:    pre-allocated empty leaf page (set when AllocatesNewRealIdx).
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
    private async ValueTask EmitFkPerTdefEntriesAsync(
        RelationshipDefinition relationship,
        long primaryTdefPage,
        TableDef primaryDef,
        long foreignTdefPage,
        TableDef foreignDef,
        CancellationToken cancellationToken)
    {
        // Resolve column numbers (deleted-column gaps mean ColNum != ordinal).
        var pkColNums = new int[relationship.PrimaryColumns.Count];
        var fkColNums = new int[relationship.ForeignColumns.Count];
        for (int i = 0; i < relationship.PrimaryColumns.Count; i++)
        {
            int pkIdx = primaryDef.FindColumnIndex(relationship.PrimaryColumns[i]);
            int fkIdx = foreignDef.FindColumnIndex(relationship.ForeignColumns[i]);
            pkColNums[i] = primaryDef.Columns[pkIdx].ColNum;
            fkColNums[i] = foreignDef.Columns[fkIdx].ColNum;
        }

        // Read both TDEF pages and decide each side's real-idx slot.
        (FkSidePlan pkPlan, List<string> pkExistingNames) = await PrepareFkSideAsync(primaryTdefPage, pkColNums, cancellationToken).ConfigureAwait(false);
        (FkSidePlan fkPlan, List<string> fkExistingNames) = await PrepareFkSideAsync(foreignTdefPage, fkColNums, cancellationToken).ConfigureAwait(false);

        // Allocate empty leaf pages for any newly-allocated real-idx slots.
        // Both leaf pages are appended before any TDEF mutation so the page
        // numbers are stable for the cross-referenced first_dp values.
        if (pkPlan.AllocatesNewRealIdx)
        {
            byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(writer._pgSz, primaryTdefPage, []);
            long lp = await writer.AppendPageAsync(leaf, cancellationToken).ConfigureAwait(false);
            pkPlan = pkPlan.WithLeafPage(lp);
        }

        if (fkPlan.AllocatesNewRealIdx)
        {
            byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(writer._pgSz, foreignTdefPage, []);
            long lp = await writer.AppendPageAsync(leaf, cancellationToken).ConfigureAwait(false);
            fkPlan = fkPlan.WithLeafPage(lp);
        }

        byte cascadeUpsByte = (byte)(relationship.CascadeUpdates ? 1 : 0);
        byte cascadeDelsByte = (byte)(relationship.CascadeDeletes ? 1 : 0);

        // Choose unique-within-tdef logical-idx names. The PK side uses the
        // relationship name; the FK side appends "_FK" to disambiguate when
        // both endpoints land on the same table (self-referential).
        string pkName = IndexHelpers.MakeUniqueLogicalIdxName(relationship.Name, pkExistingNames);
        string fkName = IndexHelpers.MakeUniqueLogicalIdxName(
            primaryTdefPage == foreignTdefPage ? relationship.Name + "_FK" : relationship.Name,
            fkExistingNames);

        // Emit both sides. PK side carries no cascade flags (cascade is an
        // FK-side property — Access only checks them when modifying the parent
        // and looking up children).
        await EmitFkLogicalIdxAsync(
            primaryTdefPage,
            pkColNums,
            pkName,
            realIdxNumThisSide: pkPlan.RealIdxNum,
            allocateNewRealIdx: pkPlan.AllocatesNewRealIdx,
            preAllocatedLeafPage: pkPlan.NewLeafPageNumber,
            relIdxNumOtherSide: fkPlan.RealIdxNum,
            relTblPageOther: foreignTdefPage,
            cascadeUps: 0,
            cascadeDels: 0,
            cancellationToken).ConfigureAwait(false);

        await EmitFkLogicalIdxAsync(
            foreignTdefPage,
            fkColNums,
            fkName,
            realIdxNumThisSide: fkPlan.RealIdxNum,
            allocateNewRealIdx: fkPlan.AllocatesNewRealIdx,
            preAllocatedLeafPage: fkPlan.NewLeafPageNumber,
            relIdxNumOtherSide: pkPlan.RealIdxNum,
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
    private async ValueTask<(FkSidePlan Plan, List<string> ExistingNames)> PrepareFkSideAsync(
        long tdefPage,
        int[] columnNumbers,
        CancellationToken cancellationToken)
    {
        byte[] page = await writer.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!TryParseFkTDefLayout(page, out FkTDefLayout layout))
            {
                throw new NotSupportedException(
                    $"TDEF at page {tdefPage} cannot be mutated in place (multi-page chain, malformed counts, or not a TDEF).");
            }

            int sharedSlot = FindCoveringRealIdx(page, columnNumbers, layout.RealIdxDescStart, layout.NumRealIdx);
            List<string> existingNames = ReadLogicalIdxNames(page, layout.LogIdxNamesStart, layout.NumIdx);

            FkSidePlan plan = sharedSlot >= 0
                ? new FkSidePlan(sharedSlot, false, 0)
                : new FkSidePlan(layout.NumRealIdx, true, 0);

            return (plan, existingNames);
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }
    }

    /// <summary>
    /// Appends one FK logical-idx entry (and optionally a new real-idx
    /// physical descriptor) to the TDEF at <paramref name="tdefPage"/>. The
    /// TDEF must fit on a single page after the addition.
    /// </summary>
    private async ValueTask EmitFkLogicalIdxAsync(
        long tdefPage,
        int[] columnNumbers,
        string indexName,
        int realIdxNumThisSide,
        bool allocateNewRealIdx,
        long preAllocatedLeafPage,
        int relIdxNumOtherSide,
        long relTblPageOther,
        byte cascadeUps,
        byte cascadeDels,
        CancellationToken cancellationToken)
    {
        byte[] td = await ReadPageOwnedAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        if (!TryParseFkTDefLayout(td, out FkTDefLayout layout))
        {
            throw new NotSupportedException(
                $"cannot mutate the TDEF at page {tdefPage} (multi-page chain, malformed counts, or not a TDEF).");
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

        int deltaRealIdxSkip = allocateNewRealIdx ? writer._tdef.RealIdxEntrySz : 0;
        int deltaRealIdxPhys = allocateNewRealIdx ? Constants.TableDefinition.Jet4.RealIdx.PhysSize : 0;
        int totalGrowth = deltaRealIdxSkip + deltaRealIdxPhys + Constants.TableDefinition.Jet4.LogicalIdx.EntrySize + nameRecordSize;

        if (currentEnd + totalGrowth > writer._pgSz)
        {
            throw new NotSupportedException(
                "TDEF would exceed a single page after adding a foreign-key index entry. " +
                "Multi-page TDEF growth is not supported.");
        }

        // Build the rewritten page.
        var newTd = new byte[writer._pgSz];
        Buffer.BlockCopy(td, 0, newTd, 0, writer._tdef.BlockEnd);

        // Real-idx skip block (existing slots, unchanged content).
        int oldRealIdxSkipLen = numRealIdx * writer._tdef.RealIdxEntrySz;
        Buffer.BlockCopy(td, writer._tdef.BlockEnd, newTd, writer._tdef.BlockEnd, oldRealIdxSkipLen);
        int newRealIdxSkipEnd = writer._tdef.BlockEnd + oldRealIdxSkipLen + deltaRealIdxSkip;

        // Column descriptors.
        int oldColStart = writer._tdef.BlockEnd + oldRealIdxSkipLen;
        int colDescBlockLen = numCols * writer._colDesc.Size;
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
        if (allocateNewRealIdx)
        {
            int phys = newRealIdxDescStart + oldRealIdxPhysLen;

            // bytes 0..3   Jet4/ACE format magic cookie (0x00000659). DAO's
            //              TDEF validation checks this during CompactDatabase;
            //              leaving it zero causes err 3011 "MSysDb".
            AccessBase.Wi32(newTd, phys, Constants.TableDefinition.Jet4.FormatMagic);

            // bytes 4..33  col_map: 10 × {col_num(2), col_order(1)}
            for (int slot = 0; slot < 10; slot++)
            {
                int so = phys + 4 + (slot * 3);
                if (slot < columnNumbers.Length)
                {
                    AccessBase.Wu16(newTd, so, columnNumbers[slot]);
                    newTd[so + 2] = 0x01; // ascending
                }
                else
                {
                    AccessBase.Wu16(newTd, so, 0xFFFF);
                    newTd[so + 2] = 0x00;
                }
            }

            // bytes 34..37 used_pages = 0 (no usage bitmap emitted)
            // bytes 38..41 first_dp = preAllocatedLeafPage
            AccessBase.Wi32(newTd, phys + 38, checked((int)preAllocatedLeafPage));

            // bytes 42..45 unknown(4) = 0
            // byte  46     flags: 0x80 (unknown-flag bit always set per Jackcess)
            // bytes 47..51 unknown(5) = 0
            newTd[phys + Constants.TableDefinition.Jet4.RealIdx.FlagsOffset] =
                Constants.TableDefinition.UnknownIndexFlag;
        }

        // Logical-idx entries (existing).
        int newLogIdxStart = newRealIdxDescStart + oldRealIdxPhysLen + deltaRealIdxPhys;
        int oldLogIdxLen = numIdx * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize;
        Buffer.BlockCopy(td, logIdxStart, newTd, newLogIdxStart, oldLogIdxLen);

        // Append the new FK logical-idx entry.
        // bytes 0..3   Jet4/ACE format magic cookie (0x00000659). DAO checks
        //              this during CompactDatabase.
        // bytes 24..27 trailing(4) = 0
        int newLogEntry = newLogIdxStart + oldLogIdxLen;
        AccessBase.Wi32(newTd, newLogEntry, Constants.TableDefinition.Jet4.FormatMagic);
        AccessBase.Wi32(newTd, newLogEntry + 4, numIdx);                  // index_num (next sequential)
        AccessBase.Wi32(newTd, newLogEntry + 8, realIdxNumThisSide);      // index_num2
        newTd[newLogEntry + 12] = 0x01;                        // rel_tbl_type — empirical: 0x01 on FK entries (appendix §"Companies")
        AccessBase.Wi32(newTd, newLogEntry + 13, relIdxNumOtherSide);     // rel_idx_num — slot on the OTHER table
        AccessBase.Wi32(newTd, newLogEntry + 17, checked((int)relTblPageOther)); // rel_tbl_page
        newTd[newLogEntry + 21] = cascadeUps;
        newTd[newLogEntry + 22] = cascadeDels;
        newTd[newLogEntry + 23] = 0x02;                        // index_type = FK

        // Logical-idx names (existing).
        int newLogIdxNamesStart = newLogEntry + Constants.TableDefinition.Jet4.LogicalIdx.EntrySize;
        Buffer.BlockCopy(td, logIdxNamesStart, newTd, newLogIdxNamesStart, logIdxNamesLen);

        // Append the new logical-idx name (UTF-16 length-prefixed).
        int newNameOffset = newLogIdxNamesStart + logIdxNamesLen;
        AccessBase.Wu16(newTd, newNameOffset, nameBytes.Length);
        Buffer.BlockCopy(nameBytes, 0, newTd, newNameOffset + 2, nameBytes.Length);

        // Trailing variable-length-column block (Access-emitted TDEFs only).
        int newTrailingStart = newNameOffset + nameRecordSize;
        if (trailingLen > 0)
        {
            Buffer.BlockCopy(td, trailingStart, newTd, newTrailingStart, trailingLen);
        }

        // Update header counts.
        AccessBase.Wi32(newTd, writer._tdef.NumCols + 2, numIdx + 1);
        if (allocateNewRealIdx)
        {
            AccessBase.Wi32(newTd, writer._tdef.NumRealIdx, numRealIdx + 1);
        }

        // tdef_len at offset 8 = (newEnd - 8). The page header (8 bytes) is
        // not counted in tdef_len, matching BuildTDefPageWithIndexOffsets.
        AccessBase.Wi32(newTd, 8, newTrailingStart + trailingLen - 8);

        await writer.WritePageAsync(tdefPage, newTd, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Walks past column descriptors and column names to return the byte
    /// offset of the real-index physical-descriptor section start, or -1
    /// when the column-name walk fails.
    /// </summary>
    internal int LocateRealIdxDescStart(byte[] td, int numCols, int numRealIdx)
    {
        int colStart = writer._tdef.BlockEnd + (numRealIdx * writer._tdef.RealIdxEntrySz);
        int pos = colStart + (numCols * writer._colDesc.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (writer.ReadColumnName(td, ref pos, out _) < 0)
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
    private int MeasureLogicalIdxNamesLength(byte[] td, int logIdxNamesStart, int numIdx)
    {
        int pos = logIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (writer.ReadColumnName(td, ref pos, out _) < 0)
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
    internal List<string> ReadLogicalIdxNames(byte[] td, int logIdxNamesStart, int numIdx)
    {
        var list = new List<string>(numIdx);
        int pos = logIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            if (writer.ReadColumnName(td, ref pos, out string n) < 0)
            {
                break;
            }

            list.Add(n);
        }

        return list;
    }

    /// <summary>
    /// Real-idx sharing per §3.3: returns the existing real-idx slot whose col_map
    /// matches <paramref name="columnNumbers"/> exactly (in declaration
    /// order); -1 when no covering real-idx exists. Jet4 col_map is fixed at
    /// 10 slots × {col_num(2), col_order(1)}.
    /// </summary>
    private int FindCoveringRealIdx(byte[] td, int[] columnNumbers, int realIdxDescStart, int numRealIdx)
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

    // ════════════════════════════════════════════════════════════════
    // Drop / Rename relationship
    // ════════════════════════════════════════════════════════════════
    //
    // Reverses CreateRelationshipAsync:
    //   • DropRelationshipAsync removes every MSysRelationships row whose
    //     szRelationship matches and (Jet4/ACE) removes the matching FK
    //     logical-idx entry from each side's TDEF, then conservatively
    //     reclaims any trailing real-idx physical-descriptor slots that the
    //     removal left unreferenced (common case: FK got the last slot on
    //     its TDEF and the slot is reclaimed cleanly; non-trailing orphans
    //     are still left for Compact & Repair to reclaim, since mid-array
    //     compaction would require cross-TDEF rel_idx_num renumbering on
    //     every other table that points at the slot).
    //     ListIndexesAsync iterates by num_idx so the FK stops surfacing
    //     immediately regardless of whether the real-idx slot was reclaimed.
    //   • RenameRelationshipAsync rewrites the szRelationship column on
    //     every matching MSysRelationships row (read all 8 columns, mark
    //     deleted, re-insert with the new name and updateTDefRowCount=false)
    //     and (Jet4/ACE) updates the matching FK logical-idx name cookie on
    //     each side's TDEF in place. Variable-length name records:
    //     shrink/grow shifts the trailing variable-column block; a grow
    //     that would push the TDEF past one page leaves the cookie
    //     unchanged (Access regenerates it from the catalog row on the
    //     next Compact & Repair pass).

    /// <summary>
    /// Snapshot of one MSysRelationships row. <c>RowValues</c> mirrors the
    /// MSysRelationships column order so it can be passed directly to
    /// <see cref="AccessWriter.InsertRowDataAsync"/> on the re-insert path used by
    /// <see cref="RenameRelationshipAsync"/>.
    /// </summary>
    private sealed record RelationshipRowSnapshot(
        RowLocation Location,
        string SzRelationship,
        string SzObject,
        string SzReferencedObject,
        string SzColumn,
        string SzReferencedColumn,
        int IColumn,
        int CColumn,
        int Grbit,
        object[] RowValues);

    /// <inheritdoc />
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
        => writer.RunAutoCommitAsync(_ => DropRelationshipCoreAsync(relationshipName, cancellationToken), cancellationToken);

    private async ValueTask DropRelationshipCoreAsync(string relationshipName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(relationshipName, nameof(relationshipName));
        Guard.ThrowIfDisposed(writer._disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        long msysRelTdefPage = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to drop.");
        }

        TableDef msysRelDef = await writer.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        List<RelationshipRowSnapshot> matches = await CollectRelationshipRowsAsync(
            msysRelTdefPage,
            msysRelDef,
            r => string.Equals(r, relationshipName, StringComparison.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);

        if (matches.Count == 0)
        {
            throw new InvalidOperationException($"No relationship named '{relationshipName}' was found.");
        }

        // Jet4/ACE only — Jet3 never received the per-TDEF FK logical-idx entries.
        if (writer._format != DatabaseFormat.Jet3Mdb)
        {
            await ForEachRelationshipFkPairAsync(
                matches,
                async (ctx, ct) =>
                {
                    // Remove the matching FK logical-idx entry from each side.
                    // Self-referential relationships (PK and FK on same TDEF) need
                    // both removals to target distinct entries — pass the column
                    // list to disambiguate.
                    int pkReleased = await TryRemoveFkLogicalIdxEntryAsync(ctx.PkEntry.TDefPage, ctx.PkColNums, ctx.FkEntry.TDefPage, ct).ConfigureAwait(false);
                    int fkReleased = await TryRemoveFkLogicalIdxEntryAsync(ctx.FkEntry.TDefPage, ctx.FkColNums, ctx.PkEntry.TDefPage, ct).ConfigureAwait(false);

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
                        await TryReclaimTrailingRealIdxAsync(ctx.PkEntry.TDefPage, ct).ConfigureAwait(false);
                    }

                    if (fkReleased >= 0 && ctx.PkEntry.TDefPage != ctx.FkEntry.TDefPage)
                    {
                        await TryReclaimTrailingRealIdxAsync(ctx.FkEntry.TDefPage, ct).ConfigureAwait(false);
                    }
                },
                cancellationToken).ConfigureAwait(false);
        }

        // Mark catalog rows deleted and adjust the row count.
        foreach (RelationshipRowSnapshot row in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await writer.MarkRowDeletedAsync(row.Location.PageNumber, row.Location.RowIndex, cancellationToken).ConfigureAwait(false);
        }

        await writer.AdjustTDefRowCountAsync(msysRelTdefPage, -matches.Count, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
        => writer.RunAutoCommitAsync(_ => RenameRelationshipCoreAsync(oldName, newName, cancellationToken), cancellationToken);

    private async ValueTask RenameRelationshipCoreAsync(string oldName, string newName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(oldName, nameof(oldName));
        Guard.NotNullOrEmpty(newName, nameof(newName));
        Guard.ThrowIfDisposed(writer._disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (string.Equals(oldName, newName, StringComparison.OrdinalIgnoreCase))
        {
            return; // No-op; matches Microsoft Access' designer behaviour.
        }

        long msysRelTdefPage = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysRelationships' table; nothing to rename.");
        }

        TableDef msysRelDef = await writer.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

        // Reject collision with an existing name (case-insensitive).
        HashSet<string> existing = await ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existing.Contains(newName))
        {
            throw new InvalidOperationException($"A relationship named '{newName}' already exists.");
        }

        List<RelationshipRowSnapshot> matches = await CollectRelationshipRowsAsync(
            msysRelTdefPage,
            msysRelDef,
            r => string.Equals(r, oldName, StringComparison.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);

        if (matches.Count == 0)
        {
            throw new InvalidOperationException($"No relationship named '{oldName}' was found.");
        }

        int szRelIdx = msysRelDef.FindColumnIndex("szRelationship");
        if (szRelIdx < 0)
        {
            throw new InvalidOperationException("MSysRelationships does not expose a 'szRelationship' column.");
        }

        foreach (RelationshipRowSnapshot row in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object[] rowValues = (object[])row.RowValues.Clone();
            rowValues[szRelIdx] = newName;

            await writer.MarkRowDeletedAsync(row.Location.PageNumber, row.Location.RowIndex, cancellationToken).ConfigureAwait(false);
            await writer.InsertSystemRowAndMaintainAsync(msysRelTdefPage, msysRelDef, Constants.SystemTableNames.Relationships, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        // Update the TDEF logical-idx name cookies on both sides so the
        // on-disk index name matches the catalog row. Jet3 never received
        // FK logical-idx entries, so this is a no-op there.
        if (writer._format != DatabaseFormat.Jet3Mdb)
        {
            await ForEachRelationshipFkPairAsync(
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

                    string newPkName = await PickUniqueLogicalIdxNameAsync(ctx.PkEntry.TDefPage, newPkBase, ct).ConfigureAwait(false);
                    _ = await TryRenameFkLogicalIdxNameAsync(ctx.PkEntry.TDefPage, ctx.PkColNums, ctx.FkEntry.TDefPage, newPkName, ct).ConfigureAwait(false);

                    string newFkName = await PickUniqueLogicalIdxNameAsync(ctx.FkEntry.TDefPage, newFkBase, ct).ConfigureAwait(false);
                    _ = await TryRenameFkLogicalIdxNameAsync(ctx.FkEntry.TDefPage, ctx.FkColNums, ctx.PkEntry.TDefPage, newFkName, ct).ConfigureAwait(false);
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
    private async ValueTask<string> PickUniqueLogicalIdxNameAsync(
        long tdefPage,
        string baseName,
        CancellationToken cancellationToken)
    {
        byte[] pageBytes = await writer.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!TryParseFkTDefLayout(pageBytes, out FkTDefLayout layout) || layout.NumIdx <= 0)
            {
                return baseName;
            }

            List<string> existing = ReadLogicalIdxNames(pageBytes, layout.LogIdxNamesStart, layout.NumIdx);
            return IndexHelpers.MakeUniqueLogicalIdxName(baseName, existing);
        }
        finally
        {
            AccessBase.ReturnPage(pageBytes);
        }
    }

    /// <summary>
    /// Walks every live row in <c>MSysRelationships</c>, materialising each
    /// row whose <c>szRelationship</c> satisfies <paramref name="namePredicate"/>
    /// into a <see cref="RelationshipRowSnapshot"/> (location + all 8 column
    /// values in MSysRelationships column order).
    /// </summary>
    private async ValueTask<List<RelationshipRowSnapshot>> CollectRelationshipRowsAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        Func<string, bool> namePredicate,
        CancellationToken cancellationToken)
    {
        var results = new List<RelationshipRowSnapshot>();
        ColumnInfo? nameCol = msysRelDef.FindColumn("szRelationship");
        ColumnInfo? objCol = msysRelDef.FindColumn("szObject");
        ColumnInfo? refObjCol = msysRelDef.FindColumn("szReferencedObject");
        ColumnInfo? colCol = msysRelDef.FindColumn("szColumn");
        ColumnInfo? refColCol = msysRelDef.FindColumn("szReferencedColumn");
        ColumnInfo? icolCol = msysRelDef.FindColumn("icolumn");
        ColumnInfo? ccolCol = msysRelDef.FindColumn("ccolumn");
        ColumnInfo? grbitCol = msysRelDef.FindColumn("grbit");
        if (nameCol == null || objCol == null || refObjCol == null || colCol == null
            || refColCol == null || icolCol == null || ccolCol == null || grbitCol == null)
        {
            return results;
        }

        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, writer._dataPage.TDefOff) != msysRelTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string name = writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (string.IsNullOrEmpty(name) || !namePredicate(name))
                    {
                        continue;
                    }

                    var values = new object[msysRelDef.Columns.Count];
                    for (int c = 0; c < values.Length; c++)
                    {
                        ColumnInfo col = msysRelDef.Columns[c];
                        string raw = writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, col);
                        values[c] = string.IsNullOrEmpty(raw)
                            ? DBNull.Value
                            : col.Type switch
                            {
                                T_LONG => writer.ParseInt32(raw),
                                T_INT => (short)writer.ParseInt32(raw),
                                T_BYTE => (byte)writer.ParseInt32(raw),
                                _ => raw,
                            };
                    }

                    results.Add(new RelationshipRowSnapshot(
                        row,
                        name,
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, objCol),
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, refObjCol),
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, colCol),
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, refColCol),
                        writer.ParseInt32(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, icolCol)),
                        writer.ParseInt32(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, ccolCol)),
                        writer.ParseInt32(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, grbitCol)),
                        values));
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return results;
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
    private async ValueTask<int> TryRemoveFkLogicalIdxEntryAsync(
        long tdefPage,
        int[] columnNumbers,
        long otherTdefPage,
        CancellationToken cancellationToken)
    {
        byte[] td = await ReadPageOwnedAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (!TryParseFkTDefLayout(td, out FkTDefLayout layout) || layout.NumIdx <= 0 || layout.NumRealIdx <= 0)
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

        if (!TryGetLogicalIdxNameRange(td, in layout, matchEntryIdx, out int removedNameStart, out int removedNameLen))
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
        AccessBase.Wi32(td, writer._tdef.NumCols + 2, layout.NumIdx - 1);
        AccessBase.Wi32(td, 8, finalEnd - 8);

        await writer.WritePageAsync(tdefPage, td, cancellationToken).ConfigureAwait(false);
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
    private async ValueTask TryReclaimTrailingRealIdxAsync(
        long tdefPage,
        CancellationToken cancellationToken)
    {
        byte[] td = await ReadPageOwnedAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (!TryParseFkTDefLayout(td, out FkTDefLayout layout) || layout.NumRealIdx <= 0)
        {
            return;
        }

        // Build the set of real-idx slots that are still referenced by some
        // logical-idx entry. A logical-idx points at one real-idx via
        // index_num2 (offset +8 in the 28-byte Jet4 entry).
        var referenced = new bool[layout.NumRealIdx];
        for (int li = 0; li < layout.NumIdx; li++)
        {
            int e = layout.LogIdxStart + (li * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
            int realIdxNum = AccessBase.Ri32(td, e + 8);
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
        int oldSkipEnd = writer._tdef.BlockEnd + (layout.NumRealIdx * writer._tdef.RealIdxEntrySz);
        int newSkipEnd = oldSkipEnd - (reclaim * writer._tdef.RealIdxEntrySz);
        Buffer.BlockCopy(td, oldSkipEnd, td, newSkipEnd, layout.CurrentEnd - oldSkipEnd);
        int endAfterStep1 = layout.CurrentEnd - (reclaim * writer._tdef.RealIdxEntrySz);

        // After step 1 the real-idx physical descriptor section starts at
        // (realIdxDescStart - reclaim * _writer._tdef.RealIdxEntrySz). We need to drop the
        // trailing N × 52 bytes of physical descriptors. Compute the new
        // boundaries.
        int newRealIdxDescStart = layout.RealIdxDescStart - (reclaim * writer._tdef.RealIdxEntrySz);
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
        AccessBase.Wi32(td, writer._tdef.NumRealIdx, layout.NumRealIdx - reclaim);
        AccessBase.Wi32(td, 8, finalEnd - 8);

        await writer.WritePageAsync(tdefPage, td, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Renames the FK logical-idx "name cookie" on <paramref name="tdefPage"/>
    /// for the entry whose backing real-idx col_map exactly covers
    /// <paramref name="columnNumbers"/> AND whose <c>rel_tbl_page</c> equals
    /// <paramref name="otherTdefPage"/>. Returns <see langword="true"/> when
    /// an entry was found and renamed; <see langword="false"/> otherwise
    /// (already renamed, never created — Jet3, multi-page TDEF, or out-of-band
    /// catalog). Variable-length name records: shrink/grow is handled by
    /// shifting the trailing variable-column block; growth that would push the
    /// TDEF past one page returns <see langword="false"/>.
    /// </summary>
    private async ValueTask<bool> TryRenameFkLogicalIdxNameAsync(
        long tdefPage,
        int[] columnNumbers,
        long otherTdefPage,
        string newName,
        CancellationToken cancellationToken)
    {
        byte[] td = await ReadPageOwnedAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (!TryParseFkTDefLayout(td, out FkTDefLayout layout) || layout.NumIdx <= 0 || layout.NumRealIdx <= 0)
        {
            return false;
        }

        int matchEntryIdx = FindFkLogicalIdxEntry(td, in layout, columnNumbers, otherTdefPage, out _);
        if (matchEntryIdx < 0)
        {
            return false;
        }

        if (!TryGetLogicalIdxNameRange(td, in layout, matchEntryIdx, out int oldNameStart, out int oldNameLen))
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
        if (finalEnd > writer._pgSz || finalEnd < layout.TrailingStart)
        {
            return false;
        }

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
        AccessBase.Wu16(td, oldNameStart, newNameBytes.Length);
        Buffer.BlockCopy(newNameBytes, 0, td, oldNameStart + 2, newNameBytes.Length);

        // Update tdef_len.
        AccessBase.Wi32(td, 8, finalEnd - 8);

        await writer.WritePageAsync(tdefPage, td, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Parsed layout of a single-page Jet4/ACE TDEF, used by the FK
    /// logical-idx mutation helpers (rename / remove / reclaim) to share the
    /// header validation and offset-computation boilerplate. Returns
    /// <see langword="false"/> from <see cref="TryParseFkTDefLayout"/> for
    /// multi-page TDEFs, malformed counts, or a column-name walk failure.
    /// </summary>
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
    /// Validates that <paramref name="td"/> is a single-page Jet4/ACE TDEF
    /// with sane counts and computes every offset required by the FK
    /// mutation helpers in one pass. Returns <see langword="false"/> when
    /// the buffer is not a TDEF, is multi-page, has out-of-range counts, or
    /// the column-name / idx-name walk fails.
    /// </summary>
    private bool TryParseFkTDefLayout(byte[] td, out FkTDefLayout layout)
    {
        layout = default;
        if (td[0] != 0x02 || AccessBase.Ru32(td, 4) != 0)
        {
            // Multi-page TDEF — out of scope for in-place mutation.
            return false;
        }

        int numCols = AccessBase.Ru16(td, writer._tdef.NumCols);
        int numIdx = AccessBase.Ri32(td, writer._tdef.NumCols + 2);
        int numRealIdx = AccessBase.Ri32(td, writer._tdef.NumRealIdx);
        if (numCols < 0 || numCols > 4096
            || numIdx < 0 || numIdx > 1000
            || numRealIdx < 0 || numRealIdx > 1000)
        {
            return false;
        }

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return false;
        }

        int logIdxStart = realIdxDescStart + (numRealIdx * Constants.TableDefinition.Jet4.RealIdx.PhysSize);
        int logIdxNamesStart = logIdxStart + (numIdx * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
        int logIdxNamesLen = MeasureLogicalIdxNamesLength(td, logIdxNamesStart, numIdx);
        if (logIdxNamesLen < 0)
        {
            return false;
        }

        int trailingStart = logIdxNamesStart + logIdxNamesLen;
        int storedTdefLen = AccessBase.Ri32(td, 8);
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
    private int FindFkLogicalIdxEntry(
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
            byte indexType = td[e + 23];
            if (indexType != 0x02)
            {
                continue;
            }

            int relTblPage = AccessBase.Ri32(td, e + 17);
            if (relTblPage != otherTdefPage)
            {
                continue;
            }

            int rin = AccessBase.Ri32(td, e + 8);
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
            if (writer.ReadColumnName(td, ref namePos, out _) < 0)
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

            CatalogEntry? pkEntry = await writer.GetCatalogEntryAsync(pair.Key.Pk, cancellationToken).ConfigureAwait(false);
            CatalogEntry? fkEntry = await writer.GetCatalogEntryAsync(pair.Key.Fk, cancellationToken).ConfigureAwait(false);
            if (pkEntry == null || fkEntry == null)
            {
                // Catalog row references a missing table — skip TDEF work.
                continue;
            }

            TableDef pkDef = await writer.ReadRequiredTableDefAsync(pkEntry.TDefPage, pair.Key.Pk, cancellationToken).ConfigureAwait(false);
            TableDef fkDef = await writer.ReadRequiredTableDefAsync(fkEntry.TDefPage, pair.Key.Fk, cancellationToken).ConfigureAwait(false);

            // Reconstruct the FK column list in icolumn order, then resolve
            // to col_num for col_map matching.
            var ordered = new List<RelationshipRowSnapshot>(pair.Value);
            ordered.Sort((a, b) => a.IColumn.CompareTo(b.IColumn));
            var pkColNames = new string[ordered.Count];
            var fkColNames = new string[ordered.Count];
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

    // ════════════════════════════════════════════════════════════════
    // Foreign-key runtime enforcement on Insert / Update / Delete
    // ════════════════════════════════════════════════════════════════
    //
    // Honors the EnforceReferentialIntegrity / CascadeUpdates / CascadeDeletes
    // flags emitted into MSysRelationships.grbit by CreateRelationshipAsync.
    // When a relationship has EnforceReferentialIntegrity=false the row
    // simply does not enter the enforced-set returned by
    // GetEnforcedRelationshipsAsync.
    //
    // Enforcement strategy is intentionally simple — there is no SQL engine
    // and no index seek; every check scans the relevant table snapshot once
    // per public mutation call (NOT once per row). For bulk InsertRowsAsync
    // the parent-key set is built lazily on first FK violation check and
    // reused across all rows in the same call. Self-referential inserts
    // augment the cached set after each successful insert so a row that
    // satisfies its own FK can be inserted.
    //
    // See docs/design/index-and-relationship-format-notes.md §7.

    /// <summary>
    /// In-memory representation of a single enforced foreign-key relationship,
    /// aggregated from one or more <c>MSysRelationships</c> rows (one per
    /// FK column). Only relationships with the
    /// <c>NO_REFERENTIAL_INTEGRITY</c> grbit flag clear are returned.
    /// </summary>
    internal sealed record FkRelationship(
        string Name,
        string PrimaryTable,
        IReadOnlyList<string> PrimaryColumns,
        string ForeignTable,
        IReadOnlyList<string> ForeignColumns,
        bool CascadeUpdates,
        bool CascadeDeletes);

    /// <summary>
    /// Per-call enforcement context: snapshot of every enforced relationship
    /// plus a lazy cache of parent-PK key sets keyed by relationship name.
    /// One context lives for the duration of a single public mutation call
    /// (Insert/Update/Delete) so the parent snapshot of a given table is
    /// loaded at most once per call even when many rows need checking.
    /// </summary>
    internal sealed class FkContext(IReadOnlyList<FkRelationship> all)
    {
        public IReadOnlyList<FkRelationship> All { get; } = all;

        public Dictionary<string, HashSet<string>> ParentKeySets { get; }
            = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Gets the per-relationship cached resolution of the parent table's
        /// PK / FK index used by <see cref="EnforceFkOnInsertAsync"/> to
        /// perform O(log N) seeks instead of a full parent snapshot scan. A
        /// value of <see langword="null"/> means "resolution attempted and
        /// the index was not usable" (Jet3 format, no covering real-idx,
        /// unsupported key type) — the enforcement loop falls back to the
        /// legacy HashSet-of-keys path in that case.
        /// </summary>
        public Dictionary<string, ParentSeekIndex?> SeekIndexes { get; }
            = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Gets the per-relationship cached resolution of the child (FK)
        /// table's real-idx covering the relationship's foreign columns.
        /// Used by <see cref="EnforceFkOnPrimaryDeleteAsync"/> /
        /// <see cref="EnforceFkOnPrimaryUpdateAsync"/> to locate dependent
        /// child rows by parent-key seek instead of an O(N) child snapshot
        /// scan. A value of <see langword="null"/> means "resolution
        /// attempted and the index is not usable" — caller falls back to
        /// the snapshot path.
        /// </summary>
        public Dictionary<string, ChildSeekIndex?> ChildSeekIndexes { get; }
            = new(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Loads every enforced foreign-key relationship from the
    /// <c>MSysRelationships</c> system table. Returns an empty list when the
    /// database does not contain that table or contains no enforced rows.
    /// </summary>
    internal async ValueTask<IReadOnlyList<FkRelationship>> GetEnforcedRelationshipsAsync(CancellationToken cancellationToken)
    {
        long pg = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (pg == 0)
        {
            return [];
        }

        DataTable t = await writer.ReadTableSnapshotAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!t.Columns.Contains("szRelationship"))
            {
                return [];
            }

            var groups = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow r in t.Rows)
            {
                object nm = r["szRelationship"];
                if (nm == DBNull.Value)
                {
                    continue;
                }

                string name = nm.ToString() ?? string.Empty;
                if (name.Length == 0)
                {
                    continue;
                }

                if (!groups.TryGetValue(name, out List<DataRow>? list))
                {
                    list = [];
                    groups[name] = list;
                }

                list.Add(r);
            }

            var result = new List<FkRelationship>(groups.Count);
            foreach (KeyValuePair<string, List<DataRow>> kvp in groups)
            {
                List<DataRow> rows = kvp.Value;
                rows.Sort((a, b) => Convert.ToInt32(a["icolumn"], CultureInfo.InvariantCulture)
                    .CompareTo(Convert.ToInt32(b["icolumn"], CultureInfo.InvariantCulture)));

                DataRow head = rows[0];
                int grbit = Convert.ToInt32(head["grbit"], CultureInfo.InvariantCulture);

                // grbit bits (Jackcess RelationshipImpl):
                //   0x00000002 NO_REFERENTIAL_INTEGRITY
                //   0x00000100 CASCADE_UPDATES
                //   0x00001000 CASCADE_DELETES
                bool enforce = (grbit & Constants.RelationshipFlags.NoRefIntegrity) == 0;
                if (!enforce)
                {
                    continue;
                }

                bool cascadeUpdates = (grbit & Constants.RelationshipFlags.CascadeUpdates) != 0;
                bool cascadeDeletes = (grbit & Constants.RelationshipFlags.CascadeDeletes) != 0;

                string primaryTable = head["szReferencedObject"]?.ToString() ?? string.Empty;
                string foreignTable = head["szObject"]?.ToString() ?? string.Empty;
                if (primaryTable.Length == 0 || foreignTable.Length == 0)
                {
                    continue;
                }

                var pk = new string[rows.Count];
                var fk = new string[rows.Count];
                for (int i = 0; i < rows.Count; i++)
                {
                    pk[i] = rows[i]["szReferencedColumn"]?.ToString() ?? string.Empty;
                    fk[i] = rows[i]["szColumn"]?.ToString() ?? string.Empty;
                }

                result.Add(new FkRelationship(kvp.Key, primaryTable, pk, foreignTable, fk, cascadeUpdates, cascadeDeletes));
            }

            return result;
        }
        finally
        {
            t.Dispose();
        }
    }

    /// <summary>
    /// Lazily builds (and caches inside <paramref name="ctx"/>) the set of
    /// composite-key strings for every row currently in <paramref name="rel"/>'s
    /// primary table. A relationship with a missing parent column or a missing
    /// parent table yields an empty set so all FK inserts will be rejected.
    /// </summary>
    private async ValueTask<HashSet<string>> GetParentKeySetAsync(FkRelationship rel, FkContext ctx, CancellationToken cancellationToken)
    {
        if (ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? cached))
        {
            return cached;
        }

        var set = new HashSet<string>(StringComparer.Ordinal);

        DataTable parent;
        try
        {
            parent = await writer.ReadTableSnapshotAsync(rel.PrimaryTable, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            ctx.ParentKeySets[rel.Name] = set;
            return set;
        }

        try
        {
            var idx = new int[rel.PrimaryColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.PrimaryColumns.Count; i++)
            {
                idx[i] = parent.Columns.IndexOf(rel.PrimaryColumns[i]);
                if (idx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (ok)
            {
                foreach (DataRow row in parent.Rows)
                {
                    string? k = IndexHelpers.BuildCompositeKey(row.ItemArray, idx);
                    if (k != null)
                    {
                        _ = set.Add(k);
                    }
                }
            }
        }
        finally
        {
            parent.Dispose();
        }

        ctx.ParentKeySets[rel.Name] = set;
        return set;
    }

    /// <summary>
    /// Validates that every enforced FK whose foreign-side table is
    /// <paramref name="foreignTable"/> is satisfied by <paramref name="values"/>.
    /// Throws <see cref="InvalidOperationException"/> on the first violation.
    /// A FK column tuple containing any null component is allowed (Access
    /// permits unset FKs unless the column itself is required).
    /// </summary>
    internal async ValueTask EnforceFkOnInsertAsync(string foreignTable, TableDef foreignDef, object[] values, FkContext ctx, CancellationToken cancellationToken)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.ForeignTable, foreignTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var fkIdx = new int[rel.ForeignColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.ForeignColumns.Count; i++)
            {
                fkIdx[i] = foreignDef.FindColumnIndex(rel.ForeignColumns[i]);
                if (fkIdx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            string? key = IndexHelpers.BuildCompositeKey(values, fkIdx);
            if (key == null)
            {
                continue;
            }

            // Try the O(log N) parent-seek path first: locate (and cache) the
            // parent's PK / FK index that backs this relationship and seek
            // the encoded composite key in its B-tree. Falls back to the
            // legacy HashSet path when the index is missing (Jet3, schema
            // mismatch) or its key column types are not supported by the
            // sort-key encoder.
            ParentSeekIndex? seekIdx = await ResolveParentSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (seekIdx != null)
            {
                if (!ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? pendingSet))
                {
                    pendingSet = new HashSet<string>(StringComparer.Ordinal);
                    ctx.ParentKeySets[rel.Name] = pendingSet;
                }

                if (pendingSet.Contains(key))
                {
                    continue;
                }

                byte[]? encodedKey = IndexHelpers.TryEncodeSeekKey(seekIdx, values);
                if (encodedKey != null)
                {
                    bool found = await IndexBTreeSeeker.ContainsKeyAsync(
                        (page, ct) => ReadPageOwnedAsync(page, ct),
                        writer._pgSz,
                        seekIdx.RootPage,
                        encodedKey,
                        cancellationToken).ConfigureAwait(false);

                    if (found)
                    {
                        continue;
                    }

                    throw new InvalidOperationException(
                        $"INSERT into '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                        $"no matching row in '{rel.PrimaryTable}' for the supplied {string.Join(", ", rel.ForeignColumns)} value(s).");
                }

                // Encoder rejected (e.g. text outside General Legacy, GUID
                // mismatch, …) — fall through to the HashSet path below.
            }

            HashSet<string> parentKeys = await GetParentKeySetAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (!parentKeys.Contains(key))
            {
                throw new InvalidOperationException(
                    $"INSERT into '{foreignTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"no matching row in '{rel.PrimaryTable}' for the supplied {string.Join(", ", rel.ForeignColumns)} value(s).");
            }
        }
    }

    /// <summary>
    /// Page-read adapter that returns a copy of the page bytes (so the
    /// seeker can hold the buffer past the read; the writer's page pool
    /// recycles the original buffer immediately).
    /// </summary>
    private async ValueTask<byte[]> ReadPageOwnedAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])page.Clone();
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }
    }

    /// <summary>
    /// Resolved seek-index core for one side of a relationship: the
    /// covering real-idx's <c>first_dp</c>, ascending flags, and the
    /// resolved column types in declaration order. Used by both the
    /// parent-side and child-side resolvers.
    /// </summary>
    private readonly record struct SeekIndexCore(long FirstDp, byte[] ColTypes, IReadOnlyList<bool> Ascending);

    /// <summary>
    /// Shared resolution: looks up the catalog entry for
    /// <paramref name="tableName"/>, maps <paramref name="columnNames"/> to
    /// (ColNum, Type) on that table, locates the covering real-idx via
    /// <see cref="TryFindCoveringRealIdxAsync"/>, and probes every key
    /// column type with the encoder. Returns <see langword="null"/> if any
    /// step fails (Jet3, missing table/column, no covering index, or an
    /// un-seekable column type).
    /// </summary>
    private async ValueTask<SeekIndexCore?> TryResolveSeekIndexCoreAsync(
        string tableName,
        IReadOnlyList<string> columnNames,
        CancellationToken cancellationToken)
    {
        if (writer._format == DatabaseFormat.Jet3Mdb)
        {
            return null;
        }

        CatalogEntry? entry = await writer.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (entry == null)
        {
            return null;
        }

        TableDef def = await writer.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        var colNums = new int[columnNames.Count];
        var colTypes = new byte[columnNames.Count];
        for (int i = 0; i < columnNames.Count; i++)
        {
            int idx = def.FindColumnIndex(columnNames[i]);
            if (idx < 0)
            {
                return null;
            }

            colNums[i] = def.Columns[idx].ColNum;
            colTypes[i] = def.Columns[idx].Type;
        }

        (long FirstDp, IReadOnlyList<bool> AscendingFlags)? hit = await TryFindCoveringRealIdxAsync(
            entry.TDefPage,
            colNums,
            cancellationToken).ConfigureAwait(false);
        if (hit == null)
        {
            return null;
        }

        // Probe each column type with the encoder; if any one rejects,
        // the seek path cannot serve this relationship and we fall back.
        for (int i = 0; i < colTypes.Length; i++)
        {
            if (!IndexKeyEncoder.IsColumnTypeSeekable(colTypes[i]))
            {
                return null;
            }
        }

        return new SeekIndexCore(hit.Value.FirstDp, colTypes, hit.Value.AscendingFlags);
    }

    /// <summary>
    /// Locates (and caches inside <paramref name="ctx"/>) the parent table's
    /// real-idx whose col_map exactly covers <paramref name="rel"/>'s
    /// PrimaryColumns in declaration order. Returns <see langword="null"/>
    /// when no covering index exists, when the format is Jet3 (no index
    /// emission), or when the parent table cannot be resolved.
    /// </summary>
    private async ValueTask<ParentSeekIndex?> ResolveParentSeekIndexAsync(
        FkRelationship rel,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        if (ctx.SeekIndexes.TryGetValue(rel.Name, out ParentSeekIndex? cached))
        {
            return cached;
        }

        ParentSeekIndex? resolved = null;
        try
        {
            SeekIndexCore? core = await TryResolveSeekIndexCoreAsync(
                rel.PrimaryTable,
                rel.PrimaryColumns,
                cancellationToken).ConfigureAwait(false);
            if (core == null)
            {
                return null;
            }

            // Map relationship.ForeignColumns → row index inside an
            // InsertRow values array (the same indexing GetParentKeySetAsync /
            // BuildCompositeKey already use).
            CatalogEntry? foreignEntry = await writer.GetCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            if (foreignEntry == null)
            {
                return null;
            }

            TableDef foreignDef = await writer.ReadRequiredTableDefAsync(foreignEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            var foreignRowIdx = new int[rel.ForeignColumns.Count];
            for (int i = 0; i < rel.ForeignColumns.Count; i++)
            {
                foreignRowIdx[i] = foreignDef.FindColumnIndex(rel.ForeignColumns[i]);
                if (foreignRowIdx[i] < 0)
                {
                    return null;
                }
            }

            var keyColumns = new ParentSeekKeyColumn[core.Value.ColTypes.Length];
            for (int i = 0; i < keyColumns.Length; i++)
            {
                keyColumns[i] = new ParentSeekKeyColumn(core.Value.ColTypes[i], core.Value.Ascending[i], foreignRowIdx[i]);
            }

            resolved = new ParentSeekIndex(core.Value.FirstDp, keyColumns);
            return resolved;
        }
        finally
        {
            ctx.SeekIndexes[rel.Name] = resolved;
        }
    }

    /// <summary>
    /// Walks the TDEF at <paramref name="tdefPage"/>, decodes every real-idx
    /// physical descriptor, and returns the first slot whose col_map exactly
    /// matches <paramref name="targetColNums"/> (in declaration order). The
    /// match is sharing-aware — a non-FK user index that happens to cover
    /// the same columns is acceptable. Returns <see langword="null"/> when
    /// no covering real-idx exists, when its <c>first_dp</c> is unset, or
    /// when the TDEF spans multiple pages.
    /// </summary>
    private async ValueTask<(long FirstDp, IReadOnlyList<bool> AscendingFlags)?> TryFindCoveringRealIdxAsync(
        long tdefPage,
        int[] targetColNums,
        CancellationToken cancellationToken)
    {
        byte[] td = await ReadPageOwnedAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        if (td[0] != 0x02 || AccessBase.Ru32(td, 4) != 0)
        {
            return null;
        }

        int numCols = AccessBase.Ru16(td, writer._tdef.NumCols);
        int numRealIdx = AccessBase.Ri32(td, writer._tdef.NumRealIdx);
        if (numCols < 0 || numCols > 4096 || numRealIdx <= 0 || numRealIdx > 1000)
        {
            return null;
        }

        int realIdxDescStart = LocateRealIdxDescStart(td, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            return null;
        }

        const int RealIdxPhysSz = Constants.TableDefinition.Jet4.RealIdx.PhysSize;
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = realIdxDescStart + (ri * RealIdxPhysSz);
            if (!IndexHelpers.RealIdxColMapMatches(td, phys, targetColNums))
            {
                continue;
            }

            // col_map matched; capture per-column ascending flags from the
            // 10-slot {col_num(2), col_order(1)} map (col_order: 0x01 = asc,
            // 0x02 = desc).
            var ascending = new bool[targetColNums.Length];
            for (int slot = 0; slot < targetColNums.Length; slot++)
            {
                ascending[slot] = td[phys + 4 + (slot * 3) + 2] != 0x02;
            }

            int firstDp = AccessBase.Ri32(td, phys + 38);
            if (firstDp <= 0)
            {
                continue;
            }

            return (firstDp, ascending);
        }

        return null;
    }

    /// <summary>
    /// Locates (and caches inside <paramref name="ctx"/>) the child (FK) table's
    /// real-idx whose col_map exactly covers <paramref name="rel"/>'s
    /// ForeignColumns in declaration order. Returns <see langword="null"/>
    /// when no covering index exists, when the format is Jet3, when the
    /// child TDEF cannot be resolved, or when any FK column type is not
    /// seekable (the cascade enforcement loop falls back to the snapshot
    /// scan in that case).
    /// </summary>
    private async ValueTask<ChildSeekIndex?> ResolveChildSeekIndexAsync(
        FkRelationship rel,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        if (ctx.ChildSeekIndexes.TryGetValue(rel.Name, out ChildSeekIndex? cached))
        {
            return cached;
        }

        ChildSeekIndex? resolved = null;
        try
        {
            SeekIndexCore? core = await TryResolveSeekIndexCoreAsync(
                rel.ForeignTable,
                rel.ForeignColumns,
                cancellationToken).ConfigureAwait(false);
            if (core == null)
            {
                return null;
            }

            var keyColumns = new ChildSeekKeyColumn[core.Value.ColTypes.Length];
            for (int i = 0; i < keyColumns.Length; i++)
            {
                keyColumns[i] = new ChildSeekKeyColumn(core.Value.ColTypes[i], core.Value.Ascending[i]);
            }

            resolved = new ChildSeekIndex(core.Value.FirstDp, keyColumns);
            return resolved;
        }
        finally
        {
            ctx.ChildSeekIndexes[rel.Name] = resolved;
        }
    }

    /// <summary>
    /// After a successful insert, augments any cached parent-key sets that
    /// reference <paramref name="primaryTable"/> with the row's PK tuple so
    /// subsequent inserts within the same call (self-references and bulk
    /// inserts where one row supplies the parent for the next) succeed.
    /// </summary>
    internal static void AugmentParentSetsAfterInsert(string primaryTable, TableDef tableDef, object[] insertedValues, FkContext ctx)
    {
        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!ctx.ParentKeySets.TryGetValue(rel.Name, out HashSet<string>? set))
            {
                continue;
            }

            var pkIdx = new int[rel.PrimaryColumns.Count];
            bool ok = true;
            for (int i = 0; i < rel.PrimaryColumns.Count; i++)
            {
                pkIdx[i] = tableDef.FindColumnIndex(rel.PrimaryColumns[i]);
                if (pkIdx[i] < 0)
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
            {
                continue;
            }

            string? key = IndexHelpers.BuildCompositeKey(insertedValues, pkIdx);
            if (key != null)
            {
                _ = set.Add(key);
            }
        }
    }

    /// <summary>
    /// Resolves <paramref name="rel"/>'s PK columns on <paramref name="primaryDef"/>
    /// and FK columns on <paramref name="childDef"/> to row ordinals (in
    /// <c>rel</c>'s declaration order). Returns <see langword="false"/> when
    /// any column is missing on its respective table — caller skips the
    /// relationship.
    /// </summary>
    private static bool TryMapFkPairOrdinals(
        FkRelationship rel,
        TableDef primaryDef,
        TableDef childDef,
        out int[] primaryPkIdx,
        out int[] fkIdx)
    {
        int n = rel.PrimaryColumns.Count;
        primaryPkIdx = new int[n];
        fkIdx = new int[n];
        for (int i = 0; i < n; i++)
        {
            primaryPkIdx[i] = primaryDef.FindColumnIndex(rel.PrimaryColumns[i]);
            fkIdx[i] = childDef.FindColumnIndex(rel.ForeignColumns[i]);
            if (primaryPkIdx[i] < 0 || fkIdx[i] < 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// For each enforced FK whose primary side is <paramref name="primaryTable"/>,
    /// inspects the parent rows currently being deleted (full typed rows in
    /// primary-table column order in <paramref name="deletedParentRows"/>)
    /// and either cascades the delete to the child table when
    /// <see cref="FkRelationship.CascadeDeletes"/> is set, or throws when
    /// child rows still reference one of the deleted PK tuples.
    /// <para>
    /// When a covering FK-side real-idx is available
    /// (<see cref="ResolveChildSeekIndexAsync"/>), each parent PK tuple is
    /// encoded once and seeked through <see cref="IndexBTreeSeeker.FindRowLocationsAsync"/>
    /// to locate dependent child rows in O(log N + K) page reads. Falls
    /// back to the legacy O(N) child-table snapshot scan when the seek
    /// path is not usable (Jet3, no covering child index, encoder-rejected
    /// key types, child row contains LVAL-only columns the single-row
    /// reader cannot decode for the recursive grandchild step).
    /// </para>
    /// </summary>
    internal async ValueTask EnforceFkOnPrimaryDeleteAsync(
        string primaryTable,
        TableDef primaryDef,
        List<object?[]> deletedParentRows,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        if (depth > AccessWriter.CascadeMaxDepth)
        {
            throw new InvalidOperationException(
                $"Foreign-key cascade depth exceeded {AccessWriter.CascadeMaxDepth}. Possible cyclic relationship.");
        }

        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            CatalogEntry childEntry = await writer.GetRequiredCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            TableDef childDef = await writer.ReadRequiredTableDefAsync(childEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);

            if (!TryMapFkPairOrdinals(rel, primaryDef, childDef, out int[] primaryPkIdx, out int[] fkIdx))
            {
                continue;
            }

            // Build per-rel typed parent PK arrays (in rel.PrimaryColumns
            // declaration order) once for both the seek and the snapshot
            // fallback paths.
            var parentPkRows = new List<object?[]>(deletedParentRows.Count);
            foreach (object?[] row in deletedParentRows)
            {
                var pk = new object?[primaryPkIdx.Length];
                bool nullPart = false;
                for (int i = 0; i < primaryPkIdx.Length; i++)
                {
                    object? v = row[primaryPkIdx[i]];
                    if (v is DBNull)
                    {
                        v = null;
                    }

                    if (v == null)
                    {
                        nullPart = true;
                        break;
                    }

                    pk[i] = v;
                }

                if (!nullPart)
                {
                    parentPkRows.Add(pk);
                }
            }

            if (parentPkRows.Count == 0)
            {
                continue;
            }

            // Fast path: child-side index seek.
            ChildSeekIndex? childSeek = await ResolveChildSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (childSeek != null)
            {
                bool seekOk = await TryProcessCascadeDeleteWithSeekAsync(
                    rel,
                    childEntry,
                    childDef,
                    childSeek,
                    parentPkRows,
                    ctx,
                    depth,
                    cancellationToken).ConfigureAwait(false);
                if (seekOk)
                {
                    continue;
                }
            }

            // ── Snapshot fallback ─────────────────────────────────────
            using DataTable childSnap = await writer.ReadTableSnapshotAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            List<RowLocation> locations = await writer.GetLiveRowLocationsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            int total = Math.Min(childSnap.Rows.Count, locations.Count);

            // Build deletedSet from typed parent PK arrays (composite-key
            // strings keyed off rel.PrimaryColumns ordering). Equivalent to
            // the legacy BuildCompositeKey path but driven by the typed
            // parent rows we received from the caller.
            var deletedSet = new HashSet<string>(StringComparer.Ordinal);
            int[] identity = new int[primaryPkIdx.Length];
            for (int i = 0; i < identity.Length; i++)
            {
                identity[i] = i;
            }

            foreach (object?[] pk in parentPkRows)
            {
                string? k = IndexHelpers.BuildCompositeKey(pk, identity);
                if (k != null)
                {
                    _ = deletedSet.Add(k);
                }
            }

            var matchingRowIndices = new List<int>();
            for (int i = 0; i < total; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = IndexHelpers.BuildCompositeKey(childSnap.Rows[i].ItemArray, fkIdx);
                if (childKey == null)
                {
                    continue;
                }

                if (deletedSet.Contains(childKey))
                {
                    matchingRowIndices.Add(i);
                }
            }

            if (matchingRowIndices.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeDeletes)
            {
                throw new InvalidOperationException(
                    $"DELETE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{matchingRowIndices.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
            }

            // Capture the child rows' OWN full typed values (in child-table
            // column order) before we delete them so any grandchild relationship
            // can be recursed via the same EnforceFkOnPrimaryDeleteAsync entry.
            var childDeletedRows = new List<object?[]>(matchingRowIndices.Count);
            foreach (int rIdx in matchingRowIndices)
            {
                childDeletedRows.Add(childSnap.Rows[rIdx].ItemArray);
            }

            await EnforceFkOnPrimaryDeleteAsync(
                rel.ForeignTable,
                childDef,
                childDeletedRows,
                ctx,
                depth + 1,
                cancellationToken).ConfigureAwait(false);

            // Cascade complex flat-child rows for the child rows we are
            // about to delete. Must precede _writer.MarkRowDeletedAsync so the
            // ConceptualTableID slots are still readable.
            var cascadeLocs = new List<RowLocation>(matchingRowIndices.Count);
            foreach (int rIdx in matchingRowIndices)
            {
                cascadeLocs.Add(locations[rIdx]);
            }

            await writer.ComplexColumns.CascadeDeleteComplexChildrenAsync(childDef, cascadeLocs, cancellationToken).ConfigureAwait(false);

            // Now delete the child rows.
            int deleted = 0;
            foreach (int rIdx in matchingRowIndices)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.MarkRowDeletedAsync(locations[rIdx].PageNumber, locations[rIdx].RowIndex, cancellationToken).ConfigureAwait(false);
                deleted++;
            }

            if (deleted > 0)
            {
                await writer.AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
                await writer.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Shared seek + live-row resolution used by both the cascade-delete and
    /// cascade-update fast paths. For each (parent key, payload) request,
    /// encodes the key, seeks the child index, dedupes matched
    /// (page, rowIndex) tuples (first payload wins), then resolves each tuple
    /// to a full <see cref="RowLocation"/> via per-page live-row enumeration.
    /// Returns <see langword="null"/> if any key fails to encode, if a seek
    /// pointer leads to an unexpected page, or if the index reports row
    /// indices the live-row enumerator cannot confirm (caller falls back to
    /// the snapshot scan).
    /// </summary>
    private async ValueTask<List<(RowLocation Loc, TPayload Payload)>?> TrySeekChildLocationsAsync<TPayload>(
        CatalogEntry childEntry,
        ChildSeekIndex childSeek,
        IEnumerable<(object?[] OldPk, TPayload Payload)> requests,
        CancellationToken cancellationToken)
    {
        var pendingByLocation = new Dictionary<long, (long DataPage, int RowIndex, TPayload Payload)>();
        foreach ((object?[] oldPk, TPayload payload) in requests)
        {
            byte[]? encoded = IndexHelpers.TryEncodeChildSeekKey(childSeek, oldPk);
            if (encoded == null)
            {
                return null;
            }

            List<(long DataPage, int RowIndex)> hits = await IndexBTreeSeeker.FindRowLocationsAsync(
                (page, ct) => ReadPageOwnedAsync(page, ct),
                writer._pgSz,
                childSeek.RootPage,
                encoded,
                cancellationToken).ConfigureAwait(false);

            foreach ((long dp, int ri) in hits)
            {
                long key = (dp << 16) | (uint)ri;
                if (!pendingByLocation.ContainsKey(key))
                {
                    pendingByLocation[key] = (dp, ri, payload);
                }
            }
        }

        var result = new List<(RowLocation Loc, TPayload Payload)>(pendingByLocation.Count);
        if (pendingByLocation.Count == 0)
        {
            return result;
        }

        // Group by data page so we read each page exactly once.
        var byPage = new Dictionary<long, HashSet<int>>();
        foreach ((long dp, int ri, _) in pendingByLocation.Values)
        {
            if (!byPage.TryGetValue(dp, out HashSet<int>? rs))
            {
                rs = [];
                byPage[dp] = rs;
            }

            _ = rs.Add(ri);
        }

        foreach (KeyValuePair<long, HashSet<int>> kvp in byPage)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await writer.ReadPageAsync(kvp.Key, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01 || AccessBase.Ri32(page, writer._dataPage.TDefOff) != childEntry.TDefPage)
                {
                    // The seek pointer led to an unexpected page — bail.
                    return null;
                }

                foreach (AccessBase.RowBound rb in writer.EnumerateLiveRowBounds(page))
                {
                    if (!kvp.Value.Contains(rb.RowIndex))
                    {
                        continue;
                    }

                    long key = (kvp.Key << 16) | (uint)rb.RowIndex;
                    if (pendingByLocation.TryGetValue(key, out (long DataPage, int RowIndex, TPayload Payload) entry))
                    {
                        result.Add((new RowLocation(kvp.Key, rb.RowIndex, rb.RowStart, rb.RowSize), entry.Payload));
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        if (result.Count != pendingByLocation.Count)
        {
            // Index points at row indices the live-row enumerator could not
            // confirm — stale index pages or page-pool race. Fall back.
            return null;
        }

        return result;
    }

    /// <summary>
    /// Reads every column of every <paramref name="locations"/> row as typed
    /// values, normalising bare nulls to <see cref="DBNull.Value"/>. Returns
    /// <see langword="null"/> if any row contains a column the single-row
    /// reader cannot decode (LVAL: T_MEMO / T_OLE / T_COMPLEX / T_ATTACHMENT).
    /// </summary>
    private async ValueTask<List<object?[]>?> TryReadAllRowsTypedAsync(
        TableDef def,
        List<RowLocation> locations,
        CancellationToken cancellationToken)
    {
        var allColumnOrdinals = new int[def.Columns.Count];
        for (int i = 0; i < allColumnOrdinals.Length; i++)
        {
            allColumnOrdinals[i] = i;
        }

        var rows = new List<object?[]>(locations.Count);
        foreach (RowLocation loc in locations)
        {
            object?[]? values = await writer.TryReadColumnValuesTypedAsync(loc, def, allColumnOrdinals, cancellationToken).ConfigureAwait(false);
            if (values == null)
            {
                return null;
            }

            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null)
                {
                    values[i] = DBNull.Value;
                }
            }

            rows.Add(values);
        }

        return rows;
    }

    /// <summary>
    /// Fast path for cascade-delete: locate dependent child rows by
    /// seeking the child (FK) table's covering index for each parent PK
    /// tuple. Returns <see langword="false"/> when the seek path cannot be
    /// completed (any parent key fails to encode, or any matched child row
    /// contains an LVAL column the single-row reader cannot decode for the
    /// recursive grandchild step) — caller falls back to the snapshot scan.
    /// </summary>
    private async ValueTask<bool> TryProcessCascadeDeleteWithSeekAsync(
        FkRelationship rel,
        CatalogEntry childEntry,
        TableDef childDef,
        ChildSeekIndex childSeek,
        List<object?[]> parentPkRows,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        // Stage 1+2: seek every parent key and resolve the matched child
        // rows to full RowLocation values. Payload is unused for delete.
        var requests = new List<(object?[] OldPk, byte Payload)>(parentPkRows.Count);
        foreach (object?[] pk in parentPkRows)
        {
            requests.Add((pk, (byte)0));
        }

        List<(RowLocation Loc, byte Payload)>? hits = await TrySeekChildLocationsAsync(
            childEntry,
            childSeek,
            requests,
            cancellationToken).ConfigureAwait(false);
        if (hits == null)
        {
            return false;
        }

        if (hits.Count == 0)
        {
            return true;
        }

        if (!rel.CascadeDeletes)
        {
            throw new InvalidOperationException(
                $"DELETE on '{rel.PrimaryTable}' violates foreign-key constraint '{rel.Name}': " +
                $"{hits.Count} dependent row(s) in '{rel.ForeignTable}' reference the deleted key(s) and cascade-delete is not enabled.");
        }

        var fullLocations = new List<RowLocation>(hits.Count);
        foreach ((RowLocation loc, _) in hits)
        {
            fullLocations.Add(loc);
        }

        // Stage 3: read each matched child row's full typed values for the
        // recursive grandchild step. If any contains an LVAL column, bail.
        List<object?[]>? childDeletedRows = await TryReadAllRowsTypedAsync(childDef, fullLocations, cancellationToken).ConfigureAwait(false);
        if (childDeletedRows == null)
        {
            return false;
        }

        await EnforceFkOnPrimaryDeleteAsync(
            rel.ForeignTable,
            childDef,
            childDeletedRows,
            ctx,
            depth + 1,
            cancellationToken).ConfigureAwait(false);

        await writer.ComplexColumns.CascadeDeleteComplexChildrenAsync(childDef, fullLocations, cancellationToken).ConfigureAwait(false);

        int deleted = 0;
        foreach (RowLocation loc in fullLocations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await writer.MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, cancellationToken).ConfigureAwait(false);
            deleted++;
        }

        if (deleted > 0)
        {
            await writer.AdjustTDefRowCountAsync(childEntry.TDefPage, -deleted, cancellationToken).ConfigureAwait(false);
            await writer.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// For each enforced FK whose primary side is <paramref name="primaryTable"/>,
    /// when a row's PK tuple is changing from <c>oldKey</c> to <c>newKey</c>,
    /// either propagates the change to dependent child rows (cascade-update)
    /// or rejects the update when child rows reference the old key and
    /// cascade-update is not enabled.
    /// </summary>
    internal async ValueTask EnforceFkOnPrimaryUpdateAsync(
        string primaryTable,
        TableDef primaryDef,
        IReadOnlyList<(string? OldKey, object?[] OldFullRow, object[] NewPkValues)> changes,
        FkContext ctx,
        int depth,
        CancellationToken cancellationToken)
    {
        if (depth > AccessWriter.CascadeMaxDepth)
        {
            throw new InvalidOperationException(
                $"Foreign-key cascade depth exceeded {AccessWriter.CascadeMaxDepth}. Possible cyclic relationship.");
        }

        foreach (FkRelationship rel in ctx.All)
        {
            if (!string.Equals(rel.PrimaryTable, primaryTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            CatalogEntry childEntry = await writer.GetRequiredCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            TableDef childDef = await writer.ReadRequiredTableDefAsync(childEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);

            // Map this relationship's PK column ordinals on the PRIMARY-side def
            // (we need to extract the new PK tuple from NewPkValues which is in
            // primary-table column order, and the old PK tuple from OldFullRow).
            if (!TryMapFkPairOrdinals(rel, primaryDef, childDef, out int[] primaryPkIdx, out int[] fkIdx))
            {
                continue;
            }

            // Build (oldKey -> (oldPkSubset, newPkSubset)) for changes whose PK
            // actually moves. movingChanges keys remain the synthetic OldKey
            // string so the snapshot fallback (which composes child-side keys
            // via BuildCompositeKey) can still match.
            var movingChanges = new Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)>(StringComparer.Ordinal);
            foreach ((string? oldKey, object?[] oldFullRow, object[] newPkValues) in changes)
            {
                if (oldKey == null)
                {
                    continue;
                }

                string? newKey = IndexHelpers.BuildCompositeKey(newPkValues, primaryPkIdx);
                if (newKey == null || string.Equals(newKey, oldKey, StringComparison.Ordinal))
                {
                    continue;
                }

                var newPkSubset = new object[rel.PrimaryColumns.Count];
                var oldPkSubset = new object?[rel.PrimaryColumns.Count];
                for (int i = 0; i < rel.PrimaryColumns.Count; i++)
                {
                    newPkSubset[i] = newPkValues[primaryPkIdx[i]];
                    oldPkSubset[i] = oldFullRow[primaryPkIdx[i]];
                }

                movingChanges[oldKey] = (oldPkSubset, newPkSubset);
            }

            if (movingChanges.Count == 0)
            {
                continue;
            }

            // Fast path: child-side index seek.
            ChildSeekIndex? childSeek = await ResolveChildSeekIndexAsync(rel, ctx, cancellationToken).ConfigureAwait(false);
            if (childSeek != null)
            {
                bool seekOk = await TryProcessCascadeUpdateWithSeekAsync(
                    rel,
                    childEntry,
                    childDef,
                    childSeek,
                    movingChanges,
                    fkIdx,
                    cancellationToken).ConfigureAwait(false);
                if (seekOk)
                {
                    continue;
                }
            }

            using DataTable childSnap = await writer.ReadTableSnapshotAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            List<RowLocation> locations = await writer.GetLiveRowLocationsAsync(childEntry.TDefPage, cancellationToken).ConfigureAwait(false);
            int total = Math.Min(childSnap.Rows.Count, locations.Count);

            // Find affected child rows.
            var affectedIndices = new List<int>();
            var affectedOldKeys = new List<string>();
            for (int i = 0; i < total; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? childKey = IndexHelpers.BuildCompositeKey(childSnap.Rows[i].ItemArray, fkIdx);
                if (childKey != null && movingChanges.ContainsKey(childKey))
                {
                    affectedIndices.Add(i);
                    affectedOldKeys.Add(childKey);
                }
            }

            if (affectedIndices.Count == 0)
            {
                continue;
            }

            if (!rel.CascadeUpdates)
            {
                throw new InvalidOperationException(
                    $"UPDATE on '{primaryTable}' violates foreign-key constraint '{rel.Name}': " +
                    $"{affectedIndices.Count} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
            }

            // Cascade — rewrite each affected row with the new FK values.
            for (int ai = 0; ai < affectedIndices.Count; ai++)
            {
                int rIdx = affectedIndices[ai];
                object[] newPkSubset = movingChanges[affectedOldKeys[ai]].NewPkSubset;
                object[] rowValues = childSnap.Rows[rIdx].ItemArray;

                for (int j = 0; j < rel.ForeignColumns.Count; j++)
                {
                    rowValues[fkIdx[j]] = newPkSubset[j] ?? DBNull.Value;
                }

                await writer.MarkRowDeletedAsync(locations[rIdx].PageNumber, locations[rIdx].RowIndex, cancellationToken).ConfigureAwait(false);
                await writer.InsertRowDataAsync(childEntry.TDefPage, childDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            await writer.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Fast path for cascade-update: for each (oldKey, oldPkSubset, newPkSubset)
    /// triplet, encode the OLD PK subset as a child-index seek key, locate
    /// every dependent child row in O(log N + K) page reads, fetch each
    /// matched row's full typed values, and rewrite it in place via
    /// delete + insert with the new FK values. Returns <see langword="false"/>
    /// when any old PK fails to encode, when matched (page, rowIndex) hits
    /// cannot be confirmed against live row bounds, or when any matched
    /// child row contains an LVAL column the single-row reader cannot
    /// decode — caller falls back to the snapshot scan.
    /// </summary>
    private async ValueTask<bool> TryProcessCascadeUpdateWithSeekAsync(
        FkRelationship rel,
        CatalogEntry childEntry,
        TableDef childDef,
        ChildSeekIndex childSeek,
        Dictionary<string, (object?[] OldPkSubset, object[] NewPkSubset)> movingChanges,
        int[] fkIdx,
        CancellationToken cancellationToken)
    {
        // Stages 1+2: seek every old PK and resolve to full row locations,
        // carrying NewPkSubset as the per-key payload.
        var requests = new List<(object?[] OldPk, object[] Payload)>(movingChanges.Count);
        foreach (KeyValuePair<string, (object?[] OldPkSubset, object[] NewPkSubset)> kvp in movingChanges)
        {
            requests.Add((kvp.Value.OldPkSubset, kvp.Value.NewPkSubset));
        }

        List<(RowLocation Loc, object[] NewPkSubset)>? rowMeta = await TrySeekChildLocationsAsync(
            childEntry,
            childSeek,
            requests,
            cancellationToken).ConfigureAwait(false);
        if (rowMeta == null)
        {
            return false;
        }

        if (rowMeta.Count == 0)
        {
            return true;
        }

        if (!rel.CascadeUpdates)
        {
            throw new InvalidOperationException(
                $"UPDATE on '{rel.PrimaryTable}' violates foreign-key constraint '{rel.Name}': " +
                $"{rowMeta.Count} dependent row(s) in '{rel.ForeignTable}' reference the old key(s) and cascade-update is not enabled.");
        }

        // Stage 3: read each matched child row's full typed values and
        // rewrite in place via delete + insert with the new FK values.
        var locations = new List<RowLocation>(rowMeta.Count);
        foreach ((RowLocation loc, _) in rowMeta)
        {
            locations.Add(loc);
        }

        List<object?[]>? rows = await TryReadAllRowsTypedAsync(childDef, locations, cancellationToken).ConfigureAwait(false);
        if (rows == null)
        {
            return false;
        }

        for (int r = 0; r < rowMeta.Count; r++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (RowLocation loc, object[] newPkSubset) = rowMeta[r];
            object?[] values = rows[r];

            object[] rowValues = new object[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                rowValues[i] = values[i] ?? DBNull.Value;
            }

            for (int j = 0; j < fkIdx.Length; j++)
            {
                rowValues[fkIdx[j]] = newPkSubset[j] ?? DBNull.Value;
            }

            await writer.MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, cancellationToken).ConfigureAwait(false);
            await writer.InsertRowDataAsync(childEntry.TDefPage, childDef, rowValues, updateTDefRowCount: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        await writer.MaintainIndexesAsync(childEntry.TDefPage, childDef, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Locates a system or user table's TDEF page number by name (case-insensitive)
    /// by scanning every <c>MSysObjects</c> row. Returns <c>0</c> when not found.
    /// </summary>
    internal async ValueTask<long> FindSystemTableTdefPageAsync(string tableName, CancellationToken cancellationToken)
    {
        TableDef? msys = await writer.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        List<CatalogRow> rows = await writer.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == Constants.SystemObjects.UserTableType
                && row.TDefPage > 0
                && string.Equals(row.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return row.TDefPage;
            }
        }

        return 0;
    }

    /// <summary>
    /// Reads the distinct values of <c>szRelationship</c> from every live row in the
    /// <c>MSysRelationships</c> table. Used to enforce relationship-name uniqueness.
    /// </summary>
    private async ValueTask<HashSet<string>> ReadExistingRelationshipNamesAsync(long msysRelTdefPage, TableDef msysRelDef, CancellationToken cancellationToken)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        ColumnInfo? nameCol = msysRelDef.FindColumn("szRelationship");
        if (nameCol == null)
        {
            return names;
        }

        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, writer._dataPage.TDefOff) != msysRelTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string name = writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.IsNullOrEmpty(name))
                    {
                        _ = names.Add(name);
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return names;
    }
}
