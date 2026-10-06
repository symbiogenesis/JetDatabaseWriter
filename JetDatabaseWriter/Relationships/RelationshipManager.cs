namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using static JetDatabaseWriter.Schema.JetTypeInfo;

#pragma warning disable SA1204

/// <summary>
/// Foreign-key relationship management for <see cref="AccessWriter"/>:
/// create/drop/rename workflow and per-TDEF FK logical-index mutation, the
/// relationship state carried through copy-and-swap schema rewrites, and the
/// relationship check and partner unlinking behind dropping a table.
/// MSysRelationships row operations live in <see cref="RelationshipCatalogStore"/>,
/// and runtime referential-integrity enforcement lives in <see cref="RelationshipEnforcer"/>.
/// The public facade owns the auto-commit scope around each workflow.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="pager">The writer's page file, through which TDEF chains are written.</param>
/// <param name="tableCatalog">Resolves the primary and foreign tables by name.</param>
/// <param name="indexes">Rebuilds FK index leaves after the per-TDEF entries change.</param>
/// <param name="pageAllocator">Allocates FK leaf pages and grows or shrinks TDEF chains.</param>
/// <param name="catalogArtifacts">Emits the relationship's <c>MSysObjects</c> row.</param>
/// <param name="catalogRows">Locates the <c>MSysRelationships</c> table.</param>
/// <param name="catalog">Reads and rewrites <c>MSysRelationships</c> rows.</param>
/// <param name="snapshots">Reads existing rows through the writer's current pages.</param>
internal sealed class RelationshipManager(
    JetFormat format,
    TableDefReader tableDefs,
    Pager pager,
    TableCatalog tableCatalog,
    IndexMaintainer indexes,
    PageAllocator pageAllocator,
    CatalogArtifactWriter catalogArtifacts,
    CatalogRowReader catalogRows,
    RelationshipCatalogStore catalog,
    TableSnapshotReader snapshots)
{
    private readonly JetFormat format = format;
    private readonly TableDefReader tableDefs = tableDefs;
    private readonly Pager pager = pager;
    private readonly TableCatalog tableCatalog = tableCatalog;
    private readonly IndexMaintainer indexes = indexes;
    private readonly PageAllocator pageAllocator = pageAllocator;
    private readonly CatalogArtifactWriter catalogArtifacts = catalogArtifacts;
    private readonly CatalogRowReader catalogRows = catalogRows;
    private readonly RelationshipCatalogStore catalog = catalog;
    private readonly TableSnapshotReader snapshots = snapshots;

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
    /// <exception cref="ArgumentException">Thrown when a key column is missing from its table, or, before anything is read, when the relationship name breaks the Access naming rules (<see cref="AccessObjectName"/>) or a name is not in a Jet3 database's code page.</exception>
    /// <exception cref="JetNotSupportedException">Thrown when the database has no <c>MSysRelationships</c> table.</exception>
    /// <exception cref="InvalidOperationException">Thrown when a relationship with the same name already exists or existing rows violate its enforced constraint.</exception>
    /// <remarks>
    /// Per <see href="docs/design/index-and-relationship-format-notes.md" /> §7. The
    /// MSysRelationships catalog rows are what the Microsoft Access
    /// Relationships designer reads. The per-TDEF FK logical-index entries
    /// (<c>index_type = 0x02</c>, <c>rel_idx_num</c>, <c>rel_tbl_page</c>)
    /// that drive runtime referential-integrity enforcement by the JET
    /// engine are emitted by <see cref="EmitFkPerTdefEntriesAsync"/> on
    /// every format, in the shape Access writes for that format.
    /// </remarks>
    /// <exception cref="JetObjectExistsException">The operation is refused with a structured <see cref="JetObjectExistsException"/>.</exception>
    /// <exception cref="JetObjectNotFoundException">A relationship key column is missing.</exception>
    internal async ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken)
    {
        Guard.NotNull(relationship, nameof(relationship));
        AccessObjectName.ThrowIfInvalid(relationship.Name, "relationship.Name", "relationship");
        Guard.NotNullOrEmpty(relationship.PrimaryTable, "relationship.PrimaryTable");
        Guard.NotNullOrEmpty(relationship.ForeignTable, "relationship.ForeignTable");
        this.ThrowIfNamesNotStorable(relationship);
        Guard.ThrowIfDisposed(this.pager.IsDisposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // Validate referenced user tables exist and load their definitions.
        ResolvedTable primaryTable = await this.tableCatalog.ResolveRequiredTableAsync(relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        ResolvedTable foreignTable = await this.tableCatalog.ResolveRequiredTableAsync(relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
        CatalogEntry primaryEntry = primaryTable.Entry;
        CatalogEntry foreignEntry = foreignTable.Entry;
        TableDef primaryDef = primaryTable.Definition;
        TableDef foreignDef = foreignTable.Definition;
        await this.indexes.ThrowIfIndexesUnmaintainableAsync(primaryEntry.TDefPage, primaryDef, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        await this.indexes.ThrowIfIndexesUnmaintainableAsync(foreignEntry.TDefPage, foreignDef, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
        await this.catalogArtifacts.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < relationship.PrimaryColumns.Count; i++)
        {
            if (primaryDef.FindColumnIndex(relationship.PrimaryColumns[i]) < 0)
            {
                throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{relationship.PrimaryColumns[i]}' was not found on table '{relationship.PrimaryTable}'.", nameof(relationship), new JetErrorInfo { TableName = relationship.PrimaryTable, ColumnName = relationship.PrimaryColumns[i], RelationshipName = relationship.Name });
            }

            if (foreignDef.FindColumnIndex(relationship.ForeignColumns[i]) < 0)
            {
                throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{relationship.ForeignColumns[i]}' was not found on table '{relationship.ForeignTable}'.", nameof(relationship), new JetErrorInfo { TableName = relationship.ForeignTable, ColumnName = relationship.ForeignColumns[i], RelationshipName = relationship.Name });
            }
        }

        foreach (string columnName in relationship.PrimaryColumns)
        {
            IndexMaintainer.ThrowIfTextCollationUnsupported(primaryDef.Columns[primaryDef.FindColumnIndex(columnName)], relationship.PrimaryTable);
        }

        foreach (string columnName in relationship.ForeignColumns)
        {
            IndexMaintainer.ThrowIfTextCollationUnsupported(foreignDef.Columns[foreignDef.FindColumnIndex(columnName)], relationship.ForeignTable);
        }

        // Locate MSysRelationships (system table — not in the user-table cache).
        long msysRelTdefPage = await this.catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new JetNotSupportedException(JetErrorCode.SystemTableMissing, "The database does not contain a 'MSysRelationships' table. Full-catalog ACCDB databases created by AccessWriter.CreateDatabaseAsync include it, but Jet/MDB outputs and slim catalog databases may require an Access-authored source before calling CreateRelationshipAsync.", new JetErrorInfo { ObjectName = "MSysRelationships" });
        }

        TableDef msysRelDef = await this.tableDefs.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

        // Reject duplicate relationship names (case-insensitive).
        HashSet<string> existingNames = await this.catalog.ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existingNames.Contains(relationship.Name))
        {
            throw new JetObjectExistsException(JetErrorCode.RelationshipExists, $"A relationship named '{relationship.Name}' already exists.", new JetErrorInfo { RelationshipName = relationship.Name });
        }

        await this.ValidateExistingRowsAsync(relationship, primaryTable, foreignTable, cancellationToken).ConfigureAwait(false);

        await this.catalog.AppendRelationshipRowsAsync(msysRelTdefPage, msysRelDef, relationship, cancellationToken).ConfigureAwait(false);

        await this.catalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan(
                [],
                [CatalogObjectArtifact.Relationship(relationship.Name)]),
            cancellationToken).ConfigureAwait(false);

        // Per-TDEF FK logical-idx entries: add index_type=0x02 logical-idx
        // entries on both PK-side and FK-side TDEFs with cross-referenced
        // rel_idx_num / rel_tbl_page so the JET engine can locate the partner
        // table without waiting for Microsoft Access Compact & Repair to
        // regenerate them from the MSysRelationships rows above. See
        // docs/design/index-and-relationship-format-notes.md §3.2 and §7.
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
        TableDef primaryDefAfter = await this.tableDefs.ReadRequiredTableDefAsync(primaryEntry.TDefPage, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        await this.indexes.MaintainIndexesAsync(primaryEntry.TDefPage, primaryDefAfter, relationship.PrimaryTable, cancellationToken).ConfigureAwait(false);
        if (foreignEntry.TDefPage != primaryEntry.TDefPage)
        {
            TableDef foreignDefAfter = await this.tableDefs.ReadRequiredTableDefAsync(foreignEntry.TDefPage, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
            await this.indexes.MaintainIndexesAsync(foreignEntry.TDefPage, foreignDefAfter, relationship.ForeignTable, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Refuses existing orphan keys before any relationship metadata or index pages are written.</summary>
    /// <param name="relationship">The proposed relationship.</param>
    /// <param name="primaryTable">The resolved parent table.</param>
    /// <param name="foreignTable">The resolved child table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetConstraintException">An existing foreign key has no matching parent key.</exception>
    private async ValueTask ValidateExistingRowsAsync(
        RelationshipDefinition relationship,
        ResolvedTable primaryTable,
        ResolvedTable foreignTable,
        CancellationToken cancellationToken)
    {
        if (!relationship.EnforceReferentialIntegrity)
        {
            return;
        }

        int[] primaryColumns = new int[relationship.PrimaryColumns.Count];
        int[] foreignColumns = new int[relationship.ForeignColumns.Count];
        for (int index = 0; index < primaryColumns.Length; index++)
        {
            primaryColumns[index] = primaryTable.Definition.FindColumnIndex(relationship.PrimaryColumns[index]);
            foreignColumns[index] = foreignTable.Definition.FindColumnIndex(relationship.ForeignColumns[index]);
        }

        List<LocatedRow> foreignRows = await this.snapshots.ReadRowsAsync(foreignTable.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var foreignKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (LocatedRow row in foreignRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? key = this.EncodeRelationshipKey(primaryTable.Definition, primaryColumns, row.Values, foreignColumns);
            if (key != null)
            {
                _ = foreignKeys.Add(key);
            }
        }

        if (foreignKeys.Count == 0)
        {
            return;
        }

        List<LocatedRow> primaryRows = primaryTable.Entry.TDefPage == foreignTable.Entry.TDefPage
            ? foreignRows
            : await this.snapshots.ReadRowsAsync(primaryTable.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        foreach (LocatedRow row in primaryRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? key = this.EncodeRelationshipKey(primaryTable.Definition, primaryColumns, row.Values, primaryColumns);
            if (key != null)
            {
                _ = foreignKeys.Remove(key);
                if (foreignKeys.Count == 0)
                {
                    return;
                }
            }
        }

        if (foreignKeys.Count != 0)
        {
            throw JetErrors.Constraint(JetErrorCode.ForeignKeyMissingParent, $"Cannot create relationship '{relationship.Name}': existing rows in '{relationship.ForeignTable}' violate referential integrity because no matching row exists in '{relationship.PrimaryTable}'.", new JetErrorInfo { TableName = relationship.ForeignTable, RelationshipName = relationship.Name });
        }
    }

    /// <summary>Encodes each component with the referenced column's index comparison semantics.</summary>
    /// <param name="primaryDef">The referenced table definition.</param>
    /// <param name="primaryColumns">The referenced column ordinals.</param>
    /// <param name="values">The row values.</param>
    /// <param name="valueColumns">The row ordinals supplying the key.</param>
    /// <returns>A key with unambiguous component boundaries, or null for a partial-null tuple.</returns>
    private string? EncodeRelationshipKey(TableDef primaryDef, int[] primaryColumns, object[] values, int[] valueColumns)
    {
        foreach (int ordinal in valueColumns)
        {
            if (values[ordinal] is null or DBNull)
            {
                return null;
            }
        }

        string[] components = new string[valueColumns.Length];
        for (int index = 0; index < components.Length; index++)
        {
            components[index] = Convert.ToBase64String(IndexKeyEncoder.EncodeColumnEntry(
                this.format,
                primaryDef.Columns[primaryColumns[index]],
                values[valueColumns[index]]));
        }

        return string.Join("|", components);
    }

    /// <summary>
    /// Refuses, before anything is read, a relationship that names something a
    /// Jet3 database's code page cannot store. <c>MSysRelationships</c> stores
    /// the relationship name and the table and column names as the caller
    /// spells them, and a lookup ignoring case can match a name the code page
    /// does not have (Greek capital mu matches a column named µ).
    /// </summary>
    /// <param name="relationship">The relationship to create.</param>
    /// <exception cref="ArgumentException">A name holds a character the database's code page does not have.</exception>
    private void ThrowIfNamesNotStorable(RelationshipDefinition relationship)
    {
        AccessObjectName.ThrowIfNotStorable(this.format, relationship.Name, "relationship.Name", "relationship");
        AccessObjectName.ThrowIfNotStorable(this.format, relationship.PrimaryTable, "relationship.PrimaryTable", "table");
        AccessObjectName.ThrowIfNotStorable(this.format, relationship.ForeignTable, "relationship.ForeignTable", "table");
        for (int i = 0; i < relationship.PrimaryColumns.Count; i++)
        {
            if (relationship.PrimaryColumns[i] is { } primaryColumn)
            {
                AccessObjectName.ThrowIfNotStorable(this.format, primaryColumn, "relationship.PrimaryColumns", "column", i);
            }

            if (relationship.ForeignColumns[i] is { } foreignColumn)
            {
                AccessObjectName.ThrowIfNotStorable(this.format, foreignColumn, "relationship.ForeignColumns", "column", i);
            }
        }
    }

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

    // ════════════════════════════════════════════════════════════════
    // Copy-and-swap schema rewrites (AddColumn / DropColumn / RenameColumn)
    // ════════════════════════════════════════════════════════════════
    //
    // TableSchemaEditor rebuilds a table into a temporary copy and swaps the
    // copy in. That usually moves the table to a new TDEF page and always
    // renumbers its logical indexes, while relationship state lives in three
    // places that must follow the table:
    //   • the table's own FK logical-idx entries, which CreateTableAsync does
    //     not emit for the copy;
    //   • each partner table's FK logical-idx entry, whose rel_tbl_page and
    //     rel_idx_num name this table's TDEF page and logical-idx number;
    //   • the MSysRelationships rows, which name the key columns.
    // The editor calls CaptureForRewriteAsync and EnsureKeyColumnsSurvive
    // before it writes anything, EmitFkEntriesForRewriteAsync on the copy
    // before the row copy (so the copy's single index rebuild fills the FK
    // leaves), and CompleteRewriteAsync once the copy has replaced the table.
    // This works the same on every format. An FK entry whose partner does not
    // link back to it, such as one naming the freed TDEF page of a table that
    // earlier builds dropped without unlinking it, or one naming itself, is
    // not captured, so the rewrite drops it rather than re-emitting a pointer
    // at a freed or reused page.

    /// <summary>
    /// Captures the relationship state of <paramref name="tableName"/> before a
    /// copy-and-swap schema rewrite: its FK logical-idx entries and the key
    /// columns its <c>MSysRelationships</c> rows name. An FK entry whose
    /// partner does not link back is left out, so the rewrite drops it instead
    /// of carrying it over; see <see cref="PartnerLinksBackAsync"/>.
    /// </summary>
    /// <param name="tableName">The table about to be rewritten.</param>
    /// <param name="tdefPage">The table's current TDEF page.</param>
    /// <param name="tableDef">The table's current definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The captured state.</returns>
    internal async ValueTask<RelationshipRewriteState> CaptureForRewriteAsync(
        string tableName,
        long tdefPage,
        TableDef tableDef,
        CancellationToken cancellationToken)
    {
        var fkEntries = new List<FkLogicalIndexSnapshot>();
        foreach (FkLogicalIndexSnapshot entry in await this.ReadFkLogicalIndexesAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false))
        {
            if (entry.RelIdxNum < 0
                || await this.PartnerLinksBackAsync(entry.RelTblPage, entry.RelIdxNum, tdefPage, entry.IndexNumber, cancellationToken).ConfigureAwait(false))
            {
                fkEntries.Add(entry);
            }
        }

        var keyColumns = new List<RelationshipKeyColumn>();
        foreach (RelationshipRowSnapshot row in await this.CollectAllRelationshipRowsAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(row.SzObject, tableName, StringComparison.OrdinalIgnoreCase))
            {
                keyColumns.Add(new RelationshipKeyColumn(row.SzRelationship, row.SzColumn));
            }

            if (string.Equals(row.SzReferencedObject, tableName, StringComparison.OrdinalIgnoreCase))
            {
                keyColumns.Add(new RelationshipKeyColumn(row.SzRelationship, row.SzReferencedColumn));
            }
        }

        return new RelationshipRewriteState(tableName, tdefPage, fkEntries, keyColumns);
    }

    /// <summary>
    /// Throws when a schema rewrite would drop a column that a relationship
    /// uses as a key column. Microsoft Access refuses the same change, and
    /// neither the FK index entries nor the <c>MSysRelationships</c> rows could
    /// follow the column.
    /// </summary>
    /// <param name="state">The state captured before the rewrite.</param>
    /// <param name="mapColumnName">Maps each current column name to its name after the rewrite, or to <see langword="null"/> for a dropped column.</param>
    /// <exception cref="InvalidOperationException">Thrown when a relationship key column would be dropped.</exception>
    /// <exception cref="JetOperationException">The operation is refused with a structured <see cref="JetOperationException"/>.</exception>
    internal static void EnsureKeyColumnsSurvive(RelationshipRewriteState state, Func<string, string?> mapColumnName)
    {
        foreach (RelationshipKeyColumn key in state.KeyColumns)
        {
            if (mapColumnName(key.ColumnName) is null)
            {
                throw new JetOperationException(JetErrorCode.KeyColumnInRelationship, $"Column '{key.ColumnName}' of table '{state.TableName}' is a key column of relationship '{key.RelationshipName}'. Drop the relationship before dropping the column.", errorInfo: new JetErrorInfo { TableName = state.TableName, ColumnName = key.ColumnName, RelationshipName = key.RelationshipName });
            }
        }

        foreach (FkLogicalIndexSnapshot entry in state.FkEntries)
        {
            foreach (string column in entry.ColumnNames)
            {
                if (mapColumnName(column) is null)
                {
                    throw new JetOperationException(JetErrorCode.KeyColumnInRelationship, $"Column '{column}' of table '{state.TableName}' is a key column of foreign-key index '{entry.Name}'. Drop the relationship before dropping the column.", errorInfo: new JetErrorInfo { TableName = state.TableName, ColumnName = column, IndexName = entry.Name });
                }
            }
        }
    }

    /// <summary>
    /// Re-emits the FK logical-idx entries captured in <paramref name="state"/>
    /// on <paramref name="targetTdefPage"/>, the rebuilt copy of the table, with
    /// their names, sides and cascade flags. Each entry gets a new logical-idx
    /// number; an entry that points back at the same table (a self-referencing
    /// relationship) is pointed at <paramref name="finalTdefPage"/> and its
    /// partner's new number. Partner tables are not touched here; see
    /// <see cref="CompleteRewriteAsync"/>. The new FK leaves are empty until the
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
    /// Finishes a schema rewrite once the rebuilt copy has replaced the table.
    /// Points each partner table's FK logical-idx entry at
    /// <paramref name="finalTdefPage"/> and at the re-emitted entry's new
    /// logical-idx number, or removes the partner's entry when this side was
    /// not re-emitted, and writes renamed key columns into the
    /// <c>MSysRelationships</c> rows.
    /// </summary>
    /// <param name="state">The state captured before the rewrite.</param>
    /// <param name="finalTdefPage">The TDEF page the table now occupies.</param>
    /// <param name="newIndexNumbers">The map returned by <see cref="EmitFkEntriesForRewriteAsync"/>.</param>
    /// <param name="mapColumnName">Maps each captured column name to its name after the rewrite.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal async ValueTask CompleteRewriteAsync(
        RelationshipRewriteState state,
        long finalTdefPage,
        IReadOnlyDictionary<int, int> newIndexNumbers,
        Func<string, string?> mapColumnName,
        CancellationToken cancellationToken)
    {
        if (state.IsEmpty)
        {
            return;
        }

        foreach (FkLogicalIndexSnapshot entry in state.FkEntries)
        {
            if (entry.RelTblPage == state.TDefPage || entry.RelIdxNum < 0)
            {
                continue;
            }

            int? newIndexNumber = newIndexNumbers.TryGetValue(entry.IndexNumber, out int number) ? number : null;
            await this.UpdatePartnerFkEntryAsync(
                entry.RelTblPage,
                entry.RelIdxNum,
                state.TDefPage,
                finalTdefPage,
                newIndexNumber,
                cancellationToken).ConfigureAwait(false);
        }

        await this.RenameKeyColumnsAsync(state, mapColumnName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads every FK logical-idx entry (<c>index_type = 0x02</c>) on the TDEF at
    /// <paramref name="tdefPage"/>. Entries whose key columns cannot be resolved
    /// against <paramref name="tableDef"/> are skipped.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table definition, used to name the key columns.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<IReadOnlyList<FkLogicalIndexSnapshot>> ReadFkLogicalIndexesAsync(
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
    /// names itself. Earlier builds' <c>DropTableAsync</c> left such entries
    /// naming the freed TDEF page of a dropped table, or the unrelated table
    /// that took the page later, and their schema rewrites turned one into an
    /// entry naming itself when the rebuilt copy took that page.
    /// </summary>
    /// <param name="partnerTdefPage">The page the entry names (<c>rel_tbl_page</c>).</param>
    /// <param name="partnerIndexNumber">The partner entry the entry names (<c>rel_idx_num</c>).</param>
    /// <param name="tdefPage">The TDEF page that holds the entry.</param>
    /// <param name="indexNumber">The entry's own <c>index_num</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>Whether the partner entry exists and points back at the entry.</returns>
    private async ValueTask<bool> PartnerLinksBackAsync(
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
            cancellationToken).ConfigureAwait(false);
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
    private async ValueTask UpdatePartnerFkEntryAsync(
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
            cancellationToken).ConfigureAwait(false);
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
    /// Writes the new names of renamed key columns into the
    /// <c>MSysRelationships</c> rows that name them (<c>szColumn</c> where the
    /// table is the child, <c>szReferencedColumn</c> where it is the parent).
    /// </summary>
    /// <param name="state">The state captured before the rewrite.</param>
    /// <param name="mapColumnName">Maps each captured column name to its name after the rewrite.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask RenameKeyColumnsAsync(
        RelationshipRewriteState state,
        Func<string, string?> mapColumnName,
        CancellationToken cancellationToken)
    {
        bool anyRenamed = false;
        foreach (RelationshipKeyColumn key in state.KeyColumns)
        {
            if (IsRenamed(key.ColumnName))
            {
                anyRenamed = true;
                break;
            }
        }

        if (!anyRenamed)
        {
            return;
        }

        long msysRelTdefPage = await this.catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            return;
        }

        TableDef msysRelDef = await this.tableDefs.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        int szColumnIdx = msysRelDef.FindColumnIndex("szColumn");
        int szReferencedColumnIdx = msysRelDef.FindColumnIndex("szReferencedColumn");
        if (szColumnIdx < 0 || szReferencedColumnIdx < 0)
        {
            return;
        }

        List<RelationshipRowSnapshot> rows = await this.catalog.CollectRowsAsync(msysRelTdefPage, msysRelDef, _ => true, cancellationToken).ConfigureAwait(false);
        var replacementRows = new List<object[]>(rows.Count);
        foreach (RelationshipRowSnapshot row in rows)
        {
            object[] values = (object[])row.RowValues.Clone();
            if (string.Equals(row.SzObject, state.TableName, StringComparison.OrdinalIgnoreCase) && IsRenamed(row.SzColumn))
            {
                values[szColumnIdx] = mapColumnName(row.SzColumn)!;
            }

            if (string.Equals(row.SzReferencedObject, state.TableName, StringComparison.OrdinalIgnoreCase) && IsRenamed(row.SzReferencedColumn))
            {
                values[szReferencedColumnIdx] = mapColumnName(row.SzReferencedColumn)!;
            }

            replacementRows.Add(values);
        }

        await this.catalog.RewriteRowsAsync(msysRelTdefPage, msysRelDef, replacementRows, cancellationToken).ConfigureAwait(false);

        bool IsRenamed(string columnName)
            => mapColumnName(columnName) is string mapped && !string.Equals(mapped, columnName, StringComparison.Ordinal);
    }

    // ════════════════════════════════════════════════════════════════
    // Dropped tables (DropTableAsync)
    // ════════════════════════════════════════════════════════════════
    //
    // Microsoft Access refuses to drop a table that a relationship names
    // (DAO / Jet SQL error 3303), and so does TableSchemaEditor: it calls
    // FindRelationshipNamesForTableAsync and EnsureTableHasNoRelationships
    // before it writes anything, so the caller drops the relationships first.
    // That decision is made from MSysRelationships, which every format has
    // and which enforcement reads; it covers relationships that do not
    // enforce referential integrity and self-referencing ones too.
    //
    // A table can still carry FK logical-idx entries that no MSysRelationships
    // row names, left by earlier builds or by a catalog edited by hand. Once
    // the table's TDEF page is freed, its partners' entries would name a free
    // page, and later an unrelated table that reuses it. RemovePartnerLinksAsync
    // removes them, by TDEF page rather than by catalog name, before the drop
    // frees the page. A schema rewrite does not call it: CompleteRewriteAsync
    // re-links the partners to the rebuilt table instead.

    /// <summary>
    /// Returns the names of the relationships whose <c>MSysRelationships</c>
    /// rows name <paramref name="tableName"/> as their primary or foreign
    /// table, whether or not they enforce referential integrity, distinct and
    /// sorted (case-insensitive). Empty when the database has no
    /// <c>MSysRelationships</c> table.
    /// </summary>
    /// <param name="tableName">The table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The relationship names.</returns>
    internal async ValueTask<IReadOnlyList<string>> FindRelationshipNamesForTableAsync(string tableName, CancellationToken cancellationToken)
    {
        var names = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (RelationshipRowSnapshot row in await this.CollectAllRelationshipRowsAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(row.SzObject, tableName, StringComparison.OrdinalIgnoreCase)
                || string.Equals(row.SzReferencedObject, tableName, StringComparison.OrdinalIgnoreCase))
            {
                _ = names.Add(row.SzRelationship);
            }
        }

        return [.. names];
    }

    /// <summary>
    /// Throws when <paramref name="relationshipNames"/> is not empty: a table
    /// that takes part in a relationship cannot be dropped. Microsoft Access
    /// refuses the same drop with error 3303 ("currently participates in one or
    /// more relationships").
    /// </summary>
    /// <param name="tableName">The table being dropped.</param>
    /// <param name="relationshipNames">The relationships that name it, from <see cref="FindRelationshipNamesForTableAsync"/>.</param>
    /// <exception cref="InvalidOperationException">Thrown when any relationship names the table.</exception>
    /// <exception cref="JetOperationException">The operation is refused with a structured <see cref="JetOperationException"/>.</exception>
    internal static void EnsureTableHasNoRelationships(string tableName, IReadOnlyList<string> relationshipNames)
    {
        if (relationshipNames.Count == 0)
        {
            return;
        }

        var quoted = new List<string>(relationshipNames.Count);
        foreach (string name in relationshipNames)
        {
            quoted.Add("'" + name + "'");
        }

        string list = string.Join(", ", quoted);
        throw new JetOperationException(JetErrorCode.TableInRelationship, relationshipNames.Count == 1 ? $"Table '{tableName}' participates in relationship {list}. Drop the relationship before dropping the table." : $"Table '{tableName}' participates in relationships {list}. Drop the relationships before dropping the table.", errorInfo: new JetErrorInfo { TableName = tableName });
    }

    /// <summary>
    /// Removes, from every other table, the FK logical-idx entries whose
    /// <c>rel_tbl_page</c> names <paramref name="tdefPage"/>, the TDEF page of
    /// a table about to be dropped, and reclaims the trailing real-idx slots
    /// those removals leave unreferenced. The partners are found through the catalog and the
    /// dropped table's own FK entries, so one-way cataloged links and partners
    /// the catalog does not list are covered too; an entry on a partner whose page now holds an unrelated
    /// table is left alone, because it does not name <paramref name="tdefPage"/>.
    /// The removed entries' index leaves stay allocated until Compact &amp;
    /// Repair, as after <see cref="DropRelationshipAsync"/>. Does nothing when
    /// the page is not a TDEF.
    /// </summary>
    /// <param name="tdefPage">The TDEF page of the table being dropped, still intact.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal async ValueTask RemovePartnerLinksAsync(long tdefPage, CancellationToken cancellationToken)
    {
        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
            tdefPage,
            this.format.PageSize,
            this.pager.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: false,
            cancellationToken).ConfigureAwait(false);
        if (chain is null || !this.TryParseFkTDefLayout(chain.Bytes, out FkTDefLayout layout))
        {
            return;
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

        // A one-way FK can name this page even when this table has no entry
        // naming its owner. Scan local table rows, including hidden and system
        // tables and ODBC TDEFs, rather than the user-table cache. Other IDs do not
        // identify local TDEF pages.
        TableDef? objects = await this.tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (objects is not null)
        {
            foreach (CatalogRow row in await this.catalogRows.GetCatalogRowsAsync(objects, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (row.IsDecoded
                    && (row.ObjectType == Constants.SystemObjects.UserTableType
                        || row.ObjectType == Constants.SystemObjects.LinkedOdbcType)
                    && row.TDefPage != tdefPage
                    && this.IsTDefPageCandidate(row.TDefPage))
                {
                    _ = partners.Add(row.TDefPage);
                }
            }
        }

        foreach (long partnerPage in partners)
        {
            await this.RemoveFkEntriesNamingAsync(partnerPage, tdefPage, cancellationToken).ConfigureAwait(false);
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
    private async ValueTask RemoveFkEntriesNamingAsync(long partnerTdefPage, long targetTdefPage, CancellationToken cancellationToken)
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
                cancellationToken).ConfigureAwait(false);
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
    private bool IsTDefPageCandidate(long page)
        => page > 2 && page < this.pager.PageCount;

    /// <summary>
    /// Reads every live <c>MSysRelationships</c> row, or none when the
    /// database has no <c>MSysRelationships</c> table.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The rows, in storage order.</returns>
    private async ValueTask<List<RelationshipRowSnapshot>> CollectAllRelationshipRowsAsync(CancellationToken cancellationToken)
    {
        long msysRelTdefPage = await this.catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            return [];
        }

        TableDef msysRelDef = await this.tableDefs.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        return await this.catalog.CollectRowsAsync(msysRelTdefPage, msysRelDef, _ => true, cancellationToken).ConfigureAwait(false);
    }

    // ════════════════════════════════════════════════════════════════
    // Drop / Rename relationship
    // ════════════════════════════════════════════════════════════════
    //
    // Reverses CreateRelationshipAsync:
    //   • DropRelationshipAsync rewrites MSysRelationships as the remaining
    //     live rows, excluding rows whose szRelationship matches, and
    //     removes the matching FK logical-idx entry from each side's TDEF
    //     (the writer's own and those Access wrote), then conservatively
    //     reclaims any trailing real-idx physical-descriptor slots that the
    //     removal left unreferenced (common case: FK got the last slot on
    //     its TDEF and the slot is reclaimed cleanly; non-trailing orphans
    //     are still left for Compact & Repair to reclaim, since mid-array
    //     compaction would require cross-TDEF rel_idx_num renumbering on
    //     every other table that points at the slot).
    //     ListIndexesAsync iterates by num_idx so the FK stops surfacing
    //     immediately regardless of whether the real-idx slot was reclaimed.
    //   • RenameRelationshipAsync rewrites MSysRelationships as live rows
    //     with szRelationship replaced on every match and updates the
    //     matching FK logical-idx name cookie on each side's TDEF through the
    //     logical-chain writer (on Jet3 the entry also moves to its new name's
    //     sorted position). Relationship Type=8 MSysObjects rows are
    //     deliberately not renamed or deleted here; DAO Compact & Repair
    //     normalizes them from MSysRelationships, while manual mutation of
    //     those rows has proven less compact-safe.

    /// <summary>
    /// Asynchronously deletes a foreign-key relationship and its per-TDEF
    /// logical-index entries.
    /// </summary>
    /// <param name="relationshipName">The case-insensitive relationship name to delete.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="JetNotSupportedException">Thrown when the database has no <c>MSysRelationships</c> table.</exception>
    /// <exception cref="InvalidOperationException">Thrown when no relationship has the supplied name.</exception>
    /// <exception cref="JetOperationException">The operation is refused with a structured <see cref="JetOperationException"/>.</exception>
    internal async ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(relationshipName, nameof(relationshipName));
        Guard.ThrowIfDisposed(this.pager.IsDisposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        long msysRelTdefPage = await this.catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new JetNotSupportedException(JetErrorCode.SystemTableMissing, "The database does not contain a 'MSysRelationships' table; nothing to drop.", new JetErrorInfo { ObjectName = "MSysRelationships" });
        }

        TableDef msysRelDef = await this.tableDefs.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
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
            throw new JetOperationException(JetErrorCode.RelationshipNotFound, $"No relationship named '{relationshipName}' was found.", errorInfo: new JetErrorInfo { RelationshipName = relationshipName });
        }

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
    /// Asynchronously renames a foreign-key relationship and its per-TDEF
    /// logical-index name cookies.
    /// </summary>
    /// <param name="oldName">The case-insensitive existing relationship name.</param>
    /// <param name="newName">The new relationship name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="JetNotSupportedException">Thrown when the database has no <c>MSysRelationships</c> table.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="newName"/> is already taken, no relationship is named <paramref name="oldName"/>, or <c>MSysRelationships</c> has no <c>szRelationship</c> column.</exception>
    /// <exception cref="ArgumentException">Thrown, before anything is read, when <paramref name="newName"/> breaks the Access naming rules (<see cref="AccessObjectName"/>) or is not in a Jet3 database's code page.</exception>
    /// <exception cref="JetOperationException">The operation is refused with a structured <see cref="JetOperationException"/>.</exception>
    /// <exception cref="JetObjectExistsException">The operation is refused with a structured <see cref="JetObjectExistsException"/>.</exception>
    internal async ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(oldName, nameof(oldName));
        AccessObjectName.ThrowIfInvalid(newName, nameof(newName), "relationship");
        AccessObjectName.ThrowIfNotStorable(this.format, newName, nameof(newName), "relationship");
        Guard.ThrowIfDisposed(this.pager.IsDisposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (string.Equals(oldName, newName, StringComparison.OrdinalIgnoreCase))
        {
            return; // No-op; matches Microsoft Access' designer behaviour.
        }

        long msysRelTdefPage = await this.catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (msysRelTdefPage <= 0)
        {
            throw new JetNotSupportedException(JetErrorCode.SystemTableMissing, "The database does not contain a 'MSysRelationships' table; nothing to rename.", new JetErrorInfo { ObjectName = "MSysRelationships" });
        }

        TableDef msysRelDef = await this.tableDefs.ReadRequiredTableDefAsync(msysRelTdefPage, Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);

        // Reject collision with an existing name (case-insensitive).
        HashSet<string> existing = await this.catalog.ReadExistingRelationshipNamesAsync(msysRelTdefPage, msysRelDef, cancellationToken).ConfigureAwait(false);
        if (existing.Contains(newName))
        {
            throw new JetObjectExistsException(JetErrorCode.RelationshipExists, $"A relationship named '{newName}' already exists.", new JetErrorInfo { RelationshipName = newName });
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
            throw new JetOperationException(JetErrorCode.RelationshipNotFound, $"No relationship named '{oldName}' was found.", new JetErrorInfo { RelationshipName = oldName });
        }

        int szRelIdx = msysRelDef.FindColumnIndex("szRelationship");
        if (szRelIdx < 0)
        {
            throw new JetOperationException(JetErrorCode.SystemTableMissing, "MSysRelationships does not expose a 'szRelationship' column.");
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
        // on-disk index name matches the catalog row.
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

            CatalogEntry? pkEntry = await this.tableCatalog.GetCatalogEntryAsync(pair.Key.Pk, cancellationToken).ConfigureAwait(false);
            CatalogEntry? fkEntry = await this.tableCatalog.GetCatalogEntryAsync(pair.Key.Fk, cancellationToken).ConfigureAwait(false);
            if (pkEntry == null || fkEntry == null)
            {
                // Catalog row references a missing table — skip TDEF work.
                continue;
            }

            TableDef pkDef = await this.tableDefs.ReadRequiredTableDefAsync(pkEntry.TDefPage, pair.Key.Pk, cancellationToken).ConfigureAwait(false);
            TableDef fkDef = await this.tableDefs.ReadRequiredTableDefAsync(fkEntry.TDefPage, pair.Key.Fk, cancellationToken).ConfigureAwait(false);

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
}
