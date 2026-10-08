namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;

#pragma warning disable SA1204

/// <summary>
/// Foreign-key relationship management for <see cref="AccessWriter"/>:
/// create/drop/rename policy and catalog lifecycle, the
/// relationship state carried through copy-and-swap schema rewrites, and the
/// relationship check and partner unlinking behind dropping a table.
/// MSysRelationships row operations live in <see cref="RelationshipCatalogStore"/>,
/// and runtime referential-integrity enforcement lives in <see cref="RelationshipEnforcer"/>.
/// Physical FK metadata lives in <see cref="ForeignKeyMetadataEditor"/>.
/// The public facade owns the auto-commit scope around each workflow.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="pager">The writer's page file, through which TDEF chains are written.</param>
/// <param name="tableCatalog">Resolves the primary and foreign tables by name.</param>
/// <param name="indexes">Rebuilds FK index leaves after the per-TDEF entries change.</param>
/// <param name="metadata">Owns physical FK indexes, TDEF chains and partner links.</param>
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
    ForeignKeyMetadataEditor metadata,
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
    private readonly ForeignKeyMetadataEditor metadata = metadata;
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
    /// engine are emitted by <see cref="ForeignKeyMetadataEditor.EmitFkPerTdefEntriesAsync"/> on
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

        await this.catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: true, cancellationToken).ConfigureAwait(false);

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

        RelationshipColumnPolicy.ThrowIfCalculated(primaryDef, relationship.PrimaryColumns, relationship.PrimaryTable, relationship.Name);
        RelationshipColumnPolicy.ThrowIfCalculated(foreignDef, relationship.ForeignColumns, relationship.ForeignTable, relationship.Name);

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
            throw new JetNotSupportedException(JetErrorCode.SystemTableMissing, "The database does not contain the 'MSysRelationships' table required to create a relationship.", new JetErrorInfo { ObjectName = "MSysRelationships" });
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

        // Native Access stores unenforced relationships only in the catalog.
        // Reciprocal FK entries activate engine enforcement even when grbit has no RI.
        if (!relationship.EnforceReferentialIntegrity)
        {
            return;
        }

        // Per-TDEF FK logical-idx entries: add index_type=0x02 logical-idx
        // entries on both PK-side and FK-side TDEFs with cross-referenced
        // rel_idx_num / rel_tbl_page so the JET engine can locate the partner
        // table without waiting for Microsoft Access Compact & Repair to
        // regenerate them from the MSysRelationships rows above. See
        // docs/design/index-and-relationship-format-notes.md §3.2 and §7.
        await this.metadata.EmitFkPerTdefEntriesAsync(
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
    // A rewrite refuses invalid partner links before creating the copy.

    /// <summary>
    /// Captures a table's FK logical indexes and catalog key columns before
    /// a schema rewrite, refusing partner metadata that cannot be preserved.
    /// </summary>    /// <param name="tableName">The table about to be rewritten.</param>
    /// <param name="tdefPage">The table's current TDEF page.</param>
    /// <param name="tableDef">The table's current definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The captured state.</returns>
    /// <exception cref="JetOperationException">A foreign-key partner link is invalid.</exception>
    internal async ValueTask<RelationshipRewriteState> CaptureForRewriteAsync(
        string tableName,
        long tdefPage,
        TableDef tableDef,
        CancellationToken cancellationToken)
    {
        var fkEntries = new List<FkLogicalIndexSnapshot>();
        foreach (FkLogicalIndexSnapshot entry in await this.metadata.ReadFkLogicalIndexesAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false))
        {
            if (entry.RelIdxNum < 0
                || await this.metadata.PartnerLinksBackAsync(entry.RelTblPage, entry.RelIdxNum, tdefPage, entry.IndexNumber, cancellationToken).ConfigureAwait(false))
            {
                fkEntries.Add(entry);
            }
            else
            {
                throw new JetOperationException(JetErrorCode.RelationshipTargetNotFound, $"Cannot rewrite table '{tableName}': foreign-key index '{entry.Name}' has an invalid partner link.", errorInfo: new JetErrorInfo { TableName = tableName, Reason = "Invalid foreign-key partner link" });
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
        if (state.FkEntries.Count > 0)
        {
            await this.catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: true, cancellationToken).ConfigureAwait(false);
        }

        return await this.metadata.EmitFkEntriesForRewriteAsync(state, targetTdefPage, targetDef, finalTdefPage, mapColumnName, cancellationToken).ConfigureAwait(false);
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

        await this.catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: true, cancellationToken).ConfigureAwait(false);

        foreach (FkLogicalIndexSnapshot entry in state.FkEntries)
        {
            if (entry.RelTblPage == state.TDefPage || entry.RelIdxNum < 0)
            {
                continue;
            }

            int? newIndexNumber = newIndexNumbers.TryGetValue(entry.IndexNumber, out int number) ? number : null;
            await this.metadata.UpdatePartnerFkEntryAsync(
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
    // row names in an inconsistent catalog. Once
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

    /// <summary>Plans relationships to remove with a table, refusing enforced relationships to another table.</summary>
    /// <param name="tableName">The table being dropped.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The removable relationship names.</returns>
    /// <exception cref="JetOperationException">An enforced relationship connects the table to another table.</exception>
    internal async ValueTask<IReadOnlyList<string>> PlanTableDropRelationshipsAsync(string tableName, CancellationToken cancellationToken)
    {
        var removable = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
        var restricted = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (RelationshipRowSnapshot row in await this.CollectAllRelationshipRowsAsync(cancellationToken).ConfigureAwait(false))
        {
            bool primary = string.Equals(row.SzReferencedObject, tableName, StringComparison.OrdinalIgnoreCase);
            bool foreign = string.Equals(row.SzObject, tableName, StringComparison.OrdinalIgnoreCase);
            if (!primary && !foreign)
            {
                continue;
            }

            if ((!primary || !foreign) && (row.Grbit & Constants.RelationshipFlags.NoRefIntegrity) == 0)
            {
                _ = restricted.Add(row.SzRelationship);
            }
            else
            {
                _ = removable.Add(row.SzRelationship);
            }
        }

        EnsureTableHasNoRelationships(tableName, [.. restricted]);
        return [.. removable];
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
        await this.catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: true, cancellationToken).ConfigureAwait(false);

        SortedSet<long>? partners = await this.metadata.ReadPartnerPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (partners is null)
        {
            return;
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
                    && this.metadata.IsTDefPageCandidate(row.TDefPage))
                {
                    _ = partners.Add(row.TDefPage);
                }
            }
        }

        foreach (long partnerPage in partners)
        {
            await this.metadata.RemoveFkEntriesNamingAsync(partnerPage, tdefPage, cancellationToken).ConfigureAwait(false);
        }
    }

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

        await this.catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: true, cancellationToken).ConfigureAwait(false);

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
                int pkReleased = await this.metadata.TryRemoveFkLogicalIdxEntryAsync(ctx.PkEntry.TDefPage, ctx.PkColNums, ctx.FkEntry.TDefPage, ct).ConfigureAwait(false);
                int fkReleased = await this.metadata.TryRemoveFkLogicalIdxEntryAsync(ctx.FkEntry.TDefPage, ctx.FkColNums, ctx.PkEntry.TDefPage, ct).ConfigureAwait(false);

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
                    await this.metadata.TryReclaimTrailingRealIdxAsync(ctx.PkEntry.TDefPage, ct).ConfigureAwait(false);
                }

                if (fkReleased >= 0 && ctx.PkEntry.TDefPage != ctx.FkEntry.TDefPage)
                {
                    await this.metadata.TryReclaimTrailingRealIdxAsync(ctx.FkEntry.TDefPage, ct).ConfigureAwait(false);
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

        await this.catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: true, cancellationToken).ConfigureAwait(false);

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

        // Update the child-side name to follow the visible catalog relationship.
        await this.ForEachRelationshipFkPairAsync(
            matches,
            async (ctx, ct) =>
            {
                // The parent-side name is an internal .rB/.rC identifier,
                // independent of the visible relationship name.
                string newFkBase = ctx.PkEntry.TDefPage == ctx.FkEntry.TDefPage
                    ? newName + "_FK"
                    : newName;
                string newFkName = await this.metadata.PickUniqueLogicalIdxNameAsync(ctx.FkEntry.TDefPage, newFkBase, ct).ConfigureAwait(false);
                _ = await this.metadata.TryRenameFkLogicalIdxNameAsync(ctx.FkEntry.TDefPage, ctx.FkColNums, ctx.PkEntry.TDefPage, newFkName, ct).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
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
