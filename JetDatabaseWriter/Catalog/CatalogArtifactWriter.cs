namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Executes <see cref="CatalogArtifactPlan"/>s: creates table artifacts (TDEF
/// chain, empty index leaves, usage map, <c>MSysObjects</c> / <c>MSysACEs</c>
/// rows, constraint registration) and applies catalog object inserts,
/// replacements, and deletions. Also bootstraps the <c>MSysObjects</c> indexes
/// of a freshly created database.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pager">The writer's page file, through which TDEF pages are written.</param>
/// <param name="catalog">The cached user-table catalog, invalidated after every catalog mutation.</param>
/// <param name="pageAllocator">Reserves TDEF and index leaf pages.</param>
/// <param name="tdefPageBuilder">Builds and patches table-definition pages.</param>
/// <param name="dataPages">Allocates usage-map pages.</param>
/// <param name="ownedMaps">Records the tables whose owned-page usage maps this writer created.</param>
/// <param name="catalogWriter">Writes <c>MSysObjects</c> and <c>MSysACEs</c> rows.</param>
/// <param name="constraints">Registers client-side column constraints for created tables.</param>
internal sealed class CatalogArtifactWriter(
    JetFormat format,
    Pager pager,
    TableCatalog catalog,
    PageAllocator pageAllocator,
    TDefPageBuilder tdefPageBuilder,
    DataPageInserter dataPages,
    IOwnedMapPolicy ownedMaps,
    CatalogWriter catalogWriter,
    ConstraintRegistry constraints)
{
    private static IReadOnlyList<ColumnDefinition> BuildFullCatalogColumnDefinitions()
        =>
        [
            new("Id", typeof(int)) { IsNullable = false, DescriptorFlagsOverride = 0x13 },
            new("ParentId", typeof(int)) { IsNullable = false, DescriptorFlagsOverride = 0x13 },
            new("Name", typeof(string), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
            new("Type", typeof(short)) { IsNullable = false, DescriptorFlagsOverride = 0x13 },
            new("DateCreate", typeof(DateTime)) { DescriptorFlagsOverride = 0x13 },
            new("DateUpdate", typeof(DateTime)) { DescriptorFlagsOverride = 0x13 },
            new("Owner", typeof(byte[]), maxLength: 255) { DescriptorFlagsOverride = 0x32 },
            new("Flags", typeof(int)) { DescriptorFlagsOverride = 0x13 },
            new("Database", typeof(string)) { DescriptorFlagsOverride = 0x12, IsCompressedUnicode = false },
            new("Connect", typeof(string)) { DescriptorFlagsOverride = 0x12, IsCompressedUnicode = false },
            new("ForeignName", typeof(string), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
            new("RmtInfoShort", typeof(byte[]), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
            new("RmtInfoLong", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("Lv", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("LvProp", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("LvModule", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
            new("LvExtra", typeof(byte[])) { DescriptorFlagsOverride = 0x12 },
        ];

    private static bool ShouldEmitAceRows(CatalogTableArtifact tableArtifact)
        => tableArtifact.EmitAceRows ?? !IsSystemCatalogFlags(tableArtifact.CatalogFlags);

    private static bool IsSystemCatalogFlags(uint catalogFlags)
        => (catalogFlags & Constants.SystemObjects.SystemTableMask) != 0;

    /// <summary>Checks catalog indexes before a plan can allocate or change table pages.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal ValueTask ThrowIfCatalogIndexesUnmaintainableAsync(CancellationToken cancellationToken)
        => catalogWriter.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken);

    /// <summary>
    /// Reserves the contiguous TDEF slots for the core ACCDB system tables
    /// (<c>MSysACEs</c>, <c>MSysQueries</c>, <c>MSysRelationships</c>) of a
    /// freshly created full-catalog database. Returns 0 when no slots are needed:
    /// the writer scaffolds these tables only on ACCDB, with the complex-column
    /// catalog (<see cref="JetFormat.SupportsComplexColumns"/>).
    /// </summary>
    /// <param name="fullCatalogSchema">Whether the full 17-column catalog schema is in use.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<long> ReserveFreshCoreSystemTablePagesAsync(bool fullCatalogSchema, CancellationToken cancellationToken)
        => format.SupportsComplexColumns && fullCatalogSchema
            ? pageAllocator.ReserveContiguousPagesAsync(3, cancellationToken)
            : new ValueTask<long>(0L);

    /// <summary>
    /// Rewrites the freshly created <c>MSysObjects</c> TDEF (page 2) with its
    /// <c>Id</c> primary key and <c>ParentIdName</c> unique index, empty leaf
    /// pages, and a writer-owned usage map. No-op for Jet3 and the slim catalog.
    /// </summary>
    /// <param name="fullCatalogSchema">Whether the full 17-column catalog schema is in use.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">Thrown when the bootstrap TDEF unexpectedly spans multiple pages.</exception>
    internal async ValueTask InitializeFreshCatalogIndexesAsync(bool fullCatalogSchema, CancellationToken cancellationToken)
    {
        if (format.IsJet3 || !fullCatalogSchema)
        {
            return;
        }

        IReadOnlyList<ColumnDefinition> columns = BuildFullCatalogColumnDefinitions();
        TableDef tableDef = TDefPageBuilder.BuildTableDefinition(columns, format);
        var indexes = new IndexDefinition[]
        {
            new("Id", "Id") { IsPrimaryKey = true },
            new("ParentIdName", ["ParentId", "Name"]) { IsUnique = true },
        };

        List<ResolvedIndex> resolvedIndexes = IndexHelpers.ResolveIndexes(indexes, tableDef);
        (byte[][] tdefPages, int[] firstDpLogicalOffsets, int[] usedPagesLogicalOffsets) = tdefPageBuilder.BuildTDefPagesWithIndexOffsets(tableDef, resolvedIndexes);
        if (tdefPages.Length != 1)
        {
            throw new InvalidDataException("Fresh MSysObjects bootstrap unexpectedly produced a multi-page TDEF.");
        }

        tdefPages[0][format.TDef.TableType] = Constants.TableDefinition.SystemTableType;
        IndexPageLayout layout = format.IndexPage;
        long[] leafPageNumbers = new long[resolvedIndexes.Count];
        for (int i = 0; i < resolvedIndexes.Count; i++)
        {
            byte[] leafPage = IndexPageCodec.BuildLeafPage(
                layout,
                format.PageSize,
                parentTdefPage: 2,
                entries: [],
                enablePrefixCompression: false);
            long leafPageNumber = await pageAllocator.AllocatePageAsync(leafPage, cancellationToken).ConfigureAwait(false);
            leafPageNumbers[i] = leafPageNumber;
            tdefPageBuilder.WriteLogicalTDefI32(tdefPages, firstDpLogicalOffsets[i], checked((int)leafPageNumber));
        }

        long usageMapPageNumber = await dataPages.AppendIndexUsageMapPageAsync(leafPageNumbers, cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < usedPagesLogicalOffsets.Length; i++)
        {
            tdefPageBuilder.WriteLogicalUsedPagesPointer(tdefPages, usedPagesLogicalOffsets[i], i + 2, usageMapPageNumber);
        }

        DataPageInserter.PatchUsageMapPointers(tdefPages[0], format.TDef, checked((int)usageMapPageNumber));
        DataPageInserter.PatchAutoNumFlag(tdefPages[0], tableDef);
        await pager.WritePageAsync(2, tdefPages[0], cancellationToken).ConfigureAwait(false);
        ownedMaps.RegisterWritable(2);
        catalog.Invalidate();
    }

    /// <summary>
    /// Table-creation helper that drives the TDEF + leaf + catalog-row pipeline
    /// for a single pre-built <see cref="CatalogTableArtifact"/>, so it can also
    /// emit hidden system tables (e.g. complex-column flat tables that need
    /// <c>MSysObjects.Flags = 0x800A0000</c>). Returns the new TDEF page number.
    /// </summary>
    /// <param name="tableArtifact">The catalog table artifact describing the table to create.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> CreateTableAsync(
        CatalogTableArtifact tableArtifact,
        CancellationToken cancellationToken)
    {
        long[] tablePages = await this.ExecutePlanAsync(
            new CatalogArtifactPlan([tableArtifact], []),
            cancellationToken).ConfigureAwait(false);

        return tablePages[0];
    }

    /// <summary>
    /// Creates every table artifact in <paramref name="plan"/>, then applies its
    /// catalog object inserts, replacements, and deletions in that order.
    /// Returns the TDEF page number of each table artifact, in plan order.
    /// </summary>
    /// <param name="plan">The catalog artifact plan.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long[]> ExecutePlanAsync(
        CatalogArtifactPlan plan,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(plan, nameof(plan));
        await this.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);

        foreach (CatalogTableArtifact artifact in plan.TableArtifacts)
        {
            TableDef definition = TDefPageBuilder.BuildTableDefinition(artifact.Columns, format);
            _ = IndexHelpers.ResolveIndexes(artifact.Indexes, definition);
        }
        long[] tablePages = new long[plan.TableArtifacts.Count];
        for (int artifactIndex = 0; artifactIndex < plan.TableArtifacts.Count; artifactIndex++)
        {
            CatalogTableArtifact tableArtifact = plan.TableArtifacts[artifactIndex];
            tablePages[artifactIndex] = await this.CreateCatalogTableArtifactAsync(tableArtifact, cancellationToken).ConfigureAwait(false);
        }

        for (int artifactIndex = 0; artifactIndex < plan.CatalogObjects.Count; artifactIndex++)
        {
            CatalogObjectArtifact catalogObject = plan.CatalogObjects[artifactIndex];
            _ = await catalogWriter.InsertCatalogObjectAsync(catalogObject, cancellationToken).ConfigureAwait(false);
        }

        for (int artifactIndex = 0; artifactIndex < plan.CatalogReplacements.Count; artifactIndex++)
        {
            UserTableCatalogReplacementArtifact replacement = plan.CatalogReplacements[artifactIndex];
            _ = await catalogWriter.ReplaceUserTableCatalogEntryAsync(
                replacement.ExistingName,
                replacement.ReplacementName,
                replacement.TDefPage,
                replacement.LvProp,
                replacement.IncludeSystemTables,
                replacement.Operation ?? $"replacing catalog row for '{replacement.ExistingName}'",
                replacement.MissingMessage,
                cancellationToken).ConfigureAwait(false);
        }

        for (int artifactIndex = 0; artifactIndex < plan.CatalogDeletions.Count; artifactIndex++)
        {
            UserTableCatalogDeletionArtifact deletion = plan.CatalogDeletions[artifactIndex];
            _ = await catalogWriter.DeleteUserTableCatalogRowsAsync(
                deletion.TableName,
                deletion.TDefPage,
                deletion.IncludeSystemTables,
                deletion.ThrowIfNotFound,
                deletion.Operation ?? $"deleting catalog row for '{deletion.TableName}'",
                deletion.MissingMessage,
                cancellationToken).ConfigureAwait(false);
        }

        if (plan.CatalogObjects.Count > 0 || plan.CatalogReplacements.Count > 0 || plan.CatalogDeletions.Count > 0)
        {
            catalog.Invalidate();
        }

        return tablePages;
    }

    private async ValueTask<long> CreateCatalogTableArtifactAsync(CatalogTableArtifact tableArtifact, CancellationToken cancellationToken)
    {
        TableDef tableDef = TDefPageBuilder.BuildTableDefinition(tableArtifact.Columns, format);
        List<ResolvedIndex> resolvedIndexes = IndexHelpers.ResolveIndexes(tableArtifact.Indexes, tableDef);
        (byte[][] tdefPages, int[] firstDpLogicalOffsets, int[] usedPagesLogicalOffsets) = tdefPageBuilder.BuildTDefPagesWithIndexOffsets(tableDef, resolvedIndexes);
        if (tableArtifact.ReservedTdefPageNumber > 0 && tdefPages.Length != 1)
        {
            throw new InvalidDataException("Reserved fresh system-table TDEF slots support only single-page TDEFs.");
        }

        if (tableArtifact.MarkSystemTableTdef && IsSystemCatalogFlags(tableArtifact.CatalogFlags))
        {
            tdefPages[0][format.TDef.TableType] = Constants.TableDefinition.SystemTableType;
        }

        // Reserve all TDEF pages first (sequential page numbers). The first
        // page's number is the table's catalog ID; subsequent pages are
        // chained via the next-page pointer at offset 4 of each non-last
        // page. Leaf pages and the usage-map page are allocated AFTER, so
        // they don't interleave with the TDEF chain and the page numbers
        // stay contiguous (tdefPages[i] lives at file page tdefPageNumber + i).
        long tdefPageNumber = tableArtifact.ReservedTdefPageNumber > 0
            ? tableArtifact.ReservedTdefPageNumber
            : await pageAllocator.ReserveContiguousPagesAsync(tdefPages.Length, cancellationToken).ConfigureAwait(false);

        // Stamp the next-page pointer at offset 4 of every non-last TDEF page.
        for (int pageIndex = 0; pageIndex < tdefPages.Length - 1; pageIndex++)
        {
            Wi32(tdefPages[pageIndex], 4, checked((int)(tdefPageNumber + pageIndex + 1)));
        }

        for (int pageIndex = 0; pageIndex < tdefPages.Length; pageIndex++)
        {
            await pager.WritePageAsync(tdefPageNumber + pageIndex, tdefPages[pageIndex], cancellationToken).ConfigureAwait(false);
        }

        bool tdefDirty = false;

        long[]? leafPageNumbers = null;

        // Emit one empty index leaf page per real index and patch its page
        // number into the corresponding `first_dp` field of the real-idx physical
        // descriptor. The leaf starts empty because CreateTableAsync inserts no
        // rows; subsequent inserts/updates/deletes maintain the B-tree via
        // MaintainIndexesAsync. See
        // docs/design/index-and-relationship-format-notes.md §7.
        if (resolvedIndexes.Count > 0)
        {
            IndexPageLayout layout = format.IndexPage;
            leafPageNumbers = new long[resolvedIndexes.Count];

            for (int indexIndex = 0; indexIndex < resolvedIndexes.Count; indexIndex++)
            {
                byte[] leafPage = IndexPageCodec.BuildLeafPage(
                    layout,
                    format.PageSize,
                    tdefPageNumber,
                    [],
                    enablePrefixCompression: false);
                long leafPageNumber = await pageAllocator.AllocatePageAsync(leafPage, cancellationToken).ConfigureAwait(false);
                leafPageNumbers[indexIndex] = leafPageNumber;
                tdefPageBuilder.WriteLogicalTDefI32(tdefPages, firstDpLogicalOffsets[indexIndex], checked((int)leafPageNumber));
            }

            tdefDirty = true;
        }

        // Allocate a per-table usage-map data page and patch the TDEF
        // `used_pages` / `free_pages` pointers (Jet4/ACE only). DAO Compact
        // & Repair walks every catalog row and dereferences `used_pages` to
        // enumerate the table's data pages; a zero pointer here aborts the
        // walk with "could not find object 'MSysDb'". The companion
        // `autonum_flag` byte at TDEF offset 0x18 is patched unconditionally
        // to 0x01: per Jackcess and verified empirically against
        // NorthwindTraders.accdb (every user table has byte 0x18 == 0x01,
        // including ones without an autonumber column). See
        // docs/design/round-trip-openrecordset-hypothesis.md.
        if (tableArtifact.EmitUsageMap && !format.IsJet3)
        {
            long usageMapPageNumber = leafPageNumbers is null
                ? await dataPages.AppendUsageMapPageAsync(cancellationToken).ConfigureAwait(false)
                : await dataPages.AppendIndexUsageMapPageAsync(leafPageNumbers, cancellationToken).ConfigureAwait(false);

            if (leafPageNumbers is not null)
            {
                for (int usedPagesIndex = 0; usedPagesIndex < usedPagesLogicalOffsets.Length; usedPagesIndex++)
                {
                    tdefPageBuilder.WriteLogicalUsedPagesPointer(
                        tdefPages,
                        usedPagesLogicalOffsets[usedPagesIndex],
                        usedPagesIndex + 2,
                        usageMapPageNumber);
                }
            }

            // PatchUsageMapPointers / PatchAutoNumFlag write only into the
            // TDEF header (offset 0x18 and the used_pages / free_pages pointers), which always live on
            // the first physical page.
            DataPageInserter.PatchUsageMapPointers(tdefPages[0], format.TDef, checked((int)usageMapPageNumber));
            DataPageInserter.PatchAutoNumFlag(tdefPages[0], tableDef);
            ownedMaps.RegisterWritable(tdefPageNumber);
            tdefDirty = true;
        }

        if (tdefDirty)
        {
            // Re-flush every TDEF page with the patched first_dp / usage-map /
            // autonum bytes and (for multi-page chains) the next-page pointers.
            for (int pageIndex = 0; pageIndex < tdefPages.Length; pageIndex++)
            {
                await pager.WritePageAsync(tdefPageNumber + pageIndex, tdefPages[pageIndex], cancellationToken).ConfigureAwait(false);
            }
        }

        // A schema rewrite hands over the original table's properties projected
        // onto the rebuilt columns; a new table's come from its column definitions.
        byte[]? lvProp = null;
        if (tableArtifact.EmitLvProp)
        {
            lvProp = tableArtifact.PersistedProperties is { } persisted
                ? persisted.ToBytes(format.Kind)
                : JetExpressionConverter.BuildLvPropBlob(tableArtifact.Columns, format.Kind);
        }

        await catalogWriter.InsertCatalogEntryAsync(
            tableArtifact.TableName,
            tdefPageNumber,
            lvProp,
            tableArtifact.CatalogFlags,
            cancellationToken).ConfigureAwait(false);

        // DAO Compact & Repair requires every user table to have ACE
        // (Access Control Entry) rows in MSysACEs. Without them DAO's
        // security-descriptor pass aborts with err 3011 "MSysDb".
        if (ShouldEmitAceRows(tableArtifact))
        {
            await catalogWriter.InsertAceRowsForTableAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        }

        if (tableArtifact.RegisterConstraints)
        {
            constraints.Register(tableArtifact.TableName, tableArtifact.Columns);
        }

        catalog.Invalidate();
        return tdefPageNumber;
    }
}
