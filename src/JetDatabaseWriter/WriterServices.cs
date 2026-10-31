namespace JetDatabaseWriter;

using System;
using System.Globalization;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueEncoding;

/// <summary>
/// Composition root for one <see cref="AccessWriter"/>. Builds every writer
/// collaborator once and passes each one the format, page, TDEF and owned-page
/// parts it uses, the writer's <see cref="Pager"/> when it writes pages,
/// and the specific sibling services it uses. Only this graph holds the pager,
/// which <see cref="DatabaseFile.ForWriter"/> hands to the writer alone. No
/// collaborator receives the facade or this object, so the service graph is
/// acyclic and each dependency is visible in a constructor signature.
/// </summary>
internal sealed class WriterServices : IDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="WriterServices"/> class.
    /// </summary>
    /// <param name="db">The open database file.</param>
    /// <param name="pager">The database file's pager, from <see cref="DatabaseFile.ForWriter"/>; the services that write pages write through it.</param>
    /// <param name="options">The writer options.</param>
    /// <param name="byteRangeLock">The cooperative JET byte-range lock bound to the database stream.</param>
    internal WriterServices(DatabaseFile db, Pager pager, AccessWriterOptions options, JetByteRangeLock byteRangeLock)
    {
        // Decoded reads of the writer's own rows go through the same database
        // file the writer writes, so they see an active transaction's journal.
        // A capacity-0 page cache keeps nothing between calls, and names
        // resolve through the writer's catalog, so DDL and rollback cannot
        // leave a stale snapshot behind.
        this.SnapshotPages = new ReaderPageCache(db.Format, db.Pages, capacity: 0);
        ReaderPageCache snapshotPages = this.SnapshotPages;
        var snapshotRows = new RowDecoder(db.Format, db.OwnedPages, snapshotPages, new LongValueDecoder(db.Format, snapshotPages), strictParsing: true);
        var columnProperties = new ColumnPropertyReader(db.Format, db.TableDefs, snapshotRows);

        this.CatalogRows = new CatalogRowReader(db.Format, db.TableDefs, db.OwnedPages);
        this.Catalog = new TableCatalog(db.Pages, db.TableDefs, this.CatalogRows, columnProperties);
        this.PageAllocator = new PageAllocator(db.Format, pager, options);
        this.TDefWriter = new TDefWriter(pager, db.TableDefs);

        TableCatalog catalog = this.Catalog;

        var snapshotCatalog = new CatalogReader(db.Format, db.TableDefs, catalog, this.CatalogRows, snapshotRows);
        var snapshots = new TableSnapshotReader(
            db.Format,
            db.Pages,
            db.TableDefs,
            db.OwnedPages,
            snapshotRows,
            snapshotCatalog);
        this.Snapshots = snapshots;
        var tdefPageBuilder = new TDefPageBuilder(db.Format, pager);
        var longValueEncoder = new LongValueEncoder(db.Format, pager, this.PageAllocator, options, new LongValueUsageMapMaintainer(db.Format, pager, this.PageAllocator, db.TableDefs, this.CatalogRows, catalog));
        this.OwnedMaps = new CatalogOwnedMapPolicy(db.Format, db.TableDefs, this.CatalogRows, db.Pages, pager, this.PageAllocator, this.TDefWriter);
        pager.AddWriteObserver(this.OwnedMaps);
        var usageMaps = new UsageMapEditor(db.Format, pager, this.PageAllocator);
        var dataPages = new DataPageInserter(db.Format, db.OwnedPages, pager, this.PageAllocator, this.OwnedMaps, usageMaps);
        var tableRows = new TableRowStore(db.Format, db.OwnedPages, pager, options, longValueEncoder, new RowEncoder(db.Format), dataPages, tdefPageBuilder);
        var autoNumbers = new AutoNumberMaintainer(db.Format, db.TableDefs, db.OwnedPages, pager);
        CatalogRowReader catalogRows = this.CatalogRows;
        var complexReferenceSeeds = new ComplexReferenceSeedReader(db.Format, db.TableDefs, db.OwnedPages, catalogRows, autoNumbers);
        var constraints = new ConstraintRegistry(
            async (tableName, ct) =>
            {
                CatalogEntry? entry = await catalog.GetCatalogEntryAsync(tableName, ct).ConfigureAwait(false);
                if (entry is null)
                {
                    return null;
                }

                return (await catalog.GetSchemaAsync(entry.TDefPage, cancellationToken: ct).ConfigureAwait(false))?.Properties;
            },
            async (tableName, tableDef, columnIndex, ct) =>
            {
                // Complex-column flat tables are system tables, so they are
                // not in the user-table catalog.
                CatalogEntry? entry = await catalog.GetCatalogEntryAsync(tableName, ct).ConfigureAwait(false);
                long tdefPage = entry?.TDefPage
                    ?? await catalogRows.FindSystemTableTdefPageAsync(tableName, ct).ConfigureAwait(false);
                return await autoNumbers.ReadUsedHighWaterAsync(tdefPage, tableDef, columnIndex, ct).ConfigureAwait(false);
            },
            async (tableName, tableDef, ct) =>
            {
                CatalogEntry? entry = await catalog.GetCatalogEntryAsync(tableName, ct).ConfigureAwait(false);
                return entry is null
                    ? 0
                    : await complexReferenceSeeds.ReadSeedAsync(entry.TDefPage, tableDef, ct).ConfigureAwait(false);
            },
            new JetDatabaseWriter.Indexes.Collation.JetTextCollation(db.Format.DefaultTextSortOrder),
            JetDatabaseWriter.Schema.Expressions.CalculatedExpressionRegionalProfile.Snapshot(options.CalculatedExpressionCulture ?? CultureInfo.CurrentCulture));

        this.Indexes = new IndexMaintainer(db.Format, db.TableDefs, pager, this.TDefWriter, this.PageAllocator, dataPages, snapshots, catalogRows);
        this.Mutations = new MutationExecutor(db.Format, pager, db.OwnedPages, dataPages, tableRows, this.Indexes, autoNumbers);
        var catalogWriter = new CatalogWriter(db.Format, db.TableDefs, db.OwnedPages, catalog, this.Indexes, this.Mutations, longValueEncoder, this.CatalogRows, pager, snapshots);
        this.CatalogWriter = catalogWriter;
        this.CatalogArtifacts = new CatalogArtifactWriter(db.Format, pager, catalog, this.PageAllocator, tdefPageBuilder, dataPages, this.OwnedMaps, catalogWriter, constraints);
        this.SystemCatalog = new SystemCatalogBootstrapper(db.Format, db.TableDefs, pager, this.Mutations, this.CatalogArtifacts);
        var uniqueIndexes = new UniqueIndexChecker(db.Format, db.Pages, db.TableDefs, snapshots);
        var complexReader = new ComplexColumnReader(db.Format, db.TableDefs, snapshotCatalog, snapshotRows, diagnosticsEnabled: false);
        this.ComplexColumns = new ComplexColumnManager(db.Format, db.TableDefs, db.OwnedPages, pager, catalog, this.Mutations, this.Indexes, this.CatalogArtifacts, this.CatalogRows, constraints, uniqueIndexes, autoNumbers, complexReferenceSeeds, this.Snapshots, complexReader);
        var history = new AppendOnlyHistoryWriter(db.Format, catalog, complexReader, snapshots, complexReferenceSeeds, this.Indexes, constraints, uniqueIndexes);
        var historySchemas = new AppendOnlyHistorySchemaEditor(db.Format, pager, db.TableDefs, this.PageAllocator, catalog, complexReader, snapshots, catalogWriter, catalogRows);

        var relationshipCatalog = new RelationshipCatalogStore(db.Format, db.TableDefs, db.OwnedPages, this.Mutations, this.CatalogRows, snapshots, catalog);
        var enforcer = new RelationshipEnforcer(db.Format, db.Pages, db.TableDefs, db.OwnedPages, catalog, this.Indexes, relationshipCatalog, this.ComplexColumns, snapshots, constraints);
        this.Relationships = new RelationshipManager(db.Format, db.TableDefs, pager, catalog, this.Indexes, new ForeignKeyMetadataEditor(db.Format, db.TableDefs, pager, this.PageAllocator, catalogRows), this.CatalogArtifacts, this.CatalogRows, relationshipCatalog, snapshots);

        this.Transactions = new TransactionLifecycle(db.Format, pager, options, byteRangeLock, catalog, dataPages, this.OwnedMaps, constraints);
        this.Data = new TableDataWriter(
            db.Format,
            db.Pages,
            catalog,
            this.Indexes,
            uniqueIndexes,
            this.Mutations,
            constraints,
            enforcer,
            this.ComplexColumns,
            history,
            snapshots);
        this.RewritePlanner = new TableRewritePlanner(db.Format, catalog, this.Indexes, this.CatalogArtifacts, constraints, this.Relationships, snapshots, autoNumbers);
        var tableStorage = new TableStorageEditor(
            db.Format,
            db.TableDefs,
            db.OwnedPages,
            pager,
            catalog,
            tableRows,
            this.Indexes,
            this.PageAllocator,
            longValueEncoder,
            catalogWriter,
            this.ComplexColumns,
            constraints,
            this.Relationships,
            autoNumbers,
            catalogRows);
        this.Schema = new TableSchemaEditor(
            db.Format,
            pager,
            catalog,
            catalogWriter,
            this.CatalogArtifacts,
            this.ComplexColumns,
            constraints,
            this.Relationships,
            snapshots,
            this.RewritePlanner,
            tableStorage,
            historySchemas);
    }

    /// <summary>Gets the explicit and auto-commit transaction lifecycle.</summary>
    internal TransactionLifecycle Transactions { get; }

    /// <summary>Gets the row insert / update / delete workflows.</summary>
    internal TableDataWriter Data { get; }

    /// <summary>Gets the table and column DDL workflows.</summary>
    internal TableSchemaEditor Schema { get; }

    /// <summary>Gets validation and projection planning for table rewrites.</summary>
    internal TableRewritePlanner RewritePlanner { get; }

    /// <summary>Gets the foreign-key relationship create / drop / rename workflows.</summary>
    internal RelationshipManager Relationships { get; }

    /// <summary>Gets the Attachment / MultiValue subsystem.</summary>
    internal ComplexColumnManager ComplexColumns { get; }

    /// <summary>Gets catalog property, identity and permission mutations.</summary>
    internal CatalogWriter CatalogWriter { get; }

    /// <summary>Gets the catalog-plan executor and fresh-catalog bootstrap.</summary>
    internal CatalogArtifactWriter CatalogArtifacts { get; }

    /// <summary>Gets core system-table and permission bootstrap.</summary>
    internal SystemCatalogBootstrapper SystemCatalog { get; }

    /// <summary>Gets the cached user-table catalog.</summary>
    internal TableCatalog Catalog { get; }

    /// <summary>Gets the read-only <c>MSysObjects</c> scanner.</summary>
    internal CatalogRowReader CatalogRows { get; }

    /// <summary>Gets the decoded reads of the writer's own rows, index metadata, and column properties.</summary>
    internal TableSnapshotReader Snapshots { get; }

    /// <summary>Gets the lease provider for the writer's journal-aware snapshot reads.</summary>
    internal ReaderPageCache SnapshotPages { get; }

    /// <summary>Releases snapshot-cache ownership after writer operations finish.</summary>
    public void Dispose() => this.SnapshotPages.Dispose();

    /// <summary>Gets the index B-tree maintainer.</summary>
    internal IndexMaintainer Indexes { get; }

    /// <summary>Gets the shared prepared-row mutation executor.</summary>
    internal MutationExecutor Mutations { get; }

    /// <summary>Gets the global page allocator.</summary>
    internal PageAllocator PageAllocator { get; }

    /// <summary>Gets the policy that decides whose owned-page usage maps the writer may extend.</summary>
    internal CatalogOwnedMapPolicy OwnedMaps { get; }

    /// <summary>Gets the in-place TDEF write-backs.</summary>
    internal TDefWriter TDefWriter { get; }
}
