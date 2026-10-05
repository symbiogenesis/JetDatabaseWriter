namespace JetDatabaseWriter;

using System.Diagnostics.CodeAnalysis;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Indexes;
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
/// collaborator once and passes each one the <see cref="DatabaseFile"/> it
/// reads pages through, the writer's <see cref="Pager"/> when it writes pages,
/// and the specific sibling services it uses. Only this graph holds the pager,
/// which <see cref="DatabaseFile.ForWriter"/> hands to the writer alone. No
/// collaborator receives the facade or this object, so the service graph is
/// acyclic and each dependency is visible in a constructor signature.
/// </summary>
internal sealed class WriterServices
{
    /// <summary>
    /// Initializes a new instance of the <see cref="WriterServices"/> class.
    /// </summary>
    /// <param name="db">The open database file.</param>
    /// <param name="pager">The database file's pager, from <see cref="DatabaseFile.ForWriter"/>; the services that write pages write through it.</param>
    /// <param name="options">The writer options.</param>
    /// <param name="byteRangeLock">The cooperative JET byte-range lock bound to the database stream.</param>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "A capacity-0 ReaderPageCache allocates no caches, so its Dispose has nothing to release.")]
    internal WriterServices(DatabaseFile db, Pager pager, AccessWriterOptions options, JetByteRangeLock byteRangeLock)
    {
        // Decoded reads of the writer's own rows go through the same database
        // file the writer writes, so they see an active transaction's journal.
        // A capacity-0 page cache keeps nothing between calls, and names
        // resolve through the writer's catalog, so DDL and rollback cannot
        // leave a stale snapshot behind.
        var snapshotPages = new ReaderPageCache(db, capacity: 0);
        var snapshotRows = new RowDecoder(db, snapshotPages, new LongValueDecoder(db, snapshotPages), strictParsing: true);
        var columnProperties = new ColumnPropertyReader(db, snapshotRows);

        this.CatalogRows = new CatalogRowReader(db);
        this.Catalog = new TableCatalog(db, this.CatalogRows, columnProperties);
        this.PageAllocator = new PageAllocator(db, pager, options);
        this.TDefWriter = new TDefWriter(pager, db.TableDefs);

        TableCatalog catalog = this.Catalog;

        var snapshots = new TableSnapshotReader(db, snapshotRows, new CatalogReader(db, catalog, this.CatalogRows, snapshotRows, columnProperties));
        this.Snapshots = snapshots;
        var tdefPageBuilder = new TDefPageBuilder(db, pager);
        var longValueEncoder = new LongValueEncoder(db, pager, this.PageAllocator, options);
        var dataPages = new DataPageInserter(db, pager, this.PageAllocator, this.CatalogRows);
        var tableRows = new TableRowStore(db, pager, options, longValueEncoder, new RowEncoder(db), dataPages, tdefPageBuilder);
        var autoNumbers = new AutoNumberMaintainer(db, pager);
        CatalogRowReader catalogRows = this.CatalogRows;
        var complexReferenceSeeds = new ComplexReferenceSeedReader(db, catalogRows, autoNumbers);
        var constraints = new ConstraintRegistry(
            async (tableName, ct) =>
            {
                CatalogEntry? entry = await catalog.GetCatalogEntryAsync(tableName, ct).ConfigureAwait(false);
                if (entry is null)
                {
                    return null;
                }

                return await snapshots.ReadLvPropBlockAsync(entry.TDefPage, ct).ConfigureAwait(false);
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
            });

        this.Indexes = new IndexMaintainer(db, pager, this.TDefWriter, this.PageAllocator, tableRows, dataPages, snapshots);
        var catalogWriter = new CatalogWriter(db, catalog, tableRows, this.Indexes, longValueEncoder, constraints, this.CatalogRows);
        this.CatalogArtifacts = new CatalogArtifactWriter(db, pager, catalog, this.PageAllocator, tdefPageBuilder, dataPages, catalogWriter, constraints);
        this.ComplexColumns = new ComplexColumnManager(db, pager, catalog, tableRows, this.Indexes, this.CatalogArtifacts, this.CatalogRows, constraints, autoNumbers, complexReferenceSeeds);

        var relationshipCatalog = new RelationshipCatalogStore(db, this.Indexes, this.CatalogRows, snapshots, catalog);
        var enforcer = new RelationshipEnforcer(db, catalog, tableRows, this.Indexes, relationshipCatalog, this.ComplexColumns, snapshots);
        this.Relationships = new RelationshipManager(db, pager, catalog, this.Indexes, this.PageAllocator, this.CatalogArtifacts, this.CatalogRows, relationshipCatalog);

        this.Transactions = new TransactionLifecycle(db, pager, options, byteRangeLock, catalog, dataPages, constraints);
        this.Data = new TableDataWriter(
            db,
            catalog,
            tableRows,
            this.Indexes,
            new UniqueIndexChecker(db, snapshots),
            autoNumbers,
            constraints,
            enforcer,
            this.ComplexColumns,
            snapshots);
        this.Schema = new TableSchemaEditor(
            db,
            pager,
            catalog,
            tableRows,
            this.Indexes,
            this.PageAllocator,
            longValueEncoder,
            catalogWriter,
            this.CatalogArtifacts,
            this.ComplexColumns,
            constraints,
            this.Relationships,
            snapshots,
            autoNumbers);
    }

    /// <summary>Gets the explicit and auto-commit transaction lifecycle.</summary>
    internal TransactionLifecycle Transactions { get; }

    /// <summary>Gets the row insert / update / delete workflows.</summary>
    internal TableDataWriter Data { get; }

    /// <summary>Gets the table and column DDL workflows.</summary>
    internal TableSchemaEditor Schema { get; }

    /// <summary>Gets the foreign-key relationship create / drop / rename workflows.</summary>
    internal RelationshipManager Relationships { get; }

    /// <summary>Gets the Attachment / MultiValue subsystem.</summary>
    internal ComplexColumnManager ComplexColumns { get; }

    /// <summary>Gets the catalog-plan executor and fresh-catalog bootstrap.</summary>
    internal CatalogArtifactWriter CatalogArtifacts { get; }

    /// <summary>Gets the cached user-table catalog.</summary>
    internal TableCatalog Catalog { get; }

    /// <summary>Gets the read-only <c>MSysObjects</c> scanner.</summary>
    internal CatalogRowReader CatalogRows { get; }

    /// <summary>Gets the decoded reads of the writer's own rows, index metadata, and column properties.</summary>
    internal TableSnapshotReader Snapshots { get; }

    /// <summary>Gets the index B-tree maintainer.</summary>
    internal IndexMaintainer Indexes { get; }

    /// <summary>Gets the global page allocator.</summary>
    internal PageAllocator PageAllocator { get; }

    /// <summary>Gets the in-place TDEF write-backs.</summary>
    internal TDefWriter TDefWriter { get; }
}
