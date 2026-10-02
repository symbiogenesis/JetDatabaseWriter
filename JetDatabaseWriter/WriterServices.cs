namespace JetDatabaseWriter;

using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueEncoding;

/// <summary>
/// Composition root for one <see cref="AccessWriter"/>. Builds every writer
/// collaborator once and passes each one the <see cref="AccessBase"/> page I/O
/// and format context plus the specific sibling services it uses. No
/// collaborator receives the facade or this object, so the service graph is
/// acyclic and each dependency is visible in a constructor signature.
/// </summary>
internal sealed class WriterServices
{
    /// <summary>
    /// Initializes a new instance of the <see cref="WriterServices"/> class.
    /// </summary>
    /// <param name="db">The writer's page I/O and format context.</param>
    /// <param name="options">The writer options.</param>
    /// <param name="byteRangeLock">The cooperative JET byte-range lock bound to the writer's stream.</param>
    /// <param name="snapshots">Reads decoded snapshots of the writer's own database.</param>
    internal WriterServices(AccessBase db, AccessWriterOptions options, JetByteRangeLock byteRangeLock, TableSnapshotReader snapshots)
    {
        this.CatalogRows = new CatalogRowReader(db);
        this.PageAllocator = new PageAllocator(db, options);

        var tdefPageBuilder = new TDefPageBuilder(db);
        var longValueEncoder = new LongValueEncoder(db, this.PageAllocator);
        var dataPages = new DataPageInserter(db, this.PageAllocator, this.CatalogRows);
        var tableRows = new TableRowStore(db, options, longValueEncoder, new RowEncoder(db), dataPages, tdefPageBuilder);
        var constraints = new ConstraintRegistry(
            snapshots.ReadTableSnapshotAsync,
            async (tableName, ct) =>
            {
                CatalogEntry? entry = await db.GetCatalogEntryAsync(tableName, ct).ConfigureAwait(false);
                if (entry is null)
                {
                    return null;
                }

                return await snapshots.ReadLvPropBlockAsync(entry.TDefPage, ct).ConfigureAwait(false);
            });

        this.Indexes = new IndexMaintainer(db, this.PageAllocator, tableRows, dataPages, snapshots);
        var catalogWriter = new CatalogWriter(db, tableRows, this.Indexes, longValueEncoder, constraints, this.CatalogRows);
        this.CatalogArtifacts = new CatalogArtifactWriter(db, this.PageAllocator, tdefPageBuilder, dataPages, catalogWriter, constraints);
        this.ComplexColumns = new ComplexColumnManager(db, tableRows, this.Indexes, this.CatalogArtifacts, this.CatalogRows, constraints);

        var relationshipCatalog = new RelationshipCatalogStore(db, this.Indexes, this.CatalogRows, snapshots);
        var enforcer = new RelationshipEnforcer(db, tableRows, this.Indexes, relationshipCatalog, this.ComplexColumns, snapshots);
        this.Relationships = new RelationshipManager(db, this.Indexes, this.PageAllocator, this.CatalogArtifacts, this.CatalogRows, relationshipCatalog);

        this.Transactions = new TransactionLifecycle(db, options, byteRangeLock);
        this.Data = new TableDataWriter(
            db,
            tableRows,
            this.Indexes,
            new UniqueIndexChecker(db, snapshots),
            new AutoNumberMaintainer(db),
            constraints,
            enforcer,
            this.ComplexColumns,
            snapshots);
        this.Schema = new TableSchemaEditor(
            db,
            tableRows,
            this.Indexes,
            this.PageAllocator,
            longValueEncoder,
            catalogWriter,
            this.CatalogArtifacts,
            this.ComplexColumns,
            constraints,
            snapshots);
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

    /// <summary>Gets the read-only <c>MSysObjects</c> scanner.</summary>
    internal CatalogRowReader CatalogRows { get; }

    /// <summary>Gets the index B-tree maintainer.</summary>
    internal IndexMaintainer Indexes { get; }

    /// <summary>Gets the global page allocator.</summary>
    internal PageAllocator PageAllocator { get; }
}
