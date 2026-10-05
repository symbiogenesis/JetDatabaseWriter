namespace JetDatabaseWriter;

using System;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.ValueDecoding;

/// <summary>
/// Composition root for one <see cref="AccessReader"/>. Builds every reader
/// collaborator once and passes each one the parts of the <see cref="DatabaseFile"/>
/// it reads through (the format profile, the pages, the TDEF reader and the
/// owned-page walk) plus the specific sibling services it uses. The row
/// decoder and the table reader still take the whole file, which they hand to
/// <see cref="RowDecodePlan"/> as the row format source. No
/// collaborator receives the facade or this object, so the service graph is
/// acyclic and each dependency is visible in a constructor signature.
/// </summary>
internal sealed class ReaderServices : IDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ReaderServices"/> class.
    /// </summary>
    /// <param name="db">The open database file.</param>
    /// <param name="options">The reader options.</param>
    internal ReaderServices(DatabaseFile db, AccessReaderOptions options)
    {
        Guard.NotNull(options, nameof(options));

        var linkedSources = new LinkedSourcePolicy(
            LinkedTableManager.CreateLinkedSourceOpenOptions(options, db.DatabasePath),
            db.DatabasePath);

        this.Operations = new AsyncReentrantOperationGate(typeof(AccessReader));
        JetFormat format = db.Profile;
        TableDefReader tableDefs = db.TableDefs;
        this.PageCache = new ReaderPageCache(format, db.Pages, options.PageCacheSize);

        var rows = new RowDecoder(db.Profile, db.OwnedPages, this.PageCache, new LongValueDecoder(format, this.PageCache), options.StrictParsing);
        var catalogRows = new CatalogRowReader(format, tableDefs, db.OwnedPages);
        this.TableCatalog = new TableCatalog(db.Pages, tableDefs, catalogRows);
        this.Catalog = new CatalogReader(format, tableDefs, this.TableCatalog, catalogRows, rows, new ColumnPropertyReader(format, tableDefs, rows));

        var complexColumns = new ComplexColumnReader(format, tableDefs, this.Catalog, rows, options.DiagnosticsEnabled);
        this.LinkedTables = new LinkedTableReader(this.Catalog, linkedSources);
        this.Tables = new TableReader(db.Profile, db.Pages, db.OwnedPages, this.PageCache, rows, this.Catalog, complexColumns, this.LinkedTables, this.Operations, options);
        this.Indexes = new IndexRowReader(format, tableDefs, this.PageCache, rows, this.Catalog, complexColumns, this.Tables, this.Operations);
        this.Schema = new SchemaReader(format, db.Pages, tableDefs, this.PageCache, this.Catalog, complexColumns, this.LinkedTables, this.Tables, this.Operations);
        this.ComplexItems = new ComplexItemReader(complexColumns, this.Operations);
    }

    /// <summary>Gets the gate every reader operation enters, so disposal can wait for in-flight work.</summary>
    internal AsyncReentrantOperationGate Operations { get; }

    /// <summary>Gets the table-data reads.</summary>
    internal TableReader Tables { get; }

    /// <summary>Gets the index metadata, seek, and predicate-inference reads.</summary>
    internal IndexRowReader Indexes { get; }

    /// <summary>Gets the schema, relationship, and statistics reads.</summary>
    internal SchemaReader Schema { get; }

    /// <summary>Gets the attachment and multi-value item reads.</summary>
    internal ComplexItemReader ComplexItems { get; }

    /// <summary>Gets the linked-table reads.</summary>
    internal LinkedTableReader LinkedTables { get; }

    /// <summary>Gets the read-side catalog queries (user and system tables, persisted column properties).</summary>
    internal CatalogReader Catalog { get; }

    /// <summary>Gets the cached user-table catalog.</summary>
    internal TableCatalog TableCatalog { get; }

    /// <summary>Gets the page and row-bound caches.</summary>
    internal ReaderPageCache PageCache { get; }

    /// <summary>Releases the page caches and drops the cached catalog lists.</summary>
    public void Dispose()
    {
        this.PageCache.Dispose();
        this.TableCatalog.Invalidate();
        this.LinkedTables.ClearCache();
    }
}
