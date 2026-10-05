namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// The user-table catalog of one database file: the <c>MSysObjects</c> rows of
/// type 1 without the system flag, scanned once and cached until a catalog
/// writer calls <see cref="Invalidate"/>. Shared by the reader and writer
/// service graphs, so both resolve table names the same way.
/// </summary>
/// <param name="pages">The file's pages; the scan summary records their count.</param>
/// <param name="tableDefs">Reads table definitions.</param>
/// <param name="catalogRows">Decodes the <c>MSysObjects</c> rows.</param>
/// <param name="properties">
/// Reads persisted column properties. When supplied,
/// <see cref="ResolveRequiredTableAsync"/> sets each calculated column's
/// <see cref="ColumnInfo.CalculatedResultType"/>, so the writer encodes the
/// cached value by the type Access stores it as. The reader graph resolves
/// definitions through <see cref="CatalogReader"/> and passes <see langword="null"/>.
/// </param>
internal sealed class TableCatalog(IPageSource pages, TableDefReader tableDefs, CatalogRowReader catalogRows, ColumnPropertyReader? properties = null)
{
    /// <summary>
    /// The calculated-column result types read for each TDEF page, by column
    /// name, so the <c>MSysObjects</c> scan behind them runs once per table
    /// until <see cref="Invalidate"/>. Guarded by its own lock.
    /// </summary>
    private readonly Dictionary<long, Dictionary<string, ColumnType>> calculatedResultTypes = [];

    /// <summary>
    /// The cached user-table list. A single reference: a volatile write of a
    /// fully built list is atomic, so readers see either the old or the new
    /// list, never a torn value.
    /// </summary>
    private volatile List<CatalogEntry>? userTables;

    /// <summary>The number of <see cref="Invalidate"/> calls so far; see <see cref="Generation"/>.</summary>
    private int generation;

    /// <summary>
    /// Gets a summary of the most recent catalog scan, or <see langword="null"/>
    /// before the first scan. Survives <see cref="Invalidate"/> so diagnostics
    /// keep describing the last scan that ran.
    /// </summary>
    internal CatalogScanSummary? LastScan { get; private set; }

    /// <summary>
    /// Gets a number that changes on every <see cref="Invalidate"/>: after each
    /// catalog write (creating, dropping or rewriting a table, adding a catalog
    /// object) and after a rollback or a failed commit. A cache of anything
    /// derived from the catalog records the generation it was built under and
    /// is stale once the generation moves on.
    /// </summary>
    internal int Generation => Volatile.Read(ref this.generation);

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken = default)
    {
        List<CatalogEntry>? cached = this.userTables;
        if (cached != null)
        {
            return cached;
        }

        cancellationToken.ThrowIfCancellationRequested();

        CatalogScanSummary scan = await this.ScanAsync(cancellationToken).ConfigureAwait(false);
        this.LastScan = scan;
        this.userTables = scan.UserTables;
        return scan.UserTables;
    }

    /// <summary>Finds a user table's catalog entry by name (case-insensitive).</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<CatalogEntry?> GetCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
    {
        List<CatalogEntry> tables = await this.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return tables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Finds a user table's catalog entry by name, or throws when it does not exist.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when no user table is named <paramref name="tableName"/>.</exception>
    internal async ValueTask<CatalogEntry> GetRequiredCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
        => await this.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"Table '{tableName}' was not found.");

    /// <summary>
    /// Resolves a user table's catalog entry and table definition, or throws when
    /// either is missing. When this catalog has a <see cref="ColumnPropertyReader"/>,
    /// each calculated column carries its persisted <c>ResultType</c>.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<ResolvedTable> ResolveRequiredTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        CatalogEntry entry = await this.GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await tableDefs.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        await this.ApplyCalculatedResultTypesAsync(entry.TDefPage, tableDef, cancellationToken).ConfigureAwait(false);
        return new ResolvedTable(entry, tableDef);
    }

    /// <summary>
    /// Reads the table definition rooted at <paramref name="tdefPage"/>, or returns
    /// <see langword="null"/> when the page holds none. When this catalog has a
    /// <see cref="ColumnPropertyReader"/>, each calculated column carries its persisted
    /// <c>ResultType</c>. That type comes from the table's <c>MSysObjects</c> row, so
    /// a caller that deletes the row reads the definition first.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken = default)
    {
        TableDef? tableDef = await tableDefs.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (tableDef is not null)
        {
            await this.ApplyCalculatedResultTypesAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
        }

        return tableDef;
    }

    /// <summary>
    /// Discards the cached user-table list and calculated result types so the
    /// next lookup re-scans <c>MSysObjects</c>, and moves <see cref="Generation"/>
    /// on so caches derived from the catalog reload too.
    /// </summary>
    internal void Invalidate()
    {
        this.userTables = null;
        lock (this.calculatedResultTypes)
        {
            this.calculatedResultTypes.Clear();
        }

        _ = Interlocked.Increment(ref this.generation);
    }

    private async ValueTask ApplyCalculatedResultTypesAsync(long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        if (properties is null || !tableDef.Columns.Exists(static c => c.IsCalculated))
        {
            return;
        }

        Dictionary<string, ColumnType>? resultTypes;
        lock (this.calculatedResultTypes)
        {
            _ = this.calculatedResultTypes.TryGetValue(tdefPage, out resultTypes);
        }

        if (resultTypes is null)
        {
            // Hydrate once from LvProp, then remember the types by column name.
            _ = await properties.HydrateCalculatedResultTypesAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
            resultTypes = new Dictionary<string, ColumnType>(StringComparer.OrdinalIgnoreCase);
            foreach (ColumnInfo column in tableDef.Columns)
            {
                if (column.IsCalculated && column.CalculatedResultType != default)
                {
                    resultTypes[column.Name] = column.CalculatedResultType;
                }
            }

            lock (this.calculatedResultTypes)
            {
                this.calculatedResultTypes[tdefPage] = resultTypes;
            }

            return;
        }

        bool changed = false;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo column = tableDef.Columns[i];
            if (column.IsCalculated
                && resultTypes.TryGetValue(column.Name, out ColumnType resultType)
                && resultType != column.CalculatedResultType)
            {
                tableDef.Columns[i] = column.WithCalculatedResultType(resultType);
                changed = true;
            }
        }

        if (changed)
        {
            tableDef.InitializeColumnMetadata();
        }
    }

    private async ValueTask<CatalogScanSummary> ScanAsync(CancellationToken cancellationToken)
    {
        long totalPages = pages.PageCount;
        TableDef? msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return new CatalogScanSummary(null, HasRequiredColumns: false, CatalogPageCount: 0, RowsScanned: 0, totalPages, []);
        }

        if (msys.FindColumn("Name") == null || msys.FindColumn("Type") == null)
        {
            return new CatalogScanSummary(msys, HasRequiredColumns: false, CatalogPageCount: 0, RowsScanned: 0, totalPages, []);
        }

        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        var result = new List<CatalogEntry>();
        var catalogPages = new HashSet<long>();
        int rowsDecoded = 0;
        foreach (CatalogRow row in rows)
        {
            _ = catalogPages.Add(row.PageNumber);
            if (!row.IsDecoded)
            {
                continue;
            }

            rowsDecoded++;
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if ((unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) != 0)
            {
                continue;
            }

            if (string.IsNullOrEmpty(row.Name) || row.TDefPage <= 0)
            {
                continue;
            }

            result.Add(new CatalogEntry(row.Name, row.TDefPage));
        }

        return new CatalogScanSummary(msys, HasRequiredColumns: true, catalogPages.Count, rowsDecoded, totalPages, result);
    }
}
