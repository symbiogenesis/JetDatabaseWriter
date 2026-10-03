namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;

/// <summary>
/// The user-table catalog of one database file: the <c>MSysObjects</c> rows of
/// type 1 without the system flag, scanned once and cached until a catalog
/// writer calls <see cref="Invalidate"/>. Shared by the reader and writer
/// service graphs, so both resolve table names the same way.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="catalogRows">Decodes the <c>MSysObjects</c> rows.</param>
internal sealed class TableCatalog(DatabaseFile db, CatalogRowReader catalogRows)
{
    /// <summary>
    /// The cached user-table list. A single reference: a volatile write of a
    /// fully built list is atomic, so readers see either the old or the new
    /// list, never a torn value.
    /// </summary>
    private volatile List<CatalogEntry>? userTables;

    /// <summary>
    /// Gets a summary of the most recent catalog scan, or <see langword="null"/>
    /// before the first scan. Survives <see cref="Invalidate"/> so diagnostics
    /// keep describing the last scan that ran.
    /// </summary>
    internal CatalogScanSummary? LastScan { get; private set; }

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

    /// <summary>Resolves a user table's catalog entry and table definition, or throws when either is missing.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<ResolvedTable> ResolveRequiredTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        CatalogEntry entry = await this.GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef tableDef = await db.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);
        return new ResolvedTable(entry, tableDef);
    }

    /// <summary>Discards the cached user-table list so the next lookup re-scans <c>MSysObjects</c>.</summary>
    internal void Invalidate() => this.userTables = null;

    private async ValueTask<CatalogScanSummary> ScanAsync(CancellationToken cancellationToken)
    {
        long totalPages = db.PhysicalPageCount;
        TableDef? msys = await db.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
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
