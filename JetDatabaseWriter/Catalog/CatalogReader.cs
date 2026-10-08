namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Read-side <c>MSysObjects</c> queries: resolves user and system table names
/// to their definitions, locates system tables, reads persisted column
/// properties, and renders the last user-table scan as
/// <see cref="AccessReader.LastDiagnostics"/>.
/// </summary>
/// <param name="format">The file's format profile, which the diagnostics describe.</param>
/// <param name="tableDefs">Reads table definitions.</param>
/// <param name="tables">The cached user-table catalog.</param>
/// <param name="catalogRows">The system-table lookup by name, shared with the writer.</param>
/// <param name="rows">Decodes <c>MSysObjects</c> rows as strings.</param>
internal sealed class CatalogReader(JetFormat format, TableDefReader tableDefs, TableCatalog tables, CatalogRowReader catalogRows, RowDecoder rows)
{
    /// <summary>
    /// Returns the <c>ResultType</c> a calculated column's persisted properties
    /// declare, or <see langword="default"/> when none is recorded.
    /// </summary>
    /// <param name="target">The column's persisted property target.</param>
    internal static ColumnType ResolveCalculatedResultType(ColumnPropertyTarget? target)
        => ColumnPropertyReader.ResolveCalculatedResultType(target);

    /// <summary>Gets a value indicating whether malformed optional metadata is refused.</summary>
    internal bool StrictParsing => rows.StrictParsing;

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<List<CatalogEntry>> GetUserTablesAsync(CancellationToken cancellationToken)
        => tables.GetUserTablesAsync(cancellationToken);

    /// <summary>
    /// Resolves <paramref name="tableName"/> to its catalog entry and table
    /// definition, falling back to system tables (which the user-table catalog
    /// excludes). Returns <see langword="null"/> when no table with columns has
    /// that name.
    /// </summary>
    /// <param name="tableName">The table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<ResolvedTable?> ResolveTableAsync(string tableName, CancellationToken cancellationToken)
    {
        List<CatalogEntry> userTables = await tables.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        CatalogEntry? entry = userTables.Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
        if (entry != null)
        {
            TableSchema? schema = await tables.GetSchemaAsync(entry.TDefPage, loadProperties: false, cancellationToken).ConfigureAwait(false);
            if (schema is { Image.Columns.Count: > 0 })
            {
                return new ResolvedTable(entry, schema);
            }
        }

        // Fall back to a system-table lookup (MSysObjects, MSysRelationships, etc.).
        // The user-table catalog filters out rows whose Flags carry SYSTABLE_MASK,
        // so a name match against the catalog scan is needed for those.
        long sysPage = await this.FindSystemTablePageAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (sysPage > 0)
        {
            TableSchema? systemSchema = await tables.GetSchemaAsync(sysPage, loadProperties: false, cancellationToken).ConfigureAwait(false);
            if (systemSchema is { Image.Columns.Count: > 0 })
            {
                return new ResolvedTable(new CatalogEntry(tableName, sysPage), systemSchema);
            }
        }

        return null;
    }

    /// <summary>
    /// Reads the table definition rooted at <paramref name="tdefPage"/>, with
    /// calculated columns' result types hydrated from the table's persisted
    /// properties. Returns <see langword="null"/> when the page holds no table
    /// definition with columns.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken)
    {
        TableDef? td = await tables.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (td is not { Columns.Count: > 0 })
        {
            return null;
        }

        return td;
    }

    /// <summary>Reads catalog identities after validating local table references.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="maxEntries">The maximum number of catalog entries to inspect.</param>
    /// <exception cref="JetDatabaseWriter.Exceptions.JetCorruptDataException">Required catalog structure or table references are invalid.</exception>
    internal async ValueTask<List<CatalogRow>> ReadValidatedObjectsAsync(CancellationToken cancellationToken, int maxEntries = int.MaxValue)
    {
        TableDef msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false)
            ?? throw new JetDatabaseWriter.Exceptions.JetCorruptDataException(JetDatabaseWriter.Exceptions.JetErrorCode.CorruptCatalog, "The MSysObjects catalog table definition could not be read.");
        List<CatalogRow> objects = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken, maxEntries).ConfigureAwait(false);
        await catalogRows.ValidateTableReferencesAsync(objects, cancellationToken).ConfigureAwait(false);
        return objects;
    }

    /// <summary>Loads the MSysObjects TableDef (page 2).</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<TableDef?> GetMSysObjectsTableDefAsync(CancellationToken cancellationToken) =>
        tableDefs.ReadTableDefAsync(2, cancellationToken);

    /// <summary>Enumerates every row of MSysObjects, decoded as strings.</summary>
    /// <param name="msys">The system-table data.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal IAsyncEnumerable<string[]> EnumerateMSysObjectsRowsAsync(TableDef msys, CancellationToken cancellationToken) =>
        rows.EnumerateRowsForTdefAsync(2, msys, cancellationToken);

    /// <summary>
    /// Enumerates every row of MSysObjects, decoding only the named columns as
    /// strings; every other column is <see cref="string.Empty"/>.
    /// </summary>
    /// <param name="msys">The system-table data.</param>
    /// <param name="columnNames">The columns to decode.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal IAsyncEnumerable<string[]> EnumerateMSysObjectsRowsAsync(TableDef msys, IReadOnlyCollection<string> columnNames, CancellationToken cancellationToken) =>
        rows.EnumerateRowsForTdefAsync(2, msys, columnNames, cancellationToken);

    /// <summary>
    /// Reads and parses the stored <c>MSysObjects.LvProp</c> bytes of the catalog
    /// row whose <c>Id</c> is exactly <paramref name="tdefPage"/> (see
    /// <see cref="ColumnPropertyReader.ReadLvPropForTableAsync"/>). Returns
    /// <see langword="null"/> when no property value is present. A present malformed
    /// or unreadable blob throws instead of becoming absent metadata.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<ColumnPropertyBlock?> ReadLvPropForTableAsync(long tdefPage, CancellationToken cancellationToken)
        => (await tables.GetSchemaAsync(tdefPage, cancellationToken: cancellationToken).ConfigureAwait(false))?.Properties;

    /// <summary>
    /// Finds the TDEF page number for a system table by name (case-insensitive).
    /// Unlike the user-table catalog, this includes system tables (SYSTABLE_MASK set)
    /// and the local definitions of linked ODBC tables, and resolves <c>MSysObjects</c>
    /// to page 2 when no catalog row names it (see
    /// <see cref="CatalogRowReader.FindSystemTableTdefPageAsync(string, bool, CancellationToken)"/>).
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<long> FindSystemTablePageAsync(string name, CancellationToken cancellationToken) =>
        catalogRows.FindSystemTableTdefPageAsync(name, includeLinkedOdbc: true, cancellationToken);

    /// <summary>
    /// Finds the TDEF page for the unique table whose name satisfies <paramref name="nameMatches"/>,
    /// such as a complex-column flat table found by its name suffix. Linked ODBC tables with local definitions match too.
    /// </summary>
    /// <param name="nameMatches">The name matches.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<long> FindSystemTablePageAsync(Predicate<string> nameMatches, CancellationToken cancellationToken) =>
        catalogRows.FindTableTdefPageAsync(nameMatches, includeLinkedOdbc: true, cancellationToken);

    /// <summary>
    /// Renders the most recent user-table scan. Scan failures are always
    /// reported; the full catalog dump only when <paramref name="diagnosticsEnabled"/>.
    /// </summary>
    /// <param name="diagnosticsEnabled">Whether to render the full catalog dump.</param>
    internal string FormatDiagnostics(bool diagnosticsEnabled)
    {
        CatalogScanSummary? scan = tables.LastScan;
        if (scan is null)
        {
            return string.Empty;
        }

        if (scan.Msys is null)
        {
            return "ERROR: Page 2 is not a valid TDEF page (null returned).";
        }

        if (!scan.HasRequiredColumns)
        {
            return "ERROR: Required catalog columns not found. Column name mismatch?";
        }

        if (!diagnosticsEnabled)
        {
            return string.Empty;
        }

        StringBuilder diag = new StringBuilder()
            .Append("JET: ")
            .Append(format.VersionName)
            .Append("  PageSize: ")
            .Append(format.PageSize)
            .Append("  TotalPages: ")
            .Append(scan.TotalPages)
            .AppendLine()
            .Append("MSysObjects cols (")
            .Append(scan.Msys.Columns.Count)
            .Append("): ")
            .AppendJoin(", ", scan.Msys.Columns.Select(static c => $"{c.Name}[{GetTypeDisplayName(c.Type)}]"))
            .AppendLine()
            .Append("Catalog pages: ")
            .Append(scan.CatalogPageCount)
            .Append("  Total rows scanned: ")
            .Append(scan.RowsScanned)
            .Append("  User tables: ")
            .Append(scan.UserTables.Count)
            .AppendLine();

        foreach (CatalogEntry e in scan.UserTables)
        {
            _ = diag.Append("  [")
                .Append(e.Name)
                .Append("] TDEF page ")
                .Append(e.TDefPage)
                .AppendLine();
        }

        return diag.ToString();
    }
}
