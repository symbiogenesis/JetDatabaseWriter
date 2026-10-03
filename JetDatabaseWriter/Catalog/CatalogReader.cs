namespace JetDatabaseWriter.Catalog;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Read-side <c>MSysObjects</c> queries: resolves user and system table names
/// to their definitions, locates system tables, reads persisted column
/// properties, and renders the last user-table scan as
/// <see cref="AccessReader.LastDiagnostics"/>.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="tables">The cached user-table catalog.</param>
/// <param name="rows">Decodes <c>MSysObjects</c> rows as strings.</param>
internal sealed class CatalogReader(DatabaseFile db, TableCatalog tables, RowDecoder rows)
{
    /// <summary>
    /// Returns the <c>ResultType</c> a calculated column's persisted properties
    /// declare, or <see langword="default"/> when none is recorded.
    /// </summary>
    /// <param name="target">The column's persisted property target.</param>
    internal static ColumnType ResolveCalculatedResultType(ColumnPropertyTarget? target)
    {
        ColumnPropertyEntry? rt = target?.Find(Constants.ColumnPropertyNames.ResultType);
        return rt?.Value.Length >= 1
            && (rt.DataType == ByteType
                || rt.DataType == IntegerType
                || rt.DataType == LongIntegerType)
            ? (ColumnType)rt.Value[0]
            : default;
    }

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
            TableDef? td = await db.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            if (td?.Columns.Count > 0)
            {
                await this.HydrateCalculatedResultTypesAsync(entry.TDefPage, td, cancellationToken).ConfigureAwait(false);
                return new ResolvedTable(entry, td);
            }
        }

        // Fall back to a system-table lookup (MSysObjects, MSysRelationships, etc.).
        // The user-table catalog filters out rows whose Flags carry SYSTABLE_MASK,
        // so a name match against the catalog scan is needed for those.
        long sysPage = await this.FindSystemTablePageAsync(
            n => string.Equals(n, tableName, StringComparison.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);
        if (sysPage > 0)
        {
            TableDef? sysTd = await db.ReadTableDefAsync(sysPage, cancellationToken).ConfigureAwait(false);
            if (sysTd?.Columns.Count > 0)
            {
                await this.HydrateCalculatedResultTypesAsync(sysPage, sysTd, cancellationToken).ConfigureAwait(false);
                return new ResolvedTable(new CatalogEntry(tableName, sysPage), sysTd);
            }
        }

        return null;
    }

    /// <summary>Loads the MSysObjects TableDef (page 2).</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<TableDef?> GetMSysObjectsTableDefAsync(CancellationToken cancellationToken) =>
        db.ReadTableDefAsync(2, cancellationToken);

    /// <summary>Enumerates every row of MSysObjects, decoded as strings.</summary>
    /// <param name="msys">The system-table data.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal IAsyncEnumerable<string[]> EnumerateMSysObjectsRowsAsync(TableDef msys, CancellationToken cancellationToken) =>
        rows.EnumerateRowsForTdefAsync(2, msys, cancellationToken);

    /// <summary>
    /// Reads and parses the <c>MSysObjects.LvProp</c> blob for the catalog row whose
    /// <c>Id</c> column's low-24 bits match <paramref name="tdefPage"/>. Returns
    /// <see langword="null"/> when the catalog has no <c>LvProp</c> column (slim
    /// schemas written by older versions of this library), the row is missing, the
    /// blob is empty, or the magic header is unrecognised.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<ColumnPropertyBlock?> ReadLvPropForTableAsync(long tdefPage, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await this.GetMSysObjectsTableDefAsync(cancellationToken).ConfigureAwait(false);
        if (msys is null)
        {
            return null;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxLvProp = msys.FindColumnIndex("LvProp");
        if (idxId < 0 || idxLvProp < 0)
        {
            return null;
        }

        await foreach (string[] row in rows.EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            if (!CatalogValueReader.TryParseInt64(row, idxId, out long id))
            {
                continue;
            }

            if (CatalogValueReader.TdefPageFromId(id) != tdefPage)
            {
                continue;
            }

            byte[]? blob = BinaryStringParser.TryDecodeBase64DataUri(
                CatalogValueReader.GetStringOrEmpty(row, idxLvProp),
                "application/octet-stream",
                out byte[] bytes)
                ? bytes
                : null;
            return ColumnPropertyBlock.Parse(blob, db.Format);
        }

        return null;
    }

    /// <summary>
    /// Finds the TDEF page number for a system table by name (case-insensitive).
    /// Unlike the user-table catalog, this includes system tables (SYSTABLE_MASK set).
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<long> FindSystemTablePageAsync(string name, CancellationToken cancellationToken) =>
        this.FindSystemTablePageAsync(
            n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase),
            cancellationToken);

    /// <summary>
    /// Finds the TDEF page for the first system table whose name satisfies <paramref name="nameMatches"/>.
    /// Shared by exact-name and suffix lookups against MSysObjects.
    /// </summary>
    /// <param name="nameMatches">The name matches.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> FindSystemTablePageAsync(Predicate<string> nameMatches, CancellationToken cancellationToken)
    {
        TableDef? msys = await db.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxName = msys.FindColumnIndex("Name");
        int idxType = msys.FindColumnIndex("Type");

        if (idxId < 0 || idxName < 0 || idxType < 0)
        {
            return 0;
        }

        await foreach (string[] row in rows.EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            string nameStr = CatalogValueReader.GetStringOrEmpty(row, idxName);
            if (!nameMatches(nameStr))
            {
                continue;
            }

            if (!CatalogValueReader.TryParseInt32(row, idxType, out int objType) || (objType != Constants.SystemObjects.UserTableType && objType != Constants.SystemObjects.LinkedOdbcType))
            {
                continue;
            }

            if (CatalogValueReader.TryParseInt64(row, idxId, out long id))
            {
                long tdefPage = CatalogValueReader.TdefPageFromId(id);
                if (tdefPage > 0)
                {
                    return tdefPage;
                }
            }
        }

        return 0;
    }

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
            .Append(db.Format == DatabaseFormat.Jet3Mdb ? "Jet3" : "Jet4/ACE")
            .Append("  PageSize: ")
            .Append(db.PageSizeBytes)
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

    private async ValueTask HydrateCalculatedResultTypesAsync(long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        if (!tableDef.Columns.Exists(static col => col.IsCalculated))
        {
            return;
        }

        ColumnPropertyBlock? properties = await this.ReadLvPropForTableAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (properties is null)
        {
            return;
        }

        bool changed = false;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            if (!col.IsCalculated)
            {
                continue;
            }

            ColumnType resultType = ResolveCalculatedResultType(properties.FindTarget(col.Name));
            if (resultType != default && resultType != col.CalculatedResultType)
            {
                tableDef.Columns[i] = col.WithCalculatedResultType(resultType);
                changed = true;
            }
        }

        if (changed)
        {
            tableDef.InitializeColumnMetadata();
        }
    }
}
