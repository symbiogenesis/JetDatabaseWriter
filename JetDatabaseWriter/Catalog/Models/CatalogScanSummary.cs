namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog;

/// <summary>
/// What one <see cref="TableCatalog"/> scan of <c>MSysObjects</c> saw. The reader
/// renders it as <see cref="AccessReader.LastDiagnostics"/>.
/// </summary>
/// <param name="Msys">The <c>MSysObjects</c> table definition, or <see langword="null"/> when page 2 is not a valid TDEF.</param>
/// <param name="HasRequiredColumns">Whether <c>MSysObjects</c> has the <c>Name</c> and <c>Type</c> columns the scan needs.</param>
/// <param name="CatalogPageCount">The number of data pages that held catalog rows.</param>
/// <param name="RowsScanned">The number of live catalog rows decoded.</param>
/// <param name="TotalPages">The number of pages in the file when the scan ran.</param>
/// <param name="UserTables">The user tables found.</param>
internal sealed record CatalogScanSummary(
    TableDef? Msys,
    bool HasRequiredColumns,
    int CatalogPageCount,
    int RowsScanned,
    long TotalPages,
    List<CatalogEntry> UserTables);
