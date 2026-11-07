namespace JetDatabaseWriter.Models;

/// <summary>
/// Summarizes per-table discovery, row retention, index regeneration, and counter repair for an <see cref="AccessDatabaseCompact"/> operation.
/// </summary>
/// <param name="OwnerTDefPage">The pinned root <c>TDef</c> page number of the table.</param>
/// <param name="TableName">The resolved table name from <c>MSysObjects</c>.</param>
/// <param name="ActiveRowCount">The number of active rows retained for this table.</param>
/// <param name="UnassignedRowCount">The number of unmapped physical rows discovered for this table.</param>
/// <param name="IndexCount">The total number of physical real indexes on the table.</param>
/// <param name="RegeneratedIndexCount">The number of damaged physical real indexes regenerated from verified active rows.</param>
/// <param name="SalvagedRowCount">The number of active rows salvaged from data pages with damaged page-type headers.</param>
/// <param name="QuarantinedPageCount">The number of unrecoverable data pages quarantined for this table.</param>
/// <param name="DeclaredRowCount">The original <c>NumRows</c> counter stored in the source <c>TDef</c> header.</param>
/// <param name="RepairedRowCount">The verified <c>NumRows</c> counter after compact planning and repair.</param>
/// <param name="DeclaredAutoNumber">The original <c>AutoNumber</c> high-water counter stored in the source <c>TDef</c> header.</param>
/// <param name="RepairedAutoNumber">The verified <c>AutoNumber</c> high-water counter after compact planning and repair.</param>
/// <param name="CountersRepaired">A value indicating whether any <c>TDef</c> counter was repaired or reset for this table.</param>
public sealed record AccessCompactTableSummary(
    long OwnerTDefPage,
    string TableName,
    long ActiveRowCount,
    long UnassignedRowCount,
    int IndexCount,
    int RegeneratedIndexCount,
    long SalvagedRowCount,
    int QuarantinedPageCount,
    uint DeclaredRowCount,
    uint RepairedRowCount,
    uint DeclaredAutoNumber,
    uint RepairedAutoNumber,
    bool CountersRepaired);
