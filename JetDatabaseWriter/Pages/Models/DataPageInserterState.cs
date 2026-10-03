namespace JetDatabaseWriter.Pages.Models;

using System.Collections.Generic;

/// <summary>
/// A <see cref="DataPageInserter"/>'s per-writer caches at one point in time:
/// the insert-page hint and the TDEFs whose owned-page usage maps the writer
/// may extend. A transaction takes one when it begins and hands it back to
/// <see cref="DataPageInserter.RestoreState"/> if it rolls back.
/// </summary>
/// <param name="HintTDefPage">The TDEF page the insert-page hint belongs to, or -1 when there is no hint.</param>
/// <param name="HintPageNumber">The data page the hint points at, or -1 when there is no hint.</param>
/// <param name="OwnedMapWritableTdefs">The TDEF pages whose owned-page usage maps the writer may extend.</param>
internal sealed record DataPageInserterState(
    long HintTDefPage,
    long HintPageNumber,
    IReadOnlyCollection<long> OwnedMapWritableTdefs);
