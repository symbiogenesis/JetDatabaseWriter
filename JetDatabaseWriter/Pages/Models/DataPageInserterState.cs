namespace JetDatabaseWriter.Pages.Models;

/// <summary>
/// A <see cref="DataPageInserter"/>'s insert-page hint at one point in time.
/// A transaction takes one when it begins and hands it back to
/// <see cref="DataPageInserter.RestoreState"/> if it rolls back.
/// </summary>
/// <param name="HintTDefPage">The TDEF page the insert-page hint belongs to, or -1 when there is no hint.</param>
/// <param name="HintPageNumber">The data page the hint points at, or -1 when there is no hint.</param>
internal sealed record DataPageInserterState(
    long HintTDefPage,
    long HintPageNumber);
