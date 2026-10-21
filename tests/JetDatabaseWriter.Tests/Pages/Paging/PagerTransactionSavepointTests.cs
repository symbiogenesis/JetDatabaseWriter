namespace JetDatabaseWriter.Tests.Pages.Paging;

using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks nested journal frames without involving disk writes.</summary>
public sealed class PagerTransactionSavepointTests
{
    /// <summary>Released children preserve the oldest parent image and append boundary.</summary>
    [Fact]
    public void NestedRelease_ParentRollbackRestoresImagesAndAppends()
    {
        var journal = new PagerTransaction(32, 16, 4);
        byte[] original = new byte[16];
        original[0] = 1;
        journal.Write(0, original);
        journal.BeginSavepoint();
        original[0] = 2;
        journal.Write(0, original);
        journal.BeginSavepoint();
        original[0] = 3;
        journal.Write(0, original);
        Assert.Equal(2, journal.Append(original));
        journal.ReleaseSavepoint();
        journal.RollbackSavepoint();
        Assert.Equal(1, journal.TryGet(0)![0]);
        Assert.Null(journal.TryGet(2));
        Assert.Equal(2, journal.NextAppendPageNumber);
        Assert.Equal(1, journal.Count);
    }

    /// <summary>Prior images do not consume budget and reservation status survives rollback.</summary>
    [Fact]
    public void BudgetFailure_RollbackRestoresReservationAndBudget()
    {
        var journal = new PagerTransaction(32, 16, 1);
        journal.ReserveZeroedPage(0);
        journal.BeginSavepoint();
        byte[] page = new byte[16];
        page[0] = 9;
        journal.Write(0, page);
        JetLimitationException error = Assert.Throws<JetLimitationException>(() => journal.Append(page));
        Assert.Equal(JetErrorCode.JournalBudgetExceeded, error.ErrorCode);
        journal.RollbackSavepoint();
        Assert.True(journal.IsZeroReservation(0));
        Assert.Equal(0, journal.TryGet(0)![0]);
        Assert.Equal(2, journal.NextAppendPageNumber);
    }
}
