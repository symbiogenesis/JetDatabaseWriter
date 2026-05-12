namespace JetDatabaseWriter.Tests.RoundTrip;

using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// DAO-driven validation tests that shell out to a bitness-matched
/// <c>powershell.exe</c> host to exercise writer output via the canonical
/// Access engine (<c>DAO.DBEngine.120</c>). Skipped automatically when
/// Microsoft Access is not installed.
/// </summary>
/// <remarks>
/// Closes §5 coverage gaps: DAO OpenRecordset row-count, DAO index traversal,
/// DAO AutoNumber continuation, and §2.1: DAO-authored Memo with embedded NULs.
/// </remarks>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoValidationTests(DaoValidationFixture fixture) : IClassFixture<DaoValidationFixture>
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoOpenRecordset_RowCount_MatchesWriterOutput()
    {
        var result = await fixture.GetCoreResultAsync(TestContext.Current.CancellationToken);

        Assert.Equal(DaoValidationFixture.CoreRowCount, result.RowCount);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoIndexTraversal_Seek_LocatesRowByPrimaryKey()
    {
        var result = await fixture.GetCoreResultAsync(TestContext.Current.CancellationToken);

        Assert.Equal($"Item_{DaoValidationFixture.CoreTargetId}", result.SeekLabel);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert()
    {
        var result = await fixture.GetCoreResultAsync(TestContext.Current.CancellationToken);

        Assert.Equal(DaoValidationFixture.CoreWriterRowCount + 1, result.AutoNumberId);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoAuthoredMemo_WithEmbeddedNuls_ReaderReturnsExactContent()
    {
        var result = await fixture.GetDaoMemoResultAsync(TestContext.Current.CancellationToken);

        Assert.Equal(DaoValidationFixture.ExpectedMemoWithNuls.Length, result.Content.Length);
        Assert.Equal(DaoValidationFixture.ExpectedMemoWithNuls, result.Content);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly()
    {
        var result = await fixture.GetCoreResultAsync(TestContext.Current.CancellationToken);

        Assert.Equal(DaoValidationFixture.ExpectedMemoWithNuls, result.MemoNuls);
        Assert.Equal("\u4F60\u597D\u4E16\u754C", result.MemoCjk);
        Assert.Equal("Start\0\u00E9\u00FC\u2603\0End", result.MemoMixed);
        Assert.Equal([0x00, 0x01, 0xFF, 0xFE, 0x42, 0x4C, 0x4F, 0x42], result.MemoBinary);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoRelationshipEnforcement_FkViolation_RaisesError()
    {
        var result = await fixture.GetCoreResultAsync(TestContext.Current.CancellationToken);

        Assert.NotEqual("0", result.FkErrorCode);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds()
    {
        var result = await fixture.GetEncryptedCompactResultAsync(TestContext.Current.CancellationToken);

        Assert.True(result.CompactedFileExists, "Compacted output file was not created.");
        Assert.True(result.ReopenedTableCount > 0, "Compacted encrypted database should reopen with tables.");
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoCompact_MultiTableStress_SurvivesCompactAndRepair()
    {
        var result = await fixture.GetStressCompactResultAsync(TestContext.Current.CancellationToken);

        Assert.True(
            result.PreCompactTableCount >= DaoValidationFixture.StressTableCount + 1,
            $"Expected at least {DaoValidationFixture.StressTableCount + 1} pre-compact tables, got {result.PreCompactTableCount}.");
        Assert.True(
            result.PostCompactTableCount >= DaoValidationFixture.StressTableCount + 1,
            $"Post-compact: expected at least {DaoValidationFixture.StressTableCount + 1} tables, got {result.PostCompactTableCount}.");

        for (int i = 0; i < 3; i++)
        {
            string tableName = $"Stress_T{i:D2}";
            Assert.True(
                result.PostCompactRowCounts.TryGetValue(tableName, out int rowCount) && rowCount == DaoValidationFixture.StressRowsPerTable,
                $"Post-compact: {tableName} row count = {rowCount}, expected {DaoValidationFixture.StressRowsPerTable}.");
        }
    }
}
