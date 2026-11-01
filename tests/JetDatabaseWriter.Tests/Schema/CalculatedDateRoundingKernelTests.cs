namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>The generated tick controller retains managed-date ties and representable endpoints.</summary>
public sealed class CalculatedDateRoundingKernelTests
{
    [Theory]
    [InlineData(0L, 0L)]
    [InlineData(1L, 0L)]
    [InlineData(4_999_999L, 0L)]
    [InlineData(5_000_000L, 0L)]
    [InlineData(5_000_001L, 10_000_000L)]
    [InlineData(9_999_999L, 10_000_000L)]
    [InlineData(10_000_000L, 10_000_000L)]
    [InlineData(863_999_500_000L, 864_000_000_000L)]
    [InlineData(3_155_378_975_985_000_000L, 3_155_378_975_980_000_000L)]
    [InlineData(3_155_378_975_985_000_001L, 3_155_378_975_990_000_000L)]
    [InlineData(3_155_378_975_995_000_001L, 3_155_378_975_990_000_000L)]
    [InlineData(3_155_378_975_999_999_999L, 3_155_378_975_990_000_000L)]
    public void WholeSecondProjection_PreservesTiesAndBounds(long ticks, long expected)
    {
        ulong actual = RecoveryKernel.RoundGeneralDateTicks((ulong)ticks);
        Assert.Equal((ulong)expected, actual);
        Assert.Equal(actual, RecoveryKernel.RoundGeneralDateTicks(actual));
    }

    [Fact]
    public void OutOfDateTimeRange_RefusesWithoutOverflow()
    {
        Assert.Equal(0UL, RecoveryKernel.RoundGeneralDateTicks((ulong)DateTime.MaxValue.Ticks + 1));
        Assert.Equal(0UL, RecoveryKernel.RoundGeneralDateTicks(ulong.MaxValue));
    }

    [Fact]
    public void ManagedDateText_DoesNotCarryBeyondFinalDay()
        => Assert.Equal("12/31/9999 11:59:59 PM", CalculatedExpressionCoercion.ToGeneralDateText(DateTime.MaxValue));
}
