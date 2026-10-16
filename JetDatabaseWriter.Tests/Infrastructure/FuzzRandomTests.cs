namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using Xunit;

/// <summary>Fuzz inputs reproduce values independently of the time of execution.</summary>
public sealed class FuzzRandomTests
{
    [Fact]
    public void DateValues_UsePinnedReplayEpoch()
    {
        var random = FuzzRandom.Create([0, 0, 0, 0]);
        DateTime actual = Assert.IsType<DateTime>(random.RandomValue(typeof(DateTime)));
        DateTime expected = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(-10000);
        Assert.Equal(expected, actual);
    }
}
