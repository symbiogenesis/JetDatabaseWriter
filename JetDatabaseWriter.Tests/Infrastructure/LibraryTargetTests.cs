namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using Xunit;

/// <summary>
/// Verifies that each hosted lane executes its intended runtime and library asset.
/// </summary>
public sealed class LibraryTargetTests
{
    [Fact]
    public void ExpectedLibraryAssetIsLoaded() =>
#if TEST_NETSTANDARD
        Assert.True(LibraryTarget.IsNetStandard);
#else
        Assert.False(LibraryTarget.IsNetStandard);
#endif

    [Fact]
    public void ExpectedRuntimeIsLoaded()
    {
        string? expected = Environment.GetEnvironmentVariable("JDW_EXPECTED_RUNTIME_MAJOR");
        if (expected is not null)
        {
            Assert.Equal(expected, Environment.Version.Major.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
    }
}
