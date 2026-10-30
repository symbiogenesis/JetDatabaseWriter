namespace JetDatabaseWriter.Tests.Infrastructure;

using Xunit;

/// <summary>
/// Verifies that each hosted lane executes its intended runtime and library asset.
/// </summary>
public sealed class LibraryTargetTests
{
    [Fact]
    public void ExpectedLibraryAssetIsLoaded() =>
        LibraryTarget.AssertExpectedLibraryAsset();

    [Fact]
    public void ExpectedRuntimeIsLoaded() =>
        LibraryTarget.AssertExpectedRuntime();
}
