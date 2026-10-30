namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Reflection;
using System.Runtime.Versioning;
using Xunit;

/// <summary>
/// Reports which build of the library the running tests loaded. The ordinary runtime
/// lanes load the net10.0 asset; TestNetStandard selects the netstandard2.1 asset.
/// </summary>
internal static class LibraryTarget
{
    /// <summary>
    /// Gets a value indicating whether the loaded library is its netstandard2.1 build,
    /// which lacks the .NET 6+ <c>RandomAccess</c> page reads.
    /// </summary>
    public static bool IsNetStandard { get; } =
        typeof(AccessReader).Assembly.GetCustomAttribute<TargetFrameworkAttribute>()?.FrameworkName
            .StartsWith(".NETStandard", StringComparison.Ordinal) == true;

    /// <summary>Verifies the runtime and library asset in every independently executed test partition.</summary>
    public static void AssertExpectedTarget()
    {
        AssertExpectedLibraryAsset();
        AssertExpectedRuntime();
    }

    /// <summary>Verifies that the loaded library matches the compiled test lane.</summary>
    public static void AssertExpectedLibraryAsset() =>
#if TEST_NETSTANDARD
        Assert.True(IsNetStandard);
#else
        Assert.False(IsNetStandard);
#endif

    /// <summary>Verifies that the executing runtime matches the hosted lane when specified.</summary>
    public static void AssertExpectedRuntime()
    {
        string? expected = Environment.GetEnvironmentVariable("JDW_EXPECTED_RUNTIME_MAJOR");
        if (expected is not null)
        {
            Assert.Equal(expected, Environment.Version.Major.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
    }
}
