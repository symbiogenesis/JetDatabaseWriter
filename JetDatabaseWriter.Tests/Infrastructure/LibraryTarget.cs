namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Reflection;
using System.Runtime.Versioning;

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
}
