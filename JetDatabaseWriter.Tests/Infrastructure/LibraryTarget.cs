namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Reflection;
using System.Runtime.Versioning;

/// <summary>
/// Reports which build of the library the running tests loaded. The net10.0 test leg
/// loads the library's net10.0 asset and the net8.0 leg loads its netstandard2.1 asset,
/// the one .NET 8 and 9 consumers get.
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
