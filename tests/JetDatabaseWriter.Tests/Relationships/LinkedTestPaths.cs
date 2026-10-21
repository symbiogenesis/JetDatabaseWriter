namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;

internal static class LinkedTestPaths
{
    /// <summary>Opens exact test-owned fixture paths; this is not an untrusted-path authorization policy.</summary>
    /// <param name="paths">The exact files created by the fixture.</param>
    /// <returns>The fixture source resolver.</returns>
    internal static Func<LinkedTableInfo, string, CancellationToken, ValueTask<Stream>> ResolverFor(params string[] paths)
    {
        var approved = new HashSet<string>(paths.Select(Path.GetFullPath), Path.DirectorySeparatorChar == '\\' ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        return (_, path, cancellationToken) =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!approved.Contains(path))
            {
                throw new UnauthorizedAccessException("The test did not authorize this source capability.");
            }

            return new ValueTask<Stream>(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read));
        };
    }

    internal static string GetCanonicalTempPath()
    {
        string fullPath = Path.GetFullPath(Path.GetTempPath());
        string root = Path.GetPathRoot(fullPath)!;
        string current = root;
        foreach (string segment in fullPath[root.Length..].Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries))
        {
            var directory = new DirectoryInfo(Path.Combine(current, segment));
            current = (directory.ResolveLinkTarget(returnFinalTarget: true) ?? directory).FullName;
        }

        return current;
    }
}
