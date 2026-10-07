namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;

internal static class LinkedTestPaths
{
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
