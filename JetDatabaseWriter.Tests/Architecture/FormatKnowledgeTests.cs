namespace JetDatabaseWriter.Tests.Architecture;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using Xunit;

/// <summary>
/// Keeps format knowledge in the format profile: library code asks
/// <see cref="JetFormat"/> for a capability flag, a layout or a value instead
/// of comparing <see cref="DatabaseFormat"/> values. A format value may be
/// named only in JetFormat.cs, in the enum itself (<c>Enums/</c>), in the
/// encryption code, which classifies a file and unmasks its header before
/// any profile exists, and in the public reader, writer and encryption
/// facades, which accept a caller's format or classify native headers.
/// The scan reads the library's source files from the repository.
/// </summary>
public sealed partial class FormatKnowledgeTests
{
    /// <summary>The library-relative paths that may name a format value: files, or folders ending in a slash.</summary>
    private static readonly string[] AllowedPaths = ["JetFormat.cs", "Enums/", "Encryption/", "AccessReader.cs", "AccessWriter.cs", "AccessDatabaseEncryption.cs"];

    [Fact]
    public async Task DatabaseFormatValues_AreNamedOnlyByTheFormatProfile()
    {
        string libraryRoot = FindLibraryRoot();
        var violations = new List<string>();

        foreach (string path in EnumerateLibrarySources(libraryRoot))
        {
            string relative = ToLibraryRelative(libraryRoot, path);
            if (IsAllowed(relative))
            {
                continue;
            }

            string[] lines = await File.ReadAllLinesAsync(path, TestContext.Current.CancellationToken);
            for (int i = 0; i < lines.Length; i++)
            {
                if (FormatValueRegex().IsMatch(lines[i]) || StaticFormatImportRegex().IsMatch(lines[i]))
                {
                    violations.Add($"{relative}({i + 1}): {lines[i].Trim()}");
                }
            }
        }

        Assert.Empty(violations);
    }

    /// <summary>
    /// Guards the scan itself: it must find the library's sources, among them
    /// the profile, which names every format value. A scan that read no file
    /// would pass the test above without checking anything.
    /// </summary>
    [Fact]
    public async Task Scan_ReadsTheLibrarySources()
    {
        string libraryRoot = FindLibraryRoot();
        string[] relativePaths = [.. EnumerateLibrarySources(libraryRoot).Select(path => ToLibraryRelative(libraryRoot, path))];

        Assert.Contains("JetFormat.cs", relativePaths);
        Assert.Contains("Schema/TDefPageBuilder.cs", relativePaths);
        Assert.DoesNotContain(relativePaths, static path => path.StartsWith("obj/", StringComparison.Ordinal) || path.StartsWith("bin/", StringComparison.Ordinal));

        string profile = await File.ReadAllTextAsync(Path.Combine(libraryRoot, "JetFormat.cs"), TestContext.Current.CancellationToken);
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            Assert.Contains($"DatabaseFormat.{format}", profile, StringComparison.Ordinal);
        }
    }

    private static bool IsAllowed(string relativePath)
        => AllowedPaths.Any(allowed => allowed.EndsWith('/')
            ? relativePath.StartsWith(allowed, StringComparison.Ordinal)
            : string.Equals(relativePath, allowed, StringComparison.Ordinal));

    private static IEnumerable<string> EnumerateLibrarySources(string libraryRoot)
        => Directory.EnumerateFiles(libraryRoot, "*.cs", SearchOption.AllDirectories)
            .Where(path =>
            {
                string relative = ToLibraryRelative(libraryRoot, path);
                return !relative.StartsWith("bin/", StringComparison.Ordinal) && !relative.StartsWith("obj/", StringComparison.Ordinal);
            })
            .OrderBy(static path => path, StringComparer.Ordinal);

    private static string ToLibraryRelative(string libraryRoot, string path)
        => Path.GetRelativePath(libraryRoot, path).Replace('\\', '/');

    /// <summary>
    /// Finds the library project's folder: the <c>JetDatabaseWriter</c>
    /// folder beside <c>JetDatabaseWriter.slnx</c>, found by walking up from
    /// the test binaries.
    /// </summary>
    /// <returns>The absolute path of the library project's folder.</returns>
    /// <exception cref="InvalidOperationException">No folder above the test binaries holds the solution file.</exception>
    private static string FindLibraryRoot()
    {
        for (DirectoryInfo? directory = new(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "JetDatabaseWriter.slnx")))
            {
                return Path.Combine(directory.FullName, "JetDatabaseWriter");
            }
        }

        throw new InvalidOperationException($"No folder above '{AppContext.BaseDirectory}' holds JetDatabaseWriter.slnx.");
    }

    [GeneratedRegex(@"\bDatabaseFormat\s*\.\s*(?:Jet3Mdb|Jet4Mdb|AceAccdb)\b")]
    private static partial Regex FormatValueRegex();

    [GeneratedRegex(@"\busing\s+static\s+[\w.]*\bDatabaseFormat\s*;")]
    private static partial Regex StaticFormatImportRegex();
}
