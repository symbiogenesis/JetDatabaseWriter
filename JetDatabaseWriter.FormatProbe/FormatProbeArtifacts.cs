namespace JetDatabaseWriter.FormatProbe;

using System.Globalization;
using System.IO;
using System.Threading.Tasks;

internal static class FormatProbeArtifacts
{
    public const string FilePrefix = "format-probe-";

    public static string GetOutputDirectory(string repoRoot)
    {
        string outputDirectory = Path.Combine(repoRoot, "docs", "format-probe");
        _ = Directory.CreateDirectory(outputDirectory);
        return outputDirectory;
    }

    public static string GetFilePath(string outputDirectory, string fileName) =>
        Path.Combine(outputDirectory, EnsurePrefixedName(fileName));

    public static string CreateWorkDirectory(string outputDirectory, string name)
    {
        string directory = GetFilePath(outputDirectory, $"{name}-{CreateTimestamp()}");
        _ = Directory.CreateDirectory(directory);
        return directory;
    }

    public static string CreateDirectory(string parentDirectory, string directoryName)
    {
        string directory = GetFilePath(parentDirectory, directoryName);
        _ = Directory.CreateDirectory(directory);
        return directory;
    }

    public static void EnsureDirectory(string directory) =>
        _ = Directory.CreateDirectory(directory);

    public static async Task WriteAllTextAsync(string path, string contents)
    {
        EnsureParentDirectory(path);
        await File.WriteAllTextAsync(path, contents);
    }

    public static void WriteAllText(string path, string contents)
    {
        EnsureParentDirectory(path);
        File.WriteAllText(path, contents);
    }

    public static async Task WriteAllBytesAsync(string path, byte[] bytes)
    {
        EnsureParentDirectory(path);
        await File.WriteAllBytesAsync(path, bytes);
    }

    public static void Copy(string sourcePath, string destinationPath, bool overwrite)
    {
        EnsureParentDirectory(destinationPath);
        File.Copy(sourcePath, destinationPath, overwrite);
    }

    private static string EnsurePrefixedName(string name)
    {
        if (Path.IsPathRooted(name)
            || name.Contains(Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || name.Contains(Path.AltDirectorySeparatorChar, StringComparison.Ordinal))
        {
            throw new ArgumentException("Format probe artifact names must be relative file or directory names.", nameof(name));
        }

        return name.StartsWith(FilePrefix, StringComparison.OrdinalIgnoreCase)
            ? name
            : string.Concat(FilePrefix, name);
    }

    private static void EnsureParentDirectory(string path)
    {
        string? parent = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(parent))
        {
            _ = Directory.CreateDirectory(parent);
        }
    }

    private static string CreateTimestamp() =>
        DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss-fffffff", CultureInfo.InvariantCulture);
}
