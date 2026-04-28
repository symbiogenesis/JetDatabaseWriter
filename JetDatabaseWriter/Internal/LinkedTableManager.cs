namespace JetDatabaseWriter.Internal;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;

/// <summary>
/// Centralises all logic for discovering, resolving, and opening linked tables
/// (MSysObjects type 4 / 6) referenced by an <see cref="AccessReader"/>. Pure
/// path-handling helpers and the MSysObjects scan that produces
/// <see cref="LinkedTableInfo"/> entries live here so <see cref="AccessReader"/>
/// keeps only the wiring needed to delegate to this manager.
/// </summary>
internal static class LinkedTableManager
{
    /// <summary>
    /// Normalises the caller-supplied allowlist of directories that linked-table
    /// source paths must reside under. Relative entries are resolved against the
    /// directory containing <paramref name="hostDatabasePath"/>.
    /// </summary>
    internal static string[] NormalizeAllowlist(IReadOnlyList<string> allowlist, string hostDatabasePath)
    {
        if (allowlist == null || allowlist.Count == 0)
        {
            return [];
        }

        string baseDirectory = Path.GetDirectoryName(hostDatabasePath) ?? Directory.GetCurrentDirectory();
        var normalized = new List<string>(allowlist.Count);

        foreach (string path in allowlist)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            string fullPath = ResolvePath(path.Trim(), baseDirectory, "linked-source allowlist");
            normalized.Add(EnsureTrailingDirectorySeparator(fullPath));
        }

        return normalized.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    /// <summary>
    /// Builds a derivative <see cref="AccessReaderOptions"/> instance suitable for
    /// re-opening the source database referenced by a linked table. The allowlist
    /// and validator are forwarded so transitively linked databases inherit the
    /// same security policy.
    /// </summary>
    internal static AccessReaderOptions CreateLinkedSourceOpenOptions(
        AccessReaderOptions options,
        string[] normalizedAllowlist,
        Func<LinkedTableInfo, string, bool>? linkedSourcePathValidator)
    {
        return new AccessReaderOptions
        {
            PageCacheSize = options.PageCacheSize,
            DiagnosticsEnabled = options.DiagnosticsEnabled,
            ParallelPageReadsEnabled = options.ParallelPageReadsEnabled,
            ValidateOnOpen = options.ValidateOnOpen,
            StrictParsing = options.StrictParsing,
            FileAccess = options.FileAccess,
            FileShare = options.FileShare,
            Password = options.Password,
            UseLockFile = options.UseLockFile,
            LockFileUserName = options.LockFileUserName,
            LockFileMachineName = options.LockFileMachineName,
            LinkedSourcePathAllowlist = normalizedAllowlist,
            LinkedSourcePathValidator = linkedSourcePathValidator,
        };
    }

    /// <summary>
    /// Enumerates every linked table (Access-native or ODBC) defined in
    /// MSysObjects on the given <paramref name="reader"/>.
    /// </summary>
    internal static async ValueTask<List<LinkedTableInfo>> GetLinkedTablesAsync(AccessReader reader, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await reader.GetMSysObjectsTableDefAsync(cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return [];
        }

        int idxName = msys.FindColumnIndex("Name");
        int idxType = msys.FindColumnIndex("Type");
        int idxFlags = msys.FindColumnIndex("Flags");
        int idxDatabase = msys.FindColumnIndex("Database");
        int idxForeignName = msys.FindColumnIndex("ForeignName");
        int idxConnect = msys.FindColumnIndex("Connect");

        if (idxName < 0 || idxType < 0)
        {
            return [];
        }

        var result = new List<LinkedTableInfo>();

        await foreach (List<string> row in reader.EnumerateMSysObjectsRowsAsync(msys, cancellationToken).ConfigureAwait(false))
        {
            string typeStr = SafeGet(row, idxType);
            if (!int.TryParse(typeStr, out int objType))
            {
                continue;
            }

            if (objType != Constants.SystemObjects.LinkedTableType && objType != Constants.SystemObjects.LinkedOdbcType)
            {
                continue;
            }

            string nameStr = SafeGet(row, idxName);
            if (string.IsNullOrEmpty(nameStr))
            {
                continue;
            }

            string flagsStr = SafeGet(row, idxFlags);
            if (long.TryParse(flagsStr, out long flagsLong) &&
                (unchecked((uint)flagsLong) & Constants.SystemObjects.SystemTableMask) != 0)
            {
                continue;
            }

            bool isOdbc = objType == Constants.SystemObjects.LinkedOdbcType;
            result.Add(new LinkedTableInfo
            {
                Name = nameStr,
                ForeignName = SafeGet(row, idxForeignName),
                SourceDatabasePath = isOdbc ? null : SafeGet(row, idxDatabase),
                ConnectionString = isOdbc ? SafeGet(row, idxConnect) : null,
                IsOdbc = isOdbc,
            });
        }

        return result;
    }

    /// <summary>
    /// Locates the linked-table entry matching <paramref name="tableName"/>
    /// (case-insensitive) or returns <see langword="null"/> when the name does
    /// not refer to a linked table.
    /// </summary>
    internal static async ValueTask<LinkedTableInfo?> FindLinkedTableAsync(AccessReader reader, string tableName, CancellationToken cancellationToken)
    {
        List<LinkedTableInfo> links = await GetLinkedTablesAsync(reader, cancellationToken).ConfigureAwait(false);
        return links.Find(l => string.Equals(l.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Opens the source database referenced by <paramref name="link"/>, applying
    /// the host reader's allowlist and validator and reusing its cached
    /// linked-source open options.
    /// </summary>
    internal static async ValueTask<AccessReader> OpenLinkedSourceAsync(
        AccessReader reader,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        string resolvedPath = ResolveLinkedSourcePath(
            link,
            reader.HostDatabasePath,
            reader.LinkedSourcePathAllowlist,
            reader.LinkedSourcePathValidator);

        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException(
                $"Source database for linked table '{link.Name}' not found: {resolvedPath}",
                resolvedPath);
        }

        return await AccessReader.OpenAsync(resolvedPath, reader.LinkedSourceOpenOptions, cancellationToken).ConfigureAwait(false);
    }

    private static string ResolveLinkedSourcePath(
        LinkedTableInfo link,
        string hostDatabasePath,
        string[] linkedSourcePathAllowlist,
        Func<LinkedTableInfo, string, bool>? linkedSourcePathValidator)
    {
        if (string.IsNullOrWhiteSpace(link.SourceDatabasePath))
        {
            throw new FileNotFoundException(
                $"Source database for linked table '{link.Name}' not found: {link.SourceDatabasePath}",
                link.SourceDatabasePath);
        }

        string rawPath = link.SourceDatabasePath.Trim();
        string baseDirectory = Path.GetDirectoryName(hostDatabasePath) ?? Directory.GetCurrentDirectory();
        string resolvedPath = ResolvePath(rawPath, baseDirectory, $"linked table '{link.Name}'");
        bool escapesHostDatabaseDirectory =
            !Path.IsPathRooted(rawPath) &&
            !IsPathWithinDirectory(resolvedPath, baseDirectory);

        bool callbackApproved = linkedSourcePathValidator?.Invoke(link, resolvedPath) ?? false;

        if (escapesHostDatabaseDirectory && !callbackApproved)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{link.SourceDatabasePath}' escapes the host database directory. " +
                "Use AccessReaderOptions.LinkedSourcePathValidator to explicitly allow trusted paths.");
        }

        if (linkedSourcePathAllowlist.Length > 0 &&
            !linkedSourcePathAllowlist.Any(root => IsPathWithinDirectory(resolvedPath, root)))
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{resolvedPath}' is not permitted by AccessReaderOptions.LinkedSourcePathAllowlist.");
        }

        if (linkedSourcePathValidator != null && !callbackApproved)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{resolvedPath}' was rejected by AccessReaderOptions.LinkedSourcePathValidator.");
        }

        return resolvedPath;
    }

    private static string ResolvePath(string path, string baseDirectory, string context)
    {
        try
        {
            return Path.IsPathRooted(path)
                ? Path.GetFullPath(path)
                : Path.GetFullPath(Path.Combine(baseDirectory, path));
        }
        catch (Exception ex) when (
            ex is ArgumentException ||
            ex is NotSupportedException ||
            ex is PathTooLongException)
        {
            throw new UnauthorizedAccessException(
                $"Invalid path in {context}: '{path}'.",
                ex);
        }
    }

    private static bool IsPathWithinDirectory(string path, string directory)
    {
        string fullPath = Path.GetFullPath(path);
        string fullDirectory = EnsureTrailingDirectorySeparator(Path.GetFullPath(directory));
        return fullPath.StartsWith(fullDirectory, StringComparison.OrdinalIgnoreCase);
    }

    private static string EnsureTrailingDirectorySeparator(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            return path;
        }

        char last = path[path.Length - 1];
        if (last != Path.DirectorySeparatorChar && last != Path.AltDirectorySeparatorChar)
        {
            return path + Path.DirectorySeparatorChar;
        }

        return path;
    }

    private static string SafeGet(List<string> row, int idx) =>
        (idx >= 0 && idx < row.Count) ? row[idx] : string.Empty;
}
