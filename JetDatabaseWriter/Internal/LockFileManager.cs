namespace JetDatabaseWriter.Internal;

using System;
using System.IO;

/// <summary>
/// Helpers for managing the JET lock-file companion (.ldb / .laccdb) that sits alongside
/// an Access database file.
/// </summary>
internal static class LockFileManager
{
    /// <summary>
    /// Returns the path of the JET lock-file companion (.ldb or .laccdb) for the given database path.
    /// </summary>
    /// <param name="databasePath">Path to the Access database (.mdb or .accdb).</param>
    /// <returns>The path the lock-file would occupy.</returns>
    public static string GetLockFilePath(string databasePath)
    {
        string ext = Path.GetExtension(databasePath);
        string lockExt = ext.Equals(".accdb", StringComparison.OrdinalIgnoreCase) ? ".laccdb" : ".ldb";
        return Path.ChangeExtension(databasePath, lockExt);
    }

    /// <summary>
    /// Creates (or overwrites) the JET lock-file companion. When <paramref name="respectExisting"/>
    /// is <see langword="true"/>, throws <see cref="IOException"/> if a lock already exists; otherwise
    /// failures are swallowed and traced as best-effort.
    /// </summary>
    /// <param name="databasePath">Path to the Access database.</param>
    /// <param name="ownerTypeName">Type name of the caller, used in trace messages.</param>
    /// <param name="respectExisting">Whether to fail if a lock-file already exists.</param>
    public static void Create(string databasePath, string ownerTypeName, bool respectExisting = false)
    {
        string lockPath = GetLockFilePath(databasePath);
        try
        {
            if (respectExisting && File.Exists(lockPath))
            {
                throw new IOException($"Database is already in use. A lockfile exists at: {lockPath}");
            }

            using var fs = new FileStream(lockPath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        }
        catch (IOException ex) when (!respectExisting)
        {
            // Best-effort: if another process holds the lock, continue without it.
            System.Diagnostics.Trace.WriteLine($"[{ownerTypeName}] Best-effort lock-file suppression in CreateLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
        catch (UnauthorizedAccessException ex) when (!respectExisting)
        {
            // Best-effort: if we lack permission, continue without it.
            System.Diagnostics.Trace.WriteLine($"[{ownerTypeName}] Best-effort lock-file suppression in CreateLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
    }

    /// <summary>
    /// Best-effort delete of the JET lock-file companion. I/O and permission failures are traced and swallowed.
    /// </summary>
    /// <param name="databasePath">Path to the Access database.</param>
    /// <param name="ownerTypeName">Type name of the caller, used in trace messages.</param>
    public static void Delete(string databasePath, string ownerTypeName)
    {
        string lockPath = GetLockFilePath(databasePath);
        try
        {
            File.Delete(lockPath);
        }
        catch (IOException ex)
        {
            // Best-effort: file may be held by another process.
            System.Diagnostics.Trace.WriteLine($"[{ownerTypeName}] Best-effort lock-file suppression in DeleteLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
        catch (UnauthorizedAccessException ex)
        {
            // Best-effort: we may lack permission.
            System.Diagnostics.Trace.WriteLine($"[{ownerTypeName}] Best-effort lock-file suppression in DeleteLockFile: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
    }
}
