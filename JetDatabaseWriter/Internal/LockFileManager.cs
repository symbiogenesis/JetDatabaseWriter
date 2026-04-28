namespace JetDatabaseWriter.Internal;

using System;
using System.IO;

/// <summary>
/// Static helpers for the JET lock-file companion (.ldb / .laccdb) that sits alongside
/// an Access database file. The slot-populating, handle-owning workflow lives in
/// <see cref="LockFileSlotWriter"/>; this type only resolves the companion path.
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
}
