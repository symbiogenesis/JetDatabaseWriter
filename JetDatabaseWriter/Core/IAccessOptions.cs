namespace JetDatabaseWriter.Core;

using System;

/// <summary>
/// Shared options used when opening Access databases.
/// </summary>
public interface IAccessOptions
{
    /// <summary>
    /// Gets a value indicating whether a lockfile (.ldb / .laccdb) is created
    /// alongside the database while it is open, and deleted on dispose.
    /// </summary>
    bool UseLockFile { get; init; }

    /// <summary>
    /// Gets the password for opening password-protected databases.
    /// Empty (the default) means no password is supplied.
    /// </summary>
    ReadOnlyMemory<char> Password { get; }
}
