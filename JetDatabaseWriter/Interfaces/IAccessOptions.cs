namespace JetDatabaseWriter.Interfaces;

using System;

/// <summary>
/// Shared options used when opening Access databases.
/// </summary>
public interface IAccessOptions
{
    /// <summary>
    /// Gets the password for opening password-protected databases.
    /// Empty (the default) means no password is supplied.
    /// </summary>
    public ReadOnlyMemory<char> Password { get; }

    /// <summary>
    /// Gets a value indicating whether a lockfile (.ldb / .laccdb) is created
    /// alongside the database while it is open, and deleted on dispose.
    /// </summary>
    public bool UseLockFile { get; }

    /// <summary>
    /// Gets the user / security name written into this opener's slot in the JET lock-file.
    /// </summary>
    public string? LockFileUserName { get; }

    /// <summary>
    /// Gets the machine / computer name written into this opener's slot in the JET lock-file.
    /// </summary>
    public string? LockFileMachineName { get; }

    /// <summary>
    /// Gets a value indicating whether cooperative byte-range page locks are taken against the database file.
    /// </summary>
    public bool UseByteRangeLocks { get; }

    /// <summary>
    /// Gets the maximum time in milliseconds to wait when acquiring a contended byte-range page lock.
    /// </summary>
    public int LockTimeoutMilliseconds { get; }
}
