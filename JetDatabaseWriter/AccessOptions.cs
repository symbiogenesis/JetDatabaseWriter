namespace JetDatabaseWriter;

using System;
using System.IO;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Base configuration options shared by Access database readers and writers.
/// </summary>
/// <param name="useByteRangeLocks">Default byte-range lock setting for the concrete option type.</param>
public abstract class AccessOptions(bool useByteRangeLocks) : IAccessOptions
{
    internal const int DefaultLockTimeoutMilliseconds = 5_000;
    private const int LockFileIdentityMaxLength = 31;

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessOptions"/> class using a plain-text password.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text password. Null means no password.</param>
    /// <param name="useByteRangeLocks">Default byte-range lock setting for the concrete option type.</param>
    protected AccessOptions(string? plainTextPassword, bool useByteRangeLocks)
        : this(useByteRangeLocks)
        => this.Password = plainTextPassword.AsMemory();

    /// <summary>
    /// Gets the password for opening password-protected databases.
    /// Empty (the default) means no password is supplied.
    /// </summary>
    public ReadOnlyMemory<char> Password { get; init; }

    /// <summary>
    /// Gets a value indicating whether a lockfile (.ldb / .laccdb) is created
    /// alongside the database while it is open, and deleted on dispose.
    /// Default: true.
    /// </summary>
    public bool UseLockFile { get; init; } = true;

    /// <summary>
    /// Gets the user / security name written into this opener's slot in the
    /// JET lock-file (.ldb / .laccdb). When <see langword="null"/> (the default),
    /// <see cref="Environment.UserName"/> is used. Truncated to 31 ASCII characters;
    /// non-ASCII characters are replaced with '?' to match Access's slot format.
    /// </summary>
    public string? LockFileUserName { get; init; }

    /// <summary>
    /// Gets the machine / computer name written into this opener's slot in the
    /// JET lock-file (.ldb / .laccdb). When <see langword="null"/> (the default),
    /// <see cref="Environment.MachineName"/> is used. Truncated to 31 ASCII characters;
    /// non-ASCII characters are replaced with '?' to match Access's slot format.
    /// </summary>
    public string? LockFileMachineName { get; init; }

    /// <summary>
    /// Gets a value indicating whether cooperative byte-range page locks are taken
    /// against the database file. No-op where <see cref="FileStream.Lock(long, long)"/>
    /// is unsupported or when opened from a non-<see cref="FileStream"/>.
    /// </summary>
    public bool UseByteRangeLocks { get; init; } = useByteRangeLocks;

    /// <summary>
    /// Gets the maximum time in milliseconds to wait when acquiring a contended
    /// byte-range page lock before throwing <see cref="IOException"/>.
    /// Default: <c>5000</c>.
    /// </summary>
    public int LockTimeoutMilliseconds { get; init; } = DefaultLockTimeoutMilliseconds;

    internal static LockFileSettings CreateDefaultLockFileSettings(bool respectExisting)
        => new(
            Enabled: true,
            RespectExisting: respectExisting,
            UserName: NormalizeLockFileIdentity(Environment.UserName),
            MachineName: NormalizeLockFileIdentity(Environment.MachineName));

    internal static int ValidateLockTimeoutMilliseconds(int value)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Lock timeout cannot be negative.");
        }

        return value;
    }

    internal static string NormalizeLockFileIdentity(string value)
    {
        int length = Math.Min(value.Length, LockFileIdentityMaxLength);
        char[] normalized = new char[length];
        for (int i = 0; i < length; i++)
        {
            char c = value[i];
            normalized[i] = c is >= (char)0x20 and < (char)0x7F ? c : '?';
        }

        return new string(normalized);
    }

    internal JetByteRangeLock CreateByteRangeLock(Stream stream)
        => JetByteRangeLock.Create(stream, this.UseByteRangeLocks, this.LockTimeoutMilliseconds);

    internal LockFileSettings CreateLockFileSettings(bool respectExisting)
        => new(
            Enabled: this.UseLockFile,
            RespectExisting: respectExisting,
            UserName: NormalizeLockFileIdentity(this.LockFileUserName ?? Environment.UserName),
            MachineName: NormalizeLockFileIdentity(this.LockFileMachineName ?? Environment.MachineName));
}
