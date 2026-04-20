namespace JetDatabaseReader;

/// <summary>
/// Configuration options for opening a JET database with <see cref="AccessWriter"/>.
/// </summary>
public sealed class AccessWriterOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether a lockfile (.ldb / .laccdb) is created
    /// alongside the database while it is open, and deleted on dispose.
    /// Default: true.
    /// </summary>
    public bool UseLockFile { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether an existing lockfile is respected.
    /// When <c>true</c> and <see cref="UseLockFile"/> is also <c>true</c>, opening a
    /// database that already has a lockfile throws an <see cref="System.IO.IOException"/>.
    /// Set to <c>false</c> to silently overwrite an existing lockfile (previous behaviour).
    /// Default: true.
    /// </summary>
    public bool RespectExistingLockFile { get; set; } = true;
}
