namespace JetDatabaseWriter;

using System.Security;

/// <summary>
/// Configuration options for opening a JET database with <see cref="AccessWriter"/>.
/// </summary>
public sealed class AccessWriterOptions : IAccessOptions
{
    private SecureString? _password;

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessWriterOptions"/> class.
    /// </summary>
    public AccessWriterOptions()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessWriterOptions"/> class using a plain-text password.
    /// The password is converted to a read-only <see cref="SecureString"/>.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text password. Null means no password.</param>
    public AccessWriterOptions(string? plainTextPassword)
    {
        _password = SecureStringUtilities.FromPlainText(plainTextPassword);
    }

    /// <summary>
    /// Gets the password for opening password-protected databases.
    /// A read-only copy is stored during initialization.
    /// When specified, it is propagated to internal reader operations used by the writer.
    /// </summary>
    public SecureString? Password
    {
        get => _password;
        init
        {
            _password?.Dispose();
            _password = SecureStringUtilities.CopyAsReadOnly(value);
        }
    }

    /// <summary>
    /// Gets a value indicating whether a lockfile (.ldb / .laccdb) is created
    /// alongside the database while it is open, and deleted on dispose.
    /// Default: true.
    /// </summary>
    public bool UseLockFile { get; init; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether an existing lockfile is respected.
    /// When <c>true</c> and <see cref="UseLockFile"/> is also <c>true</c>, opening a
    /// database that already has a lockfile throws an <see cref="System.IO.IOException"/>.
    /// When <c>true</c>, lockfile creation is strict: if the lockfile cannot be created
    /// (for example, due to permissions), the open operation throws.
    /// Set to <c>false</c> for best-effort lockfile behavior (previous behaviour).
    /// Default: true.
    /// </summary>
    public bool RespectExistingLockFile { get; set; } = true;
}
