namespace JetDatabaseWriter;

using System;
using System.IO;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Base configuration options shared by Access database readers and writers.
/// </summary>
/// <param name="useByteRangeLocks">Default byte-range lock setting for the concrete option type.</param>
public abstract class AccessOptions(bool useByteRangeLocks)
{
    internal const int DefaultLockTimeoutMilliseconds = 5_000;
    private const int LockFileIdentityMaxLength = 31;
    private ReadOnlyMemory<char> password;
    private bool useLockFile = true;

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
    public ReadOnlyMemory<char> Password
    {
        get => this.password;
        init => this.password = value;
    }

    /// <summary>
    /// Gets the maximum password-hash iteration count permitted while opening
    /// an encrypted file. Default: 1,000,000. Agile permits at most 10,000,000;
    /// native Standard uses 50,000. Increase explicitly for a trusted costly file.
    /// Zero permits only descriptors that require no iterations.
    /// </summary>
    public int MaxEncryptionSpinCount { get; init; } = 1_000_000;

    /// <summary>
    /// Gets the maximum encryption descriptor size in bytes. For Agile this
    /// caps the XML after its eight-byte version header; for binary providers
    /// it caps the complete descriptor. Default: 1 MiB. Must be positive.
    /// </summary>
    public int MaxEncryptionInfoBytes { get; init; } = 1024 * 1024;

    /// <summary>
    /// Gets the maximum logical byte length of one table definition, including
    /// continuation pages. Default: 16 MiB. Must be positive. Increase explicitly
    /// to permit larger schemas; aggregate schema and cache memory is separate.
    /// </summary>
    public int MaxTableDefinitionBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>
    /// Gets the maximum combined logical table-definition bytes inspected by one
    /// catalog traversal, including the system catalog. Default: 64 MiB. Must be
    /// positive. Catalog row collections are also limited to one entry per 64
    /// bytes of this budget (at least one entry); this is not a total-memory limit.
    /// </summary>
    public int MaxSchemaTraversalBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Gets the maximum stored byte length permitted for one MEMO or OLE long value
    /// when reading, writing, or compacting a database.
    /// Default: 16,777,215 bytes (<c>0xFFFFFF</c>). The caller may set a positive budget
    /// up to the native 30-bit LVAL descriptor maximum of 1,073,741,823 bytes (<c>0x3FFFFFFF</c>).
    /// Larger budgets permit correspondingly larger allocations for individual values.
    /// </summary>
    public int MaxLongValueBytes { get; init; } = 0xFFFFFF;

    /// <summary>
    /// Gets a value indicating whether a lockfile (.ldb / .laccdb) slot is maintained
    /// alongside a file database while open. Disposal releases this opener's slot and
    /// attempts to remove an unused lockfile; other holders or permissions can retain it.
    /// Default: true.
    /// </summary>
    public bool UseLockFile
    {
        get => this.useLockFile;
        init => this.useLockFile = value;
    }

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
    /// is unsupported or when opened from a non-<see cref="FileStream"/>. Only
    /// <see cref="AccessWriter"/> takes them: <see cref="AccessReader"/> never
    /// writes and ignores this option.
    /// </summary>
    public bool UseByteRangeLocks { get; init; } = useByteRangeLocks;

    /// <summary>
    /// Gets the maximum time in milliseconds to wait when acquiring a contended
    /// byte-range page lock before throwing <see cref="Exceptions.JetLockException"/>
    /// with <see cref="Exceptions.JetErrorCode.LockTimeout"/>.
    /// Default: <c>5000</c>.
    /// </summary>
    public int LockTimeoutMilliseconds { get; init; } = DefaultLockTimeoutMilliseconds;

    /// <summary>
    /// Gets the maximum combined pending and live page leases and in-flight direct page reads. Default: 4096.
    /// The positive allowance is shared by concurrent operations on this reader or writer.
    /// Exhaustion refuses immediately, including cache hits, instead of waiting
    /// while a traversal holds other leases. Together with retained cache capacity
    /// and <see cref="MaxLivePageFrames"/>, this bounds page images owned through the
    /// reader or writer cache; pooled array sizes, decoded values and caller-owned
    /// memory remain separate resources.
    /// </summary>
    public int MaxActivePageLeases { get; init; } = 4096;

    /// <summary>
    /// Gets the maximum total live page frames (retained cached frames plus evicted frames
    /// still pinned by active leases). Default: 0, which uses the retained cache capacity
    /// plus <see cref="MaxActivePageLeases"/>. Set a positive value to bound combined
    /// retained and evicted-pinned page frames explicitly. Negative values are refused.
    /// </summary>
    public int MaxLivePageFrames { get; init; }

    /// <summary>
    /// Gets the maximum aggregate row-directory slot entries cached across live page frames.
    /// Default: 262,144. Must be positive. When a page frame's parsed row directory would
    /// exceed the remaining budget, the directory is returned to the caller without being
    /// retained on the frame.
    /// </summary>
    public int MaxCachedRowDirectoryEntries { get; init; } = 262_144;

    /// <summary>
    /// Gets an optional persistent recovery adapter for caller-supplied non-<see cref="FileStream"/>
    /// database streams. When <see langword="null"/> (the default), non-<see cref="FileStream"/> streams
    /// use in-process rollback only and do not persist cross-open recovery journals.
    /// </summary>
    public IPersistentStreamRecoveryAdapter? PersistentStreamRecoveryAdapter { get; init; }

    /// <summary>Copies every option while replacing only connection-specific settings.</summary>
    /// <param name="password">The password resolved for this connection.</param>
    /// <param name="useLockFile">Whether this connection maintains a lockfile.</param>
    /// <returns>A shallow copy retaining the concrete option type and caller-supplied collaborators.</returns>
    private protected AccessOptions CloneWithConnectionSettings(ReadOnlyMemory<char> password, bool useLockFile)
    {
        AccessOptions copy = (AccessOptions)this.MemberwiseClone();
        copy.password = password;
        copy.useLockFile = useLockFile;
        return copy;
    }

    internal static void ValidatePersistentStreamRecoveryAdapter(IPersistentStreamRecoveryAdapter? adapter)
    {
        if (adapter is null)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(adapter.DatabaseIdentity))
        {
#pragma warning disable CA2208 // The public options parameter carries this value.
            throw new ArgumentException("The persistent stream recovery adapter must supply a non-empty DatabaseIdentity.", "options");
#pragma warning restore CA2208
        }

        if (adapter.Generation < 0)
        {
#pragma warning disable CA2208 // The public options parameter carries this value.
            throw new ArgumentOutOfRangeException("options", adapter.Generation, "The persistent stream recovery adapter generation cannot be negative.");
#pragma warning restore CA2208
        }

        if (!adapter.HasQuiescentCrashFencing)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "Persistent caller-stream recovery requires host crash-quiescence confirmation (HasQuiescentCrashFencing = true).");
        }
    }

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
