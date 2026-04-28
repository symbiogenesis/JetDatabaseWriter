namespace JetDatabaseWriter.Core;

using System;
using System.Runtime.InteropServices;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;

/// <summary>
/// Configuration options for opening a JET database with <see cref="AccessWriter"/>.
/// </summary>
public sealed class AccessWriterOptions : IAccessOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AccessWriterOptions"/> class.
    /// </summary>
    public AccessWriterOptions()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessWriterOptions"/> class using a plain-text password.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text password. Null means no password.</param>
    public AccessWriterOptions(string? plainTextPassword)
    {
        Password = plainTextPassword.AsMemory();
    }

    /// <summary>
    /// Gets the password for opening password-protected databases.
    /// When specified, it is propagated to internal reader operations used by the writer.
    /// </summary>
    public ReadOnlyMemory<char> Password { get; init; }

    /// <summary>
    /// Gets a value indicating whether a lockfile (.ldb / .laccdb) is created
    /// alongside the database while it is open, and deleted on dispose.
    /// Default: true.
    /// </summary>
    public bool UseLockFile { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether <see cref="AccessWriter.CreateDatabaseAsync(string, DatabaseFormat, AccessWriterOptions?, System.Threading.CancellationToken)"/>
    /// emits the full 17-column Microsoft Access <c>MSysObjects</c> catalog schema
    /// (<c>Id, ParentId, Name, Type, DateCreate, DateUpdate, Owner, Flags, Database,
    /// Connect, ForeignName, RmtInfoShort, RmtInfoLong, Lv, LvProp, LvModule, LvExtra</c>)
    /// instead of the historical 9-column slim schema.
    /// <para>
    /// The full schema is required to persist column-level properties such as
    /// <c>DefaultValueExpression</c>, <c>ValidationRuleExpression</c>,
    /// <c>ValidationText</c>, and <c>Description</c>, because they are stored
    /// in the <c>LvProp</c> column. The slim schema is retained as an opt-out
    /// for tests or callers that hash whole-file output and depend on the legacy
    /// byte layout.
    /// </para>
    /// <para>
    /// Default: <see langword="true"/>. Has no effect when opening an existing
    /// database — the on-disk catalog schema is whatever the file already contains.
    /// </para>
    /// </summary>
    public bool WriteFullCatalogSchema { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether an existing lockfile is respected.
    /// When <c>true</c> and <see cref="UseLockFile"/> is also <c>true</c>, opening a
    /// database that already has a lockfile throws an <see cref="System.IO.IOException"/>.
    /// When <c>true</c>, lockfile creation is strict: if the lockfile cannot be created
    /// (for example, due to permissions), the open operation throws.
    /// Set to <c>false</c> for best-effort lockfile behavior (previous behaviour).
    /// Default: true.
    /// </summary>
    public bool RespectExistingLockFile { get; init; } = true;

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
    /// against the database file during writes (Win32 <c>LockFileEx</c>). When
    /// enabled, every page-write call exclusively locks the page-sized byte range
    /// at <c>pageNumber * pageSize</c> for the duration of the write, mirroring the
    /// JET locking protocol that Microsoft Access, the OLE DB JET provider, and the
    /// ACE engine all observe. This makes concurrent openers (Access included)
    /// serialise their page mutations against this writer.
    /// <para>
    /// Default: <see langword="true"/> on Windows; <see langword="false"/> on other
    /// platforms (the call is silently a no-op there). Has no effect when the writer
    /// was opened from a non-<see cref="System.IO.FileStream"/>, since there is no
    /// Win32 file handle to lock against.
    /// </para>
    /// </summary>
    public bool UseByteRangeLocks { get; init; } = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    /// <summary>
    /// Gets the maximum time in milliseconds to wait when acquiring a contended
    /// byte-range page lock before throwing <see cref="System.IO.IOException"/>.
    /// Matches the JET "Object is currently in use" timeout semantics.
    /// Default: <c>5000</c>.
    /// </summary>
    public int LockTimeoutMilliseconds { get; init; } = 5_000;

    /// <summary>
    /// Gets the maximum number of distinct pages a single explicit transaction
    /// (started via <see cref="AccessWriter.BeginTransactionAsync(System.Threading.CancellationToken)"/>)
    /// may journal in memory before the next page write throws a
    /// <see cref="JetDatabaseWriter.Exceptions.JetLimitationException"/> and the
    /// transaction is automatically rolled back. Each journaled page costs
    /// <see cref="AccessBase.PageSize"/> bytes of process memory.
    /// Default: <c>16384</c> (~64 MiB at the standard 4&#8239;KiB ACE page size).
    /// </summary>
    public int MaxTransactionPageBudget { get; init; } = 16_384;

    /// <summary>
    /// Gets a value indicating whether every public mutation method on
    /// <see cref="AccessWriter"/> is wrapped in an implicit
    /// <see cref="JetTransaction"/> when no explicit transaction is active.
    /// When <see langword="true"/>, each call to <c>CreateTableAsync</c>,
    /// <c>InsertRowsAsync</c>, <c>UpdateRowsAsync</c>, etc. begins a private
    /// transaction at entry, commits it on success, and rolls it back on
    /// exception &#8212; so a crash mid-call leaves the database in its
    /// pre-call state instead of in whatever partially-flushed state the
    /// page-write pipeline had reached. Calls made inside an explicit
    /// transaction are unaffected.
    /// <para>
    /// Default: <see langword="false"/> (preserves today's flush-per-page
    /// behaviour). The flag is intentionally opt-in for the first release;
    /// the plan is to flip the default in a later major version once it has
    /// bake time.
    /// </para>
    /// </summary>
    public bool UseTransactionalWrites { get; init; }
}
