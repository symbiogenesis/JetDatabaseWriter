namespace JetDatabaseWriter;

using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Configuration options for opening a JET database with <see cref="AccessWriter"/>.
/// </summary>
public sealed class AccessWriterOptions : AccessOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AccessWriterOptions"/> class.
    /// </summary>
    public AccessWriterOptions()
        : base(useByteRangeLocks: JetByteRangeLock.PlatformSupportsByteRangeLocks())
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessWriterOptions"/> class using a plain-text password.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text password. Null means no password.</param>
    public AccessWriterOptions(string? plainTextPassword)
        : base(plainTextPassword, useByteRangeLocks: JetByteRangeLock.PlatformSupportsByteRangeLocks())
    {
    }

    /// <summary>
    /// Gets a value indicating whether <see cref="AccessWriter.CreateDatabaseAsync(string, DatabaseFormat, AccessWriterOptions?, System.Threading.CancellationToken)"/>
    /// emits the full 17-column Microsoft Access <c>MSysObjects</c> catalog schema.
    /// <code>Id, ParentId, Name, Type, DateCreate, DateUpdate, Owner, Flags, Database,
    /// Connect, ForeignName, RmtInfoShort, RmtInfoLong, Lv, LvProp, LvModule, LvExtra</code>
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
    /// When <c>true</c> and <see cref="AccessOptions.UseLockFile"/> is also <c>true</c>, opening a
    /// database that already has a lockfile throws an <see cref="System.IO.IOException"/>.
    /// When <c>true</c>, lockfile creation is strict: if the lockfile cannot be created
    /// (for example, due to permissions), the open operation throws.
    /// Set to <c>false</c> for best-effort lockfile behavior (previous behaviour).
    /// Default: true.
    /// </summary>
    public bool RespectExistingLockFile { get; init; } = true;

    /// <summary>
    /// Gets the maximum number of distinct pages a single explicit transaction
    /// (started via <see cref="AccessWriter.BeginTransactionAsync(System.Threading.CancellationToken)"/>)
    /// may journal in memory before the next page write throws a
    /// <see cref="Exceptions.JetLimitationException"/> and the
    /// transaction is automatically rolled back. Each journaled page costs
    /// <see cref="AccessBase.PageSize"/> bytes of process memory.
    /// Default: <c>16384</c> (~64 MiB at the standard 4&#8239;KiB ACE page size).
    /// </summary>
    public int MaxTransactionPageBudget { get; init; } = 16_384;

    /// <summary>
    /// Gets the secure-erase behavior used by destructive writer operations.
    /// The default preserves normal JET behavior: deleted rows are marked
    /// deleted but their old payload bytes may remain in the file until Access
    /// or the writer reuses the space. When set to
    /// <see cref="SecureEraseMode.DeletedRowsAndFreedPages"/>, deleted row
    /// bodies and freed page payloads are overwritten before the storage is
    /// returned to the global page free list.
    /// </summary>
    public SecureEraseMode SecureEraseMode { get; init; } = SecureEraseMode.None;

    /// <summary>
    /// Gets a value indicating whether every public mutation method on
    /// <see cref="AccessWriter"/> is wrapped in an implicit
    /// <see cref="JetTransaction"/> when no explicit transaction is active.
    /// When <see langword="true"/>, each call to <c>CreateTableAsync</c>,
    /// <c>InsertRowsAsync</c>, <c>UpdateRowsAsync</c>, etc. begins a private
    /// transaction at entry, commits it on success, and rolls it back on
    /// exception before commit replay &#8212; so validation and write-preparation
    /// failures leave the database in its pre-call state instead of in whatever
    /// partially-flushed state the page-write pipeline had reached. Calls made
    /// inside an explicit transaction are unaffected.
    /// <para>
    /// This is an in-memory page journal, not a durable write-ahead log. If the
    /// process, stream, device, or cancellation token fails after
    /// <see cref="JetTransaction.CommitAsync(System.Threading.CancellationToken)"/>
    /// starts replaying buffered pages, pages already written remain on disk and
    /// no recovery pass is attempted.
    /// </para>
    /// <para>
    /// Default: <see langword="false"/> (preserves the flush-per-page
    /// behaviour). The flag is intentionally opt-in for the first release;
    /// the plan is to flip the default in a later major version once it has
    /// bake time.
    /// </para>
    /// </summary>
    public bool UseTransactionalWrites { get; init; }
}
