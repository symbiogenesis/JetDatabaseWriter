namespace JetDatabaseWriter;

using System;
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
    /// Gets the maximum number of distinct pages an explicit transaction started
    /// via <see cref="AccessWriter.BeginTransactionAsync(System.Threading.CancellationToken)"/>
    /// may journal in memory before the next page write throws a
    /// <see cref="Exceptions.JetLimitationException"/>. The transaction stays active,
    /// with the failed operation rolled back to its internal savepoint. Private
    /// statement transactions are exempt. Each journaled page costs
    /// <see cref="AccessBase.PageSize"/> bytes of process memory.
    /// Must be at least <c>1</c>: <c>OpenAsync</c> and <c>CreateDatabaseAsync</c>
    /// throw <see cref="System.ArgumentOutOfRangeException"/> (parameter
    /// <c>options</c>) for zero or less, before they open, create or write the
    /// file.
    /// Default: <c>16384</c> (~64 MiB at the standard 4&#8239;KiB ACE page size).
    /// </summary>
    public int MaxTransactionPageBudget { get; init; } = 16_384;

    /// <summary>Gets the maximum writer page-cache frame count. Default: 256; zero disables caching.</summary>
    public int PageCacheSize { get; init; } = 256;

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
    /// Gets a value indicating whether successful statement write-back requests
    /// a durable flush to the device when the store supports it. Schema and row
    /// mutations use a private statement transaction regardless of this option:
    /// a work-phase failure or cancellation discards its pages, and a write or
    /// flush failure restores the original bytes and length. Explicit transaction
    /// commits always request a durable flush.
    /// <para>
    /// Cancellation is ignored once physical write-back starts. If restoration
    /// also fails, the writer rejects further mutations with <c>WriterFaulted</c>.
    /// The undo images are held in memory: process or power loss during replay
    /// can leave a partial transaction on disk, with no crash recovery. Compound
    /// encrypted containers are rewrapped on disposal; this option does not make
    /// their individual statements durable. Creation and physical shrinking use
    /// their separate maintenance paths.
    /// </para>
    /// <para>Default: <see langword="false"/>.</para>
    /// </summary>
    public bool UseTransactionalWrites { get; init; }

    /// <summary>
    /// Checks the options a writer cannot work with. <c>OpenAsync</c> and
    /// <c>CreateDatabaseAsync</c> call it first, so a bad value fails before the
    /// file is opened, created or written and before a lock-file slot is taken.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="MaxTransactionPageBudget"/> is zero or negative, or <see cref="PageCacheSize"/> is negative; the parameter named is <c>options</c>, the public parameter that carries it.</exception>
    internal void Validate()
    {
        if (this.PageCacheSize < 0)
        {
#pragma warning disable CA2208 // The public options parameter carries this value.
            throw new ArgumentOutOfRangeException("options", this.PageCacheSize, "AccessWriterOptions.PageCacheSize must be nonnegative.");
#pragma warning restore CA2208
        }

        if (this.MaxTransactionPageBudget <= 0)
        {
#pragma warning disable CA2208 // The value arrives through the public methods' options parameter, which the exception names.
            throw new ArgumentOutOfRangeException(
                "options",
                this.MaxTransactionPageBudget,
                $"AccessWriterOptions.MaxTransactionPageBudget must be at least 1 page; it is {this.MaxTransactionPageBudget}.");
#pragma warning restore CA2208
        }
    }
}
