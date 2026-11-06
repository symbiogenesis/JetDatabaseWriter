namespace JetDatabaseWriter;

using System;

/// <summary>
/// Configuration options for <see cref="AccessDatabaseCompact"/> operations.
/// </summary>
public sealed class AccessCompactOptions : AccessOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AccessCompactOptions"/> class.
    /// </summary>
    public AccessCompactOptions()
        : base(useByteRangeLocks: true)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessCompactOptions"/> class using a plain-text password.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text database password, or <see langword="null"/> for an unprotected database.</param>
    public AccessCompactOptions(string? plainTextPassword)
        : base(plainTextPassword, useByteRangeLocks: true)
    {
    }

    /// <summary>
    /// Gets a value indicating whether <see cref="AccessDatabaseCompact.CompactToAsync"/> and
    /// <see cref="AccessDatabaseCompact.CompactAsync"/> may write a validated salvage copy to a separate
    /// destination when damaged catalog headers, unassigned rows, or opaque pages prevent complete reclamation.
    /// In-place replacement (<see cref="AccessDatabaseCompact.CompactInPlaceAsync"/>) always requires
    /// <see cref="Models.AccessCompactReport.ReplacementAuthorized"/> and never replaces a database with a partial recovery.
    /// Default: <see langword="false"/>.
    /// </summary>
    public bool AllowPartialRecovery { get; init; }

    /// <summary>
    /// Gets the maximum physical page count permitted for source and destination databases during compaction.
    /// Default: <c>524,288</c> (2 GiB at 4 KiB pages).
    /// </summary>
    public long MaxPages { get; init; } = 524_288L;

    /// <summary>
    /// Gets the maximum number of local tables permitted in a compacted database.
    /// Default: <c>4,096</c>.
    /// </summary>
    public int MaxTables { get; init; } = 4_096;

    /// <summary>
    /// Gets the maximum simultaneous working memory admission in bytes for compact planning, construction, and validation.
    /// Default: <c>536,870,912</c> (512 MiB).
    /// </summary>
    public long MaxWorkingBytes { get; init; } = 512L * 1024 * 1024;

    /// <summary>
    /// Gets the maximum cumulative read bytes permitted for compact construction or validation.
    /// Default: <c>17,179,869,184</c> (16 GiB).
    /// </summary>
    public long MaxReadBytes { get; init; } = 16L * 1024 * 1024 * 1024;

    /// <summary>
    /// Gets the maximum cumulative write bytes permitted for compact construction.
    /// Default: <c>17,179,869,184</c> (16 GiB).
    /// </summary>
    public long MaxWriteBytes { get; init; } = 16L * 1024 * 1024 * 1024;

    internal void Validate()
    {
        if (this.MaxEncryptionSpinCount is < 0 or > 10_000_000)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxEncryptionSpinCount), this.MaxEncryptionSpinCount, "MaxEncryptionSpinCount must be between 0 and 10,000,000.");
        }

        if (this.MaxEncryptionInfoBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxEncryptionInfoBytes), this.MaxEncryptionInfoBytes, "MaxEncryptionInfoBytes must be positive.");
        }

        if (this.MaxTableDefinitionBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxTableDefinitionBytes), this.MaxTableDefinitionBytes, "MaxTableDefinitionBytes must be positive.");
        }

        if (this.MaxSchemaTraversalBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxSchemaTraversalBytes), this.MaxSchemaTraversalBytes, "MaxSchemaTraversalBytes must be positive.");
        }

        if (this.MaxPages is <= 0 or > 0x1000000L)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxPages), this.MaxPages, "MaxPages must be between 1 and 16,777,216.");
        }

        if (this.MaxTables <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxTables), this.MaxTables, "MaxTables must be positive.");
        }

        if (this.MaxWorkingBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxWorkingBytes), this.MaxWorkingBytes, "MaxWorkingBytes must be positive.");
        }

        if (this.MaxReadBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxReadBytes), this.MaxReadBytes, "MaxReadBytes must be positive.");
        }

        if (this.MaxWriteBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(this.MaxWriteBytes), this.MaxWriteBytes, "MaxWriteBytes must be positive.");
        }

        _ = ValidateLockTimeoutMilliseconds(this.LockTimeoutMilliseconds);
    }
}
