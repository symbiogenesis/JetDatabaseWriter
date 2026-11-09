namespace JetDatabaseWriter.Interfaces;

using System.IO;

/// <summary>
/// Supplies persistent database identity, monotonic generation binding, journal discovery,
/// and exclusive lease acquisition for caller-owned <see cref="Stream"/> instances.
/// </summary>
/// <remarks>
/// Stream overloads without an adapter retain live in-process rollback only.
/// Configuring this adapter on <see cref="AccessOptions.PersistentStreamRecoveryAdapter"/>
/// enables persistent undo-journal preparation, pre-header recovery on reopen, and
/// hot-journal refusal across caller-owned streams.
/// </remarks>
public interface IPersistentStreamRecoveryAdapter
{
    /// <summary>
    /// Gets the stable, non-empty canonical identity of the underlying database storage.
    /// </summary>
    public string DatabaseIdentity { get; }

    /// <summary>
    /// Gets the current non-negative monotonic transaction generation persisted by the backing store.
    /// </summary>
    public long Generation { get; }

    /// <summary>
    /// Gets a value indicating whether the backing store fences and retires prior in-flight I/O
    /// before a newly acquired lease replays or mutates the database stream.
    /// </summary>
    public bool HasQuiescentCrashFencing { get; }

    /// <summary>
    /// Determines whether pending recovery or preparation evidence exists for the database stream.
    /// </summary>
    /// <param name="databaseStream">The caller-supplied database stream.</param>
    /// <returns><see langword="true"/> when committed or preparing journal evidence is present; otherwise, <see langword="false"/>.</returns>
    public bool HasPendingJournal(Stream databaseStream);

    /// <summary>
    /// Acquires exclusive recovery and write authority for <paramref name="databaseStream"/>,
    /// invalidating any stale prior owner before returning.
    /// </summary>
    /// <param name="databaseStream">The caller-supplied database stream.</param>
    /// <returns>An active exclusive lease held for the lifetime of the writer.</returns>
    public IPersistentStreamRecoveryLease AcquireExclusiveLease(Stream databaseStream);
}
