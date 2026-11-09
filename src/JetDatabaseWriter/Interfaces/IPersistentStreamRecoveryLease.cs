namespace JetDatabaseWriter.Interfaces;

using System;
using System.IO;

/// <summary>
/// Represents an active exclusive writer and recovery lease over a caller-owned <see cref="Stream"/>,
/// providing persistent journal staging, promotion, fencing, and retirement.
/// </summary>
public interface IPersistentStreamRecoveryLease : IDisposable
{
    /// <summary>
    /// Gets the stable, non-empty canonical identity of the leased database storage.
    /// </summary>
    public string DatabaseIdentity { get; }

    /// <summary>
    /// Gets the currently bound monotonic transaction generation for the leased database.
    /// </summary>
    public long Generation { get; }

    /// <summary>
    /// Persistently binds the next monotonic transaction generation before a preparing journal
    /// is promoted to active recovery evidence.
    /// </summary>
    /// <param name="nextGeneration">The strictly advancing generation number to bind.</param>
    public void BindGeneration(long nextGeneration);

    /// <summary>
    /// Attempts to open any pending committed and/or preparing journal streams for recovery.
    /// </summary>
    /// <param name="committedJournal">Receives the promoted journal stream if present; otherwise, <see langword="null"/>.</param>
    /// <param name="preparingJournal">Receives the preparing journal stream if present; otherwise, <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if either journal stream exists; otherwise, <see langword="false"/>.</returns>
    public bool TryOpenPendingJournal(out Stream? committedJournal, out Stream? preparingJournal);

    /// <summary>
    /// Creates a new, empty readable/writable/seekable preparing journal stream for the current transaction.
    /// </summary>
    /// <returns>The newly created preparing journal stream.</returns>
    public Stream CreatePreparingJournal();

    /// <summary>
    /// Atomically promotes the fenced preparing journal to the active recovery journal and returns
    /// a readable, writable, seekable stream positioned over the promoted journal.
    /// </summary>
    /// <returns>The promoted active journal stream.</returns>
    public Stream PromotePreparingJournal();

    /// <summary>
    /// Discards an unpromoted preparing journal after preparation failure.
    /// </summary>
    public void DiscardPreparingJournal();

    /// <summary>
    /// Retires active and preparing journal evidence after a confirmed database fence and recovery or commit decision.
    /// </summary>
    public void RetireJournal();

    /// <summary>
    /// Fences all written bytes and the current length of <paramref name="databaseStream"/> to stable storage.
    /// </summary>
    /// <param name="databaseStream">The database stream to fence.</param>
    public void FenceDatabase(Stream databaseStream);

    /// <summary>
    /// Fences all written bytes and the current length of <paramref name="journalStream"/> to stable storage.
    /// </summary>
    /// <param name="journalStream">The preparing or active journal stream to fence.</param>
    public void FenceJournal(Stream journalStream);
}
