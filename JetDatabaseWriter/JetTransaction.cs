namespace JetDatabaseWriter;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Represents an explicit, in-memory write transaction against a single
/// <see cref="AccessWriter"/>. Page mutations performed inside a transaction
/// are buffered in a <see cref="PageJournal"/> (the new contents of each dirty
/// page) until <see cref="CommitAsync"/> writes them over the database file in
/// place. <see cref="RollbackAsync"/> (and <see cref="DisposeAsync"/> on an
/// uncommitted transaction) discards the journal: nothing reaches the file
/// before commit, so rollback leaves the file as it was and also returns the
/// writer's cached catalog and constraint state to how it was when the
/// transaction began.
/// </summary>
/// <remarks>
/// <para>
/// Commit is not crash-atomic. There is no before-image or write-ahead log, so
/// if the process, stream or device fails after commit has started writing
/// pages, the file holds part of the transaction and no recovery is attempted.
/// </para>
/// <para>
/// Only one transaction may be active at a time per <see cref="AccessWriter"/>;
/// a second concurrent <see cref="AccessWriter.BeginTransactionAsync"/> call
/// throws <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// The journal grows in process memory at <c>PageSize</c> bytes per dirty page.
/// <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> caps the journal;
/// exceeding the cap throws <see cref="JetLimitationException"/> from the next
/// page write. The transaction stays active, but the operation that hit the
/// cap may be partly applied to the journal, so roll it back. An implicit
/// <see cref="AccessWriterOptions.UseTransactionalWrites"/> transaction is
/// rolled back automatically.
/// </para>
/// </remarks>
public sealed class JetTransaction : IAsyncDisposable
{
    /// <summary>The message thrown when a commit or rollback targets a transaction that has ended.</summary>
    internal const string TerminatedMessage = "The transaction has already been committed or rolled back, or its commit failed.";

    private readonly TransactionLifecycle lifecycle;

    internal JetTransaction(TransactionLifecycle lifecycle, PageJournal journal)
    {
        this.lifecycle = lifecycle;
        this.Journal = journal;
    }

    /// <summary>Gets a value indicating whether the transaction has been committed.</summary>
    public bool IsCommitted { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the transaction has been rolled back,
    /// leaving the database file as it was before the transaction. This is
    /// also set when <see cref="CommitAsync"/> fails before it starts writing
    /// the first page.
    /// When <see cref="CommitAsync"/> throws and both this and
    /// <see cref="IsCommitted"/> are <see langword="false"/>, the commit failed
    /// after it had started writing pages, and the file may hold part of the
    /// transaction.
    /// </summary>
    public bool IsRolledBack { get; private set; }

    /// <summary>Gets the number of distinct pages currently buffered in the journal.</summary>
    public int JournaledPageCount => this.Journal.Count;

    internal PageJournal Journal { get; }

    /// <summary>Gets a value indicating whether a commit failed after it had started writing pages.</summary>
    internal bool IsCommitFailed { get; private set; }

    internal bool IsTerminated => this.IsCommitted || this.IsRolledBack || this.IsCommitFailed;

    /// <summary>
    /// Writes every buffered page over the database file in ascending page
    /// order, applying per-page encryption and acquiring cooperative
    /// byte-range locks (when enabled) just like a non-transactional write
    /// would, then flushes the stream. The transaction ends whether or not
    /// the commit succeeds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// If the commit fails before it writes the first page (for example
    /// because <paramref name="cancellationToken"/> was cancelled, or the
    /// commit lock timed out), the file is unchanged and
    /// <see cref="IsRolledBack"/> is <see langword="true"/>.
    /// </para>
    /// <para>
    /// Once the first page write starts, cancellation is ignored and the
    /// commit runs to completion, because stopping partway would leave the
    /// file holding only part of the transaction. If an I/O error stops it
    /// instead, the exception propagates, neither <see cref="IsCommitted"/>
    /// nor <see cref="IsRolledBack"/> is set, and the file may hold part of
    /// the transaction; restore it from a copy taken before the commit.
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">A token used to cancel the commit before it starts writing pages.</param>
    /// <returns>A task representing the asynchronous commit.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the transaction has already ended or is not active on its writer.</exception>
    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        => this.lifecycle.CommitTransactionAsync(this, cancellationToken);

    /// <summary>
    /// Discards the journal without touching the database file, and returns
    /// the writer's cached catalog and constraint state to how it was when the
    /// transaction began. Subsequent <see cref="CommitAsync"/> or
    /// <see cref="RollbackAsync"/> calls throw
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous rollback.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the transaction has already ended or is not active on its writer.</exception>
    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
        => this.lifecycle.RollbackTransactionAsync(this, cancellationToken);

    /// <summary>
    /// Rolls back the transaction if it has not ended. Equivalent to calling
    /// <see cref="RollbackAsync"/> and discarding an "already ended" error.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose.</returns>
    public async ValueTask DisposeAsync()
    {
        if (this.IsTerminated)
        {
            return;
        }

        try
        {
            await this.RollbackAsync().ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // Already terminated by another caller — DisposeAsync is best-effort.
        }
    }

    internal void MarkCommitted() => this.IsCommitted = true;

    internal void MarkRolledBack() => this.IsRolledBack = true;

    /// <summary>Ends the transaction after a commit that failed once it had started writing pages.</summary>
    internal void MarkCommitFailed() => this.IsCommitFailed = true;
}
