namespace JetDatabaseWriter;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Represents an explicit, in-memory write transaction against a single
/// <see cref="AccessWriter"/>. Page mutations performed inside a transaction
/// are buffered in a <see cref="PagerTransaction"/> (the new contents of each dirty
/// page) until <see cref="CommitAsync"/> writes them over the database file in
/// place. <see cref="RollbackAsync"/> (and <see cref="DisposeAsync"/> on an
/// uncommitted transaction) discards the journal: nothing reaches the file
/// before commit, so rollback leaves the file as it was and also returns the
/// writer's cached catalog and constraint state to how it was when the
/// transaction began.
/// </summary>
/// <remarks>
/// <para>
/// Commit retains raw before-images and restores them after a write or flush
/// failure. File-backed transactions persist recovery images beside the database;
/// the next writer open validates and recovers a process-interrupted commit under
/// exclusive access, with the database and journal retained at their original path.
/// Power-loss persistence of journal directory entries is unverified. Other
/// caller-supplied streams cannot recover after a process crash.
/// </para>
/// <para>
/// Repeated <see cref="AccessWriter.BeginTransactionAsync"/> calls nest up to seven
/// levels. Database mutations are permitted only at levels one through five, matching
/// native DAO writes; end deeper scopes before writing. Resolve the innermost first.
/// A child commit merges its changes
/// into its parent without writing or flushing the database; a parent rollback also
/// undoes committed children. Child rollback or disposal restores only that child.
/// Only the outermost commit persists changes. Scopes belong to the writer, so calls
/// from other tasks participate in its current innermost scope.
/// </para>
/// <para>
/// The shared journal holds <c>PageSize</c> bytes per distinct dirty page.
/// <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> caps that page count;
/// nested savepoint prior images and writer-state snapshots consume additional memory.
/// All scopes share this budget, and opening a child does not reset it.
/// Exceeding the cap throws <see cref="JetLimitationException"/> from the next
/// page write. The transaction stays active, but the operation that hit the
/// cap is rolled back to its internal savepoint. The transaction remains usable.
/// Private statement transactions are automatic regardless of
/// <see cref="AccessWriterOptions.UseTransactionalWrites"/>; that option controls
/// the additional durable-flush request for non-file stores.
/// </para>
/// </remarks>
public sealed class JetTransaction : IAsyncDisposable
{
    /// <summary>The message thrown when a commit or rollback targets a transaction that has ended.</summary>
    internal const string TerminatedMessage = "The transaction has already been committed or rolled back, or its commit failed.";

    private readonly TransactionLifecycle lifecycle;

    internal JetTransaction(TransactionLifecycle lifecycle, PagerTransaction journal, JetTransaction? parent = null)
    {
        this.lifecycle = lifecycle;
        this.Journal = journal;
        this.Parent = parent;
        this.NestingLevel = (parent?.NestingLevel ?? 0) + 1;
    }

    /// <summary>Gets a value indicating whether this scope committed. A child commit remains subject to parent rollback.</summary>
    public bool IsCommitted { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the transaction has been rolled back,
    /// leaving the database file as it was before the transaction. This is
    /// also set when <see cref="CommitAsync"/> fails and restores the original
    /// page images and file length.
    /// When <see cref="CommitAsync"/> throws and both this and
    /// <see cref="IsCommitted"/> are <see langword="false"/>, the writer faulted
    /// during journal preparation, undo, or an uncertain commit-decision flush.
    /// Further mutations are refused; retain file recovery evidence and reopen
    /// with the writer, or restore a custom stream through the caller's protocol.
    /// </summary>
    public bool IsRolledBack { get; private set; }

    /// <summary>Gets the number of distinct pages currently buffered in the journal shared by all scopes.</summary>
    public int JournaledPageCount => this.Journal.Count;

    internal PagerTransaction Journal { get; }

    internal JetTransaction? Parent { get; }

    internal int NestingLevel { get; }

    /// <summary>Gets a value indicating whether a commit failed after it had started writing pages.</summary>
    internal bool IsCommitFailed { get; private set; }

    internal bool IsTerminated => this.IsCommitted || this.IsRolledBack || this.IsCommitFailed;

    /// <summary>
    /// Merges a child scope into its parent, or writes every buffered page over the database
    /// file in ascending page order, applying per-page encryption and acquiring cooperative
    /// byte-range locks (when enabled) just like a non-transactional write
    /// would, then flushes the stream. The transaction ends whether or not
    /// the commit succeeds. An ancestor cannot commit while a child is active.
    /// </summary>
    /// <remarks>
    /// <para>
    /// If the commit fails before the first page write starts (for example
    /// because <paramref name="cancellationToken"/> was cancelled, or the
    /// commit lock timed out), the file is unchanged and
    /// <see cref="IsRolledBack"/> is <see langword="true"/>. Failure to prepare
    /// the persistent journal instead faults the writer.
    /// </para>
    /// <para>
    /// Once the first page write starts, cancellation is ignored and the
    /// commit runs to completion, because stopping partway would leave the
    /// file holding only part of the transaction. A write or flush failure before
    /// recording the commit decision
    /// restores the raw original page images and file length, marks the
    /// transaction rolled back, and propagates the original exception. If
    /// restoration also fails, an <see cref="AggregateException"/> carries both
    /// failures and the writer rejects further mutations. Dispose then only
    /// releases handles. For files, retain the journal and reopen with the writer
    /// to attempt validated recovery; custom streams need caller-managed recovery.
    /// A failed commit-decision flush also faults the writer and preserves the
    /// database and journal, because the committed outcome is uncertain.
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">A token used to cancel the commit before it starts writing pages.</param>
    /// <returns>A task representing the asynchronous commit.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the transaction has already ended or is not active on its writer.</exception>
    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        => this.lifecycle.CommitTransactionAsync(this, cancellationToken);

    /// <summary>
    /// Discards this scope's changes without touching the database file, and returns
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
    /// Rolls back the transaction if it is the current scope and has not ended.
    /// Disposing an ancestor while a child is active throws and leaves both scopes active.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose.</returns>
    /// <exception cref="InvalidOperationException">A child transaction is still active on this writer.</exception>
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
        catch (JetOperationException ex) when (ex.ErrorCode == JetErrorCode.TransactionEnded)
        {
            // Already terminated by another caller — DisposeAsync is best-effort.
        }
    }

    internal void MarkCommitted() => this.IsCommitted = true;

    internal void MarkRolledBack() => this.IsRolledBack = true;

    /// <summary>Ends the transaction after a commit that failed once it had started writing pages.</summary>
    internal void MarkCommitFailed() => this.IsCommitFailed = true;
}
