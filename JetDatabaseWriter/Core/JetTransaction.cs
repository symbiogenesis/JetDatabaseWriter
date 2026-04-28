namespace JetDatabaseWriter.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal.Transactions;

/// <summary>
/// Represents an explicit, in-memory write transaction against a single
/// <see cref="AccessWriter"/>. Page mutations performed inside a transaction
/// are buffered in a <see cref="PageJournal"/> until <see cref="CommitAsync"/>
/// atomically replays them to the database file. <see cref="RollbackAsync"/>
/// (and <see cref="DisposeAsync"/> on an uncommitted transaction) discards
/// the journal — because nothing was written to disk during the transaction,
/// rollback leaves the file in its pre-transaction state.
/// </summary>
/// <remarks>
/// <para>
/// Only one transaction may be active at a time per <see cref="AccessWriter"/>;
/// a second concurrent <see cref="AccessWriter.BeginTransactionAsync"/> call
/// throws <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// The journal grows in process memory at <c>PageSize</c> bytes per dirty page.
/// <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> caps the journal;
/// exceeding the cap throws <see cref="JetDatabaseWriter.Exceptions.JetLimitationException"/>
/// from the next page write, and the transaction is automatically rolled back.
/// </para>
/// </remarks>
public sealed class JetTransaction : IAsyncDisposable
{
    private readonly AccessWriter _writer;
    private readonly PageJournal _journal;
    private bool _committed;
    private bool _rolledBack;

    internal JetTransaction(AccessWriter writer, PageJournal journal)
    {
        _writer = writer;
        _journal = journal;
    }

    /// <summary>Gets a value indicating whether the transaction has been committed.</summary>
    public bool IsCommitted => _committed;

    /// <summary>Gets a value indicating whether the transaction has been rolled back.</summary>
    public bool IsRolledBack => _rolledBack;

    /// <summary>Gets the number of distinct pages currently buffered in the journal.</summary>
    public int JournaledPageCount => _journal.Count;

    internal PageJournal Journal => _journal;

    internal bool IsTerminated => _committed || _rolledBack;

    /// <summary>
    /// Atomically writes every buffered page to the database file, applying
    /// per-page encryption and acquiring cooperative byte-range locks (when
    /// enabled) just like a non-transactional write would.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous commit.</returns>
    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        => _writer.CommitTransactionAsync(this, cancellationToken);

    /// <summary>
    /// Discards the journal without touching the database file. Safe to call
    /// repeatedly; subsequent <see cref="CommitAsync"/> calls throw
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous rollback.</returns>
    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
        => _writer.RollbackTransactionAsync(this, cancellationToken);

    /// <summary>
    /// Rolls back the transaction if it has not been committed. Equivalent to
    /// calling <see cref="RollbackAsync"/> and discarding any errors.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose.</returns>
    public async ValueTask DisposeAsync()
    {
        if (IsTerminated)
        {
            return;
        }

        try
        {
            await RollbackAsync().ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // Already terminated by another caller — DisposeAsync is best-effort.
        }
    }

    internal void MarkCommitted() => _committed = true;

    internal void MarkRolledBack() => _rolledBack = true;
}
