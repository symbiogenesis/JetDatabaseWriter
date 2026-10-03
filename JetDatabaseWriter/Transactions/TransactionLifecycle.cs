namespace JetDatabaseWriter.Transactions;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Manages the explicit page-buffered transaction lifecycle for an
/// <see cref="AccessWriter"/>: begin, auto-commit wrapping, commit replay,
/// rollback, and dispose-time teardown. Owns the active transaction; the
/// page journal it attaches lives on <see cref="DatabaseFile.ActiveJournal"/>
/// because every page read and write consults it.
/// </summary>
/// <remarks>
/// The journal holds only page images. The writer also caches state in
/// memory that a transaction can change: the user-table catalog, the
/// insert-page hint and writable owned-map set, and the constraint registry.
/// A rollback restores or invalidates each of them, so the writer behaves as
/// if the transaction had never run.
/// </remarks>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="options">The writer options; supplies the auto-commit switch and journal page budget.</param>
/// <param name="byteRangeLock">The cooperative JET byte-range lock used for the commit-lock sentinel.</param>
/// <param name="catalog">The writer's cached user-table catalog, invalidated on rollback.</param>
/// <param name="dataPages">Owns the insert-page hint and writable owned-map set, restored on rollback.</param>
/// <param name="constraints">The writer's constraint registry, restored on rollback.</param>
internal sealed class TransactionLifecycle(
    DatabaseFile db,
    AccessWriterOptions options,
    JetByteRangeLock byteRangeLock,
    TableCatalog catalog,
    DataPageInserter dataPages,
    ConstraintRegistry constraints)
{
    /// <summary>
    /// The writer's in-memory state when <see cref="ActiveTransaction"/> began,
    /// put back if it rolls back. Set and cleared together with it.
    /// </summary>
    private WriterState? stateAtBegin;

    /// <summary>Gets the active explicit transaction, or <see langword="null"/> when none is active.</summary>
    internal JetTransaction? ActiveTransaction { get; private set; }

    /// <summary>
    /// Begins an explicit page-buffered transaction against the owning writer.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ObjectDisposedException">Thrown when the writer has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Thrown when another transaction is already active on the writer.</exception>
    internal async ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken)
    {
        db.ThrowIfDisposed();

        cancellationToken.ThrowIfCancellationRequested();

        await db.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveTransaction is not null)
            {
                throw new InvalidOperationException(
                    "A transaction is already active on this writer. Only one concurrent transaction per AccessWriter is supported.");
            }

            long baseLength = db.DatabaseLengthBytes;
            var journal = new PageJournal(baseLength, db.PageSizeBytes, options.MaxTransactionPageBudget);
            var tx = new JetTransaction(this, journal);
            this.stateAtBegin = new WriterState(dataPages.CaptureState(), constraints.CaptureSnapshot());
            db.ActiveJournal = journal;
            this.ActiveTransaction = tx;
            return tx;
        }
        finally
        {
            _ = db.IoGate.Release();
        }
    }

    /// <summary>
    /// If <see cref="AccessWriterOptions.UseTransactionalWrites"/> is enabled
    /// and no explicit transaction is currently active, wraps
    /// <paramref name="work"/> in a private <see cref="JetTransaction"/> so a
    /// exception before commit replay leaves the database in its pre-call state.
    /// </summary>
    /// <param name="work">The work to execute.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        if (!options.UseTransactionalWrites || this.ActiveTransaction is not null || db.IsDisposed)
        {
            await work(cancellationToken).ConfigureAwait(false);
            return;
        }

        JetTransaction tx = await this.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await work(cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            try
            {
                if (!tx.IsTerminated)
                {
                    await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (InvalidOperationException)
            {
                // Already terminated by a concurrent commit/rollback path.
            }
            catch (IOException)
            {
                // Best-effort rollback; surface the original failure.
            }

            throw;
        }
    }

    /// <summary>
    /// Generic-result variant of <see cref="RunAutoCommitAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="TResult">The result type produced by <paramref name="work"/>.</typeparam>
    /// <param name="work">The work to execute.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
    {
        if (!options.UseTransactionalWrites || this.ActiveTransaction is not null || db.IsDisposed)
        {
            return await work(cancellationToken).ConfigureAwait(false);
        }

        JetTransaction tx = await this.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            TResult? result = await work(cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch
        {
            try
            {
                if (!tx.IsTerminated)
                {
                    await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (InvalidOperationException)
            {
                // Already terminated.
            }
            catch (IOException)
            {
                // Best-effort rollback.
            }

            throw;
        }
    }

    /// <summary>
    /// Commits the supplied <paramref name="transaction"/>: detaches the
    /// journal from the writer and replays each buffered page (in ascending
    /// page-number order) through the normal page-write pipeline so that
    /// per-page encryption and cooperative byte-range locks are honoured.
    /// A failure, including a commit-lock timeout, marks the transaction
    /// rolled back and restores the writer's state as for a rollback.
    /// </summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ObjectDisposedException">Thrown when the writer has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is terminated or is not active on this writer.</exception>
    internal async ValueTask CommitTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        db.ThrowIfDisposed();

        PageJournal journal;
        WriterState? state;
        await db.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (transaction.IsTerminated)
            {
                throw new InvalidOperationException("The transaction has already been committed or rolled back.");
            }

            if (!ReferenceEquals(this.ActiveTransaction, transaction))
            {
                throw new InvalidOperationException("The transaction is not active on this writer.");
            }

            journal = transaction.Journal;
            state = this.stateAtBegin;

            // Detach the journal first so the page-write loop below routes
            // straight to disk.
            db.ActiveJournal = null;
            this.ActiveTransaction = null;
            this.stateAtBegin = null;
        }
        finally
        {
            _ = db.IoGate.Release();
        }

        long? commitLockOffset = null;
        try
        {
            commitLockOffset = await byteRangeLock.AcquireCommitLockOffsetAsync(
                isAccdb: db.Format == Enums.DatabaseFormat.AceAccdb,
                cancellationToken).ConfigureAwait(false);

            foreach (KeyValuePair<long, byte[]> entry in journal.EnumerateInOrder())
            {
                cancellationToken.ThrowIfCancellationRequested();
                await db.WritePageAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
            }

            await this.FlushDurableAsync(cancellationToken).ConfigureAwait(false);
            transaction.MarkCommitted();
        }
        catch
        {
            transaction.MarkRolledBack();
            this.RestoreWriterState(state);
            throw;
        }
        finally
        {
            byteRangeLock.ReleaseCommitLock(commitLockOffset);
        }
    }

    /// <summary>
    /// Rolls back the supplied <paramref name="transaction"/>: discards the
    /// in-memory journal without touching the database file, and puts the
    /// writer's cached catalog, insert hint, owned-map set and constraint
    /// registry back to their state when the transaction began.
    /// </summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is terminated or is not active on this writer.</exception>
    internal async ValueTask RollbackTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        cancellationToken.ThrowIfCancellationRequested();

        await db.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (transaction.IsTerminated)
            {
                throw new InvalidOperationException("The transaction has already been committed or rolled back.");
            }

            if (!ReferenceEquals(this.ActiveTransaction, transaction))
            {
                throw new InvalidOperationException("The transaction is not active on this writer.");
            }

            db.ActiveJournal = null;
            this.ActiveTransaction = null;
            this.RestoreWriterState(this.stateAtBegin);
            this.stateAtBegin = null;
            transaction.MarkRolledBack();
        }
        finally
        {
            _ = db.IoGate.Release();
        }
    }

    /// <summary>
    /// Drops any in-flight transaction so its journal does not survive
    /// dispose. Nothing has been written to disk for an uncommitted
    /// transaction, so this is equivalent to an implicit rollback.
    /// </summary>
    internal async ValueTask DisposeActiveTransactionAsync()
    {
        if (this.ActiveTransaction is null)
        {
            return;
        }

        try
        {
            await this.ActiveTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            db.ActiveJournal = null;
            this.ActiveTransaction = null;
            this.stateAtBegin = null;
        }
    }

    /// <summary>
    /// Flushes the underlying stream durably.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask FlushDurableAsync(CancellationToken cancellationToken)
        => await db.FlushDatabaseStreamAsync(flushToDisk: true, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Returns the writer's in-memory state to <paramref name="state"/> after
    /// a transaction's journal was discarded. The catalog is re-scanned on next
    /// use rather than restored, because the transaction may have created,
    /// dropped or renamed tables.
    /// </summary>
    /// <param name="state">The state captured when the transaction began.</param>
    private void RestoreWriterState(WriterState? state)
    {
        catalog.Invalidate();
        if (state is null)
        {
            return;
        }

        dataPages.RestoreState(state.DataPages);
        constraints.Restore(state.Constraints);
    }

    /// <summary>The writer's in-memory state that a transaction can change.</summary>
    /// <param name="DataPages">The insert-page hint and writable owned-map set.</param>
    /// <param name="Constraints">The constraint registry's contents.</param>
    private sealed record WriterState(DataPageInserterState DataPages, ConstraintRegistrySnapshot Constraints);
}
