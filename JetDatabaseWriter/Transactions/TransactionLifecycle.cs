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
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Manages the explicit page-buffered transaction lifecycle for an
/// <see cref="AccessWriter"/>: begin, auto-commit wrapping, commit replay,
/// rollback, and dispose-time teardown. Owns the active transaction; the
/// page journal it attaches lives in the writer's <see cref="Pager"/>, because
/// every page read and write consults it, and is attached and detached only
/// through the pager's <see cref="Pager.JournalGate"/>.
/// </summary>
/// <remarks>
/// The journal holds only page images. The writer also caches state in
/// memory that a transaction can change: the user-table catalog, the
/// insert-page hint, the owned-map policy's decisions, and the constraint
/// registry. A rollback restores or invalidates each of them, so the writer
/// behaves as if the transaction had never run.
/// </remarks>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pager">The writer's page file, which holds the journal and through which a commit writes and flushes.</param>
/// <param name="options">The writer options; supplies the auto-commit switch and journal page budget.</param>
/// <param name="byteRangeLock">The cooperative JET byte-range lock used for the commit-lock sentinel.</param>
/// <param name="catalog">The writer's cached user-table catalog, invalidated on rollback.</param>
/// <param name="dataPages">Owns the insert-page hint, restored on rollback.</param>
/// <param name="ownedMaps">Decides whose owned-page usage maps the writer may extend; its decisions are restored on rollback.</param>
/// <param name="constraints">The writer's constraint registry, restored on rollback.</param>
internal sealed class TransactionLifecycle(
    JetFormat format,
    Pager pager,
    AccessWriterOptions options,
    JetByteRangeLock byteRangeLock,
    TableCatalog catalog,
    DataPageInserter dataPages,
    IOwnedMapPolicy ownedMaps,
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
        pager.ThrowIfDisposed();

        cancellationToken.ThrowIfCancellationRequested();

        using Pager.JournalGate gate = await pager.EnterJournalGateAsync(cancellationToken).ConfigureAwait(false);
        if (this.ActiveTransaction is not null)
        {
            throw new InvalidOperationException(
                "A transaction is already active on this writer. Only one concurrent transaction per AccessWriter is supported.");
        }

        var journal = new PageJournal(gate.PhysicalLengthBytes, format.PageSize, options.MaxTransactionPageBudget);
        var tx = new JetTransaction(this, journal);
        this.stateAtBegin = new WriterState(dataPages.CaptureState(), ownedMaps.Capture(), constraints.CaptureSnapshot());
        gate.Attach(journal);
        this.ActiveTransaction = tx;
        return tx;
    }

    /// <summary>
    /// If <see cref="AccessWriterOptions.UseTransactionalWrites"/> is enabled
    /// and no explicit transaction is currently active, wraps
    /// <paramref name="work"/> in a private <see cref="JetTransaction"/> so an
    /// exception before commit replay leaves the database in its pre-call state.
    /// </summary>
    /// <param name="work">The work to execute.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        if (!options.UseTransactionalWrites || this.ActiveTransaction is not null || pager.IsDisposed)
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
        if (!options.UseTransactionalWrites || this.ActiveTransaction is not null || pager.IsDisposed)
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
    /// page-number order) in place through the normal page-write pipeline, so
    /// that per-page encryption and cooperative byte-range locks are honoured,
    /// then flushes. The replay is not crash-atomic: there is no before-image
    /// or redo log, so a failure partway through leaves the pages written so
    /// far on disk.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Once the detach succeeds the transaction always ends. A failure before
    /// the first page write (cancellation, or a commit-lock timeout) leaves the
    /// file untouched: the transaction is marked rolled back and the writer's
    /// state is restored as for a rollback. Cancellation is honoured only up to
    /// that point; the replay and flush then run to completion, because
    /// stopping them would tear the file. A failure after replay starts (an
    /// I/O error while writing or flushing) marks the transaction neither
    /// committed nor rolled back, since the file may hold part of it, and
    /// discards the writer's cached catalog and insert hint.
    /// </para>
    /// </remarks>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">A token used to cancel the operation before replay starts.</param>
    /// <exception cref="ObjectDisposedException">Thrown when the writer has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is terminated or is not active on this writer.</exception>
    internal async ValueTask CommitTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        pager.ThrowIfDisposed();

        PageJournal journal;
        WriterState? state;

        // The detach is memory-only, and the I/O gate is held only briefly per
        // page by other callers, so it ignores the token: a token that is
        // already cancelled then ends the transaction as a rollback below
        // instead of leaving it half-detached. The gate is released before the
        // page writes below, which take it themselves.
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None).ConfigureAwait(false))
        {
            if (transaction.IsTerminated)
            {
                throw new InvalidOperationException(JetTransaction.TerminatedMessage);
            }

            if (!ReferenceEquals(this.ActiveTransaction, transaction))
            {
                throw new InvalidOperationException("The transaction is not active on this writer.");
            }

            journal = transaction.Journal;
            state = this.stateAtBegin;

            // Detach the journal first so the page-write loop below routes
            // straight to disk.
            gate.Detach();
            this.ActiveTransaction = null;
            this.stateAtBegin = null;
        }

        long? commitLockOffset = null;
        bool replayStarted = false;
        try
        {
            commitLockOffset = await byteRangeLock.AcquireCommitLockOffsetAsync(format.CommitLockOffset, cancellationToken).ConfigureAwait(false);

            // Last point at which cancellation is honoured: nothing has
            // reached the file yet. Stopping the replay partway would leave
            // some of the transaction's pages on disk and the rest lost.
            cancellationToken.ThrowIfCancellationRequested();

            // Set before the first write: a write that fails may already have
            // changed part of its page.
            replayStarted = true;
            foreach (KeyValuePair<long, byte[]> entry in journal.EnumerateInOrder())
            {
                await pager.WritePageAsync(entry.Key, entry.Value, CancellationToken.None).ConfigureAwait(false);
            }

            await this.FlushDurableAsync(CancellationToken.None).ConfigureAwait(false);
            transaction.MarkCommitted();
        }
        catch
        {
            if (replayStarted)
            {
                transaction.MarkCommitFailed();
                this.DiscardCachesAfterFailedReplay(state);
            }
            else
            {
                transaction.MarkRolledBack();
                this.RestoreWriterState(state);
            }

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
    /// writer's cached catalog, insert hint, owned-map decisions and
    /// constraint registry back to their state when the transaction began.
    /// </summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is terminated or is not active on this writer.</exception>
    internal async ValueTask RollbackTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        cancellationToken.ThrowIfCancellationRequested();

        using Pager.JournalGate gate = await pager.EnterJournalGateAsync(cancellationToken).ConfigureAwait(false);
        if (transaction.IsTerminated)
        {
            throw new InvalidOperationException(JetTransaction.TerminatedMessage);
        }

        if (!ReferenceEquals(this.ActiveTransaction, transaction))
        {
            throw new InvalidOperationException("The transaction is not active on this writer.");
        }

        gate.Detach();
        this.ActiveTransaction = null;
        this.RestoreWriterState(this.stateAtBegin);
        this.stateAtBegin = null;
        transaction.MarkRolledBack();
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
            // The rollback above detaches the journal under the gate; when it
            // fails, the journal is dropped here without waiting for the gate.
            pager.ForceDetachJournal();
            this.ActiveTransaction = null;
            this.stateAtBegin = null;
        }
    }

    /// <summary>
    /// Flushes the underlying stream durably.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask FlushDurableAsync(CancellationToken cancellationToken)
        => await pager.FlushAsync(toDisk: true, cancellationToken).ConfigureAwait(false);

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
        ownedMaps.Restore(state.OwnedMaps);
        constraints.Restore(state.Constraints);
    }

    /// <summary>
    /// Drops the writer's cached state after commit replay failed partway. The
    /// file then holds some of the transaction's pages and not others, so
    /// neither the state from before the transaction nor the transaction's own
    /// is known to match it: the catalog is re-scanned on next use, the insert
    /// hint and the owned-map policy's refusals are forgotten, and its
    /// writable set goes back to the TDEFs known before the transaction. The
    /// constraint registry keeps the transaction's entries, so AutoNumber
    /// counters never move back over values that may have reached the file.
    /// </summary>
    /// <param name="state">The state captured when the transaction began.</param>
    private void DiscardCachesAfterFailedReplay(WriterState? state)
    {
        catalog.Invalidate();
        if (state is null)
        {
            return;
        }

        dataPages.RestoreState(new DataPageInserterState(HintTDefPage: -1, HintPageNumber: -1));
        ownedMaps.Restore(state.OwnedMaps with { RefusedTdefs = [] });
    }

    /// <summary>The writer's in-memory state that a transaction can change.</summary>
    /// <param name="DataPages">The insert-page hint.</param>
    /// <param name="OwnedMaps">The owned-map policy's decisions.</param>
    /// <param name="Constraints">The constraint registry's contents.</param>
    private sealed record WriterState(DataPageInserterState DataPages, OwnedMapPolicyState OwnedMaps, ConstraintRegistrySnapshot Constraints);
}
