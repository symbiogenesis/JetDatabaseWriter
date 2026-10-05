namespace JetDatabaseWriter.Transactions;

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Exceptions;
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
[SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable", Justification = "The mutation semaphore never creates a wait handle and remains alive for queued calls, including concurrent disposal; disposing it would race those callers.")]
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
    private readonly SemaphoreSlim mutationGate = new(1, 1);
    private readonly AsyncLocal<MutationContext?> mutationActive = new();

    /// <summary>
    /// The writer's in-memory state when <see cref="ActiveTransaction"/> began,
    /// put back if it rolls back. Set and cleared together with it.
    /// </summary>
    private WriterState? stateAtBegin;

    /// <summary>Gets the active explicit transaction, or <see langword="null"/> when none is active.</summary>
    internal JetTransaction? ActiveTransaction { get; private set; }

    /// <summary>Serializes public mutations and rejects callback re-entry.</summary>
    /// <param name="work">The operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous completion.</returns>
    internal ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
        => this.RunSerializedAsync(() => this.RunAutoCommitCoreAsync(work, cancellationToken), cancellationToken);

    /// <summary>Serializes a result-producing mutation.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="work">The operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous completion.</returns>
    internal ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
        => this.RunSerializedAsync(() => this.RunAutoCommitCoreAsync(work, cancellationToken), cancellationToken);

    /// <summary>Serializes disposal with outstanding mutations.</summary>
    /// <param name="work">The complete teardown.</param>
    /// <returns>The asynchronous completion.</returns>
    internal ValueTask RunDisposalAsync(Func<ValueTask> work)
        => this.RunSerializedAsync(() => pager.IsDisposed ? default : work(), CancellationToken.None);

    /// <summary>Serializes maintenance that cannot use a journal.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="work">The operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous completion.</returns>
    internal ValueTask<TResult> RunMutationAsync<TResult>(Func<ValueTask<TResult>> work, CancellationToken cancellationToken)
        => this.RunSerializedAsync(work, cancellationToken);

    /// <summary>Begins a serialized explicit transaction.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous completion.</returns>
    internal ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken)
        => this.RunSerializedAsync(() => this.BeginTransactionCoreAsync(cancellationToken), cancellationToken);

    /// <summary>Commits under the mutation gate.</summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous completion.</returns>
    internal ValueTask CommitTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
        => this.RunSerializedAsync(() => this.CommitTransactionCoreAsync(transaction, cancellationToken), CancellationToken.None);

    /// <summary>Rolls back under the mutation gate.</summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous completion.</returns>
    internal ValueTask RollbackTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
        => this.RunSerializedAsync(() => this.RollbackTransactionCoreAsync(transaction, cancellationToken), cancellationToken);

    /// <summary>Begins call write-back for creation and maintenance.</summary>
    /// <returns>The reference-counted scope.</returns>
    internal WriteScope BeginWriteScope() => pager.BeginWriteScope();

    /// <summary>Flushes pending writes before a container rewrap.</summary>
    /// <returns>The completion.</returns>
    internal ValueTask FlushPendingWritesAsync() => pager.FlushPendingWritesAsync();

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
            await this.RollbackTransactionCoreAsync(this.ActiveTransaction, CancellationToken.None).ConfigureAwait(false);
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
    /// Begins an explicit page-buffered transaction against the owning writer.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ObjectDisposedException">Thrown when the writer has been disposed.</exception>
    /// <exception cref="JetOperationException">Another transaction is already active on the writer.</exception>
    private async ValueTask<JetTransaction> BeginTransactionCoreAsync(CancellationToken cancellationToken)
    {
        pager.ThrowIfDisposed();

        cancellationToken.ThrowIfCancellationRequested();

        using Pager.JournalGate gate = await pager.EnterJournalGateAsync(cancellationToken).ConfigureAwait(false);
        if (this.ActiveTransaction is not null)
        {
            throw JetErrors.Operation(
                JetErrorCode.TransactionAlreadyActive,
                "A transaction is already active on this writer. Only one concurrent transaction per AccessWriter is supported.");
        }

        var journal = new PagerTransaction(gate.PhysicalLengthBytes, format.PageSize, options.MaxTransactionPageBudget);
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
    private async ValueTask RunAutoCommitCoreAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        if (this.ActiveTransaction is { } active)
        {
            await this.RunInSavepointAsync(active, work, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!options.UseTransactionalWrites || pager.IsDisposed)
        {
            await using WriteScope scope = pager.BeginWriteScope();
            await work(cancellationToken).ConfigureAwait(false);
            return;
        }

        JetTransaction tx = await this.BeginTransactionCoreAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await work(cancellationToken).ConfigureAwait(false);
            await this.CommitTransactionCoreAsync(tx, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            try
            {
                if (!tx.IsTerminated)
                {
                    await this.RollbackTransactionCoreAsync(tx, CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (JetOperationException ex) when (ex.ErrorCode is JetErrorCode.TransactionEnded or JetErrorCode.TransactionNotActive)
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
    private async ValueTask<TResult> RunAutoCommitCoreAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
    {
        if (this.ActiveTransaction is { } active)
        {
            return await this.RunInSavepointAsync(active, work, cancellationToken).ConfigureAwait(false);
        }

        if (!options.UseTransactionalWrites || pager.IsDisposed)
        {
            await using WriteScope scope = pager.BeginWriteScope();
            return await work(cancellationToken).ConfigureAwait(false);
        }

        JetTransaction tx = await this.BeginTransactionCoreAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            TResult? result = await work(cancellationToken).ConfigureAwait(false);
            await this.CommitTransactionCoreAsync(tx, cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch
        {
            try
            {
                if (!tx.IsTerminated)
                {
                    await this.RollbackTransactionCoreAsync(tx, CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (JetOperationException ex) when (ex.ErrorCode is JetErrorCode.TransactionEnded or JetErrorCode.TransactionNotActive)
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
    /// <exception cref="JetOperationException">The transaction is terminated or is not active on this writer.</exception>
    private async ValueTask CommitTransactionCoreAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        if (transaction.IsTerminated)
        {
            throw JetErrors.Operation(JetErrorCode.TransactionEnded, JetTransaction.TerminatedMessage);
        }

        pager.ThrowIfDisposed();

        PagerTransaction journal;
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
                throw JetErrors.Operation(JetErrorCode.TransactionEnded, JetTransaction.TerminatedMessage);
            }

            if (!ReferenceEquals(this.ActiveTransaction, transaction))
            {
                throw JetErrors.Operation(JetErrorCode.TransactionNotActive, "The transaction is not active on this writer.");
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
            await pager.CommitAsync(journal, () => replayStarted = true, cancellationToken).ConfigureAwait(false);
            transaction.MarkCommitted();
        }
        catch
        {
            pager.InvalidateAll();
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
    /// <exception cref="JetOperationException">The transaction is terminated or is not active on this writer.</exception>
    private async ValueTask RollbackTransactionCoreAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        if (transaction.IsTerminated)
        {
            throw JetErrors.Operation(JetErrorCode.TransactionEnded, JetTransaction.TerminatedMessage);
        }

        cancellationToken.ThrowIfCancellationRequested();

        using Pager.JournalGate gate = await pager.EnterJournalGateAsync(cancellationToken).ConfigureAwait(false);
        if (transaction.IsTerminated)
        {
            throw JetErrors.Operation(JetErrorCode.TransactionEnded, JetTransaction.TerminatedMessage);
        }

        if (!ReferenceEquals(this.ActiveTransaction, transaction))
        {
            throw JetErrors.Operation(JetErrorCode.TransactionNotActive, "The transaction is not active on this writer.");
        }

        gate.Detach();
        this.ActiveTransaction = null;
        this.RestoreWriterState(this.stateAtBegin);
        this.stateAtBegin = null;
        transaction.MarkRolledBack();
    }

    /// <summary>Serializes a mutation and detects active callback re-entry.</summary>
    /// <param name="work">The operation.</param>
    /// <param name="cancellationToken">The token used while awaiting the gate.</param>
    /// <returns>The asynchronous completion.</returns>
    /// <exception cref="JetOperationException">A mutation was called from an active mutation callback.</exception>
    private async ValueTask RunSerializedAsync(Func<ValueTask> work, CancellationToken cancellationToken)
    {
        if (this.mutationActive.Value is { IsActive: true })
        {
            throw JetErrors.Operation(JetErrorCode.ReentrantWriterCall, "A writer mutation cannot be called from another writer mutation's callback.");
        }

        await this.mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        var context = new MutationContext();
        this.mutationActive.Value = context;
        try
        {
            await work().ConfigureAwait(false);
        }
        finally
        {
            context.End();
            this.mutationActive.Value = null;
            _ = this.mutationGate.Release();
        }
    }

    private async ValueTask<TResult> RunSerializedAsync<TResult>(Func<ValueTask<TResult>> work, CancellationToken cancellationToken)
    {
        TResult result = default!;
        Func<ValueTask> invoke = async () => result = await work().ConfigureAwait(false);
        await this.RunSerializedAsync(invoke, cancellationToken).ConfigureAwait(false);
        return result;
    }

    private async ValueTask RunInSavepointAsync(JetTransaction transaction, Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
        => await this.RunInSavepointAsync<object?>(
            transaction,
            async token =>
            {
                await work(token).ConfigureAwait(false);
                return null;
            },
            cancellationToken).ConfigureAwait(false);

    private async ValueTask<TResult> RunInSavepointAsync<TResult>(JetTransaction transaction, Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
    {
        WriterState state = new(dataPages.CaptureState(), ownedMaps.Capture(), constraints.CaptureSnapshot());
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(cancellationToken).ConfigureAwait(false))
        {
            transaction.Journal.BeginSavepoint();
        }

        try
        {
            TResult result = await work(cancellationToken).ConfigureAwait(false);
            using Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None).ConfigureAwait(false);
            transaction.Journal.ReleaseSavepoint();
            return result;
        }
        catch
        {
            using Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None).ConfigureAwait(false);
            transaction.Journal.RollbackSavepoint();
            pager.InvalidateAll();
            this.RestoreWriterState(state);
            throw;
        }
    }

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

    private sealed class MutationContext
    {
        private int active = 1;

        internal bool IsActive => Volatile.Read(ref this.active) != 0;

        internal void End() => Volatile.Write(ref this.active, 0);
    }

    /// <summary>The writer's in-memory state that a transaction can change.</summary>
    /// <param name="DataPages">The insert-page hint.</param>
    /// <param name="OwnedMaps">The owned-map policy's decisions.</param>
    /// <param name="Constraints">The constraint registry's contents.</param>
    private sealed record WriterState(DataPageInserterState DataPages, OwnedMapPolicyState OwnedMaps, ConstraintRegistrySnapshot Constraints);
}
