namespace JetDatabaseWriter.Transactions;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;

/// <summary>
/// Manages the explicit page-buffered transaction lifecycle for an
/// <see cref="AccessWriter"/>: begin, auto-commit wrapping, commit replay,
/// rollback, and dispose-time teardown. Owns the active transaction; the
/// page journal it attaches lives on <see cref="DatabaseFile.ActiveJournal"/>
/// because every page read and write consults it.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="options">The writer options; supplies the auto-commit switch and journal page budget.</param>
/// <param name="byteRangeLock">The cooperative JET byte-range lock used for the commit-lock sentinel.</param>
internal sealed class TransactionLifecycle(DatabaseFile db, AccessWriterOptions options, JetByteRangeLock byteRangeLock)
{
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

            // Detach the journal first so the page-write loop below routes
            // straight to disk.
            db.ActiveJournal = null;
            this.ActiveTransaction = null;
        }
        finally
        {
            _ = db.IoGate.Release();
        }

        long? commitLockOffset = await byteRangeLock.AcquireCommitLockOffsetAsync(
            isAccdb: db.Format == Enums.DatabaseFormat.AceAccdb,
            cancellationToken).ConfigureAwait(false);

        try
        {
            foreach (KeyValuePair<long, byte[]> entry in journal.EnumerateInOrder())
            {
                cancellationToken.ThrowIfCancellationRequested();
                await db.WritePageAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
            }

            await this.BumpCommitLockByteAsync(cancellationToken).ConfigureAwait(false);
            await this.FlushDurableAsync(cancellationToken).ConfigureAwait(false);
            transaction.MarkCommitted();
        }
        catch
        {
            transaction.MarkRolledBack();
            throw;
        }
        finally
        {
            byteRangeLock.ReleaseCommitLock(commitLockOffset);
        }
    }

    /// <summary>
    /// Rolls back the supplied <paramref name="transaction"/>: discards the
    /// in-memory journal without touching the database file.
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
        }
    }

    /// <summary>
    /// Increments the page-0 "commit lock byte" at header offset <c>0x14</c>.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask BumpCommitLockByteAsync(CancellationToken cancellationToken)
    {
        byte[] page0 = await db.ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
        try
        {
            page0[0x14] = unchecked((byte)(page0[0x14] + 1));
            await db.WritePageAsync(0, page0, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            DatabaseFile.ReturnPage(page0);
        }
    }

    /// <summary>
    /// Flushes the underlying stream durably.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask FlushDurableAsync(CancellationToken cancellationToken)
        => await db.FlushDatabaseStreamAsync(flushToDisk: true, cancellationToken).ConfigureAwait(false);
}
