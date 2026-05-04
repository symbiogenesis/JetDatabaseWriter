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
/// rollback, and dispose-time teardown.
/// </summary>
internal sealed class TransactionLifecycle(AccessWriter writer)
{
    /// <summary>
    /// Begins an explicit page-buffered transaction against the owning writer.
    /// </summary>
    internal async ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken)
    {
        if (writer._disposed)
        {
            throw new ObjectDisposedException(nameof(AccessWriter));
        }

        cancellationToken.ThrowIfCancellationRequested();

        await writer.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (writer.ActiveTransaction is not null)
            {
                throw new InvalidOperationException(
                    "A transaction is already active on this writer. Only one concurrent transaction per AccessWriter is supported.");
            }

            long baseLength = writer._stream.Length;
            var journal = new PageJournal(baseLength, writer.PageSize, writer.Options.MaxTransactionPageBudget);
            var tx = new JetTransaction(writer, journal);
            writer.ActiveJournal = journal;
            writer.ActiveTransaction = tx;
            return tx;
        }
        finally
        {
            _ = writer.IoGate.Release();
        }
    }

    /// <summary>
    /// If <see cref="AccessWriterOptions.UseTransactionalWrites"/> is enabled
    /// and no explicit transaction is currently active, wraps
    /// <paramref name="work"/> in a private <see cref="JetTransaction"/> so a
    /// crash mid-call leaves the database in its pre-call state.
    /// </summary>
    internal async ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        if (!writer.Options.UseTransactionalWrites || writer.ActiveTransaction is not null || writer._disposed)
        {
            await work(cancellationToken).ConfigureAwait(false);
            return;
        }

        JetTransaction tx = await BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
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
    internal async ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
    {
        if (!writer.Options.UseTransactionalWrites || writer.ActiveTransaction is not null || writer._disposed)
        {
            return await work(cancellationToken).ConfigureAwait(false);
        }

        JetTransaction tx = await BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            TResult result = await work(cancellationToken).ConfigureAwait(false);
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
    /// journal from the writer and replays each buffered page through the
    /// normal page-write pipeline.
    /// </summary>
    internal async ValueTask CommitTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        if (writer._disposed)
        {
            throw new ObjectDisposedException(nameof(AccessWriter));
        }

        PageJournal journal;
        await writer.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (transaction.IsTerminated)
            {
                throw new InvalidOperationException("The transaction has already been committed or rolled back.");
            }

            if (!ReferenceEquals(writer.ActiveTransaction, transaction))
            {
                throw new InvalidOperationException("The transaction is not active on this writer.");
            }

            journal = transaction.Journal;

            // Detach the journal first so the page-write loop below routes
            // straight to disk.
            writer.ActiveJournal = null;
            writer.ActiveTransaction = null;
        }
        finally
        {
            _ = writer.IoGate.Release();
        }

        using IDisposable commitLock = await writer.ByteRangeLock.AcquireCommitLockAsync(
            isAccdb: writer.DatabaseFormat == Enums.DatabaseFormat.AceAccdb, cancellationToken).ConfigureAwait(false);

        try
        {
            foreach (KeyValuePair<long, byte[]> entry in journal.EnumerateInOrder())
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.WritePageAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
            }

            await BumpCommitLockByteAsync(cancellationToken).ConfigureAwait(false);
            await FlushDurableAsync(cancellationToken).ConfigureAwait(false);
            transaction.MarkCommitted();
        }
        catch
        {
            transaction.MarkRolledBack();
            throw;
        }
    }

    /// <summary>
    /// Rolls back the supplied <paramref name="transaction"/>: discards the
    /// in-memory journal without touching the database file.
    /// </summary>
    internal async ValueTask RollbackTransactionAsync(JetTransaction transaction, CancellationToken cancellationToken)
    {
        Guard.NotNull(transaction, nameof(transaction));

        cancellationToken.ThrowIfCancellationRequested();

        await writer.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (transaction.IsTerminated)
            {
                throw new InvalidOperationException("The transaction has already been committed or rolled back.");
            }

            if (!ReferenceEquals(writer.ActiveTransaction, transaction))
            {
                throw new InvalidOperationException("The transaction is not active on this writer.");
            }

            writer.ActiveJournal = null;
            writer.ActiveTransaction = null;
            transaction.MarkRolledBack();
        }
        finally
        {
            _ = writer.IoGate.Release();
        }
    }

    /// <summary>
    /// Increments the page-0 "commit lock byte" at header offset <c>0x14</c>.
    /// </summary>
    private async ValueTask BumpCommitLockByteAsync(CancellationToken cancellationToken)
    {
        byte[] page0 = await writer.ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
        try
        {
            page0[0x14] = unchecked((byte)(page0[0x14] + 1));
            await writer.WritePageAsync(0, page0, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            AccessBase.ReturnPage(page0);
        }
    }

    /// <summary>
    /// Flushes the underlying stream durably.
    /// </summary>
    private async ValueTask FlushDurableAsync(CancellationToken cancellationToken)
    {
        await writer.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (writer._stream is FileStream fs)
            {
#pragma warning disable CA1849
                fs.Flush(flushToDisk: true);
#pragma warning restore CA1849
            }
            else
            {
                await writer._stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = writer.IoGate.Release();
        }
    }
}
