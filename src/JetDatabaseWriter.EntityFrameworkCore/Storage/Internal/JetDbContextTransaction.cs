namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Represents an active <see cref="IDbContextTransaction"/> backed by a <see cref="JetDatabaseWriter.JetTransaction"/>.
/// </summary>
public sealed class JetDbContextTransaction : IDbContextTransaction
{
    private readonly JetTransactionManager manager;
    private bool completed;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetDbContextTransaction"/> class.
    /// </summary>
    /// <param name="manager">The owning <see cref="JetTransactionManager"/>.</param>
    /// <param name="transactionId">The unique identifier for this transaction.</param>
    public JetDbContextTransaction(JetTransactionManager manager, Guid transactionId)
    {
        ArgumentNullException.ThrowIfNull(manager);

        this.manager = manager;
        this.TransactionId = transactionId;
    }

    /// <inheritdoc/>
    public Guid TransactionId { get; }

    /// <inheritdoc/>
    public void Commit()
        => throw new NotSupportedException(
            "Synchronous Commit is not supported by JetDatabaseWriter.EntityFrameworkCore. Use CommitAsync instead.");

    /// <inheritdoc/>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (this.completed)
        {
            throw new InvalidOperationException("This transaction has already been completed.");
        }

        this.completed = true;
        await this.manager.CommitTransactionInternalAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public void Rollback()
        => throw new NotSupportedException(
            "Synchronous Rollback is not supported by JetDatabaseWriter.EntityFrameworkCore. Use RollbackAsync instead.");

    /// <inheritdoc/>
    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (this.completed)
        {
            throw new InvalidOperationException("This transaction has already been completed.");
        }

        this.completed = true;
        await this.manager.RollbackTransactionInternalAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        this.completed = true;
        this.manager.ClearCurrentTransaction();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (!this.completed)
        {
            this.completed = true;
            await this.manager.DisposeActiveTransactionAsync().ConfigureAwait(false);
        }
    }
}
