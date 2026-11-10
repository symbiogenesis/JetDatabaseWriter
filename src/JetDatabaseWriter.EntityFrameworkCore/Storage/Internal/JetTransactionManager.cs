namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Manages explicit database transactions for the <see cref="JetDatabaseWriter"/> Entity Framework Core provider.
/// </summary>
public sealed class JetTransactionManager : IDbContextTransactionManager, IDisposable, IAsyncDisposable
{
    private readonly IJetClientConnection connection;
    private JetDbContextTransaction? currentTransaction;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetTransactionManager"/> class.
    /// </summary>
    /// <param name="connection">The client connection for the current context scope.</param>
    public JetTransactionManager(IJetClientConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);

        this.connection = connection;
    }

    /// <inheritdoc/>
    public IDbContextTransaction? CurrentTransaction => this.currentTransaction;

    /// <inheritdoc/>
    public IDbContextTransaction BeginTransaction()
        => throw new NotSupportedException(
            "Synchronous BeginTransaction is not supported by JetDatabaseWriter.EntityFrameworkCore. Use BeginTransactionAsync instead.");

    /// <inheritdoc/>
    public async Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (this.currentTransaction is not null)
        {
            throw new InvalidOperationException("A transaction is already in progress on this context.");
        }

        _ = await this.connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        this.currentTransaction = new JetDbContextTransaction(this, Guid.NewGuid());
        return this.currentTransaction;
    }

    /// <inheritdoc/>
    public void CommitTransaction()
        => throw new NotSupportedException(
            "Synchronous CommitTransaction is not supported by JetDatabaseWriter.EntityFrameworkCore. Use CommitTransactionAsync instead.");

    /// <inheritdoc/>
    public Task CommitTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (this.currentTransaction is null)
        {
            throw new InvalidOperationException("No transaction is currently in progress.");
        }

        return this.currentTransaction.CommitAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public void RollbackTransaction()
        => throw new NotSupportedException(
            "Synchronous RollbackTransaction is not supported by JetDatabaseWriter.EntityFrameworkCore. Use RollbackTransactionAsync instead.");

    /// <inheritdoc/>
    public Task RollbackTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (this.currentTransaction is null)
        {
            throw new InvalidOperationException("No transaction is currently in progress.");
        }

        return this.currentTransaction.RollbackAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public void ResetState()
        => this.Dispose();

    /// <inheritdoc/>
    public Task ResetStateAsync(CancellationToken cancellationToken = default)
        => this.DisposeAsync().AsTask();

    /// <inheritdoc/>
    public void Dispose()
    {
        this.currentTransaction?.Dispose();
        this.currentTransaction = null;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (this.currentTransaction is not null)
        {
            await this.currentTransaction.DisposeAsync().ConfigureAwait(false);
            this.currentTransaction = null;
        }
    }

    internal async Task CommitTransactionInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            await this.connection.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (this.connection.ActiveTransaction is null)
            {
                this.currentTransaction = null;
            }
        }
    }

    internal async Task RollbackTransactionInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            await this.connection.RollbackTransactionAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (this.connection.ActiveTransaction is null)
            {
                this.currentTransaction = null;
            }
        }
    }

    internal async ValueTask DisposeActiveTransactionAsync()
    {
        this.currentTransaction = null;
        await this.connection.DisposeTransactionAsync().ConfigureAwait(false);
    }

    internal Task CreateSavepointInternalAsync(string name, CancellationToken cancellationToken)
        => this.connection.CreateSavepointAsync(name, cancellationToken).AsTask();

    internal Task RollbackToSavepointInternalAsync(string name, CancellationToken cancellationToken)
        => this.connection.RollbackToSavepointAsync(name, cancellationToken).AsTask();

    internal Task ReleaseSavepointInternalAsync(string name, CancellationToken cancellationToken)
        => this.connection.ReleaseSavepointAsync(name, cancellationToken).AsTask();

    internal void ClearCurrentTransaction()
        => this.currentTransaction = null;
}
