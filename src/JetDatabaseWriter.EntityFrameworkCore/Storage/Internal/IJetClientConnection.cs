namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Manages exclusive access to <see cref="AccessReader"/> and <see cref="AccessWriter"/> for a <see cref="Microsoft.EntityFrameworkCore.DbContext"/> scope.
/// </summary>
public interface IJetClientConnection : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets the absolute database file path resolved when the connection is constructed, or <see langword="null"/> when backed by a stream.
    /// </summary>
    public string? DatabasePath { get; }

    /// <summary>
    /// Gets the configured database format.
    /// </summary>
    public DatabaseFormat Format { get; }

    /// <summary>
    /// Gets the currently active <see cref="AccessWriter"/> when an explicit transaction is open.
    /// </summary>
    public AccessWriter? ActiveWriter { get; }

    /// <summary>
    /// Gets the currently active <see cref="JetTransaction"/> when an explicit transaction is open.
    /// </summary>
    public JetTransaction? ActiveTransaction { get; }

    /// <summary>
    /// Returns an open <see cref="AccessReader"/> for read operations outside an explicit transaction.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The open <see cref="AccessReader"/>.</returns>
    /// <remarks>The returned reader has no operation lease. Advanced callers must isolate its subsequent use from peer operations on the same stream.</remarks>
    public ValueTask<AccessReader> GetReaderAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks for user tables while holding the connection's operation lease through the catalog read.
    /// </summary>
    /// <param name="cancellationToken">Cancels lease acquisition and the catalog read.</param>
    /// <returns>Whether the database contains a user table.</returns>
    public ValueTask<bool> HasUserTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the cached <see cref="AccessReader"/> if one is open.
    /// </summary>
    /// <returns>A task representing the asynchronous close operation.</returns>
    public ValueTask CloseReaderAsync();

    /// <summary>
    /// Rents an <see cref="AccessWriter"/> for write operations. If an explicit transaction is active, returns <see cref="ActiveWriter"/>.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A tuple containing the <see cref="AccessWriter"/> and a flag indicating whether the caller owns its disposal via <see cref="ReleaseWriterAsync"/>.</returns>
    public ValueTask<(AccessWriter Writer, bool OwnedByCaller)> RentWriterAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases an <see cref="AccessWriter"/> previously obtained from <see cref="RentWriterAsync"/>.
    /// </summary>
    /// <param name="writer">The writer to release.</param>
    /// <param name="ownedByCaller">Whether the caller owns the writer's lifetime.</param>
    /// <returns>A task representing the asynchronous release operation.</returns>
    public ValueTask ReleaseWriterAsync(AccessWriter writer, bool ownedByCaller);

    /// <summary>
    /// Creates a new database using <see cref="AccessWriter.CreateDatabaseAsync(string, DatabaseFormat, AccessWriterOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The newly created <see cref="AccessWriter"/>.</returns>
    public ValueTask<AccessWriter> CreateDatabaseWriterAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Begins an explicit transaction on the database.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The started <see cref="JetTransaction"/>.</returns>
    public ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the currently active transaction and closes its writer.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous commit operation.</returns>
    public ValueTask CommitTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back the currently active transaction and closes its writer.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous rollback operation.</returns>
    public ValueTask RollbackTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Disposes the active transaction and writer if still open.
    /// </summary>
    /// <returns>A task representing the asynchronous cleanup operation.</returns>
    public ValueTask DisposeTransactionAsync();

    /// <summary>
    /// Creates a named savepoint within the currently active transaction using a nested <see cref="JetTransaction"/> scope.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous savepoint creation.</returns>
    public ValueTask CreateSavepointAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back all changes made since the specified savepoint was created while keeping the savepoint active.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous savepoint rollback.</returns>
    public ValueTask RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the specified savepoint by merging its nested <see cref="JetTransaction"/> scope into its parent transaction.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous savepoint release.</returns>
    public ValueTask ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether the underlying database exists and contains a valid JET header.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns><see langword="true"/> if the database exists and can be opened; otherwise, <see langword="false"/>.</returns>
    public ValueTask<bool> CanConnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the underlying database file or clears the underlying stream if it exists.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns><see langword="true"/> if the database existed and was deleted; otherwise, <see langword="false"/>.</returns>
    public ValueTask<bool> DeleteDatabaseAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads rows for <paramref name="entityType"/> as <see cref="ValueBuffer"/> instances, using B-tree index seeks when <paramref name="seekConditions"/> match a table index.
    /// </summary>
    /// <param name="entityType">The EF Core entity type to read.</param>
    /// <param name="seekConditions">Optional index seek conditions extracted from the query.</param>
    /// <param name="parameters">The current query parameter values.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The loaded <see cref="ValueBuffer"/> rows.</returns>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads rows for <paramref name="entityType"/> as <see cref="ValueBuffer"/> instances, decoding only <paramref name="requiredProperties"/> when specified.
    /// </summary>
    /// <param name="entityType">The EF Core entity type to read.</param>
    /// <param name="seekConditions">Optional index seek conditions extracted from the query.</param>
    /// <param name="parameters">The current query parameter values.</param>
    /// <param name="requiredProperties">Optional set of properties required by the query projection.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The loaded <see cref="ValueBuffer"/> rows.</returns>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows for <paramref name="entityType"/> as <see cref="ValueBuffer"/> instances, decoding only <paramref name="requiredProperties"/> when specified.
    /// </summary>
    /// <param name="entityType">The EF Core entity type to read.</param>
    /// <param name="seekConditions">Optional index seek conditions extracted from the query.</param>
    /// <param name="parameters">The current query parameter values.</param>
    /// <param name="requiredProperties">Optional set of properties required by the query projection.</param>
    /// <param name="maxRows">Optional maximum number of rows to read before short-circuiting the scan.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The loaded <see cref="ValueBuffer"/> rows.</returns>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        int? maxRows,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> matching rows for <paramref name="entityType"/> as <see cref="ValueBuffer"/> instances,
    /// optionally using an index-ordered scan for <paramref name="orderedProperties"/> and evaluating <paramref name="rowFilter"/> before counting toward <paramref name="maxRows"/>.
    /// </summary>
    /// <param name="entityType">The EF Core entity type to read.</param>
    /// <param name="seekConditions">Optional index seek conditions extracted from the query.</param>
    /// <param name="parameters">The current query parameter values.</param>
    /// <param name="requiredProperties">Optional set of properties required by the query projection.</param>
    /// <param name="maxRows">Optional maximum number of matching rows to read before short-circuiting the scan.</param>
    /// <param name="orderedProperties">Optional leading ascending sort properties that may be satisfied by an index scan.</param>
    /// <param name="rowFilter">Optional row filter evaluated on each decoded <see cref="ValueBuffer"/> before counting toward <paramref name="maxRows"/>.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The loaded <see cref="ValueBuffer"/> rows.</returns>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        int? maxRows,
        IReadOnlyList<JetOrdering>? orderedProperties,
        Func<ValueBuffer, bool>? rowFilter,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams matching rows in the requested order, buffering for a CLR sort when no equivalent index order is available.
    /// The enumerator owns its read lease until disposal.
    /// </summary>
    /// <param name="entityType">The EF entity type.</param>
    /// <param name="seekConditions">Optional index seek conditions.</param>
    /// <param name="parameters">The runtime query parameters.</param>
    /// <param name="requiredProperties">The properties required by the query.</param>
    /// <param name="maxRows">The maximum matching rows when the requested order can be supplied.</param>
    /// <param name="orderedProperties">Optional leading sort properties and directions.</param>
    /// <param name="rowFilter">The residual filter evaluated before counting matching rows.</param>
    /// <param name="cancellationToken">Cancels scan acquisition and enumeration.</param>
    /// <returns>The lazy row sequence.</returns>
    public IAsyncEnumerable<ValueBuffer> StreamTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        int? maxRows,
        IReadOnlyList<JetOrdering>? orderedProperties,
        Func<ValueBuffer, bool>? rowFilter,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Visits matching table rows using a reusable value buffer. The visitor must not retain
    /// the buffer or its backing array after returning; returning false stops the scan.
    /// </summary>
    /// <param name="entityType">The EF entity type.</param>
    /// <param name="seekConditions">Optional index seek conditions.</param>
    /// <param name="parameters">The runtime query parameters.</param>
    /// <param name="requiredProperties">The properties required by the query.</param>
    /// <param name="visitor">The synchronous visitor, whose buffer is valid only during its call.</param>
    /// <param name="cancellationToken">Cancels scan acquisition and enumeration.</param>
    /// <returns>A task representing the scan.</returns>
    public ValueTask ScanTableRowsAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        Func<ValueBuffer, bool> visitor,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads rows for <paramref name="entityType"/> matching any of <paramref name="keyTuples"/> on <paramref name="keyProperties"/>,
    /// using B-tree index seeks when a covering index is available.
    /// </summary>
    /// <param name="entityType">The EF Core entity type to read.</param>
    /// <param name="keyProperties">The properties forming the lookup key.</param>
    /// <param name="keyTuples">The distinct non-null model key tuples to match.</param>
    /// <param name="requiredProperties">Optional set of properties required by the query.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The matching <see cref="ValueBuffer"/> rows.</returns>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersByKeysAsync(
        IEntityType entityType,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        IReadOnlySet<IProperty>? requiredProperties = null,
        CancellationToken cancellationToken = default);
}
