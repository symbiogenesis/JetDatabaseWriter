namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>
/// Opens a database through the library's internal write layers — the
/// <see cref="DatabaseFile"/> and a <see cref="WriterServices"/> graph built
/// over it — for tests that drive a single writer service, inspect a writer
/// service's state, or patch pages directly through the writer's
/// <see cref="Pager"/>. Nothing goes through <see cref="AccessWriter"/>, so
/// the facade carries no test-only members. The
/// mutation helpers (<see cref="CreateTableAsync"/>, <see cref="InsertRowAsync"/>
/// and the rest) run their service call inside
/// <see cref="JetDatabaseWriter.Transactions.TransactionLifecycle.RunAutoCommitAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/>,
/// as the facade's methods do, so <see cref="AccessWriterOptions.UseTransactionalWrites"/>
/// wraps each in its own transaction. No lock-file slot is taken and
/// Agile-encrypted containers are not unwrapped.
/// </summary>
internal sealed class WriterHarness : IAsyncDisposable
{
    private WriterHarness(DatabaseFile database, Pager pager, WriterServices services)
    {
        this.Database = database;
        this.Pager = pager;
        this.Services = services;
    }

    /// <summary>Gets the open database file.</summary>
    public DatabaseFile Database { get; }

    /// <summary>Gets the database file's pager, which <see cref="Services"/> write pages through.</summary>
    public Pager Pager { get; }

    /// <summary>Gets the writer service graph built over <see cref="Database"/>.</summary>
    public WriterServices Services { get; }

    /// <summary>Opens <paramref name="path"/> for writing.</summary>
    /// <param name="path">The database file path.</param>
    /// <param name="options">Optional writer options.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static async ValueTask<WriterHarness> OpenAsync(string path, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
#pragma warning disable CA2000 // The stream overload owns the stream once the file is open (leaveOpen: false), and the catch disposes it when the open throws.
        FileStream stream = PageFile.OpenFileStream(path, FileAccess.ReadWrite, FileShare.Read, FileOptions.Asynchronous | FileOptions.RandomAccess);
#pragma warning restore CA2000 // The stream overload owns the stream once the file is open (leaveOpen: false), and the catch disposes it when the open throws.
        try
        {
            return await OpenAsync(stream, options, leaveOpen: false, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Opens the database held in <paramref name="stream"/> for writing.</summary>
    /// <param name="stream">A readable, writable, seekable stream holding the database bytes.</param>
    /// <param name="options">Optional writer options.</param>
    /// <param name="leaveOpen">Whether <paramref name="stream"/> stays open after the harness is disposed.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The returned harness owns the database file, and the catch disposes it when construction fails.")]
    public static async ValueTask<WriterHarness> OpenAsync(Stream stream, AccessWriterOptions? options = null, bool leaveOpen = true, CancellationToken cancellationToken = default)
    {
        options ??= new AccessWriterOptions { UseLockFile = false };
        options.Validate();
        string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
        byte[] header = await PageFile.ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);
        var database = DatabaseFile.ForWriter(stream, header, options.Password, path, leaveOpen, out Pager pager, options.PageCacheSize);
        try
        {
            pager.ByteRangeLock = options.CreateByteRangeLock(stream);
            var services = new WriterServices(database, pager, options, pager.ByteRangeLock);
            return new WriterHarness(database, pager, services);
        }
        catch
        {
            await database.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Begins an explicit transaction, as <see cref="AccessWriter.BeginTransactionAsync"/> does.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The transaction.</returns>
    public ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        => this.Services.Transactions.BeginTransactionAsync(cancellationToken);

    /// <summary>Creates a table with indexes, as <see cref="AccessWriter.CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, CancellationToken)"/> does.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="columns">The columns.</param>
    /// <param name="indexes">The indexes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the table exists.</returns>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken = default)
        => this.Services.Transactions.RunAutoCommitAsync(_ => this.Services.Schema.CreateDeclaredTableAsync(tableName, columns, indexes, cancellationToken), cancellationToken);

    /// <summary>Inserts one row, as <see cref="AccessWriter.InsertRowAsync(string, object?[], CancellationToken)"/> does.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="values">The column values in column order.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the row is written.</returns>
    public ValueTask InsertRowAsync(string tableName, object?[] values, CancellationToken cancellationToken = default)
        => this.Services.Transactions.RunAutoCommitAsync(_ => this.Services.Data.InsertRowAsync(tableName, values, cancellationToken), cancellationToken);

    /// <summary>Inserts rows, as <see cref="AccessWriter.InsertRowsAsync(string, IEnumerable{object?[]}, CancellationToken)"/> does.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rows">The rows, each in column order.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of rows inserted.</returns>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object?[]> rows, CancellationToken cancellationToken = default)
        => this.Services.Transactions.RunAutoCommitAsync(_ => this.Services.Data.InsertRowsAsync(tableName, rows, cancellationToken), cancellationToken);

    /// <summary>Updates the rows whose column equals a value, as <see cref="AccessWriter.UpdateRowsAsync(string, string, object?, IReadOnlyDictionary{string, object?}, CancellationToken)"/> does.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="predicateColumn">The column to match.</param>
    /// <param name="predicateValue">The value to match.</param>
    /// <param name="updatedValues">The new values by column name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of rows updated.</returns>
    public ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object?> updatedValues, CancellationToken cancellationToken = default)
        => this.Services.Transactions.RunAutoCommitAsync(
            _ => this.Services.Data.UpdateRowsAsync(tableName, RowCriteria.Where(predicateColumn, predicateValue), new RowValues(updatedValues), cancellationToken),
            cancellationToken);

    /// <summary>Deletes the rows whose column equals a value, as <see cref="AccessWriter.DeleteRowsAsync(string, string, object?, CancellationToken)"/> does.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="predicateColumn">The column to match.</param>
    /// <param name="predicateValue">The value to match.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of rows deleted.</returns>
    public ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default)
        => this.Services.Transactions.RunAutoCommitAsync(
            _ => this.Services.Data.DeleteRowsAsync(tableName, RowCriteria.Where(predicateColumn, predicateValue), cancellationToken),
            cancellationToken);

    /// <summary>Creates a relationship, as <see cref="AccessWriter.CreateRelationshipAsync"/> does.</summary>
    /// <param name="relationship">The relationship.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the relationship exists.</returns>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => this.Services.Transactions.RunAutoCommitAsync(_ => this.Services.Relationships.CreateRelationshipAsync(relationship, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await this.Services.Transactions.DisposeActiveTransactionAsync().ConfigureAwait(false);
        await this.Database.DisposeAsync().ConfigureAwait(false);
    }
}
