namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks failed statements leave their explicit transaction usable.</summary>
public sealed class TransactionSavepointTests
{
    /// <summary>A large failed insert restores earlier journal images and its append boundary.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BudgetFailure_RestoresPriorInsert(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, MaxTransactionPageBudget = 16 };
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Label", typeof(string), maxLength: 200)], TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1, "Before"], TestContext.Current.CancellationToken);
        int count = transaction.JournaledPageCount;
        List<object?[]> rows = [.. Enumerable.Range(2, 2000).Select(id => new object?[] { id, new string('x', 200) })];
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(() => writer.InsertRowsAsync("Items", rows, TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(JetErrorCode.JournalBudgetExceeded, error.ErrorCode);
        Assert.Equal(count, transaction.JournaledPageCount);
        Assert.False(transaction.IsRolledBack);
        await writer.InsertRowAsync("Items", [2, "After"], TestContext.Current.CancellationToken);
        await transaction.CommitAsync(TestContext.Current.CancellationToken);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    /// <summary>A producer failure rolls back rows already yielded by that statement.</summary>
    [Fact]
    public async Task ProducerFailure_RestoresInsertedRows()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<InvalidDataException>(() => writer.InsertRowsAsync("Items", FailingRows(), TestContext.Current.CancellationToken).AsTask());
        await writer.InsertRowAsync("Items", [2], TestContext.Current.CancellationToken);
        await transaction.CommitAsync(TestContext.Current.CancellationToken);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    /// <summary>Cancellation after yielding a row discards only that statement.</summary>
    [Fact]
    public async Task CancelledProducer_RestoresStatement()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writer.InsertRowsAsync("Items", CancelledRows(cancellation), cancellation.Token).AsTask());
        Assert.Equal(0, transaction.JournaledPageCount);
        await writer.InsertRowAsync("Items", [2], TestContext.Current.CancellationToken);
        await transaction.CommitAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>A callback cannot start another mutation, commit, rollback or disposal.</summary>
    /// <param name="operation">The nested mutation.</param>
    [Theory]
    [InlineData("Insert")]
    [InlineData("Commit")]
    [InlineData("Rollback")]
    [InlineData("Dispose")]
    public async Task ReentrantCallback_ThrowsTypedRefusal(string operation)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Task? nested = null;
        IEnumerable<object?[]> Rows()
        {
            nested = operation switch
            {
                "Commit" => transaction.CommitAsync(TestContext.Current.CancellationToken).AsTask(),
                "Rollback" => transaction.RollbackAsync(TestContext.Current.CancellationToken).AsTask(),
                "Dispose" => writer.DisposeAsync().AsTask(),
                _ => writer.InsertRowAsync("Items", [9], TestContext.Current.CancellationToken).AsTask(),
            };
            yield return [1];
        }

        _ = await writer.InsertRowsAsync("Items", Rows(), TestContext.Current.CancellationToken);
        Assert.NotNull(nested);
        JetOperationException error = await Assert.ThrowsAsync<JetOperationException>(() => nested);
        Assert.Equal(JetErrorCode.ReentrantWriterCall, error.ErrorCode);
        await transaction.RollbackAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>A failure after freeing a table restores both allocation metadata and cached free pages.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FailedDrop_RestoresAllocator(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await harness.BeginTransactionAsync(TestContext.Current.CancellationToken);
        SortedSet<long> before = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<InvalidDataException>(() => harness.Services.Transactions.RunAutoCommitAsync(
            async token =>
            {
                await harness.Services.Schema.DropTableAsync("Items", token);
                throw new InvalidDataException("Failure after freeing the table's pages.");
                },
            TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(before, await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, TestContext.Current.CancellationToken));
        await harness.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], [], TestContext.Current.CancellationToken);
        await harness.InsertRowAsync("Other", [2], TestContext.Current.CancellationToken);
        Assert.Empty(await PageAudit.FindMultiplyOwnedDataPagesAsync(harness, TestContext.Current.CancellationToken));
        Assert.Empty(await PageAudit.FindUnlinkedReservedPagesAsync(harness.Database, harness.Services.PageAllocator, TestContext.Current.CancellationToken));
        await transaction.CommitAsync(TestContext.Current.CancellationToken);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
        Assert.Equal(1, await reader.GetRealRowCountAsync("Other", TestContext.Current.CancellationToken));
    }

    /// <summary>A failed schema statement removes its created table and can create it again.</summary>
    [Fact]
    public async Task FailedCreate_RemovesCatalogAndConstraintState()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Seed", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await harness.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<InvalidDataException>(() => harness.Services.Transactions.RunAutoCommitAsync(
            async token =>
            {
                await harness.Services.Schema.CreateDeclaredTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], [], token);
                throw new InvalidDataException("Failure after creating the table.");
                },
            TestContext.Current.CancellationToken).AsTask());
        Assert.Null(await harness.Services.Catalog.GetCatalogEntryAsync("Items", TestContext.Current.CancellationToken));
        await harness.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], [], TestContext.Current.CancellationToken);
        await harness.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        await transaction.CommitAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>A failed cascading delete restores parent, child and free-page state.</summary>
    [Fact]
    public async Task FailedCascade_RestoresEveryTable()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Parent", [new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("PK_Parent", "Id") { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("Child", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Parent", [1], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Child", [1, 1], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Child", "Parent", "Id", "Child", "ParentId") { CascadeDeletes = true }, TestContext.Current.CancellationToken);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await harness.BeginTransactionAsync(TestContext.Current.CancellationToken);
        SortedSet<long> before = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<InvalidDataException>(() => harness.Services.Transactions.RunAutoCommitAsync(
            async token =>
            {
                _ = await harness.Services.Data.DeleteRowsAsync("Parent", RowCriteria.All(), token);
                throw new InvalidDataException("Failure after the cascade freed pages.");
                },
            TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(before, await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, TestContext.Current.CancellationToken));
        await harness.InsertRowAsync("Parent", [2], TestContext.Current.CancellationToken);
        await harness.InsertRowAsync("Child", [2, 2], TestContext.Current.CancellationToken);
        await transaction.CommitAsync(TestContext.Current.CancellationToken);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Parent", TestContext.Current.CancellationToken));
        Assert.Equal(2, await reader.GetRealRowCountAsync("Child", TestContext.Current.CancellationToken));
    }

    /// <summary>A child execution context can write once the callback that spawned it has ended.</summary>
    [Fact]
    public async Task DelayedCallbackContext_CanWriteAfterStatement()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        var proceed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? delayed = null;
        IEnumerable<object?[]> Rows()
        {
            delayed = Task.Run(
                async () =>
                {
                    _ = await proceed.Task;
                    await writer.InsertRowAsync("Items", [2], TestContext.Current.CancellationToken);
                },
                TestContext.Current.CancellationToken);
            yield return [1];
        }

        _ = await writer.InsertRowsAsync("Items", Rows(), TestContext.Current.CancellationToken);
        proceed.SetResult(true);
        Assert.NotNull(delayed);
        await delayed;
        await transaction.CommitAsync(TestContext.Current.CancellationToken);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }
    /// <summary>Commit waits for a whole statement, including its suspended callback.</summary>
    [Fact]
    public async Task Commit_WaitsForActiveStatement()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await harness.BeginTransactionAsync(TestContext.Current.CancellationToken);
        var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task statement = harness.Services.Transactions.RunAutoCommitAsync(
            async token =>
            {
                await harness.Services.Data.InsertRowAsync("Items", [1], token);
                entered.SetResult(true);
                _ = await proceed.Task.WaitAsync(token);
                await harness.Services.Data.InsertRowAsync("Items", [2], token);
                },
            TestContext.Current.CancellationToken).AsTask();
        _ = await entered.Task.WaitAsync(TestContext.Current.CancellationToken);
        Task commit = transaction.CommitAsync(TestContext.Current.CancellationToken).AsTask();
        try
        {
            Assert.False(commit.IsCompleted);
        }
        finally
        {
            proceed.SetResult(true);
        }

        await Task.WhenAll(statement, commit);
        Assert.True(transaction.IsCommitted);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    /// <summary>Queued disposals run teardown once and transaction disposal remains idempotent.</summary>
    [Fact]
    public async Task ConcurrentDisposal_RunsTeardownOnce()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await harness.BeginTransactionAsync(TestContext.Current.CancellationToken);
        var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task statement = harness.Services.Transactions.RunAutoCommitAsync(
            async token =>
            {
                entered.SetResult(true);
                _ = await proceed.Task.WaitAsync(token);
                },
            TestContext.Current.CancellationToken).AsTask();
        _ = await entered.Task.WaitAsync(TestContext.Current.CancellationToken);
        int teardowns = 0;
        async ValueTask TeardownAsync()
        {
            teardowns++;
            await harness.Services.Transactions.DisposeActiveTransactionAsync();
            await harness.Database.DisposeAsync();
        }

        Task first = harness.Services.Transactions.RunDisposalAsync(TeardownAsync).AsTask();
        Task second = harness.Services.Transactions.RunDisposalAsync(TeardownAsync).AsTask();
        Task transactionDispose = transaction.DisposeAsync().AsTask();
        try
        {
            Assert.False(first.IsCompleted);
            Assert.False(second.IsCompleted);
            Assert.False(transactionDispose.IsCompleted);
        }
        finally
        {
            proceed.SetResult(true);
        }

        await Task.WhenAll(statement, first, second, transactionDispose);
        Assert.Equal(1, teardowns);
        Assert.True(transaction.IsRolledBack);
    }
    private static IEnumerable<object?[]> CancelledRows(CancellationTokenSource cancellation)
    {
        yield return [1];
        cancellation.Cancel();
        yield return [2];
    }
    private static IEnumerable<object?[]> FailingRows()
    {
        yield return [1];
        throw new InvalidDataException("Producer failed after its first row.");
    }
}
