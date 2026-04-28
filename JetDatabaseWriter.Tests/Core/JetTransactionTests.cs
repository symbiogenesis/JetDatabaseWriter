namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for explicit page-buffered transactions (Phase 3 of the
/// concurrency-and-transactions plan). Exercises the
/// <see cref="AccessWriter.BeginTransactionAsync"/> /
/// <see cref="JetTransaction.CommitAsync"/> /
/// <see cref="JetTransaction.RollbackAsync"/> surface end-to-end through a
/// round-trip with <see cref="AccessReader"/>.
/// </summary>
public sealed class JetTransactionTests
{
    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static List<ColumnDefinition> ItemsSchema() =>
    [
        new("Id", typeof(int)),
        new("Label", typeof(string), maxLength: 50),
    ];

    [Fact]
    public async Task BeginTransaction_ReturnsActiveTransaction()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(tx);
        Assert.False(tx.IsCommitted);
        Assert.False(tx.IsRolledBack);
    }

    [Fact]
    public async Task BeginTransaction_TwiceWithoutCommit_Throws()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using JetTransaction first = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(first);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.BeginTransactionAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Commit_PersistsBufferedInserts()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);

            Assert.True(tx.JournaledPageCount > 0);

            await tx.CommitAsync(TestContext.Current.CancellationToken);
            Assert.True(tx.IsCommitted);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Rollback_DiscardsBufferedInserts()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);

            await tx.RollbackAsync(TestContext.Current.CancellationToken);
            Assert.True(tx.IsRolledBack);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Dispose_WithoutCommit_RollsBackImplicitly()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            // Begin tx, do work, dispose without committing.
            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            try
            {
                await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);
            }
            finally
            {
                await tx.DisposeAsync();
            }

            Assert.True(tx.IsRolledBack);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task ReadInsideTransaction_SeesUncommittedWrites()
    {
        // Inserts inside a transaction must be visible to subsequent reads
        // performed by the same writer (via the journal-shadow read path),
        // otherwise a multi-row insert that allocates a new data page would
        // immediately fail to find that page on the next AppendRow call.
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        // 100 rows comfortably forces multiple page mutations and at least one
        // new appended page; the writer's own row append path round-trips
        // through ReadPageAsync between writes.
        var rows = new List<object[]>();
        for (int i = 1; i <= 100; i++)
        {
            rows.Add([i, "Row" + i]);
        }

        int inserted = await writer.InsertRowsAsync("Items", rows, TestContext.Current.CancellationToken);
        Assert.Equal(100, inserted);

        await tx.CommitAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Commit_AfterRollback_Throws()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await tx.RollbackAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tx.CommitAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task JournalBudgetExceeded_ThrowsJetLimitationException()
    {
        var ms = new MemoryStream();
        var writerOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,

            // Tiny budget: the very first table-creation pass already mutates
            // multiple pages, so the budget will trip immediately.
            MaxTransactionPageBudget = 1,
        };

        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            writerOptions,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken));
    }
}
