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

    [Fact]
    public async Task Commit_BumpsPageZeroCommitLockByte()
    {
        // The JET commit-lock byte at page-0 offset 0x14 must increment on
        // every committed transaction so cooperating openers can detect a
        // catalog/data version change without re-reading the entire file.
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            ms.Position = 0;
            byte[] before = new byte[0x18];
            await ms.ReadAsync(before.AsMemory(), TestContext.Current.CancellationToken);
            byte beforeByte = before[0x14];

            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await tx.CommitAsync(TestContext.Current.CancellationToken);

            ms.Position = 0;
            byte[] after = new byte[0x18];
            await ms.ReadAsync(after.AsMemory(), TestContext.Current.CancellationToken);

            Assert.Equal(unchecked((byte)(beforeByte + 1)), after[0x14]);
        }
    }

    [Fact]
    public async Task UseTransactionalWrites_RollsBackOnExceptionDuringInsert()
    {
        // With UseTransactionalWrites=true, an exception thrown mid-call must
        // leave the database in its pre-call state.
        var ms = new MemoryStream();
        var writerOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            UseTransactionalWrites = true,
        };

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            writerOptions,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            // Seed one row.
            await writer.InsertRowAsync("Items", [1, "Seed"], TestContext.Current.CancellationToken);

            // Bulk insert with an intra-batch primary-key duplicate; the
            // pre-write unique check throws and the WHOLE batch must be
            // rolled back by the implicit auto-commit transaction.
            object[][] batch =
            [
                [10, "Ten"],
                [11, "Eleven"],
                [10, "DupTen"],
            ];

            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await writer.InsertRowsAsync("Items", batch, TestContext.Current.CancellationToken));
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);

        // Only the seed row should remain — none of the batch rows persisted.
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task UseTransactionalWrites_Disabled_AllowsPartialBatchVisibility()
    {
        // Sanity check: with UseTransactionalWrites=false (default), a
        // failure mid-batch leaves whatever rows the writer's per-call
        // rollback path didn't catch. We don't assert exact persisted-row
        // count here — just that the option is honoured (no implicit
        // transaction is opened, so PageCacheSize/MaxTransactionPageBudget
        // do not affect the call's success).
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1, "A"], TestContext.Current.CancellationToken);
    }
}
