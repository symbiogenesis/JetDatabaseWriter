namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Tests.ComplexColumns;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Nested scopes share one journal and resolve in reverse begin order.</summary>
public sealed class NestedTransactionTests
{
    private static readonly int[] FirstTwoIds = [1, 2];
    private static readonly int[] RetryIds = [3];
    private static readonly int[] DepthIds = [1, 2, 3, 4, 5, 8];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A child commit remains buffered and is undone by its parent.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ChildCommit_OuterRollback_LeavesExactOriginalBytes(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, format);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], Ct);
        byte[] original = stream.ToArray();
        await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [1], Ct);
        await using JetTransaction inner = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [2], Ct);
        await inner.CommitAsync(Ct);
        Assert.True(inner.IsCommitted);
        Assert.False(outer.IsCommitted);
        Assert.Equal(original, stream.ToArray());
        await outer.RollbackAsync(Ct);
        Assert.Equal(original, stream.ToArray());
        Assert.Empty(await ReadIdsAsync(stream));
    }

    /// <summary>Child rollback restores schema, allocation hints and counters while retaining parent changes.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ChildRollback_RestoresWriterState_AndParentCanCommit(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateAsync(stream, format))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true }, new("Text", typeof(string), 100)], Ct);
            await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
            await writer.InsertRowAsync("Items", [DBNull.Value, "outer"], Ct);
            await using (JetTransaction inner = await writer.BeginTransactionAsync(Ct))
            {
                await writer.CreateTableAsync("Temporary", [new("Id", typeof(int))], Ct);
                List<object?[]> rows = [.. Enumerable.Range(0, 150).Select(_ => new object?[] { DBNull.Value, new string('x', 100) })];
                await writer.InsertRowsAsync("Items", rows, Ct);
                await inner.RollbackAsync(Ct);
            }

            await writer.CreateTableAsync("Temporary", [new("Value", typeof(string), 40)], Ct);
            await writer.InsertRowAsync("Temporary", ["survives"], Ct);
            await writer.InsertRowAsync("Items", [DBNull.Value, "after"], Ct);
            await outer.CommitAsync(Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        Assert.Equal(FirstTwoIds, await ReadIdsAsync(reader));
        Assert.Equal(1, await reader.GetRealRowCountAsync("Temporary", Ct));
        Assert.Equal(2, await reader.GetRealRowCountAsync("Items", Ct));
    }

    /// <summary>An ancestor cannot resolve while a child remains open; disposal rolls back each child.</summary>
    [Fact]
    public async Task ActiveChild_RejectsAncestorResolution_AndDisposeRestoresParent()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, DatabaseFormat.AceAccdb);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [1], Ct);
        await using (JetTransaction middle = await writer.BeginTransactionAsync(Ct))
        {
            await writer.InsertRowAsync("Items", [2], Ct);
            await using JetTransaction inner = await writer.BeginTransactionAsync(Ct);
            await writer.InsertRowAsync("Items", [3], Ct);
            JetOperationException commit = await Assert.ThrowsAsync<JetOperationException>(() => outer.CommitAsync(Ct).AsTask());
            JetOperationException rollback = await Assert.ThrowsAsync<JetOperationException>(() => middle.RollbackAsync(Ct).AsTask());
            Assert.Equal(JetErrorCode.TransactionNotActive, commit.ErrorCode);
            Assert.Equal(JetErrorCode.TransactionNotActive, rollback.ErrorCode);
            JetOperationException dispose = await Assert.ThrowsAsync<JetOperationException>(() => outer.DisposeAsync().AsTask());
            Assert.Equal(JetErrorCode.TransactionNotActive, dispose.ErrorCode);
            Assert.False(outer.IsRolledBack);
            await inner.CommitAsync(Ct);
        }

        Assert.Equal(0, await writer.DeleteRowsAsync("Items", "Id", 2, Ct));
        await Assert.ThrowsAsync<JetConstraintException>(() => writer.InsertRowAsync("Items", [1], Ct).AsTask());
        await writer.InsertRowAsync("Items", [4], Ct);
        await outer.CommitAsync(Ct);
    }

    /// <summary>The outer commit still restores custom-stream bytes after child release.</summary>
    /// <param name="flush">Whether the fault occurs on flush rather than a partial page write.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CustomStream_OuterCommitFailure_UndoesReleasedChildren(bool flush)
    {
        await using var stream = new WriteFaultStream();
        await using AccessWriter writer = await CreateAsync(stream, DatabaseFormat.AceAccdb);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], Ct);
        byte[] original = stream.ToArray();
        await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [1], Ct);
        await using JetTransaction inner = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [2], Ct);
        await inner.CommitAsync(Ct);
        if (flush)
        {
            stream.FailOnFlush(1);
        }
        else
        {
            stream.FailDuringWrite(1);
        }

        await Assert.ThrowsAsync<IOException>(() => outer.CommitAsync(Ct).AsTask());
        Assert.True(outer.IsRolledBack);
        Assert.Equal(original, stream.ToArray());
        await writer.InsertRowAsync("Items", [3], Ct);
        Assert.Equal(RetryIds, await ReadIdsAsync(stream));
    }

    /// <summary>Writer disposal unwinds every unresolved child without replay.</summary>
    [Fact]
    public async Task WriterDispose_UnwindsAllNestedScopes()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, DatabaseFormat.AceAccdb);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], Ct);
        byte[] original = stream.ToArray();
        JetTransaction outer = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [1], Ct);
        JetTransaction inner = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [2], Ct);
        await writer.DisposeAsync();
        Assert.True(outer.IsRolledBack);
        Assert.True(inner.IsRolledBack);
        Assert.Equal(original, stream.ToArray());
        await outer.DisposeAsync();
        await inner.DisposeAsync();
    }

    /// <summary>The begin and mutation depth limits preserve the journal and leave open scopes usable.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SevenLevels_RefusedEighthBegin_PreservesJournalAndScopes(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, format);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        byte[] original = stream.ToArray();
        var scopes = new List<JetTransaction>();
        try
        {
            for (int level = 1; level <= 7; level++)
            {
                scopes.Add(await writer.BeginTransactionAsync(Ct));
                if (level <= 5)
                {
                    await writer.InsertRowAsync("Items", [level], Ct);
                }
            }

            int pages = scopes[0].JournaledPageCount;
            JetOperationException limit = await Assert.ThrowsAsync<JetOperationException>(() => writer.BeginTransactionAsync(Ct).AsTask());
            Assert.Equal(JetErrorCode.TransactionNestingLimit, limit.ErrorCode);
            Assert.Equal(pages, scopes[0].JournaledPageCount);
            Assert.Equal(original, stream.ToArray());
            for (int index = 6; index >= 5; index--)
            {
                JetOperationException mutation = await Assert.ThrowsAsync<JetOperationException>(() => writer.InsertRowAsync("Items", [8], Ct).AsTask());
                Assert.Equal(JetErrorCode.TransactionNestingLimit, mutation.ErrorCode);
                Assert.Equal(pages, scopes[0].JournaledPageCount);
                Assert.Equal(original, stream.ToArray());
                await scopes[index].RollbackAsync(Ct);
            }

            await Assert.ThrowsAsync<JetConstraintException>(() => writer.InsertRowAsync("Items", [5], Ct).AsTask());
            await writer.InsertRowAsync("Items", [8], Ct);
            for (int index = 4; index >= 0; index--)
            {
                await scopes[index].CommitAsync(Ct);
            }
        }
        finally
        {
            for (int index = scopes.Count - 1; index >= 0; index--)
            {
                await scopes[index].DisposeAsync();
            }
        }

        Assert.Equal(DepthIds, await ReadIdsAsync(stream));
    }

    /// <summary>Cancelled child commit rolls back that child; cancelled begin or rollback preserves the current scope.</summary>
    [Fact]
    public async Task ChildCancellation_PreservesParentAndRestoresChildState()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream, DatabaseFormat.AceAccdb);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsAutoIncrement = true }], Ct);
        await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [DBNull.Value], Ct);
        await using JetTransaction child = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [DBNull.Value], Ct);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writer.BeginTransactionAsync(cancelled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => child.RollbackAsync(cancelled.Token).AsTask());
        Assert.False(child.IsRolledBack);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => child.CommitAsync(cancelled.Token).AsTask());
        Assert.True(child.IsRolledBack);
        Assert.False(outer.IsRolledBack);
        await writer.InsertRowAsync("Items", [DBNull.Value], Ct);
        await outer.CommitAsync(Ct);
        Assert.Equal(FirstTwoIds, await ReadIdsAsync(stream));
    }

    /// <summary>File-backed children do not create a recovery decision; only outer commit persists the combined changes.</summary>
    [Fact]
    public async Task FileBacked_OuterCommit_PersistsReleasedChildAndCleansRecoveryJournal()
    {
        string directory = Path.Combine(Path.GetTempPath(), "NestedTransactionTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "nested.accdb");
        try
        {
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.AceAccdb, cancellationToken: Ct))
            {
                await writer.CreateTableAsync("Items", [new("Id", typeof(int))], Ct);
                await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
                await writer.InsertRowAsync("Items", [1], Ct);
                await using JetTransaction child = await writer.BeginTransactionAsync(Ct);
                await writer.InsertRowAsync("Items", [2], Ct);
                await child.CommitAsync(Ct);
                Assert.False(File.Exists(path + ".jdw-journal"));
                await outer.CommitAsync(Ct);
                Assert.True(outer.IsCommitted);
                Assert.False(File.Exists(path + ".jdw-journal"));
            }

            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
            Assert.Equal(FirstTwoIds, await ReadIdsAsync(reader));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>A failed statement within a child restores its budget and counters without losing the parent.</summary>
    [Fact]
    public async Task ChildStatementBudgetFailure_RestoresFrame_AndBothScopesRemainUsable()
    {
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, MaxTransactionPageBudget = 16 };
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, options, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsAutoIncrement = true }, new("Text", typeof(string), 200)], Ct);
        await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [DBNull.Value, "parent"], Ct);
        int parentPages = outer.JournaledPageCount;
        await using JetTransaction child = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [DBNull.Value, "child"], Ct);
        int childPages = child.JournaledPageCount;
        List<object?[]> rows = [.. Enumerable.Range(0, 2000).Select(_ => new object?[] { DBNull.Value, new string('x', 200) })];
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(() => writer.InsertRowsAsync("Items", rows, Ct).AsTask());
        Assert.Equal(JetErrorCode.JournalBudgetExceeded, error.ErrorCode);
        Assert.Equal(childPages, child.JournaledPageCount);
        Assert.False(child.IsRolledBack);
        await writer.InsertRowAsync("Items", [DBNull.Value, "after failed statement"], Ct);
        await child.RollbackAsync(Ct);
        Assert.Equal(parentPages, outer.JournaledPageCount);
        await writer.InsertRowAsync("Items", [DBNull.Value, "after child rollback"], Ct);
        await outer.CommitAsync(Ct);
        Assert.Equal(FirstTwoIds, await ReadIdsAsync(stream));
    }

    /// <summary>Per-parent complex reference counters return to the parent snapshot after child rollback.</summary>
    [Fact]
    public async Task ChildRollback_RestoresComplexReferenceCounter()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateAsync(stream, DatabaseFormat.AceAccdb))
        {
            await writer.CreateTableAsync("Docs", [new("Id", typeof(int)), new("Files", typeof(byte[])) { IsAttachment = true }], Ct);
            await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value], Ct);
            await using (JetTransaction child = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowsAsync("Docs", [[2, DBNull.Value], [3, DBNull.Value]], Ct);
                await child.RollbackAsync(Ct);
            }

            await writer.InsertRowAsync("Docs", [4, DBNull.Value], Ct);
            await outer.CommitAsync(Ct);
        }

        ComplexColumnTestSupport.RawTable docs = await ComplexColumnTestSupport.ReadRawTableAsync(stream, "Docs");
        Assert.Equal(
            ["1|1", "4|2"],
            docs.Rows.Select(row => $"{row[0]}|{ComplexColumnTestSupport.Slot(docs, row, "Files")}").Order(StringComparer.Ordinal));
        Assert.Equal(2, docs.ComplexAutoNumber);
    }

    private static ValueTask<AccessWriter> CreateAsync(MemoryStream stream, DatabaseFormat format) =>
        AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: Ct);

    private static async Task<int[]> ReadIdsAsync(MemoryStream stream)
    {
        await using var copy = new MemoryStream(stream.ToArray());
        await using AccessReader reader = await AccessReader.OpenAsync(copy, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        return await ReadIdsAsync(reader);
    }

    private static async Task<int[]> ReadIdsAsync(AccessReader reader)
    {
        var ids = new List<int>();
        await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
        {
            ids.Add((int)row[0]);
        }

        ids.Sort();
        return [.. ids];
    }
}
