namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Connects the lifecycle specification's composed scope and generation transitions to production.</summary>
public sealed class WriterLifecycleConformanceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Released children keep their terminal identity while later rollback and failed statements invalidate derived state.</summary>
    [Fact]
    public async Task ReleasedChild_StatementFailure_AndRollback_PreserveScopeIdentity()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        byte[] original = stream.ToArray();
        await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [1], Ct);
        await using JetTransaction released = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [2], Ct);
        long beforeRelease = writer.Pager.InvalidationGeneration;
        await released.CommitAsync(Ct);
        Assert.Equal(beforeRelease, writer.Pager.InvalidationGeneration);
        await released.DisposeAsync();
        Assert.Equal(beforeRelease, writer.Pager.InvalidationGeneration);
        JetOperationException ended = await Assert.ThrowsAsync<JetOperationException>(() => released.RollbackAsync(Ct).AsTask());
        Assert.Equal(JetErrorCode.TransactionEnded, ended.ErrorCode);
        Assert.Equal(original, stream.ToArray());

        await using JetTransaction child = await writer.BeginTransactionAsync(Ct);
        long beforeFailure = writer.Pager.InvalidationGeneration;
        await Assert.ThrowsAsync<JetConstraintException>(() => writer.InsertRowAsync("Items", [2], Ct).AsTask());
        Assert.True(writer.Pager.InvalidationGeneration > beforeFailure);
        Assert.Same(child, writer.Services.Transactions.ActiveTransaction);
        Assert.False(child.IsRolledBack);
        await writer.InsertRowAsync("Items", [3], Ct);
        long beforeRollback = writer.Pager.InvalidationGeneration;
        await child.RollbackAsync(Ct);
        Assert.True(writer.Pager.InvalidationGeneration > beforeRollback);
        Assert.Same(outer, writer.Services.Transactions.ActiveTransaction);
        await writer.InsertRowAsync("Items", [3], Ct);
        await outer.RollbackAsync(Ct);
        Assert.True(released.IsCommitted);
        Assert.True(child.IsRolledBack);
        Assert.True(outer.IsRolledBack);
        Assert.Equal(original, stream.ToArray());
        Assert.Null(writer.Services.Transactions.ActiveTransaction);
        Assert.False(writer.Pager.IsJournalActive);
    }

    /// <summary>Teardown resolves all seven active tokens once and preserves the identity of an earlier released child.</summary>
    [Fact]
    public async Task WriterDisposal_UnwindsMaximumDepth_AndRepeatedDisposalIsInert()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateAsync(stream);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], Ct);
        byte[] original = stream.ToArray();
        var active = new List<JetTransaction> { await writer.BeginTransactionAsync(Ct) };
        await writer.InsertRowAsync("Items", [1], Ct);
        await using JetTransaction released = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [2], Ct);
        await released.CommitAsync(Ct);
        for (int depth = 2; depth <= 7; depth++)
        {
            active.Add(await writer.BeginTransactionAsync(Ct));
        }

        await writer.DisposeAsync();
        long disposedGeneration = writer.Pager.InvalidationGeneration;
        foreach (JetTransaction scope in active)
        {
            Assert.True(scope.IsRolledBack);
            await scope.DisposeAsync();
        }

        await released.DisposeAsync();
        await writer.DisposeAsync();
        Assert.Equal(disposedGeneration, writer.Pager.InvalidationGeneration);
        Assert.True(released.IsCommitted);
        Assert.False(released.IsRolledBack);
        Assert.True(writer.Pager.IsDisposed);
        Assert.False(writer.Pager.IsJournalActive);
        Assert.Null(writer.Services.Transactions.ActiveTransaction);
        Assert.Equal(original, stream.ToArray());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => writer.BeginTransactionAsync(Ct).AsTask());
    }

    /// <summary>Faulted writes refuse new mutations while current-generation reads remain possible and teardown only releases resources.</summary>
    [Fact]
    public async Task FailedUndo_AllowsCurrentGenerationRead_ButDisposalDoesNotWrite()
    {
        await using var stream = new WriteFaultStream();
        await using AccessWriter writer = await CreateAsync(stream);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], Ct);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        await writer.InsertRowAsync("Items", [1], Ct);
        stream.FailWritesFrom(1);
        await Assert.ThrowsAsync<AggregateException>(() => transaction.CommitAsync(Ct).AsTask());
        Assert.True(writer.Pager.IsFaulted);
        Assert.True(transaction.IsCommitFailed);
        Assert.False(transaction.IsCommitted);
        Assert.False(transaction.IsRolledBack);
        writer.Pager.InvalidateAll();
        long beforeRead = writer.Pager.Statistics.StoreReads;
        byte[] first = await writer.Pager.ReadPageCopyAsync(0, Ct);
        byte[] second = await writer.Pager.ReadPageCopyAsync(0, Ct);
        Assert.Equal(first, second);
        Assert.Equal(beforeRead + 1, writer.Pager.Statistics.StoreReads);
        Assert.Equal(1, writer.Pager.Statistics.CachedFrames);
        JetOperationException faulted = await Assert.ThrowsAsync<JetOperationException>(() => writer.BeginTransactionAsync(Ct).AsTask());
        Assert.Equal(JetErrorCode.WriterFaulted, faulted.ErrorCode);
        int beforeDispose = stream.WriteCount;
        await writer.DisposeAsync();
        await transaction.DisposeAsync();
        await writer.DisposeAsync();
        Assert.Equal(beforeDispose, stream.WriteCount);
    }

    private static ValueTask<AccessWriter> CreateAsync(MemoryStream stream) =>
        AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: Ct);
}
