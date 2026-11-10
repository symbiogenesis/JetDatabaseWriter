namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

#pragma warning disable EF1001 // Hold the provider writer lease to exercise cancellation while completion waits for connection entry.
public sealed class JetTransactionCancellationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Canceled_Completion_Preserves_The_Transaction_For_Retry(bool commit, bool whileWaiting)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CancellationContext> options = new DbContextOptionsBuilder<CancellationContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true)
            .Options;
        await using CancellationContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(ct);
        await transaction.CreateSavepointAsync("retained", ct);
        context.Add(new Item { Id = 1 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));

        IJetClientConnection connection = context.GetService<IJetClientConnection>();
        (AccessWriter Writer, bool OwnedByCaller)? rental = whileWaiting ? await connection.RentWriterAsync(ct) : null;
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try
        {
            if (!whileWaiting)
            {
                await canceled.CancelAsync();
            }

            Task completion = commit ? transaction.CommitAsync(canceled.Token) : transaction.RollbackAsync(canceled.Token);
            if (whileWaiting)
            {
                Assert.False(completion.IsCompleted);
                await canceled.CancelAsync();
            }

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => completion);
            Assert.Same(transaction, context.Database.CurrentTransaction);
        }
        finally
        {
            if (rental is { } held)
            {
                await connection.ReleaseWriterAsync(held.Writer, held.OwnedByCaller);
            }
        }

        await transaction.ReleaseSavepointAsync("retained", ct);
        if (commit)
        {
            await transaction.CommitAsync(ct);
        }
        else
        {
            await transaction.RollbackAsync(ct);
        }

        Assert.Null(context.Database.CurrentTransaction);
        Assert.Equal(commit ? 1 : 0, await context.Set<Item>().AsNoTracking().CountAsync(ct));
        await using IDbContextTransaction next = await context.Database.BeginTransactionAsync(ct);
        await transaction.DisposeAsync();
        Assert.Same(next, context.Database.CurrentTransaction);
        await next.RollbackAsync(ct);
    }

    private sealed class CancellationContext(DbContextOptions<CancellationContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
    }

    private sealed class Item
    {
        public int Id { get; set; }
    }
}
#pragma warning restore EF1001
