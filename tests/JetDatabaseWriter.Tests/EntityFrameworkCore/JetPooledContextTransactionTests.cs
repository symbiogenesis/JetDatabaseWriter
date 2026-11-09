namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

public sealed class JetPooledContextTransactionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Returning_Context_To_Pool_Rolls_Back_And_Invalidates_Transaction(bool asynchronously)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ServiceCollection services = new();
        _ = services.AddPooledDbContextFactory<PooledContext>(options => options.UseJetDatabaseWriter(stream, leaveOpen: true), poolSize: 1);
        await using ServiceProvider provider = services.BuildServiceProvider();
        IDbContextFactory<PooledContext> factory = provider.GetRequiredService<IDbContextFactory<PooledContext>>();
        PooledContext first = await factory.CreateDbContextAsync(ct);
        IDbContextTransaction stale;
        try
        {
            Assert.True(await first.Database.EnsureCreatedAsync(ct));
            stale = await first.Database.BeginTransactionAsync(ct);
            first.Add(new Item { Id = 1 });
            _ = await first.SaveChangesAsync(ct);
        }
        finally
        {
            if (asynchronously)
            {
                await first.DisposeAsync();
            }
            else
            {
#pragma warning disable CA1849 // Exercise the synchronous context-pool reset boundary.
                first.Dispose();
#pragma warning restore CA1849
            }
        }

        await using (stale)
        await using (PooledContext second = await factory.CreateDbContextAsync(ct))
        {
            Assert.Same(first, second);
            Assert.False(await second.Set<Item>().AsNoTracking().AnyAsync(ct));
            await using IDbContextTransaction current = await second.Database.BeginTransactionAsync(ct);
            second.Add(new Item { Id = 2 });
            _ = await second.SaveChangesAsync(ct);

            await Assert.ThrowsAsync<InvalidOperationException>(() => stale.CreateSavepointAsync("stale", ct));
            await Assert.ThrowsAsync<InvalidOperationException>(() => stale.RollbackToSavepointAsync("stale", ct));
            await Assert.ThrowsAsync<InvalidOperationException>(() => stale.ReleaseSavepointAsync("stale", ct));
            await Assert.ThrowsAsync<InvalidOperationException>(() => stale.CommitAsync(ct));
            await Assert.ThrowsAsync<InvalidOperationException>(() => stale.RollbackAsync(ct));
            await stale.DisposeAsync();
            Assert.Same(current, second.Database.CurrentTransaction);
            await current.CommitAsync(ct);
        }

        await using PooledContext verification = await factory.CreateDbContextAsync(ct);
        Item item = await verification.Set<Item>().AsNoTracking().SingleAsync(ct);
        Assert.Equal(2, item.Id);
    }

#pragma warning disable CA1812 // EF creates this context through its pooled factory.
    private sealed class PooledContext(DbContextOptions<PooledContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
        }
    }
#pragma warning restore CA1812

    private sealed class Item
    {
        public int Id { get; set; }
    }
}
