namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetSaveChangesAtomicityTests
{
    [Fact]
    public async Task SaveChanges_Refuses_An_Unprotected_Batch_At_The_Nesting_Limit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<AtomicContext> options = new DbContextOptionsBuilder<AtomicContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (AtomicContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            _ = context.Add(new UniqueRow { Id = 1, Code = "taken" });
            Assert.Equal(1, await context.SaveChangesAsync(ct));
            context.ChangeTracker.Clear();
            await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(ct);
            await transaction.CreateSavepointAsync("one", ct);
            await transaction.CreateSavepointAsync("two", ct);
            await transaction.CreateSavepointAsync("three", ct);
            await transaction.CreateSavepointAsync("four", ct);
            FirstRow first = new() { Id = 1 };
            UniqueRow duplicate = new() { Id = 2, Code = "taken" };
            context.AddRange(first, duplicate);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.SaveChangesAsync(ct));
            Assert.Contains("nesting", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Same(transaction, context.Database.CurrentTransaction);
            Assert.Equal(EntityState.Added, context.Entry(first).State);
            Assert.Equal(0, await context.Set<FirstRow>().CountAsync(ct));
            await transaction.ReleaseSavepointAsync("four", ct);
            _ = await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));
            Assert.Equal(0, await context.Set<FirstRow>().CountAsync(ct));
            context.ChangeTracker.Clear();
            await transaction.CommitAsync(ct);
        }

        await using AtomicContext verify = new(options);
        Assert.Equal(0, await verify.Set<FirstRow>().CountAsync(ct));
        Assert.Equal(1, await verify.Set<UniqueRow>().CountAsync(ct));
    }

    private sealed class AtomicContext(DbContextOptions<AtomicContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<FirstRow>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<UniqueRow>(entity =>
            {
                _ = entity.Property(row => row.Id).ValueGeneratedNever();
                _ = entity.Property(row => row.Code).HasMaxLength(40);
                _ = entity.HasIndex(row => row.Code).IsUnique();
            });
        }
    }

    private sealed class FirstRow
    {
        public int Id { get; set; }
    }

    private sealed class UniqueRow
    {
        public int Id { get; set; }

        public string Code { get; set; } = string.Empty;
    }
}
