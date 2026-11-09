namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetRelationshipBehaviorTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NoAction_RejectsAnOrphanInsert(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<NoActionContext> options = new DbContextOptionsBuilder<NoActionContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using NoActionContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        _ = context.Add(new Child { Id = 1, ParentId = 99 });

        _ = await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NoAction_RejectsDeletingAParentWhoseChildIsNotLoaded(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<NoActionContext> options = new DbContextOptionsBuilder<NoActionContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (NoActionContext seed = new(options))
        {
            Assert.True(await seed.Database.EnsureCreatedAsync(ct));
            seed.AddRange(new Parent { Id = 1 }, new Child { Id = 1, ParentId = 1 });
            Assert.Equal(2, await seed.SaveChangesAsync(ct));
        }

        await using NoActionContext context = new(options);
        _ = context.Remove(new Parent { Id = 1 });

        _ = await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));
        Assert.Equal(1, await context.Set<Parent>().CountAsync(ct));
        Assert.Equal(1, await context.Set<Child>().CountAsync(ct));
    }

    private sealed class NoActionContext(DbContextOptions<NoActionContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Parent>().Property(parent => parent.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Child>().Property(child => child.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Child>().HasOne<Parent>().WithMany()
                .HasForeignKey(child => child.ParentId).OnDelete(DeleteBehavior.NoAction);
        }
    }

    private sealed class Parent
    {
        public int Id { get; set; }
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }
    }
}
