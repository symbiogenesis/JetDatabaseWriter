namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetUniqueSaveOrderingTests
{
    private static readonly string[] HandoffCodes = ["freed", "TAKEN"];
    private static readonly int[] ChainNumbers = [20, 30, 10];
    private static readonly string[] ExchangedCodes = ["second", "first", "third"];

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_ReleasesUniqueValuesBeforeReplacementInserts(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Item original = new() { Id = 1, Code = "taken" };
        _ = context.Add(original);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        original.Code = "freed";
        _ = context.Add(new Item { Id = 2, Code = "TAKEN" });

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(HandoffCodes, await context.Set<Item>().OrderBy(row => row.Id).Select(row => row.Code).ToArrayAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_InterleavesNewParentsUniqueReleasesAndReplacementInserts(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Item original = new() { Id = 1, Code = "taken" };
        _ = context.Add(original);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Item prerequisite = new() { Id = 3, Code = "parent" };
        original.Code = "freed";
        original.ParentId = prerequisite.Id;
        context.AddRange(new Item { Id = 2, Code = "taken" }, prerequisite);

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(3, (await context.Set<Item>().SingleAsync(row => row.Id == 1, ct)).ParentId);
        Assert.Equal(3, await context.Set<Item>().CountAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_ReleasesCompositeUniqueValuesThroughModifiedChains(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        CompositeItem first = new() { Id = 1, Partition = 1, Number = 10 };
        CompositeItem second = new() { Id = 2, Partition = 1, Number = 20 };
        context.AddRange(first, second);
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        first.Number = 20;
        second.Number = 30;
        _ = context.Add(new CompositeItem { Id = 3, Partition = 1, Number = 10 });

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(ChainNumbers, await context.Set<CompositeItem>().OrderBy(row => row.Id).Select(row => row.Number).ToArrayAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_KeepsCyclicValueExchangesInOneUpdateBatch(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Item first = new() { Id = 1, Code = "first", Alias = "taken" };
        Item second = new() { Id = 2, Code = "second", Alias = "other" };
        context.AddRange(first, second);
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        first.Code = "second";
        second.Code = "first";
        first.Alias = "freed";
        _ = context.Add(new Item { Id = 3, Code = "third", Alias = "taken" });

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(ExchangedCodes, await context.Set<Item>().OrderBy(row => row.Id).Select(row => row.Code).ToArrayAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_RefusesImpossibleUniqueAndForeignKeyCyclesWithoutMutation(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Item original = new() { Id = 1, Code = "taken" };
        _ = context.Add(original);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        original.Code = "freed";
        original.ParentId = 2;
        _ = context.Add(new Item { Id = 2, Code = "taken" });

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Item remaining = await context.Set<Item>().SingleAsync(ct);
        Assert.Equal("taken", remaining.Code);
        Assert.Null(remaining.ParentId);
    }

    private static OrderingContext CreateContext(Stream stream, DatabaseFormat format)
        => new(new DbContextOptionsBuilder<OrderingContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);

    private sealed class OrderingContext(DbContextOptions<OrderingContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Item>(entity =>
            {
                _ = entity.Property(row => row.Id).ValueGeneratedNever();
                _ = entity.Property(row => row.Code).HasMaxLength(32);
                _ = entity.Property(row => row.Alias).HasMaxLength(32);
                _ = entity.HasIndex(row => row.Code).IsUnique();
                _ = entity.HasIndex(row => row.Alias).IsUnique();
                _ = entity.HasOne<Item>().WithMany().HasForeignKey(row => row.ParentId)
                    .OnDelete(DeleteBehavior.ClientNoAction);
            });
            _ = modelBuilder.Entity<CompositeItem>(entity =>
            {
                _ = entity.Property(row => row.Id).ValueGeneratedNever();
                _ = entity.HasIndex(row => new { row.Partition, row.Number }).IsUnique();
            });
        }
    }

    private sealed class Item
    {
        public int Id { get; set; }

        public string Code { get; set; } = string.Empty;

        public string? Alias { get; set; }

        public int? ParentId { get; set; }
    }

    private sealed class CompositeItem
    {
        public int Id { get; set; }

        public int Partition { get; set; }

        public int Number { get; set; }
    }
}
