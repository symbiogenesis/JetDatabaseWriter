namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetOrderedStreamingTests
{
    private static readonly AsyncLocal<Action?> OnPredicate = new();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Indexed_Order_Yields_The_First_Match_Without_Evaluating_The_Table(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderedContext context = await CreateAsync(stream);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;
        int visited = 0;
        OnPredicate.Value = () => visited++;
        try
        {
            await using IAsyncEnumerator<int> rows = context.Items.AsNoTracking().Where(item => Matches(item.Id)).OrderBy(item => item.Id)
                .Select(item => item.Id).AsAsyncEnumerable().GetAsyncEnumerator(ct);
            Assert.True(await rows.MoveNextAsync());
            Assert.Equal(2, rows.Current);
            Assert.Equal(2, visited);
        }
        finally
        {
            OnPredicate.Value = null;
        }

        context.Items.Add(new Item { Id = 21, Name = "after disposal" });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
    }

    [Theory]
    [InlineData(QueryTrackingBehavior.TrackAll)]
    [InlineData(QueryTrackingBehavior.NoTracking)]
    [InlineData(QueryTrackingBehavior.NoTrackingWithIdentityResolution)]
    public async Task Ordered_Paging_Filters_Before_Counting_And_Preserves_Tracking(QueryTrackingBehavior tracking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderedContext context = await CreateAsync(stream);
        context.ChangeTracker.QueryTrackingBehavior = tracking;
        Item[] rows = await context.Items.Where(item => item.Id % 2 == 0).OrderByDescending(item => item.Id).Skip(2).Take(3).ToArrayAsync(ct);
        Assert.Equal([16, 14, 12], rows.Select(item => item.Id));
        Assert.Equal(tracking == QueryTrackingBehavior.TrackAll ? 3 : 0, context.ChangeTracker.Entries().Count());
    }

    [Fact]
    public async Task Text_And_Multiple_Orderings_Retain_Clr_Sort_Semantics()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderedContext context = await CreateAsync(stream);
        Item[] source = await context.Items.AsNoTracking().ToArrayAsync(ct);
        Assert.Equal(
            source.OrderBy(item => item.Name).Select(item => item.Id),
            await context.Items.OrderBy(item => item.Name).Select(item => item.Id).ToArrayAsync(ct));
        Assert.Equal(
            source.OrderByDescending(item => item.Id).OrderBy(item => item.Name).Select(item => item.Id),
            await context.Items.OrderByDescending(item => item.Id).OrderBy(item => item.Name).Select(item => item.Id).ToArrayAsync(ct));
        Assert.Equal(
            source.OrderByDescending(item => item.Id).Take(8).OrderBy(item => item.Name).Select(item => item.Id),
            await context.Items.OrderByDescending(item => item.Id).Take(8).OrderBy(item => item.Name).Select(item => item.Id).ToArrayAsync(ct));
    }

    [Fact]
    public async Task Cancellation_After_An_Ordered_Row_Releases_The_Read_Lease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderedContext context = await CreateAsync(stream);
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(ct);
        await using (IAsyncEnumerator<int> rows = context.Items.OrderBy(item => item.Id).Select(item => item.Id).AsAsyncEnumerable().GetAsyncEnumerator(canceled.Token))
        {
            Assert.True(await rows.MoveNextAsync());
            await canceled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await rows.MoveNextAsync());
        }

        context.Items.Add(new Item { Id = 21 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
    }

    private static bool Matches(int id)
    {
        OnPredicate.Value?.Invoke();
        return id % 2 == 0;
    }

    private static async Task<OrderedContext> CreateAsync(Stream stream)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        OrderedContext context = new(new DbContextOptionsBuilder<OrderedContext>().UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options);
        await context.Database.EnsureCreatedAsync(ct);
        context.Items.AddRange(Enumerable.Range(1, 20).Select(id => new Item { Id = id, Name = id % 2 == 0 ? "a" : "B" }));
        await context.SaveChangesAsync(ct);
        context.ChangeTracker.Clear();
        return context;
    }

    private sealed class OrderedContext(DbContextOptions<OrderedContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
            modelBuilder.Entity<Item>().HasIndex(item => item.Name);
        }
    }

    private sealed class Item
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
