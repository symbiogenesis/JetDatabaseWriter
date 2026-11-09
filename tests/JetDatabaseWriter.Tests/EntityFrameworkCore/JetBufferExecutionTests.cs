namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetBufferExecutionTests
{
    private static readonly AsyncLocal<Action?> OnConstruct = new();
    private static readonly int[] SelectedAmounts = [2, 4, 6];
    private static readonly int[] AllAmounts = [0, 2, 4, 6];
    private static readonly int[] GroupCounts = [2, 2];
    private static readonly int[] UpdatedAmounts = [0, 2, 14, 16];
    private static readonly int[] RemainingIds = [1, 2, 4];

    [Fact]
    public async Task ScalarProjectionsGroupsAndJoinsDoNotConstructEntities()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BufferContext context = await CreateContextAsync(stream);
        OnConstruct.Value = () => throw new InvalidOperationException("Unexpected entity construction.");
        try
        {
            Assert.Equal(SelectedAmounts, await context.Items.Where(x => x.Id > 1).Select(x => x.Amount).ToArrayAsync(ct));
            Assert.Equal(AllAmounts, await context.Items.Select(x => x.Amount).Distinct().ToArrayAsync(ct));
            Assert.Equal(GroupCounts, await context.Items.GroupBy(x => x.Id % 2).Select(g => g.Count()).ToArrayAsync(ct));
            Assert.Equal(AllAmounts, await context.Items.Join(context.Items, x => x.Id, y => y.Id, (_, y) => y.Amount).ToArrayAsync(ct));
        }
        finally
        {
            OnConstruct.Value = null;
        }
    }

    [Fact]
    public async Task AggregatesDoNotConstructEntitiesAndKeepLinqSemantics()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BufferContext context = await CreateContextAsync(stream);
        OnConstruct.Value = () => throw new InvalidOperationException("Unexpected entity construction.");
        try
        {
            Assert.Equal(12, await context.Items.SumAsync(x => x.Amount, ct));
            Assert.Equal(0, await context.Items.MinAsync(x => x.Amount, ct));
            Assert.Equal(6, await context.Items.MaxAsync(x => x.Amount, ct));
            Assert.Equal(3, await context.Items.AverageAsync(x => x.Amount, ct));
            Assert.True(await context.Items.AllAsync(x => x.Amount >= 0, ct));
            Assert.False(await context.Items.AllAsync(x => x.Amount > 0, ct));
            Assert.Equal(0, await context.Items.Where(x => x.Id < 0).SumAsync(x => x.Amount, ct));
            Assert.Null(await context.Items.Where(x => x.Id < 0).MaxAsync(x => (int?)x.Amount, ct));
        }
        finally
        {
            OnConstruct.Value = null;
        }
    }

    [Fact]
    public async Task PlainEntityEnumerationMaterializesOneRowAtATime()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BufferContext context = await CreateContextAsync(stream);
        int constructed = 0;
        OnConstruct.Value = () => constructed++;
        try
        {
            await using IAsyncEnumerator<BufferItem> enumerator = context.Items.AsNoTracking().AsAsyncEnumerable().GetAsyncEnumerator(ct);
            Assert.True(await enumerator.MoveNextAsync());
            Assert.Equal(1, constructed);
            Assert.Equal(1, enumerator.Current.Id);
        }
        finally
        {
            OnConstruct.Value = null;
        }
    }

    [Fact]
    public async Task EntityProjectionMaterializesOnlySurvivingRowsAndTracksReturnedInstances()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BufferContext context = await CreateContextAsync(stream);
        int constructed = 0;
        OnConstruct.Value = () => constructed++;
        try
        {
            var results = await context.Items.OrderByDescending(x => x.Id).Take(1).Select(x => new { Entity = x, x.Amount }).ToArrayAsync(ct);
            var result = Assert.Single(results);
            Assert.Equal(1, constructed);
            Assert.Equal(4, result.Entity.Id);
            Assert.Same(result.Entity, context.ChangeTracker.Entries<BufferItem>().Single().Entity);
        }
        finally
        {
            OnConstruct.Value = null;
        }
    }

    [Fact]
    public async Task BulkMutationsReadBuffersWithoutConstructingEntities()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BufferContext context = await CreateContextAsync(stream);
        OnConstruct.Value = () => throw new InvalidOperationException("Unexpected entity construction.");
        try
        {
            Assert.Equal(2, await context.Items.Where(x => x.Id > 2).ExecuteUpdateAsync(s => s.SetProperty(x => x.Amount, x => x.Amount + 10), ct));
            Assert.Equal(UpdatedAmounts, await context.Items.Select(x => x.Amount).ToArrayAsync(ct));
            Assert.Equal(1, await context.Items.Where(x => x.Amount > 10).OrderBy(x => x.Id).Take(1).ExecuteDeleteAsync(ct));
            Assert.Equal(RemainingIds, await context.Items.Select(x => x.Id).ToArrayAsync(ct));
        }
        finally
        {
            OnConstruct.Value = null;
        }
    }

    private static async Task<BufferContext> CreateContextAsync(MemoryStream stream)
    {
        BufferContext context = new(new DbContextOptionsBuilder<BufferContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        await context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken);
        context.Items.AddRange(Enumerable.Range(1, 4).Select(i => new BufferItem { Id = i, Amount = (i - 1) * 2 }));
        await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        context.ChangeTracker.Clear();
        return context;
    }

    private sealed class BufferContext(DbContextOptions<BufferContext> options) : DbContext(options)
    {
        public DbSet<BufferItem> Items => this.Set<BufferItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<BufferItem>().Property(x => x.Id).ValueGeneratedNever();
    }

    private sealed class BufferItem
    {
        public BufferItem() => OnConstruct.Value?.Invoke();

        public int Id { get; set; }

        public int Amount { get; set; }
    }
}
