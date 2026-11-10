namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetGraphScalarQueryTests
{
    private static readonly AsyncLocal<Action<Type>?> OnConstruct = new();
    private static readonly AsyncLocal<Action?> OnDecode = new();
    private static readonly AsyncLocal<Action<int>?> OnProbeDecode = new();
    private static readonly int[] VisibleIds = [1, 3];
    private static readonly int[] DerivedIds = [3];
    private static readonly int[] AllIds = [1, 2, 3];

    [Fact]
    public async Task ScalarProjectionsIgnoreIncludesAndDoNotConstructOwnedOrInheritedEntities()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        OnConstruct.Value = _ => throw new InvalidOperationException("Unexpected entity construction.");
        OnDecode.Value = () => throw new InvalidOperationException("Unexpected unrelated column decoding.");
        try
        {
            Assert.Equal(VisibleIds, await context.Items.OrderBy(item => item.Id).Select(item => item.Id).ToArrayAsync(ct));
            Assert.Equal(VisibleIds, await context.Items.Include(item => item.Children).OrderBy(item => item.Id).Select(item => item.Id).ToArrayAsync(ct));
            Assert.Equal(DerivedIds, await context.Set<SpecialItem>().Select(item => item.Id).ToArrayAsync(ct));
            Assert.Equal(AllIds, await context.Items.IgnoreQueryFilters().OrderBy(item => item.Id).Select(item => item.Id).ToArrayAsync(ct));
            Assert.Empty(context.ChangeTracker.Entries());
        }
        finally
        {
            OnConstruct.Value = null;
            OnDecode.Value = null;
        }
    }

    [Fact]
    public async Task ScalarAggregatesRetainFiltersAndDerivedDiscriminatorsWithoutConstructingGraphs()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        OnConstruct.Value = _ => throw new InvalidOperationException("Unexpected entity construction.");
        OnDecode.Value = () => throw new InvalidOperationException("Unexpected unrelated column decoding.");
        try
        {
            Assert.Equal(2, await context.Items.CountAsync(ct));
            Assert.Equal(4, await context.Items.SumAsync(item => item.Id, ct));
            Assert.Equal(4, await context.Items.Include(item => item.Children).SumAsync(item => item.Id, ct));
            Assert.Equal(3, await context.Set<SpecialItem>().SumAsync(item => item.Id, ct));
            Assert.Equal(1, await context.Set<SpecialItem>().CountAsync(ct));
            Assert.True(await context.Items.AllAsync(item => item.Active, ct));
            Assert.Empty(context.ChangeTracker.Entries());
        }
        finally
        {
            OnConstruct.Value = null;
            OnDecode.Value = null;
        }
    }

    [Fact]
    public async Task ScalarStreamingStopsBeforeDecodingLaterRowsAndReleasesItsLease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        int decoded = 0;
        OnProbeDecode.Value = _ => decoded++;
        try
        {
            await using (IAsyncEnumerator<int> rows = context.Items.Select(item => item.Probe).AsAsyncEnumerable().GetAsyncEnumerator(ct))
            {
                Assert.True(await rows.MoveNextAsync());
                Assert.Equal(1, rows.Current);
                Assert.Equal(1, decoded);
            }

            Assert.Equal(2, await context.Items.CountAsync(ct));
        }
        finally
        {
            OnProbeDecode.Value = null;
        }
    }

    [Theory(DisableParallelization = true)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FirstScalarRowAllocationDoesNotGrowWithTheUnrequestedGraph(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream smallStream = new();
        await using MemoryStream largeStream = new();
        await using GraphContext small = await CreateAsync(smallStream, 16);
        await using GraphContext large = await CreateAsync(largeStream, 512);
        await using IDbContextTransaction? smallScope = transaction ? await small.Database.BeginTransactionAsync(ct) : null;
        await using IDbContextTransaction? largeScope = transaction ? await large.Database.BeginTransactionAsync(ct) : null;
        for (int warmup = 0; warmup < 3; warmup++)
        {
            _ = await MeasureFirstScalarRowAsync(small, ct);
            _ = await MeasureFirstScalarRowAsync(large, ct);
        }

        long smallBytes = await MeasureFirstScalarRowAsync(small, ct);
        long largeBytes = await MeasureFirstScalarRowAsync(large, ct);
        Assert.True(largeBytes <= smallBytes + 131_072, $"First-row allocation grew from {smallBytes:N0} to {largeBytes:N0} bytes for 16 versus 512 graph roots.");
    }

    [Fact]
    public async Task LimitedIncludesDoNotConstructUnrequestedRootEntities()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        int roots = 0;
        OnConstruct.Value = type => roots += type == typeof(Item) ? 1 : 0;
        try
        {
            Item row = Assert.Single(await context.Items.Include(item => item.Children).Take(1).ToArrayAsync(ct));
            Assert.Equal(1, row.Id);
            Assert.Equal("detail-1", row.Detail.Text);
            Assert.Single(row.Children);
            Assert.Equal(1, roots);
        }
        finally
        {
            OnConstruct.Value = null;
        }
    }

    [Fact]
    public async Task LimitedIncludesPreservePredicateAndPagingBoundaries()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        Item[] empty = await context.Items.Include(item => item.Children).Take(1).Where(item => item.Id == 3).ToArrayAsync(ct);
        Assert.Empty(empty);
        Item row = Assert.Single(await context.Items.Include(item => item.Children).Skip(1).Where(item => item.Id == 3).Take(1).ToArrayAsync(ct));
        Assert.Equal(3, row.Id);
        Assert.IsType<SpecialItem>(row);
        Assert.Equal("detail-3", row.Detail.Text);
        Assert.Single(row.Children);
    }

    [Fact]
    public async Task EntityResultsRetainOwnedAutoIncludedAndDerivedGraphsAndTracking()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        Item[] rows = await context.Items.OrderBy(item => item.Id).ToArrayAsync(ct);
        Assert.Equal([1, 3], rows.Select(item => item.Id));
        Assert.All(rows, item => Assert.Equal($"detail-{item.Id}", item.Detail.Text));
        Assert.All(rows, item => Assert.Single(item.Children));
        Assert.IsType<SpecialItem>(rows[1]);
        Assert.All(rows, item => Assert.Same(item, context.Items.Find(item.Id)));
    }

    [Fact]
    public async Task DerivedOperatorsPreserveFilteringAndInvalidCastFailures()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        int[] derived = await context.Items.OfType<SpecialItem>().Select(item => item.Id).ToArrayAsync(ct);
        Assert.Equal(DerivedIds, derived);
        _ = await Assert.ThrowsAsync<InvalidCastException>(() => context.Items.Cast<SpecialItem>().Select(item => item.Number).ToArrayAsync(ct));
        _ = await Assert.ThrowsAsync<InvalidCastException>(() => context.Items.Select(item => ((SpecialItem)item).Number).ToArrayAsync(ct));
    }

    [Fact]
    public async Task ObjectAndInterfaceCastsRetainEntitiesAndTheirIncludedGraphs()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using GraphContext context = await CreateAsync(stream);
        object[] objects = await context.Items.Include(item => item.Children).Cast<object>().ToArrayAsync(ct);
        object[] ofTypeObjects = await context.Items.Include(item => item.Children).OfType<object>().ToArrayAsync(ct);
        IGraphItem[] interfaces = await context.Items.Include(item => item.Children).Cast<IGraphItem>().ToArrayAsync(ct);
        IGraphItem[] ofTypeInterfaces = await context.Items.Include(item => item.Children).OfType<IGraphItem>().ToArrayAsync(ct);
        foreach (object[] results in new[] { objects, ofTypeObjects, interfaces, ofTypeInterfaces })
        {
            Assert.Equal(VisibleIds, results.Cast<Item>().Select(item => item.Id));
            Assert.All(results.Cast<Item>(), item => Assert.Single(item.Children));
            Assert.All(results.Cast<Item>(), item => Assert.Equal($"detail-{item.Id}", item.Detail.Text));
        }
    }

    private static string Decode(string value)
    {
        OnDecode.Value?.Invoke();
        return value;
    }

    private static int DecodeProbe(int value)
    {
        OnProbeDecode.Value?.Invoke(value);
        return value;
    }

    private static async Task<long> MeasureFirstScalarRowAsync(GraphContext context, CancellationToken cancellationToken)
    {
        long before = GC.GetTotalAllocatedBytes(precise: true);
        await using (IAsyncEnumerator<int> rows = context.Items.Select(item => item.Id).AsAsyncEnumerable().GetAsyncEnumerator(cancellationToken))
        {
            Assert.True(await rows.MoveNextAsync());
            Assert.Equal(1, rows.Current);
        }

        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    private static async Task<GraphContext> CreateAsync(MemoryStream stream, int rowCount = 3)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        GraphContext context = new(new DbContextOptionsBuilder<GraphContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        try
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            for (int id = 1; id <= rowCount; id++)
            {
                Item item = id == 1 ? new Item() : new SpecialItem { Number = id * 10 };
                item.Id = id;
                item.Probe = id;
                item.Active = id != 2;
                item.Payload = new string('x', 1024);
                item.Detail = new Detail { Text = $"detail-{id}" };
                item.Children.Add(new Child { Id = id, OwnerId = id });
                context.Add(item);
            }

            _ = await context.SaveChangesAsync(ct);
            context.ChangeTracker.Clear();
            return context;
        }
        catch
        {
            await context.DisposeAsync();
            throw;
        }
    }

    private sealed class GraphContext(DbContextOptions<GraphContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>().ToTable("Items");
            modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
            modelBuilder.Entity<Item>().Property(item => item.Payload).HasConversion(value => value, value => Decode(value));
            modelBuilder.Entity<Item>().Property(item => item.Probe).HasConversion(value => value, value => DecodeProbe(value));
            modelBuilder.Entity<Item>().HasQueryFilter(item => item.Active);
            modelBuilder.Entity<Item>().OwnsOne(item => item.Detail);
            modelBuilder.Entity<Item>().HasMany(item => item.Children).WithOne().HasForeignKey(child => child.OwnerId);
            modelBuilder.Entity<Item>().Navigation(item => item.Children).AutoInclude();
            modelBuilder.Entity<SpecialItem>();
            modelBuilder.Entity<Child>().Property(child => child.Id).ValueGeneratedNever();
        }
    }

    private interface IGraphItem
    {
        public int Id { get; }
    }

    private class Item : IGraphItem
    {
        public Item() => OnConstruct.Value?.Invoke(typeof(Item));

        public int Id { get; set; }

        public bool Active { get; set; }

        public int Probe { get; set; }

        public string Payload { get; set; } = string.Empty;

        public Detail Detail { get; set; } = null!;

        public List<Child> Children { get; set; } = [];
    }

    private sealed class SpecialItem : Item
    {
        public int Number { get; set; }
    }

    private sealed class Detail
    {
        public Detail() => OnConstruct.Value?.Invoke(typeof(Detail));

        public string Text { get; set; } = string.Empty;
    }

    private sealed class Child
    {
        public Child() => OnConstruct.Value?.Invoke(typeof(Child));

        public int Id { get; set; }

        public int OwnerId { get; set; }
    }
}
