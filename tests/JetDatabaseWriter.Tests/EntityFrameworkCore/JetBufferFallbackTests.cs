namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetBufferFallbackTests
{
    [Fact]
    public async Task EntityInstanceMethod_PreservesItsReceiver()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
        int[] ids = await context.Items.Where(x => x.IsEligible()).Select(x => x.Id).ToArrayAsync(ct);
        Assert.Equal([1, 2], ids);
    }

    [Fact]
    public async Task GenericEntityProjection_PreservesClassConstraints()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
        EntityBox<EqualItem>[] boxes = await context.Items.AsNoTracking().Select(x => new EntityBox<EqualItem>(x)).ToArrayAsync(ct);
        Assert.Equal([1, 2, 3], boxes.Select(x => x.Value.Id));
    }

    [Fact]
    public async Task ShadowProperty_ResolvesAnObjectConvertedEntityReceiver()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
#pragma warning disable IDE0004 // Preserve the explicit object conversion in the query expression under test.
        int[] values = await context.Items.Select(x => EF.Property<int>((object)x, "ShadowNumber")).ToArrayAsync(ct);
#pragma warning restore IDE0004
        Assert.Equal([10, 20, 30], values);
    }

    [Fact]
    public async Task EntityDistinctAndGrouping_UseEntityEquality()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
        Assert.Equal(2, await context.Items.Distinct().CountAsync(ct));
        int[] counts = await context.Items.GroupBy(x => x).Select(group => group.Count()).ToArrayAsync(ct);
        Assert.Equal([2, 1], counts);
    }

    [Fact]
    public async Task EntityReferenceEquals_PreservesReferenceSemantics()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
        Assert.All(await context.Items.Select(x => ReferenceEquals(x, x)).ToArrayAsync(ct), Assert.True);
    }

    [Fact]
    public async Task RepeatedEntityProjection_PreservesSameRowReferencesWithoutTracking()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
        var results = await context.Items.AsNoTracking().Select(x => new { First = x, Second = x }).ToArrayAsync(ct);
        Assert.Equal(3, results.Length);
        Assert.All(results, item => Assert.Same(item.First, item.Second));
        Assert.Empty(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task UnmatchedLeftJoinEntityProjection_ReturnsNullInnerEntities()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
        var results = await context.Items.AsNoTracking()
            .LeftJoin(context.Others.AsNoTracking(), x => x.Id, y => y.OwnerId, (x, y) => new { Outer = x, Inner = y })
            .ToArrayAsync(ct);
        Assert.Equal(3, results.Length);
        Assert.NotNull(results[0].Inner);
        Assert.Null(results[1].Inner);
        Assert.Null(results[2].Inner);
    }

    [Fact]
    public async Task PrivateSetterDtoProjection_PromotesItsEntityToTheTrackedInstance()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FallbackContext context = await CreateContextAsync(stream);
        EqualItem tracked = await context.Items.SingleAsync(x => x.Id == 1, ct);
        PrivateSetterProjection result = await context.Items.Where(x => x.Id == 1).Select(x => new PrivateSetterProjection(x)).SingleAsync(ct);
        Assert.Same(tracked, result.Entity);
    }

    private static async Task<FallbackContext> CreateContextAsync(MemoryStream stream)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        FallbackContext context = new(new DbContextOptionsBuilder<FallbackContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        try
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            for (int id = 1; id <= 3; id++)
            {
                EqualItem item = new() { Id = id, Amount = id < 3 ? 2 : 4 };
                context.Add(item);
                context.Entry(item).Property<int>("ShadowNumber").CurrentValue = id * 10;
            }

            context.Others.Add(new OtherItem { Id = 17, OwnerId = 1 });
            Assert.Equal(4, await context.SaveChangesAsync(ct));
            context.ChangeTracker.Clear();
            return context;
        }
        catch
        {
            await context.DisposeAsync();
            throw;
        }
    }

    private sealed class FallbackContext(DbContextOptions<FallbackContext> options) : DbContext(options)
    {
        public DbSet<EqualItem> Items => this.Set<EqualItem>();

        public DbSet<OtherItem> Others => this.Set<OtherItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<EqualItem>().Property(x => x.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<EqualItem>().Property<int>("ShadowNumber");
            _ = modelBuilder.Entity<OtherItem>().Property(x => x.Id).ValueGeneratedNever();
        }
    }

    private sealed class EqualItem : IEquatable<EqualItem>
    {
        public int Id { get; set; }

        public int Amount { get; set; }

        public bool IsEligible() => this.Amount == 2;

        public bool Equals(EqualItem? other) => other is not null && this.Amount == other.Amount;

        public override bool Equals(object? obj) => obj is EqualItem other && this.Equals(other);

        public override int GetHashCode() => this.Amount;
    }

    private sealed class EntityBox<TEntity>(TEntity value)
        where TEntity : class
    {
        public TEntity Value { get; } = value;
    }

    private sealed class OtherItem
    {
        public int Id { get; set; }

        public int OwnerId { get; set; }
    }

    private sealed class PrivateSetterProjection(EqualItem entity)
    {
#pragma warning disable RCS1170 // The query result promoter must replace the entity through this private setter.
        public EqualItem Entity { get; private set; } = entity;
#pragma warning restore RCS1170
    }
}
