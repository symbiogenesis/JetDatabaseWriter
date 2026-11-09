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

public sealed class JetQueryFilterTests
{
    private static readonly int[] TenantTwoIds = [2];
    private static readonly int[] TenantTwoAllIds = [2, 4];
    private static readonly int[] ActiveIds = [1, 2];
    private static readonly int[] OtherIds = [1, 3, 4];
    private static readonly int[] TenantOneIds = [1];
    private static readonly int[] TenantOneAllIds = [1, 3];

    [Theory]
    [InlineData("stream")]
    [InlineData("ordered")]
    [InlineData("aggregate")]
    [InlineData("fallback")]
    [InlineData("include")]
    [InlineData("navigation")]
    [InlineData("update")]
    [InlineData("delete")]
    public async Task ContextFiltersUseTheExecutingContext(string executionPath)
    {
        await using MemoryStream stream = new();
        DbContextOptions<TenantContext> options = CreateOptions(stream);
        await SeedAsync(options);
        await using TenantContext context = new(options, 2);
        CancellationToken ct = TestContext.Current.CancellationToken;

        switch (executionPath)
        {
            case "stream":
                Assert.Equal(TenantTwoIds, await context.Items.Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "ordered":
                Assert.Equal(TenantTwoIds, await context.Items.OrderBy(item => item.Id).Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "aggregate":
                Assert.Equal(2, await context.Items.SumAsync(item => item.Id, ct));
                break;
            case "fallback":
                Assert.Equal(TenantTwoIds, await context.Items.Where(item => AcceptEntity(item)).Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "include":
                TenantGroup group = Assert.Single(await context.Groups.Include(group => group.Items).ToArrayAsync(ct));
                Assert.Equal(2, group.Id);
                Assert.Equal(2, Assert.Single(group.Items).Id);
                break;
            case "navigation":
                Assert.Equal(TenantTwoIds, await context.Items.Where(item => item.Group.TenantId == 2).Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "update":
                Assert.Equal(1, await context.Items.ExecuteUpdateAsync(setters => setters.SetProperty(item => item.Amount, 99), ct));
                Assert.Equal(TenantTwoIds, await context.Items.IgnoreQueryFilters().Where(item => item.Amount == 99).Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "delete":
                Assert.Equal(1, await context.Items.ExecuteDeleteAsync(ct));
                Assert.Equal(OtherIds, await context.Items.IgnoreQueryFilters().OrderBy(item => item.Id).Select(item => item.Id).ToArrayAsync(ct));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(executionPath));
        }
    }

    [Theory]
    [InlineData("stream")]
    [InlineData("aggregate")]
    [InlineData("fallback")]
    [InlineData("include")]
    [InlineData("navigation")]
    [InlineData("update")]
    [InlineData("delete")]
    public async Task NamedFilterDisablingPreservesOtherFilters(string executionPath)
    {
        await using MemoryStream stream = new();
        DbContextOptions<TenantContext> options = CreateOptions(stream);
        await SeedAsync(options);
        await using TenantContext context = new(options, 2);
        CancellationToken ct = TestContext.Current.CancellationToken;

        IQueryable<TenantItem> items = context.Items.IgnoreQueryFilters(["SoftDeletion"]);
        switch (executionPath)
        {
            case "stream":
                Assert.Equal(TenantTwoAllIds, await items.Select(item => item.Id).ToArrayAsync(ct));
                Assert.Equal(ActiveIds, await context.Items.IgnoreQueryFilters(["Tenant"]).OrderBy(item => item.Id).Select(item => item.Id).ToArrayAsync(ct));
                Assert.Equal(TenantTwoIds, await context.Items.IgnoreQueryFilters(["Unknown"]).Select(item => item.Id).ToArrayAsync(ct));
                Assert.Equal(TenantTwoIds, await context.Items.IgnoreQueryFilters([]).Select(item => item.Id).ToArrayAsync(ct));
                Assert.Equal(4, await items.IgnoreQueryFilters(["Tenant"]).CountAsync(ct));
                Assert.Equal(4, await context.Items.IgnoreQueryFilters().IgnoreQueryFilters(["Unknown"]).CountAsync(ct));
                break;
            case "aggregate":
                Assert.Equal(6, await items.SumAsync(item => item.Id, ct));
                Assert.Equal(2, await items.CountAsync(ct));
                break;
            case "fallback":
                Assert.Equal(TenantTwoAllIds, await items.Where(item => AcceptEntity(item)).Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "include":
                TenantGroup group = Assert.Single(await context.Groups.IgnoreQueryFilters(["SoftDeletion"]).Include(group => group.Items).ToArrayAsync(ct));
                Assert.Equal(2, group.Id);
                Assert.Equal(TenantTwoAllIds, group.Items.OrderBy(item => item.Id).Select(item => item.Id));
                break;
            case "navigation":
                Assert.Equal(TenantTwoAllIds, await items.Where(item => item.Group.TenantId == 2).Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "update":
                Assert.Equal(2, await items.ExecuteUpdateAsync(setters => setters.SetProperty(item => item.Amount, 99), ct));
                Assert.Equal(TenantTwoAllIds, await context.Items.IgnoreQueryFilters().Where(item => item.Amount == 99).Select(item => item.Id).ToArrayAsync(ct));
                break;
            case "delete":
                Assert.Equal(2, await items.ExecuteDeleteAsync(ct));
                Assert.Equal(TenantOneAllIds, await context.Items.IgnoreQueryFilters().Select(item => item.Id).ToArrayAsync(ct));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(executionPath));
        }
    }

    [Fact]
    public async Task CachedAndCompiledQueriesObserveCurrentContextValues()
    {
        await using MemoryStream stream = new();
        DbContextOptions<TenantContext> options = CreateOptions(stream);
        await SeedAsync(options);
        Func<TenantContext, IAsyncEnumerable<int>> compiled = EF.CompileAsyncQuery((TenantContext context) => context.Items.Select(item => item.Id));
        await using TenantContext first = new(options, 1);
        await using TenantContext second = new(options, 2);
        List<int> actual = [];
        await foreach (int id in compiled(second).WithCancellation(TestContext.Current.CancellationToken))
        {
            actual.Add(id);
        }

        Assert.Equal(TenantTwoIds, actual);
#pragma warning disable CA1861 // The array construction deliberately remains inside the compiled query expression.
        Func<TenantContext, IAsyncEnumerable<int>> compiledNamed = EF.CompileAsyncQuery((TenantContext context) => context.Items.IgnoreQueryFilters(new[] { "SoftDeletion" }).Select(item => item.Id));
#pragma warning restore CA1861
        actual.Clear();
        await foreach (int id in compiledNamed(second).WithCancellation(TestContext.Current.CancellationToken))
        {
            actual.Add(id);
        }

        Assert.Equal(TenantTwoAllIds, actual);
        Assert.Equal(TenantOneIds, await first.Items.Select(item => item.Id).ToArrayAsync(TestContext.Current.CancellationToken));
        first.TenantId = 2;
        Assert.Equal(TenantTwoIds, await first.Items.Select(item => item.Id).ToArrayAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(QueryTrackingBehavior.NoTracking, QueryTrackingBehavior.TrackAll)]
    [InlineData(QueryTrackingBehavior.TrackAll, QueryTrackingBehavior.NoTracking)]
    [InlineData(QueryTrackingBehavior.NoTracking, QueryTrackingBehavior.NoTrackingWithIdentityResolution)]
    [InlineData(QueryTrackingBehavior.NoTrackingWithIdentityResolution, QueryTrackingBehavior.TrackAll)]
    [InlineData(QueryTrackingBehavior.TrackAll, QueryTrackingBehavior.NoTrackingWithIdentityResolution)]
    [InlineData(QueryTrackingBehavior.NoTrackingWithIdentityResolution, QueryTrackingBehavior.NoTracking)]
    public async Task LastTrackingModifierWins(QueryTrackingBehavior first, QueryTrackingBehavior last)
    {
        await using MemoryStream stream = new();
        DbContextOptions<TenantContext> options = CreateOptions(stream);
        await SeedAsync(options);
        await using TenantContext context = new(options, 1);
        IQueryable<TenantItem> query = SetTracking(SetTracking(context.Items, first), last);
        Assert.Single(await query.ToArrayAsync(TestContext.Current.CancellationToken));
        Assert.Equal(last == QueryTrackingBehavior.TrackAll ? 1 : 0, context.ChangeTracker.Entries().Count());
    }

    [Theory]
    [InlineData(QueryTrackingBehavior.TrackAll)]
    [InlineData(QueryTrackingBehavior.NoTracking)]
    [InlineData(QueryTrackingBehavior.NoTrackingWithIdentityResolution)]
    public async Task CompiledTrackingModifierHonorsExplicitBehavior(QueryTrackingBehavior behavior)
    {
        await using MemoryStream stream = new();
        DbContextOptions<TenantContext> options = CreateOptions(stream);
        await SeedAsync(options);
        await using TenantContext context = new(options, 1);
        Func<TenantContext, IAsyncEnumerable<TenantItem>> compiled = EF.CompileAsyncQuery((TenantContext executingContext) => executingContext.Items.AsTracking(behavior));
        await foreach (TenantItem item in compiled(context).WithCancellation(TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, item.Id);
        }

        Assert.Equal(behavior == QueryTrackingBehavior.TrackAll ? 1 : 0, context.ChangeTracker.Entries().Count());
    }

    private static bool AcceptEntity(TenantItem item) => item.Id > 0;

    private static IQueryable<TenantItem> SetTracking(IQueryable<TenantItem> query, QueryTrackingBehavior behavior)
        => behavior switch
        {
            QueryTrackingBehavior.TrackAll => query.AsTracking(),
            QueryTrackingBehavior.NoTracking => query.AsNoTracking(),
            QueryTrackingBehavior.NoTrackingWithIdentityResolution => query.AsNoTrackingWithIdentityResolution(),
            _ => throw new ArgumentOutOfRangeException(nameof(behavior)),
        };

    private static DbContextOptions<TenantContext> CreateOptions(Stream stream)
        => new DbContextOptionsBuilder<TenantContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options;

    private static async Task SeedAsync(DbContextOptions<TenantContext> options)
    {
        await using TenantContext context = new(options, 1);
        CancellationToken ct = TestContext.Current.CancellationToken;
        await context.Database.EnsureCreatedAsync(ct);
        context.AddRange(new TenantGroup { Id = 1, TenantId = 1 }, new TenantGroup { Id = 2, TenantId = 2 });
        context.AddRange(
            new TenantItem { Id = 1, TenantId = 1, GroupId = 1 },
            new TenantItem { Id = 2, TenantId = 2, GroupId = 2 },
            new TenantItem { Id = 3, TenantId = 1, GroupId = 1, IsDeleted = true },
            new TenantItem { Id = 4, TenantId = 2, GroupId = 2, IsDeleted = true });
        await context.SaveChangesAsync(ct);
    }

    private sealed class TenantContext(DbContextOptions<TenantContext> options, int tenantId) : DbContext(options)
    {
        public int TenantId { get; set; } = tenantId;

        public DbSet<TenantItem> Items => this.Set<TenantItem>();

        public DbSet<TenantGroup> Groups => this.Set<TenantGroup>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TenantItem>().Property(item => item.Id).ValueGeneratedNever();
            modelBuilder.Entity<TenantItem>().HasIndex(item => item.TenantId);
            modelBuilder.Entity<TenantItem>().HasQueryFilter("Tenant", item => item.TenantId == this.TenantId)
                .HasQueryFilter("SoftDeletion", item => !item.IsDeleted);
            modelBuilder.Entity<TenantGroup>().Property(group => group.Id).ValueGeneratedNever();
            modelBuilder.Entity<TenantGroup>().HasQueryFilter("Tenant", group => group.TenantId == this.TenantId);
            modelBuilder.Entity<TenantItem>().HasOne(item => item.Group).WithMany(group => group.Items).HasForeignKey(item => item.GroupId);
        }
    }

    private sealed class TenantItem
    {
        public int Id { get; set; }

        public int TenantId { get; set; }

        public bool IsDeleted { get; set; }

        public int Amount { get; set; }

        public int GroupId { get; set; }

        public TenantGroup Group { get; set; } = null!;
    }

    private sealed class TenantGroup
    {
        public int Id { get; set; }

        public int TenantId { get; set; }

        public ICollection<TenantItem> Items { get; } = [];
    }
}
