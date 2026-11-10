namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetProjectionTrackingBoundaryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetOnlyOwnedProjection_ReturnsTrackedNavigationInstances(bool preload)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BoundaryContext context = await CreateContextAsync(stream);
        OwnedOwner? tracked = preload ? await context.Set<OwnedOwner>().SingleAsync(ct) : null;
        OwnedEnvelope result = await context.Set<OwnedOwner>()
            .Select(x => new OwnedEnvelope(x, x.Detail)).SingleAsync(ct);
        if (tracked is not null)
        {
            Assert.Same(tracked, result.Owner);
        }

        Assert.Same(result.Owner.Detail, result.Detail);
        Assert.Equal(EntityState.Unchanged, context.Entry(result.Detail).State);
        result.Detail.Text = "changed";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal("changed", (await context.Set<OwnedOwner>().SingleAsync(ct)).Detail.Text);
    }

    [Fact]
    public async Task GetOnlyComplexProjection_PreservesComplexValuesWhileTracking()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BoundaryContext context = await CreateContextAsync(stream);
        ComplexEnvelope result = await context.Set<ComplexOwner>()
            .Select(x => new ComplexEnvelope(x)).SingleAsync(ct);
        Assert.Equal(EntityState.Unchanged, context.Entry(result.Entity).State);
        Assert.Equal(17, context.Entry(result.Entity).ComplexProperty(x => x.Left).Property<int>("Number").CurrentValue);
        Assert.Equal(23, context.Entry(result.Entity).ComplexProperty(x => x.Right).Property<int>("Number").CurrentValue);
        result.Entity.Left.Code = "changed";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        ComplexOwner loaded = await context.Set<ComplexOwner>().SingleAsync(ct);
        Assert.Equal("changed", loaded.Left.Code);
        Assert.Equal(17, context.Entry(loaded).ComplexProperty(x => x.Left).Property<int>("Number").CurrentValue);
        Assert.Equal(23, context.Entry(loaded).ComplexProperty(x => x.Right).Property<int>("Number").CurrentValue);
    }

    [Fact]
    public async Task OuterProjectedPredicateAndOrdering_UseStoredScalarValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BoundaryContext context = await CreateContextAsync(stream);
        OwnedOwner tracked = await context.Set<OwnedOwner>().SingleAsync(ct);
        tracked.Name = "local";
        tracked.Rank = 7;
        OwnedEnvelope[] results = await context.Set<OwnedOwner>()
            .Select(x => new OwnedEnvelope(x, x.Detail))
            .Where(x => x.Owner.Name == "original" && x.Owner.Rank == 1)
            .OrderBy(x => x.Owner.Rank).ToArrayAsync(ct);
        Assert.Same(tracked, Assert.Single(results).Owner);
    }

    [Fact]
    public async Task OuterComplexPredicateAndOrdering_UseStoredScalarValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using BoundaryContext context = await CreateContextAsync(stream);
        context.Add(new ComplexOwner { Id = 2, Left = new ComplexData { Code = "second", Number = 3 } });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        ComplexOwner tracked = await context.Set<ComplexOwner>().SingleAsync(x => x.Id == 1, ct);
        tracked.Left.Code = "local";
        tracked.Left.Number = -100;
        IQueryable<ComplexEnvelope> source = context.Set<ComplexOwner>().Select(x => new ComplexEnvelope(x));
        ComplexEnvelope[] matches = await source.Where(x => x.Entity.Left.Code == "left" && x.Entity.Left.Number == 17)
            .OrderBy(x => x.Entity.Left.Number).ToArrayAsync(ct);
        Assert.Same(tracked, Assert.Single(matches).Entity);
        ComplexEnvelope[] ordered = await source.OrderBy(x => x.Entity.Left.Number).ToArrayAsync(ct);
        Assert.Equal([2, 1], ordered.Select(x => x.Entity.Id));
        ComplexEnvelope dynamicPropertyMatch = await source.SingleAsync(x => EF.Property<int>(x.Entity.Left, "Number") == 17, ct);
        Assert.Same(tracked, dynamicPropertyMatch.Entity);
    }

    private static async Task<BoundaryContext> CreateContextAsync(MemoryStream stream)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        BoundaryContext context = new(new DbContextOptionsBuilder<BoundaryContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        try
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            context.Add(new OwnedOwner { Id = 1, Name = "original", Rank = 1, Detail = new OwnedDetail { Text = "original" } });
            ComplexOwner complex = new() { Id = 1, Left = new ComplexData { Code = "left" }, Right = new ComplexData { Code = "right" } };
            context.Add(complex);
            context.Entry(complex).ComplexProperty(x => x.Left).Property<int>("Number").CurrentValue = 17;
            context.Entry(complex).ComplexProperty(x => x.Right).Property<int>("Number").CurrentValue = 23;
            Assert.Equal(2, await context.SaveChangesAsync(ct));
            context.ChangeTracker.Clear();
            return context;
        }
        catch
        {
            await context.DisposeAsync();
            throw;
        }
    }

    private sealed class BoundaryContext(DbContextOptions<BoundaryContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<OwnedOwner>().Property(x => x.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<OwnedOwner>().OwnsOne(x => x.Detail);
            _ = modelBuilder.Entity<OwnedOwner>().Navigation(x => x.Detail).IsRequired();
            _ = modelBuilder.Entity<ComplexOwner>().Property(x => x.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<ComplexOwner>().ComplexProperty(x => x.Left).Property(x => x.Number).HasColumnName("LeftNumber");
            _ = modelBuilder.Entity<ComplexOwner>().ComplexProperty(x => x.Right).Property(x => x.Number).HasColumnName("RightNumber");
        }
    }

    private sealed class OwnedOwner
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Rank { get; set; }

        public OwnedDetail Detail { get; set; } = new();
    }

    private sealed class OwnedDetail
    {
        public string Text { get; set; } = string.Empty;
    }

    private sealed class ComplexOwner
    {
        public int Id { get; set; }

        public ComplexData Left { get; set; } = new();

        public ComplexData Right { get; set; } = new();
    }

    private sealed class ComplexData
    {
        public string Code { get; set; } = string.Empty;

        public int Number { get; set; }
    }

    private sealed class OwnedEnvelope(OwnedOwner owner, OwnedDetail detail)
    {
        public OwnedOwner Owner { get; } = owner;

        public OwnedDetail Detail { get; } = detail;
    }

    private sealed class ComplexEnvelope(ComplexOwner entity)
    {
        public ComplexOwner Entity { get; } = entity;
    }
}
