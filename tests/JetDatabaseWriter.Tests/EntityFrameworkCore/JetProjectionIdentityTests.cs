namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetProjectionIdentityTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task GetOnlyProjection_ResolvesTrackedEntityBeforeConstruction(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using IdentityContext context = await CreateContextAsync(stream, format);
        IdentityRow tracked = await context.Rows.SingleAsync(x => x.Id == 1, ct);
        tracked.Name = "local";

        EntityEnvelope result = await context.Rows.Where(x => x.Id == 1)
            .Select(x => new EntityEnvelope(x, x.Name)).SingleAsync(ct);

        Assert.Same(tracked, result.Entity);
        Assert.Equal("local", result.NameAtConstruction);
        Assert.Equal("original", result.ProjectedName);
        result.Entity.Name = "changed";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal("changed", await context.Rows.Where(x => x.Id == 1).Select(x => x.Name).SingleAsync(ct));
    }

    [Fact]
    public async Task FactoryProjection_ReceivesTrackedEntity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using IdentityContext context = await CreateContextAsync(stream, DatabaseFormat.AceAccdb);
        IdentityRow tracked = await context.Rows.SingleAsync(x => x.Id == 1, ct);
        EntityEnvelope result = await context.Rows.Where(x => x.Id == 1).Select(x => CreateEnvelope(x)).SingleAsync(ct);
        Assert.Same(tracked, result.Entity);
        Assert.Single(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task FilterAfterProjection_TracksOnlySurvivingEntities()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using IdentityContext context = await CreateContextAsync(stream, DatabaseFormat.AceAccdb);
        EntityEnvelope[] results = await context.Rows.Select(x => new EntityEnvelope(x, x.Name))
            .Where(x => x.Entity.Id == 1).ToArrayAsync(ct);
        Assert.Single(results);
        Assert.Same(results[0].Entity, Assert.Single(context.ChangeTracker.Entries()).Entity);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetOnlyProjection_HonorsNoTrackingIdentityResolution(bool resolveIdentity)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using IdentityContext context = await CreateContextAsync(stream, DatabaseFormat.AceAccdb);
        IQueryable<IdentityRow> source = resolveIdentity
            ? context.Rows.AsNoTrackingWithIdentityResolution()
            : context.Rows.AsNoTracking();
        EntityEnvelope[] results = await source.Where(x => x.Id == 1).Concat(source.Where(x => x.Id == 1))
            .Select(x => new EntityEnvelope(x, x.Name)).ToArrayAsync(ct);
        Assert.Equal(2, results.Length);
        if (resolveIdentity)
        {
            Assert.Same(results[0].Entity, results[1].Entity);
        }
        else
        {
            Assert.NotSame(results[0].Entity, results[1].Entity);
        }

        Assert.Empty(context.ChangeTracker.Entries());
    }

    private static EntityEnvelope CreateEnvelope(IdentityRow row) => new(row, row.Name);

    private static async Task<IdentityContext> CreateContextAsync(MemoryStream stream, DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        IdentityContext context = new(new DbContextOptionsBuilder<IdentityContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);
        try
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            context.AddRange(new IdentityRow { Id = 1, Name = "original" }, new IdentityRow { Id = 2, Name = "other" });
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

    private sealed class IdentityContext(DbContextOptions<IdentityContext> options) : DbContext(options)
    {
        public DbSet<IdentityRow> Rows => this.Set<IdentityRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<IdentityRow>().Property(x => x.Id).ValueGeneratedNever();
    }

    private sealed class IdentityRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class EntityEnvelope(IdentityRow entity, string projectedName)
    {
        public IdentityRow Entity { get; } = entity;

        public string NameAtConstruction { get; } = entity.Name;

        public string ProjectedName { get; } = projectedName;
    }
}
