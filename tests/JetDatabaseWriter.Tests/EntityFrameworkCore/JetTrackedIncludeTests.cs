namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetTrackedIncludeTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FilteredIncludesPreservePreviouslyTrackedRelationships(bool skipNavigation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<IncludeContext> options = await CreateDatabaseAsync(stream, ct);
        await using IncludeContext context = new(options);
        Owner original = await LoadOwnerAsync(context, skipNavigation, filtered: false, ct);
        Dictionary<int, object> originalRelated = skipNavigation
            ? original.Tags.ToDictionary(tag => tag.Id, tag => (object)tag)
            : original.Children.ToDictionary(child => child.Id, child => (object)child);

        Owner filtered = await LoadOwnerAsync(context, skipNavigation, filtered: true, ct);
        Owner repeated = await LoadOwnerAsync(context, skipNavigation, filtered: true, ct);

        Assert.Same(original, filtered);
        Assert.Same(original, repeated);
        Assert.Equal([1, 2], GetRelatedIds(original, skipNavigation));
        if (skipNavigation)
        {
            Assert.All(original.Tags, tag => Assert.Same(originalRelated[tag.Id], tag));
        }
        else
        {
            Assert.All(original.Children, child => Assert.Same(originalRelated[child.Id], child));
        }

        Assert.True(context.Entry(original).Collection(skipNavigation ? nameof(Owner.Tags) : nameof(Owner.Children)).IsLoaded);
        Assert.False(context.ChangeTracker.HasChanges());
        Assert.Equal(0, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Owner stored = await LoadOwnerAsync(context, skipNavigation, filtered: false, ct);
        Assert.Equal([1, 2], GetRelatedIds(stored, skipNavigation));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IncludesPreservePendingRelationshipChanges(bool skipNavigation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<IncludeContext> options = await CreateDatabaseAsync(stream, ct);
        await using IncludeContext context = new(options);
        Owner original = await LoadOwnerAsync(context, skipNavigation, filtered: false, ct);
        original.Name = "changed";
        if (skipNavigation)
        {
            Tag removed = original.Tags.Single(tag => tag.Id == 2);
            Assert.True(original.Tags.Remove(removed));
            original.Tags.Add(new Tag { Id = 3 });
        }
        else
        {
            Child removed = original.Children.Single(child => child.Id == 2);
            Assert.True(original.Children.Remove(removed));
            original.Children.Add(new Child { Id = 3 });
        }

        context.ChangeTracker.DetectChanges();
        Owner loaded = await LoadOwnerAsync(context, skipNavigation, filtered: false, ct);

        Assert.Same(original, loaded);
        Assert.Equal("changed", loaded.Name);
        Assert.Equal([1, 3], GetRelatedIds(loaded, skipNavigation));
        _ = await context.SaveChangesAsync(ct);
        context.ChangeTracker.Clear();
        Owner stored = await LoadOwnerAsync(context, skipNavigation, filtered: false, ct);
        Assert.Equal("changed", stored.Name);
        Assert.Equal([1, 3], GetRelatedIds(stored, skipNavigation));
    }

    [Fact]
    public async Task IncludesPreserveReparentedTrackedChildren()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<IncludeContext> options = await CreateDatabaseAsync(stream, ct);
        await using IncludeContext context = new(options);
        Owner original = await LoadOwnerAsync(context, skipNavigation: false, filtered: false, ct);
        Owner destination = new() { Id = 2, Name = "destination" };
        _ = context.Add(destination);
        _ = await context.SaveChangesAsync(ct);
        Child moved = original.Children.Single(child => child.Id == 2);
        moved.Owner = destination;
        moved.OwnerId = destination.Id;
        context.ChangeTracker.DetectChanges();

        Owner loaded = await context.Set<Owner>().Where(owner => owner.Id == 1)
            .Include(owner => owner.Children).SingleAsync(ct);

        Assert.Same(original, loaded);
        Assert.Equal([1], GetRelatedIds(loaded, skipNavigation: false));
        Assert.Same(destination, moved.Owner);
        Assert.Same(moved, Assert.Single(destination.Children));
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Child stored = await context.Set<Child>().SingleAsync(child => child.Id == 2, ct);
        Assert.Equal(2, stored.OwnerId);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task UntrackedFilteredIncludesRetainTheirOwnResults(bool skipNavigation, bool identityResolution)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<IncludeContext> options = await CreateDatabaseAsync(stream, ct);
        await using IncludeContext context = new(options);
        Owner original = await LoadOwnerAsync(context, skipNavigation, filtered: false, ct);
        IQueryable<Owner> query = identityResolution
            ? context.Set<Owner>().AsNoTrackingWithIdentityResolution()
            : context.Set<Owner>().AsNoTracking();
        Owner filtered = skipNavigation
            ? await query.Include(owner => owner.Tags.Where(tag => tag.Id == 1)).SingleAsync(ct)
            : await query.Include(owner => owner.Children.Where(child => child.Id == 1)).SingleAsync(ct);

        Assert.NotSame(original, filtered);
        Assert.Equal([1], GetRelatedIds(filtered, skipNavigation));
        Assert.Equal([1, 2], GetRelatedIds(original, skipNavigation));
        Assert.False(context.ChangeTracker.HasChanges());
        Assert.Equal(0, await context.SaveChangesAsync(ct));
    }

    private static IEnumerable<int> GetRelatedIds(Owner owner, bool skipNavigation)
        => skipNavigation
            ? owner.Tags.Select(tag => tag.Id).Order()
            : owner.Children.Select(child => child.Id).Order();

    private static async Task<Owner> LoadOwnerAsync(IncludeContext context, bool skipNavigation, bool filtered, CancellationToken ct)
    {
        IQueryable<Owner> query = context.Set<Owner>();
        if (skipNavigation)
        {
            query = filtered
                ? query.Include(owner => owner.Tags.Where(tag => tag.Id == 1))
                : query.Include(owner => owner.Tags);
        }
        else
        {
            query = filtered
                ? query.Include(owner => owner.Children.Where(child => child.Id == 1))
                : query.Include(owner => owner.Children);
        }

        return await query.SingleAsync(ct);
    }

    private static async Task<DbContextOptions<IncludeContext>> CreateDatabaseAsync(MemoryStream stream, CancellationToken ct)
    {
        DbContextOptions<IncludeContext> options = new DbContextOptionsBuilder<IncludeContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using IncludeContext seed = new(options);
        Assert.True(await seed.Database.EnsureCreatedAsync(ct));
        _ = seed.Add(new Owner
        {
            Id = 1,
            Name = "original",
            Children = [new Child { Id = 1 }, new Child { Id = 2 }],
            Tags = [new Tag { Id = 1 }, new Tag { Id = 2 }],
        });
        _ = await seed.SaveChangesAsync(ct);
        return options;
    }

    private sealed class IncludeContext(DbContextOptions<IncludeContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Owner>().Property(owner => owner.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Child>().Property(child => child.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Tag>().Property(tag => tag.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Owner>().HasMany(owner => owner.Children).WithOne(child => child.Owner)
                .HasForeignKey(child => child.OwnerId).IsRequired();
            _ = modelBuilder.Entity<Owner>().HasMany(owner => owner.Tags).WithMany(tag => tag.Owners);
        }
    }

    private sealed class Owner
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<Child> Children { get; set; } = [];

        public List<Tag> Tags { get; set; } = [];
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int OwnerId { get; set; }

        public Owner Owner { get; set; } = null!;
    }

    private sealed class Tag
    {
        public int Id { get; set; }

        public List<Owner> Owners { get; set; } = [];
    }
}
