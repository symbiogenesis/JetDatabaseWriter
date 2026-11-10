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

public sealed class JetSkipNavigationQueryFilterTests
{
    private static readonly int[] TenantOneActiveIds = [1];
    private static readonly int[] TenantTwoActiveIds = [3];
    private static readonly int[] TenantOneIds = [1, 2];
    private static readonly int[] ActiveIds = [1, 3];
    private static readonly int[] AllIds = [1, 2, 3, 4];

    [Theory]
    [InlineData(QueryTrackingBehavior.TrackAll)]
    [InlineData(QueryTrackingBehavior.NoTracking)]
    [InlineData(QueryTrackingBehavior.NoTrackingWithIdentityResolution)]
    public async Task IncludesApplyJoinEntityFiltersInBothDirections(QueryTrackingBehavior tracking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<LinkContext> options = await CreateDatabaseAsync(stream, ct);
        await using (LinkContext context = new(options, 1))
        {
            Assert.Equal(TenantOneActiveIds, await context.Links.Select(link => link.TagId).ToArrayAsync(ct));
            LinkPost post = await context.Posts.AsTracking(tracking).Include(post => post.Tags).SingleAsync(ct);
            Assert.Equal(TenantOneActiveIds, post.Tags.Select(tag => tag.Id));
            Assert.Equal(tracking == QueryTrackingBehavior.TrackAll ? 1 : 0, context.ChangeTracker.Entries<PostTag>().Count());
        }

        await using LinkContext inverse = new(options, 1);
        LinkTag[] tags = await inverse.Tags.AsTracking(tracking).Include(tag => tag.Posts).OrderBy(tag => tag.Id).ToArrayAsync(ct);
        Assert.Equal(TenantOneActiveIds, tags.Where(tag => tag.Posts.Count > 0).Select(tag => tag.Id));
        Assert.Equal(1, Assert.Single(tags[0].Posts).Id);
        Assert.Equal(tracking == QueryTrackingBehavior.TrackAll ? 1 : 0, inverse.ChangeTracker.Entries<PostTag>().Count());
    }

    [Theory]
    [InlineData("SoftDeletion")]
    [InlineData("Tenant")]
    [InlineData("Unknown")]
    [InlineData("All")]
    public async Task IgnoringJoinFiltersPreservesTheRemainingFilters(string ignoredFilter)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<LinkContext> options = await CreateDatabaseAsync(stream, ct);
        await using LinkContext context = new(options, 1);
        IQueryable<LinkPost> posts = context.Posts.AsNoTracking();
        posts = ignoredFilter == "All" ? posts.IgnoreQueryFilters() : posts.IgnoreQueryFilters([ignoredFilter]);
        LinkPost post = await posts.Include(post => post.Tags).SingleAsync(ct);
        int[] expected = ignoredFilter switch
        {
            "SoftDeletion" => TenantOneIds,
            "Tenant" => ActiveIds,
            "All" => AllIds,
            _ => TenantOneActiveIds,
        };

        Assert.Equal(expected, post.Tags.OrderBy(tag => tag.Id).Select(tag => tag.Id));
    }

    [Fact]
    public async Task CachedAndCompiledJoinFiltersUseTheExecutingContext()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<LinkContext> options = await CreateDatabaseAsync(stream, ct);
        await using LinkContext first = new(options, 1);
        await using LinkContext second = new(options, 2);
        LinkPost firstPost = await first.Posts.AsNoTracking().Include(post => post.Tags).SingleAsync(ct);
        Assert.Equal(TenantOneActiveIds, firstPost.Tags.Select(tag => tag.Id));
        LinkPost secondPost = await second.Posts.AsNoTracking().Include(post => post.Tags).SingleAsync(ct);
        Assert.Equal(TenantTwoActiveIds, secondPost.Tags.Select(tag => tag.Id));
        first.TenantId = 2;
        LinkPost changedPost = await first.Posts.AsNoTracking().Include(post => post.Tags).SingleAsync(ct);
        Assert.Equal(TenantTwoActiveIds, changedPost.Tags.Select(tag => tag.Id));

        Func<LinkContext, IAsyncEnumerable<LinkPost>> compiled = EF.CompileAsyncQuery(
            (LinkContext context) => context.Posts.AsNoTracking().Include(post => post.Tags));
        await AssertCompiledTagsAsync(compiled, second, TenantTwoActiveIds, ct);
        second.TenantId = 1;
        await AssertCompiledTagsAsync(compiled, second, TenantOneActiveIds, ct);
#pragma warning disable CA1861 // The array must remain inside the compiled query expression.
        Func<LinkContext, IAsyncEnumerable<LinkPost>> compiledNamed = EF.CompileAsyncQuery(
            (LinkContext context) => context.Posts.AsNoTracking().IgnoreQueryFilters(new[] { "SoftDeletion" }).Include(post => post.Tags));
#pragma warning restore CA1861
        await AssertCompiledTagsAsync(compiledNamed, second, TenantOneIds, ct);
    }

    private static async Task AssertCompiledTagsAsync(
        Func<LinkContext, IAsyncEnumerable<LinkPost>> compiled,
        LinkContext context,
        int[] expected,
        CancellationToken cancellationToken)
    {
        List<LinkPost> posts = [];
        await foreach (LinkPost post in compiled(context).WithCancellation(cancellationToken))
        {
            posts.Add(post);
        }

        Assert.Equal(expected, Assert.Single(posts).Tags.OrderBy(tag => tag.Id).Select(tag => tag.Id));
    }

    private static async Task<DbContextOptions<LinkContext>> CreateDatabaseAsync(Stream stream, CancellationToken cancellationToken)
    {
        DbContextOptions<LinkContext> options = new DbContextOptionsBuilder<LinkContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using LinkContext context = new(options, 1);
        await context.Database.EnsureCreatedAsync(cancellationToken);
        context.Add(new LinkPost { Id = 1 });
        for (int id = 1; id <= 4; id++)
        {
            context.Add(new LinkTag { Id = id });
            context.Add(new PostTag { PostId = 1, TagId = id, TenantId = id <= 2 ? 1 : 2, IsDeleted = id % 2 == 0 });
        }

        await context.SaveChangesAsync(cancellationToken);
        return options;
    }

    private sealed class LinkContext(DbContextOptions<LinkContext> options, int tenantId) : DbContext(options)
    {
        public int TenantId { get; set; } = tenantId;

        public DbSet<LinkPost> Posts => this.Set<LinkPost>();

        public DbSet<LinkTag> Tags => this.Set<LinkTag>();

        public DbSet<PostTag> Links => this.Set<PostTag>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LinkPost>().Property(post => post.Id).ValueGeneratedNever();
            modelBuilder.Entity<LinkTag>().Property(tag => tag.Id).ValueGeneratedNever();
            modelBuilder.Entity<LinkPost>().HasMany(post => post.Tags).WithMany(tag => tag.Posts).UsingEntity<PostTag>(
                link => link.HasOne<LinkTag>().WithMany().HasForeignKey(link => link.TagId),
                link => link.HasOne<LinkPost>().WithMany().HasForeignKey(link => link.PostId));
            modelBuilder.Entity<PostTag>().HasKey(link => new { link.PostId, link.TagId });
            modelBuilder.Entity<PostTag>().HasQueryFilter("Tenant", link => link.TenantId == this.TenantId)
                .HasQueryFilter("SoftDeletion", link => !link.IsDeleted);
        }
    }

    private sealed class LinkPost
    {
        public int Id { get; set; }

        public List<LinkTag> Tags { get; set; } = [];
    }

    private sealed class LinkTag
    {
        public int Id { get; set; }

        public List<LinkPost> Posts { get; set; } = [];
    }

    private sealed class PostTag
    {
        public int PostId { get; set; }

        public int TagId { get; set; }

        public int TenantId { get; set; }

        public bool IsDeleted { get; set; }
    }
}
