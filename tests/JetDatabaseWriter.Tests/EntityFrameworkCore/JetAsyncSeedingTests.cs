namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetAsyncSeedingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Seeder_RunsForNewAndExistingDatabases_UsingCurrentContext(bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        List<bool> changedFlags = [];
        SeederContext? expectedContext = null;
        DbContextOptions<SeederContext> options = CreateOptions(stream)
            .UseAsyncSeeding(async (context, changed, token) =>
            {
                Assert.Same(expectedContext, context);
                Assert.Equal(ct, token);
                Assert.NotNull(context.Database.CurrentTransaction);
                changedFlags.Add(changed);
                if (!await context.Items.AnyAsync(token))
                {
                    context.Items.Add(new SeedItem { Id = 1, Name = "callback" });
                    Assert.Equal(1, await context.SaveChangesAsync(token));
                }
            }).Options;
        await using SeederContext context = new(options);
        expectedContext = context;
        if (migrate)
        {
            await context.Database.MigrateAsync(ct);
            await context.Database.MigrateAsync(ct);
        }
        else
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            Assert.False(await context.Database.EnsureCreatedAsync(ct));
        }

        Assert.Equal(new[] { true, false }, changedFlags);
        Assert.Equal("callback", (await context.Items.SingleAsync(ct)).Name);
        Assert.Null(context.Database.CurrentTransaction);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SeederFailure_RollsBackWrites_AndPreservesException(bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        InvalidOperationException failure = new("seed callback failed");
        DbContextOptions<SeederContext> options = CreateOptions(stream)
            .UseAsyncSeeding(async (context, _, token) =>
            {
                context.Items.AddRange(
                    new SeedItem { Id = 1, Name = "first" },
                    new SeedItem { Id = 2, Name = "second" });
                Assert.Equal(2, await context.SaveChangesAsync(token));
                throw failure;
            }).Options;
        await using SeederContext context = new(options);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => CreateOrMigrateAsync(context, migrate, ct)));
        Assert.Null(context.Database.CurrentTransaction);
        context.ChangeTracker.Clear();
        Assert.Empty(await context.Items.ToListAsync(ct));
        await using SeederContext reopened = new(CreateOptions(stream).Options);
        Assert.Empty(await reopened.Items.ToListAsync(ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SeederCancellation_RollsBackWrites_AndPropagatesToken(bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using CancellationTokenSource cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        await using MemoryStream stream = new();
        DbContextOptions<SeederContext> options = CreateOptions(stream)
            .UseAsyncSeeding(async (context, _, token) =>
            {
                Assert.Equal(cancellation.Token, token);
                context.Items.Add(new SeedItem { Id = 1, Name = "cancelled" });
                Assert.Equal(1, await context.SaveChangesAsync(token));
                await cancellation.CancelAsync();
                token.ThrowIfCancellationRequested();
            }).Options;
        await using SeederContext context = new(options);
        OperationCanceledException exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CreateOrMigrateAsync(context, migrate, cancellation.Token));
        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.Null(context.Database.CurrentTransaction);
        context.ChangeTracker.Clear();
        Assert.Empty(await context.Items.ToListAsync(ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncCreation_RejectsSyncOnlySeeder(bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        bool syncCalled = false;
        DbContextOptions<SeederContext> options = CreateOptions(stream)
            .UseSeeding((_, _) => syncCalled = true).Options;
        await using SeederContext context = new(options);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateOrMigrateAsync(context, migrate, ct));
        Assert.Contains("UseAsyncSeeding", exception.Message, StringComparison.Ordinal);
        Assert.False(syncCalled);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Seeder_PreservesCallerTransactionOwnership(bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using (SeederContext initial = new(CreateOptions(stream).Options))
        {
            Assert.True(await initial.Database.EnsureCreatedAsync(ct));
        }

        DbContextOptions<SeederContext> options = CreateOptions(stream)
            .UseAsyncSeeding(async (context, changed, token) =>
            {
                Assert.False(changed);
                context.Items.Add(new SeedItem { Id = 1, Name = "caller transaction" });
                Assert.Equal(1, await context.SaveChangesAsync(token));
            }).Options;
        await using SeederContext context = new(options);
        await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(ct);
        await CreateOrMigrateAsync(context, migrate, ct);
        Assert.Same(transaction, context.Database.CurrentTransaction);
        Assert.Equal(1, await context.Items.CountAsync(ct));
        await transaction.RollbackAsync(ct);
        context.ChangeTracker.Clear();
        Assert.Empty(await context.Items.ToListAsync(ct));
    }

    [Fact]
    public async Task Migrate_ReportsMissingModelSeeds_AsOperationsPerformed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using (SeederContext initial = new(CreateOptions(stream).Options))
        {
            Assert.True(await initial.Database.EnsureCreatedAsync(ct));
        }

        List<bool> changedFlags = [];
        DbContextOptions<ModelSeedContext> options = new DbContextOptionsBuilder<ModelSeedContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .UseAsyncSeeding((_, changed, _) =>
            {
                changedFlags.Add(changed);
                return Task.CompletedTask;
            }).Options;
        await using ModelSeedContext context = new(options);
        await context.Database.MigrateAsync(ct);
        await context.Database.MigrateAsync(ct);
        Assert.Equal(new[] { true, false }, changedFlags);
        Assert.Equal("model seed", (await context.Set<SeedItem>().SingleAsync(ct)).Name);
    }

    [Fact]
    public async Task UntypedSeeder_UsesCurrentContextAndCancellationToken()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContext? observedContext = null;
        Func<DbContext, bool, CancellationToken, Task> seeder = (context, changed, token) =>
        {
            Assert.True(changed);
            Assert.Equal(ct, token);
            observedContext = context;
            return Task.CompletedTask;
        };
        DbContextOptions<SeederContext> options = CreateOptions(stream).UseAsyncSeeding(seeder).Options;
        await using SeederContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Assert.Same(context, observedContext);
    }

    private static DbContextOptionsBuilder<SeederContext> CreateOptions(MemoryStream stream)
        => new DbContextOptionsBuilder<SeederContext>().UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true);

    private static async Task CreateOrMigrateAsync(DbContext context, bool migrate, CancellationToken cancellationToken)
    {
        if (migrate)
        {
            await context.Database.MigrateAsync(cancellationToken);
        }
        else
        {
            _ = await context.Database.EnsureCreatedAsync(cancellationToken);
        }
    }

    private sealed class SeederContext(DbContextOptions<SeederContext> options) : DbContext(options)
    {
        public DbSet<SeedItem> Items => this.Set<SeedItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<SeedItem>(entity =>
            {
                _ = entity.ToTable("SeedItems");
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.Property(x => x.Name).HasMaxLength(40);
            });
    }

    private sealed class ModelSeedContext(DbContextOptions<ModelSeedContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<SeedItem>(entity =>
            {
                _ = entity.ToTable("SeedItems");
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.Property(x => x.Name).HasMaxLength(40);
                _ = entity.HasData(new SeedItem { Id = 8, Name = "model seed" });
            });
    }

    private sealed class SeedItem
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}