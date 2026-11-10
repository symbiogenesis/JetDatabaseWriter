namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetOwnedSeedTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task CreationAndMigration_PreserveOptionalAndNestedOwnedSeeds(DatabaseFormat format, bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<OptionalContext> options = new DbContextOptionsBuilder<OptionalContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using OptionalContext context = new(options);
        if (migrate)
        {
            await context.Database.MigrateAsync(ct);
        }
        else
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
        }

        Owner owner = await context.Set<Owner>().SingleAsync(ct);
        Assert.Equal("owner", owner.Name);
        Assert.NotNull(owner.Detail);
        Assert.Equal("detail", owner.Detail.Note);
        Assert.NotNull(owner.Detail.Nested);
        Assert.Equal("nested", owner.Detail.Nested.Text);
        owner.Detail.Note = "changed";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        await context.Database.MigrateAsync(ct);
        await context.Database.MigrateAsync(ct);
        context.ChangeTracker.Clear();
        owner = await context.Set<Owner>().SingleAsync(ct);
        Assert.Equal("changed", owner.Detail!.Note);
        Assert.Equal("nested", owner.Detail.Nested!.Text);
    }

    [Fact]
    public async Task Creation_PreservesRequiredOwnedSeeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<RequiredContext> options = new DbContextOptionsBuilder<RequiredContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using RequiredContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Owner owner = await context.Set<Owner>().SingleAsync(ct);
        Assert.Equal("detail", owner.Detail!.Note);
    }

    [Fact]
    public async Task MissingOwnerSeed_RefusesWithoutCreatingUserTables()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<OrphanContext> options = new DbContextOptionsBuilder<OrphanContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using OrphanContext context = new(options);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => context.Database.EnsureCreatedAsync(ct));
        Assert.Contains("seed", exception.Message, StringComparison.OrdinalIgnoreCase);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        Assert.Empty(await reader.ListTablesAsync(ct));
    }

    private class SeedContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Owner>(entity =>
            {
                _ = entity.Property(owner => owner.Id).ValueGeneratedNever();
                _ = entity.Property(owner => owner.Name).HasMaxLength(40);
                _ = entity.OwnsOne(owner => owner.Detail, owned =>
                {
                    _ = owned.WithOwner().HasForeignKey("OwnerId");
                    _ = owned.Property(detail => detail.Note).HasMaxLength(40);
                    _ = owned.HasData(new { OwnerId = 1, Note = "detail" });
                    _ = owned.OwnsOne(detail => detail.Nested, nested =>
                    {
                        _ = nested.WithOwner().HasForeignKey("DetailId");
                        _ = nested.Property(value => value.Text).HasMaxLength(40);
                        _ = nested.HasData(new { DetailId = 1, Text = "nested" });
                    });
                });
            });
    }

    private class OptionalContext(DbContextOptions options) : SeedContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            _ = modelBuilder.Entity<Owner>().HasData(new { Id = 1, Name = "owner" });
        }
    }

    private sealed class RequiredContext(DbContextOptions<RequiredContext> options) : OptionalContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            _ = modelBuilder.Entity<Owner>().Navigation(owner => owner.Detail).IsRequired();
            _ = modelBuilder.Entity<Owner>().OwnsOne(owner => owner.Detail).Property(detail => detail.Note).IsRequired();
        }
    }

    private sealed class OrphanContext(DbContextOptions<OrphanContext> options) : SeedContext(options);

#pragma warning disable CA1812 // EF materializes these seed-only entity types.
    private sealed class Owner
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public Detail? Detail { get; set; }
    }

    private sealed class Detail
    {
        public string? Note { get; set; }

        public NestedDetail? Nested { get; set; }
    }

    private sealed class NestedDetail
    {
        public string? Text { get; set; }
    }
#pragma warning restore CA1812
}
