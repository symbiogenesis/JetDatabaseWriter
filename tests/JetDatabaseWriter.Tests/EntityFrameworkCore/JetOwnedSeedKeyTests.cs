namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Xunit;

public sealed class JetOwnedSeedKeyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnedSeeds_MatchInheritedCompositeBinaryConvertedAndCustomComparerKeys(bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CompositeContext> options = new DbContextOptionsBuilder<CompositeContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (CompositeContext context = new(options))
        {
            if (migrate)
            {
                await context.Database.MigrateAsync(ct);
            }
            else
            {
                Assert.True(await context.Database.EnsureCreatedAsync(ct));
            }

            await context.Database.MigrateAsync(ct);
            await context.Database.MigrateAsync(ct);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        using DataTable table = await reader.ReadTableAsync("OwnedSeedKeys", cancellationToken: ct);
        DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal("owner", row["Id"]);
        Assert.Equal("7", row["Partition"]);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, Assert.IsType<byte[]>(row["Stamp"]));
        Assert.Equal("owned", row["Detail_Note"]);
        Assert.Equal("derived", row["Extra"]);
    }

    [Fact]
    public async Task ConflictingSharedSeedColumns_RefuseWithoutCreatingUserTables()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<ConflictContext> options = new DbContextOptionsBuilder<ConflictContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using ConflictContext context = new(options);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => context.Database.EnsureCreatedAsync(ct));
        Assert.Contains("conflicting", exception.Message, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        Assert.Empty(await reader.ListTablesAsync(ct));
    }

    [Fact]
    public async Task SiblingOwnedSeeds_ResolveTheirDerivedPrincipalTypes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<SiblingContext> options = new DbContextOptionsBuilder<SiblingContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using SiblingContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        LeftOwner left = await context.Set<LeftOwner>().SingleAsync(ct);
        RightOwner right = await context.Set<RightOwner>().SingleAsync(ct);
        Assert.Equal("left", left.Left!.Note);
        Assert.Equal("right", right.Right!.Note);
    }

    private sealed class CompositeContext(DbContextOptions<CompositeContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<CompositeOwner>(entity =>
            {
                _ = entity.ToTable("OwnedSeedKeys");
                _ = entity.HasKey(owner => new { owner.Id, owner.Partition, owner.Stamp });
                _ = entity.Property(owner => owner.Id).HasMaxLength(40).ValueGeneratedNever();
                entity.Property(owner => owner.Id).Metadata.SetValueComparer(new ValueComparer<string>(
                    static (left, right) => string.Equals(left, right, StringComparison.OrdinalIgnoreCase),
                    static value => StringComparer.OrdinalIgnoreCase.GetHashCode(value),
                    static value => value));
                _ = entity.Property(owner => owner.Partition).HasConversion<string>().HasMaxLength(20).ValueGeneratedNever();
                _ = entity.Property(owner => owner.Stamp).HasMaxLength(4).ValueGeneratedNever();
                _ = entity.OwnsOne(owner => owner.Detail, owned =>
                {
                    _ = owned.WithOwner().HasForeignKey("OwnerId", "OwnerPartition", "OwnerStamp");
                    _ = owned.Property<string>("OwnerId").HasMaxLength(40);
                    _ = owned.Property<int>("OwnerPartition").HasConversion<string>().HasMaxLength(20);
                    _ = owned.Property<byte[]>("OwnerStamp").HasMaxLength(4);
                    _ = owned.Property(detail => detail.Note).HasMaxLength(40);
                    _ = owned.HasData(new { OwnerId = "OWNER", OwnerPartition = 7, OwnerStamp = new byte[] { 1, 2, 3, 4 }, Note = "owned" });
                });
            });
            _ = modelBuilder.Entity<DerivedOwner>(entity =>
            {
                _ = entity.Property(owner => owner.Extra).HasMaxLength(40);
                _ = entity.HasData(new { Id = "owner", Partition = 7, Stamp = new byte[] { 1, 2, 3, 4 }, Extra = "derived" });
            });
        }
    }

    private sealed class ConflictContext(DbContextOptions<ConflictContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<TokenOwner>(entity =>
            {
                _ = entity.Property(owner => owner.Id).ValueGeneratedNever();
                _ = entity.Property(owner => owner.Token).IsConcurrencyToken();
                _ = entity.HasData(new { Id = 1, Token = 1 });
                _ = entity.OwnsOne(owner => owner.Detail, owned =>
                {
                    _ = owned.WithOwner().HasForeignKey("OwnerId");
                    _ = owned.Property(detail => detail.Token).HasColumnName("Token").IsConcurrencyToken();
                    _ = owned.HasData(new { OwnerId = 1, Token = 2 });
                });
            });
    }

    private sealed class SiblingContext(DbContextOptions<SiblingContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<SiblingOwner>().Property(owner => owner.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<LeftOwner>(entity =>
            {
                _ = entity.HasData(new { Id = 1 });
                _ = entity.OwnsOne(owner => owner.Left, owned =>
                {
                    _ = owned.WithOwner().HasForeignKey("OwnerId");
                    _ = owned.Property(detail => detail.Note).HasMaxLength(40);
                    _ = owned.HasData(new { OwnerId = 1, Note = "left" });
                });
            });
            _ = modelBuilder.Entity<RightOwner>(entity =>
            {
                _ = entity.HasData(new { Id = 2 });
                _ = entity.OwnsOne(owner => owner.Right, owned =>
                {
                    _ = owned.WithOwner().HasForeignKey("OwnerId");
                    _ = owned.Property(detail => detail.Note).HasMaxLength(40);
                    _ = owned.HasData(new { OwnerId = 2, Note = "right" });
                });
            });
        }
    }

#pragma warning disable CA1812 // EF materializes these seed-only entity types.
    private class CompositeOwner
    {
        public string Id { get; set; } = string.Empty;

        public int Partition { get; set; }

        public byte[] Stamp { get; set; } = [];

        public SeedDetail? Detail { get; set; }
    }

    private sealed class DerivedOwner : CompositeOwner
    {
        public string? Extra { get; set; }
    }

    private sealed class SeedDetail
    {
        public string? Note { get; set; }
    }

    private sealed class TokenOwner
    {
        public int Id { get; set; }

        public int Token { get; set; }

        public TokenDetail? Detail { get; set; }
    }

    private sealed class TokenDetail
    {
        public int Token { get; set; }
    }

    private class SiblingOwner
    {
        public int Id { get; set; }
    }

    private sealed class LeftOwner : SiblingOwner
    {
        public SeedDetail? Left { get; set; }
    }

    private sealed class RightOwner : SiblingOwner
    {
        public SeedDetail? Right { get; set; }
    }
#pragma warning restore CA1812
}
