namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetEntityIdentityComparisonTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EntityOperators_CompareTrackedAndDetachedPrimaryKeys(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using EqualityContext context = await CreateContextAsync(stream, format);
        OrdinaryRow tracked = await context.Set<OrdinaryRow>().SingleAsync(x => x.Id == 1, ct);
        OrdinaryRow detached = new() { Id = 1 };
        Assert.Equal(1, await context.Set<OrdinaryRow>().CountAsync(x => x == tracked, ct));
        Assert.Equal(1, await context.Set<OrdinaryRow>().CountAsync(x => detached == x, ct));
        Assert.Equal(1, await context.Set<OrdinaryRow>().CountAsync(x => x != detached, ct));
        Assert.True(await context.Set<OrdinaryRow>().ContainsAsync(detached, ct));
        OrdinaryRow? missing = null;
        Assert.Equal(0, await context.Set<OrdinaryRow>().CountAsync(x => x == missing, ct));
        Assert.Equal(2, await context.Set<OrdinaryRow>().CountAsync(x => x != missing, ct));
        Assert.Single(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task CompositeBinaryKeys_UseStructuralComparisonForOperatorsAndContains()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using EqualityContext context = await CreateContextAsync(stream, DatabaseFormat.AceAccdb);
        BinaryRow detached = new() { Id = [1, 2, 3, 4], Partition = 7 };
        Assert.Equal(1, await context.Set<BinaryRow>().CountAsync(x => x == detached, ct));
        Assert.True(await context.Set<BinaryRow>().ContainsAsync(detached, ct));
        BinaryRow wrongPartition = new() { Id = [1, 2, 3, 4], Partition = 8 };
        Assert.False(await context.Set<BinaryRow>().ContainsAsync(wrongPartition, ct));
        Assert.Equal(1, await context.Set<BinaryRow>().CountAsync(x => x != wrongPartition, ct));
        Assert.Empty(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task ShadowKeys_UseTrackedAndCandidateValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using EqualityContext context = await CreateContextAsync(stream, DatabaseFormat.AceAccdb);
        ShadowRow tracked = await context.Set<ShadowRow>().SingleAsync(ct);
        Assert.Equal(1, await context.Set<ShadowRow>().CountAsync(x => x == tracked, ct));
        Assert.True(await context.Set<ShadowRow>().ContainsAsync(tracked, ct));
        Assert.Single(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task InheritanceKeys_CompareWithinTheirHierarchy()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using EqualityContext context = await CreateContextAsync(stream, DatabaseFormat.AceAccdb);
        DerivedRow detached = new() { Id = 2 };
        Assert.Equal(1, await context.Set<BaseRow>().CountAsync(x => x == detached, ct));
        Assert.Equal(1, await context.Set<DerivedRow>().CountAsync(x => x == detached, ct));
        OrdinaryRow otherHierarchy = new() { Id = 2 };
#pragma warning disable IDE0004 // Object conversions allow comparing entities from different mapped hierarchies.
        Assert.Equal(0, await context.Set<BaseRow>().CountAsync(x => (object)x == (object)otherHierarchy, ct));
#pragma warning restore IDE0004
        Assert.Empty(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task KeylessEquality_RefusesWithAnExplicitDiagnostic()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using EqualityContext context = await CreateContextAsync(stream, DatabaseFormat.AceAccdb);
        KeylessRow detached = new() { Name = "value" };
        NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
            context.Set<KeylessRow>().CountAsync(x => x == detached, ct));
        Assert.Contains("primary key", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<EqualityContext> CreateContextAsync(MemoryStream stream, DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        EqualityContext context = new(new DbContextOptionsBuilder<EqualityContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);
        try
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            context.AddRange(new OrdinaryRow { Id = 1 }, new OrdinaryRow { Id = 2 });
            context.Add(new BinaryRow { Id = [1, 2, 3, 4], Partition = 7 });
            ShadowRow shadow = new() { Name = "shadow" };
            context.Add(shadow);
            context.Entry(shadow).Property<int>("Id").CurrentValue = 7;
            context.AddRange(new BaseRow { Id = 1 }, new DerivedRow { Id = 2, Detail = "derived" });
            Assert.Equal(6, await context.SaveChangesAsync(ct));
            context.ChangeTracker.Clear();
            return context;
        }
        catch
        {
            await context.DisposeAsync();
            throw;
        }
    }

    private sealed class EqualityContext(DbContextOptions<EqualityContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<OrdinaryRow>().Property(x => x.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<BinaryRow>(entity =>
            {
                _ = entity.HasKey(x => new { x.Id, x.Partition });
                _ = entity.Property(x => x.Id).HasMaxLength(4).ValueGeneratedNever();
            });
            _ = modelBuilder.Entity<ShadowRow>().Property<int>("Id").ValueGeneratedNever();
            _ = modelBuilder.Entity<ShadowRow>().HasKey("Id");
            _ = modelBuilder.Entity<BaseRow>().Property(x => x.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<DerivedRow>();
            _ = modelBuilder.Entity<KeylessRow>().HasNoKey();
        }
    }

    private sealed class OrdinaryRow
    {
        public int Id { get; set; }
    }

    private sealed class BinaryRow
    {
        public byte[] Id { get; set; } = [];

        public int Partition { get; set; }
    }

    private sealed class ShadowRow
    {
        public string Name { get; set; } = string.Empty;
    }

    private class BaseRow
    {
        public int Id { get; set; }
    }

    private sealed class DerivedRow : BaseRow
    {
        public string Detail { get; set; } = string.Empty;
    }

    private sealed class KeylessRow
    {
        public string Name { get; set; } = string.Empty;
    }
}
