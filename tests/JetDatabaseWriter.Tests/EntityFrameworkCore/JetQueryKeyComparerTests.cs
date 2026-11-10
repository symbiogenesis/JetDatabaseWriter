namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Diagnostics;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Xunit;

public sealed class JetQueryKeyComparerTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BinaryCompositeKeys_MatchReferenceAndCollectionIncludes(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        await using KeyContext context = await CreateAsync(stream, messages);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;
        BinaryOwner owner = await context.Set<BinaryOwner>().AsNoTracking().Include(row => row.Children).SingleAsync(ct);
        Assert.Equal(2, owner.Children.Count);
        Assert.All(owner.Children, child => Assert.Same(owner, child.Owner));
        BinaryChild[] children = await context.Set<BinaryChild>().AsNoTrackingWithIdentityResolution().Include(row => row.Owner).OrderBy(row => row.Id).ToArrayAsync(ct);
        Assert.NotNull(children[0].Owner);
        Assert.Same(children[0].Owner, children[1].Owner);
        Assert.Contains(messages, message => message.Contains("IndexSeek", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(QueryTrackingBehavior.TrackAll)]
    [InlineData(QueryTrackingBehavior.NoTrackingWithIdentityResolution)]
    public async Task RepeatedBinaryKeys_ResolveOneEntityInJoinedProjection(QueryTrackingBehavior tracking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = await CreateAsync(stream);
        context.ChangeTracker.QueryTrackingBehavior = tracking;
        BinaryOwner[] owners = await context.Set<BinaryChild>().Join(context.Set<BinaryOwner>(), child => child.OwnerPartition, owner => owner.Partition, (_, owner) => owner).ToArrayAsync(ct);
        Assert.Equal(2, owners.Length);
        Assert.Same(owners[0], owners[1]);
        if (tracking == QueryTrackingBehavior.TrackAll)
        {
            Assert.Single(context.ChangeTracker.Entries<BinaryOwner>());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CustomKeyComparer_KeyLoadsDoNotPruneEquivalentNativeKeys(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = await CreateAsync(stream);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;
        IEntityType entityType = context.Model.FindEntityType(typeof(CustomOwner))!;
        IReadOnlyList<IProperty> properties = entityType.FindPrimaryKey()!.Properties;
#pragma warning disable EF1001 // Exercise the key-read admission and fallback paths directly.
        IReadOnlyList<ValueBuffer> rows = await context.GetService<IJetClientConnection>().ReadTableValueBuffersByKeysAsync(entityType, properties, [[11]], cancellationToken: ct);
#pragma warning restore EF1001
        Assert.Equal(1, Assert.Single(rows)[properties[0].GetIndex()]);
    }

    [Theory]
    [InlineData(QueryTrackingBehavior.TrackAll)]
    [InlineData(QueryTrackingBehavior.NoTrackingWithIdentityResolution)]
    public async Task CustomKeyComparer_ResolvesEquivalentStoredKeys(QueryTrackingBehavior tracking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = await CreateAsync(stream);
        context.Add(new CustomOwner { Id = 11 });
        _ = await context.SaveChangesAsync(ct);
        context.ChangeTracker.Clear();
        context.ChangeTracker.QueryTrackingBehavior = tracking;
        CustomOwner[] owners = await context.Set<CustomOwner>().ToArrayAsync(ct);
        Assert.Equal(2, owners.Length);
        Assert.Same(owners[0], owners[1]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CustomPrincipalComparer_MatchesForeignKeysUsingPrincipalSemantics(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = await CreateAsync(stream);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;
        TextOwner owner = await context.Set<TextOwner>().AsNoTracking().Include(row => row.Children).SingleAsync(ct);
        Assert.Single(owner.Children);
        TextChild child = await context.Set<TextChild>().AsNoTracking().Include(row => row.Owner).SingleAsync(ct);
        Assert.NotNull(child.Owner);
        Assert.Equal("principal", child.Owner.Id);
    }

    [Fact]
    public async Task JoinPrefetch_PreservesClrEqualityWhenMappedComparerIsBroader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = await CreateAsync(stream);
        int[] matches = await context.Set<CustomJoinOuter>().Join(context.Set<CustomJoinInner>(), outer => outer.Number, inner => inner.Id, (_, inner) => inner.Id).ToArrayAsync(ct);
        Assert.Equal([11], matches);
    }

#pragma warning disable EF1001 // Verify retained key snapshots against caller mutation.
    [Fact]
    public async Task KeySnapshots_RetainBinaryValuesAndComparerHashesAfterCallerMutation()
    {
        await using MemoryStream stream = new();
        await using KeyContext context = await CreateAsync(stream);
        IReadOnlyList<IProperty> properties = context.Model.FindEntityType(typeof(BinaryOwner))!.FindPrimaryKey()!.Properties;
        byte[] bytes = [1, 2, 3, 4];
        object?[] values = [bytes, 7];
        JetKeyTuple key = new(properties, values);
        HashSet<JetKeyTuple> keys = [key];
        bytes[0] = 9;
        values[1] = 8;
        Assert.Contains(new JetKeyTuple(properties, [new byte[] { 1, 2, 3, 4 }, 7]), keys);
        Assert.DoesNotContain(new JetKeyTuple(properties, [bytes, 7]), keys);
        IReadOnlyList<IProperty> customProperties = context.Model.FindEntityType(typeof(CustomOwner))!.FindPrimaryKey()!.Properties;
        JetKeyTuple first = new(customProperties, [1]);
        JetKeyTuple equivalent = new(customProperties, [11]);
        Assert.Equal(first, equivalent);
        Assert.Equal(first.GetHashCode(), equivalent.GetHashCode());
    }

#pragma warning restore EF1001

    private static async Task<KeyContext> CreateAsync(MemoryStream stream, ConcurrentQueue<string>? messages = null)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        DbContextOptionsBuilder<KeyContext> builder = new DbContextOptionsBuilder<KeyContext>().UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true);
        if (messages is not null)
        {
            _ = builder.LogTo(messages.Enqueue, [JetEventId.TableRead], LogLevel.Debug);
        }

        DbContextOptions<KeyContext> options = builder.Options;
        await using (KeyContext seed = new(options))
        {
            _ = await seed.Database.EnsureCreatedAsync(ct);
            seed.Add(new BinaryOwner { Id = [1, 2, 3, 4], Partition = 7 });
            seed.AddRange(new BinaryChild { Id = 1, OwnerId = [1, 2, 3, 4], OwnerPartition = 7 }, new BinaryChild { Id = 2, OwnerId = [1, 2, 3, 4], OwnerPartition = 7 });
            seed.Add(new CustomOwner { Id = 1 });
            seed.Add(new TextOwner { Id = "principal" });
            seed.Add(new TextChild { Id = 1, OwnerId = "PRINCIPAL" });
            seed.AddRange(new CustomJoinOuter { Id = 1, Number = 1 }, new CustomJoinOuter { Id = 2, Number = 11 });
            seed.Add(new CustomJoinInner { Id = 11 });
            _ = await seed.SaveChangesAsync(ct);
        }

        return new KeyContext(options);
    }

    private sealed class KeyContext(DbContextOptions<KeyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<BinaryOwner>().HasKey(row => new { row.Id, row.Partition });
            _ = modelBuilder.Entity<BinaryOwner>().Property(row => row.Id).HasMaxLength(4).ValueGeneratedNever();
            _ = modelBuilder.Entity<BinaryChild>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<BinaryChild>().Property(row => row.OwnerId).HasMaxLength(4);
            _ = modelBuilder.Entity<BinaryOwner>().HasMany(row => row.Children).WithOne(row => row.Owner).HasForeignKey(row => new { row.OwnerId, row.OwnerPartition });
            _ = modelBuilder.Entity<CustomOwner>().Property(row => row.Id).ValueGeneratedNever();
            modelBuilder.Entity<CustomOwner>().Property(row => row.Id).Metadata.SetValueComparer(new ValueComparer<int>((left, right) => left % 10 == right % 10, value => value % 10, value => value));
            _ = modelBuilder.Entity<TextOwner>().Property(row => row.Id).HasMaxLength(30).ValueGeneratedNever();
            modelBuilder.Entity<TextOwner>().Property(row => row.Id).Metadata.SetValueComparer(new ValueComparer<string>((left, right) => StringComparer.OrdinalIgnoreCase.Equals(left, right), value => StringComparer.OrdinalIgnoreCase.GetHashCode(value), value => value));
            _ = modelBuilder.Entity<TextChild>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<TextChild>().Property(row => row.OwnerId).HasMaxLength(30);
            modelBuilder.Entity<TextChild>().Property(row => row.OwnerId).Metadata.SetValueComparer(new ValueComparer<string>((left, right) => StringComparer.Ordinal.Equals(left, right), value => StringComparer.Ordinal.GetHashCode(value), value => value));
            _ = modelBuilder.Entity<TextOwner>().HasMany(row => row.Children).WithOne(row => row.Owner).HasForeignKey(row => row.OwnerId);
            _ = modelBuilder.Entity<CustomJoinOuter>().Property(row => row.Id).ValueGeneratedNever();
            modelBuilder.Entity<CustomJoinOuter>().Property(row => row.Number).Metadata.SetValueComparer(new ValueComparer<int>((left, right) => left % 10 == right % 10, value => value % 10, value => value));
            _ = modelBuilder.Entity<CustomJoinInner>().Property(row => row.Id).ValueGeneratedNever();
        }
    }

    private sealed class BinaryOwner
    {
        public byte[] Id { get; set; } = [];

        public int Partition { get; set; }

        public List<BinaryChild> Children { get; set; } = [];
    }

    private sealed class BinaryChild
    {
        public int Id { get; set; }

        public byte[] OwnerId { get; set; } = [];

        public int OwnerPartition { get; set; }

        public BinaryOwner? Owner { get; set; }
    }

    private sealed class CustomOwner
    {
        public int Id { get; set; }
    }

    private sealed class TextOwner
    {
        public string Id { get; set; } = string.Empty;

        public List<TextChild> Children { get; set; } = [];
    }

    private sealed class TextChild
    {
        public int Id { get; set; }

        public string OwnerId { get; set; } = string.Empty;

        public TextOwner? Owner { get; set; }
    }

    private sealed class CustomJoinOuter
    {
        public int Id { get; set; }

        public int Number { get; set; }
    }

    private sealed class CustomJoinInner
    {
        public int Id { get; set; }
    }
}
