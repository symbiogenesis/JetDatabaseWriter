namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetOrderedStreamingSemanticsTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConvertedOrdering_RetainsNarrowingAndCheckedSelectorBehavior(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderedContext context = await CreateAsync(stream);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;

        int[] actual = await context.Items.OrderBy(item => unchecked((byte)item.Id)).Select(item => item.Id).ToArrayAsync(ct);
        Assert.Equal([256, 1, 258, 3, 4, 5], actual);
        int[] prefix = await context.Items.OrderBy(item => unchecked((byte)item.Id)).Take(2).Select(item => item.Id).ToArrayAsync(ct);
        Assert.Equal([256, 1], prefix);
        await Assert.ThrowsAsync<OverflowException>(() => context.Items.OrderBy(item => checked((byte)item.Id)).Select(item => item.Id).ToArrayAsync(ct));
        await Assert.ThrowsAsync<OverflowException>(() => context.Items.OrderBy(item => checked((byte)item.Id)).Take(1).Select(item => item.Id).ToArrayAsync(ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompositeNullableAndMissingIndexOrders_MatchClrOrdering(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderedContext context = await CreateAsync(stream);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;
        Item[] source = await context.Items.AsNoTracking().ToArrayAsync(ct);

        Assert.Equal(
            source.OrderBy(item => item.Group).ThenByDescending(item => item.Score).Select(item => item.Id),
            await context.Items.OrderBy(item => item.Group).ThenByDescending(item => item.Score).Select(item => item.Id).ToArrayAsync(ct));
        Assert.Equal(
            source.OrderByDescending(item => item.Group).ThenBy(item => item.Score).Select(item => item.Id),
            await context.Items.OrderByDescending(item => item.Group).ThenBy(item => item.Score).Select(item => item.Id).ToArrayAsync(ct));
        Assert.Equal(
            source.Where(item => item.Id != 3).OrderBy(item => item.UnindexedScore).Skip(1).Take(3).Select(item => item.Id),
            await context.Items.Where(item => item.Id != 3).OrderBy(item => item.UnindexedScore).Skip(1).Take(3).Select(item => item.Id).ToArrayAsync(ct));
        IComparer<int> reverse = Comparer<int>.Create(static (left, right) => right.CompareTo(left));
        Assert.Equal(
            source.OrderBy(item => item.Id, reverse).Select(item => item.Id),
            await context.Items.OrderBy(item => item.Id, reverse).Select(item => item.Id).ToArrayAsync(ct));
    }

#pragma warning disable EF1001 // Exercise the raw streaming contract with a projected set that omits the sort property.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FallbackSort_DecodesAnUnprojectedSortProperty(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderedContext context = await CreateAsync(stream);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;
        IJetClientConnection connection = context.GetService<IJetClientConnection>();
        IEntityType entity = context.Model.FindEntityType(typeof(Item))!;
        IProperty id = entity.FindProperty(nameof(Item.Id))!;
        IProperty name = entity.FindProperty(nameof(Item.Name))!;
        List<int> actual = [];
        await foreach (ValueBuffer row in connection.StreamTableValueBuffersAsync(
            entity,
            seekConditions: null,
            parameters: new Dictionary<string, object?>(),
            requiredProperties: new HashSet<IProperty> { id },
            maxRows: 2,
            orderedProperties: [new JetOrdering(name, Descending: false)],
            rowFilter: buffer => (int)buffer[id.GetIndex()]! != 3,
            ct))
        {
            actual.Add((int)row[id.GetIndex()]!);
        }

        Assert.Equal([258, 256], actual);
    }
#pragma warning restore EF1001

    private static async Task<OrderedContext> CreateAsync(Stream stream)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        OrderedContext context = new(new DbContextOptionsBuilder<OrderedContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        await context.Database.EnsureCreatedAsync(ct);
        context.Items.AddRange(
            new Item { Id = 1, Group = 1, Score = 2, UnindexedScore = 60, Name = "f" },
            new Item { Id = 256, Group = null, Score = 4, UnindexedScore = 50, Name = "b" },
            new Item { Id = 258, Group = null, Score = 3, UnindexedScore = 40, Name = "a" },
            new Item { Id = 3, Group = 1, Score = 1, UnindexedScore = 30, Name = "c" },
            new Item { Id = 4, Group = 2, Score = 6, UnindexedScore = 20, Name = "d" },
            new Item { Id = 5, Group = 2, Score = 5, UnindexedScore = 10, Name = "e" });
        await context.SaveChangesAsync(ct);
        context.ChangeTracker.Clear();
        return context;
    }

    private sealed class OrderedContext(DbContextOptions<OrderedContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
            modelBuilder.Entity<Item>().HasIndex(item => new { item.Group, item.Score }).IsDescending(false, true);
        }
    }

    private sealed class Item
    {
        public int Id { get; set; }

        public int? Group { get; set; }

        public int Score { get; set; }

        public int UnindexedScore { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
