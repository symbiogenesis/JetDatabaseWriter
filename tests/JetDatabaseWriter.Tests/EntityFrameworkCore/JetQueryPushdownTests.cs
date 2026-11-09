namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetQueryPushdownTests
{
    [Fact]
    public async Task Skip_Then_Filter_Then_Take_Preserves_Filtered_Limit()
    {
        await using MemoryStream stream = new();
        await using QueryContext context = new(CreateOptions(stream));
        await SeedAsync(context);

        int[] actual = await context.Items.AsNoTracking().Skip(2).Where(x => x.Id > 10).Take(5)
            .Select(x => x.Id).ToArrayAsync(TestContext.Current.CancellationToken);

        Assert.Equal([11, 12, 13, 14, 15], actual);
    }

    [Fact]
    public async Task Take_Then_Filter_Then_First_Scans_The_Inner_Window()
    {
        await using MemoryStream stream = new();
        await using QueryContext context = new(CreateOptions(stream));
        await SeedAsync(context);

        Item? actual = await context.Items.AsNoTracking().Take(10).Where(x => x.Id == 8)
            .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(actual);
        Assert.Equal(8, actual.Id);
    }

    [Fact]
    public async Task Filter_After_Take_Does_Not_Seek_Outside_The_Window()
    {
        await using MemoryStream stream = new();
        await using QueryContext context = new(CreateOptions(stream));
        await SeedAsync(context);

        int[] actual = await context.Items.AsNoTracking().Take(5).Where(x => x.Id > 10)
            .Select(x => x.Id).ToArrayAsync(TestContext.Current.CancellationToken);

        Assert.Empty(actual);
    }

    [Fact]
    public async Task Descending_Ordered_Limit_With_Residual_Filter_Preserves_Order()
    {
        await using MemoryStream stream = new();
        await using QueryContext context = new(CreateOptions(stream));
        await SeedAsync(context);

        int[] actual = await context.Items.AsNoTracking().Where(x => x.Active).OrderByDescending(x => x.Id)
            .Take(4).Select(x => x.Id).ToArrayAsync(TestContext.Current.CancellationToken);

        Assert.Equal([20, 18, 16, 14], actual);
    }

    [Fact]
    public async Task Three_Table_Join_With_Post_Join_Filter_And_Inner_Filter_Works()
    {
        await using MemoryStream stream = new();
        await using QueryContext context = new(CreateOptions(stream));
        await SeedAsync(context);

        var query = context.Items.AsNoTracking()
            .Join(
                context.Groups.Where(group => group.Active),
                item => item.GroupId,
                group => group.Id,
                (item, group) => new { item, group })
            .Join(
                context.Categories,
                pair => pair.group.CategoryId,
                category => category.Id,
                (pair, category) => new { pair.item, pair.group, category })
            .Where(pair => pair.item.Id > 15 && pair.category.Id == 2)
            .OrderBy(pair => pair.item.Id)
            .Select(pair => new { pair.item.Id, Group = pair.group.Name, Category = pair.category.Name });

        var actual = await query.ToArrayAsync(TestContext.Current.CancellationToken);

        Assert.Equal([16, 18, 20], actual.Select(row => row.Id));
        Assert.All(actual, row => Assert.Equal("Second", row.Category));
    }

    [Fact]
    public async Task Left_Join_Preserves_Unmatched_Rows_And_Post_Join_Null_Filter()
    {
        await using MemoryStream stream = new();
        await using QueryContext context = new(CreateOptions(stream));
        await SeedAsync(context);

        int[] actual = await context.Items.AsNoTracking()
            .LeftJoin(
                context.Groups.Where(group => group.Id == 2),
                item => item.GroupId,
                group => group.Id,
                (item, group) => new { item, group })
            .Where(pair => pair.group == null)
            .OrderBy(pair => pair.item.Id)
            .Select(pair => pair.item.Id)
            .ToArrayAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 20).Where(id => id % 2 != 0), actual);
    }

    [Fact]
    public async Task Navigation_Projection_Preserves_Root_Filter_And_Related_Keys()
    {
        await using MemoryStream stream = new();
        await using QueryContext context = new(CreateOptions(stream));
        await SeedAsync(context);

        var actual = await context.Items.AsNoTracking().Where(item => item.Id > 18 && item.Group.Active)
            .OrderBy(item => item.Id)
            .Select(item => new { item.Id, item.Group.Name, Category = item.Group.Category.Name })
            .ToArrayAsync(TestContext.Current.CancellationToken);

        var row = Assert.Single(actual);
        Assert.Equal(20, row.Id);
        Assert.Equal("Even", row.Name);
        Assert.Equal("Second", row.Category);
    }

#pragma warning disable EF1001 // Verify the provider's internal index scan contract directly.
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task Snapshot_Index_Scan_Reverses_Primary_Key_And_Honors_Mixed_Directions(DatabaseFormat format)
    {
        await using MemoryStream stream = new();
        DbContextOptions<QueryContext> options = new DbContextOptionsBuilder<QueryContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using QueryContext context = new(options);
        await SeedAsync(context);
        await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(TestContext.Current.CancellationToken);
        IJetClientConnection connection = context.GetService<IJetClientConnection>();
        IEntityType entityType = context.Model.FindEntityType(typeof(Item))!;
        IProperty id = entityType.FindProperty(nameof(Item.Id))!;
        IProperty group = entityType.FindProperty(nameof(Item.GroupId))!;
        IReadOnlyDictionary<string, object?> parameters = new Dictionary<string, object?>();
        IReadOnlySet<IProperty> properties = new HashSet<IProperty> { id, group };

        IReadOnlyList<ValueBuffer> reversed = await connection.ReadTableValueBuffersAsync(
            entityType,
            seekConditions: null,
            parameters,
            properties,
            maxRows: 3,
            [new JetOrdering(id, Descending: true)],
            rowFilter: null,
            TestContext.Current.CancellationToken);
        Assert.Equal([20, 19, 18], reversed.Select(buffer => (int)buffer[id.GetIndex()]!));

        IReadOnlyList<ValueBuffer> mixed = await connection.ReadTableValueBuffersAsync(
            entityType,
            seekConditions: null,
            parameters,
            properties,
            maxRows: 4,
            [new JetOrdering(group, Descending: false), new JetOrdering(id, Descending: true)],
            rowFilter: null,
            TestContext.Current.CancellationToken);
        Assert.Equal([19, 17, 15, 13], mixed.Select(buffer => (int)buffer[id.GetIndex()]!));
    }
#pragma warning restore EF1001

    private static DbContextOptions<QueryContext> CreateOptions(Stream stream)
        => new DbContextOptionsBuilder<QueryContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;

    private static async Task SeedAsync(QueryContext context)
    {
        _ = await context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken);
        context.Categories.AddRange(new Category { Id = 1, Name = "First" }, new Category { Id = 2, Name = "Second" });
        context.Groups.AddRange(new Group { Id = 1, Name = "Odd", CategoryId = 1 }, new Group { Id = 2, Name = "Even", Active = true, CategoryId = 2 });
        context.Items.AddRange(Enumerable.Range(1, 20).Select(id => new Item { Id = id, Active = id % 2 == 0, GroupId = id % 2 == 0 ? 2 : 1 }));
        _ = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        context.ChangeTracker.Clear();
    }

    private sealed class QueryContext(DbContextOptions<QueryContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        public DbSet<Group> Groups => this.Set<Group>();

        public DbSet<Category> Categories => this.Set<Category>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>().HasKey(x => x.Id);
            modelBuilder.Entity<Item>().Property(x => x.Id).ValueGeneratedNever();
            modelBuilder.Entity<Item>().HasOne(x => x.Group).WithMany().HasForeignKey(x => x.GroupId);
            modelBuilder.Entity<Item>().HasIndex(x => new { x.GroupId, x.Id }).IsDescending(false, true);
            modelBuilder.Entity<Group>().Property(x => x.Id).ValueGeneratedNever();
            modelBuilder.Entity<Group>().HasOne(x => x.Category).WithMany().HasForeignKey(x => x.CategoryId);
            modelBuilder.Entity<Category>().Property(x => x.Id).ValueGeneratedNever();
        }
    }

    private sealed class Item
    {
        public int Id { get; set; }

        public bool Active { get; set; }

        public int GroupId { get; set; }

        public Group Group { get; set; } = null!;
    }

    private sealed class Group
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public bool Active { get; set; }

        public int CategoryId { get; set; }

        public Category Category { get; set; } = null!;
    }

    private sealed class Category
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
