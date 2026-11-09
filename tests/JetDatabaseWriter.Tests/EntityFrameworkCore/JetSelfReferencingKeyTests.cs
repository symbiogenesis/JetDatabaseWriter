namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetSelfReferencingKeyTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Inserts_OrderEverySelfReferenceByItsActualPrincipalKey(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using NodeContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Node root = new() { Id = 1, Code = "ROOT", Partition = 10, Number = 1 };
        Node middle = new() { Id = 2, Code = "MIDDLE", Partition = 10, Number = 2, ParentCode = root.Code };
        Node leaf = new()
        {
            Id = 3,
            Code = "LEAF",
            Partition = 10,
            Number = 3,
            AlternateParentPartition = middle.Partition,
            AlternateParentNumber = middle.Number,
        };
        context.AddRange(leaf, middle, root);

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        Assert.Equal(3, await context.Set<Node>().CountAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Deletes_OrderEverySelfReferenceByItsOriginalPrincipalKey(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using NodeContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Node root = new() { Id = 1, Code = "ROOT", Partition = 10, Number = 1 };
        _ = context.Add(root);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Node middle = new() { Id = 2, Code = "MIDDLE", Partition = 10, Number = 2, ParentCode = root.Code };
        _ = context.Add(middle);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Node leaf = new()
        {
            Id = 3,
            Code = "LEAF",
            Partition = 10,
            Number = 3,
            AlternateParentPartition = middle.Partition,
            AlternateParentNumber = middle.Number,
        };
        _ = context.Add(leaf);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.RemoveRange(root, middle, leaf);

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        Assert.Equal(0, await context.Set<Node>().CountAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ForeignKeyChanges_PrecedeDeletionOfTheReferencedAlternateKeys(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using NodeContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Node root = new() { Id = 1, Code = "ROOT", Partition = 10, Number = 1 };
        _ = context.Add(root);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Node child = new()
        {
            Id = 2,
            Code = "CHILD",
            Partition = 10,
            Number = 2,
            ParentCode = root.Code,
            AlternateParentPartition = root.Partition,
            AlternateParentNumber = root.Number,
        };
        _ = context.Add(child);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        _ = context.Remove(root);
        child.ParentCode = null;
        child.AlternateParentPartition = null;
        child.AlternateParentNumber = null;

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Node survivingChild = await context.Set<Node>().SingleAsync(ct);
        Assert.Equal(child.Id, survivingChild.Id);
        Assert.Null(survivingChild.ParentCode);
        Assert.Null(survivingChild.AlternateParentPartition);
        Assert.Null(survivingChild.AlternateParentNumber);
    }

    private static NodeContext CreateContext(Stream stream, DatabaseFormat format)
        => new(new DbContextOptionsBuilder<NodeContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);

    private sealed class NodeContext(DbContextOptions<NodeContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<Node>(entity =>
            {
                _ = entity.Property(node => node.Id).ValueGeneratedNever();
                _ = entity.Property(node => node.Code).HasMaxLength(32);
                _ = entity.Property(node => node.ParentCode).HasMaxLength(32);
                _ = entity.HasAlternateKey(node => node.Code);
                _ = entity.HasAlternateKey(node => new { node.Partition, node.Number });
                _ = entity.HasOne<Node>().WithMany().HasForeignKey(node => node.ParentCode)
                    .HasPrincipalKey(node => node.Code).OnDelete(DeleteBehavior.ClientNoAction);
                _ = entity.HasOne<Node>().WithMany()
                    .HasForeignKey(node => new { node.AlternateParentPartition, node.AlternateParentNumber })
                    .HasPrincipalKey(node => new { node.Partition, node.Number }).OnDelete(DeleteBehavior.ClientNoAction);
            });
    }

    private sealed class Node
    {
        public int Id { get; set; }

        public string Code { get; set; } = string.Empty;

        public int Partition { get; set; }

        public int Number { get; set; }

        public string? ParentCode { get; set; }

        public int? AlternateParentPartition { get; set; }

        public int? AlternateParentNumber { get; set; }
    }
}
