namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Xunit;

public sealed class JetSaveDependencyComparerTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BinaryKeys_OrderSelfReferencingInsertsAndDeletes(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using DependencyContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        BinaryNode parent = new() { Id = [1, 0, 0, 0] };
        BinaryNode child = new() { Id = [2, 0, 0, 0], ParentId = [1, 0, 0, 0] };
        Assert.NotSame(parent.Id, child.ParentId);
        context.AddRange(child, parent);

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        BinaryNode[] rows = await context.Set<BinaryNode>().ToArrayAsync(ct);
        BinaryNode loadedParent = rows.Single(static row => row.ParentId is null);
        BinaryNode loadedChild = rows.Single(static row => row.ParentId is not null);
        Assert.NotSame(loadedParent.Id, loadedChild.ParentId);
        context.RemoveRange(loadedParent, loadedChild);
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        Assert.Equal(0, await context.Set<BinaryNode>().CountAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BinaryKeys_DisconnectChildrenBeforeDeletingParents(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using DependencyContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        BinaryNode parent = new() { Id = [1, 0, 0, 0] };
        _ = context.Add(parent);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        BinaryNode child = new() { Id = [2, 0, 0, 0], ParentId = [1, 0, 0, 0] };
        _ = context.Add(child);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        child.ParentId = null;
        _ = context.Remove(parent);

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        BinaryNode remaining = await context.Set<BinaryNode>().SingleAsync(ct);
        Assert.Equal(child.Id, remaining.Id);
        Assert.Null(remaining.ParentId);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CustomKeyComparer_OrdersSelfReferencingInserts(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using DependencyContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        TextNode parent = new() { Id = "ROOT" };
        TextNode child = new() { Id = "CHILD", ParentId = "root" };
        context.AddRange(child, parent);

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        Assert.Equal(2, await context.Set<TextNode>().CountAsync(ct));
    }

    private static DependencyContext CreateContext(Stream stream, DatabaseFormat format)
        => new(new DbContextOptionsBuilder<DependencyContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);

    private sealed class DependencyContext(DbContextOptions<DependencyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<BinaryNode>(entity =>
            {
                _ = entity.Property(row => row.Id).HasMaxLength(4).ValueGeneratedNever();
                _ = entity.Property(row => row.ParentId).HasMaxLength(4);
                _ = entity.HasOne<BinaryNode>().WithMany().HasForeignKey(row => row.ParentId)
                    .OnDelete(DeleteBehavior.ClientNoAction);
            });
            _ = modelBuilder.Entity<TextNode>(entity =>
            {
                _ = entity.Property(row => row.Id).HasMaxLength(32).ValueGeneratedNever();
                entity.Property(row => row.Id).Metadata.SetValueComparer(new ValueComparer<string>(
                    static (left, right) => StringComparer.OrdinalIgnoreCase.Equals(left, right),
                    static value => StringComparer.OrdinalIgnoreCase.GetHashCode(value),
                    static value => value));
                _ = entity.Property(row => row.ParentId).HasMaxLength(32);
                _ = entity.HasOne<TextNode>().WithMany().HasForeignKey(row => row.ParentId)
                    .OnDelete(DeleteBehavior.ClientNoAction);
            });
        }
    }

    private sealed class BinaryNode
    {
        public byte[] Id { get; set; } = [];

        public byte[]? ParentId { get; set; }
    }

    private sealed class TextNode
    {
        public string Id { get; set; } = string.Empty;

        public string? ParentId { get; set; }
    }
}
