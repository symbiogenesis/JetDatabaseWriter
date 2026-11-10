namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.TestSupport;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetWorkloadFixtureTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BenchmarkWorkload_QueriesAndMutations_PreserveExpectedRows(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<WorkloadContext> options = new DbContextOptionsBuilder<WorkloadContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (WorkloadContext schema = new(options))
        {
            Assert.True(await schema.Database.EnsureCreatedAsync(ct));
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, leaveOpen: true, cancellationToken: ct))
        {
            _ = await writer.InsertRowsAsync(EfWorkloadData.ParentTable, EfWorkloadData.ParentRows(100).Select(values => new RowValues(values)), ct);
            _ = await writer.InsertRowsAsync(EfWorkloadData.ChildTable, EfWorkloadData.ChildRows(100).Select(values => new RowValues(values)), ct);
        }

        await using WorkloadContext context = new(options);
        int[] ids = await context.Parents.AsNoTracking().OrderBy(parent => parent.Id)
            .Take(EfWorkloadData.PageSize).Select(parent => parent.Id).ToArrayAsync(ct);
        Assert.Equal(Enumerable.Range(1, EfWorkloadData.PageSize), ids);
        Parent[] tracked = await context.Parents.OrderBy(parent => parent.Id).Take(EfWorkloadData.PageSize).ToArrayAsync(ct);
        Assert.All(tracked, parent => Assert.Equal(EfWorkloadData.DetailText, parent.Detail?.Note));
        Assert.Equal(EfWorkloadData.PageSize, context.ChangeTracker.Entries<Parent>().Count());
        context.ChangeTracker.Clear();

        Parent[] graph = await context.Parents.AsNoTracking().OrderBy(parent => parent.Id)
            .Take(EfWorkloadData.PageSize).Include(parent => parent.Children).ToArrayAsync(ct);
        Assert.Equal(EfWorkloadData.PageSize, graph.Length);
        EfWorkloadData.VerifyPage(graph.Select(parent => (parent.Id, parent.Score, parent.Detail?.Note, parent.Children.Count)), withChildren: true);
        Assert.All(graph, parent =>
        {
            Assert.Equal(EfWorkloadData.ChildrenPerParent, parent.Children.Count);
            Assert.All(parent.Children, child => Assert.Equal(parent.Id, child.ParentId));
        });
        Guid lookup = EfWorkloadData.ChildKey(2);
        Assert.Equal(1, await context.Children.Where(child => child.Id == lookup).Select(child => child.ParentId).SingleAsync(ct));

        context.Parents.AddRange(Enumerable.Range(101, EfWorkloadData.PageSize)
            .Select(id => new Parent { Id = id, Score = id * 2, Name = "inserted" }));
        Assert.Equal(EfWorkloadData.PageSize, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        int updated = await context.Parents.Where(parent => parent.Id <= EfWorkloadData.PageSize)
            .ExecuteUpdateAsync(setters => setters.SetProperty(parent => parent.Score, parent => parent.Score + 1), ct);
        Assert.Equal(EfWorkloadData.PageSize, updated);
        Assert.Equal(3, await context.Parents.Where(parent => parent.Id == 1).Select(parent => parent.Score).SingleAsync(ct));
        Assert.Equal(EfWorkloadData.PageSize, await context.Parents.Where(parent => parent.Id > 90 && parent.Id <= 100).ExecuteDeleteAsync(ct));
        Assert.Equal(90 * EfWorkloadData.ChildrenPerParent, await context.Children.CountAsync(ct));
        Assert.Equal(100, await context.Parents.CountAsync(ct));
    }

    private sealed class WorkloadContext(DbContextOptions<WorkloadContext> options) : DbContext(options)
    {
        public DbSet<Parent> Parents => this.Set<Parent>();

        public DbSet<Child> Children => this.Set<Child>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Parent>(entity =>
            {
                _ = entity.ToTable(EfWorkloadData.ParentTable);
                _ = entity.Property(parent => parent.Id).ValueGeneratedNever();
                _ = entity.Property(parent => parent.Name).HasMaxLength(40);
                _ = entity.OwnsOne(parent => parent.Detail, owned =>
                    _ = owned.Property(detail => detail.Note).HasColumnName("Note").HasMaxLength(40));
                _ = entity.HasMany(parent => parent.Children).WithOne().HasForeignKey(child => child.ParentId)
                    .OnDelete(DeleteBehavior.Cascade);
            });
            _ = modelBuilder.Entity<Child>(entity =>
            {
                _ = entity.ToTable(EfWorkloadData.ChildTable);
                _ = entity.Property(child => child.Id).ValueGeneratedNever();
            });
        }
    }

#pragma warning disable CA1812 // EF materializes these fixture entities from stored rows.
    private sealed class Parent
    {
        public int Id { get; set; }

        public int Score { get; set; }

        public string Name { get; set; } = string.Empty;

        public Detail? Detail { get; set; }

        public List<Child> Children { get; set; } = [];
    }

    private sealed class Detail
    {
        public string? Note { get; set; }
    }

    private sealed class Child
    {
        public Guid Id { get; set; }

        public int ParentId { get; set; }
    }
#pragma warning restore CA1812
}
