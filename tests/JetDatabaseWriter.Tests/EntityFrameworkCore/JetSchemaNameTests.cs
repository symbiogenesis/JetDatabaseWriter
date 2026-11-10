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
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetSchemaNameTests
{
    private static readonly string LongTableName = new('T', 60);

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task LongGeneratedNames_PreserveEveryUniqueIndex(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<IndexContext> options = new DbContextOptionsBuilder<IndexContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (IndexContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            await context.Database.MigrateAsync(ct);
            context.AddRange(new IndexedRow { Id = 1, First = "first", Second = "duplicate" }, new IndexedRow { Id = 2, First = "second", Second = "duplicate" });
            await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        IndexMetadata[] indexes = (await reader.ListIndexesAsync(LongTableName, ct)).Where(static index => index.EnforcesUniqueness).ToArray();
        Assert.Equal(3, indexes.Length);
        Assert.Equal(3, indexes.Select(static index => index.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.All(indexes, static index => Assert.InRange(index.Name.Length, 1, 64));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task LongGeneratedNames_PreserveEveryAlternateKey(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<AlternateKeyContext> options = new DbContextOptionsBuilder<AlternateKeyContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (AlternateKeyContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            await context.Database.MigrateAsync(ct);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        IndexMetadata[] indexes = (await reader.ListIndexesAsync(LongTableName, ct)).Where(static index => index.EnforcesUniqueness).ToArray();
        Assert.Equal(3, indexes.Length);
        Assert.Equal(3, indexes.Select(static index => index.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task LongGeneratedNames_PreserveEveryRelationship(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<RelationshipContext> options = new DbContextOptionsBuilder<RelationshipContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (RelationshipContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            await context.Database.MigrateAsync(ct);
            context.Add(new Parent { Id = 1 });
            Assert.Equal(1, await context.SaveChangesAsync(ct));
            context.Add(new Child { Id = 1, FirstParentId = 1, SecondParentId = 99 });
            await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        IReadOnlyList<RelationshipMetadata> relationships = await reader.ListRelationshipsAsync(ct);
        Assert.Equal(2, relationships.Count);
        Assert.All(relationships, static relationship => Assert.InRange(relationship.Name.Length, 1, 64));
    }

    [Fact]
    public async Task ConflictingExplicitIndexName_IsRejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<ConflictingNameContext> options = new DbContextOptionsBuilder<ConflictingNameContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using ConflictingNameContext context = new(options);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.EnsureCreatedAsync(ct));
        Assert.Contains("PrimaryKey", exception.Message, StringComparison.Ordinal);
    }

    private sealed class IndexContext(DbContextOptions<IndexContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<IndexedRow>().ToTable(LongTableName);
            _ = modelBuilder.Entity<IndexedRow>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.First).IsUnique();
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Second).IsUnique();
        }
    }

    private sealed class AlternateKeyContext(DbContextOptions<AlternateKeyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<IndexedRow>().ToTable(LongTableName);
            _ = modelBuilder.Entity<IndexedRow>().HasAlternateKey(row => row.First);
            _ = modelBuilder.Entity<IndexedRow>().HasAlternateKey(row => row.Second);
        }
    }

    private sealed class ConflictingNameContext(DbContextOptions<ConflictingNameContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.First, "PrimaryKey").IsUnique();
    }

    private sealed class RelationshipContext(DbContextOptions<RelationshipContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Parent>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Child>().ToTable(LongTableName);
            _ = modelBuilder.Entity<Child>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Child>().HasOne<Parent>().WithMany().HasForeignKey(row => row.FirstParentId).OnDelete(DeleteBehavior.NoAction);
            _ = modelBuilder.Entity<Child>().HasOne<Parent>().WithMany().HasForeignKey(row => row.SecondParentId).OnDelete(DeleteBehavior.NoAction);
        }
    }

    private sealed class IndexedRow
    {
        public int Id { get; set; }

        public string First { get; set; } = string.Empty;

        public string Second { get; set; } = string.Empty;
    }

    private sealed class Parent
    {
        public int Id { get; set; }
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int FirstParentId { get; set; }

        public int SecondParentId { get; set; }
    }
}
