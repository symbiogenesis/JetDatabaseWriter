namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetMigrationConstraintTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "unique")]
    [InlineData(DatabaseFormat.Jet4Mdb, "unique")]
    [InlineData(DatabaseFormat.AceAccdb, "unique")]
    [InlineData(DatabaseFormat.Jet3Mdb, "descending")]
    [InlineData(DatabaseFormat.Jet4Mdb, "descending")]
    [InlineData(DatabaseFormat.AceAccdb, "descending")]
    [InlineData(DatabaseFormat.Jet3Mdb, "columns")]
    [InlineData(DatabaseFormat.Jet4Mdb, "columns")]
    [InlineData(DatabaseFormat.AceAccdb, "columns")]
    public async Task IncompatibleNamedIndex_RefusesMigrationAndRollsBackAdditions(DatabaseFormat format, string change)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (OriginalContext original = new(options))
        {
            Assert.True(await original.Database.EnsureCreatedAsync(ct));
            original.Add(new IndexedRow { Id = 1, Code = "preserved", Other = "other" });
            Assert.Equal(1, await original.SaveChangesAsync(ct));
        }

        await using (DbContext changed = change switch
        {
            "unique" => new UniqueContext(options),
            "descending" => new DescendingContext(options),
            "columns" => new DifferentColumnsContext(options),
            _ => throw new ArgumentException("Unknown model change.", nameof(change)),
        })
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => changed.Database.MigrateAsync(ct));
            Assert.Contains("IX_Rows_Code", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        Assert.DoesNotContain(await reader.GetColumnMetadataAsync("Rows", ct), static column => column.Name == nameof(IndexedRow.Added));
        IndexMetadata index = Assert.Single((await reader.ListIndexesAsync("Rows", ct)).Where(static index => index.Name == "IX_Rows_Code"));
        Assert.False(index.EnforcesUniqueness);
        Assert.True(Assert.Single(index.Columns).IsAscending);
        Assert.Equal(nameof(IndexedRow.Code), Assert.Single(index.Columns).Name);
    }

    [Fact]
    public async Task EquivalentIndexWithDifferentName_RemainsIdempotent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (RenamedIndexContext original = new(options))
        {
            Assert.True(await original.Database.EnsureCreatedAsync(ct));
        }

        await using (OriginalContext current = new(options))
        {
            await current.Database.MigrateAsync(ct);
            await current.Database.MigrateAsync(ct);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        Assert.Equal(2, (await reader.ListIndexesAsync("Rows", ct)).Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task IncompatiblePrimaryKey_RefusesMigration(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (OriginalContext original = new(options))
        {
            Assert.True(await original.Database.EnsureCreatedAsync(ct));
        }

        await using DifferentPrimaryContext current = new(options);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => current.Database.MigrateAsync(ct));
        Assert.Contains("PrimaryKey", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MissingPrimaryKey_RefusesMigration(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Rows", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Code", typeof(string), 255), new ColumnDefinition("Other", typeof(string))], cancellationToken: ct);
        }

        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using OriginalContext current = new(options);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => current.Database.MigrateAsync(ct));
        Assert.Contains("PrimaryKey", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, true)]
    public async Task IncompatibleRelationship_RefusesMigration(DatabaseFormat format, bool enforceIntegrity, bool cascadeUpdates, bool cascadeDeletes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<RelationshipContext> options = new DbContextOptionsBuilder<RelationshipContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (RelationshipContext original = new(options))
        {
            Assert.True(await original.Database.EnsureCreatedAsync(ct));
        }

        const string relationshipName = "FK_Children_Parents_ParentId";
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, leaveOpen: true, cancellationToken: ct))
        {
            await writer.DropRelationshipAsync(relationshipName, ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(relationshipName, "Parents", "Id", "Children", "ParentId")
            {
                EnforceReferentialIntegrity = enforceIntegrity,
                CascadeUpdates = cascadeUpdates,
                CascadeDeletes = cascadeDeletes,
            }, ct);
        }

        await using (RelationshipContext current = new(options))
        {
            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => current.Database.MigrateAsync(ct));
            Assert.Contains(relationshipName, exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        RelationshipMetadata relationship = Assert.Single(await reader.ListRelationshipsAsync(ct));
        Assert.Equal(enforceIntegrity, relationship.EnforcesReferentialIntegrity);
        Assert.Equal(cascadeUpdates, relationship.CascadeUpdates);
        Assert.Equal(cascadeDeletes, relationship.CascadeDeletes);
    }

    private static void ConfigureIndexedRow(ModelBuilder modelBuilder)
    {
        _ = modelBuilder.Entity<IndexedRow>().ToTable("Rows");
        _ = modelBuilder.Entity<IndexedRow>().Property(row => row.Id).ValueGeneratedNever();
    }

    private sealed class OriginalContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().Ignore(row => row.Added);
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Code);
        }
    }

    private sealed class RenamedIndexContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().Ignore(row => row.Added);
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Code, "ExistingCodeIndex");
        }
    }

    private sealed class UniqueContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Code).IsUnique();
        }
    }

    private sealed class DescendingContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Code).IsDescending();
        }
    }

    private sealed class DifferentColumnsContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Other, "IX_Rows_Code");
        }
    }

    private sealed class DifferentPrimaryContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().HasKey(row => row.Code);
        }
    }

    private sealed class RelationshipContext(DbContextOptions<RelationshipContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Parent>().ToTable("Parents");
            _ = modelBuilder.Entity<Child>().ToTable("Children");
            _ = modelBuilder.Entity<Child>().HasOne<Parent>().WithMany().HasForeignKey(row => row.ParentId).OnDelete(DeleteBehavior.NoAction);
        }
    }

    private sealed class IndexedRow
    {
        public int Id { get; set; }

        public string Code { get; set; } = string.Empty;

        public string Other { get; set; } = string.Empty;

        public int Added { get; set; }
    }

    private sealed class Parent
    {
        public int Id { get; set; }
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }
    }
}
