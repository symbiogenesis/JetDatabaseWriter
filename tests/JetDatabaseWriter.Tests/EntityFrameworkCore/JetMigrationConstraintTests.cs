namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetMigrationConstraintTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FailedMigration_InCallerTransaction_PreservesPriorWorkAndSavepoint(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (OriginalContext original = new(options))
        {
            Assert.True(await original.Database.EnsureCreatedAsync(ct));
        }

        await using (UniqueContext changed = new(options))
        {
            await using IDbContextTransaction transaction = await changed.Database.BeginTransactionAsync(ct);
            await InsertExistingRowAsync(changed, 1, ct);
            await transaction.CreateSavepointAsync("retained", ct);
            await InsertExistingRowAsync(changed, 2, ct);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => changed.Database.MigrateAsync(ct));
            Assert.Contains("IX_Rows_Code", exception.Message, StringComparison.Ordinal);
            Assert.Same(transaction, changed.Database.CurrentTransaction);
            Assert.False(await HasAddedColumnAsync(changed, ct));
            Assert.Equal(2, await changed.Set<IndexedRow>().CountAsync(ct));
            await transaction.RollbackToSavepointAsync("retained", ct);
            Assert.Equal(1, await changed.Set<IndexedRow>().CountAsync(ct));
            await transaction.ReleaseSavepointAsync("retained", ct);
            await transaction.CommitAsync(ct);
        }

        await using OriginalContext verify = new(options);
        Assert.Equal(1, await verify.Set<IndexedRow>().CountAsync(ct));
        Assert.False(await HasAddedColumnAsync(verify, ct));
    }

    [Theory]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    public async Task Migration_AtWriteNestingLimit_RefusesBeforeMutationAndRetriesAfterRelease(int nestingDepth)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (OriginalContext original = new(options))
        {
            Assert.True(await original.Database.EnsureCreatedAsync(ct));
        }

        await using AddedColumnContext changed = new(options);
        await using IDbContextTransaction transaction = await changed.Database.BeginTransactionAsync(ct);
        await InsertExistingRowAsync(changed, 1, ct);
        await transaction.CreateSavepointAsync("one", ct);
        await transaction.CreateSavepointAsync("two", ct);
        await transaction.CreateSavepointAsync("three", ct);
        await transaction.CreateSavepointAsync("four", ct);
        for (int level = 6; level <= nestingDepth; level++)
        {
            await transaction.CreateSavepointAsync($"level-{level}", ct);
        }

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => changed.Database.MigrateAsync(ct));
        Assert.Contains("nesting", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Same(transaction, changed.Database.CurrentTransaction);
        Assert.False(await HasAddedColumnAsync(changed, ct));
        await transaction.ReleaseSavepointAsync("four", ct);
        await changed.Database.MigrateAsync(ct);
        Assert.True(await HasAddedColumnAsync(changed, ct));
        Assert.Equal(1, await changed.Set<IndexedRow>().CountAsync(ct));
        await transaction.RollbackToSavepointAsync("one", ct);
        Assert.False(await HasAddedColumnAsync(changed, ct));
        Assert.Equal(1, await changed.Set<IndexedRow>().CountAsync(ct));
        await transaction.ReleaseSavepointAsync("one", ct);
        await transaction.CommitAsync(ct);
    }

    [Fact]
    public async Task CanceledMigration_InCallerTransaction_RollsBackSchemaAndPreservesSavepoint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (OriginalContext original = new(options))
        {
            Assert.True(await original.Database.EnsureCreatedAsync(ct));
        }

        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(ct);
        MigrationSeed seed = new();
        await using SeededMigrationContext changed = new(options, seed);
        _ = changed.GetService<IDesignTimeModel>().Model;
        await using IDbContextTransaction transaction = await changed.Database.BeginTransactionAsync(ct);
        await InsertExistingRowAsync(changed, 1, ct);
        await transaction.CreateSavepointAsync("retained", ct);
        await InsertExistingRowAsync(changed, 2, ct);

        // Seed values are read after the additive schema work, canceling after the column was added.
        seed.OnRead = canceled.Cancel;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => changed.Database.MigrateAsync(canceled.Token));
        seed.OnRead = null;
        Assert.True(canceled.IsCancellationRequested);
        Assert.Same(transaction, changed.Database.CurrentTransaction);
        Assert.False(await HasAddedColumnAsync(changed, ct));
        Assert.Equal(2, await changed.Set<IndexedRow>().CountAsync(ct));
        await transaction.RollbackToSavepointAsync("retained", ct);
        Assert.Equal(1, await changed.Set<IndexedRow>().CountAsync(ct));
        await transaction.ReleaseSavepointAsync("retained", ct);
        await changed.Database.MigrateAsync(ct);
        Assert.True(await HasAddedColumnAsync(changed, ct));
        await transaction.CommitAsync(ct);
    }

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
        IndexMetadata index = Assert.Single(await reader.ListIndexesAsync("Rows", ct), static index => index.Name == "IX_Rows_Code");
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
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relationshipName, "Parents", "Id", "Children", "ParentId")
                {
                    EnforceReferentialIntegrity = enforceIntegrity,
                    CascadeUpdates = cascadeUpdates,
                    CascadeDeletes = cascadeDeletes,
                },
                ct);
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

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MalformedRelationshipGroup_RefusesMigrationInsteadOfAcceptingPartialKey(DatabaseFormat format)
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
            long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, ct);
            TableDef definition = await writer.Database.TableDefs.ReadRequiredTableDefAsync(page, Constants.SystemTableNames.Relationships, ct);
            object[] row = definition.CreateNullValueRow();
            definition.SetValueByName(row, "szRelationship", relationshipName);
            definition.SetValueByName(row, "szObject", "Children");
            definition.SetValueByName(row, "szReferencedObject", "Parents");
            definition.SetValueByName(row, "szColumn", "ParentId");
            definition.SetValueByName(row, "szReferencedColumn", string.Empty);
            definition.SetValueByName(row, "icolumn", 1);
            definition.SetValueByName(row, "ccolumn", 2);
            definition.SetValueByName(row, "grbit", 0);
            await writer.Services.Mutations.InsertSystemRowAndMaintainAsync(page, definition, Constants.SystemTableNames.Relationships, row, cancellationToken: ct);
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct))
        {
            Assert.Empty(await reader.ListRelationshipsAsync(ct));
        }

        await using RelationshipContext current = new(options);
        JetObjectExistsException exception = await Assert.ThrowsAsync<JetObjectExistsException>(() => current.Database.MigrateAsync(ct));
        Assert.Equal(JetErrorCode.RelationshipExists, exception.ErrorCode);
    }

#pragma warning disable EF1001 // Inspect schema and preserve caller work through the active writer's normal connection lease.
    private static async Task InsertExistingRowAsync(DbContext context, int id, CancellationToken cancellationToken)
    {
        IJetClientConnection connection = context.GetService<IJetClientConnection>();
        (AccessWriter writer, bool ownedByCaller) = await connection.RentWriterAsync(cancellationToken);
        try
        {
            await writer.InsertRowAsync("Rows", new RowValues { ["Id"] = id, ["Code"] = $"code-{id}", ["Other"] = "caller" }, cancellationToken);
        }
        finally
        {
            await connection.ReleaseWriterAsync(writer, ownedByCaller);
        }
    }

    private static async Task<bool> HasAddedColumnAsync(DbContext context, CancellationToken cancellationToken)
    {
        IJetClientConnection connection = context.GetService<IJetClientConnection>();
        (AccessWriter writer, bool ownedByCaller) = await connection.RentWriterAsync(cancellationToken);
        try
        {
            ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Rows", cancellationToken);
            Assert.NotNull(table);
            return table.Definition.FindColumnIndex(nameof(IndexedRow.Added)) >= 0;
        }
        finally
        {
            await connection.ReleaseWriterAsync(writer, ownedByCaller);
        }
    }
#pragma warning restore EF1001

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

    private sealed class AddedColumnContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Code);
        }
    }

    private sealed class SeededMigrationContext(DbContextOptions options, MigrationSeed seed) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            ConfigureIndexedRow(modelBuilder);
            _ = modelBuilder.Entity<IndexedRow>().HasIndex(row => row.Code);
            _ = modelBuilder.Entity<IndexedRow>().HasData(seed);
        }
    }

    private sealed class MigrationSeed
    {
        public Action? OnRead { get; set; }

        public int Id
        {
            get
            {
                this.OnRead?.Invoke();
                return 3;
            }
        }

        public string Code { get; } = "seed";

        public string Other { get; } = "seed";

        public int Added { get; } = 1;
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

#pragma warning disable CA1812 // These types supply relationship metadata without constructing related entities.
    private sealed class Parent
    {
        public int Id { get; set; }
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }
    }
#pragma warning restore CA1812
}
