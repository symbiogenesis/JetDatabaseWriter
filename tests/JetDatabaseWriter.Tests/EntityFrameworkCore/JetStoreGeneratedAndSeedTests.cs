namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

public sealed class JetStoreGeneratedAndSeedTests
{
    [Fact]
    public async Task SaveChanges_HydratesGeneratedValues_ForTextAndDateKeys()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<GeneratedContext> options = new DbContextOptionsBuilder<GeneratedContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;
        await using GeneratedContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        TextRow text = new() { Id = "text-key", Quantity = 3 };
        DateRow date = new() { Id = new DateTime(2026, 8, 9), Quantity = 4 };
        context.AddRange(text, date);
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        Assert.Equal(15, text.DefaultPoints);
        Assert.Equal(30, text.Total);
        Assert.Equal(15, date.DefaultPoints);
        Assert.Equal(40, date.Total);
        text.Quantity = 5;
        date.Quantity = 6;
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        Assert.Equal(50, text.Total);
        Assert.Equal(60, date.Total);
    }

    [Fact]
    public async Task SaveChanges_HydratesGeneratedValues_WhenKeyIndexIsUnavailable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<GeneratedContext> options = new DbContextOptionsBuilder<GeneratedContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;
        await using GeneratedContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
#pragma warning disable EF1001 // Exercise the provider's writer lease to remove the key index before SaveChanges.
        IJetClientConnection connection = context.GetService<IJetClientConnection>();
        (AccessWriter writer, bool owned) = await connection.RentWriterAsync(ct);
        try
        {
            await writer.DropIndexAsync("IntegerRows", "PrimaryKey", ct);
        }
        finally
        {
            await connection.ReleaseWriterAsync(writer, owned);
        }
#pragma warning restore EF1001

        IntegerRow row = new() { Id = 17, Quantity = 7 };
        context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(15, row.DefaultPoints);
        Assert.Equal(70, row.Total);
        row.Quantity = 8;
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(80, row.Total);
    }

    [Fact]
    public async Task Migrate_IsIdempotent_ForBinaryAndConvertedCompositeKeys()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<SeedContext> options = new DbContextOptionsBuilder<SeedContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;
        await using SeedContext context = new(options);
        await context.Database.MigrateAsync(ct);
        context.Add(new BinarySeed { Id = [9, 8, 7, 6], Name = "unrelated" });
        context.Add(new ConvertedSeed { Id = 99, Partition = 4, Name = "unrelated" });
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        await context.Database.MigrateAsync(ct);
        await context.Database.MigrateAsync(ct);
        Assert.Equal(2, await context.Set<BinarySeed>().CountAsync(ct));
        Assert.Equal(2, await context.Set<ConvertedSeed>().CountAsync(ct));
        Assert.Equal("seed", await context.Set<BinarySeed>().Where(x => x.Name == "seed").Select(x => x.Name).SingleAsync(ct));
    }

    private sealed class GeneratedContext(DbContextOptions<GeneratedContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<TextRow>(entity =>
            {
                _ = entity.ToTable("TextRows");
                _ = entity.HasKey(x => x.Id);
                _ = entity.Property(x => x.Id).HasMaxLength(40).ValueGeneratedNever();
                _ = entity.Property(x => x.DefaultPoints).HasJetDefaultValueExpression("=10 + 5");
                _ = entity.Property(x => x.Total).HasJetCalculatedExpression("[Quantity] * 10");
            });
            _ = modelBuilder.Entity<DateRow>(entity =>
            {
                _ = entity.ToTable("DateRows");
                _ = entity.HasKey(x => x.Id);
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.Property(x => x.DefaultPoints).HasJetDefaultValueExpression("=10 + 5");
                _ = entity.Property(x => x.Total).HasJetCalculatedExpression("[Quantity] * 10");
            });
            _ = modelBuilder.Entity<IntegerRow>(entity =>
            {
                _ = entity.ToTable("IntegerRows");
                _ = entity.HasKey(x => x.Id);
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.Property(x => x.DefaultPoints).HasJetDefaultValueExpression("=10 + 5");
                _ = entity.Property(x => x.Total).HasJetCalculatedExpression("[Quantity] * 10");
            });
        }
    }

    private sealed class SeedContext(DbContextOptions<SeedContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<BinarySeed>(entity =>
            {
                _ = entity.HasKey(x => x.Id);
                _ = entity.Property(x => x.Id).HasMaxLength(4).ValueGeneratedNever();
                _ = entity.Property(x => x.Name).HasMaxLength(40);
                _ = entity.HasData(new BinarySeed { Id = [1, 2, 3, 4], Name = "seed" });
            });
            _ = modelBuilder.Entity<ConvertedSeed>(entity =>
            {
                _ = entity.HasKey(x => new { x.Id, x.Partition });
                _ = entity.Property(x => x.Id).HasConversion<string>().HasMaxLength(20).ValueGeneratedNever();
                _ = entity.Property(x => x.Partition).ValueGeneratedNever();
                _ = entity.Property(x => x.Name).HasMaxLength(40);
                _ = entity.HasData(new ConvertedSeed { Id = 7, Partition = 2, Name = "seed" });
            });
        }
    }

    private sealed class TextRow
    {
        public string Id { get; set; } = string.Empty;

        public int Quantity { get; set; }

        public int DefaultPoints { get; set; }

        public int Total { get; set; }
    }

    private sealed class DateRow
    {
        public DateTime Id { get; set; }

        public int Quantity { get; set; }

        public int DefaultPoints { get; set; }

        public int Total { get; set; }
    }

    private sealed class IntegerRow
    {
        public int Id { get; set; }

        public int Quantity { get; set; }

        public int DefaultPoints { get; set; }

        public int Total { get; set; }
    }

    private sealed class BinarySeed
    {
        public byte[] Id { get; set; } = [];

        public string Name { get; set; } = string.Empty;
    }

    private sealed class ConvertedSeed
    {
        public int Id { get; set; }

        public short Partition { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
