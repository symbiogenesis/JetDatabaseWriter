namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

public sealed class JetSharedColumnValidationTests
{
    [Fact]
    public async Task IndependentEntityTypesSharingTable_AreRejectedBeforeMutation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<UnrelatedTableContext> options = new DbContextOptionsBuilder<UnrelatedTableContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using UnrelatedTableContext context = new(options);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.EnsureCreatedAsync(ct));

        Assert.Contains("SharedTable", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, stream.Length);
    }

    [Fact]
    public async Task IndependentPropertiesSharingColumn_AreRejectedBeforeMutation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CollisionContext> options = new DbContextOptionsBuilder<CollisionContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using CollisionContext context = new(options);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.EnsureCreatedAsync(ct));

        Assert.Contains("Shared", exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(CollisionRow.First), exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(CollisionRow.Second), exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, stream.Length);
    }

    [Theory]
    [InlineData("length")]
    [InlineData("storage-type")]
    [InlineData("clr-type")]
    [InlineData("precision")]
    [InlineData("scale")]
    [InlineData("currency")]
    [InlineData("converter")]
    [InlineData("default")]
    [InlineData("validation")]
    [InlineData("zero-length")]
    public async Task IncompatibleSiblingColumns_AreRejectedBeforeMutation(string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using SiblingContext context = CreateSiblingContext(stream, DatabaseFormat.AceAccdb, mode);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.EnsureCreatedAsync(ct));

        Assert.Contains("Shared", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, stream.Length);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "compatible")]
    [InlineData(DatabaseFormat.Jet4Mdb, "compatible")]
    [InlineData(DatabaseFormat.AceAccdb, "compatible")]
    [InlineData(DatabaseFormat.AceAccdb, "compatible-converter")]
    public async Task CompatibleSiblingColumnsAndOwnedKeys_RoundTrip(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using SiblingContext context = CreateSiblingContext(stream, format, mode);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        context.AddRange(
            new FirstSibling { Id = 1, Data = "first", Detail = new Detail { Data = "owned first" } },
            new SecondSibling { Id = 2, Data = "second", Detail = new Detail { Data = "owned second" } });
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();

        FirstSibling first = await context.Set<FirstSibling>().SingleAsync(ct);
        SecondSibling second = await context.Set<SecondSibling>().SingleAsync(ct);
        Assert.Equal("first", first.Data);
        Assert.Equal("second", second.Data);
        Assert.Equal("owned first", first.Detail.Data);
        Assert.Equal("owned second", second.Detail.Data);
        first.Data = "updated first";
        second.Detail.Data = "updated owned";
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal("updated first", (await context.Set<FirstSibling>().SingleAsync(ct)).Data);
        Assert.Equal("updated owned", (await context.Set<SecondSibling>().SingleAsync(ct)).Detail.Data);
    }

    private static SiblingContext CreateSiblingContext(MemoryStream stream, DatabaseFormat format, string mode)
    {
        DbContextOptions<SiblingContext> options = new DbContextOptionsBuilder<SiblingContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .ReplaceService<IModelCacheKeyFactory, SharingModelCacheKeyFactory>().Options;
        return new SiblingContext(options, mode);
    }

    private sealed class CollisionContext(DbContextOptions<CollisionContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<CollisionRow>().Property(row => row.First).HasColumnName("Shared");
            _ = modelBuilder.Entity<CollisionRow>().Property(row => row.Second).HasColumnName("shared");
        }
    }

    private sealed class UnrelatedTableContext(DbContextOptions<UnrelatedTableContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<CollisionRow>().ToTable("SharedTable");
            _ = modelBuilder.Entity<CollisionRow>().Property(row => row.Id).HasColumnName("FirstId");
            _ = modelBuilder.Entity<UnrelatedRow>().ToTable("SharedTable");
            _ = modelBuilder.Entity<UnrelatedRow>().Property(row => row.Id).HasColumnName("SecondId");
        }
    }

#pragma warning disable CA1812 // EF dependency injection constructs the registered service.
    private sealed class SharingModelCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => (context.GetType(), ((SiblingContext)context).Mode, designTime);
    }
#pragma warning restore CA1812

    private sealed class SiblingContext(DbContextOptions<SiblingContext> options, string mode) : DbContext(options)
    {
        internal string Mode { get; } = mode;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Sibling>().HasDiscriminator<string>("Kind")
                .HasValue<FirstSibling>("first").HasValue<SecondSibling>("second");
            _ = modelBuilder.Entity<Sibling>().OwnsOne(row => row.Detail, owned => owned.Property(detail => detail.Data).HasMaxLength(40));
            _ = modelBuilder.Entity<FirstSibling>().Property(row => row.Data).HasColumnName("Shared").HasMaxLength(40);
            _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data).HasColumnName("shared").HasMaxLength(40);
            switch (this.Mode)
            {
                case "compatible-converter":
                    _ = modelBuilder.Entity<FirstSibling>().Property(row => row.Data)
                        .HasConversion(value => value + "|", value => value.Substring(0, value.Length - 1));
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data)
                        .HasConversion(value => value + "|", value => value.Substring(0, value.Length - 1));
                    break;
                case "clr-type":
                    _ = modelBuilder.Entity<SecondSibling>().Ignore(row => row.Data);
                    _ = modelBuilder.Entity<SecondSibling>().Property<int>("Number").HasColumnName("Shared");
                    break;
                case "precision":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Amount).HasPrecision(15, 4);
                    break;
                case "scale":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Amount).HasPrecision(18, 2);
                    break;
                case "currency":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Amount).UseJetCurrency();
                    break;
                case "length":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data).HasMaxLength(80);
                    break;
                case "storage-type":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data)
                        .HasConversion(value => value.Length, value => value.ToString(CultureInfo.InvariantCulture));
                    break;
                case "converter":
                    _ = modelBuilder.Entity<FirstSibling>().Property(row => row.Data).HasConversion(value => value + "A", value => value.Substring(0, value.Length - 1));
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data).HasConversion(value => value + "B", value => value.Substring(0, value.Length - 1));
                    break;
                case "default":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data).HasJetDefaultValue("different");
                    break;
                case "validation":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data).HasJetValidationRule("Len([Shared]) > 0");
                    break;
                case "zero-length":
                    _ = modelBuilder.Entity<SecondSibling>().Property(row => row.Data).HasJetAllowZeroLength(false);
                    break;
                case "compatible":
                    break;
                default:
                    throw new InvalidOperationException($"Unknown column-sharing scenario: {this.Mode}.");
            }
        }
    }

#pragma warning disable CA1812 // These model-only types exercise validation before any entity is constructed.
    private sealed class CollisionRow
    {
        public int Id { get; set; }

        public int First { get; set; }

        public int Second { get; set; }
    }

    private sealed class UnrelatedRow
    {
        public int Id { get; set; }
    }
#pragma warning restore CA1812

    private abstract class Sibling
    {
        public int Id { get; set; }

        public Detail Detail { get; set; } = new();
    }

    private sealed class FirstSibling : Sibling
    {
        public string Data { get; set; } = string.Empty;

        public decimal Amount { get; set; }
    }

    private sealed class SecondSibling : Sibling
    {
        public string Data { get; set; } = string.Empty;

        public decimal Amount { get; set; }
    }

    private sealed class Detail
    {
        public string Data { get; set; } = string.Empty;
    }
}
