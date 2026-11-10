namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetOwnedUniqueSaveOrderingTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task OwnedUpdates_ReleaseUniqueValuesBeforeNewOwnerInserts(DatabaseFormat format, bool modifyOwner)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<OrderingContext> options = new DbContextOptionsBuilder<OrderingContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using OrderingContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Owner original = new() { Id = 1, Detail = new Detail { Code = "taken", Revision = 1 } };
        _ = context.Add(original);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        await using OrderingContext stale = new(options);
        Owner previous = await stale.Set<Owner>().SingleAsync(ct);
        original.Detail.Code = "freed";
        original.Detail.Revision = 2;
        if (modifyOwner)
        {
            original.Name = "changed";
        }

        _ = context.Add(new Owner { Id = 2, Detail = new Detail { Code = "TAKEN", Revision = 1 } });
        context.ChangeTracker.DetectChanges();
        Assert.Equal(modifyOwner ? EntityState.Modified : EntityState.Unchanged, context.Entry(original).State);

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        previous.Detail.Code = "stale";
        _ = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => stale.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Owner[] owners = await context.Set<Owner>().OrderBy(owner => owner.Id).ToArrayAsync(ct);
        Assert.Equal("freed", owners[0].Detail.Code);
        Assert.Equal(2, owners[0].Detail.Revision);
        Assert.Equal("TAKEN", owners[1].Detail.Code);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task OwnedUpdates_KeepCyclicExchangesInOnePhysicalRowBatch(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = new(new DbContextOptionsBuilder<OrderingContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Owner first = new() { Id = 1, Detail = new Detail { Code = "first", Alias = "taken" } };
        Owner second = new() { Id = 2, Detail = new Detail { Code = "second", Alias = "other" } };
        context.AddRange(first, second);
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        first.Detail.Code = "second";
        second.Detail.Code = "first";
        first.Detail.Alias = "freed";
        _ = context.Add(new Owner { Id = 3, Detail = new Detail { Code = "third", Alias = "taken" } });

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Owner[] owners = await context.Set<Owner>().OrderBy(owner => owner.Id).ToArrayAsync(ct);
        Assert.Equal("second", owners[0].Detail.Code);
        Assert.Equal("first", owners[1].Detail.Code);
        Assert.Equal("taken", owners[2].Detail.Alias);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task OwnedUpdates_AwaitNewForeignKeyPrincipalsBeforeReleasingUniqueValues(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = new(new DbContextOptionsBuilder<OrderingContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Owner original = new() { Id = 1, Detail = new Detail { Code = "taken" } };
        _ = context.Add(original);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        original.Detail.Code = "freed";
        original.Detail.CountryId = 7;
        context.AddRange(new Owner { Id = 2, Detail = new Detail { Code = "taken" } }, new Country { Id = 7 });

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(7, (await context.Set<Owner>().SingleAsync(owner => owner.Id == 1, ct)).Detail.CountryId);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ModifiedGeneratedProperties_AcquireExplicitSentinelValues(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using OrderingContext context = new(new DbContextOptionsBuilder<OrderingContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        NumberRow first = new() { Id = 1, Number = 7 };
        NumberRow second = new() { Id = 2, Number = 2 };
        context.AddRange(first, second);
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        second.Number = 0;
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        first.Number = 0;
        second.Number = 9;
        _ = context.Add(new NumberRow { Id = 3, Number = 7 });

        Assert.Equal(3, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        NumberRow[] rows = await context.Set<NumberRow>().OrderBy(row => row.Id).ToArrayAsync(ct);
        Assert.Equal(0, rows[0].Number);
        Assert.Equal(9, rows[1].Number);
        Assert.Equal(7, rows[2].Number);
    }

    private sealed class OrderingContext(DbContextOptions<OrderingContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Owner>(entity =>
            {
                _ = entity.Property(owner => owner.Id).ValueGeneratedNever();
                _ = entity.Property(owner => owner.Name).HasMaxLength(32);
                _ = entity.OwnsOne(owner => owner.Detail, owned =>
                {
                    _ = owned.Property(detail => detail.Code).HasMaxLength(32);
                    _ = owned.Property(detail => detail.Alias).HasMaxLength(32);
                    _ = owned.HasIndex(detail => detail.Code).IsUnique();
                    _ = owned.HasIndex(detail => detail.Alias).IsUnique();
                    _ = owned.Property(detail => detail.Revision).IsConcurrencyToken();
                    _ = owned.HasOne<Country>().WithMany().HasForeignKey(detail => detail.CountryId)
                        .OnDelete(DeleteBehavior.ClientNoAction);
                });
            });
            _ = modelBuilder.Entity<Country>().Property(country => country.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<NumberRow>(entity =>
            {
                _ = entity.Property(row => row.Id).ValueGeneratedNever();
                _ = entity.Property(row => row.Number).HasDefaultValue(77);
                _ = entity.HasIndex(row => row.Number).IsUnique();
            });
        }
    }

    private sealed class Owner
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public Detail Detail { get; set; } = new();
    }

    private sealed class Detail
    {
        public string Code { get; set; } = string.Empty;

        public string? Alias { get; set; }

        public int Revision { get; set; }

        public int? CountryId { get; set; }
    }

    private sealed class Country
    {
        public int Id { get; set; }
    }

    private sealed class NumberRow
    {
        public int Id { get; set; }

        public int Number { get; set; }
    }
}
