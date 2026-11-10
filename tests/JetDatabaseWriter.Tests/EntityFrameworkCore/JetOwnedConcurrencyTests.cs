namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetOwnedConcurrencyTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Shared_ConcurrencyTokens_Reject_Stale_Owned_Updates(bool modifyOwner, bool modifySibling)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<ConcurrencyContext> options = await CreateConcurrencyDatabaseAsync(stream, ct);
        await using ConcurrencyContext first = new(options);
        await using ConcurrencyContext stale = new(options);
        Document current = await first.Set<Document>().SingleAsync(ct);
        Document previous = await stale.Set<Document>().SingleAsync(ct);
        current.Detail.Text = "first";
        current.Detail.Revision = 2;
        Assert.Equal(1, await first.SaveChangesAsync(ct));
        if (modifySibling)
        {
            previous.Billing.Text = "stale";
        }
        else
        {
            previous.Detail.Text = "stale";
        }

        if (modifyOwner)
        {
            previous.Name = "stale owner";
        }

        _ = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => stale.SaveChangesAsync(ct));
        await using ConcurrencyContext verify = new(options);
        Document result = await verify.Set<Document>().SingleAsync(ct);
        Assert.Equal("first", result.Detail.Text);
        Assert.Equal("billing", result.Billing.Text);
        Assert.Equal("owner", result.Name);
    }

    [Fact]
    public async Task Shared_ConcurrencyTokens_Follow_Sequential_Updates_Within_The_Same_Context()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<ConcurrencyContext> options = await CreateConcurrencyDatabaseAsync(stream, ct);
        await using ConcurrencyContext context = new(options);
        Document document = await context.Set<Document>().SingleAsync(ct);
        document.Detail.Revision = 2;
        document.Detail.Text = "detail changed";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(2, context.Entry(document).Property<int>("Revision").CurrentValue);
        Assert.Equal(2, document.Billing.Revision);
        document.Billing.Text = "billing changed";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Document result = await context.Set<Document>().SingleAsync(ct);
        Assert.Equal("detail changed", result.Detail.Text);
        Assert.Equal("billing changed", result.Billing.Text);
        Assert.Equal(2, result.Detail.Revision);
        Assert.Equal(2, result.Billing.Revision);
    }

    [Fact]
    public async Task Multiple_Owned_Changes_Use_One_Row_And_Refresh_All_Shared_Tokens()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<ConcurrencyContext> options = await CreateConcurrencyDatabaseAsync(stream, ct);
        await using ConcurrencyContext context = new(options);
        Document document = await context.Set<Document>().SingleAsync(ct);
        document.Detail.Text = "detail changed";
        document.Detail.Revision = 2;
        document.Billing.Text = "billing changed";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(2, context.Entry(document).Property<int>("Revision").CurrentValue);
        Assert.Equal(2, document.Billing.Revision);
        context.ChangeTracker.Clear();
        Document result = await context.Set<Document>().SingleAsync(ct);
        Assert.Equal("detail changed", result.Detail.Text);
        Assert.Equal("billing changed", result.Billing.Text);
        Assert.Equal(2, result.Detail.Revision);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Owner_Updates_Check_Original_Tokens_From_Owned_Entries(bool modifyOwned)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<OwnedTokenContext> options = new DbContextOptionsBuilder<OwnedTokenContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (OwnedTokenContext seed = new(options))
        {
            Assert.True(await seed.Database.EnsureCreatedAsync(ct));
            _ = seed.Add(new Document { Id = 1, Name = "owner", Detail = new RevisionDetail { Text = "detail", Revision = 1 } });
            Assert.Equal(1, await seed.SaveChangesAsync(ct));
        }

        await using OwnedTokenContext first = new(options);
        await using OwnedTokenContext stale = new(options);
        Document current = await first.Set<Document>().SingleAsync(ct);
        Document previous = await stale.Set<Document>().SingleAsync(ct);
        current.Detail.Revision = 2;
        Assert.Equal(1, await first.SaveChangesAsync(ct));
        previous.Name = "stale owner";
        if (modifyOwned)
        {
            previous.Detail.Text = "stale detail";
        }

        _ = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => stale.SaveChangesAsync(ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Owner_Deletes_Check_Original_Tokens_From_Owned_Entries(bool batch)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<OwnedTokenContext> options = new DbContextOptionsBuilder<OwnedTokenContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (OwnedTokenContext seed = new(options))
        {
            Assert.True(await seed.Database.EnsureCreatedAsync(ct));
            _ = seed.Add(new Document { Id = 1, Name = "owner", Detail = new RevisionDetail { Text = "detail", Revision = 1 } });
            if (batch)
            {
                _ = seed.Add(new Document { Id = 2, Name = "second", Detail = new RevisionDetail { Text = "detail", Revision = 1 } });
            }

            Assert.Equal(batch ? 2 : 1, await seed.SaveChangesAsync(ct));
        }

        await using OwnedTokenContext first = new(options);
        await using OwnedTokenContext stale = new(options);
        Document current = await first.Set<Document>().FirstAsync(ct);
        List<Document> previous = await stale.Set<Document>().ToListAsync(ct);
        current.Detail.Revision = 2;
        Assert.Equal(1, await first.SaveChangesAsync(ct));
        stale.RemoveRange(previous);

        _ = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => stale.SaveChangesAsync(ct));
        await using OwnedTokenContext verify = new(options);
        Assert.Equal(batch ? 2 : 1, await verify.Set<Document>().CountAsync(ct));
    }

    [Fact]
    public async Task Batched_Owner_Updates_Refresh_Shared_Tokens_For_Each_Row()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<ConcurrencyContext> options = await CreateConcurrencyDatabaseAsync(stream, ct);
        await using ConcurrencyContext context = new(options);
        Document second = new()
        {
            Id = 2,
            Name = "second",
            Detail = new RevisionDetail { Text = "detail", Revision = 1 },
            Billing = new RevisionDetail { Text = "billing", Revision = 1 },
        };
        _ = context.Add(second);
        context.Entry(second).Property<int>("Revision").CurrentValue = 1;
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        List<Document> documents = await context.Set<Document>().ToListAsync(ct);
        foreach (Document document in documents)
        {
            document.Name = "batch";
            document.Detail.Revision = 2;
            document.Billing.Text = "changed";
        }

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        foreach (Document document in documents)
        {
            Assert.Equal(2, context.Entry(document).Property<int>("Revision").CurrentValue);
            Assert.Equal(2, document.Billing.Revision);
            document.Name = "again";
        }

        Assert.Equal(2, await context.SaveChangesAsync(ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Calculated_Shared_Tokens_Refresh_After_Owner_And_Owned_Source_Changes(bool modifyOwner)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CalculatedTokenContext> options = new DbContextOptionsBuilder<CalculatedTokenContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using CalculatedTokenContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Document document = new()
        {
            Id = 1,
            Name = "owner",
            Quantity = 1,
            Detail = new RevisionDetail { Text = "detail", Quantity = 2 },
        };
        _ = context.Add(document);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(4, context.Entry(document).Property<int>("Revision").CurrentValue);
        Assert.Equal(4, document.Detail.Revision);
        if (modifyOwner)
        {
            document.Quantity++;
        }
        else
        {
            document.Detail.Quantity++;
        }

        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(5, context.Entry(document).Property<int>("Revision").CurrentValue);
        Assert.Equal(5, document.Detail.Revision);
        document.Detail.Text = "next save";
        Assert.Equal(1, await context.SaveChangesAsync(ct));
    }

    [Fact]
    public async Task Failed_Batches_Preserve_Calculated_Tokens_On_Unchanged_Row_Peers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CalculatedTokenContext> options = new DbContextOptionsBuilder<CalculatedTokenContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (CalculatedTokenContext seed = new(options))
        {
            Assert.True(await seed.Database.EnsureCreatedAsync(ct));
            seed.AddRange(
                new Document { Id = 1, Name = "first", Quantity = 1, Detail = new RevisionDetail { Text = "detail", Quantity = 2 } },
                new Document { Id = 2, Name = "second", Quantity = 1, Detail = new RevisionDetail { Text = "detail", Quantity = 2 } });
            Assert.Equal(2, await seed.SaveChangesAsync(ct));
        }

        await using CalculatedTokenContext stale = new(options);
        List<Document> documents = await stale.Set<Document>().ToListAsync(ct);
        Document first = documents.Single(document => document.Id == 1);
        Document second = documents.Single(document => document.Id == 2);
        await using (CalculatedTokenContext peer = new(options))
        {
            Document updated = await peer.Set<Document>().SingleAsync(document => document.Id == 2, ct);
            updated.Quantity = 2;
            Assert.Equal(1, await peer.SaveChangesAsync(ct));
        }

        first.Quantity = 2;
        second.Detail.Text = "stale detail";
        _ = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => stale.SaveChangesAsync(ct));
        Assert.Equal(4, stale.Entry(first).Property<int>("Revision").CurrentValue);
        Assert.Equal(4, first.Detail.Revision);
        Assert.Equal(4, stale.Entry(first.Detail).Property(detail => detail.Revision).OriginalValue);
        await using CalculatedTokenContext verify = new(options);
        Document result = await verify.Set<Document>().SingleAsync(document => document.Id == 1, ct);
        Assert.Equal(1, result.Quantity);
        Assert.Equal(4, result.Detail.Revision);
    }

    private static async Task<DbContextOptions<ConcurrencyContext>> CreateConcurrencyDatabaseAsync(Stream stream, CancellationToken ct)
    {
        DbContextOptions<ConcurrencyContext> options = new DbContextOptionsBuilder<ConcurrencyContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using ConcurrencyContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        Document document = new()
        {
            Id = 1,
            Name = "owner",
            Detail = new RevisionDetail { Text = "detail", Revision = 1 },
            Billing = new RevisionDetail { Text = "billing", Revision = 1 },
        };
        _ = context.Add(document);
        context.Entry(document).Property<int>("Revision").CurrentValue = 1;
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        return options;
    }

    private sealed class ConcurrencyContext(DbContextOptions<ConcurrencyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<Document>(entity =>
            {
                _ = entity.Property(document => document.Id).ValueGeneratedNever();
                _ = entity.Property(document => document.Name).HasMaxLength(40);
                _ = entity.Property<int>("Revision").HasColumnName("Revision").IsConcurrencyToken();
                _ = entity.OwnsOne(document => document.Detail, owned =>
                {
                    _ = owned.Property(detail => detail.Text).HasColumnName("DetailText").HasMaxLength(40);
                    _ = owned.Property(detail => detail.Revision).HasColumnName("Revision").IsConcurrencyToken();
                });
                _ = entity.OwnsOne(document => document.Billing, owned =>
                {
                    _ = owned.Property(detail => detail.Text).HasColumnName("BillingText").HasMaxLength(40);
                    _ = owned.Property(detail => detail.Revision).HasColumnName("Revision").IsConcurrencyToken();
                });
            });
    }

    private sealed class OwnedTokenContext(DbContextOptions<OwnedTokenContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<Document>(entity =>
            {
                _ = entity.Property(document => document.Id).ValueGeneratedNever();
                _ = entity.Property(document => document.Name).HasMaxLength(40);
                _ = entity.Ignore(document => document.Billing);
                _ = entity.OwnsOne(document => document.Detail, owned =>
                {
                    _ = owned.Property(detail => detail.Text).HasMaxLength(40);
                    _ = owned.Property(detail => detail.Revision).IsConcurrencyToken();
                });
            });
    }

    private sealed class CalculatedTokenContext(DbContextOptions<CalculatedTokenContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<Document>(entity =>
            {
                _ = entity.Property(document => document.Id).ValueGeneratedNever();
                _ = entity.Property(document => document.Name).HasMaxLength(40);
                _ = entity.Property<int>("Revision").HasColumnName("Revision")
                    .HasJetCalculatedExpression("[Quantity] + [OwnedQuantity] + 1").IsConcurrencyToken();
                _ = entity.Ignore(document => document.Billing);
                _ = entity.OwnsOne(document => document.Detail, owned =>
                {
                    _ = owned.Property(detail => detail.Text).HasMaxLength(40);
                    _ = owned.Property(detail => detail.Quantity).HasColumnName("OwnedQuantity");
                    _ = owned.Property(detail => detail.Revision).HasColumnName("Revision")
                        .HasJetCalculatedExpression("[Quantity] + [OwnedQuantity] + 1").IsConcurrencyToken();
                });
            });
    }

    private sealed class Document
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Quantity { get; set; }

        public RevisionDetail Detail { get; set; } = new();

        public RevisionDetail Billing { get; set; } = new();
    }

    private sealed class RevisionDetail
    {
        public string Text { get; set; } = string.Empty;

        public int Revision { get; set; }

        public int Quantity { get; set; }
    }
}
