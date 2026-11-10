namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetJet3DecimalFacetTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Jet3_RejectsUnrepresentableDecimalFacets(bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<PreciseContext> options = new DbContextOptionsBuilder<PreciseContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.Jet3Mdb, leaveOpen: true).Options;
        await using PreciseContext context = new(options);
        NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            if (migrate)
            {
                await context.Database.MigrateAsync(ct);
            }
            else
            {
                _ = await context.Database.EnsureCreatedAsync(ct);
            }
        });
        Assert.Contains("Currency", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DecimalCapableFormats_PreserveExplicitScale(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<PreciseContext> options = new DbContextOptionsBuilder<PreciseContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using PreciseContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        context.Add(new NumericRow { Id = 1, Amount = 1.234567m });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(1.234567m, (await context.Set<NumericRow>().SingleAsync(ct)).Amount);
    }

    [Fact]
    public async Task Jet3_ExplicitCurrency_UsesCurrencyScale()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CurrencyContext> options = new DbContextOptionsBuilder<CurrencyContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.Jet3Mdb, leaveOpen: true).Options;
        await using CurrencyContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        context.Add(new NumericRow { Id = 1, Amount = 1.23456m });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(1.2346m, (await context.Set<NumericRow>().SingleAsync(ct)).Amount);
    }

    private sealed class PreciseContext(DbContextOptions<PreciseContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<NumericRow>().Property(row => row.Amount).HasPrecision(12, 6);
    }

    private sealed class CurrencyContext(DbContextOptions<CurrencyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<NumericRow>().Property(row => row.Amount).HasPrecision(12, 6).UseJetCurrency();
    }

    private sealed class NumericRow
    {
        public int Id { get; set; }

        public decimal Amount { get; set; }
    }
}
