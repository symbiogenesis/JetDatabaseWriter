namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Xunit;

public sealed class JetLiteralDefaultFacetTests
{
    [Theory]
    [InlineData("date-extended")]
    [InlineData("date-minimum")]
    [InlineData("date-rounding")]
    public async Task DateDefaults_MatchGeneratedAndReloadedValues(string scenario)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FacetContext context = CreateContext(stream, scenario, DatabaseFormat.AceAccdb);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        FacetRow row = new() { Id = 1 };
        _ = context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal(row.Date, (await context.Set<FacetRow>().SingleAsync(ct)).Date);
    }

    [Theory]
    [InlineData("numeric-overflow", DatabaseFormat.AceAccdb)]
    [InlineData("numeric-rounding-overflow", DatabaseFormat.AceAccdb)]
    [InlineData("currency-overflow", DatabaseFormat.AceAccdb)]
    [InlineData("currency-rounding-overflow", DatabaseFormat.AceAccdb)]
    [InlineData("required-provider-null", DatabaseFormat.AceAccdb)]
    [InlineData("empty-text", DatabaseFormat.AceAccdb)]
    [InlineData("date-overflow", DatabaseFormat.AceAccdb)]
    [InlineData("inferred-currency-overflow", DatabaseFormat.Jet3Mdb)]
    [InlineData("autonumber-default", DatabaseFormat.AceAccdb)]
    [InlineData("calculated-default", DatabaseFormat.AceAccdb)]
    public async Task InvalidPhysicalDefault_RefusesBeforeMutation(string scenario, DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FacetContext context = CreateContext(stream, scenario, format);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.EnsureCreatedAsync(ct));

        Assert.Contains("default", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, stream.Length);
    }

    [Theory]
    [InlineData("numeric-rounding")]
    [InlineData("currency-rounding")]
    public async Task RoundedDefaults_MatchGeneratedAndReloadedValues(string scenario)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FacetContext context = CreateContext(stream, scenario, DatabaseFormat.AceAccdb);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        FacetRow row = new() { Id = 1 };
        _ = context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        decimal expectedNumeric = scenario == "numeric-rounding" ? 999m : 1.2346m;
        Assert.Equal(expectedNumeric, row.Numeric);
        Assert.Equal(1.235m, row.Optional);
        Assert.Equal(1.235m, row.Converted);
        context.ChangeTracker.Clear();
        FacetRow reloaded = await context.Set<FacetRow>().SingleAsync(ct);
        Assert.Equal(row.Numeric, reloaded.Numeric);
        Assert.Equal(row.Optional, reloaded.Optional);
        Assert.Equal(row.Converted, reloaded.Converted);
    }

    [Theory]
    [InlineData("date-extended")]
    [InlineData("date-minimum")]
    [InlineData("inferred-currency-overflow")]
    public async Task SupportedPhysicalDefault_CreatesSchema(string scenario)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using FacetContext context = CreateContext(stream, scenario, DatabaseFormat.AceAccdb);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
    }

    private static FacetContext CreateContext(MemoryStream stream, string scenario, DatabaseFormat format)
    {
        DbContextOptions<FacetContext> options = new DbContextOptionsBuilder<FacetContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .ReplaceService<IModelCacheKeyFactory, FacetCacheKeyFactory>().Options;
        return new FacetContext(options, scenario, format);
    }

#pragma warning disable CA1812 // EF dependency injection constructs the registered service.
    private sealed class FacetCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => (context.GetType(), ((FacetContext)context).Scenario, ((FacetContext)context).Format, designTime);
    }
#pragma warning restore CA1812

    private sealed class FacetContext(DbContextOptions<FacetContext> options, string scenario, DatabaseFormat format) : DbContext(options)
    {
        internal string Scenario { get; } = scenario;

        internal DatabaseFormat Format { get; } = format;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            EntityTypeBuilder<FacetRow> entity = modelBuilder.Entity<FacetRow>();
            _ = entity.Property(row => row.Id).ValueGeneratedNever();
            _ = entity.Ignore(row => row.Numeric).Ignore(row => row.Optional).Ignore(row => row.Converted)
                .Ignore(row => row.Text).Ignore(row => row.Date).Ignore(row => row.Large);
            switch (this.Scenario)
            {
                case "autonumber-default":
                    _ = entity.Property(row => row.Id).HasJetDefaultValue(5);
                    break;
                case "calculated-default":
                    _ = entity.Property(row => row.Numeric).HasJetCalculatedExpression("1 + 1").HasJetDefaultValue(5);
                    break;
                case "numeric-overflow":
                    _ = entity.Property(row => row.Numeric).HasPrecision(3, 0).HasJetDefaultValue(1000m);
                    break;
                case "numeric-rounding-overflow":
                    _ = entity.Property(row => row.Numeric).HasPrecision(3, 0).HasJetDefaultValue(999.5m);
                    break;
                case "currency-overflow":
                    _ = entity.Property(row => row.Numeric).UseJetCurrency().HasJetDefaultValue(decimal.MaxValue);
                    break;
                case "currency-rounding-overflow":
                    _ = entity.Property(row => row.Numeric).UseJetCurrency().HasJetDefaultValue(922337203685477.58075m);
                    break;
                case "required-provider-null":
                    _ = entity.Property(row => row.Text).IsRequired().HasConversion<string?>(value => value == "default" ? null : value, value => value!)
                        .HasMaxLength(20).HasJetDefaultValue("default");
                    break;
                case "empty-text":
                    _ = entity.Property(row => row.Text).HasMaxLength(20).HasJetAllowZeroLength(false).HasJetDefaultValue(string.Empty);
                    break;
                case "date-overflow":
                    _ = entity.Property(row => row.Date).HasJetDefaultValue(new DateTime(1, 1, 2));
                    break;
                case "date-extended":
                    _ = entity.Property(row => row.Date).UseJetDateTimeExtended().HasJetDefaultValue(new DateTime(1, 1, 2));
                    break;
                case "date-minimum":
                    _ = entity.Property(row => row.Date).HasJetDefaultValue(DateTime.MinValue);
                    break;
                case "date-rounding":
                    _ = entity.Property(row => row.Date).HasJetDefaultValue(new DateTime(2026, 11, 9).AddTicks(12345));
                    break;
                case "inferred-currency-overflow":
                    _ = entity.Property(row => row.Large).HasJetDefaultValue(ulong.MaxValue);
                    break;
                case "numeric-rounding":
                case "currency-rounding":
                    _ = entity.Property(row => row.Optional).HasPrecision(8, 3).HasJetDefaultValue(1.23456m);
                    _ = entity.Property(row => row.Converted).HasConversion(value => value * 10m, value => value / 10m)
                        .HasPrecision(8, 2).HasJetDefaultValue(1.23456m);
                    if (this.Scenario == "numeric-rounding")
                    {
                        _ = entity.Property(row => row.Numeric).HasPrecision(3, 0).HasJetDefaultValue(999.4m);
                    }
                    else
                    {
                        _ = entity.Property(row => row.Numeric).UseJetCurrency().HasJetDefaultValue(1.23456m);
                    }

                    break;
                default:
                    throw new InvalidOperationException($"Unknown literal-default scenario: {this.Scenario}.");
            }
        }
    }

    private sealed class FacetRow
    {
        public int Id { get; set; }

        public decimal Numeric { get; set; }

        public decimal? Optional { get; set; }

        public decimal Converted { get; set; }

        public string Text { get; set; } = string.Empty;

        public DateTime Date { get; set; }

        public ulong Large { get; set; }
    }
}
