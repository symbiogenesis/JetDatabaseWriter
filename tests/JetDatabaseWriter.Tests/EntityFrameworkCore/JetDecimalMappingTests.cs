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

public sealed class JetDecimalMappingTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DecimalDefaults_UseEnumAndConverterProviderTypes(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<NumericContext> options = new DbContextOptionsBuilder<NumericContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using (NumericContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            context.Add(new NumericRow
            {
                Kind = UnsignedKind.Maximum,
                ConvertedUnsigned = new UnsignedValue(ulong.MaxValue),
                DirectUnsigned = ulong.MaxValue,
                ConvertedDuration = new DurationValue(TimeSpan.MinValue),
                DirectDuration = TimeSpan.MaxValue,
                ConvertedFraction = 1234,
                ExplicitUnsigned = 12_345_678,
                ExplicitDecimal = 12.345m,
            });
            Assert.Equal(1, await context.SaveChangesAsync(ct));
            context.ChangeTracker.Clear();
            NumericRow loaded = await context.Set<NumericRow>().SingleAsync(ct);
            Assert.Equal(UnsignedKind.Maximum, loaded.Kind);
            Assert.Equal(new UnsignedValue(ulong.MaxValue), loaded.ConvertedUnsigned);
            Assert.Equal(ulong.MaxValue, loaded.DirectUnsigned);
            Assert.Equal(new DurationValue(TimeSpan.MinValue), loaded.ConvertedDuration);
            Assert.Equal(TimeSpan.MaxValue, loaded.DirectDuration);
            Assert.Equal(1234UL, loaded.ConvertedFraction);
            Assert.Equal(12_345_678UL, loaded.ExplicitUnsigned);
            Assert.Equal(12.34m, loaded.ExplicitDecimal);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("NumericRows", ct);
        foreach (string name in new[]
        {
            nameof(NumericRow.Kind), nameof(NumericRow.ConvertedUnsigned), nameof(NumericRow.DirectUnsigned),
            nameof(NumericRow.ConvertedDuration), nameof(NumericRow.DirectDuration),
        })
        {
            ColumnMetadata column = columns.Single(c => c.Name == name);
            Assert.Equal(20, column.NumericPrecision);
            Assert.Equal(0, column.NumericScale);
        }

        ColumnMetadata fraction = columns.Single(static c => c.Name == nameof(NumericRow.ConvertedFraction));
        Assert.Equal(18, fraction.NumericPrecision);
        Assert.Equal(4, fraction.NumericScale);
        ColumnMetadata explicitUnsigned = columns.Single(static c => c.Name == nameof(NumericRow.ExplicitUnsigned));
        Assert.Equal(8, explicitUnsigned.NumericPrecision);
        Assert.Equal(0, explicitUnsigned.NumericScale);
        ColumnMetadata explicitDecimal = columns.Single(static c => c.Name == nameof(NumericRow.ExplicitDecimal));
        Assert.Equal(12, explicitDecimal.NumericPrecision);
        Assert.Equal(2, explicitDecimal.NumericScale);
    }

    private enum UnsignedKind : ulong
    {
        Maximum = ulong.MaxValue,
    }

    private readonly record struct UnsignedValue(ulong Value);

    private readonly record struct DurationValue(TimeSpan Value);

    private sealed class NumericContext(DbContextOptions<NumericContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<NumericRow>(entity =>
            {
                _ = entity.ToTable("NumericRows");
                _ = entity.Property(x => x.ConvertedUnsigned).HasConversion(x => x.Value, x => new UnsignedValue(x));
                _ = entity.Property(x => x.ConvertedDuration).HasConversion(x => x.Value, x => new DurationValue(x));
                _ = entity.Property(x => x.ConvertedFraction).HasConversion(x => x / 1000m, x => (ulong)(x * 1000m));
                _ = entity.Property(x => x.ExplicitUnsigned).HasPrecision(8, 0);
                _ = entity.Property(x => x.ExplicitDecimal).HasPrecision(12, 2);
            });
    }

    private sealed class NumericRow
    {
        public int Id { get; set; }

        public UnsignedKind Kind { get; set; }

        public UnsignedValue ConvertedUnsigned { get; set; }

        public ulong DirectUnsigned { get; set; }

        public DurationValue ConvertedDuration { get; set; }

        public TimeSpan DirectDuration { get; set; }

        public ulong ConvertedFraction { get; set; }

        public ulong ExplicitUnsigned { get; set; }

        public decimal ExplicitDecimal { get; set; }
    }
}