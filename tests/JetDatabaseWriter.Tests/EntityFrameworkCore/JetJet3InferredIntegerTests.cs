namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetJet3InferredIntegerTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InferredIntegerStorage_RoundTripsRepresentableValues(bool negativeDuration)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<IntegerContext> options = CreateOptions(stream);
        await using IntegerContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        TimeSpan duration = TimeSpan.FromDays(400) + TimeSpan.FromTicks(1234567);
        if (negativeDuration)
        {
            duration = -duration;
        }

        IntegerRow row = new()
        {
            Id = 1,
            Duration = duration,
            Unsigned = 922_337_203_685_477UL,
            ConvertedDuration = new DurationValue(duration),
            ConvertedUnsigned = new UnsignedValue(922_337_203_685_477UL),
        };
        context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();

        IntegerRow loaded = await context.Set<IntegerRow>().SingleAsync(ct);
        Assert.Equal(row.Duration, loaded.Duration);
        Assert.Equal(row.Unsigned, loaded.Unsigned);
        Assert.Equal(row.ConvertedDuration, loaded.ConvertedDuration);
        Assert.Equal(row.ConvertedUnsigned, loaded.ConvertedUnsigned);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InferredIntegerStorage_RefusesOutOfRangeValuesWithoutChangingTheRow(bool unsigned)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<IntegerContext> options = CreateOptions(stream);
        await using IntegerContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        IntegerRow row = new() { Id = 1, Duration = TimeSpan.FromTicks(17), Unsigned = 23 };
        context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        if (unsigned)
        {
            row.Unsigned = ulong.MaxValue;
        }
        else
        {
            row.Duration = TimeSpan.MaxValue;
        }

        await Assert.ThrowsAsync<OverflowException>(() => context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        IntegerRow loaded = await context.Set<IntegerRow>().SingleAsync(ct);
        Assert.Equal(TimeSpan.FromTicks(17), loaded.Duration);
        Assert.Equal(23UL, loaded.Unsigned);
    }

    [Fact]
    public async Task ExplicitIntegerPrecision_StillRefusesUnrepresentableCapacity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<ExplicitIntegerContext> options = new DbContextOptionsBuilder<ExplicitIntegerContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.Jet3Mdb, leaveOpen: true).Options;
        await using ExplicitIntegerContext context = new(options);
        await Assert.ThrowsAsync<NotSupportedException>(() => context.Database.EnsureCreatedAsync(ct));
    }

    private static DbContextOptions<IntegerContext> CreateOptions(Stream stream)
        => new DbContextOptionsBuilder<IntegerContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.Jet3Mdb, leaveOpen: true).Options;

    private readonly record struct DurationValue(TimeSpan Value);

    private readonly record struct UnsignedValue(ulong Value);

    private sealed class IntegerContext(DbContextOptions<IntegerContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<IntegerRow>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<IntegerRow>().Property(row => row.ConvertedDuration).HasConversion(value => value.Value, value => new DurationValue(value));
            _ = modelBuilder.Entity<IntegerRow>().Property(row => row.ConvertedUnsigned).HasConversion(value => value.Value, value => new UnsignedValue(value));
        }
    }

    private sealed class ExplicitIntegerContext(DbContextOptions<ExplicitIntegerContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<IntegerRow>().Ignore(row => row.ConvertedDuration);
            _ = modelBuilder.Entity<IntegerRow>().Ignore(row => row.ConvertedUnsigned);
            _ = modelBuilder.Entity<IntegerRow>().Property(row => row.Duration).HasPrecision(20, 0);
        }
    }

    private sealed class IntegerRow
    {
        public int Id { get; set; }

        public TimeSpan Duration { get; set; }

        public ulong Unsigned { get; set; }

        public DurationValue ConvertedDuration { get; set; }

        public UnsignedValue ConvertedUnsigned { get; set; }
    }
}
