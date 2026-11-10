namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Xunit;

public sealed class JetLiteralDefaultTests
{
    [Fact]
    public async Task LiteralDefaults_NormalizeModelValuesBeforeProviderConversion()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<LiteralContext> options = new DbContextOptionsBuilder<LiteralContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;
        await using LiteralContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        LiteralRow row = new() { Id = 1 };
        context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        AssertDefaults(row);
        context.ChangeTracker.Clear();
        AssertDefaults(await context.Set<LiteralRow>().AsNoTracking().SingleAsync(ct));
    }

    [Fact]
    public async Task LiteralDefaults_PreserveExplicitValuesAndCustomSentinel()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<LiteralContext> options = new DbContextOptionsBuilder<LiteralContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;
        await using LiteralContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        LiteralRow row = new()
        {
            Id = 2,
            Widened = 15,
            Decimal = 16m,
            Optional = 0L,
            Day = DayOfWeek.Friday,
            OptionalDay = DayOfWeek.Sunday,
            Converted = 18,
            Code = new Code("explicit"),
            Priority = 0,
        };
        context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        LiteralRow reloaded = await context.Set<LiteralRow>().AsNoTracking().SingleAsync(ct);
        Assert.Equal(15L, reloaded.Widened);
        Assert.Equal(16m, reloaded.Decimal);
        Assert.Equal(0L, reloaded.Optional);
        Assert.Equal(DayOfWeek.Friday, reloaded.Day);
        Assert.Equal(DayOfWeek.Sunday, reloaded.OptionalDay);
        Assert.Equal(18L, reloaded.Converted);
        Assert.Equal(new Code("explicit"), reloaded.Code);
        Assert.Equal(0L, reloaded.Priority);
    }

    [Fact]
    public Task InvalidNumericDefault_RefusesBeforeDatabaseMutation()
        => AssertInvalidDefaultAsync<long>("not-a-number");

    [Fact]
    public Task OverflowingDefault_RefusesBeforeDatabaseMutation()
        => AssertInvalidDefaultAsync<byte>(300);

    [Fact]
    public Task UnsupportedBinaryDefault_RefusesBeforeDatabaseMutation()
        => AssertInvalidDefaultAsync<byte[]>(new byte[] { 1, 2 });

    [Fact]
    public Task ProviderTypedDefault_RefusesBeforeDatabaseMutation()
        => AssertInvalidDefaultAsync<Code>("provider-value");

    private static void AssertDefaults(LiteralRow row)
    {
        Assert.Equal(5L, row.Widened);
        Assert.Equal(6m, row.Decimal);
        Assert.Equal(7L, row.Optional);
        Assert.Equal(DayOfWeek.Tuesday, row.Day);
        Assert.Equal(DayOfWeek.Wednesday, row.OptionalDay);
        Assert.Equal(8L, row.Converted);
        Assert.Equal(new Code("default"), row.Code);
        Assert.Equal(9L, row.Priority);
    }

    private static async Task AssertInvalidDefaultAsync<T>(object defaultValue)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<InvalidContext<T>> options = new DbContextOptionsBuilder<InvalidContext<T>>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;
        await using InvalidContext<T> context = new(options, defaultValue);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.EnsureCreatedAsync(ct));
        Assert.Contains("default", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, stream.Length);
    }

    private readonly record struct Code(string Value);

    private sealed class LiteralContext(DbContextOptions<LiteralContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<LiteralRow>(entity =>
            {
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.Property(x => x.Widened).HasJetDefaultValue(5);
                _ = entity.Property(x => x.Decimal).HasJetDefaultValue(6);
                _ = entity.Property(x => x.Optional).HasJetDefaultValue(7);
                _ = entity.Property(x => x.Day).HasJetDefaultValue(2);
                _ = entity.Property(x => x.OptionalDay).HasJetDefaultValue(3);
                _ = entity.Property(x => x.Converted).HasConversion<string>().HasMaxLength(20).HasJetDefaultValue(8);
                _ = entity.Property(x => x.Code).HasConversion(x => x.Value, x => new Code(x)).HasMaxLength(20).HasJetDefaultValue(new Code("default"));
                _ = entity.Property(x => x.Priority).HasJetDefaultValue(9).HasSentinel(-1L);
            });
    }

    private sealed class InvalidContext<T>(DbContextOptions<InvalidContext<T>> options, object defaultValue) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity("InvalidRows", entity =>
            {
                _ = entity.Property<int>("Id").ValueGeneratedNever();
                _ = entity.HasKey("Id");
                PropertyBuilder<T> property = entity.Property<T>("Value");
                if (typeof(T) == typeof(Code))
                {
                    _ = property.HasConversion(new ValueConverter<Code, string>(x => x.Value, x => new Code(x))).HasMaxLength(20);
                }

                _ = property.HasJetDefaultValue(defaultValue);
            });
    }

    private sealed class LiteralRow
    {
        public int Id { get; set; }

        public long Widened { get; set; }

        public decimal Decimal { get; set; }

        public long? Optional { get; set; }

        public DayOfWeek Day { get; set; }

        public DayOfWeek? OptionalDay { get; set; }

        public long Converted { get; set; }

        public Code Code { get; set; }

        public long Priority { get; set; } = -1;
    }
}
