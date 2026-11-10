namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetExplicitGeneratedKeyTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_PreservesExplicitNegativeGeneratedKeys(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        IntegerRow integer = new() { Id = -7 };
        LongRow longInteger = new() { Id = -9 };
        context.AddRange(integer, longInteger);
        Assert.False(context.Entry(integer).Property(row => row.Id).IsTemporary);
        Assert.False(context.Entry(longInteger).Property(row => row.Id).IsTemporary);

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        Assert.Equal(-7, integer.Id);
        Assert.Equal(-9, longInteger.Id);
        context.ChangeTracker.Clear();
        Assert.Equal(-7, (await context.Set<IntegerRow>().SingleAsync(ct)).Id);
        Assert.Equal(-9, (await context.Set<LongRow>().SingleAsync(ct)).Id);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_PreservesExplicitZeroWithCustomSentinel(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        SentinelRow row = new() { Id = 0 };
        _ = context.Add(row);
        Assert.False(context.Entry(row).Property(item => item.Id).IsTemporary);

        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(0, row.Id);
        context.ChangeTracker.Clear();
        Assert.Equal(0, (await context.Set<SentinelRow>().SingleAsync(ct)).Id);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SaveChanges_ReplacesTemporaryGeneratedKeys(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using KeyContext context = CreateContext(stream, format);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        IntegerRow integer = new();
        SentinelRow sentinel = new() { Id = -1 };
        context.AddRange(integer, sentinel);
        Assert.True(context.Entry(integer).Property(row => row.Id).IsTemporary);
        Assert.True(context.Entry(sentinel).Property(row => row.Id).IsTemporary);

        Assert.Equal(2, await context.SaveChangesAsync(ct));
        Assert.True(integer.Id > 0);
        Assert.True(sentinel.Id > 0);
    }

    private static KeyContext CreateContext(Stream stream, DatabaseFormat format)
        => new(new DbContextOptionsBuilder<KeyContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options);

    private sealed class KeyContext(DbContextOptions<KeyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<IntegerRow>();
            _ = modelBuilder.Entity<LongRow>();
            _ = modelBuilder.Entity<SentinelRow>().Property(row => row.Id).HasSentinel(-1);
        }
    }

    private sealed class IntegerRow
    {
        public int Id { get; set; }
    }

    private sealed class LongRow
    {
        public long Id { get; set; }
    }

    private sealed class SentinelRow
    {
        public int Id { get; set; }
    }
}
