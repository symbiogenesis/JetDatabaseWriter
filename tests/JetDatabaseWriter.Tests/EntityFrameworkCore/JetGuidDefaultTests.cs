namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetGuidDefaultTests
{
    private static readonly Guid LiteralDefault = new("11111111-2222-3333-4444-555555555555");
    private static readonly Guid ExpressionDefault = new("66666666-7777-8888-9999-aaaaaaaaaaaa");
    private static readonly Guid CustomSentinel = new("bbbbbbbb-cccc-dddd-eeee-ffffffffffff");

    [Fact]
    public async Task ConfiguredGuidDefaults_OverrideTemporaryGeneration()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<GuidContext> options = new DbContextOptionsBuilder<GuidContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using GuidContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        GuidRow row = new() { Id = 1 };
        _ = context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(LiteralDefault, row.Literal);
        Assert.Equal(LiteralDefault, row.Custom);
        Assert.Equal(LiteralDefault, row.Optional);
        Assert.Equal(ExpressionDefault, row.Expression);
        context.ChangeTracker.Clear();
        GuidRow reloaded = await context.Set<GuidRow>().SingleAsync(ct);
        Assert.Equal(row.Literal, reloaded.Literal);
        Assert.Equal(row.Custom, reloaded.Custom);
        Assert.Equal(row.Optional, reloaded.Optional);
        Assert.Equal(row.Expression, reloaded.Expression);
    }

    [Fact]
    public async Task ConfiguredGuidDefaults_PreserveExplicitValuesAndCustomSentinel()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<GuidContext> options = new DbContextOptionsBuilder<GuidContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using GuidContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        GuidRow row = new() { Id = 1, Literal = ExpressionDefault, Custom = Guid.Empty, Optional = Guid.Empty, Expression = LiteralDefault };
        _ = context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        GuidRow reloaded = await context.Set<GuidRow>().SingleAsync(ct);
        Assert.Equal(ExpressionDefault, reloaded.Literal);
        Assert.Equal(Guid.Empty, reloaded.Custom);
        Assert.Equal(Guid.Empty, reloaded.Optional);
        Assert.Equal(LiteralDefault, reloaded.Expression);
    }

    [Fact]
    public async Task LiteralGuidKey_UsesConfiguredDefault()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<GuidKeyContext> options = new DbContextOptionsBuilder<GuidKeyContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using GuidKeyContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        GuidKeyRow row = new();
        _ = context.Add(row);
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(LiteralDefault, row.Id);
        context.ChangeTracker.Clear();
        Assert.Equal(LiteralDefault, (await context.Set<GuidKeyRow>().SingleAsync(ct)).Id);
    }

    private sealed class GuidContext(DbContextOptions<GuidContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<GuidRow>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<GuidRow>().Property(row => row.Literal).HasJetDefaultValue(LiteralDefault);
            _ = modelBuilder.Entity<GuidRow>().Property(row => row.Custom).HasJetDefaultValue(LiteralDefault).HasSentinel(CustomSentinel);
            _ = modelBuilder.Entity<GuidRow>().Property(row => row.Optional).HasJetDefaultValue(LiteralDefault);
            _ = modelBuilder.Entity<GuidRow>().Property(row => row.Expression).HasJetDefaultValueExpression("{guid 66666666-7777-8888-9999-aaaaaaaaaaaa}");
        }
    }

    private sealed class GuidKeyContext(DbContextOptions<GuidKeyContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<GuidKeyRow>().Property(row => row.Id).HasJetDefaultValue(LiteralDefault);
    }

    private sealed class GuidRow
    {
        public int Id { get; set; }

        public Guid Literal { get; set; }

        public Guid Custom { get; set; } = CustomSentinel;

        public Guid? Optional { get; set; }

        public Guid Expression { get; set; }
    }

    private sealed class GuidKeyRow
    {
        public Guid Id { get; set; }
    }
}
