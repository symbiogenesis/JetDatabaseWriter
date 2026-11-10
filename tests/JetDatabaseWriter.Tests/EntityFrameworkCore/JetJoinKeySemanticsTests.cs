namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetJoinKeySemanticsTests
{
    [Theory]
    [InlineData("Join")]
    [InlineData("GroupJoin")]
    [InlineData("LeftJoin")]
    public async Task CustomComparersPreserveMatchesOutsideExactKeys(string operation)
    {
        await using MemoryStream stream = new();
        await using JoinContext context = new(new DbContextOptionsBuilder<JoinContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        await SeedAsync(context);
        IEqualityComparer<int> comparer = new ParityComparer();
        IQueryable<int> query = operation switch
        {
            "Join" => context.OuterRows.Join(context.InnerRows, outer => outer.Number, inner => inner.Number, (outer, inner) => inner.Id, comparer),
            "GroupJoin" => context.OuterRows.GroupJoin(context.InnerRows, outer => outer.Number, inner => inner.Number, (outer, matches) => matches.Select(inner => inner.Id).Single(), comparer),
            "LeftJoin" => context.OuterRows.LeftJoin(context.InnerRows, outer => outer.Number, inner => inner.Number, (outer, inner) => inner == null ? -1 : inner.Id, comparer),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };

        Assert.Equal(2, Assert.Single(await query.ToArrayAsync(TestContext.Current.CancellationToken)));
    }

    [Theory]
    [InlineData("Join")]
    [InlineData("GroupJoin")]
    [InlineData("LeftJoin")]
    public async Task CompositeKeysPreserveNullComponentMatches(string operation)
    {
        await using MemoryStream stream = new();
        await using JoinContext context = new(new DbContextOptionsBuilder<JoinContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        await SeedAsync(context);
        IQueryable<int> query = operation switch
        {
            "Join" => context.OuterRows.Join(context.InnerRows, outer => new { outer.NullablePart, outer.OtherPart }, inner => new { inner.NullablePart, inner.OtherPart }, (outer, inner) => inner.Id),
            "GroupJoin" => context.OuterRows.GroupJoin(context.InnerRows, outer => new { outer.NullablePart, outer.OtherPart }, inner => new { inner.NullablePart, inner.OtherPart }, (outer, matches) => matches.Select(inner => inner.Id).Single()),
            "LeftJoin" => context.OuterRows.LeftJoin(context.InnerRows, outer => new { outer.NullablePart, outer.OtherPart }, inner => new { inner.NullablePart, inner.OtherPart }, (outer, inner) => inner == null ? -1 : inner.Id),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };

        Assert.Equal(2, Assert.Single(await query.ToArrayAsync(TestContext.Current.CancellationToken)));
    }

    private static async Task SeedAsync(JoinContext context)
    {
        _ = await context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken);
        context.OuterRows.Add(new OuterRow { Id = 1, Number = 1, OtherPart = 7 });
        context.InnerRows.Add(new InnerRow { Id = 2, Number = 3, OtherPart = 7 });
        _ = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        context.ChangeTracker.Clear();
    }

    private sealed class ParityComparer : IEqualityComparer<int>
    {
        public bool Equals(int x, int y) => x % 2 == y % 2;

        public int GetHashCode(int obj) => obj % 2;
    }

    private sealed class JoinContext(DbContextOptions<JoinContext> options) : DbContext(options)
    {
        public DbSet<OuterRow> OuterRows => this.Set<OuterRow>();

        public DbSet<InnerRow> InnerRows => this.Set<InnerRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<OuterRow>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<InnerRow>().Property(row => row.Id).ValueGeneratedNever();
        }
    }

    private sealed class OuterRow
    {
        public int Id { get; set; }

        public int Number { get; set; }

        public int? NullablePart { get; set; }

        public int OtherPart { get; set; }
    }

    private sealed class InnerRow
    {
        public int Id { get; set; }

        public int Number { get; set; }

        public int? NullablePart { get; set; }

        public int OtherPart { get; set; }
    }
}
