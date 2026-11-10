namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetOrderedStreamingAllocationTests
{
    [Fact(DisableParallelization = true)]
    public async Task FirstOrderedRow_DoesNotAllocateBuffersForTheWholeTable()
    {
        await using MemoryStream smallStream = new();
        await using MemoryStream largeStream = new();
        await using AllocationContext small = await CreateAsync(smallStream, 100);
        await using AllocationContext large = await CreateAsync(largeStream, 4000);
        for (int warmup = 0; warmup < 3; warmup++)
        {
            _ = await ReadFirstRowAsync(small);
            _ = await ReadFirstRowAsync(large);
        }

        long smallBytes = await MeasureFirstRowAsync(small);
        long largeBytes = await MeasureFirstRowAsync(large);
        Assert.True(largeBytes <= smallBytes + 131_072, $"First-row allocation grew from {smallBytes:N0} to {largeBytes:N0} bytes for 100 versus 4,000 rows.");
    }

    private static async Task<AllocationContext> CreateAsync(Stream stream, int count)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        AllocationContext context = new(new DbContextOptionsBuilder<AllocationContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        await context.Database.EnsureCreatedAsync(ct);
        context.Items.AddRange(Enumerable.Range(1, count).Select(static id => new Item { Id = id }));
        await context.SaveChangesAsync(ct);
        context.ChangeTracker.Clear();
        return context;
    }

    private static async Task<long> MeasureFirstRowAsync(AllocationContext context)
    {
        long before = GC.GetTotalAllocatedBytes(precise: true);
        Assert.Equal(1, await ReadFirstRowAsync(context));
        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    private static async Task<int> ReadFirstRowAsync(AllocationContext context)
    {
        await using IAsyncEnumerator<int> rows = context.Items.AsNoTracking().OrderBy(item => item.Id).Select(item => item.Id)
            .AsAsyncEnumerable().GetAsyncEnumerator(TestContext.Current.CancellationToken);
        Assert.True(await rows.MoveNextAsync());
        return rows.Current;
    }

    private sealed class AllocationContext(DbContextOptions<AllocationContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
    }

    private sealed class Item
    {
        public int Id { get; set; }
    }
}
