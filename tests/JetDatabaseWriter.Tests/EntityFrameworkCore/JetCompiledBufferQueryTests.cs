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

public sealed class JetCompiledBufferQueryTests
{
    [Fact]
    public async Task EmptyBufferedCompiledSequence_ObservesEnumeratorCancellation()
    {
        await using MemoryStream stream = new();
        await using ReviewContext context = await CreateContextAsync(stream);
        Func<ReviewContext, IAsyncEnumerable<int>> query = EF.CompileAsyncQuery((ReviewContext db) => db.Rows.OrderByDescending(x => x.Id).Where(x => x.Id < 0).Select(x => x.Id));
        CancellationToken canceled = new(canceled: true);
        await using IAsyncEnumerator<int> enumerator = query(context).GetAsyncEnumerator(canceled);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());
    }

    [Fact]
    public async Task EmptyShapedCompiledSequence_ObservesEnumeratorCancellation()
    {
        await using MemoryStream stream = new();
        await using ReviewContext context = await CreateContextAsync(stream);
        var query = EF.CompileAsyncQuery((ReviewContext db) => db.Rows.OrderByDescending(x => x.Id).Where(x => x.Id < 0).Select(x => new { Entity = x }));
        CancellationToken canceled = new(canceled: true);
        await using var enumerator = query(context).GetAsyncEnumerator(canceled);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());
    }

    [Fact]
    public async Task CompiledWindowQuery_RebindsParametersAcrossCallsAndContexts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream firstStream = new();
        await using ReviewContext first = await CreateContextAsync(firstStream);
        Func<ReviewContext, int, int, IAsyncEnumerable<int>> query = EF.CompileAsyncQuery((ReviewContext db, int window, int minimum) =>
            db.Rows.Take(window).Where(x => x.Id >= minimum).OrderByDescending(x => x.Id).Select(x => x.Id));
        int[] firstValues = await ReadAsync(query(first, 5, 3), ct);
        Assert.Equal([5, 4, 3], firstValues);
        Assert.Empty(await ReadAsync(query(first, 2, 3), ct));
        int[] repeatedValues = await ReadAsync(query(first, 8, 7), ct);
        Assert.Equal([8, 7], repeatedValues);
        await using MemoryStream secondStream = new();
        await using ReviewContext second = await CreateContextAsync(secondStream);
        int[] secondValues = await ReadAsync(query(second, 4, 2), ct);
        Assert.Equal([4, 3, 2], secondValues);
        Assert.Empty(await ReadAsync(query(second, 0, 0), ct));
    }

    private static async Task<int[]> ReadAsync(IAsyncEnumerable<int> sequence, CancellationToken cancellationToken)
    {
        List<int> result = [];
        await foreach (int value in sequence.WithCancellation(cancellationToken))
        {
            result.Add(value);
        }

        return [.. result];
    }

    private static async Task<ReviewContext> CreateContextAsync(Stream stream)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ReviewContext context = new(new DbContextOptionsBuilder<ReviewContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        try
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            context.Rows.AddRange(Enumerable.Range(1, 8).Select(id => new ReviewRow { Id = id }));
            Assert.Equal(8, await context.SaveChangesAsync(ct));
            context.ChangeTracker.Clear();
            return context;
        }
        catch
        {
            await context.DisposeAsync();
            throw;
        }
    }

    private sealed class ReviewContext(DbContextOptions<ReviewContext> options) : DbContext(options)
    {
        public DbSet<ReviewRow> Rows => this.Set<ReviewRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            _ = modelBuilder.Entity<ReviewRow>().Property(x => x.Id).ValueGeneratedNever();
    }

    private sealed class ReviewRow
    {
        public int Id { get; set; }
    }
}
