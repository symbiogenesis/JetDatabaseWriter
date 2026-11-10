namespace JetDatabaseWriter.Benchmarks.Queries;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;

/// <summary>Measures ordered EF queries with diagnostics disabled by the default logging configuration.</summary>
[MemoryDiagnoser]
#pragma warning disable CA1001 // BenchmarkDotNet invokes GlobalCleanup to dispose the context and stream.
public class EfOrderedQueryBenchmarks
#pragma warning restore CA1001
{
    private MemoryStream? stream;
    private OrderedContext? context;

    /// <summary>Gets or sets the table size.</summary>
    [Params(100, 10000)]
    public int RowCount { get; set; }

    private OrderedContext Context => this.context ?? throw new InvalidOperationException("Setup has not completed.");

    /// <summary>Builds a primary-key indexed table and checks each measured query.</summary>
    /// <returns>The asynchronous setup.</returns>
    /// <exception cref="InvalidOperationException">A measured query returned unexpected rows.</exception>
    [GlobalSetup]
    public async Task Setup()
    {
        this.stream = new MemoryStream();
        DbContextOptions<OrderedContext> options = new DbContextOptionsBuilder<OrderedContext>().UseJetDatabaseWriter(this.stream, leaveOpen: true).Options;
        await using (OrderedContext schema = new(options))
        {
            await schema.Database.EnsureCreatedAsync().ConfigureAwait(false);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(this.stream, leaveOpen: true).ConfigureAwait(false))
        {
            await writer.InsertRowsAsync("Items", Enumerable.Range(1, this.RowCount).Select(static id => new object?[] { id, id % 2 == 0 })).ConfigureAwait(false);
        }

        this.context = new OrderedContext(options);
        if (await this.OrderedFirstRow().ConfigureAwait(false) != 1
            || await this.OrderedFilteredPage().ConfigureAwait(false) != 110
            || await this.UnorderedFirstRow().ConfigureAwait(false) != 1)
        {
            throw new InvalidOperationException("The EF query benchmark returned unexpected rows.");
        }
    }

    /// <summary>Releases the context and database image.</summary>
    /// <returns>The asynchronous cleanup.</returns>
    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.context is not null)
        {
            await this.context.DisposeAsync().ConfigureAwait(false);
        }

        if (this.stream is not null)
        {
            await this.stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Requests one row from an unbounded ordered projection.</summary>
    /// <returns>The first primary key.</returns>
    [Benchmark]
    public async Task<int> OrderedFirstRow()
    {
        await using IAsyncEnumerator<int> rows = this.Context.Items.AsNoTracking().OrderBy(static item => item.Id).Select(static item => item.Id).AsAsyncEnumerable().GetAsyncEnumerator();
        return await rows.MoveNextAsync().ConfigureAwait(false) ? rows.Current : 0;
    }

    /// <summary>Reads ten matching rows in index order with a residual filter.</summary>
    /// <returns>The sum of the ten smallest even primary keys.</returns>
    [Benchmark]
    public async Task<int> OrderedFilteredPage()
    {
        int[] rows = await this.Context.Items.AsNoTracking().Where(static item => item.Active).OrderBy(static item => item.Id).Take(10).Select(static item => item.Id).ToArrayAsync().ConfigureAwait(false);
        return rows.Sum();
    }

    /// <summary>Qualifies disabled diagnostics overhead on an unchanged streaming path.</summary>
    /// <returns>The first primary key.</returns>
    [Benchmark]
    public async Task<int> UnorderedFirstRow()
    {
        await using IAsyncEnumerator<int> rows = this.Context.Items.AsNoTracking().Select(static item => item.Id).AsAsyncEnumerable().GetAsyncEnumerator();
        return await rows.MoveNextAsync().ConfigureAwait(false) ? rows.Current : 0;
    }

    private sealed class OrderedContext(DbContextOptions<OrderedContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>().ToTable("Items");
            modelBuilder.Entity<Item>().Property(static item => item.Id).ValueGeneratedNever();
        }
    }

#pragma warning disable CA1812 // EF compiles this entity's materializer; the benchmark projects raw scalar buffers.
    private sealed class Item
    {
        public int Id { get; set; }

        public bool Active { get; set; }
    }
#pragma warning restore CA1812
}
