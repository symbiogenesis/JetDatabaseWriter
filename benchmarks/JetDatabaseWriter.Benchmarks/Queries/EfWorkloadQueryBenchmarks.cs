namespace JetDatabaseWriter.Benchmarks.Queries;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.TestSupport;
using Microsoft.EntityFrameworkCore;

/// <summary>Measures EF tracking, owned scalar projections, graph pages, GUID lookups and connection reopening.</summary>
[MemoryDiagnoser]
#pragma warning disable CA1001 // BenchmarkDotNet invokes GlobalCleanup to dispose the context and stream.
public class EfWorkloadQueryBenchmarks
#pragma warning restore CA1001
{
    private MemoryStream? stream;
    private string? path;
    private DbContextOptions<EfWorkloadContext>? options;
    private EfWorkloadContext? context;
    private Guid lookupKey;

    /// <summary>Gets or sets the number of parents; every parent has three children.</summary>
    [Params(100, 4000)]
    public int RowCount { get; set; }

    /// <summary>Gets or sets a value indicating whether the database uses a file instead of a caller stream.</summary>
    [Params(false, true)]
    public bool FileBacked { get; set; }

    private EfWorkloadContext Context => this.context ?? throw new InvalidOperationException("Setup has not completed.");

    private DbContextOptions<EfWorkloadContext> Options => this.options ?? throw new InvalidOperationException("Setup has not completed.");

    /// <summary>Builds the shared fixture and validates each measured query.</summary>
    /// <returns>The asynchronous setup.</returns>
    [GlobalSetup]
    public async Task Setup()
    {
        byte[] image = await EfWorkloadContext.CreateImageAsync(this.RowCount).ConfigureAwait(false);
        DbContextOptionsBuilder<EfWorkloadContext> builder = new();
        if (this.FileBacked)
        {
            this.path = Path.Combine(Path.GetTempPath(), $"jdw-ef-query-{Guid.NewGuid():N}.accdb");
            await File.WriteAllBytesAsync(this.path, image).ConfigureAwait(false);
            _ = builder.UseJetDatabaseWriter(this.path);
        }
        else
        {
            this.stream = new MemoryStream(image);
            _ = builder.UseJetDatabaseWriter(this.stream, leaveOpen: true);
        }

        this.options = builder.Options;
        this.context = new EfWorkloadContext(this.options);
        this.lookupKey = EfWorkloadData.ChildKey(this.RowCount);
        EfWorkloadParent[] page = await this.Context.Parents.AsNoTracking().OrderBy(parent => parent.Id)
            .Take(EfWorkloadData.PageSize).Include(parent => parent.Children).ToArrayAsync().ConfigureAwait(false);
        EfWorkloadData.VerifyPage(page.Select(parent => (parent.Id, parent.Score, parent.Detail?.Note, parent.Children.Count)), withChildren: true);
        Expect(nameof(this.OwnedScalarPage), 55, await this.OwnedScalarPage().ConfigureAwait(false));
        Expect(nameof(this.NoTrackingEntityPage), 110, await this.NoTrackingEntityPage().ConfigureAwait(false));
        Expect(nameof(this.TrackingEntityPage), 110, await this.TrackingEntityPage().ConfigureAwait(false));
        Expect(nameof(this.IncludeGraphPage), 30, await this.IncludeGraphPage().ConfigureAwait(false));
        Expect(nameof(this.GuidKeyLookup), ((this.RowCount - 1) / EfWorkloadData.ChildrenPerParent) + 1, await this.GuidKeyLookup().ConfigureAwait(false));
        Expect(nameof(this.ReopenedFirstRow), 1, await this.ReopenedFirstRow().ConfigureAwait(false));
        this.Context.ChangeTracker.Clear();
    }

    /// <summary>Releases contexts and removes this benchmark's private database.</summary>
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

        if (this.path is not null)
        {
            File.Delete(this.path);
        }
    }

    /// <summary>Projects only root keys from a model containing an owned reference.</summary>
    /// <returns>The sum of the first ten keys.</returns>
    [Benchmark]
    public async Task<int> OwnedScalarPage()
    {
        int[] rows = await this.Context.Parents.AsNoTracking().OrderBy(parent => parent.Id)
            .Take(EfWorkloadData.PageSize).Select(parent => parent.Id).ToArrayAsync().ConfigureAwait(false);
        return rows.Sum();
    }

    /// <summary>Materializes a no-tracking page with its owned references.</summary>
    /// <returns>The sum of the returned scores.</returns>
    [Benchmark]
    public async Task<int> NoTrackingEntityPage()
    {
        EfWorkloadParent[] rows = await this.Context.Parents.AsNoTracking().OrderBy(parent => parent.Id)
            .Take(EfWorkloadData.PageSize).ToArrayAsync().ConfigureAwait(false);
        return rows.Sum(parent => parent.Score);
    }

    /// <summary>Clears the preceding page and materializes a fresh tracked page.</summary>
    /// <returns>The sum of the returned scores.</returns>
    [Benchmark]
    public async Task<int> TrackingEntityPage()
    {
        this.Context.ChangeTracker.Clear();
        EfWorkloadParent[] rows = await this.Context.Parents.OrderBy(parent => parent.Id)
            .Take(EfWorkloadData.PageSize).ToArrayAsync().ConfigureAwait(false);
        return rows.Sum(parent => parent.Score);
    }

    /// <summary>Loads a no-tracking page of parents with their complete child collections.</summary>
    /// <returns>The number of children loaded.</returns>
    [Benchmark]
    public async Task<int> IncludeGraphPage()
    {
        EfWorkloadParent[] rows = await this.Context.Parents.AsNoTracking().OrderBy(parent => parent.Id)
            .Take(EfWorkloadData.PageSize).Include(parent => parent.Children).ToArrayAsync().ConfigureAwait(false);
        return rows.Sum(parent => parent.Children.Count);
    }

    /// <summary>Looks up a noninteger primary key and projects its foreign key.</summary>
    /// <returns>The matching parent key.</returns>
    [Benchmark]
    public Task<int> GuidKeyLookup()
        => this.Context.Children.Where(child => child.Id == this.lookupKey).Select(child => child.ParentId).SingleAsync();

    /// <summary>Opens a new context and reads one scalar; operating-system caches remain uncontrolled.</summary>
    /// <returns>The first parent key.</returns>
    [Benchmark]
    public async Task<int> ReopenedFirstRow()
    {
        await using EfWorkloadContext reopened = new(this.Options);
        return await reopened.Parents.AsNoTracking().OrderBy(parent => parent.Id).Select(parent => parent.Id).FirstAsync().ConfigureAwait(false);
    }

    private static void Expect(string operation, int expected, int actual)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException($"{operation} returned {actual}; expected {expected}.");
        }
    }
}
