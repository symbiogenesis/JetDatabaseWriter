namespace JetDatabaseWriter.Benchmarks.Writer;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.TestSupport;
using Microsoft.EntityFrameworkCore;

/// <summary>Measures EF writes against a fresh copy of an unmeasured relational fixture.</summary>
[MemoryDiagnoser]
#pragma warning disable CA1001 // BenchmarkDotNet invokes IterationCleanup to dispose the stream.
public class EfWorkloadWriteBenchmarks
#pragma warning restore CA1001
{
    private const int ParentCount = 1000;
    private byte[] baselineImage = [];
    private MemoryStream? stream;
    private string? path;

    /// <summary>Gets or sets a value indicating whether the database uses a file instead of a caller stream.</summary>
    [Params(false, true)]
    public bool FileBacked { get; set; }

    private DbContextOptions<EfWorkloadContext> Options { get; set; } = null!;

    /// <summary>Builds one baseline image and checks each operation on its own copy.</summary>
    /// <returns>The asynchronous setup.</returns>
    [GlobalSetup]
    public async Task Setup()
    {
        this.baselineImage = await EfWorkloadContext.CreateImageAsync(ParentCount).ConfigureAwait(false);
        this.Reset();
        try
        {
            Expect(await this.SaveBatch().ConfigureAwait(false));
        }
        finally
        {
            this.Cleanup();
        }

        this.Reset();
        try
        {
            Expect(await this.BulkUpdate().ConfigureAwait(false));
        }
        finally
        {
            this.Cleanup();
        }

        this.Reset();
        try
        {
            Expect(await this.BulkDeleteWithCascade().ConfigureAwait(false));
        }
        finally
        {
            this.Cleanup();
        }
    }

    /// <summary>Copies the baseline before each destructive operation, outside measurement.</summary>
    [IterationSetup]
    public void Reset()
    {
        DbContextOptionsBuilder<EfWorkloadContext> builder = new();
        if (this.FileBacked)
        {
            this.path = Path.Combine(Path.GetTempPath(), $"jdw-ef-write-{Guid.NewGuid():N}.accdb");
            File.WriteAllBytes(this.path, this.baselineImage);
            _ = builder.UseJetDatabaseWriter(this.path);
        }
        else
        {
            this.stream = new MemoryStream();
            this.stream.Write(this.baselineImage);
            _ = builder.UseJetDatabaseWriter(this.stream, leaveOpen: true);
        }

        this.Options = builder.Options;
    }

    /// <summary>Releases the iteration image after its context has closed.</summary>
    [IterationCleanup]
    public void Cleanup()
    {
        this.stream?.Dispose();
        this.stream = null;
        if (this.path is not null)
        {
            File.Delete(this.path);
            this.path = null;
        }

        this.Options = null!;
    }

    /// <summary>Opens a context, tracks ten new rows and atomically saves the batch.</summary>
    /// <returns>The number of saved rows.</returns>
    [Benchmark]
    public async Task<int> SaveBatch()
    {
        await using EfWorkloadContext context = new(this.Options);
        context.Parents.AddRange(Enumerable.Range(ParentCount + 1, EfWorkloadData.PageSize)
            .Select(id => new EfWorkloadParent { Id = id, Score = id * 2, Name = "inserted" }));
        return await context.SaveChangesAsync().ConfigureAwait(false);
    }

    /// <summary>Opens a context and updates ten rows without constructing entities.</summary>
    /// <returns>The number of updated rows.</returns>
    [Benchmark]
    public async Task<int> BulkUpdate()
    {
        await using EfWorkloadContext context = new(this.Options);
        return await context.Parents.Where(parent => parent.Id <= EfWorkloadData.PageSize)
            .ExecuteUpdateAsync(setters => setters.SetProperty(parent => parent.Score, parent => parent.Score + 1)).ConfigureAwait(false);
    }

    /// <summary>Opens a context and deletes ten parents plus their thirty children.</summary>
    /// <returns>The number of deleted parent rows.</returns>
    [Benchmark]
    public async Task<int> BulkDeleteWithCascade()
    {
        await using EfWorkloadContext context = new(this.Options);
        return await context.Parents.Where(parent => parent.Id > ParentCount - EfWorkloadData.PageSize)
            .ExecuteDeleteAsync().ConfigureAwait(false);
    }

    private static void Expect(int actual)
    {
        if (actual != EfWorkloadData.PageSize)
        {
            throw new InvalidOperationException($"The EF write workload affected {actual} rows; expected {EfWorkloadData.PageSize}.");
        }
    }
}
