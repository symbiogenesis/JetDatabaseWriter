namespace JetDatabaseWriter.Benchmarks.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.TestSupport;

/// <summary>Compares physical page traffic with and without the writer frame cache.</summary>
[MemoryDiagnoser]
public class AccessWriterPageIoBenchmarks : IAsyncDisposable
{
    private byte[] baseline = [];
    private byte[] northwind = [];
    private MemoryStream? iterationImage;
    private PageTraceStream? trace;
    private AccessWriter? writer;
    private long reads;
    private long writes;
    private long flushes;
    private int distinctReads;

    /// <summary>Gets or sets the frame-cache capacity.</summary>
    [Params(0, 256)]
    public int PageCacheSize { get; set; }

    /// <summary>Gets or sets whether calls use private transactions.</summary>
    [Params(false, true)]
    public bool Transactional { get; set; }

    /// <summary>Creates unmeasured database images for each workload.</summary>
    /// <returns>The setup completion.</returns>
    [GlobalSetup]
    public async Task Setup()
    {
        using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, this.Options(), leaveOpen: true))
        {
            await writer.CreateTableAsync("PageIo", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 255)]);
            await writer.InsertRowAsync("PageIo", [1, "baseline"]);
        }

        this.baseline = stream.ToArray();
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");
        using var northwindStream = new MemoryStream();
        await using (FileStream source = File.OpenRead(path))
        {
            await source.CopyToAsync(northwindStream);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(northwindStream, this.Options(), leaveOpen: true))
        {
            await writer.CreateTableAsync("PageIo", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 255)]);
        }

        this.northwind = northwindStream.ToArray();
    }

    /// <summary>Reports physical page counts beside BenchmarkDotNet's timing report.</summary>
    [GlobalCleanup]
    public void Report()
        => Console.WriteLine($"Page IO: reads={this.reads}, distinct-reads={this.distinctReads}, writes={this.writes}, flushes={this.flushes}, cache={this.PageCacheSize}, transactional={this.Transactional}");

    /// <summary>Inserts 999 rows into a newly created ACCDB.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task NewAccdb_Bulk999()
        => this.RunAsync(async writer =>
        {
            IEnumerable<object?[]> rows = Enumerable.Range(1000, 999).Select(i => new object?[] { i, $"row-{i}" });
            _ = await writer.InsertRowsAsync("PageIo", rows);
        });

    /// <summary>Inserts one row into a newly created ACCDB.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task NewAccdb_Single()
        => this.RunAsync(async writer => await writer.InsertRowAsync("PageIo", [2, "single"]));

    /// <summary>Updates one row in a newly created ACCDB.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task NewAccdb_Update()
        => this.RunAsync(async writer =>
        {
            _ = await writer.UpdateRowsAsync("PageIo", "Id", 1, new Dictionary<string, object?> { ["Name"] = "updated" });
        });

    /// <summary>Deletes one row in a newly created ACCDB.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task NewAccdb_Delete()
        => this.RunAsync(async writer => { _ = await writer.DeleteRowsAsync("PageIo", "Id", 1); });

    /// <summary>Rewrites a table to add a column.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task NewAccdb_AddColumn()
        => this.RunAsync(async writer => await writer.AddColumnAsync("PageIo", new("Added", typeof(int))));

    /// <summary>Measures a session's first Northwind insert.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task Northwind_FirstInsert()
        => this.RunAsync(async writer => await writer.InsertRowAsync("PageIo", [1, "first"]));

    /// <summary>Measures a session's second Northwind insert after an unmeasured first insert.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task Northwind_SecondInsert()
        => this.RunAsync(async writer => await writer.InsertRowAsync("PageIo", [2, "second"]));

    /// <summary>Opens an unmeasured fresh image for the new-database workloads.</summary>
    /// <returns>The setup completion.</returns>
    [IterationSetup(Targets = [nameof(NewAccdb_Bulk999), nameof(NewAccdb_Single), nameof(NewAccdb_Update), nameof(NewAccdb_Delete), nameof(NewAccdb_AddColumn)])]
    public Task SetupNewIteration() => this.PrepareIterationAsync(this.baseline, warmInsert: false);

    /// <summary>Opens a fresh Northwind writer before timing the first insert.</summary>
    /// <returns>The setup completion.</returns>
    [IterationSetup(Target = nameof(Northwind_FirstInsert))]
    public Task SetupFirstNorthwindIteration() => this.PrepareIterationAsync(this.northwind, warmInsert: false);

    /// <summary>Opens and warms a Northwind writer before timing the second insert.</summary>
    /// <returns>The setup completion.</returns>
    [IterationSetup(Target = nameof(Northwind_SecondInsert))]
    public Task SetupSecondNorthwindIteration() => this.PrepareIterationAsync(this.northwind, warmInsert: true);

    /// <summary>Records page counts and disposes the iteration's resources outside measurement.</summary>
    /// <returns>The cleanup completion.</returns>
    [IterationCleanup]
    public Task CleanupIteration() => this.DisposeAsync().AsTask();

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (this.trace is { } activeTrace)
        {
            IReadOnlyDictionary<long, long> readCounts = activeTrace.ReadCounts(4096);
            this.reads = readCounts.Values.Sum();
            this.distinctReads = readCounts.Count;
            this.writes = activeTrace.WriteCounts(4096).Values.Sum();
            this.flushes = activeTrace.Flushes;
        }

        AccessWriter? activeWriter = this.writer;
        this.writer = null;
        try
        {
            if (activeWriter is not null)
            {
                await activeWriter.DisposeAsync();
            }
        }
        finally
        {
            this.trace?.Dispose();
            this.trace = null;
            this.iterationImage?.Dispose();
            this.iterationImage = null;
            GC.SuppressFinalize(this);
        }
    }

    private AccessWriterOptions Options() => new()
    {
        PageCacheSize = this.PageCacheSize,
        UseTransactionalWrites = this.Transactional,
        UseLockFile = false,
        UseByteRangeLocks = false,
    };

    private Task RunAsync(Func<AccessWriter, Task> operation)
        => operation(this.writer ?? throw new InvalidOperationException("The iteration writer is not open."));

    private async Task PrepareIterationAsync(byte[] image, bool warmInsert)
    {
        this.iterationImage = new MemoryStream();
        await this.iterationImage.WriteAsync(image);
        this.trace = new PageTraceStream(this.iterationImage);
        this.writer = await AccessWriter.OpenAsync(this.trace, this.Options(), leaveOpen: true);
        if (warmInsert)
        {
            await this.writer.InsertRowAsync("PageIo", [1, "warm"]);
        }

        this.trace.Reset();
    }
}