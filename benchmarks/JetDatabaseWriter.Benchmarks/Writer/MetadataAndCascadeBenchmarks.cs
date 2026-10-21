namespace JetDatabaseWriter.Benchmarks.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

/// <summary>Measures complex-column schema changes and cascades carrying external Memo values.</summary>
[MemoryDiagnoser]
public class MetadataAndCascadeBenchmarks : IAsyncDisposable
{
    private const int Children = 32;
    private static readonly string Notes = new('m', 6_000);
    private byte[] baseline = [];
    private MemoryStream? stream;
    private AccessWriter? writer;

    /// <summary>Gets or sets the writer's frame-cache capacity.</summary>
    [Params(0, 256)]
    public int CacheSize { get; set; }

    /// <summary>Builds and validates an immutable fixture outside measurement.</summary>
    /// <returns>The setup completion.</returns>
    /// <exception cref="InvalidOperationException">A workload does not preserve the expected data.</exception>
    [GlobalSetup]
    public async Task Setup()
    {
        await using var image = new MemoryStream();
        await using (AccessWriter fixture = await AccessWriter.CreateDatabaseAsync(image, DatabaseFormat.AceAccdb, this.Options(), leaveOpen: true))
        {
            await fixture.CreateTableAsync("Parents", [new("Id", typeof(int)) { IsPrimaryKey = true }]);
            await fixture.CreateTableAsync("Children", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int)), new("Notes", typeof(string))]);
            await fixture.CreateTableAsync("Documents", [new("Id", typeof(int)), new("Files", typeof(byte[])) { IsAttachment = true }]);
            await fixture.InsertRowAsync("Parents", [1]);
            await fixture.InsertRowsAsync("Children", Enumerable.Range(1, Children).Select(id => new object?[] { id, 1, Notes }));
            await fixture.CreateRelationshipAsync(new RelationshipDefinition("Children_Parents", "Parents", ["Id"], "Children", ["ParentId"]) { CascadeUpdates = true });
        }

        this.baseline = image.ToArray();
        await this.PrepareAsync();
        await this.RenameComplexColumn();
        if (await this.CascadeKeyUpdate() != 1)
        {
            throw new InvalidOperationException("The cascade did not update one parent.");
        }

        await this.writer!.DisposeAsync();
        this.writer = null;
        this.stream!.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(this.stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true))
        {
            IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync("Documents");
            if (complex.Count != 1 || complex[0].ColumnName != "RenamedFiles")
            {
                throw new InvalidOperationException("The complex-column rename lost its catalog binding.");
            }

            int count = 0;
            await foreach (object?[] row in reader.Rows("Children"))
            {
                if (!Equals(row[1], 3) || !Equals(row[2], Notes))
                {
                    throw new InvalidOperationException("The cascade changed an unassigned child value.");
                }

                count++;
            }

            if (count != Children)
            {
                throw new InvalidOperationException("The cascade lost child rows.");
            }
        }

        await this.DisposeAsync();
    }

    /// <summary>Renames a complex column through the public schema workflow.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public ValueTask RenameComplexColumn() => this.writer!.RenameColumnAsync("Documents", "Files", "RenamedFiles");

    /// <summary>Moves a parent key and all children without changing their Memo payloads.</summary>
    /// <returns>The number of updated parent rows.</returns>
    [Benchmark]
    public ValueTask<int> CascadeKeyUpdate()
        => this.writer!.UpdateRowsAsync("Parents", "Id", 1, new Dictionary<string, object?> { ["Id"] = 3 });

#pragma warning disable RS0030 // BenchmarkDotNet requires synchronous iteration hooks outside measurement.
    /// <summary>Opens a fresh copy of the immutable fixture outside measurement.</summary>
    [IterationSetup]
    public void Prepare() => this.PrepareAsync().GetAwaiter().GetResult();

    /// <summary>Disposes the iteration outside measurement.</summary>
    [IterationCleanup]
    public void Cleanup() => this.DisposeAsync().AsTask().GetAwaiter().GetResult();
#pragma warning restore RS0030

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (this.writer is not null)
            {
                await this.writer.DisposeAsync();
            }
        }
        finally
        {
            this.writer = null;
            this.stream?.Dispose();
            this.stream = null;
            GC.SuppressFinalize(this);
        }
    }

    private AccessWriterOptions Options() => new() { PageCacheSize = this.CacheSize, UseLockFile = false, UseByteRangeLocks = false };

    private async Task PrepareAsync()
    {
        this.stream = new MemoryStream();
        await this.stream.WriteAsync(this.baseline);
        this.stream.Position = 0;
        this.writer = await AccessWriter.OpenAsync(this.stream, this.Options(), leaveOpen: true);
    }
}
