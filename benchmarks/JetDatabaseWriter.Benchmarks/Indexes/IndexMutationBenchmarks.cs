namespace JetDatabaseWriter.Benchmarks.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;

/// <summary>Measures page emission, compressed partitioning and indexed batch maintenance.</summary>
[MemoryDiagnoser]
public class IndexMutationBenchmarks : IAsyncDisposable
{
    private readonly List<IndexEntry> leafEntries = [];
    private readonly List<DecodedIntermediateEntry> intermediateEntries = [];
    private readonly List<IndexEntry> spliceEntries = [];
    private readonly List<IndexEntry> spliceAdditions = [];
    private readonly List<IndexEntry> unorderedSpliceAdditions = [];
    private readonly object?[][] sameLeafRows = CreateRows(1);
    private readonly object?[][] crossLeafRows = [.. CreateRows(1), .. CreateRows(80_001)];
    private byte[] baseline = [];
    private MemoryStream? image;
    private AccessWriter? writer;

    private AccessWriter ActiveWriter => this.writer ?? throw new InvalidOperationException("The iteration writer is not open.");

    /// <summary>Creates a multi-leaf index and shared-prefix key fixtures outside measurement.</summary>
    /// <returns>The setup completion.</returns>
    /// <exception cref="InvalidOperationException">The fixture does not retain every expected entry or row.</exception>
    [GlobalSetup]
    public async Task Setup()
    {
        for (int i = 0; i < 600; i++)
        {
            byte[] key = new byte[128];
            key[0] = checked((byte)(i / 100));
            key[^1] = checked((byte)(i % 100));
            var entry = new IndexEntry(key, 100 + i, 0);
            this.intermediateEntries.Add(new(entry, 1000 + i));
            this.spliceEntries.Add(entry);
            if (i % 10 == 0)
            {
                this.spliceAdditions.Add(new(key, 100 + i, 1));
            }

            if (i < 40)
            {
                this.leafEntries.Add(entry);
            }
        }

        this.unorderedSpliceAdditions.AddRange(this.spliceAdditions);
        this.unorderedSpliceAdditions.Reverse();
        this.ValidateSplice(this.spliceEntries, this.spliceAdditions);
        this.ValidateSplice(this.spliceEntries, this.unorderedSpliceAdditions);

        byte[] leaf = IndexPageCodec.BuildLeafPage(IndexPageLayout.Jet4, 4096, 7, this.leafEntries, true);
        if (IndexPageCodec.DecodeLeafEntries(IndexPageLayout.Jet4, leaf, 4096).Count != this.leafEntries.Count)
        {
            throw new InvalidOperationException("The page emission fixture lost entries.");
        }

        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, Options(), leaveOpen: true))
        {
            await writer.CreateTableAsync("Indexed", [new("Id", typeof(int))], [new("IX_Id", "Id")]);
            object?[][] seed = new object?[1000][];
            for (int i = 0; i < seed.Length; i++)
            {
                seed[i] = [i * 100];
            }

            if (await writer.InsertRowsAsync("Indexed", seed) != seed.Length)
            {
                throw new InvalidOperationException("The indexed mutation fixture lost seed rows.");
            }
        }

        this.baseline = stream.ToArray();
    }

    /// <summary>Emits one prefix-compressed leaf page.</summary>
    /// <returns>The encoded page.</returns>
    [Benchmark]
    public byte[]? EmitLeaf()
        => IndexPageCodec.TryBuildLeafPage(IndexPageLayout.Jet4, 4096, 7, this.leafEntries);

    /// <summary>Partitions long intermediate keys whose shared prefix changes between groups.</summary>
    /// <returns>The partition count.</returns>
    /// <exception cref="InvalidOperationException">The fixture does not require a split.</exception>
    [Benchmark]
    public int SplitIntermediate()
        => IndexHelpers.TryGreedySplitIntermediateInN(IndexPageLayout.Jet4, 4096, 7, this.intermediateEntries)?.Count
            ?? throw new InvalidOperationException("The intermediate fixture did not split.");

    /// <summary>Splices sorted shared-prefix entries with duplicate-key additions.</summary>
    /// <returns>The resulting entry count.</returns>
    /// <exception cref="InvalidOperationException">The valid splice fixture was refused.</exception>
    [Benchmark]
    public int SpliceSortedEntries()
        => IndexEntrySplicer.Splice(this.spliceEntries, this.spliceAdditions, [])?.Count
            ?? throw new InvalidOperationException("The sorted splice fixture was refused.");

    /// <summary>Splices sorted entries with reversed additions through the general fallback.</summary>
    /// <returns>The resulting entry count.</returns>
    /// <exception cref="InvalidOperationException">The valid splice fixture was refused.</exception>
    [Benchmark]
    public int SpliceUnorderedEntries()
        => IndexEntrySplicer.Splice(this.spliceEntries, this.unorderedSpliceAdditions, [])?.Count
            ?? throw new InvalidOperationException("The unordered splice fixture was refused.");

    /// <summary>Inserts a batch whose keys share one target leaf.</summary>
    /// <returns>The inserted row count.</returns>
    [Benchmark]
    public ValueTask<int> InsertSameLeaf()
        => this.ActiveWriter.InsertRowsAsync("Indexed", this.sameLeafRows);

    /// <summary>Inserts a batch spanning two target leaves.</summary>
    /// <returns>The inserted row count.</returns>
    [Benchmark]
    public ValueTask<int> InsertCrossLeaf()
        => this.ActiveWriter.InsertRowsAsync("Indexed", this.crossLeafRows);

#pragma warning disable RS0030 // BenchmarkDotNet requires synchronous iteration hooks outside measurement.
    /// <summary>Opens a fresh copy of the seeded database before destructive mutations.</summary>
    [IterationSetup(Targets = [nameof(InsertSameLeaf), nameof(InsertCrossLeaf)])]
    public void PrepareIteration() => this.PrepareIterationAsync().GetAwaiter().GetResult();

    /// <summary>Closes the mutated database outside measurement.</summary>
    [IterationCleanup(Targets = [nameof(InsertSameLeaf), nameof(InsertCrossLeaf)])]
    public void CleanupIteration() => this.DisposeAsync().AsTask().GetAwaiter().GetResult();
#pragma warning restore RS0030

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (this.writer is { } activeWriter)
        {
            this.writer = null;
            await activeWriter.DisposeAsync();
        }

        this.image?.Dispose();
        this.image = null;
        GC.SuppressFinalize(this);
    }

    private static AccessWriterOptions Options() => new()
    {
        UseTransactionalWrites = false,
        UseLockFile = false,
        UseByteRangeLocks = false,
    };

    private static object?[][] CreateRows(int first)
    {
        object?[][] rows = new object?[16][];
        for (int i = 0; i < rows.Length; i++)
        {
            rows[i] = [first + i];
        }

        return rows;
    }

    private void ValidateSplice(List<IndexEntry> existing, List<IndexEntry> additions)
    {
        IndexEntry[] original = [.. existing];
        IndexEntry[] added = [.. additions];
        List<IndexEntry> result = IndexEntrySplicer.Splice(existing, additions, [])
            ?? throw new InvalidOperationException("The splice fixture was refused.");
        if (result.Count != this.spliceEntries.Count + this.spliceAdditions.Count
            || !original.SequenceEqual(existing) || !added.SequenceEqual(additions))
        {
            throw new InvalidOperationException("The splice fixture changed its inputs or entry count.");
        }

        int offset = 0;
        for (int i = 0; i < this.spliceEntries.Count; i++)
        {
            if (result[offset++] != this.spliceEntries[i]
                || (i % 10 == 0 && result[offset++] != this.spliceAdditions[i / 10]))
            {
                throw new InvalidOperationException("The splice fixture changed key or tie ordering.");
            }
        }
    }

    private async Task PrepareIterationAsync()
    {
        this.image = new MemoryStream();
        this.image.Write(this.baseline);
        this.image.Position = 0;
        this.writer = await AccessWriter.OpenAsync(this.image, Options(), leaveOpen: true, cancellationToken: CancellationToken.None);
    }
}
