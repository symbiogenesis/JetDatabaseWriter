namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Enums;

public enum ReadAheadEligibilityBenchmarkShape
{
    /// <summary>MEMO table mixing inline, single-page and chained values.</summary>
    Memo = 0,

    /// <summary>OLE table whose values each fill one LVAL page.</summary>
    Ole = 1,

    /// <summary>Numeric/date-heavy table with no long values.</summary>
    Numeric = 2,
}

/// <summary>
/// Measures warm full scans on the table shapes and cache sizes that
/// <c>TableReader.ShouldReadAheadTablePages</c> has excluded from read-ahead:
/// tables with MEMO or OLE columns (still excluded, since read-ahead measured no
/// gain on them) and page caches under three pages or turned off (eligible since
/// the cache stopped returning evicted pages to the shared pool). Each case
/// scans through a primed reader, so the OS cache is hot and the page cache holds
/// whatever it can keep. To measure a rule change, run this class against builds
/// with and without it; the in-run <c>Disabled</c> rows also turn off
/// random-access page reads, so they are not a read-ahead-only baseline.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderReadAheadEligibilityBenchmarks
{
    private AccessReader? reader;
    private string tableName = null!;

    [Params(ReadAheadEligibilityBenchmarkShape.Memo, ReadAheadEligibilityBenchmarkShape.Ole, ReadAheadEligibilityBenchmarkShape.Numeric)]
    public ReadAheadEligibilityBenchmarkShape Shape { get; set; }

    [Params(0, 2, 256)]
    public int PageCacheSize { get; set; }

    [Params(PageReadOptimizationMode.Disabled, PageReadOptimizationMode.Auto)]
    public PageReadOptimizationMode PageReadOptimizationMode { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureAllAsync().ConfigureAwait(false);
        (string databasePath, this.tableName) = this.Shape switch
        {
            ReadAheadEligibilityBenchmarkShape.Memo => (SyntheticDatabases.MemoDbPath, SyntheticDatabases.MemoTable),
            ReadAheadEligibilityBenchmarkShape.Ole => (SyntheticDatabases.MemoDbPath, SyntheticDatabases.OleSinglePageTable),
            ReadAheadEligibilityBenchmarkShape.Numeric => (SyntheticDatabases.NumericDbPath, SyntheticDatabases.NumericTable),
            _ => throw new ArgumentOutOfRangeException(nameof(this.Shape), this.Shape, null),
        };

        this.reader = await AccessReader.OpenAsync(
            databasePath,
            new AccessReaderOptions { PageCacheSize = this.PageCacheSize, PageReadOptimizationMode = this.PageReadOptimizationMode }).ConfigureAwait(false);
        _ = await this.FullTableScan().ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async Task<int> FullTableScan()
    {
        AccessReader primed = this.reader ?? throw new InvalidOperationException("The reader was not initialized.");
        int count = 0;
        await foreach (object[] row in primed.Rows(this.tableName).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }
}
