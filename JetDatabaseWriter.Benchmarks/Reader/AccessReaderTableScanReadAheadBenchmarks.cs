namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Enums;

public enum TableScanBenchmarkShape
{
    /// <summary>Numeric/date-heavy table.</summary>
    Numeric = 0,

    /// <summary>Short-text-heavy table.</summary>
    Text = 1,

    /// <summary>Wide mixed-column table.</summary>
    Wide = 2,
}

public enum TableScanBenchmarkTemperature
{
    /// <summary>Open a new reader and perform the first scan.</summary>
    ColdOpenFirstScan = 0,

    /// <summary>Reuse a primed reader for a repeat scan.</summary>
    WarmRepeatScan = 1,
}

/// <summary>
/// Measures the table-scan read-ahead path across simple table shapes.
/// Cold runs include open plus first scan; warm runs reuse a primed reader so
/// they isolate repeat enumeration with the reader and OS caches already hot.
/// The first-row benchmark separates startup latency from full-scan throughput.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderTableScanReadAheadBenchmarks
{
    private AccessReader? warmReader;
    private string databasePath = null!;
    private string tableName = null!;

    [Params(TableScanBenchmarkShape.Numeric, TableScanBenchmarkShape.Text, TableScanBenchmarkShape.Wide)]
    public TableScanBenchmarkShape Shape { get; set; }

    [Params(PageReadOptimizationMode.Disabled, PageReadOptimizationMode.Auto, PageReadOptimizationMode.Enabled)]
    public PageReadOptimizationMode PageReadOptimizationMode { get; set; }

    [Params(TableScanBenchmarkTemperature.ColdOpenFirstScan, TableScanBenchmarkTemperature.WarmRepeatScan)]
    public TableScanBenchmarkTemperature Temperature { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureAllAsync().ConfigureAwait(false);
        (this.databasePath, this.tableName) = ResolveShape(this.Shape);

        if (this.Temperature == TableScanBenchmarkTemperature.WarmRepeatScan)
        {
            this.warmReader = await AccessReader.OpenAsync(this.databasePath, this.CreateOptions()).ConfigureAwait(false);
            _ = await CountRowsAsync(this.warmReader, this.tableName).ConfigureAwait(false);
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.warmReader is not null)
        {
            await this.warmReader.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async Task<int> FullTableScan()
    {
        if (this.Temperature == TableScanBenchmarkTemperature.WarmRepeatScan)
        {
            return await CountRowsAsync(
                this.warmReader ?? throw new InvalidOperationException("Warm reader was not initialized."),
                this.tableName).ConfigureAwait(false);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(this.databasePath, this.CreateOptions()).ConfigureAwait(false);
        return await CountRowsAsync(reader, this.tableName).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<int> FirstRow()
    {
        if (this.Temperature == TableScanBenchmarkTemperature.WarmRepeatScan)
        {
            return await CountFirstRowAsync(
                this.warmReader ?? throw new InvalidOperationException("Warm reader was not initialized."),
                this.tableName).ConfigureAwait(false);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(this.databasePath, this.CreateOptions()).ConfigureAwait(false);
        return await CountFirstRowAsync(reader, this.tableName).ConfigureAwait(false);
    }

    private static async Task<int> CountRowsAsync(AccessReader reader, string tableName)
    {
        int count = 0;
        await foreach (object[] row in reader.Rows(tableName).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    private static async Task<int> CountFirstRowAsync(AccessReader reader, string tableName)
    {
        await foreach (object[] row in reader.Rows(tableName).ConfigureAwait(false))
        {
            _ = row;
            return 1;
        }

        return 0;
    }

    private static (string DatabasePath, string TableName) ResolveShape(TableScanBenchmarkShape shape)
        => shape switch
        {
            TableScanBenchmarkShape.Numeric => (SyntheticDatabases.NumericDbPath, SyntheticDatabases.NumericTable),
            TableScanBenchmarkShape.Text => (SyntheticDatabases.TextDbPath, SyntheticDatabases.TextTable),
            TableScanBenchmarkShape.Wide => (SyntheticDatabases.WideDbPath, SyntheticDatabases.WideTable),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, null),
        };

    private AccessReaderOptions CreateOptions()
        => new() { PageReadOptimizationMode = this.PageReadOptimizationMode };
}
