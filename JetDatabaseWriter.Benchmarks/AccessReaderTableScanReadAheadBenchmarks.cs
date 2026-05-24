namespace JetDatabaseWriter.Benchmarks;

using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

public enum TableScanBenchmarkShape
{
    /// <summary>Numeric/date-heavy table.</summary>
    Numeric,

    /// <summary>Short-text-heavy table.</summary>
    Text,

    /// <summary>Wide mixed-column table.</summary>
    Wide,
}

public enum TableScanBenchmarkTemperature
{
    /// <summary>Open a new reader and perform the first scan.</summary>
    ColdOpenFirstScan,

    /// <summary>Reuse a primed reader for a repeat scan.</summary>
    WarmRepeatScan,
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
    private AccessReader? _warmReader;
    private string _databasePath = null!;
    private string _tableName = null!;

    [Params(TableScanBenchmarkShape.Numeric, TableScanBenchmarkShape.Text, TableScanBenchmarkShape.Wide)]
    public TableScanBenchmarkShape Shape { get; set; }

    [Params(false, true)]
    public bool ParallelPageReadsEnabled { get; set; }

    [Params(TableScanBenchmarkTemperature.ColdOpenFirstScan, TableScanBenchmarkTemperature.WarmRepeatScan)]
    public TableScanBenchmarkTemperature Temperature { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureAllAsync().ConfigureAwait(false);
        (_databasePath, _tableName) = ResolveShape(Shape);

        if (Temperature == TableScanBenchmarkTemperature.WarmRepeatScan)
        {
            _warmReader = await AccessReader.OpenAsync(_databasePath, CreateOptions()).ConfigureAwait(false);
            _ = await CountRowsAsync(_warmReader, _tableName).ConfigureAwait(false);
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_warmReader is not null)
        {
            await _warmReader.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async Task<int> FullTableScan()
    {
        if (Temperature == TableScanBenchmarkTemperature.WarmRepeatScan)
        {
            return await CountRowsAsync(
                _warmReader ?? throw new InvalidOperationException("Warm reader was not initialized."),
                _tableName).ConfigureAwait(false);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(_databasePath, CreateOptions()).ConfigureAwait(false);
        return await CountRowsAsync(reader, _tableName).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<int> FirstRow()
    {
        if (Temperature == TableScanBenchmarkTemperature.WarmRepeatScan)
        {
            return await CountFirstRowAsync(
                _warmReader ?? throw new InvalidOperationException("Warm reader was not initialized."),
                _tableName).ConfigureAwait(false);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(_databasePath, CreateOptions()).ConfigureAwait(false);
        return await CountFirstRowAsync(reader, _tableName).ConfigureAwait(false);
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
        => new() { ParallelPageReadsEnabled = ParallelPageReadsEnabled };
}
