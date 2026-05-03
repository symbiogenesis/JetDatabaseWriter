namespace JetDatabaseWriter.Benchmarks;

using System;
using System.IO;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Core;

/// <summary>
/// Phase 1 benchmarks: measure <see cref="AccessReader.OpenAsync(string, AccessReaderOptions?, System.Threading.CancellationToken)"/>
/// (catalog + system table parsing) in isolation across DBs of
/// different shapes. This is the baseline Phase 5 of the v2 plan
/// (open-floor work) needs.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderOpenBenchmarks
{
    private static readonly string NorthwindPath =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");

    [GlobalSetup]
    public Task Setup() => SyntheticDatabases.EnsureAllAsync();

    [Benchmark]
    public async Task<long> Open_Northwind()
    {
        await using var r = await AccessReader.OpenAsync(NorthwindPath).ConfigureAwait(false);
        return r.PageSize;
    }

    [Benchmark]
    public async Task<long> Open_Synthetic_Numeric()
    {
        await using var r = await AccessReader.OpenAsync(SyntheticDatabases.NumericDbPath).ConfigureAwait(false);
        return r.PageSize;
    }

    [Benchmark]
    public async Task<long> Open_Synthetic_Wide()
    {
        await using var r = await AccessReader.OpenAsync(SyntheticDatabases.WideDbPath).ConfigureAwait(false);
        return r.PageSize;
    }
}
