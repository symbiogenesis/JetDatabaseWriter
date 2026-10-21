namespace JetDatabaseWriter.Benchmarks.Queries;

using System;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Linq;

/// <summary>
/// Compares aggregate allocations at two row counts with the same typed streaming read.
/// Both paths allocate the entities they read; the difference isolates query and aggregate
/// overhead, which must stay constant instead of retaining a growing row buffer.
/// </summary>
[MemoryDiagnoser]
public class QueryAggregateAllocationBenchmarks
{
    private AccessReader? reader;

    /// <summary>Gets or sets the number of rows each aggregate consumes.</summary>
    [Params(1000, SyntheticDatabases.QueryLargeRows)]
    public int RowCount { get; set; }

    private AccessReader Reader => this.reader ?? throw new InvalidOperationException("The reader was not initialized.");

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureQueryAsync().ConfigureAwait(false);
        this.reader = await AccessReader.OpenAsync(SyntheticDatabases.QueryDbPath).ConfigureAwait(false);
        int expected = this.RowCount / 1000 * 499500;
        if (await this.Rows_Sum_Large().ConfigureAwait(false) != expected
            || await this.Query_SumAsync_Large().ConfigureAwait(false) != expected)
        {
            throw new InvalidOperationException("The streamed aggregate did not return the expected sum.");
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Sums the same mapped rows directly, without the query provider or aggregate operator.</summary>
    /// <returns>The sum of their Score values.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> Rows_Sum_Large()
    {
        int sum = 0;
        await foreach (QueryBenchmarks.ScoreRow row in this.Reader.Rows<QueryBenchmarks.ScoreRow>(SyntheticDatabases.QueryLargeTable)
            .Take(this.RowCount).ConfigureAwait(false))
        {
            sum = checked(sum + row.Score);
        }

        return sum;
    }

    /// <summary>Sums the mapped rows through the streaming query aggregate.</summary>
    /// <returns>The sum of their Score values.</returns>
    [Benchmark]
    public async Task<int> Query_SumAsync_Large() =>
        await this.Reader.Query<QueryBenchmarks.ScoreRow>(SyntheticDatabases.QueryLargeTable)
            .Take(this.RowCount)
            .SumAsync(row => row.Score)
            .ConfigureAwait(false);
}
