namespace JetDatabaseWriter.Benchmarks.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;

/// <summary>
/// The per-call cost of typed reads and <c>Query&lt;T&gt;()</c> over the synthetic query
/// database: a 6-row table for the fixed cost of one call (materializer and predicate
/// compiles, translation, the in-memory tail), and a 25,000-row table with a primary key
/// and a non-unique Score index for index-ordered reads, range seeks and aggregates.
/// Setup runs each benchmark once and throws unless it returns the expected result.
/// The eager-loading baseline is <see cref="QueryIncludeBenchmarks"/>.
/// </summary>
[MemoryDiagnoser]
public class QueryBenchmarks
{
    private const int LookupId = 4;
    private const decimal MinimumPrice = 30m;
    private const int ScoreThreshold = 500;
    private const int PageSize = 10;

    private readonly int capturedId = LookupId;

    private readonly int capturedScore = ScoreThreshold;

    private AccessReader? reader;

    private AccessReader Reader => this.reader ?? throw new InvalidOperationException("The reader was not initialized.");

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureQueryAsync().ConfigureAwait(false);
        this.reader = await AccessReader.OpenAsync(SyntheticDatabases.QueryDbPath).ConfigureAwait(false);

        Expect(nameof(this.RowsTyped_SmallTable), SyntheticDatabases.QuerySmallRows, await this.RowsTyped_SmallTable().ConfigureAwait(false));
        Expect(nameof(this.Query_WhereCapturedPk_FirstOrDefault), LookupId, await this.Query_WhereCapturedPk_FirstOrDefault().ConfigureAwait(false));
        Expect(nameof(this.Query_WhereOrderBy_ToList_Small), 4, await this.Query_WhereOrderBy_ToList_Small().ConfigureAwait(false));
        Expect(nameof(this.Query_OrderByPk_Take10_Large), 45, await this.Query_OrderByPk_Take10_Large().ConfigureAwait(false));
        Expect(nameof(this.RowsPredicate_RangeSeek_First10_Large), PageSize, await this.RowsPredicate_RangeSeek_First10_Large().ConfigureAwait(false));
        Expect(nameof(this.Query_Select_ToList_Small), SyntheticDatabases.QuerySmallRows, await this.Query_Select_ToList_Small().ConfigureAwait(false));
        Expect(nameof(this.Query_SumAsync_Large), 12_487_500, await this.Query_SumAsync_Large().ConfigureAwait(false));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Streams the 6-row table through <c>Rows&lt;T&gt;</c>.</summary>
    /// <returns>The rows read.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> RowsTyped_SmallTable()
    {
        int count = 0;
        await foreach (SmallRow unused in this.Reader.Rows<SmallRow>(SyntheticDatabases.QuerySmallTable).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    /// <summary>Looks one row up by a captured primary-key value.</summary>
    /// <returns>The Id of the row found.</returns>
    /// <exception cref="InvalidOperationException">No row matched.</exception>
    [Benchmark]
    public async Task<int> Query_WhereCapturedPk_FirstOrDefault()
    {
        int id = this.capturedId;
        SmallRow? row = await this.Reader.Query<SmallRow>(SyntheticDatabases.QuerySmallTable)
            .Where(r => r.Id == id)
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
        return row?.Id ?? throw new InvalidOperationException("No row matched.");
    }

    /// <summary>Filters the 6-row table on a non-indexed column and sorts the result in memory.</summary>
    /// <returns>The rows returned.</returns>
    [Benchmark]
    public async Task<int> Query_WhereOrderBy_ToList_Small()
    {
        List<SmallRow> rows = await this.Reader.Query<SmallRow>(SyntheticDatabases.QuerySmallTable)
            .Where(r => r.Price >= MinimumPrice)
            .OrderBy(r => r.Name)
            .ToListAsync()
            .ConfigureAwait(false);
        return rows.Count;
    }

    /// <summary>Reads the first ten rows of the 25,000-row table in primary-key order.</summary>
    /// <returns>The sum of the ten Ids (0 to 9).</returns>
    [Benchmark]
    public async Task<int> Query_OrderByPk_Take10_Large()
    {
        List<ScoreRow> rows = await this.Reader.Query<ScoreRow>(SyntheticDatabases.QueryLargeTable)
            .OrderBy(r => r.Id)
            .Take(PageSize)
            .ToListAsync()
            .ConfigureAwait(false);
        return rows.Sum(r => r.Id);
    }

    /// <summary>
    /// Reads the first ten rows whose Score is at least a captured threshold through the
    /// inferred Score index; half the table is in range.
    /// </summary>
    /// <returns>The rows read.</returns>
    [Benchmark]
    public async Task<int> RowsPredicate_RangeSeek_First10_Large()
    {
        int threshold = this.capturedScore;
        int count = 0;
        await foreach (ScoreRow unused in this.Reader.Rows<ScoreRow>(SyntheticDatabases.QueryLargeTable, r => r.Score >= threshold).ConfigureAwait(false))
        {
            if (++count == PageSize)
            {
                break;
            }
        }

        return count;
    }

    /// <summary>Projects one column of the 6-row table through the in-memory tail.</summary>
    /// <returns>The values returned.</returns>
    [Benchmark]
    public async Task<int> Query_Select_ToList_Small()
    {
        List<string> names = await this.Reader.Query<SmallRow>(SyntheticDatabases.QuerySmallTable)
            .Select(r => r.Name)
            .ToListAsync()
            .ConfigureAwait(false);
        return names.Count;
    }

    /// <summary>Sums Score over the 25,000-row table.</summary>
    /// <returns>The sum.</returns>
    [Benchmark]
    public async Task<int> Query_SumAsync_Large() =>
        await this.Reader.Query<ScoreRow>(SyntheticDatabases.QueryLargeTable)
            .SumAsync(r => r.Score)
            .ConfigureAwait(false);

    private static void Expect(string benchmark, int expected, int actual)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException($"{benchmark} returned {actual}; expected {expected}.");
        }
    }

    /// <summary>A row of <see cref="SyntheticDatabases.QuerySmallTable"/>.</summary>
    public sealed class SmallRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public decimal Price { get; set; }
    }

    /// <summary>A row of <see cref="SyntheticDatabases.QueryLargeTable"/>.</summary>
    public sealed class ScoreRow
    {
        public int Id { get; set; }

        public int Score { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
