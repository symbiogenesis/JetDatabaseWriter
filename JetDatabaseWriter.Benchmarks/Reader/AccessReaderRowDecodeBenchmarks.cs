namespace JetDatabaseWriter.Benchmarks.Reader;

using System.Data;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// Per-row decode benchmarks: isolate per-row decode cost from the
/// <c>OpenAsync</c> floor by pre-opening the reader once in
/// <c>[GlobalSetup]</c>. The legacy <see cref="AccessReaderBenchmarks"/>
/// class is left unchanged so historical numbers remain comparable.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderRowDecodeBenchmarks
{
    private AccessReader numericReader = null!;
    private AccessReader textReader = null!;
    private AccessReader wideReader = null!;
    private AccessReader numericReaderRescan = null!;
    private AccessReader memoReader = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureAllAsync().ConfigureAwait(false);
        this.numericReader = await AccessReader.OpenAsync(SyntheticDatabases.NumericDbPath).ConfigureAwait(false);
        this.textReader = await AccessReader.OpenAsync(SyntheticDatabases.TextDbPath).ConfigureAwait(false);
        this.wideReader = await AccessReader.OpenAsync(SyntheticDatabases.WideDbPath).ConfigureAwait(false);

        // Dedicated reader for the row-bounds re-scan benchmark. Sized to hold
        // every data page of NumericTable so the second pass is a pure cache
        // hit and the row-bounds memoization shows up cleanly.
        this.numericReaderRescan = await AccessReader.OpenAsync(
            SyntheticDatabases.NumericDbPath,
            new AccessReaderOptions { PageCacheSize = 2048 }).ConfigureAwait(false);
        this.memoReader = await AccessReader.OpenAsync(SyntheticDatabases.MemoDbPath).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await this.numericReader.DisposeAsync().ConfigureAwait(false);
        await this.textReader.DisposeAsync().ConfigureAwait(false);
        await this.wideReader.DisposeAsync().ConfigureAwait(false);
        await this.numericReaderRescan.DisposeAsync().ConfigureAwait(false);
        await this.memoReader.DisposeAsync().ConfigureAwait(false);
    }

    // ── Numeric / date-heavy ──────────────────────────────────────────

    [Benchmark]
    public async Task<int> Decode_Numeric_Untyped()
    {
        int count = 0;
        await foreach (object[] row in this.numericReader.Rows(SyntheticDatabases.NumericTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Numeric_Typed()
    {
        int count = 0;
        await foreach (NumericRow? row in this.numericReader.Rows<NumericRow>(SyntheticDatabases.NumericTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Numeric_AsStrings()
    {
        int count = 0;
        await foreach (string[] row in this.numericReader.RowsAsStrings(SyntheticDatabases.NumericTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Numeric_DataTable()
    {
        DataTable dt = await this.numericReader.ReadDataTableAsync(SyntheticDatabases.NumericTable).ConfigureAwait(false);
        return dt.Rows.Count;
    }

    // ── Text-heavy ────────────────────────────────────────────────────

    [Benchmark]
    public async Task<int> Decode_Text_Untyped()
    {
        int count = 0;
        await foreach (object[] row in this.textReader.Rows(SyntheticDatabases.TextTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Text_Typed()
    {
        int count = 0;
        await foreach (TextRow? row in this.textReader.Rows<TextRow>(SyntheticDatabases.TextTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Text_AsStrings()
    {
        int count = 0;
        await foreach (string[] row in this.textReader.RowsAsStrings(SyntheticDatabases.TextTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Text_DataTable()
    {
        DataTable dt = await this.textReader.ReadDataTableAsync(SyntheticDatabases.TextTable).ConfigureAwait(false);
        return dt.Rows.Count;
    }

    // ── Wide (40 cols, narrow DTO binds 4) ────────────────────────────

    [Benchmark]
    public async Task<int> Decode_Wide_Untyped()
    {
        int count = 0;
        await foreach (object[] row in this.wideReader.Rows(SyntheticDatabases.WideTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Wide_Typed_NarrowProjection()
    {
        int count = 0;
        await foreach (WideRowNarrowProjection? row in this.wideReader.Rows<WideRowNarrowProjection>(SyntheticDatabases.WideTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    // ── Re-scan (row-bounds cache) ──────────────────────────────────
    // Two passes over the same table inside one op. With the page cache
    // sized to hold every data page (default 256, NumericTable fits),
    // the second pass should hit the row-bounds memo on every page and
    // skip the per-page parse work the first pass paid.

    [Benchmark]
    public async Task<int> Decode_Numeric_Untyped_TwoPass()
    {
        int count = 0;
        for (int pass = 0; pass < 2; pass++)
        {
            await foreach (object[] row in this.numericReaderRescan.Rows(SyntheticDatabases.NumericTable).ConfigureAwait(false))
            {
                _ = row;
                count++;
            }
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Numeric_ColdOpen_FirstScan() => await CountColdUntypedRowsAsync(
            SyntheticDatabases.NumericDbPath,
            SyntheticDatabases.NumericTable,
            options: null).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Numeric_ColdOpen_FirstScan_CacheDisabled() => await CountColdUntypedRowsAsync(
            SyntheticDatabases.NumericDbPath,
            SyntheticDatabases.NumericTable,
            new AccessReaderOptions { PageCacheSize = 0 }).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Numeric_ColdOpen_FirstScan_LargeCache() => await CountColdUntypedRowsAsync(
            SyntheticDatabases.NumericDbPath,
            SyntheticDatabases.NumericTable,
            new AccessReaderOptions { PageCacheSize = 2048 }).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Numeric_ColdOpen_FirstScan_ParallelReads() => await CountColdUntypedRowsAsync(
            SyntheticDatabases.NumericDbPath,
            SyntheticDatabases.NumericTable,
            new AccessReaderOptions { ParallelPageReadsEnabled = true }).ConfigureAwait(false);

    // ── Memo (LVAL) decode ────────────────────────────────────────────
    // Mixes inline (32 B), single-LVAL-page (~2 KB), and chained-LVAL
    // (~16 KB) payloads so each benchmark op exercises all three branches
    // of ReadLongValueAsync / ReadLvalChainAsync. Establishes a baseline
    // for any future LVAL decode-path optimization.

    [Benchmark]
    public async Task<int> Decode_Memo_Untyped()
    {
        int count = 0;
        await foreach (object[] row in this.memoReader.Rows(SyntheticDatabases.MemoTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Memo_Typed()
    {
        int count = 0;
        await foreach (MemoRow? row in this.memoReader.Rows<Models.MemoRow>(SyntheticDatabases.MemoTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Memo_DataTable()
    {
        DataTable dt = await this.memoReader.ReadDataTableAsync(SyntheticDatabases.MemoTable).ConfigureAwait(false);
        return dt.Rows.Count;
    }

    // Isolated LVAL branches. These keep the mixed benchmark above intact while
    // making it obvious whether an optimization helped inline, single-page, or
    // chained long values.

    [Benchmark]
    public async Task<int> Decode_Memo_Inline_Untyped()
        => await CountUntypedRowsAsync(this.memoReader, SyntheticDatabases.MemoInlineTable).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Memo_SinglePage_Untyped()
        => await CountUntypedRowsAsync(this.memoReader, SyntheticDatabases.MemoSinglePageTable).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Memo_Chained_Untyped()
        => await CountUntypedRowsAsync(this.memoReader, SyntheticDatabases.MemoChainedTable).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Ole_Inline_Untyped()
        => await CountUntypedRowsAsync(this.memoReader, SyntheticDatabases.OleInlineTable).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Ole_SinglePage_Untyped()
        => await CountUntypedRowsAsync(this.memoReader, SyntheticDatabases.OleSinglePageTable).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Decode_Ole_Chained_Untyped()
        => await CountUntypedRowsAsync(this.memoReader, SyntheticDatabases.OleChainedTable).ConfigureAwait(false);

    private static async Task<int> CountUntypedRowsAsync(AccessReader reader, string tableName)
    {
        int count = 0;
        await foreach (object[] row in reader.Rows(tableName).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    private static async Task<int> CountColdUntypedRowsAsync(string databasePath, string tableName, AccessReaderOptions? options)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(databasePath, options).ConfigureAwait(false);
        return await CountUntypedRowsAsync(reader, tableName).ConfigureAwait(false);
    }
}
