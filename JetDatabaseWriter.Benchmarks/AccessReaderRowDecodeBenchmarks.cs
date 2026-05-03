namespace JetDatabaseWriter.Benchmarks;

using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Models;
using JetDatabaseWriter.Core;

/// <summary>
/// Phase 1 benchmarks: isolate per-row decode cost from the
/// <c>OpenAsync</c> floor by pre-opening the reader once in
/// <c>[GlobalSetup]</c>. The legacy <see cref="AccessReaderBenchmarks"/>
/// class is left unchanged so the v1 plan's Phase 0/7 numbers remain
/// comparable. See <c>docs/design/read-perf-plan-v2.md</c>.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderRowDecodeBenchmarks
{
    private AccessReader _numericReader = null!;
    private AccessReader _textReader = null!;
    private AccessReader _wideReader = null!;
    private AccessReader _numericReaderRescan = null!;
    private AccessReader _memoReader = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureAllAsync().ConfigureAwait(false);
        _numericReader = await AccessReader.OpenAsync(SyntheticDatabases.NumericDbPath).ConfigureAwait(false);
        _textReader = await AccessReader.OpenAsync(SyntheticDatabases.TextDbPath).ConfigureAwait(false);
        _wideReader = await AccessReader.OpenAsync(SyntheticDatabases.WideDbPath).ConfigureAwait(false);

        // Dedicated reader for the Phase 6 re-scan benchmark. Sized to hold
        // every data page of NumericTable so the second pass is a pure cache
        // hit and the row-bounds memoization shows up cleanly.
        _numericReaderRescan = await AccessReader.OpenAsync(
            SyntheticDatabases.NumericDbPath,
            new AccessReaderOptions { PageCacheSize = 2048 }).ConfigureAwait(false);
        _memoReader = await AccessReader.OpenAsync(SyntheticDatabases.MemoDbPath).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _numericReader.DisposeAsync().ConfigureAwait(false);
        await _textReader.DisposeAsync().ConfigureAwait(false);
        await _wideReader.DisposeAsync().ConfigureAwait(false);
        await _numericReaderRescan.DisposeAsync().ConfigureAwait(false);
        await _memoReader.DisposeAsync().ConfigureAwait(false);
    }

    // ── Numeric / date-heavy ──────────────────────────────────────────

    [Benchmark]
    public async Task<int> Decode_Numeric_Untyped()
    {
        int count = 0;
        await foreach (object[] row in _numericReader.Rows(SyntheticDatabases.NumericTable).ConfigureAwait(false))
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
        await foreach (NumericRow row in _numericReader.Rows<NumericRow>(SyntheticDatabases.NumericTable).ConfigureAwait(false))
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
        await foreach (string[] row in _numericReader.RowsAsStrings(SyntheticDatabases.NumericTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Numeric_DataTable()
    {
        var dt = await _numericReader.ReadDataTableAsync(SyntheticDatabases.NumericTable).ConfigureAwait(false);
        return dt!.Rows.Count;
    }

    // ── Text-heavy ────────────────────────────────────────────────────

    [Benchmark]
    public async Task<int> Decode_Text_Untyped()
    {
        int count = 0;
        await foreach (object[] row in _textReader.Rows(SyntheticDatabases.TextTable).ConfigureAwait(false))
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
        await foreach (TextRow row in _textReader.Rows<TextRow>(SyntheticDatabases.TextTable).ConfigureAwait(false))
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
        await foreach (string[] row in _textReader.RowsAsStrings(SyntheticDatabases.TextTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Text_DataTable()
    {
        var dt = await _textReader.ReadDataTableAsync(SyntheticDatabases.TextTable).ConfigureAwait(false);
        return dt!.Rows.Count;
    }

    // ── Wide (40 cols, narrow DTO binds 4) ────────────────────────────

    [Benchmark]
    public async Task<int> Decode_Wide_Untyped()
    {
        int count = 0;
        await foreach (object[] row in _wideReader.Rows(SyntheticDatabases.WideTable).ConfigureAwait(false))
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
        await foreach (WideRowNarrowProjection row in _wideReader.Rows<WideRowNarrowProjection>(SyntheticDatabases.WideTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    // ── Re-scan (Phase 6 row-bounds cache) ────────────────────────────
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
            await foreach (object[] row in _numericReaderRescan.Rows(SyntheticDatabases.NumericTable).ConfigureAwait(false))
            {
                _ = row;
                count++;
            }
        }

        return count;
    }

    // ── Memo (LVAL) decode ────────────────────────────────────────────
    // Mixes inline (32 B), single-LVAL-page (~2 KB), and chained-LVAL
    // (~16 KB) payloads so each benchmark op exercises all three branches
    // of ReadLongValueAsync / ReadLvalChainAsync. Establishes a baseline
    // for any future LVAL decode-path optimization.

    [Benchmark]
    public async Task<int> Decode_Memo_Untyped()
    {
        int count = 0;
        await foreach (object[] row in _memoReader.Rows(SyntheticDatabases.MemoTable).ConfigureAwait(false))
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
        await foreach (Models.MemoRow row in _memoReader.Rows<Models.MemoRow>(SyntheticDatabases.MemoTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<int> Decode_Memo_DataTable()
    {
        var dt = await _memoReader.ReadDataTableAsync(SyntheticDatabases.MemoTable).ConfigureAwait(false);
        return dt!.Rows.Count;
    }
}
