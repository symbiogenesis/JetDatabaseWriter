namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Benchmarks.Models;
using JetDatabaseWriter.Enums;

/// <summary>
/// Full scans of <see cref="SyntheticDatabases.LargeLongValueTable"/>, whose
/// 1.5M-character MEMOs and 2 MB OLE values each span more LVAL pages than the
/// default 256-page cache, across page-cache sizes and read modes. Decoding such
/// a chain evicts the data page the scan is still on; when the cache returned
/// evicted pages to the shared pool, the next page read overwrote that page and
/// every row after the value was lost. The other long-value fixtures fit in the
/// cache, which is how that bug went unseen. Setup runs every benchmark once and
/// throws unless it returns all 30 rows with every value at its full length, so a
/// regression shows as NA instead of as a fast result.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderLargeLongValueBenchmarks
{
    private AccessReader? reader;

    [Params(0, 8, 256)]
    public int PageCacheSize { get; set; }

    [Params(PageReadOptimizationMode.Disabled, PageReadOptimizationMode.Auto)]
    public PageReadOptimizationMode PageReadOptimizationMode { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureLargeLongValueAsync().ConfigureAwait(false);
        this.reader = await AccessReader.OpenAsync(SyntheticDatabases.LargeLongValueDbPath, this.CreateOptions()).ConfigureAwait(false);

        (await this.FullScan_Rows().ConfigureAwait(false)).Verify(nameof(this.FullScan_Rows));
        (await this.FullScan_RowsTyped().ConfigureAwait(false)).Verify(nameof(this.FullScan_RowsTyped));
        (await this.ColdOpen_FullScan().ConfigureAwait(false)).Verify(nameof(this.ColdOpen_FullScan));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Warm <c>Rows()</c> scan through the reader opened in setup.</summary>
    /// <returns>The rows read and the MEMO and OLE lengths they carried.</returns>
    /// <exception cref="InvalidOperationException">Setup has not opened the reader.</exception>
    [Benchmark(Baseline = true)]
    public Task<LongValueScanTotals> FullScan_Rows() =>
        ScanRowsAsync(this.reader ?? throw new InvalidOperationException("The reader was not initialized."));

    /// <summary>Warm <c>Rows&lt;T&gt;()</c> scan through the reader opened in setup.</summary>
    /// <returns>The rows read and the MEMO and OLE lengths they carried.</returns>
    /// <exception cref="InvalidOperationException">Setup has not opened the reader.</exception>
    [Benchmark]
    public async Task<LongValueScanTotals> FullScan_RowsTyped()
    {
        AccessReader primed = this.reader ?? throw new InvalidOperationException("The reader was not initialized.");
        int rows = 0;
        long memoChars = 0;
        long oleBytes = 0;
        await foreach (LargeLongValueRow row in primed.Rows<LargeLongValueRow>(SyntheticDatabases.LargeLongValueTable).ConfigureAwait(false))
        {
            rows++;
            memoChars += row.Body?.Length ?? 0;
            oleBytes += row.Blob?.Length ?? 0;
        }

        return new LongValueScanTotals(rows, memoChars, oleBytes);
    }

    /// <summary>Opens a new reader, so its page cache starts empty, and scans with <c>Rows()</c>.</summary>
    /// <returns>The rows read and the MEMO and OLE lengths they carried.</returns>
    [Benchmark]
    public async Task<LongValueScanTotals> ColdOpen_FullScan()
    {
        await using AccessReader cold = await AccessReader.OpenAsync(SyntheticDatabases.LargeLongValueDbPath, this.CreateOptions()).ConfigureAwait(false);
        return await ScanRowsAsync(cold).ConfigureAwait(false);
    }

    private static async Task<LongValueScanTotals> ScanRowsAsync(AccessReader reader)
    {
        int rows = 0;
        long memoChars = 0;
        long oleBytes = 0;
        await foreach (object[] row in reader.Rows(SyntheticDatabases.LargeLongValueTable).ConfigureAwait(false))
        {
            rows++;
            memoChars += (row[1] as string)?.Length ?? 0;
            oleBytes += (row[2] as byte[])?.Length ?? 0;
        }

        return new LongValueScanTotals(rows, memoChars, oleBytes);
    }

    private AccessReaderOptions CreateOptions() => new()
    {
        PageCacheSize = this.PageCacheSize,
        PageReadOptimizationMode = this.PageReadOptimizationMode,
    };
}

/// <summary>What one scan of <see cref="SyntheticDatabases.LargeLongValueTable"/> returned.</summary>
/// <param name="Rows">The rows read.</param>
/// <param name="MemoChars">The MEMO characters read.</param>
/// <param name="OleBytes">The OLE bytes read.</param>
public readonly record struct LongValueScanTotals(int Rows, long MemoChars, long OleBytes)
{
    /// <summary>Throws unless the scan returned every row with every value at its full length.</summary>
    /// <param name="scan">The benchmark that ran the scan, for the message.</param>
    /// <exception cref="InvalidOperationException">A row or part of a value is missing.</exception>
    public void Verify(string scan)
    {
        if (this.Rows != SyntheticDatabases.LargeLongValueRows
            || this.MemoChars != SyntheticDatabases.LargeLongValueMemoChars
            || this.OleBytes != SyntheticDatabases.LargeLongValueOleBytes)
        {
            throw new InvalidOperationException(
                $"{scan} returned {this.Rows} rows, {this.MemoChars} MEMO chars and {this.OleBytes} OLE bytes; expected "
                + $"{SyntheticDatabases.LargeLongValueRows} rows, {SyntheticDatabases.LargeLongValueMemoChars} MEMO chars and "
                + $"{SyntheticDatabases.LargeLongValueOleBytes} OLE bytes.");
        }
    }
}
