namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Enums;

public enum ConcurrentScanBenchmarkShape
{
    /// <summary>The unencrypted 25,000-row numeric database.</summary>
    Plain = 0,

    /// <summary>The same database encrypted as <c>AccdbAgileCfb</c>, so every page read also decrypts.</summary>
    AesEncrypted = 1,
}

/// <summary>
/// Runs <see cref="Concurrency"/> full scans of the 25,000-row numeric table at
/// once, either all on one shared reader or one scan on each of that many
/// path-opened readers. Scans on a shared reader share its page cache, its one
/// file handle and, on an AES-encrypted file, one set of AES transforms used
/// under a lock; separate readers share none of them. How a shared reader's
/// page reads meet depends on <see cref="PageReadOptimizationMode"/>:
/// <list type="bullet">
/// <item><see cref="PageReadOptimizationMode.Disabled"/> seeks and reads its
/// stream, so every page read the cache misses waits for the reader's I/O
/// gate.</item>
/// <item><see cref="PageReadOptimizationMode.Auto"/> on .NET 6 or later reads
/// pages through <c>RandomAccess</c> at an offset, which bypasses the gate.
/// Windows still serializes I/O on the reader's synchronous handle.</item>
/// </list>
/// Comparing the two benchmarks shows what that sharing costs or saves as
/// concurrency grows. Every scan must return all 25,000 rows, and setup runs
/// both benchmarks once to check that.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderConcurrentScanBenchmarks
{
    private AccessReader? sharedReader;
    private AccessReader[] separateReaders = [];

    [Params(1, 2, 4, 8)]
    public int Concurrency { get; set; }

    [Params(ConcurrentScanBenchmarkShape.Plain, ConcurrentScanBenchmarkShape.AesEncrypted)]
    public ConcurrentScanBenchmarkShape Shape { get; set; }

    [Params(PageReadOptimizationMode.Disabled, PageReadOptimizationMode.Auto)]
    public PageReadOptimizationMode PageReadOptimizationMode { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureAesNumericAsync().ConfigureAwait(false);
        this.sharedReader = await this.OpenReaderAsync().ConfigureAwait(false);
        this.separateReaders = new AccessReader[this.Concurrency];
        for (int i = 0; i < this.separateReaders.Length; i++)
        {
            this.separateReaders[i] = await this.OpenReaderAsync().ConfigureAwait(false);
        }

        _ = await this.SharedReader().ConfigureAwait(false);
        _ = await this.SeparateReaders().ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.sharedReader is not null)
        {
            await this.sharedReader.DisposeAsync().ConfigureAwait(false);
        }

        foreach (AccessReader reader in this.separateReaders)
        {
            await reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>All scans on the one reader opened in setup.</summary>
    /// <returns>The total rows read.</returns>
    /// <exception cref="InvalidOperationException">Setup has not opened the reader.</exception>
    [Benchmark(Baseline = true)]
    public Task<int> SharedReader()
    {
        AccessReader shared = this.sharedReader ?? throw new InvalidOperationException("The reader was not initialized.");
        var readers = new AccessReader[this.Concurrency];
        Array.Fill(readers, shared);
        return ScanConcurrentlyAsync(readers);
    }

    /// <summary>One scan on each of the readers opened in setup.</summary>
    /// <returns>The total rows read.</returns>
    [Benchmark]
    public Task<int> SeparateReaders() => ScanConcurrentlyAsync(this.separateReaders);

    private static async Task<int> ScanConcurrentlyAsync(AccessReader[] readers)
    {
        var scans = new Task<int>[readers.Length];
        for (int i = 0; i < scans.Length; i++)
        {
            AccessReader reader = readers[i];
            scans[i] = Task.Run(() => CountRowsAsync(reader));
        }

        int total = 0;
        foreach (int count in await Task.WhenAll(scans).ConfigureAwait(false))
        {
            if (count != SyntheticDatabases.NumericRows)
            {
                throw new InvalidOperationException(
                    $"A scan returned {count} of the {SyntheticDatabases.NumericRows} rows in '{SyntheticDatabases.NumericTable}'.");
            }

            total += count;
        }

        return total;
    }

    private static async Task<int> CountRowsAsync(AccessReader reader)
    {
        int count = 0;
        await foreach (object[] row in reader.Rows(SyntheticDatabases.NumericTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    private ValueTask<AccessReader> OpenReaderAsync() => this.Shape == ConcurrentScanBenchmarkShape.Plain
        ? AccessReader.OpenAsync(
            SyntheticDatabases.NumericDbPath,
            new AccessReaderOptions { PageReadOptimizationMode = this.PageReadOptimizationMode })
        : AccessReader.OpenAsync(
            SyntheticDatabases.AesNumericDbPath,
            new AccessReaderOptions(SyntheticDatabases.AesPassword) { PageReadOptimizationMode = this.PageReadOptimizationMode });
}
