namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Enums;
using Microsoft.Win32.SafeHandles;

/// <summary>
/// Measures opening a reader on a database file the OS has not cached and
/// scanning one table, where the reader's file handle and access hint matter
/// most. Each iteration first copies the database with
/// <c>FILE_FLAG_NO_BUFFERING</c> and write-through, so none of the copy's
/// pages are in the OS file cache when the reader opens it. Windows only.
/// </summary>
/// <remarks>
/// The first scan of the MEMO and OLE tables also reads every page of the
/// file to find the table's data pages, because the writer's inline
/// owned-pages map misses most of them (see <c>docs/todo.md</c>), so those
/// cases measure that whole-file pass as well.
/// </remarks>
[MemoryDiagnoser]
[InvocationCount(1)]
public class AccessReaderColdScanBenchmarks
{
    /// <summary><c>FILE_FLAG_NO_BUFFERING</c>, which <see cref="FileOptions"/> does not name.</summary>
    private const FileOptions NoBuffering = (FileOptions)0x20000000;

    /// <summary>Unbuffered writes need sector-aligned buffers, offsets and lengths.</summary>
    private const int Alignment = 4096;

    private const int CopyChunkBytes = 1 << 20;

    private string sourcePath = string.Empty;
    private string tableName = string.Empty;
    private string coldPath = string.Empty;

    [Params(ReadAheadEligibilityBenchmarkShape.Memo, ReadAheadEligibilityBenchmarkShape.Ole, ReadAheadEligibilityBenchmarkShape.Numeric)]
    public ReadAheadEligibilityBenchmarkShape Shape { get; set; }

    [Params(PageReadOptimizationMode.Disabled, PageReadOptimizationMode.Auto)]
    public PageReadOptimizationMode PageReadOptimizationMode { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException("The cold-scan benchmarks copy files with FILE_FLAG_NO_BUFFERING, which is Windows only.");
        }

        await SyntheticDatabases.EnsureAllAsync().ConfigureAwait(false);
        (this.sourcePath, this.tableName) = this.Shape switch
        {
            ReadAheadEligibilityBenchmarkShape.Memo => (SyntheticDatabases.MemoDbPath, SyntheticDatabases.MemoTable),
            ReadAheadEligibilityBenchmarkShape.Ole => (SyntheticDatabases.MemoDbPath, SyntheticDatabases.OleSinglePageTable),
            ReadAheadEligibilityBenchmarkShape.Numeric => (SyntheticDatabases.NumericDbPath, SyntheticDatabases.NumericTable),
            _ => throw new ArgumentOutOfRangeException(nameof(this.Shape), this.Shape, null),
        };
    }

    [IterationSetup]
    public void IterationSetup()
    {
        this.coldPath = Path.Combine(Path.GetTempPath(), $"JetBenchCold_{Guid.NewGuid():N}.accdb");
        CopyWithoutCaching(this.sourcePath, this.coldPath);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        File.Delete(this.coldPath);
        File.Delete(Path.ChangeExtension(this.coldPath, ".laccdb"));
    }

    [Benchmark]
    public async Task<int> ColdOpenFullScan()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            this.coldPath,
            new AccessReaderOptions { PageReadOptimizationMode = this.PageReadOptimizationMode }).ConfigureAwait(false);
        int count = 0;
        await foreach (object[] row in reader.Rows(this.tableName).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    /// <summary>
    /// Copies <paramref name="source"/> without leaving the copy's pages in the
    /// OS file cache. The buffer is a pinned array read from an aligned offset,
    /// since unbuffered writes need sector-aligned memory.
    /// </summary>
    /// <param name="source">The file to copy; its length must be a multiple of the page size.</param>
    /// <param name="destination">The copy to create.</param>
    /// <exception cref="InvalidDataException">The source is not a whole number of 4 KB pages.</exception>
    private static void CopyWithoutCaching(string source, string destination)
    {
        byte[] buffer = GC.AllocateArray<byte>(CopyChunkBytes + Alignment, pinned: true);
        long address = Marshal.UnsafeAddrOfPinnedArrayElement(buffer, 0).ToInt64();
        int start = (int)((Alignment - (address % Alignment)) % Alignment);
        Span<byte> chunk = buffer.AsSpan(start, CopyChunkBytes);

        using SafeFileHandle input = File.OpenHandle(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using SafeFileHandle output = File.OpenHandle(destination, FileMode.Create, FileAccess.Write, FileShare.None, NoBuffering | FileOptions.WriteThrough);
        long offset = 0;
        while (true)
        {
            int read = RandomAccess.Read(input, chunk, offset);
            if (read == 0)
            {
                return;
            }

            if (read % Alignment != 0)
            {
                throw new InvalidDataException($"'{source}' is not a whole number of {Alignment}-byte pages.");
            }

            RandomAccess.Write(output, chunk[..read], offset);
            offset += read;
        }
    }
}
