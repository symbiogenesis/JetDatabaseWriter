namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;

public enum OwnedPageDiscoveryPath
{
    /// <summary>Use the recognized per-table owned-pages usage map.</summary>
    RecognizedUsageMap,

    /// <summary>Force the safety fallback that scans the whole file for owned data pages.</summary>
    WholeFileFallback,
}

/// <summary>
/// Measures cold owned-page discovery for a small target table in a database
/// that also contains a much larger unrelated table. The fallback fixture keeps
/// the target table readable but gives its owned-pages map an unrecognized row
/// type so <see cref="AccessReader"/> must build the whole-file owner index.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderOwnedPageDiscoveryBenchmarks
{
    [Params(OwnedPageDiscoveryPath.RecognizedUsageMap, OwnedPageDiscoveryPath.WholeFileFallback)]
    public OwnedPageDiscoveryPath Path { get; set; }

    [GlobalSetup]
    public async Task Setup() => await SyntheticDatabases.EnsureOwnedPageDiscoveryAsync().ConfigureAwait(false);

    [Benchmark]
    public async Task<int> ColdOpenFirstRow()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(ResolveDatabasePath()).ConfigureAwait(false);
        return await CountFirstRowAsync(reader).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<int> ColdOpenFullScan()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(ResolveDatabasePath()).ConfigureAwait(false);
        return await CountRowsAsync(reader).ConfigureAwait(false);
    }

    private static async Task<int> CountFirstRowAsync(AccessReader reader)
    {
        await foreach (object[] row in reader.Rows(SyntheticDatabases.OwnedPageDiscoveryTargetTable).ConfigureAwait(false))
        {
            _ = row;
            return 1;
        }

        return 0;
    }

    private static async Task<int> CountRowsAsync(AccessReader reader)
    {
        int count = 0;
        await foreach (object[] row in reader.Rows(SyntheticDatabases.OwnedPageDiscoveryTargetTable).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    private string ResolveDatabasePath()
        => Path switch
        {
            OwnedPageDiscoveryPath.RecognizedUsageMap => SyntheticDatabases.OwnedPageDiscoveryMappedDbPath,
            OwnedPageDiscoveryPath.WholeFileFallback => SyntheticDatabases.OwnedPageDiscoveryFallbackDbPath,
            _ => throw new ArgumentOutOfRangeException(nameof(Path), Path, null),
        };
}
