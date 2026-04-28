namespace JetDatabaseWriter.Benchmarks;

using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Internal.Collections;

[MemoryDiagnoser]
public class LruCacheBenchmarks
{
    private LruCache<int, string> _cache = null!;

    [Params(64, 256, 1024)]
    public int Capacity { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _cache = new LruCache<int, string>(Capacity);
        for (int i = 0; i < Capacity; i++)
        {
            _cache.Add(i, $"value_{i}");
        }
    }

    [Benchmark]
    public bool TryGetValue_Hit()
    {
        _cache.TryGetValue(0, out _);
        return true;
    }

    [Benchmark]
    public bool TryGetValue_Miss()
    {
        _cache.TryGetValue(-1, out _);
        return true;
    }

    [Benchmark]
    public void Add_Existing()
    {
        _cache.Add(0, "updated");
    }

    [Benchmark]
    public void Add_Evict()
    {
        // Exceeds capacity, forcing eviction of the LRU entry (key 0 gets evicted).
        _cache.Add(Capacity + 1, "new");

        // Restore steady state: evict the new key by re-adding the original.
        _cache.Add(0, "value_0");
    }

    [Benchmark]
    public void MixedWorkload()
    {
        for (int i = 0; i < 100; i++)
        {
            _cache.TryGetValue(i % Capacity, out _);
            _cache.Add(i % Capacity, $"v{i}");
        }
    }
}
