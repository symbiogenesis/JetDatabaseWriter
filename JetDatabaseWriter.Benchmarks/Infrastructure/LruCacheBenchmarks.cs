namespace JetDatabaseWriter.Benchmarks.Infrastructure;

using System;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Infrastructure;

[MemoryDiagnoser]
public class LruCacheBenchmarks : IDisposable
{
    private LruCache<int, string> _cache = null!;

    [Params(64, 256, 1024)]
    public int Capacity { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        this._cache = new LruCache<int, string>(this.Capacity);
        for (int i = 0; i < this.Capacity; i++)
        {
            this._cache.Add(i, $"value_{i}");
        }
    }

    [Benchmark]
    public bool TryGetValue_Hit()
    {
        this._cache.TryGetValue(0, out _);
        return true;
    }

    [Benchmark]
    public bool TryGetValue_Miss()
    {
        this._cache.TryGetValue(-1, out _);
        return true;
    }

    [Benchmark]
    public void Add_Existing() => this._cache.Add(0, "updated");

    [Benchmark]
    public void Add_Evict()
    {
        // Exceeds capacity, forcing eviction of the LRU entry (key 0 gets evicted).
        this._cache.Add(this.Capacity + 1, "new");

        // Restore steady state: evict the new key by re-adding the original.
        this._cache.Add(0, "value_0");
    }

    [Benchmark]
    public void MixedWorkload()
    {
        for (int i = 0; i < 100; i++)
        {
            this._cache.TryGetValue(i % this.Capacity, out _);
            this._cache.Add(i % this.Capacity, $"v{i}");
        }
    }

    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            this._cache.Dispose();
        }
    }
}
