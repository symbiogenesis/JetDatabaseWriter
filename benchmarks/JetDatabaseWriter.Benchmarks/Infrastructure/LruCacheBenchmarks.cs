namespace JetDatabaseWriter.Benchmarks.Infrastructure;

using System;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Infrastructure;

[MemoryDiagnoser]
public class LruCacheBenchmarks : IDisposable
{
    private const int Capacity = 256;

    private LruCache<int, string> cache = null!;
    private int nextEvictionKey = Capacity;

    [GlobalSetup]
    public void Setup()
    {
        this.cache = new LruCache<int, string>(Capacity);
        this.FillCache();

        // Check beyond a complete rotation: reusing a fixed pair of keys only
        // evicts once and silently turns this benchmark into existing-key updates.
        for (int i = 0; i < Capacity + 2; i++)
        {
            int expectedKey = (Capacity + i) % (Capacity + 1);
            if (this.cache.TryGetValue(expectedKey, out _))
            {
                throw new InvalidOperationException("The eviction workload must insert a missing key on every call.");
            }

            this.Add_Evict();
            if (!this.cache.TryGetValue(expectedKey, out _) || this.cache.Count != Capacity)
            {
                throw new InvalidOperationException("The eviction workload must replace exactly one cached entry.");
            }
        }

        this.cache.Clear();
        this.FillCache();
        this.nextEvictionKey = Capacity;
    }

    [Benchmark]
    public bool TryGetValue_Hit() => this.cache.TryGetValue(0, out _);

    [Benchmark]
    public bool TryGetValue_Miss() => this.cache.TryGetValue(-1, out _);

    [Benchmark]
    public void Add_Existing() => this.cache.Add(0, "updated");

    [Benchmark]
    public void Add_Evict()
    {
        // A capacity + 1 working set keeps every insertion on the eviction path.
        this.cache.Add(this.nextEvictionKey, "new");
        this.nextEvictionKey = this.nextEvictionKey == Capacity ? 0 : this.nextEvictionKey + 1;
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
            this.cache.Dispose();
        }
    }

    private void FillCache()
    {
        for (int i = 0; i < Capacity; i++)
        {
            this.cache.Add(i, $"value_{i}");
        }
    }
}
