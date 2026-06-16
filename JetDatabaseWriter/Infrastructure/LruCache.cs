namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// Thread-safe cache with CLOCK (second-chance) approximate-LRU eviction.
/// Uses an array-backed doubly-linked list with a sentinel node to eliminate
/// per-entry heap allocations and improve CPU cache locality.
/// Uses <see cref="ReaderWriterLockSlim"/> so concurrent cache hits run under
/// the shared read lock: a hit only sets a per-entry reference bit instead of
/// reordering the recency list, and the deferred reorder is applied under the
/// write lock during eviction.
/// </summary>
/// <typeparam name="TKey">The type of keys in the cache.</typeparam>
/// <typeparam name="TValue">The type of values in the cache.</typeparam>
internal sealed class LruCache<TKey, TValue> : IDisposable
    where TKey : notnull
{
    private const int Sentinel = 0;

    private readonly int capacity;
    private readonly Dictionary<TKey, int> map;
    private readonly Node[] nodes;
    private readonly Action<TValue>? onEvict;
    private readonly ReaderWriterLockSlim rwLock = new();

    /// <summary>
    /// Tracks the next allocatable node slot; slot 0 is reserved for the sentinel.
    /// </summary>
    private int nextSlot = 1;

    /// <summary>
    /// Cache hit counter for monitoring effectiveness. Updated with <see cref="Interlocked"/> because hits are recorded under the shared read lock, where concurrent readers may increment it simultaneously.
    /// </summary>
    private long hits;

    /// <summary>
    /// Cache miss counter for monitoring effectiveness. Updated with <see cref="Interlocked"/> because misses are recorded under the shared read lock, where concurrent readers may increment it simultaneously.
    /// </summary>
    private long misses;

    public LruCache(int capacity, Action<TValue>? onEvict = null)
    {
        this.capacity = capacity;
        this.onEvict = onEvict;
        this.map = new Dictionary<TKey, int>(capacity);
        this.nodes = new Node[capacity + 1]; // +1 for sentinel
        this.nodes[Sentinel].Next = Sentinel;
        this.nodes[Sentinel].Prev = Sentinel;
    }

    public int Count
    {
        get
        {
            this.rwLock.EnterReadLock();
            try
            {
                return this.map.Count;
            }
            finally
            {
                this.rwLock.ExitReadLock();
            }
        }
    }

    /// <summary>Gets the number of successful <see cref="TryGetValue"/> lookups since construction.</summary>
    public long Hits => Interlocked.Read(ref this.hits);

    /// <summary>Gets the number of failed <see cref="TryGetValue"/> lookups since construction.</summary>
    public long Misses => Interlocked.Read(ref this.misses);

    public bool TryGetValue(TKey key, out TValue value)
    {
        this.rwLock.EnterReadLock();
        try
        {
            if (this.map.TryGetValue(key, out int idx))
            {
                value = this.nodes[idx].Value;

                // CLOCK second-chance: record the access by setting the entry's
                // reference bit instead of reordering the recency list, so cache
                // hits stay on the shared read lock. The deferred reorder runs
                // under the write lock during eviction. Concurrent readers only
                // ever write the constant true here, so the unsynchronized write
                // is benign; the bit is read and cleared exclusively under the
                // write lock, which cannot overlap a read-lock holder.
                this.nodes[idx].Referenced = true;
                Interlocked.Increment(ref this.hits);
                return true;
            }

            value = default!;
            Interlocked.Increment(ref this.misses);
            return false;
        }
        finally
        {
            this.rwLock.ExitReadLock();
        }
    }

    public void Add(TKey key, TValue value)
    {
        this.rwLock.EnterWriteLock();
        try
        {
            if (this.map.TryGetValue(key, out int existingIdx))
            {
                if (this.nodes[Sentinel].Next != existingIdx)
                {
                    this.MoveToFront(existingIdx);
                }

                this.nodes[existingIdx].Value = value;

                // Now most-recently used by position, so drop any pending second chance.
                this.nodes[existingIdx].Referenced = false;
                return;
            }

            int nodeIdx;
            if (this.map.Count >= this.capacity)
            {
                // Pick a victim via the CLOCK second-chance scan and reuse its slot in-place (zero allocation).
                nodeIdx = this.SelectEvictionVictim();
                this.Detach(nodeIdx);
                ref Node evicted = ref this.nodes[nodeIdx];
                this.map.Remove(evicted.Key);
                TValue? evictedValue = evicted.Value;

                // Clear references so reused slot doesn't temporarily root the old key/value.
                evicted.Key = default!;
                evicted.Value = default!;
                this.onEvict?.Invoke(evictedValue);
            }
            else
            {
                nodeIdx = this.nextSlot++;
            }

            this.nodes[nodeIdx].Key = key;
            this.nodes[nodeIdx].Value = value;

            // New entries start unreferenced (also resets a reused evicted slot's stale bit).
            this.nodes[nodeIdx].Referenced = false;
            this.Prepend(nodeIdx);
            this.map[key] = nodeIdx;
        }
        finally
        {
            this.rwLock.ExitWriteLock();
        }
    }

    public void Clear()
    {
        this.rwLock.EnterWriteLock();
        try
        {
            if (this.onEvict != null)
            {
                foreach (KeyValuePair<TKey, int> kvp in this.map)
                {
                    this.onEvict(this.nodes[kvp.Value].Value);
                }
            }

            // Null out references so the backing array doesn't keep keys/values alive.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<Node>())
            {
                Array.Clear(this.nodes, 0, this.nextSlot);
            }

            this.map.Clear();
            this.nodes[Sentinel].Next = Sentinel;
            this.nodes[Sentinel].Prev = Sentinel;
            this.nextSlot = 1;
        }
        finally
        {
            this.rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Selects the slot to evict using the CLOCK (second-chance) algorithm: walk
    /// from the LRU end and give every entry whose reference bit is set a second
    /// chance by clearing the bit and promoting it to most-recently-used. The
    /// first entry found with a clear bit is returned. Always called under the
    /// write lock. Terminates within a single pass because each promotion clears
    /// exactly one bit, so the tail bit is guaranteed clear after one full cycle.
    /// </summary>
    private int SelectEvictionVictim()
    {
        int victim = this.nodes[Sentinel].Prev;
        while (this.nodes[victim].Referenced)
        {
            this.nodes[victim].Referenced = false;
            this.MoveToFront(victim);
            victim = this.nodes[Sentinel].Prev;
        }

        return victim;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Detach(int idx)
    {
        ref Node node = ref this.nodes[idx];
        this.nodes[node.Prev].Next = node.Next;
        this.nodes[node.Next].Prev = node.Prev;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MoveToFront(int idx)
    {
        this.Detach(idx);
        this.Prepend(idx);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Prepend(int idx)
    {
        ref Node node = ref this.nodes[idx];
        int oldHead = this.nodes[Sentinel].Next;
        node.Next = oldHead;
        node.Prev = Sentinel;
        this.nodes[oldHead].Prev = idx;
        this.nodes[Sentinel].Next = idx;
    }

    private struct Node
    {
        public TKey Key { get; set; }

        public TValue Value { get; set; }

        public int Prev { get; set; }

        public int Next { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this entry has been accessed
        /// since the last recency reorder (the CLOCK reference bit). Set under the
        /// shared read lock on a cache hit and cleared under the write lock during
        /// the eviction second-chance scan.
        /// </summary>
        public bool Referenced { get; set; }
    }

    public void Dispose() => this.rwLock.Dispose();
}
