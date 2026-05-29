namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// Thread-safe LRU (Least Recently Used) cache implementation.
/// Uses an array-backed doubly-linked list with a sentinel node to eliminate
/// per-entry heap allocations and improve CPU cache locality.
/// Uses <see cref="ReaderWriterLockSlim"/> so concurrent readers (cache hits
/// that don't MoveToFront) pay only the shared-lock cost.
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
    /// Cache hit counter for monitoring effectiveness. Updated under the write lock for simplicity, since hits are only recorded on entries that need to be moved to the front of the list.
    /// </summary>
    private long hits;

    /// <summary>
    /// Cache miss counter for monitoring effectiveness. Updated under the write lock for simplicity, since misses are only recorded on entries that need to be moved to the front of the list.
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
    public long Hits
    {
        get
        {
            this.rwLock.EnterReadLock();
            try
            {
                return this.hits;
            }
            finally
            {
                this.rwLock.ExitReadLock();
            }
        }
    }

    /// <summary>Gets the number of failed <see cref="TryGetValue"/> lookups since construction.</summary>
    public long Misses
    {
        get
        {
            this.rwLock.EnterReadLock();
            try
            {
                return this.misses;
            }
            finally
            {
                this.rwLock.ExitReadLock();
            }
        }
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        this.rwLock.EnterWriteLock();
        try
        {
            if (this.map.TryGetValue(key, out int idx))
            {
                if (this.nodes[Sentinel].Next != idx)
                {
                    this.MoveToFront(idx);
                }

                value = this.nodes[idx].Value;
                this.hits++;
                return true;
            }

            value = default!;
            this.misses++;
            return false;
        }
        finally
        {
            this.rwLock.ExitWriteLock();
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
                return;
            }

            int nodeIdx;
            if (this.map.Count >= this.capacity)
            {
                // Evict LRU entry and reuse its slot in-place (zero allocation).
                nodeIdx = this.nodes[Sentinel].Prev;
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
    }

    public void Dispose() => this.rwLock.Dispose();
}
