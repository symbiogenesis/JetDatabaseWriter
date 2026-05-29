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
        map = new Dictionary<TKey, int>(capacity);
        nodes = new Node[capacity + 1]; // +1 for sentinel
        nodes[Sentinel].Next = Sentinel;
        nodes[Sentinel].Prev = Sentinel;
    }

    public int Count
    {
        get
        {
            rwLock.EnterReadLock();
            try
            {
                return map.Count;
            }
            finally
            {
                rwLock.ExitReadLock();
            }
        }
    }

    /// <summary>Gets the number of successful <see cref="TryGetValue"/> lookups since construction.</summary>
    public long Hits
    {
        get
        {
            rwLock.EnterReadLock();
            try
            {
                return hits;
            }
            finally
            {
                rwLock.ExitReadLock();
            }
        }
    }

    /// <summary>Gets the number of failed <see cref="TryGetValue"/> lookups since construction.</summary>
    public long Misses
    {
        get
        {
            rwLock.EnterReadLock();
            try
            {
                return misses;
            }
            finally
            {
                rwLock.ExitReadLock();
            }
        }
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        rwLock.EnterWriteLock();
        try
        {
            if (map.TryGetValue(key, out int idx))
            {
                if (nodes[Sentinel].Next != idx)
                {
                    MoveToFront(idx);
                }

                value = nodes[idx].Value;
                hits++;
                return true;
            }

            value = default!;
            misses++;
            return false;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public void Add(TKey key, TValue value)
    {
        rwLock.EnterWriteLock();
        try
        {
            if (map.TryGetValue(key, out int existingIdx))
            {
                if (nodes[Sentinel].Next != existingIdx)
                {
                    MoveToFront(existingIdx);
                }

                nodes[existingIdx].Value = value;
                return;
            }

            int nodeIdx;
            if (map.Count >= capacity)
            {
                // Evict LRU entry and reuse its slot in-place (zero allocation).
                nodeIdx = nodes[Sentinel].Prev;
                Detach(nodeIdx);
                ref Node evicted = ref nodes[nodeIdx];
                map.Remove(evicted.Key);
                TValue? evictedValue = evicted.Value;

                // Clear references so reused slot doesn't temporarily root the old key/value.
                evicted.Key = default!;
                evicted.Value = default!;
                onEvict?.Invoke(evictedValue);
            }
            else
            {
                nodeIdx = nextSlot++;
            }

            nodes[nodeIdx].Key = key;
            nodes[nodeIdx].Value = value;
            Prepend(nodeIdx);
            map[key] = nodeIdx;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    public void Clear()
    {
        rwLock.EnterWriteLock();
        try
        {
            if (onEvict != null)
            {
                foreach (KeyValuePair<TKey, int> kvp in map)
                {
                    onEvict(nodes[kvp.Value].Value);
                }
            }

            // Null out references so the backing array doesn't keep keys/values alive.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<Node>())
            {
                Array.Clear(nodes, 0, nextSlot);
            }

            map.Clear();
            nodes[Sentinel].Next = Sentinel;
            nodes[Sentinel].Prev = Sentinel;
            nextSlot = 1;
        }
        finally
        {
            rwLock.ExitWriteLock();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Detach(int idx)
    {
        ref Node node = ref nodes[idx];
        nodes[node.Prev].Next = node.Next;
        nodes[node.Next].Prev = node.Prev;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MoveToFront(int idx)
    {
        Detach(idx);
        Prepend(idx);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Prepend(int idx)
    {
        ref Node node = ref nodes[idx];
        int oldHead = nodes[Sentinel].Next;
        node.Next = oldHead;
        node.Prev = Sentinel;
        nodes[oldHead].Prev = idx;
        nodes[Sentinel].Next = idx;
    }

    private struct Node
    {
        public TKey Key { get; set; }

        public TValue Value { get; set; }

        public int Prev { get; set; }

        public int Next { get; set; }
    }

    public void Dispose() => rwLock.Dispose();
}
