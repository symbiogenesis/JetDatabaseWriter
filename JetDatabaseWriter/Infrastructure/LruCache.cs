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

    private readonly int _capacity;
    private readonly Dictionary<TKey, int> _map;
    private readonly Node[] _nodes;
    private readonly Action<TValue>? _onEvict;
    private readonly ReaderWriterLockSlim _lock = new();
    private int _nextSlot = 1; // 0 is reserved for sentinel
    private long _hits;
    private long _misses;

    public LruCache(int capacity, Action<TValue>? onEvict = null)
    {
        _capacity = capacity;
        _onEvict = onEvict;
        _map = new Dictionary<TKey, int>(capacity);
        _nodes = new Node[capacity + 1]; // +1 for sentinel
        _nodes[Sentinel].Next = Sentinel;
        _nodes[Sentinel].Prev = Sentinel;
    }

    public int Count
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _map.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>Gets the number of successful <see cref="TryGetValue"/> lookups since construction.</summary>
    public long Hits
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _hits;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>Gets the number of failed <see cref="TryGetValue"/> lookups since construction.</summary>
    public long Misses
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _misses;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_map.TryGetValue(key, out int idx))
            {
                if (_nodes[Sentinel].Next != idx)
                {
                    MoveToFront(idx);
                }

                value = _nodes[idx].Value;
                _hits++;
                return true;
            }

            value = default!;
            _misses++;
            return false;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Add(TKey key, TValue value)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_map.TryGetValue(key, out int existingIdx))
            {
                if (_nodes[Sentinel].Next != existingIdx)
                {
                    MoveToFront(existingIdx);
                }

                _nodes[existingIdx].Value = value;
                return;
            }

            int nodeIdx;
            if (_map.Count >= _capacity)
            {
                // Evict LRU entry and reuse its slot in-place (zero allocation).
                nodeIdx = _nodes[Sentinel].Prev;
                Detach(nodeIdx);
                ref Node evicted = ref _nodes[nodeIdx];
                _map.Remove(evicted.Key);
                TValue evictedValue = evicted.Value;

                // Clear references so reused slot doesn't temporarily root the old key/value.
                evicted.Key = default!;
                evicted.Value = default!;
                _onEvict?.Invoke(evictedValue);
            }
            else
            {
                nodeIdx = _nextSlot++;
            }

            _nodes[nodeIdx].Key = key;
            _nodes[nodeIdx].Value = value;
            Prepend(nodeIdx);
            _map[key] = nodeIdx;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Clear()
    {
        _lock.EnterWriteLock();
        try
        {
            if (_onEvict != null)
            {
                foreach (var kvp in _map)
                {
                    _onEvict(_nodes[kvp.Value].Value);
                }
            }

            // Null out references so the backing array doesn't keep keys/values alive.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<Node>())
            {
                Array.Clear(_nodes, 0, _nextSlot);
            }

            _map.Clear();
            _nodes[Sentinel].Next = Sentinel;
            _nodes[Sentinel].Prev = Sentinel;
            _nextSlot = 1;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Detach(int idx)
    {
        ref Node node = ref _nodes[idx];
        _nodes[node.Prev].Next = node.Next;
        _nodes[node.Next].Prev = node.Prev;
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
        ref Node node = ref _nodes[idx];
        int oldHead = _nodes[Sentinel].Next;
        node.Next = oldHead;
        node.Prev = Sentinel;
        _nodes[oldHead].Prev = idx;
        _nodes[Sentinel].Next = idx;
    }

    private struct Node
    {
        public TKey Key;
        public TValue Value;
        public int Prev;
        public int Next;
    }

    public void Dispose()
    {
        _lock.Dispose();
    }
}
