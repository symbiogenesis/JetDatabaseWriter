namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

/// <summary>
/// Thread-safe LRU (Least Recently Used) cache implementation.
/// Uses an array-backed doubly-linked list with a sentinel node to eliminate
/// per-entry heap allocations and improve CPU cache locality.
/// </summary>
/// <typeparam name="TKey">The type of keys in the cache.</typeparam>
/// <typeparam name="TValue">The type of values in the cache.</typeparam>
internal sealed class LruCache<TKey, TValue>
    where TKey : notnull
{
    private const int Sentinel = 0;

    private readonly int _capacity;
    private readonly Dictionary<TKey, int> _map;
    private readonly Node[] _nodes;
    private readonly Action<TValue>? _onEvict;
    private readonly object _lock = new object();
    private int _nextSlot = 1; // 0 is reserved for sentinel

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
            lock (_lock)
            {
                return _map.Count;
            }
        }
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        lock (_lock)
        {
            if (_map.TryGetValue(key, out int idx))
            {
                MoveToFront(idx);
                value = _nodes[idx].Value;
                return true;
            }

            value = default!;
            return false;
        }
    }

    public void Add(TKey key, TValue value)
    {
        lock (_lock)
        {
            if (_map.TryGetValue(key, out int existingIdx))
            {
                MoveToFront(existingIdx);
                _nodes[existingIdx].Value = value;
                return;
            }

            int nodeIdx;
            if (_map.Count >= _capacity)
            {
                // Evict LRU entry and reuse its slot in-place (zero allocation).
                nodeIdx = _nodes[Sentinel].Prev;
                Detach(nodeIdx);
                _map.Remove(_nodes[nodeIdx].Key);
                _onEvict?.Invoke(_nodes[nodeIdx].Value);
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
    }

    public void Clear()
    {
        Clear(_onEvict);
    }

    public void Clear(Action<TValue>? onRemove)
    {
        lock (_lock)
        {
            if (onRemove != null)
            {
                foreach (var kvp in _map)
                {
                    onRemove(_nodes[kvp.Value].Value);
                }
            }

            _map.Clear();
            _nodes[Sentinel].Next = Sentinel;
            _nodes[Sentinel].Prev = Sentinel;
            _nextSlot = 1;
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
}
