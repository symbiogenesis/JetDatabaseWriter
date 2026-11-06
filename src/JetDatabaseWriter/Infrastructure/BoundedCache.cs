namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

/// <summary>Bounds retained metadata with serialized construction and least-recently-used eviction.</summary>
/// <typeparam name="TKey">The immutable metadata identity.</typeparam>
/// <typeparam name="TValue">The cached metadata.</typeparam>
/// <param name="capacity">The retained entry limit; nonpositive values disable retention.</param>
internal sealed class BoundedCache<TKey, TValue>(int capacity)
    where TKey : notnull
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock gate = new();
#else
    private readonly object gate = new();
#endif
    private readonly Dictionary<TKey, LinkedListNode<(TKey Key, TValue Value)>> entries = [];
    private readonly LinkedList<(TKey Key, TValue Value)> recent = new();

    /// <summary>Gets the number of retained entries.</summary>
    internal int Count
    {
        get
        {
            lock (this.gate)
            {
                return this.entries.Count;
            }
        }
    }

    /// <summary>Gets an existing value if present and promotes it as most recently used.</summary>
    /// <param name="key">The immutable metadata identity.</param>
    /// <param name="value">Receives the cached value when found.</param>
    /// <returns><see langword="true"/> when the key was present in the cache.</returns>
    internal bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        if (capacity <= 0)
        {
            value = default;
            return false;
        }

        lock (this.gate)
        {
            if (this.entries.TryGetValue(key, out LinkedListNode<(TKey Key, TValue Value)>? entry))
            {
                this.recent.Remove(entry);
                this.recent.AddFirst(entry);
                value = entry.Value.Value;
                return true;
            }

            value = default;
            return false;
        }
    }

    /// <summary>Stores or updates an entry and evicts the least-recently-used entry if over capacity.</summary>
    /// <param name="key">The immutable metadata identity.</param>
    /// <param name="value">The value to store.</param>
    internal void Set(TKey key, TValue value)
    {
        if (capacity <= 0)
        {
            return;
        }

        lock (this.gate)
        {
            if (this.entries.TryGetValue(key, out LinkedListNode<(TKey Key, TValue Value)>? existing))
            {
                this.recent.Remove(existing);
                existing.Value = (key, value);
                this.recent.AddFirst(existing);
                return;
            }

            LinkedListNode<(TKey Key, TValue Value)> added = this.recent.AddFirst((key, value));
            this.entries.Add(key, added);
            if (this.entries.Count > capacity && this.recent.Last is { } oldest)
            {
                this.entries.Remove(oldest.Value.Key);
                this.recent.RemoveLast();
            }
        }
    }

    /// <summary>Gets an existing value or retains one successfully constructed value, including null.</summary>
    /// <param name="key">The immutable metadata identity.</param>
    /// <param name="create">The factory, serialized when retention is enabled; exceptions are not cached.</param>
    /// <returns>The cached or newly constructed value.</returns>
    internal TValue GetOrCreate(TKey key, Func<TKey, TValue> create)
    {
        if (capacity <= 0)
        {
            return create(key);
        }

        lock (this.gate)
        {
            if (this.entries.TryGetValue(key, out LinkedListNode<(TKey Key, TValue Value)>? entry))
            {
                this.recent.Remove(entry);
                this.recent.AddFirst(entry);
                return entry.Value.Value;
            }

            TValue value = create(key);
            LinkedListNode<(TKey Key, TValue Value)> added = this.recent.AddFirst((key, value));
            this.entries.Add(key, added);
            if (this.entries.Count > capacity && this.recent.Last is { } oldest)
            {
                this.entries.Remove(oldest.Value.Key);
                this.recent.RemoveLast();
            }

            return value;
        }
    }
}
