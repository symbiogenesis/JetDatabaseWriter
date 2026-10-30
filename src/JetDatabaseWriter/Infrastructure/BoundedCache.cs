namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Collections.Generic;

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
