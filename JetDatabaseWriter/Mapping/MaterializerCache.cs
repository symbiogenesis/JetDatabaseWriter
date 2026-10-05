namespace JetDatabaseWriter.Mapping;

using System;
using System.Collections.Generic;

/// <summary>Bounded materializer cache with serialized compilation and least-recently-used eviction.</summary>
/// <typeparam name="T">The cached materializer value.</typeparam>
internal sealed class MaterializerCache<T>
{
    private const int Capacity = 64;
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock gate = new();
#else
    private readonly object gate = new();
#endif
    private readonly Dictionary<RowShape, LinkedListNode<(RowShape Shape, T Value)>> entries = [];
    private readonly LinkedList<(RowShape Shape, T Value)> recent = new();

    /// <summary>Gets an existing value or publishes one newly created value.</summary>
    /// <param name="shape">The immutable row shape.</param>
    /// <param name="create">The value factory.</param>
    /// <returns>The cached value.</returns>
    internal T Get(RowShape shape, Func<T> create)
    {
        lock (this.gate)
        {
            if (this.entries.TryGetValue(shape, out LinkedListNode<(RowShape Shape, T Value)>? entry))
            {
                this.recent.Remove(entry);
                this.recent.AddFirst(entry);
                return entry.Value.Value;
            }

            T value = create();
            LinkedListNode<(RowShape Shape, T Value)> added = this.recent.AddFirst((shape, value));
            this.entries.Add(shape, added);
            if (this.entries.Count > Capacity && this.recent.Last is { } oldest)
            {
                this.entries.Remove(oldest.Value.Shape);
                this.recent.RemoveLast();
            }

            return value;
        }
    }
}
