namespace JetDatabaseWriter.Mapping;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Process-wide bounded cache for compiled row materializers and direct/hybrid decoders keyed by
/// entity <see cref="Type"/> and <see cref="RowShape"/>. Uses an ephemeron table so collectible
/// <c>AssemblyLoadContext</c> types can unload, paired with a weak FIFO queue that bounds retained
/// non-collectible types to <see cref="MaxTypes"/>. Each type's LRU shape cache retains at most
/// <see cref="MaxShapesPerType"/> shapes.
/// </summary>
internal static class MaterializerCache
{
    internal const int MaxTypes = 256;
    internal const int MaxShapesPerType = 64;

    private static readonly ConditionalWeakTable<Type, BoundedCache<RowShape, object>> Tables = [];
    private static readonly LinkedList<WeakReference<Type>> RecentTypes = new();

    /// <summary>Gets the number of active entity types currently tracked in the bounded cache.</summary>
    internal static int CachedTypeCount
    {
        get
        {
            lock (Tables)
            {
                PruneCollectedTypes();
                return RecentTypes.Count;
            }
        }
    }

    /// <summary>Gets or creates the cached materializer container for <paramref name="type"/> and <paramref name="shape"/>.</summary>
    /// <typeparam name="TValue">The materializer container type.</typeparam>
    /// <param name="type">The entity type.</param>
    /// <param name="shape">The immutable row shape.</param>
    /// <param name="factory">The factory creating a materializer container for the shape.</param>
    /// <returns>The cached or newly created materializer container.</returns>
    internal static TValue GetOrCreate<TValue>(Type type, RowShape shape, Func<RowShape, TValue> factory)
        where TValue : class
    {
        Guard.NotNull(type, nameof(type));
        Guard.NotNull(shape, nameof(shape));
        Guard.NotNull(factory, nameof(factory));

        if (!Tables.TryGetValue(type, out BoundedCache<RowShape, object>? shapeCache))
        {
            var created = new BoundedCache<RowShape, object>(MaxShapesPerType);
            lock (Tables)
            {
                if (!Tables.TryGetValue(type, out shapeCache))
                {
                    Tables.Add(type, created);
                    TrackType(type);
                    shapeCache = created;
                }
            }
        }

        return (TValue)shapeCache.GetOrCreate(shape, s => factory(s));
    }

    private static void TrackType(Type type)
    {
        PruneCollectedTypes();
        RecentTypes.AddFirst(new WeakReference<Type>(type));
        while (RecentTypes.Count > MaxTypes && RecentTypes.Last is { } oldest)
        {
            RecentTypes.RemoveLast();
            if (oldest.Value.TryGetTarget(out Type? evicted))
            {
                _ = Tables.Remove(evicted);
            }
        }
    }

    private static void PruneCollectedTypes()
    {
        LinkedListNode<WeakReference<Type>>? node = RecentTypes.First;
        while (node is not null)
        {
            LinkedListNode<WeakReference<Type>>? next = node.Next;
            if (!node.Value.TryGetTarget(out Type? target) || !Tables.TryGetValue(target, out _))
            {
                RecentTypes.Remove(node);
            }

            node = next;
        }
    }
}
