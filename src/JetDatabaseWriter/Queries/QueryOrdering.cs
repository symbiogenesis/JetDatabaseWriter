namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Linq;

/// <summary>Builds a stable multikey ordering shared by root queries, collection includes and query tails.</summary>
internal static class QueryOrdering
{
    /// <summary>Orders a sequence by each prepared key in written order, retaining stable ties and selector failures.</summary>
    /// <typeparam name="T">The elements being ordered.</typeparam>
    /// <param name="source">The buffered root/tail rows or one parent's loaded children.</param>
    /// <param name="keys">The nonempty, contiguous ordering run.</param>
    /// <returns>The deferred stable ordering.</returns>
    internal static IOrderedEnumerable<T> Apply<T>(IEnumerable<T> source, IReadOnlyList<QuerySortKey<T>> keys)
    {
        QuerySortKey<T> first = keys[0];
        IOrderedEnumerable<T> ordered = first.Descending
            ? source.OrderByDescending(first.Selector, first.Comparer)
            : source.OrderBy(first.Selector, first.Comparer);
        for (int i = 1; i < keys.Count; i++)
        {
            QuerySortKey<T> key = keys[i];
            ordered = key.Descending
                ? ordered.ThenByDescending(key.Selector, key.Comparer)
                : ordered.ThenBy(key.Selector, key.Comparer);
        }

        return ordered;
    }
}
