namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// A contiguous run of <c>OrderBy</c>/<c>ThenBy</c> keys. Ordering buffers the source
/// and sorts it; later stages observe the sorted sequence.
/// </summary>
internal sealed class OrderStage : QueryStage
{
    public List<OrderingKey> Keys { get; } = [];

    public override async IAsyncEnumerable<T> Apply<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new List<T>();
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(item);
        }

        foreach (T item in Sort(buffer, this.Keys))
        {
            yield return item;
        }
    }

    private static List<T> Sort<T>(List<T> buffer, List<OrderingKey> keys)
    {
        Func<T, object?> first = CompileKey<T>(keys[0].KeySelector);
        IOrderedEnumerable<T> ordered = keys[0].Descending
            ? buffer.OrderByDescending(first, QueryKeyComparer.Instance)
            : buffer.OrderBy(first, QueryKeyComparer.Instance);
        for (int i = 1; i < keys.Count; i++)
        {
            Func<T, object?> key = CompileKey<T>(keys[i].KeySelector);
            ordered = keys[i].Descending
                ? ordered.ThenByDescending(key, QueryKeyComparer.Instance)
                : ordered.ThenBy(key, QueryKeyComparer.Instance);
        }

        return ordered.ToList();
    }

    private static Func<T, object?> CompileKey<T>(LambdaExpression selector)
    {
        ParameterExpression parameter = selector.Parameters[0];
        Expression body = Expression.Convert(selector.Body, typeof(object));
        return Expression.Lambda<Func<T, object?>>(body, parameter).Compile();
    }
}
