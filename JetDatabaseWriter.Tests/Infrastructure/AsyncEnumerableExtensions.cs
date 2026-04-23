namespace JetDatabaseWriter.Tests;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

internal static class AsyncEnumerableExtensions
{
    public static async ValueTask<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source, CancellationToken ct = default)
    {
        var list = new List<T>();
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            list.Add(item);
        }

        return list;
    }

    public static async ValueTask<int> CountAsync<T>(this IAsyncEnumerable<T> source, CancellationToken ct = default)
    {
        int count = 0;
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    public static async ValueTask<T?> FirstOrDefaultAsync<T>(this IAsyncEnumerable<T> source, CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            return item;
        }

        return default;
    }

    public static async IAsyncEnumerable<T> TakeAsync<T>(this IAsyncEnumerable<T> source, int count, [EnumeratorCancellation] CancellationToken ct = default)
    {
        int taken = 0;
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            yield return item;
            if (++taken >= count)
            {
                break;
            }
        }
    }
}
