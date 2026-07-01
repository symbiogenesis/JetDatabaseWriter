namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>A <c>Take</c> stage that yields at most a leading count of rows.</summary>
/// <param name="count">The maximum number of leading rows to yield.</param>
internal sealed class TakeStage(int count) : QueryStage
{
    public override async IAsyncEnumerable<T> Apply<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (count <= 0)
        {
            yield break;
        }

        int taken = 0;
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
            if (++taken >= count)
            {
                yield break;
            }
        }
    }
}
