namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>A <c>Skip</c> stage that discards a leading count of rows.</summary>
/// <param name="count">The number of leading rows to discard.</param>
internal sealed class SkipStage(int count) : QueryStage
{
    public override async IAsyncEnumerable<T> Apply<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        int remaining = count;
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (remaining > 0)
            {
                remaining--;
                continue;
            }

            yield return item;
        }
    }
}
