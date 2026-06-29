namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>A <c>Where</c> stage that keeps the rows matching its predicate.</summary>
/// <param name="predicate">The row predicate; compiled once per execution and applied to each row.</param>
internal sealed class FilterStage(LambdaExpression predicate) : QueryStage
{
    public LambdaExpression Predicate { get; } = predicate;

    public override async IAsyncEnumerable<T> Apply<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var compiled = (Func<T, bool>)this.Predicate.Compile();
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (compiled(item))
            {
                yield return item;
            }
        }
    }
}
