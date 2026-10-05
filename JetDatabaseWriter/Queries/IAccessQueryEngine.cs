namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Non-generic execution surface a query provider exposes so an
/// <see cref="AccessQueryable{T}"/> can run its expression without naming the
/// provider's entity type. It is async-only, like the query results it serves.
/// </summary>
internal interface IAccessQueryEngine
{
    /// <summary>
    /// Prepares <paramref name="expression"/> and streams its results, boxed: the engine's rows,
    /// or the output of the operators above them (a projection's values can be null).
    /// Preparation validates all operators, including second query sources, synchronously;
    /// the returned sequence does not read rows until it is enumerated.
    /// </summary>
    /// <param name="expression">The query expression to run.</param>
    /// <param name="cancellationToken">A token used to cancel the enumeration.</param>
    /// <returns>The query's results as they are produced.</returns>
    public IAsyncEnumerable<object?> ExecuteStreamAsync(Expression expression, CancellationToken cancellationToken);

    /// <summary>
    /// Counts the rows <paramref name="expression"/> produces. Counting the whole
    /// table takes a metadata fast path; every other shape streams the rows and
    /// counts them without materializing a list.
    /// </summary>
    /// <param name="expression">The query expression to count.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of rows the query produces.</returns>
    public ValueTask<long> CountAsync(Expression expression, CancellationToken cancellationToken);
}
