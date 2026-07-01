namespace JetDatabaseWriter.Queries;

using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Non-generic execution surface a query provider exposes so an
/// <see cref="AccessQueryable{T}"/> can run its expression without naming the
/// provider's entity type.
/// </summary>
internal interface IAccessQueryEngine
{
    public IAsyncEnumerable<object> ExecuteStreamAsync(Expression expression, CancellationToken cancellationToken);

    public IEnumerable ExecuteSyncList(Expression expression);

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
