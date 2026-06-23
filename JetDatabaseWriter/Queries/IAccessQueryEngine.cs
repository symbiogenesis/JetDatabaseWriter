namespace JetDatabaseWriter.Queries;

using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;

/// <summary>
/// Non-generic execution surface a query provider exposes so an
/// <see cref="AccessQueryable{T}"/> can run its expression without naming the
/// provider's entity type.
/// </summary>
internal interface IAccessQueryEngine
{
    public IAsyncEnumerable<object> ExecuteStreamAsync(Expression expression, CancellationToken cancellationToken);

    public IEnumerable ExecuteSyncList(Expression expression);
}
