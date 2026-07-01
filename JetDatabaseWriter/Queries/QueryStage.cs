namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Threading;

/// <summary>
/// A single query operator in the order it was written. Stages are applied in
/// sequence so the query honors LINQ semantics (for example <c>Take</c> then
/// <c>Where</c> takes before it filters) rather than collapsing into fixed phases.
/// </summary>
internal abstract class QueryStage
{
    public abstract IAsyncEnumerable<T> Apply<T>(IAsyncEnumerable<T> source, CancellationToken cancellationToken);
}
