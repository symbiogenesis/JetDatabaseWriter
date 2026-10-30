namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Linq.Expressions;

/// <summary>
/// A contiguous <c>OrderBy</c>/<c>ThenBy</c> run applied to a parent's children. Keys
/// compare through <see cref="QueryKeyComparer"/>, the same comparer the in-memory query
/// sort uses, so ordering matches the rest of the pipeline.
/// </summary>
internal sealed class IncludeOrderOperation : IncludeOperation
{
    private readonly List<QuerySortKey<object>> keys = [];

    /// <summary>Appends an ordering key to this run, in <c>OrderBy</c>/<c>ThenBy</c> order.</summary>
    /// <param name="selector">The key-selector lambda.</param>
    /// <param name="descending">Whether the key sorts in descending order.</param>
    public void AddKey(LambdaExpression selector, bool descending) =>
        this.keys.Add(new QuerySortKey<object>(CompileSelector(selector), QueryKeyComparer.Instance, descending));

    public override IEnumerable<object> Apply(IEnumerable<object> source) => QueryOrdering.Apply(source, this.keys);
}
