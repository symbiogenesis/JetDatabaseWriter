namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

/// <summary>
/// A contiguous <c>OrderBy</c>/<c>ThenBy</c> run applied to a parent's children. Keys
/// compare through <see cref="QueryKeyComparer"/>, the same comparer the in-memory query
/// sort uses, so ordering matches the rest of the pipeline.
/// </summary>
internal sealed class IncludeOrderOperation : IncludeOperation
{
    private readonly List<(Func<object, object?> KeySelector, bool Descending)> keys = [];

    /// <summary>Appends an ordering key to this run, in <c>OrderBy</c>/<c>ThenBy</c> order.</summary>
    /// <param name="selector">The key-selector lambda.</param>
    /// <param name="descending">Whether the key sorts in descending order.</param>
    public void AddKey(LambdaExpression selector, bool descending) =>
        this.keys.Add((CompileSelector(selector), descending));

    public override IEnumerable<object> Apply(IEnumerable<object> source)
    {
        IOrderedEnumerable<object> ordered = this.keys[0].Descending
            ? source.OrderByDescending(this.keys[0].KeySelector, QueryKeyComparer.Instance)
            : source.OrderBy(this.keys[0].KeySelector, QueryKeyComparer.Instance);
        for (int i = 1; i < this.keys.Count; i++)
        {
            ordered = this.keys[i].Descending
                ? ordered.ThenByDescending(this.keys[i].KeySelector, QueryKeyComparer.Instance)
                : ordered.ThenBy(this.keys[i].KeySelector, QueryKeyComparer.Instance);
        }

        return ordered;
    }
}
