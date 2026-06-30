namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// An <see cref="IQueryable{T}"/> over a single Access table that is also
/// async-enumerable. Composition builds an expression tree through the provider;
/// enumeration (sync or async) delegates to the provider's
/// <see cref="IAccessQueryEngine"/>. The provider surfaces the ordered sibling
/// <see cref="AccessOrderedQueryable{T}"/> only for the result of an ordering operator,
/// so this type deliberately does <em>not</em> implement <see cref="IOrderedQueryable{T}"/>:
/// <c>ThenBy</c> / <c>ThenByDescending</c> stay reachable only after <c>OrderBy</c> /
/// <c>OrderByDescending</c>, matching LINQ semantics.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
internal class AccessQueryable<T> : IQueryable<T>, IAsyncEnumerable<T>
{
    public AccessQueryable(IQueryProvider provider)
    {
        this.Provider = provider;
        this.Expression = Expression.Constant(this);
    }

    public AccessQueryable(IQueryProvider provider, Expression expression)
    {
        this.Provider = provider;
        this.Expression = expression;
    }

    public Type ElementType => typeof(T);

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        foreach (object item in ((IAccessQueryEngine)this.Provider).ExecuteSyncList(this.Expression))
        {
            yield return (T)item;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await foreach (object item in ((IAccessQueryEngine)this.Provider).ExecuteStreamAsync(this.Expression, cancellationToken).ConfigureAwait(false))
        {
            yield return (T)item;
        }
    }
}
