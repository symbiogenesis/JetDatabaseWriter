namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

/// <summary>
/// Adapts a composed <see cref="IQueryable{TEntity}"/> (always an
/// <see cref="AccessQueryable{TEntity}"/>) to <see cref="IIncludableQueryable{TEntity, TProperty}"/>
/// so an <c>Include</c> / <c>ThenInclude</c> chain can carry the most recently
/// included property type. Every member delegates to the wrapped query, including
/// async enumeration, so the public async terminals keep working on the result.
/// </summary>
/// <typeparam name="TEntity">The query element type.</typeparam>
/// <typeparam name="TProperty">The most recently included navigation type.</typeparam>
/// <param name="source">The composed query to wrap.</param>
internal sealed class IncludableQueryable<TEntity, TProperty>(IQueryable<TEntity> source)
    : IIncludableQueryable<TEntity, TProperty>, IAsyncEnumerable<TEntity>
{
    public Type ElementType => source.ElementType;

    public Expression Expression => source.Expression;

    public IQueryProvider Provider => source.Provider;

    public IEnumerator<TEntity> GetEnumerator() => source.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => source.GetEnumerator();

    public IAsyncEnumerator<TEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        ((IAsyncEnumerable<TEntity>)source).GetAsyncEnumerator(cancellationToken);
}
