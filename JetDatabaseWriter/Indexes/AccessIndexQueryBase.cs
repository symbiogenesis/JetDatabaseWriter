namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;

internal abstract class AccessIndexQueryBase<TRow> : IAccessIndexQuery<TRow>
{
    protected AccessIndexQueryBase(
        AccessReader reader,
        string tableName,
        string indexName,
        IndexQueryCriteria criteria)
    {
        Guard.NotNull(reader, nameof(reader));
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(indexName, nameof(indexName));
        Guard.NotNull(criteria, nameof(criteria));

        this.Reader = reader;
        this.TableName = tableName;
        this.IndexName = indexName;
        this.Criteria = criteria;
    }

    protected AccessReader Reader { get; }

    protected string TableName { get; }

    protected string IndexName { get; }

    protected IndexQueryCriteria Criteria { get; }

    public IAccessIndexQuery<TRow> WhereEquals(params object?[] keyValues) =>
        this.With(IndexQueryCriteria.Exact(keyValues));

    public IAccessIndexQuery<TRow> WhereKeyPrefix(params object?[] prefixValues) =>
        this.With(IndexQueryCriteria.KeyPrefix(prefixValues));

    public IAccessIndexQuery<TRow> WhereBetween(
        object? lower,
        object? upper,
        bool lowerInclusive = true,
        bool upperInclusive = true) =>
        this.WhereRange(
            new IndexKeyBound([lower], lowerInclusive),
            new IndexKeyBound([upper], upperInclusive));

    public IAccessIndexQuery<TRow> WhereRange(IndexKeyBound? lower, IndexKeyBound? upper) =>
        this.With(IndexQueryCriteria.Range(lower, upper));

    public abstract IAsyncEnumerable<TRow> ToRowsAsync(CancellationToken cancellationToken = default);

    protected abstract IAccessIndexQuery<TRow> WithCriteria(IndexQueryCriteria nextCriteria);

    private IAccessIndexQuery<TRow> With(IndexQueryCriteria nextCriteria)
    {
        if (this.Criteria.IsFiltered)
        {
            throw new InvalidOperationException("An index query can contain only one index-key predicate.");
        }

        return this.WithCriteria(nextCriteria);
    }
}
