namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;

internal sealed class AccessObjectIndexQuery : IAccessIndexQuery<object[]>
{
    private readonly AccessReader reader;
    private readonly string tableName;
    private readonly string indexName;
    private readonly IndexQueryCriteria criteria;

    public AccessObjectIndexQuery(AccessReader reader, string tableName, string indexName)
        : this(reader, tableName, indexName, IndexQueryCriteria.All)
    {
    }

    private AccessObjectIndexQuery(
        AccessReader reader,
        string tableName,
        string indexName,
        IndexQueryCriteria criteria)
    {
        Guard.NotNull(reader, nameof(reader));
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(indexName, nameof(indexName));
        Guard.NotNull(criteria, nameof(criteria));

        this.reader = reader;
        this.tableName = tableName;
        this.indexName = indexName;
        this.criteria = criteria;
    }

    public IAccessIndexQuery<object[]> WhereEquals(params object?[] keyValues) =>
        this.With(IndexQueryCriteria.Exact(keyValues));

    public IAccessIndexQuery<object[]> WhereKeyPrefix(params object?[] prefixValues) =>
        this.With(IndexQueryCriteria.KeyPrefix(prefixValues));

    public IAccessIndexQuery<object[]> WhereBetween(
        object? lower,
        object? upper,
        bool lowerInclusive = true,
        bool upperInclusive = true) =>
        this.WhereRange(
            new IndexKeyBound([lower], lowerInclusive),
            new IndexKeyBound([upper], upperInclusive));

    public IAccessIndexQuery<object[]> WhereRange(IndexKeyBound? lower, IndexKeyBound? upper) =>
        this.With(IndexQueryCriteria.Range(lower, upper));

    public IAsyncEnumerable<object[]> ToRowsAsync(CancellationToken cancellationToken = default) =>
        this.reader.ReadIndexRowsAsObjectsAsync(this.tableName, this.indexName, this.criteria, cancellationToken);

    private AccessObjectIndexQuery With(IndexQueryCriteria nextCriteria)
    {
        if (this.criteria.IsFiltered)
        {
            throw new InvalidOperationException("An index query can contain only one index-key predicate.");
        }

        return new AccessObjectIndexQuery(this.reader, this.tableName, this.indexName, nextCriteria);
    }
}
