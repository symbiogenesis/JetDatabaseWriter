namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Tables;

internal sealed class AccessTypedIndexQuery<T> : AccessIndexQueryBase<T>
    where T : class, new()
{
    public AccessTypedIndexQuery(IndexRowReader indexes, string tableName, string indexName)
        : base(indexes, tableName, indexName, IndexQueryCriteria.All)
    {
    }

    private AccessTypedIndexQuery(
        IndexRowReader indexes,
        string tableName,
        string indexName,
        IndexQueryCriteria criteria)
        : base(indexes, tableName, indexName, criteria)
    {
    }

    public override IAsyncEnumerable<T> ToRowsAsync(CancellationToken cancellationToken = default) =>
        this.Indexes.ReadIndexRowsAsync<T>(this.TableName, this.IndexName, this.Criteria, cancellationToken);

    protected override IAccessIndexQuery<T> WithCriteria(IndexQueryCriteria nextCriteria) =>
        new AccessTypedIndexQuery<T>(this.Indexes, this.TableName, this.IndexName, nextCriteria);
}
