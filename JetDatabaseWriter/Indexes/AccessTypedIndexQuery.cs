namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Interfaces;

internal sealed class AccessTypedIndexQuery<T> : AccessIndexQueryBase<T>
    where T : class, new()
{
    public AccessTypedIndexQuery(AccessReader reader, string tableName, string indexName)
        : base(reader, tableName, indexName, IndexQueryCriteria.All)
    {
    }

    private AccessTypedIndexQuery(
        AccessReader reader,
        string tableName,
        string indexName,
        IndexQueryCriteria criteria)
        : base(reader, tableName, indexName, criteria)
    {
    }

    public override IAsyncEnumerable<T> ToRowsAsync(CancellationToken cancellationToken = default) =>
        this.Reader.ReadIndexRowsAsync<T>(this.TableName, this.IndexName, this.Criteria, cancellationToken);

    protected override IAccessIndexQuery<T> WithCriteria(IndexQueryCriteria nextCriteria) =>
        new AccessTypedIndexQuery<T>(this.Reader, this.TableName, this.IndexName, nextCriteria);
}
