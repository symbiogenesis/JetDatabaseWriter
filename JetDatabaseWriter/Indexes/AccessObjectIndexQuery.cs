namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Tables;

internal sealed class AccessObjectIndexQuery : AccessIndexQueryBase<object[]>
{
    public AccessObjectIndexQuery(IndexRowReader indexes, string tableName, string indexName)
        : base(indexes, tableName, indexName, IndexQueryCriteria.All)
    {
    }

    private AccessObjectIndexQuery(
        IndexRowReader indexes,
        string tableName,
        string indexName,
        IndexQueryCriteria criteria)
        : base(indexes, tableName, indexName, criteria)
    {
    }

    public override IAsyncEnumerable<object[]> ToRowsAsync(CancellationToken cancellationToken = default) =>
        this.Indexes.ReadIndexRowsAsObjectsAsync(this.TableName, this.IndexName, this.Criteria, cancellationToken);

    protected override IAccessIndexQuery<object[]> WithCriteria(IndexQueryCriteria nextCriteria) =>
        new AccessObjectIndexQuery(this.Indexes, this.TableName, this.IndexName, nextCriteria);
}
