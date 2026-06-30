namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Interfaces;

internal sealed class AccessObjectIndexQuery : AccessIndexQueryBase<object[]>
{
    public AccessObjectIndexQuery(AccessReader reader, string tableName, string indexName)
        : base(reader, tableName, indexName, IndexQueryCriteria.All)
    {
    }

    private AccessObjectIndexQuery(
        AccessReader reader,
        string tableName,
        string indexName,
        IndexQueryCriteria criteria)
        : base(reader, tableName, indexName, criteria)
    {
    }

    public override IAsyncEnumerable<object[]> ToRowsAsync(CancellationToken cancellationToken = default) =>
        this.Reader.ReadIndexRowsAsObjectsAsync(this.TableName, this.IndexName, this.Criteria, cancellationToken);

    protected override IAccessIndexQuery<object[]> WithCriteria(IndexQueryCriteria nextCriteria) =>
        new AccessObjectIndexQuery(this.Reader, this.TableName, this.IndexName, nextCriteria);
}
