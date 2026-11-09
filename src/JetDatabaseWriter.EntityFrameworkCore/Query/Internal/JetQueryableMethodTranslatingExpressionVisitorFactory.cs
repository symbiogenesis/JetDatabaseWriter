namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using Microsoft.EntityFrameworkCore.Query;

/// <summary>
/// Satisfies EF's compilation-context service dependency while refusing the unused shaped-query translation path.
/// Queries are compiled directly by the provider's query executor.
/// </summary>
public sealed class JetQueryableMethodTranslatingExpressionVisitorFactory : IQueryableMethodTranslatingExpressionVisitorFactory
{
    /// <inheritdoc/>
    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => throw new NotSupportedException("JET queries are compiled by the provider's query executor.");
}
