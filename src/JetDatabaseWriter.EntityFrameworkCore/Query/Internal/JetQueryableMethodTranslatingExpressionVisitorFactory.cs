namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using Microsoft.EntityFrameworkCore.Query;

/// <summary>
/// Creates <see cref="JetQueryableMethodTranslatingExpressionVisitor"/> instances.
/// </summary>
public sealed class JetQueryableMethodTranslatingExpressionVisitorFactory : IQueryableMethodTranslatingExpressionVisitorFactory
{
    private readonly QueryableMethodTranslatingExpressionVisitorDependencies dependencies;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetQueryableMethodTranslatingExpressionVisitorFactory"/> class.
    /// </summary>
    /// <param name="dependencies">Parameter object containing dependencies for this service.</param>
    public JetQueryableMethodTranslatingExpressionVisitorFactory(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);

        this.dependencies = dependencies;
    }

    /// <inheritdoc/>
    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
    {
        ArgumentNullException.ThrowIfNull(queryCompilationContext);

        return new JetQueryableMethodTranslatingExpressionVisitor(this.dependencies, queryCompilationContext);
    }
}
