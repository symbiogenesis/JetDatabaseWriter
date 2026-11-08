namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using Microsoft.EntityFrameworkCore.Query;

/// <summary>
/// Creates <see cref="JetShapedQueryCompilingExpressionVisitor"/> instances.
/// </summary>
public sealed class JetShapedQueryCompilingExpressionVisitorFactory : IShapedQueryCompilingExpressionVisitorFactory
{
    private readonly ShapedQueryCompilingExpressionVisitorDependencies dependencies;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetShapedQueryCompilingExpressionVisitorFactory"/> class.
    /// </summary>
    /// <param name="dependencies">Parameter object containing dependencies for this service.</param>
    public JetShapedQueryCompilingExpressionVisitorFactory(
        ShapedQueryCompilingExpressionVisitorDependencies dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);

        this.dependencies = dependencies;
    }

    /// <inheritdoc/>
    public ShapedQueryCompilingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
    {
        ArgumentNullException.ThrowIfNull(queryCompilationContext);

        return new JetShapedQueryCompilingExpressionVisitor(this.dependencies, queryCompilationContext);
    }
}
