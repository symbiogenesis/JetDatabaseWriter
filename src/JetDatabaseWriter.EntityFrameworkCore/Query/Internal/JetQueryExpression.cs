namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Represents a server-side query expression over a JET/ACE table in the EF Core query pipeline.
/// </summary>
public sealed class JetQueryExpression : Expression, IPrintableExpression
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JetQueryExpression"/> class.
    /// </summary>
    /// <param name="entityType">The root entity type queried.</param>
    public JetQueryExpression(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        this.EntityType = entityType;
    }

    /// <summary>
    /// Gets the root entity type queried.
    /// </summary>
    public IEntityType EntityType { get; }

    /// <inheritdoc/>
    public override ExpressionType NodeType => ExpressionType.Extension;

    /// <inheritdoc/>
    public override Type Type => typeof(IEnumerable<ValueBuffer>);

    /// <inheritdoc/>
    public void Print(ExpressionPrinter expressionPrinter)
    {
        ArgumentNullException.ThrowIfNull(expressionPrinter);

        _ = expressionPrinter.Append($"JetQueryExpression({this.EntityType.DisplayName()})");
    }

    /// <inheritdoc/>
    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
}
