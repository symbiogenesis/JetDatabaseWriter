namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Compiles shaped query expressions and builds structural entity materializers for the <see cref="JetDatabaseWriter"/> provider.
/// </summary>
/// <param name="dependencies">Parameter object containing dependencies for this service.</param>
/// <param name="queryCompilationContext">The query compilation context.</param>
public sealed class JetShapedQueryCompilingExpressionVisitor(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : ShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext)
{
    private static readonly MethodInfo ExecuteEntityTableMethod =
        typeof(JetShapedQueryCompilingExpressionVisitor).GetMethod(
            nameof(ExecuteEntityTableAsync),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>
    /// Creates an entity materializer delegate for <paramref name="entityType"/> honoring the current <see cref="QueryCompilationContext.QueryTrackingBehavior"/>.
    /// </summary>
    /// <param name="entityType">The entity type to materialize.</param>
    /// <returns>A compiled delegate that materializes an entity from a <see cref="ValueBuffer"/>.</returns>
    public Func<QueryContext, ValueBuffer, object> CreateEntityMaterializer(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        ParameterExpression valueBufferParam = Expression.Parameter(typeof(ValueBuffer), "valueBuffer");
        StructuralTypeShaperExpression shaper = new(entityType, valueBufferParam, nullable: false);
        Expression toInject = shaper;
        if (entityType.IsOwned())
        {
            List<IEntityType> owners = [];
            IEntityType? current = entityType;
            while (current?.IsOwned() == true && current.FindOwnership() is { } ownership)
            {
                current = ownership.PrincipalEntityType;
                owners.Add(current);
            }

            if (owners.Count > 0)
            {
                owners.Reverse();
                List<Expression> blockExpressions = [];
                foreach (IEntityType ownerType in owners)
                {
                    blockExpressions.Add(new StructuralTypeShaperExpression(ownerType, valueBufferParam, nullable: false));
                }

                blockExpressions.Add(shaper);
                toInject = Expression.Block(blockExpressions);
            }
        }

        Expression injected = this.InjectStructuralTypeMaterializers(toInject);
        if (toInject is BlockExpression && injected is BlockExpression injectedBlock && injectedBlock.Expressions.Count > 0)
        {
            injected = injectedBlock.Expressions[^1];
        }

        Expression boxed = Expression.Convert(injected, typeof(object));
        return Expression.Lambda<Func<QueryContext, ValueBuffer, object>>(
            boxed,
            QueryCompilationContext.QueryContextParameter,
            valueBufferParam).Compile();
    }

    /// <inheritdoc/>
    protected override Expression VisitShapedQuery(ShapedQueryExpression shapedQueryExpression)
    {
        ArgumentNullException.ThrowIfNull(shapedQueryExpression);

        if (!this.QueryCompilationContext.IsAsync)
        {
            throw new NotSupportedException(
                "Synchronous query execution is not supported by JetDatabaseWriter.EntityFrameworkCore. Use asynchronous EF Core query methods such as ToListAsync, FirstOrDefaultAsync, SingleOrDefaultAsync, CountAsync, or AnyAsync.");
        }

        if (shapedQueryExpression.QueryExpression is not JetQueryExpression jetQuery)
        {
            throw new InvalidOperationException("Unexpected query expression type.");
        }

        IEntityType entityType = jetQuery.EntityType;
        Func<QueryContext, ValueBuffer, object> materializer = this.CreateEntityMaterializer(entityType);
        MethodInfo genericExecute = ExecuteEntityTableMethod.MakeGenericMethod(entityType.ClrType);
        return Expression.Call(
            genericExecute,
            QueryCompilationContext.QueryContextParameter,
            Expression.Constant(entityType),
            Expression.Constant(materializer));
    }

    private static async IAsyncEnumerable<TEntity> ExecuteEntityTableAsync<TEntity>(
        QueryContext queryContext,
        IEntityType entityType,
        Func<QueryContext, ValueBuffer, object> materializer,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var jetContext = (JetQueryContext)queryContext;
        CancellationToken effectiveToken = cancellationToken != default ? cancellationToken : queryContext.CancellationToken;
        IReadOnlyList<ValueBuffer> buffers = await jetContext.Connection.ReadTableValueBuffersAsync(
            entityType,
            seekConditions: null,
            queryContext.Parameters,
            effectiveToken).ConfigureAwait(false);

        foreach (ValueBuffer buffer in buffers)
        {
            effectiveToken.ThrowIfCancellationRequested();
            yield return (TEntity)materializer(queryContext, buffer);
        }
    }
}
