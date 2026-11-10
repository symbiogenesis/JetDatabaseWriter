namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

internal static partial class JetAsyncQueryExecutor
{
    private static TProperty ReadCandidateProperty<TProperty>(object? entity, IProperty property, JetExecutionMaterializedState state)
    {
        if (entity is null)
        {
            return default!;
        }

        object? value = state.EntityBuffers.TryGetValue(entity, out ValueBuffer buffer)
            ? buffer[property.GetIndex()]
            : property.GetGetter().GetClrValueUsingContainingEntity(entity);
        return value is TProperty typed ? typed : default!;
    }

    private static TEntity ResolveProjectedEntity<TEntity>(
        TEntity entity,
        QueryContext context,
        JetExecutionMaterializedState state,
        JetQueryPlan plan)
    {
        if (entity is null
            || (plan.TrackingBehaviorOverride ?? context.Context.ChangeTracker.QueryTrackingBehavior) != QueryTrackingBehavior.TrackAll
            || !state.EntityBuffers.TryGetValue(entity, out ValueBuffer buffer)
            || !state.EntityTypesByInstance.TryGetValue(entity, out IEntityType? entityType)
            || entityType.FindPrimaryKey() is not { } key)
        {
            return entity;
        }

#pragma warning disable EF1001 // Resolve an existing tracked entity before a client result constructor receives it.
        InternalEntityEntry? entry = context.TryGetEntry(key, ExtractKey(key, buffer).Values!, throwOnNullKey: false, out bool hasNullKey);
        bool alreadyTracked = !hasNullKey && entry is { EntityState: not EntityState.Detached };
#pragma warning restore EF1001
        if (!alreadyTracked)
        {
            // New candidates remain untracked until the final result survives filtering and paging.
            return entity;
        }

        return (TEntity)PromoteEntityInstance(entity, buffer, context, plan, state, state.ProjectedTrackedEntities);
    }

    private sealed class ProjectionEntityRewriter(
        Expression context,
        Expression state,
        JetQueryPlan plan,
        IModel model) : ExpressionVisitor
    {
        private static readonly MethodInfo ResolveMethod = typeof(JetAsyncQueryExecutor)
            .GetMethod(nameof(ResolveProjectedEntity), BindingFlags.NonPublic | BindingFlags.Static)!;

        public Expression ResolveEntity(Expression expression)
        {
            if (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs } unary)
            {
                return unary.Update(this.ResolveEntity(unary.Operand));
            }

            return model.FindEntityType(expression.Type) is null
                ? expression
                : Expression.Call(ResolveMethod.MakeGenericMethod(expression.Type), expression, context, state, Expression.Constant(plan));
        }

        protected override Expression VisitNew(NewExpression node)
            => node.Update(node.Arguments.Select(argument => this.ResolveEntity(this.Visit(argument))));

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
            => node.Update(this.ResolveEntity(this.Visit(node.Expression)));

        protected override Expression VisitNewArray(NewArrayExpression node)
            => node.Update(node.Expressions.Select(expression => this.ResolveEntity(this.Visit(expression))));

        protected override Expression VisitMethodCall(MethodCallExpression node)
            => node.Update(this.Visit(node.Object), node.Arguments.Select(argument => this.ResolveEntity(this.Visit(argument))));
    }
}
