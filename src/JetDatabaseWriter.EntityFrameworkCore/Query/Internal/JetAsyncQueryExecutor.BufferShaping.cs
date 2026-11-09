namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

internal static partial class JetAsyncQueryExecutor
{
    private static Func<QueryContext, IAsyncEnumerable<TResult>> BuildShapedBufferSequence<TBuffer, TResult>(
        Expression query, ParameterExpression context, ParameterExpression roots, JetQueryPlan plan, IModel model)
    {
        ParameterExpression input = Expression.Parameter(typeof(TBuffer), "input");
        ParameterExpression scope = Expression.Parameter(typeof(BufferResultScope), "scope");
        Expression shape = ShapeBufferResult(input, typeof(TResult), scope, model);
        Func<TBuffer, BufferResultScope, TResult> materialize = Expression.Lambda<Func<TBuffer, BufferResultScope, TResult>>(shape, input, scope).Compile();
        Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<TBuffer>> evaluate = Expression.Lambda<Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<TBuffer>>>(
            query, context, roots).Compile();
        plan.RequiresMaterializers = true;
        return qc => ExecuteShapedBufferSequence(qc, plan, evaluate, materialize);
    }

    private static Expression ShapeBufferResult(Expression value, Type result, Expression scope, IModel model)
    {
        if (value.Type == result)
        {
            return value;
        }

        if (value.Type == typeof(ValueBuffer) && model.FindEntityType(result) is { } entityType)
        {
            if (entityType.FindPrimaryKey() is null)
            {
                throw new BufferTranslationException();
            }

            return Expression.Convert(
                Expression.Call(
                    scope,
                    nameof(BufferResultScope.Materialize),
                    null,
                    Expression.Constant(entityType, typeof(IEntityType)),
                    value),
                result);
        }

        if (value.Type.IsGenericType && result.IsGenericType
            && value.Type.GetGenericTypeDefinition() == result.GetGenericTypeDefinition()
            && (result.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
                || result.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true))
        {
            ConstructorInfo? constructor = result.GetConstructors().SingleOrDefault() ?? throw new BufferTranslationException();

            MemberInfo[] members = result.IsValueType
                ? result.GetFields(BindingFlags.Public | BindingFlags.Instance).Cast<MemberInfo>().ToArray()
                : result.GetProperties(BindingFlags.Public | BindingFlags.Instance).Cast<MemberInfo>().ToArray();
            Expression[] arguments = members.Select(member =>
            {
                Expression source = Expression.MakeMemberAccess(value, value.Type.GetMember(member.Name)[0]);
                Type target = member is PropertyInfo property ? property.PropertyType : ((FieldInfo)member).FieldType;
                return ShapeBufferResult(source, target, scope, model);
            }).ToArray();
            return Expression.New(constructor, arguments);
        }

        throw new BufferTranslationException();
    }

    private static async IAsyncEnumerable<TResult> ExecuteShapedBufferSequence<TBuffer, TResult>(
        QueryContext context,
        JetQueryPlan plan,
        Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<TBuffer>> evaluate,
        Func<TBuffer, BufferResultScope, TResult> materialize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Dictionary<int, IReadOnlyList<ValueBuffer>> roots = await LoadBufferRootsAsync(context, plan, cancellationToken).ConfigureAwait(false);
        BufferResultScope scope = new(context, plan);
        foreach (TBuffer item in evaluate(context, roots))
        {
            cancellationToken.ThrowIfCancellationRequested();
            context.CancellationToken.ThrowIfCancellationRequested();
            scope.BeginResult();
            yield return materialize(item, scope);
        }
    }

    private sealed class BufferResultScope
    {
        private readonly QueryContext context;
        private readonly JetQueryPlan plan;
        private readonly bool tracking;
        private readonly Dictionary<(IEntityType Type, StructuralKey Key), object>? identities;
        private readonly Dictionary<(IEntityType Type, ValueBuffer Buffer), object> resultEntities = [];

        public BufferResultScope(QueryContext context, JetQueryPlan plan)
        {
            this.context = context;
            this.plan = plan;
            QueryTrackingBehavior behavior = plan.TrackingBehaviorOverride ?? context.Context.ChangeTracker.QueryTrackingBehavior;
            this.tracking = behavior == QueryTrackingBehavior.TrackAll;
            if (this.tracking)
            {
                context.InitializeStateManager(standAlone: false);
            }
            else if (behavior == QueryTrackingBehavior.NoTrackingWithIdentityResolution)
            {
                this.identities = [];
            }
        }

        public void BeginResult() => this.resultEntities.Clear();

        public object? Materialize(IEntityType entityType, ValueBuffer buffer)
        {
            if (buffer.IsEmpty)
            {
                return null;
            }

            if (this.resultEntities.TryGetValue((entityType, buffer), out object? existing))
            {
                return existing;
            }

            if (this.identities is not null && entityType.FindPrimaryKey() is { } key)
            {
                (IEntityType Type, StructuralKey Key) identity = (entityType, ExtractKey(key, buffer));
                if (!this.identities.TryGetValue(identity, out object? entity))
                {
                    entity = this.plan.GetUntrackedMaterializer(this.context, entityType)(this.context, buffer);
                    this.identities.Add(identity, entity);
                }

                this.resultEntities[(entityType, buffer)] = entity;
                return entity;
            }

            object materialized = (this.tracking ? this.plan.GetTrackedMaterializer(this.context, entityType) : this.plan.GetUntrackedMaterializer(this.context, entityType))(this.context, buffer);
            this.resultEntities[(entityType, buffer)] = materialized;
            return materialized;
        }
    }
}
