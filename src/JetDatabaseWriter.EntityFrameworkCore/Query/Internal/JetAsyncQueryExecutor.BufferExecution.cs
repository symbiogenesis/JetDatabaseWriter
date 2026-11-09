namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

internal static partial class JetAsyncQueryExecutor
{
    private static bool CanExecuteBuffers(JetQueryPlan plan)
        => plan.IncludeChains.Count == 0 && plan.QueryReferencedNavigations.Count == 0
            && plan.RootTables.All(static p => p.RootEntityType.GetDerivedTypes().Any() == false
                && p.QueryEntityType == p.RootEntityType
                && !p.RootEntityType.GetNavigations().Any(static n => n.TargetEntityType.IsOwned()));

    private static bool TryBuildBufferSequence<TElement>(
        Expression query, JetQueryPlan plan, IModel model,
        out Func<QueryContext, IAsyncEnumerable<TElement>>? executor)
    {
        executor = null;
        if (!CanExecuteBuffers(plan))
        {
            return false;
        }

        ParameterExpression context = Expression.Parameter(typeof(QueryContext), "context");
        ParameterExpression roots = Expression.Parameter(typeof(Dictionary<int, IReadOnlyList<ValueBuffer>>), "roots");
        if (plan.RootTables.Count == 1)
        {
            try
            {
                Expression streamed = new BufferQueryRewriter(context, roots, plan, model, streaming: true).Visit(query)!;
                if (typeof(IAsyncEnumerable<TElement>).IsAssignableFrom(streamed.Type))
                {
                    executor = Expression.Lambda<Func<QueryContext, IAsyncEnumerable<TElement>>>(streamed, context).Compile();
                    return true;
                }

                if (streamed.Type == typeof(IAsyncEnumerable<ValueBuffer>) && model.FindEntityType(typeof(TElement)) is { } entityType)
                {
                    var source = Expression.Lambda<Func<QueryContext, IAsyncEnumerable<ValueBuffer>>>(streamed, context).Compile();
                    executor = qc => MaterializeStream<TElement>(source(qc), qc, plan, entityType);
                    return true;
                }
            }
            catch (BufferTranslationException)
            {
                // Blocking and entity-valued projection operators use the buffered query path.
            }
        }

        try
        {
            Expression rewritten = new BufferQueryRewriter(context, roots, plan, model, streaming: false).Visit(query)!;
            if (!typeof(IEnumerable<TElement>).IsAssignableFrom(rewritten.Type))
            {
                return false;
            }

            var evaluate = Expression.Lambda<Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<TElement>>>(
                rewritten, context, roots).Compile();
            executor = qc => ExecuteBufferSequence(qc, plan, evaluate);
            return true;
        }
        catch (BufferTranslationException)
        {
            return false;
        }
    }

    private static bool TryBuildBufferScalar<TScalar>(
        Expression query, JetQueryPlan plan, IModel model,
        out Func<QueryContext, Task<TScalar>>? executor)
    {
        executor = null;
        if (!CanExecuteBuffers(plan))
        {
            return false;
        }

        ParameterExpression context = Expression.Parameter(typeof(QueryContext), "context");
        ParameterExpression roots = Expression.Parameter(typeof(Dictionary<int, IReadOnlyList<ValueBuffer>>), "roots");
        try
        {
            Expression rewritten = new BufferQueryRewriter(context, roots, plan, model, streaming: false).Visit(query)!;
            if (rewritten.Type != typeof(TScalar))
            {
                return false;
            }

            var evaluate = Expression.Lambda<Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, TScalar>>(
                rewritten, context, roots).Compile();
            executor = async qc => evaluate(qc, await LoadBufferRootsAsync(qc, plan).ConfigureAwait(false));
            return true;
        }
        catch (BufferTranslationException)
        {
            return false;
        }
    }

    private static async IAsyncEnumerable<TElement> ExecuteBufferSequence<TElement>(
        QueryContext context, JetQueryPlan plan,
        Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<TElement>> evaluate,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Dictionary<int, IReadOnlyList<ValueBuffer>> roots = await LoadBufferRootsAsync(context, plan).ConfigureAwait(false);
        foreach (TElement element in evaluate(context, roots))
        {
            cancellationToken.ThrowIfCancellationRequested();
            context.CancellationToken.ThrowIfCancellationRequested();
            yield return element;
        }
    }

    private static async Task<Dictionary<int, IReadOnlyList<ValueBuffer>>> LoadBufferRootsAsync(QueryContext context, JetQueryPlan plan)
    {
        Dictionary<int, IReadOnlyList<ValueBuffer>> roots = [];
        foreach (JetTableLoadPlan table in plan.RootTables)
        {
            roots[table.RootId] = await ((JetQueryContext)context).Connection.ReadTableValueBuffersAsync(
                table.RootEntityType, table.SeekConditions, context.Parameters, table.RequiredProperties,
                context.CancellationToken).ConfigureAwait(false);
        }

        return roots;
    }

    private static IQueryable<ValueBuffer> BufferRoot(Dictionary<int, IReadOnlyList<ValueBuffer>> roots, int id)
        => roots[id].AsQueryable();

    private static IAsyncEnumerable<ValueBuffer> StreamBufferRoot(QueryContext context, JetTableLoadPlan table)
        => ((JetQueryContext)context).Connection.StreamTableValueBuffersAsync(
            table.RootEntityType, table.SeekConditions, context.Parameters, table.RequiredProperties,
            maxRows: null, orderedProperties: null, rowFilter: null, context.CancellationToken);

    private static async IAsyncEnumerable<TElement> MaterializeStream<TElement>(
        IAsyncEnumerable<ValueBuffer> source, QueryContext context, JetQueryPlan plan, IEntityType entityType,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        QueryTrackingBehavior behavior = plan.TrackingBehaviorOverride ?? context.Context.ChangeTracker.QueryTrackingBehavior;
        bool tracking = behavior == QueryTrackingBehavior.TrackAll;
        if (tracking)
        {
            context.InitializeStateManager(standAlone: false);
        }

        Func<QueryContext, ValueBuffer, object> materializer = tracking
            ? plan.GetTrackedMaterializer(context, entityType) : plan.GetUntrackedMaterializer(context, entityType);
        Dictionary<StructuralKey, object>? identities = behavior == QueryTrackingBehavior.NoTrackingWithIdentityResolution ? [] : null;
        IKey? key = entityType.FindPrimaryKey();
        await foreach (ValueBuffer buffer in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            object entity;
            if (identities is not null && key is not null)
            {
                StructuralKey identity = ExtractKey(key, buffer);
                if (!identities.TryGetValue(identity, out entity!))
                {
                    entity = materializer(context, buffer);
                    identities.Add(identity, entity);
                }
            }
            else
            {
                entity = materializer(context, buffer);
            }

            yield return (TElement)entity;
        }
    }

    private static async IAsyncEnumerable<T> StreamWhere<T>(IAsyncEnumerable<T> source, Func<T, bool> predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (predicate(item))
            {
                yield return item;
            }
        }
    }

    private static async IAsyncEnumerable<TResult> StreamSelect<T, TResult>(IAsyncEnumerable<T> source, Func<T, TResult> selector,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return selector(item);
        }
    }

    private static async IAsyncEnumerable<T> StreamSkip<T>(IAsyncEnumerable<T> source, int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (count > 0)
            {
                count--;
            }
            else
            {
                yield return item;
            }
        }
    }

    private static async IAsyncEnumerable<T> StreamTake<T>(IAsyncEnumerable<T> source, int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (count <= 0)
        {
            yield break;
        }

        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
            if (--count == 0)
            {
                yield break;
            }
        }
    }

    private static async IAsyncEnumerable<T> StreamDistinct<T>(IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        HashSet<T> seen = [];
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (seen.Add(item))
            {
                yield return item;
            }
        }
    }

    private sealed class BufferTranslationException : Exception;

    private sealed class BufferQueryRewriter(
        ParameterExpression context, ParameterExpression roots, JetQueryPlan plan, IModel model, bool streaming) : ExpressionVisitor
    {
        private readonly Dictionary<ParameterExpression, ParameterExpression> parameters = [];

        public HashSet<IProperty> ReferencedProperties { get; } = [];

        private Type MapType(Type type)
        {
            if (model.FindEntityType(type) is not null)
            {
                return typeof(ValueBuffer);
            }

            if (type.IsGenericType)
            {
                Type[] args = type.GetGenericArguments().Select(this.MapType).ToArray();
                return type.GetGenericTypeDefinition().MakeGenericType(args);
            }

            return type;
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is QueryParameterExpression parameter)
            {
                return Expression.Call(GetParameterValueMethod.MakeGenericMethod(parameter.Type), context, Expression.Constant(parameter.Name));
            }

            if (node is not EntityQueryRootExpression root || !plan.RootPlansByExpression.TryGetValue(root, out JetTableLoadPlan? table))
            {
                throw new BufferTranslationException();
            }

            Expression result = streaming
                ? Expression.Call(typeof(JetAsyncQueryExecutor), nameof(StreamBufferRoot), null, context, Expression.Constant(table))
                : Expression.Call(typeof(JetAsyncQueryExecutor), nameof(BufferRoot), null, roots, Expression.Constant(table.RootId));
            if (!plan.IgnoreQueryFilters)
            {
                foreach (IQueryFilter filter in GetAllQueryFilters(table.QueryEntityType))
                {
                    if (filter.Expression is not LambdaExpression lambda)
                    {
                        throw new BufferTranslationException();
                    }

                    LambdaExpression predicate = (LambdaExpression)this.Visit(lambda);
                    result = streaming
                        ? Expression.Call(typeof(JetAsyncQueryExecutor), nameof(StreamWhere), [typeof(ValueBuffer)], result, predicate, Expression.Default(typeof(CancellationToken)))
                        : Expression.Call(typeof(Queryable), nameof(Queryable.Where), [typeof(ValueBuffer)], result, Expression.Quote(predicate));
                }
            }

            return result;
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            ParameterExpression[] mapped = node.Parameters.Select(p => Expression.Parameter(this.MapType(p.Type), p.Name)).ToArray();
            for (int i = 0; i < mapped.Length; i++)
            {
                this.parameters[node.Parameters[i]] = mapped[i];
            }

            return Expression.Lambda(this.Visit(node.Body)!, mapped);
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => this.parameters.TryGetValue(node, out ParameterExpression? parameter) ? parameter : node;

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null && model.FindEntityType(node.Expression.Type) is { } entityType)
            {
                IProperty? property = entityType.FindProperty(node.Member);
                if (property is null)
                {
                    throw new BufferTranslationException();
                }

                this.ReferencedProperties.Add(property);

                return Expression.Call(ReadValueBufferPropertyMethod.MakeGenericMethod(node.Type), this.Visit(node.Expression)!, Expression.Constant(property.GetIndex()));
            }

            Expression? receiver = this.Visit(node.Expression);
            if (receiver is not null && receiver.Type != node.Expression!.Type)
            {
                MemberInfo? member = receiver.Type.GetMember(node.Member.Name, BindingFlags.Public | BindingFlags.Instance).FirstOrDefault();
                return member is null ? throw new BufferTranslationException() : Expression.MakeMemberAccess(receiver, member);
            }

            return node.Update(receiver);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            Expression[] args = node.Arguments.Select(a => this.Visit(a)!).ToArray();
            Type mapped = this.MapType(node.Type);
            if (mapped == node.Type)
            {
                if (node.Arguments.Where((a, i) => a.Type != args[i].Type).Any())
                {
                    throw new BufferTranslationException();
                }

                return node.Update(args);
            }

            ConstructorInfo? constructor = mapped.GetConstructor(args.Select(static a => a.Type).ToArray());
            if (constructor is null)
            {
                throw new BufferTranslationException();
            }

            return node.Members is null ? Expression.New(constructor, args)
                : Expression.New(constructor, args, node.Members.Select(m => mapped.GetMember(m.Name)[0]));
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            Expression operand = this.Visit(node.Operand)!;
            if (node.NodeType == ExpressionType.Quote)
            {
                return Expression.Quote((LambdaExpression)operand);
            }

            Type mapped = this.MapType(node.Type);
            return Expression.MakeUnary(node.NodeType, operand, mapped, node.Method);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            Expression unwrapped = UnwrapEfQueryModifiers(node);
            if (!ReferenceEquals(unwrapped, node))
            {
                return this.Visit(unwrapped)!;
            }

            if (node.Method.DeclaringType == typeof(EF) && node.Method.Name == nameof(EF.Property)
                && node.Arguments[1] is ConstantExpression { Value: string name }
                && model.FindEntityType(node.Arguments[0].Type) is { } entityType)
            {
                IProperty? property = entityType.FindProperty(name);
                if (property is not null)
                {
                    this.ReferencedProperties.Add(property);
                }
                return property is null ? throw new BufferTranslationException()
                    : Expression.Call(ReadValueBufferPropertyMethod.MakeGenericMethod(node.Type), this.Visit(node.Arguments[0])!, Expression.Constant(property.GetIndex()));
            }

            if (node.Method.DeclaringType == typeof(DbFunctionsExtensions) && node.Method.Name == nameof(DbFunctionsExtensions.Like))
            {
                return Expression.Call(EvaluateLikeMethod, this.Visit(node.Arguments[1])!, this.Visit(node.Arguments[2])!,
                    node.Arguments.Count == 4 ? this.Visit(node.Arguments[3])! : Expression.Constant(null, typeof(string)));
            }

            if (streaming && node.Method.DeclaringType == typeof(Queryable))
            {
                string method = node.Method.Name switch
                {
                    nameof(Queryable.Where) => nameof(StreamWhere),
                    nameof(Queryable.Select) => nameof(StreamSelect),
                    nameof(Queryable.Skip) when node.Arguments[1].Type == typeof(int) => nameof(StreamSkip),
                    nameof(Queryable.Take) when node.Arguments[1].Type == typeof(int) => nameof(StreamTake),
                    nameof(Queryable.Distinct) when node.Arguments.Count == 1 => nameof(StreamDistinct),
                    _ => throw new BufferTranslationException(),
                };
                List<Expression> arguments = node.Arguments.Select(a => a.NodeType == ExpressionType.Quote
                    ? this.Visit(((UnaryExpression)a).Operand)! : this.Visit(a)!).ToList();
                if (arguments.Any(static a => a is LambdaExpression { Parameters.Count: > 1 }))
                {
                    throw new BufferTranslationException();
                }

                arguments.Add(Expression.Default(typeof(CancellationToken)));
                return Expression.Call(typeof(JetAsyncQueryExecutor), method, node.Method.GetGenericArguments().Select(this.MapType).ToArray(), arguments.ToArray());
            }

            Expression[] args = node.Arguments.Select(a => this.Visit(a)!).ToArray();
            MethodInfo mappedMethod = node.Method.IsGenericMethod
                ? node.Method.GetGenericMethodDefinition().MakeGenericMethod(node.Method.GetGenericArguments().Select(this.MapType).ToArray()) : node.Method;
            ParameterInfo[] declared = mappedMethod.GetParameters();
            if (declared.Where((p, i) => !p.ParameterType.IsAssignableFrom(args[i].Type)).Any())
            {
                throw new BufferTranslationException();
            }

            return Expression.Call(this.Visit(node.Object), mappedMethod, args);
        }
    }
}
