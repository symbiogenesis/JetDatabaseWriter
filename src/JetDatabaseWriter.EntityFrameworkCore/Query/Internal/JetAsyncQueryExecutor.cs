namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Compiles and executes EF Core LINQ queries over <see cref="JetDatabaseWriter"/> with B-tree index seek pushdown,
/// structural entity materialization, change-tracker identity resolution, and eager navigation loading.
/// Synchronous query entry points execute as thin sync-over-async wrappers over the asynchronous query pipeline.
/// </summary>
internal static class JetAsyncQueryExecutor
{
    private static readonly MethodInfo BuildSequenceExecutorMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(BuildSequenceExecutor),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo BuildScalarExecutorMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(BuildScalarExecutor),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo BuildSyncSequenceExecutorMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(BuildSyncSequenceExecutor),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo BuildSyncQueryableExecutorMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(BuildSyncQueryableExecutor),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo BuildSyncIncludableQueryableExecutorMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(BuildSyncIncludableQueryableExecutor),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo BuildSyncScalarExecutorMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(BuildSyncScalarExecutor),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo ReadValueBufferPropertyMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(ReadValueBufferProperty),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo ReadEfPropertyMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(ReadEfProperty),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo EvaluateLikeMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(EvaluateLike),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo EvaluateContainsMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(EvaluateContains),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo GetParameterValueMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(GetParameterValue),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo GetRootEnumerableMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(GetRootEnumerable),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo AsQueryableGenericMethod =
        typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(static m => m.Name == nameof(Queryable.AsQueryable) && m.IsGenericMethod);

    private static readonly MethodInfo[] EnumerableStaticMethods =
        typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static);

    /// <summary>
    /// Compiles an EF Core query expression into an executable delegate.
    /// </summary>
    /// <typeparam name="TResult">The query result type.</typeparam>
    /// <param name="query">The LINQ query expression.</param>
    /// <param name="async">Whether the query is executed asynchronously.</param>
    /// <param name="dependencies">The database dependencies.</param>
    /// <param name="preprocessorFactory">The query translation preprocessor factory.</param>
    /// <param name="shaperFactory">The shaped query compiling expression visitor factory.</param>
    /// <returns>A compiled query executor delegate.</returns>
    /// <exception cref="NotSupportedException">Thrown when the query result type is unsupported.</exception>
    internal static Func<QueryContext, TResult> Compile<TResult>(
        Expression query,
        bool async,
        DatabaseDependencies dependencies,
        IQueryTranslationPreprocessorFactory preprocessorFactory,
        IShapedQueryCompilingExpressionVisitorFactory shaperFactory)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(dependencies);
        ArgumentNullException.ThrowIfNull(preprocessorFactory);
        ArgumentNullException.ThrowIfNull(shaperFactory);

        QueryCompilationContext compilationContext = dependencies.QueryCompilationContextFactory.Create(async);
        IModel model = compilationContext.Model;
        JetQueryPlan plan = JetQueryPlanExtractor.Analyze(query, model);

        Type resultType = typeof(TResult);
        if (!async)
        {
            return CompileSync<TResult>(query, plan, model, resultType);
        }

        if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            Type elementType = resultType.GetGenericArguments()[0];
            MethodInfo genericMethod = BuildSequenceExecutorMethod.MakeGenericMethod(elementType);
            return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, model])!;
        }

        if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(Task<>))
        {
            Type scalarType = resultType.GetGenericArguments()[0];
            MethodInfo genericMethod = BuildScalarExecutorMethod.MakeGenericMethod(scalarType);
            return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, model])!;
        }

        throw new NotSupportedException($"Unsupported asynchronous query result type '{resultType}'.");
    }

    private static Func<QueryContext, TResult> CompileSync<TResult>(
        Expression query,
        JetQueryPlan plan,
        IModel model,
        Type resultType)
    {
        if (resultType.IsGenericType)
        {
            Type genericDef = resultType.GetGenericTypeDefinition();
            if (genericDef == typeof(IEnumerable<>) && !IsSingleValueSequenceResult(query, resultType))
            {
                Type elementType = resultType.GetGenericArguments()[0];
                MethodInfo genericMethod = BuildSyncSequenceExecutorMethod.MakeGenericMethod(elementType);
                return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, model])!;
            }

            if (genericDef == typeof(IQueryable<>) || genericDef == typeof(IOrderedQueryable<>))
            {
                Type elementType = resultType.GetGenericArguments()[0];
                MethodInfo genericMethod = BuildSyncQueryableExecutorMethod.MakeGenericMethod(elementType);
                return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, model])!;
            }

            if (genericDef == typeof(IIncludableQueryable<,>))
            {
                Type[] genericArgs = resultType.GetGenericArguments();
                MethodInfo genericMethod = BuildSyncIncludableQueryableExecutorMethod.MakeGenericMethod(genericArgs[0], genericArgs[1]);
                return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, model])!;
            }
        }

        if ((resultType == typeof(object)
             || resultType == typeof(IEnumerable)
             || resultType == typeof(IQueryable)
             || resultType == typeof(IOrderedQueryable))
            && TryGetUntypedSequenceElementType(query.Type, out Type? untypedElementType))
        {
            MethodInfo genericMethod = BuildSyncQueryableExecutorMethod.MakeGenericMethod(untypedElementType!);
            return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, model])!;
        }

        MethodInfo scalarMethod = BuildSyncScalarExecutorMethod.MakeGenericMethod(resultType);
        return (Func<QueryContext, TResult>)scalarMethod.Invoke(null, [query, plan, model])!;
    }

    private static bool IsSingleValueSequenceResult(Expression query, Type resultType)
        => query is MethodCallExpression { Arguments.Count: >= 1 } call
            && call.Method.DeclaringType == typeof(Queryable)
            && call.Method.Name is nameof(Queryable.First)
                or nameof(Queryable.FirstOrDefault)
                or nameof(Queryable.Single)
                or nameof(Queryable.SingleOrDefault)
                or nameof(Queryable.Last)
                or nameof(Queryable.LastOrDefault)
                or nameof(Queryable.ElementAt)
                or nameof(Queryable.ElementAtOrDefault)
                or nameof(Queryable.Min)
                or nameof(Queryable.Max)
                or nameof(Queryable.MinBy)
                or nameof(Queryable.MaxBy)
                or nameof(Queryable.Aggregate)
            && call.Type == resultType
            && typeof(IQueryable<>).MakeGenericType(resultType).IsAssignableFrom(call.Arguments[0].Type);

    private static bool TryGetUntypedSequenceElementType(Type queryType, out Type? elementType)
    {
        if (queryType != typeof(string) && queryType != typeof(byte[]))
        {
            if (queryType.IsGenericType)
            {
                Type def = queryType.GetGenericTypeDefinition();
                if (def == typeof(IQueryable<>)
                    || def == typeof(IOrderedQueryable<>)
                    || def == typeof(IIncludableQueryable<,>)
                    || def == typeof(IEnumerable<>))
                {
                    elementType = queryType.GetGenericArguments()[0];
                    return true;
                }
            }

            foreach (Type iface in queryType.GetInterfaces())
            {
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IQueryable<>))
                {
                    elementType = iface.GetGenericArguments()[0];
                    return true;
                }
            }
        }

        elementType = null;
        return false;
    }

    private static Func<QueryContext, IEnumerable<TElement>> BuildSyncSequenceExecutor<TElement>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        Func<QueryContext, IAsyncEnumerable<TElement>> asyncExecutor = BuildSequenceExecutor<TElement>(query, plan, model);
        return queryContext => ExecuteSequenceSync(asyncExecutor, queryContext);
    }

    private static Func<QueryContext, IOrderedQueryable<TElement>> BuildSyncQueryableExecutor<TElement>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        Func<QueryContext, IAsyncEnumerable<TElement>> asyncExecutor = BuildSequenceExecutor<TElement>(query, plan, model);
        return queryContext => (IOrderedQueryable<TElement>)ExecuteSequenceSync(asyncExecutor, queryContext).AsQueryable();
    }

    private static Func<QueryContext, IIncludableQueryable<TElement, TProperty>> BuildSyncIncludableQueryableExecutor<TElement, TProperty>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        Func<QueryContext, IAsyncEnumerable<TElement>> asyncExecutor = BuildSequenceExecutor<TElement>(query, plan, model);
        return queryContext => new MaterializedIncludableQueryable<TElement, TProperty>(
            (IOrderedQueryable<TElement>)ExecuteSequenceSync(asyncExecutor, queryContext).AsQueryable());
    }

    private static Func<QueryContext, TScalar> BuildSyncScalarExecutor<TScalar>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        Func<QueryContext, Task<TScalar>> asyncExecutor = BuildScalarExecutor<TScalar>(query, plan, model);
        return queryContext => ExecuteScalarSync(asyncExecutor, queryContext);
    }

    private static List<TElement> ExecuteSequenceSync<TElement>(
        Func<QueryContext, IAsyncEnumerable<TElement>> asyncExecutor,
        QueryContext queryContext)
    {
#pragma warning disable RS0030, MA0042 // Synchronous query execution is a required sync-over-async callback boundary.
        return MaterializeAsync(asyncExecutor, queryContext)
            .GetAwaiter()
            .GetResult();
#pragma warning restore RS0030, MA0042

        static async Task<List<TElement>> MaterializeAsync(
            Func<QueryContext, IAsyncEnumerable<TElement>> executor,
            QueryContext qc)
        {
            List<TElement> results = [];
            await foreach (TElement item in executor(qc).WithCancellation(qc.CancellationToken).ConfigureAwait(false))
            {
                results.Add(item);
            }

            return results;
        }
    }

    private static TScalar ExecuteScalarSync<TScalar>(
        Func<QueryContext, Task<TScalar>> asyncExecutor,
        QueryContext queryContext) =>
#pragma warning disable RS0030, MA0042 // Synchronous query execution is a required sync-over-async callback boundary.
        asyncExecutor(queryContext)
            .GetAwaiter()
            .GetResult();
#pragma warning restore RS0030, MA0042

    private static Func<QueryContext, IAsyncEnumerable<TElement>> BuildSequenceExecutor<TElement>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        Func<QueryContext, JetExecutionMaterializedState, IEnumerable<TElement>> compiledQuery =
            CompileSequenceDelegate<TElement>(query, plan, model);
        return queryContext => ExecuteSequenceAsync(queryContext, plan, compiledQuery);
    }

    private static Func<QueryContext, Task<TScalar>> BuildScalarExecutor<TScalar>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        if (TryBuildEntityFreeAggregateExecutor<TScalar>(query, plan, out Func<QueryContext, Task<TScalar>>? fastExecutor))
        {
            return fastExecutor!;
        }

        Func<QueryContext, JetExecutionMaterializedState, TScalar> compiledQuery =
            CompileScalarDelegate<TScalar>(query, plan, model);
        return queryContext => ExecuteScalarAsync(queryContext, plan, compiledQuery);
    }

    private static Expression StripQuotesAndObjectConvert(Expression expression)
    {
        Expression current = expression;
        while (current is UnaryExpression unary
               && (unary.NodeType == ExpressionType.Quote
                   || (unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked
                       && unary.Type == typeof(object))))
        {
            current = unary.Operand;
        }

        return current;
    }

    private static Expression UnwrapEfQueryModifiers(Expression expression)
    {
        Expression current = expression;
        while (current is MethodCallExpression wrapperCall
               && wrapperCall.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
               && wrapperCall.Method.Name is nameof(EntityFrameworkQueryableExtensions.AsNoTracking)
                   or nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution)
                   or nameof(EntityFrameworkQueryableExtensions.AsTracking)
                   or nameof(EntityFrameworkQueryableExtensions.IgnoreQueryFilters)
                   or nameof(EntityFrameworkQueryableExtensions.IgnoreAutoIncludes)
                   or nameof(EntityFrameworkQueryableExtensions.TagWith)
                   or nameof(EntityFrameworkQueryableExtensions.TagWithCallSite))
        {
            current = wrapperCall.Arguments[0];
        }

        return current;
    }

    private static TProperty ReadValueBufferProperty<TProperty>(ValueBuffer buffer, int index)
    {
        object? value = (uint)index < (uint)buffer.Count ? buffer[index] : null;
        return value is TProperty typed ? typed : default!;
    }

    private static bool TryBuildEntityFreeAggregateExecutor<TScalar>(
        Expression query,
        JetQueryPlan plan,
        out Func<QueryContext, Task<TScalar>>? executor)
    {
        executor = null;
        if (plan.RootTables.Count != 1
            || plan.IncludeChains.Count > 0
            || plan.QueryReferencedNavigations.Count > 0)
        {
            return false;
        }

        JetTableLoadPlan tablePlan = plan.RootTables[0];
        if (tablePlan.CandidateRowFilter is not null)
        {
            return false;
        }

        Expression current = UnwrapEfQueryModifiers(query);
        if (current is not MethodCallExpression { Arguments.Count: 1 or 2 } terminalCall
            || terminalCall.Method.DeclaringType != typeof(Queryable))
        {
            return false;
        }

        Expression inner = terminalCall.Arguments[0];
        while (true)
        {
            Expression unwrapped = UnwrapEfQueryModifiers(inner);
            if (unwrapped is MethodCallExpression { Arguments.Count: 2 } whereCall
                && whereCall.Method.DeclaringType == typeof(Queryable)
                && whereCall.Method.Name == nameof(Queryable.Where)
                && StripQuotesAndObjectConvert(whereCall.Arguments[1]) is LambdaExpression { Parameters.Count: 1 } wLambda
                && wLambda.Parameters[0].Type.IsAssignableFrom(tablePlan.QueryEntityType.ClrType))
            {
                inner = whereCall.Arguments[0];
                continue;
            }

            inner = unwrapped;
            break;
        }

        if (inner is not EntityQueryRootExpression)
        {
            return false;
        }

        Func<QueryContext, ValueBuffer, bool>? vbFilter = tablePlan.ValueBufferRowFilter;

        if (terminalCall.Method.Name == nameof(Queryable.Count) && typeof(TScalar) == typeof(int))
        {
            async Task<int> ExecuteCountAsync(QueryContext queryContext)
            {
                CancellationToken ct = queryContext.CancellationToken;
                ct.ThrowIfCancellationRequested();
                var jetContext = (JetQueryContext)queryContext;
                int count = 0;
                _ = await jetContext.Connection.ReadTableValueBuffersAsync(
                    tablePlan.RootEntityType,
                    tablePlan.SeekConditions,
                    queryContext.Parameters,
                    tablePlan.RequiredProperties,
                    maxRows: null,
                    orderedAscendingProperties: null,
                    rowFilter: buffer =>
                    {
                        if (vbFilter is null || vbFilter(queryContext, buffer))
                        {
                            count++;
                        }

                        return false;
                    },
                    ct).ConfigureAwait(false);
                return count;
            }

            executor = (Func<QueryContext, Task<TScalar>>)(object)(Func<QueryContext, Task<int>>)ExecuteCountAsync;
            return true;
        }

        if (terminalCall.Method.Name == nameof(Queryable.LongCount) && typeof(TScalar) == typeof(long))
        {
            async Task<long> ExecuteLongCountAsync(QueryContext queryContext)
            {
                CancellationToken ct = queryContext.CancellationToken;
                ct.ThrowIfCancellationRequested();
                var jetContext = (JetQueryContext)queryContext;
                long count = 0;
                _ = await jetContext.Connection.ReadTableValueBuffersAsync(
                    tablePlan.RootEntityType,
                    tablePlan.SeekConditions,
                    queryContext.Parameters,
                    tablePlan.RequiredProperties,
                    maxRows: null,
                    orderedAscendingProperties: null,
                    rowFilter: buffer =>
                    {
                        if (vbFilter is null || vbFilter(queryContext, buffer))
                        {
                            count++;
                        }

                        return false;
                    },
                    ct).ConfigureAwait(false);
                return count;
            }

            executor = (Func<QueryContext, Task<TScalar>>)(object)(Func<QueryContext, Task<long>>)ExecuteLongCountAsync;
            return true;
        }

        if (terminalCall.Method.Name == nameof(Queryable.Any) && typeof(TScalar) == typeof(bool))
        {
            async Task<bool> ExecuteAnyAsync(QueryContext queryContext)
            {
                CancellationToken ct = queryContext.CancellationToken;
                ct.ThrowIfCancellationRequested();
                var jetContext = (JetQueryContext)queryContext;
                IReadOnlyList<ValueBuffer> buffers = await jetContext.Connection.ReadTableValueBuffersAsync(
                    tablePlan.RootEntityType,
                    tablePlan.SeekConditions,
                    queryContext.Parameters,
                    tablePlan.RequiredProperties,
                    maxRows: 1,
                    orderedAscendingProperties: null,
                    rowFilter: vbFilter is not null ? buffer => vbFilter(queryContext, buffer) : null,
                    ct).ConfigureAwait(false);
                return buffers.Count > 0;
            }

            executor = (Func<QueryContext, Task<TScalar>>)(object)(Func<QueryContext, Task<bool>>)ExecuteAnyAsync;
            return true;
        }

        return false;
    }

    private static Func<QueryContext, JetExecutionMaterializedState, IEnumerable<TElement>> CompileSequenceDelegate<TElement>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        ParameterExpression queryContextParam = Expression.Parameter(typeof(QueryContext), "queryContext");
        ParameterExpression stateParam = Expression.Parameter(typeof(JetExecutionMaterializedState), "state");

        Expression rewritten = new JetCompiledExpressionRewriter(
            queryContextParam,
            stateParam,
            model,
            plan.RootPlansByExpression,
            plan.IgnoreQueryFilters).Visit(query);

        if (typeof(IEnumerable<TElement>).IsAssignableFrom(rewritten.Type))
        {
            Expression body = rewritten.Type == typeof(IEnumerable<TElement>)
                ? rewritten
                : Expression.Convert(rewritten, typeof(IEnumerable<TElement>));
            return Expression.Lambda<Func<QueryContext, JetExecutionMaterializedState, IEnumerable<TElement>>>(
                body,
                queryContextParam,
                stateParam).Compile();
        }

        Expression boxed = Expression.Convert(rewritten, typeof(object));
        Func<QueryContext, JetExecutionMaterializedState, object?> rawCompiled =
            Expression.Lambda<Func<QueryContext, JetExecutionMaterializedState, object?>>(
                boxed,
                queryContextParam,
                stateParam).Compile();
        return (ctx, st) => CoerceSequenceResult<TElement>(rawCompiled(ctx, st));
    }

    private static Func<QueryContext, JetExecutionMaterializedState, TScalar> CompileScalarDelegate<TScalar>(
        Expression query,
        JetQueryPlan plan,
        IModel model)
    {
        ParameterExpression queryContextParam = Expression.Parameter(typeof(QueryContext), "queryContext");
        ParameterExpression stateParam = Expression.Parameter(typeof(JetExecutionMaterializedState), "state");

        Expression rewritten = new JetCompiledExpressionRewriter(
            queryContextParam,
            stateParam,
            model,
            plan.RootPlansByExpression,
            plan.IgnoreQueryFilters).Visit(query);

        Expression body = rewritten.Type == typeof(TScalar)
            ? rewritten
            : Expression.Convert(rewritten, typeof(TScalar));
        return Expression.Lambda<Func<QueryContext, JetExecutionMaterializedState, TScalar>>(
            body,
            queryContextParam,
            stateParam).Compile();
    }

    private static IEnumerable<TElement> CoerceSequenceResult<TElement>(object? raw)
    {
        if (raw is IEnumerable<TElement> typed)
        {
            return typed;
        }

        if (raw is IEnumerable untyped)
        {
            return untyped.Cast<TElement>();
        }

        return [];
    }

    private static async IAsyncEnumerable<TElement> ExecuteSequenceAsync<TElement>(
        QueryContext queryContext,
        JetQueryPlan plan,
        Func<QueryContext, JetExecutionMaterializedState, IEnumerable<TElement>> compiledQuery,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        CancellationToken effectiveToken = cancellationToken != default ? cancellationToken : queryContext.CancellationToken;
        effectiveToken.ThrowIfCancellationRequested();

        QueryTrackingBehavior trackingBehavior = plan.TrackingBehaviorOverride
            ?? queryContext.Context.ChangeTracker.QueryTrackingBehavior;
        bool isTracking = trackingBehavior == QueryTrackingBehavior.TrackAll;
        bool resolveIdentityUntracked = trackingBehavior == QueryTrackingBehavior.NoTrackingWithIdentityResolution;

        if (isTracking)
        {
            queryContext.InitializeStateManager(standAlone: false);
        }

        JetExecutionMaterializedState state = await LoadRootTablesAndQueryNavigationsAsync(
            queryContext,
            plan,
            isTracking,
            resolveIdentityUntracked,
            effectiveToken).ConfigureAwait(false);

        if (plan.IncludeChains.Count == 0)
        {
            Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>>? existingTrackedByType =
                isTracking ? [] : null;
            List<(object Entity, ValueBuffer Buffer)>? unusedIncludeRoots = null;
            HashSet<object>? unusedSeenRoots = null;

            foreach (TElement rawElement in compiledQuery(queryContext, state))
            {
                effectiveToken.ThrowIfCancellationRequested();
                TElement current = rawElement;
                if (isTracking && current is object elementObj)
                {
                    unusedIncludeRoots ??= [];
                    unusedSeenRoots ??= [with(ReferenceEqualityComparer.Instance)];
                    object? promoted = PromoteResultGraph(
                        elementObj,
                        queryContext,
                        plan,
                        state,
                        existingTrackedByType!,
                        isTracking: true,
                        unusedIncludeRoots,
                        unusedSeenRoots);
                    if (promoted is TElement typedPromoted)
                    {
                        current = typedPromoted;
                    }
                }

                yield return current;
            }

            yield break;
        }

        List<TElement> results = [.. compiledQuery(queryContext, state)];

        if (results.Count > 0)
        {
            Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType = [];
            List<(object Entity, ValueBuffer Buffer)> rootRowsForIncludes = [];
            HashSet<object> seenIncludeRoots = [with(ReferenceEqualityComparer.Instance)];
            for (int i = 0; i < results.Count; i++)
            {
                if (results[i] is not object elementObj)
                {
                    continue;
                }

                object? promoted = PromoteResultGraph(
                    elementObj,
                    queryContext,
                    plan,
                    state,
                    existingTrackedByType,
                    isTracking,
                    rootRowsForIncludes,
                    seenIncludeRoots);
                if (promoted is TElement typedPromoted)
                {
                    results[i] = typedPromoted;
                }
            }

            if (plan.IncludeChains.Count > 0 && rootRowsForIncludes.Count > 0)
            {
                await LoadIncludesForResultEntitiesAsync(
                    (JetQueryContext)queryContext,
                    plan,
                    state,
                    rootRowsForIncludes,
                    existingTrackedByType,
                    isTracking,
                    resolveIdentityUntracked,
                    effectiveToken).ConfigureAwait(false);
            }
        }

        foreach (TElement element in results)
        {
            effectiveToken.ThrowIfCancellationRequested();
            yield return element;
        }
    }

    private static async Task<TScalar> ExecuteScalarAsync<TScalar>(
        QueryContext queryContext,
        JetQueryPlan plan,
        Func<QueryContext, JetExecutionMaterializedState, TScalar> compiledQuery)
    {
        CancellationToken effectiveToken = queryContext.CancellationToken;
        effectiveToken.ThrowIfCancellationRequested();

        QueryTrackingBehavior trackingBehavior = plan.TrackingBehaviorOverride
            ?? queryContext.Context.ChangeTracker.QueryTrackingBehavior;
        bool isTracking = trackingBehavior == QueryTrackingBehavior.TrackAll;
        bool resolveIdentityUntracked = trackingBehavior == QueryTrackingBehavior.NoTrackingWithIdentityResolution;

        if (isTracking)
        {
            queryContext.InitializeStateManager(standAlone: false);
        }

        JetExecutionMaterializedState state = await LoadRootTablesAndQueryNavigationsAsync(
            queryContext,
            plan,
            isTracking,
            resolveIdentityUntracked,
            effectiveToken).ConfigureAwait(false);

        TScalar result = compiledQuery(queryContext, state);

        if (result is object resultObj)
        {
            Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType = [];
            List<(object Entity, ValueBuffer Buffer)> rootRowsForIncludes = [];
            HashSet<object> seenIncludeRoots = [with(ReferenceEqualityComparer.Instance)];
            object? promoted = PromoteResultGraph(
                resultObj,
                queryContext,
                plan,
                state,
                existingTrackedByType,
                isTracking,
                rootRowsForIncludes,
                seenIncludeRoots);
            if (promoted is TScalar typedPromoted)
            {
                result = typedPromoted;
            }

            if (plan.IncludeChains.Count > 0 && rootRowsForIncludes.Count > 0)
            {
                await LoadIncludesForResultEntitiesAsync(
                    (JetQueryContext)queryContext,
                    plan,
                    state,
                    rootRowsForIncludes,
                    existingTrackedByType,
                    isTracking,
                    resolveIdentityUntracked,
                    effectiveToken).ConfigureAwait(false);
            }
        }

        return result;
    }

    private static async ValueTask<JetExecutionMaterializedState> LoadRootTablesAndQueryNavigationsAsync(
        QueryContext queryContext,
        JetQueryPlan plan,
        bool isTracking,
        bool resolveIdentityUntracked,
        CancellationToken cancellationToken)
    {
        var jetContext = (JetQueryContext)queryContext;
        JetExecutionMaterializedState state = new();

        Dictionary<IEntityType, IReadOnlyList<ValueBuffer>> unindexedBufferCache = [];
        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps = [];
        bool deduplicateCandidates = isTracking || resolveIdentityUntracked;

        foreach (JetTableLoadPlan tablePlan in plan.RootTables)
        {
            IEntityType rootEntityType = tablePlan.RootEntityType;
            int? maxRows = tablePlan.MaxRowsEvaluator?.Invoke(queryContext.Parameters);
            IReadOnlyList<ValueBuffer> buffers;
            List<(object Entity, ValueBuffer Buffer)>? preMaterializedRows = null;

            if (tablePlan.JoinLoadSpec is { } joinSpec
                && tablePlan.SeekConditions.Count == 0
                && state.RootRows.TryGetValue(joinSpec.OuterPlan.RootId, out List<(object Entity, ValueBuffer Buffer)>? outerRows))
            {
                HashSet<StructuralKey> seenJoinKeys = [];
                List<object?[]> distinctJoinKeyTuples = [];
                foreach ((_, ValueBuffer outerBuf) in outerRows)
                {
                    if (TryExtractNonNullKey(joinSpec.OuterKeyProperties, outerBuf, out StructuralKey joinKey)
                        && seenJoinKeys.Add(joinKey))
                    {
                        distinctJoinKeyTuples.Add(joinKey.Values);
                    }
                }

                buffers = distinctJoinKeyTuples.Count == 0
                    ? []
                    : await jetContext.Connection.ReadTableValueBuffersByKeysAsync(
                        rootEntityType,
                        joinSpec.InnerKeyProperties,
                        distinctJoinKeyTuples,
                        tablePlan.RequiredProperties,
                        cancellationToken).ConfigureAwait(false);
            }
            else
            {
                bool canShareUnindexedCache = tablePlan.SeekConditions.Count == 0
                    && tablePlan.RequiredProperties is null
                    && maxRows is null
                    && tablePlan.OrderedAscendingProperties is null
                    && tablePlan.ValueBufferRowFilter is null
                    && tablePlan.CandidateRowFilter is null;
                if (canShareUnindexedCache && unindexedBufferCache.TryGetValue(rootEntityType, out IReadOnlyList<ValueBuffer>? cachedBuffers))
                {
                    buffers = cachedBuffers;
                }
                else
                {
                    Func<ValueBuffer, bool>? bufferFilter = null;
                    if (tablePlan.CandidateRowFilter is not null)
                    {
                        preMaterializedRows = [];
                        HashSet<object> retainedEntities = [with(ReferenceEqualityComparer.Instance)];
                        bufferFilter = buffer =>
                        {
                            if (tablePlan.ValueBufferRowFilter is not null
                                && !tablePlan.ValueBufferRowFilter(queryContext, buffer))
                            {
                                return false;
                            }

                            object entity = MaterializeSingleCandidateRow(
                                queryContext,
                                rootEntityType,
                                buffer,
                                plan,
                                state,
                                deduplicateCandidates,
                                untrackedIdentityMaps);
                            if (!tablePlan.QueryEntityType.ClrType.IsInstanceOfType(entity)
                                || !tablePlan.CandidateRowFilter(queryContext, state, entity))
                            {
                                if (!retainedEntities.Contains(entity))
                                {
                                    _ = state.EntityBuffers.Remove(entity);
                                    _ = state.EntityTypesByInstance.Remove(entity);
                                }

                                return false;
                            }

                            _ = retainedEntities.Add(entity);
                            preMaterializedRows.Add((entity, buffer));
                            return true;
                        };
                    }
                    else if (tablePlan.ValueBufferRowFilter is not null)
                    {
                        bufferFilter = buffer => tablePlan.ValueBufferRowFilter(queryContext, buffer);
                    }

                    buffers = await jetContext.Connection.ReadTableValueBuffersAsync(
                        rootEntityType,
                        tablePlan.SeekConditions,
                        queryContext.Parameters,
                        tablePlan.RequiredProperties,
                        maxRows,
                        tablePlan.OrderedAscendingProperties,
                        bufferFilter,
                        cancellationToken).ConfigureAwait(false);
                    if (canShareUnindexedCache)
                    {
                        unindexedBufferCache[rootEntityType] = buffers;
                    }
                }
            }

            List<(object Entity, ValueBuffer Buffer)> rows = preMaterializedRows
                ?? MaterializeCandidateRows(
                    queryContext,
                    rootEntityType,
                    buffers,
                    plan,
                    state,
                    deduplicateCandidates,
                    untrackedIdentityMaps,
                    cancellationToken);
            state.RootRows[tablePlan.RootId] = rows;

            if (!state.TablesByEntityType.TryGetValue(rootEntityType, out List<(object Entity, ValueBuffer Buffer)>? merged))
            {
                state.TablesByEntityType[rootEntityType] = [.. rows];
            }
            else
            {
                HashSet<object> seenEntities = [with(ReferenceEqualityComparer.Instance), .. merged.Select(static r => r.Entity)];
                foreach ((object Entity, ValueBuffer Buffer) row in rows)
                {
                    if (seenEntities.Add(row.Entity))
                    {
                        merged.Add(row);
                    }
                }
            }
        }

        foreach (INavigationBase navigationBase in plan.QueryReferencedNavigations)
        {
            IEntityType declaringRoot = navigationBase.DeclaringEntityType.GetRootType();
            IEntityType targetRoot = navigationBase.TargetEntityType.GetRootType();

            if (!state.TablesByEntityType.TryGetValue(declaringRoot, out List<(object Entity, ValueBuffer Buffer)>? declaringRows))
            {
                IReadOnlyList<ValueBuffer> declaringBuffers = await jetContext.Connection.ReadTableValueBuffersAsync(
                    declaringRoot,
                    seekConditions: null,
                    queryContext.Parameters,
                    cancellationToken).ConfigureAwait(false);
                declaringRows = MaterializeCandidateRows(
                    queryContext,
                    declaringRoot,
                    declaringBuffers,
                    plan,
                    state,
                    deduplicateCandidates,
                    untrackedIdentityMaps,
                    cancellationToken);
                state.TablesByEntityType[declaringRoot] = declaringRows;
            }

            if (navigationBase is ISkipNavigation skipNavigation)
            {
                List<(object Entity, ValueBuffer Buffer)> skipTargetRows = await LoadAndFixupSkipNavigationAsync(
                    jetContext,
                    skipNavigation,
                    declaringRows,
                    collectionFilter: null,
                    plan,
                    state,
                    existingTrackedByType: null,
                    initializedCollections: null,
                    untrackedIdentityMaps,
                    isTracking: false,
                    deduplicateCandidates: true,
                    cancellationToken).ConfigureAwait(false);
                MergeIntoTablesByEntityType(state, targetRoot, skipTargetRows);
                continue;
            }

            var navigation = (INavigation)navigationBase;
            IReadOnlyList<ValueBuffer> targetBuffers = await LoadRelatedValueBuffersAsync(
                jetContext,
                navigation,
                declaringRows,
                cancellationToken).ConfigureAwait(false);
            List<(object Entity, ValueBuffer Buffer)> loadedTargetRows = MaterializeCandidateRows(
                queryContext,
                targetRoot,
                targetBuffers,
                plan,
                state,
                deduplicateCandidates: true,
                untrackedIdentityMaps,
                cancellationToken);
            if (!plan.IgnoreQueryFilters)
            {
                loadedTargetRows = ApplyEntityQueryFilters(queryContext, plan, state, targetRoot, loadedTargetRows);
            }

            MergeIntoTablesByEntityType(state, targetRoot, loadedTargetRows);
            List<(object Entity, ValueBuffer Buffer)> targetRows = state.TablesByEntityType[targetRoot];

            FixupNavigation(
                queryContext,
                navigation,
                declaringRows,
                targetRows,
                collectionTransform: null,
                state,
                markNavigationLoaded: false);
        }

        return state;
    }

    private static void MergeIntoTablesByEntityType(
        JetExecutionMaterializedState state,
        IEntityType targetRoot,
        List<(object Entity, ValueBuffer Buffer)> loadedTargetRows)
    {
        if (!state.TablesByEntityType.TryGetValue(targetRoot, out List<(object Entity, ValueBuffer Buffer)>? targetRows))
        {
            state.TablesByEntityType[targetRoot] = [.. loadedTargetRows];
        }
        else if (loadedTargetRows.Count > 0)
        {
            HashSet<object> seenTargetEntities = [with(ReferenceEqualityComparer.Instance), .. targetRows.Select(static r => r.Entity)];
            foreach ((object Entity, ValueBuffer Buffer) row in loadedTargetRows)
            {
                if (seenTargetEntities.Add(row.Entity))
                {
                    targetRows.Add(row);
                }
            }
        }
    }

    private static async ValueTask LoadIncludesForResultEntitiesAsync(
        JetQueryContext queryContext,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        List<(object Entity, ValueBuffer Buffer)> resultRows,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType,
        bool isTracking,
        bool resolveIdentityUntracked,
        CancellationToken cancellationToken)
    {
        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps = [];
        HashSet<(object Entity, INavigationBase Navigation)> initializedCollections = [with(EntityNavigationPairComparer.Instance)];

        foreach (JetIncludeChain chain in plan.IncludeChains)
        {
            if (chain.Steps.Count == 0)
            {
                continue;
            }

            Type firstDeclaringClrType = chain.Steps[0].Navigation.DeclaringEntityType.ClrType;
            List<(object Entity, ValueBuffer Buffer)> currentDeclaringRows =
                [.. resultRows.Where(r => firstDeclaringClrType.IsInstanceOfType(r.Entity))];
            foreach (JetIncludeStep step in chain.Steps)
            {
                if (currentDeclaringRows.Count == 0)
                {
                    break;
                }

                if (step.Navigation is ISkipNavigation skipNav)
                {
                    currentDeclaringRows = await LoadAndFixupSkipNavigationAsync(
                        queryContext,
                        skipNav,
                        currentDeclaringRows,
                        step.CollectionFilter,
                        plan,
                        state,
                        existingTrackedByType,
                        initializedCollections,
                        untrackedIdentityMaps,
                        isTracking,
                        deduplicateCandidates: isTracking || resolveIdentityUntracked,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var navigation = (INavigation)step.Navigation;
                IEntityType targetRoot = navigation.TargetEntityType.GetRootType();

                IReadOnlyList<ValueBuffer> targetBuffers = await LoadRelatedValueBuffersAsync(
                    queryContext,
                    navigation,
                    currentDeclaringRows,
                    cancellationToken).ConfigureAwait(false);

                List<(object Entity, ValueBuffer Buffer)> candidateTargetRows = MaterializeCandidateRows(
                    queryContext,
                    targetRoot,
                    targetBuffers,
                    plan,
                    state,
                    deduplicateCandidates: isTracking || resolveIdentityUntracked,
                    untrackedIdentityMaps,
                    cancellationToken);

                if (!plan.IgnoreQueryFilters)
                {
                    candidateTargetRows = ApplyEntityQueryFilters(queryContext, plan, state, targetRoot, candidateTargetRows);
                }

                currentDeclaringRows = FixupNavigationAndPromoteMatchedTargets(
                    queryContext,
                    navigation,
                    currentDeclaringRows,
                    candidateTargetRows,
                    step.CollectionFilter,
                    plan,
                    state,
                    existingTrackedByType,
                    initializedCollections,
                    isTracking);
            }
        }
    }

    private static async ValueTask<List<(object Entity, ValueBuffer Buffer)>> LoadAndFixupSkipNavigationAsync(
        JetQueryContext queryContext,
        ISkipNavigation skipNavigation,
        List<(object Entity, ValueBuffer Buffer)> declaringRows,
        Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>? collectionFilter,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>>? existingTrackedByType,
        HashSet<(object Entity, INavigationBase Navigation)>? initializedCollections,
        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps,
        bool isTracking,
        bool deduplicateCandidates,
        CancellationToken cancellationToken)
    {
        if (declaringRows.Count == 0 || skipNavigation.Inverse is not ISkipNavigation inverseSkip)
        {
            return [];
        }

        IClrCollectionAccessor? collectionAccessor = skipNavigation.GetCollectionAccessor();
        foreach ((object declaringEntity, _) in declaringRows)
        {
            object? collectionInstance = collectionAccessor?.GetOrCreate(declaringEntity, forMaterialization: true);
            if (initializedCollections?.Add((declaringEntity, skipNavigation)) == true)
            {
                ClearCollectionInstance(collectionInstance);
            }

            if (isTracking)
            {
                queryContext.SetNavigationIsLoaded(declaringEntity, skipNavigation);
            }
        }

        IEntityType joinEntityType = skipNavigation.JoinEntityType;
        IForeignKey sourceFk = skipNavigation.ForeignKey;
        IForeignKey targetFk = inverseSkip.ForeignKey;
        IReadOnlyList<IProperty> declaringKeyProps = sourceFk.PrincipalKey.Properties;
        IReadOnlyList<IProperty> joinSourceFkProps = sourceFk.Properties;
        IReadOnlyList<IProperty> joinTargetFkProps = targetFk.Properties;
        IReadOnlyList<IProperty> targetKeyProps = targetFk.PrincipalKey.Properties;

        if (declaringKeyProps.Count == 0
            || declaringKeyProps.Count != joinSourceFkProps.Count
            || targetKeyProps.Count == 0
            || targetKeyProps.Count != joinTargetFkProps.Count)
        {
            return [];
        }

        Dictionary<StructuralKey, List<object>> declaringEntitiesByKey = [];
        List<object?[]> distinctSourceKeyTuples = [];
        foreach ((object declaringEntity, ValueBuffer declaringBuffer) in declaringRows)
        {
            if (!TryExtractNonNullKey(declaringKeyProps, declaringBuffer, out StructuralKey srcKey))
            {
                continue;
            }

            if (!declaringEntitiesByKey.TryGetValue(srcKey, out List<object>? entitiesForKey))
            {
                entitiesForKey = [];
                declaringEntitiesByKey[srcKey] = entitiesForKey;
                distinctSourceKeyTuples.Add(srcKey.Values);
            }

            entitiesForKey.Add(declaringEntity);
        }

        if (distinctSourceKeyTuples.Count == 0)
        {
            return [];
        }

        IReadOnlyList<ValueBuffer> joinBuffers = await queryContext.Connection.ReadTableValueBuffersByKeysAsync(
            joinEntityType,
            joinSourceFkProps,
            distinctSourceKeyTuples,
            requiredProperties: null,
            cancellationToken).ConfigureAwait(false);
        if (joinBuffers.Count == 0)
        {
            return [];
        }

        List<(object Entity, ValueBuffer Buffer)> joinRows = MaterializeCandidateRows(
            queryContext,
            joinEntityType,
            joinBuffers,
            plan,
            state,
            deduplicateCandidates,
            untrackedIdentityMaps,
            cancellationToken);

        HashSet<StructuralKey> seenTargetKeys = [];
        List<object?[]> distinctTargetKeyTuples = [];
        List<(StructuralKey SourceKey, StructuralKey TargetKey, object JoinEntity, ValueBuffer JoinBuffer)> joinLinks = [];

        foreach ((object joinEntity, ValueBuffer joinBuffer) in joinRows)
        {
            if (!TryExtractNonNullKey(joinSourceFkProps, joinBuffer, out StructuralKey srcKey)
                || !TryExtractNonNullKey(joinTargetFkProps, joinBuffer, out StructuralKey tgtKey))
            {
                continue;
            }

            joinLinks.Add((srcKey, tgtKey, joinEntity, joinBuffer));
            if (seenTargetKeys.Add(tgtKey))
            {
                distinctTargetKeyTuples.Add(tgtKey.Values);
            }
        }

        if (distinctTargetKeyTuples.Count == 0)
        {
            return [];
        }

        IEntityType targetRoot = skipNavigation.TargetEntityType.GetRootType();
        IReadOnlyList<ValueBuffer> targetBuffers = await queryContext.Connection.ReadTableValueBuffersByKeysAsync(
            targetRoot,
            targetKeyProps,
            distinctTargetKeyTuples,
            requiredProperties: null,
            cancellationToken).ConfigureAwait(false);

        List<(object Entity, ValueBuffer Buffer)> candidateTargetRows = MaterializeCandidateRows(
            queryContext,
            targetRoot,
            targetBuffers,
            plan,
            state,
            deduplicateCandidates,
            untrackedIdentityMaps,
            cancellationToken);
        if (!plan.IgnoreQueryFilters)
        {
            candidateTargetRows = ApplyEntityQueryFilters(queryContext, plan, state, targetRoot, candidateTargetRows);
        }

        Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)> targetByKey = [];
        foreach ((object targetEntity, ValueBuffer targetBuffer) in candidateTargetRows)
        {
            if (TryExtractNonNullKey(targetKeyProps, targetBuffer, out StructuralKey tgtKey))
            {
                targetByKey[tgtKey] = (targetEntity, targetBuffer);
            }
        }

        Dictionary<object, List<(object TargetEntity, ValueBuffer TargetBuffer, object JoinEntity, ValueBuffer JoinBuffer)>> matchedByDeclaring =
            [with(ReferenceEqualityComparer.Instance)];
        foreach ((StructuralKey srcKey, StructuralKey tgtKey, object joinEntity, ValueBuffer joinBuffer) in joinLinks)
        {
            if (!declaringEntitiesByKey.TryGetValue(srcKey, out List<object>? declaringList)
                || !targetByKey.TryGetValue(tgtKey, out (object Entity, ValueBuffer Buffer) targetEntry))
            {
                continue;
            }

            foreach (object declaringEntity in declaringList)
            {
                if (!matchedByDeclaring.TryGetValue(
                    declaringEntity,
                    out List<(object TargetEntity, ValueBuffer TargetBuffer, object JoinEntity, ValueBuffer JoinBuffer)>? list))
                {
                    list = [];
                    matchedByDeclaring[declaringEntity] = list;
                }

                list.Add((targetEntry.Entity, targetEntry.Buffer, joinEntity, joinBuffer));
            }
        }

        IClrCollectionAccessor? inverseCollection = inverseSkip.IsCollection ? inverseSkip.GetCollectionAccessor() : null;
        List<(object Entity, ValueBuffer Buffer)> promotedTargets = [];
        HashSet<object> promotedSet = [with(ReferenceEqualityComparer.Instance)];

        foreach (KeyValuePair<object, List<(object TargetEntity, ValueBuffer TargetBuffer, object JoinEntity, ValueBuffer JoinBuffer)>> kvp in matchedByDeclaring)
        {
            object declaringEntity = kvp.Key;
            List<(object TargetEntity, ValueBuffer TargetBuffer, object JoinEntity, ValueBuffer JoinBuffer)> entries = kvp.Value;

            if (collectionFilter is not null)
            {
                Dictionary<object, (ValueBuffer TargetBuffer, object JoinEntity, ValueBuffer JoinBuffer)> metaByTarget =
                    [with(ReferenceEqualityComparer.Instance)];
                List<object> rawTargets = [];
                foreach ((object tObj, ValueBuffer tBuf, object jObj, ValueBuffer jBuf) in entries)
                {
                    metaByTarget[tObj] = (tBuf, jObj, jBuf);
                    rawTargets.Add(tObj);
                }

                List<(object TargetEntity, ValueBuffer TargetBuffer, object JoinEntity, ValueBuffer JoinBuffer)> filtered = [];
                foreach (object surviving in collectionFilter(queryContext, state, rawTargets))
                {
                    if (metaByTarget.TryGetValue(surviving, out (ValueBuffer TargetBuffer, object JoinEntity, ValueBuffer JoinBuffer) meta))
                    {
                        filtered.Add((surviving, meta.TargetBuffer, meta.JoinEntity, meta.JoinBuffer));
                    }
                }

                entries = filtered;
            }

            foreach ((object rawTarget, ValueBuffer targetBuffer, object rawJoin, ValueBuffer joinBuffer) in entries)
            {
                if (isTracking && existingTrackedByType is not null)
                {
                    _ = PromoteEntityInstance(rawJoin, joinBuffer, queryContext, plan, state, existingTrackedByType);
                }
                else
                {
                    if (!state.SkipJoinEntitiesByTarget.TryGetValue(rawTarget, out List<(object JoinEntity, ValueBuffer JoinBuffer)>? joinList))
                    {
                        joinList = [];
                        state.SkipJoinEntitiesByTarget[rawTarget] = joinList;
                    }

                    joinList.Add((rawJoin, joinBuffer));
                }

                object targetEntity = isTracking && existingTrackedByType is not null
                    ? PromoteEntityInstance(rawTarget, targetBuffer, queryContext, plan, state, existingTrackedByType)
                    : rawTarget;
                if (promotedSet.Add(targetEntity))
                {
                    promotedTargets.Add((targetEntity, targetBuffer));
                }

                if (collectionAccessor?.Contains(declaringEntity, targetEntity) == false)
                {
                    collectionAccessor.Add(declaringEntity, targetEntity, forMaterialization: true);
                }

                if (inverseCollection?.Contains(targetEntity, declaringEntity) == false)
                {
                    inverseCollection.Add(targetEntity, declaringEntity, forMaterialization: true);
                }
            }
        }

        return promotedTargets;
    }

    private static async ValueTask<IReadOnlyList<ValueBuffer>> LoadRelatedValueBuffersAsync(
        JetQueryContext queryContext,
        INavigation navigation,
        List<(object Entity, ValueBuffer Buffer)> declaringRows,
        CancellationToken cancellationToken)
    {
        if (declaringRows.Count == 0)
        {
            return [];
        }

        IEntityType targetRoot = navigation.TargetEntityType.GetRootType();
        IForeignKey foreignKey = navigation.ForeignKey;
        IReadOnlyList<IProperty> sourceKeyProps = navigation.IsOnDependent
            ? foreignKey.Properties
            : foreignKey.PrincipalKey.Properties;
        IReadOnlyList<IProperty> targetKeyProps = navigation.IsOnDependent
            ? foreignKey.PrincipalKey.Properties
            : foreignKey.Properties;

        if (sourceKeyProps.Count == 0 || sourceKeyProps.Count != targetKeyProps.Count)
        {
            return [];
        }

        HashSet<StructuralKey> seenKeys = [];
        List<object?[]> distinctKeyTuples = [];
        foreach ((_, ValueBuffer buffer) in declaringRows)
        {
            if (TryExtractNonNullKey(sourceKeyProps, buffer, out StructuralKey key) && seenKeys.Add(key))
            {
                distinctKeyTuples.Add(key.Values);
            }
        }

        if (distinctKeyTuples.Count == 0)
        {
            return [];
        }

        return await queryContext.Connection.ReadTableValueBuffersByKeysAsync(
            targetRoot,
            targetKeyProps,
            distinctKeyTuples,
            requiredProperties: null,
            cancellationToken).ConfigureAwait(false);
    }

    private static object? PromoteResultGraph(
        object? value,
        QueryContext queryContext,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType,
        bool isTracking,
        List<(object Entity, ValueBuffer Buffer)> rootRowsForIncludes,
        HashSet<object> seenIncludeRoots)
    {
        if (value is null)
        {
            return null;
        }

        if (state.EntityBuffers.TryGetValue(value, out ValueBuffer buffer))
        {
            object effectiveEntity = isTracking
                ? PromoteEntityInstance(value, buffer, queryContext, plan, state, existingTrackedByType)
                : value;
            if (seenIncludeRoots.Add(effectiveEntity))
            {
                rootRowsForIncludes.Add((effectiveEntity, buffer));
            }

            return effectiveEntity;
        }

        if (!isTracking && plan.IncludeChains.Count == 0)
        {
            return value;
        }

        Type type = value.GetType();
        if (type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal)
            || type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateOnly)
            || type == typeof(TimeOnly) || type == typeof(TimeSpan) || type == typeof(Guid))
        {
            return value;
        }

        if (value is IList list && !list.IsReadOnly)
        {
            for (int i = 0; i < list.Count; i++)
            {
                object? origElem = list[i];
                if (origElem is not null)
                {
                    object? promotedElem = PromoteResultGraph(
                        origElem,
                        queryContext,
                        plan,
                        state,
                        existingTrackedByType,
                        isTracking,
                        rootRowsForIncludes,
                        seenIncludeRoots);
                    if (!ReferenceEquals(origElem, promotedElem))
                    {
                        list[i] = promotedElem;
                    }
                }
            }

            return value;
        }

        if (value is IEnumerable)
        {
            return value;
        }

        if (type.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
            && type.Name.Contains("AnonymousType", StringComparison.Ordinal))
        {
            ConstructorInfo[] ctors = type.GetConstructors();
            PropertyInfo[] props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (ctors.Length == 1 && ctors[0].GetParameters().Length == props.Length)
            {
                object?[] args = new object?[props.Length];
                bool anyChanged = false;
                for (int i = 0; i < props.Length; i++)
                {
                    object? origVal = props[i].GetValue(value);
                    object? promotedVal = PromoteResultGraph(
                        origVal,
                        queryContext,
                        plan,
                        state,
                        existingTrackedByType,
                        isTracking,
                        rootRowsForIncludes,
                        seenIncludeRoots);
                    args[i] = promotedVal;
                    if (!ReferenceEquals(origVal, promotedVal))
                    {
                        anyChanged = true;
                    }
                }

                return anyChanged ? ctors[0].Invoke(args) : value;
            }
        }

        if (type.IsValueType && type.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true)
        {
            FieldInfo[] fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
            ConstructorInfo? tupleCtor = type.GetConstructors().FirstOrDefault(c => c.GetParameters().Length == fields.Length);
            if (tupleCtor is not null)
            {
                object?[] args = new object?[fields.Length];
                bool anyChanged = false;
                for (int i = 0; i < fields.Length; i++)
                {
                    object? origVal = fields[i].GetValue(value);
                    object? promotedVal = PromoteResultGraph(
                        origVal,
                        queryContext,
                        plan,
                        state,
                        existingTrackedByType,
                        isTracking,
                        rootRowsForIncludes,
                        seenIncludeRoots);
                    args[i] = promotedVal;
                    if (!ReferenceEquals(origVal, promotedVal))
                    {
                        anyChanged = true;
                    }
                }

                return anyChanged ? tupleCtor.Invoke(args) : value;
            }
        }

        if (!type.IsValueType)
        {
            foreach (PropertyInfo prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!prop.CanRead || prop.GetIndexParameters().Length != 0)
                {
                    continue;
                }

                object? origVal = prop.GetValue(value);
                if (origVal is null)
                {
                    continue;
                }

                object? promotedProp = PromoteResultGraph(
                    origVal,
                    queryContext,
                    plan,
                    state,
                    existingTrackedByType,
                    isTracking,
                    rootRowsForIncludes,
                    seenIncludeRoots);
                if (prop.CanWrite && !ReferenceEquals(origVal, promotedProp))
                {
                    prop.SetValue(value, promotedProp);
                }
            }
        }

        return value;
    }

    private static object PromoteEntityInstance(
        object candidateEntity,
        ValueBuffer buffer,
        QueryContext queryContext,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType)
    {
        if (state.AlreadyTrackedEntities.Contains(candidateEntity))
        {
            return candidateEntity;
        }

        if (state.PromotedEntities.TryGetValue(candidateEntity, out object? alreadyPromoted))
        {
            return alreadyPromoted;
        }

        if (!state.EntityTypesByInstance.TryGetValue(candidateEntity, out IEntityType? concreteType))
        {
            concreteType = queryContext.Context.Model.FindEntityType(candidateEntity.GetType());
        }

        if (concreteType is null)
        {
            return candidateEntity;
        }

        IEntityType rootType = concreteType.GetRootType();
        IKey? primaryKey = rootType.FindPrimaryKey();
        StructuralKey? extractedKey = null;
        if (primaryKey is not null)
        {
            StructuralKey key = ExtractKey(primaryKey, buffer);
            extractedKey = key;

            if (existingTrackedByType.TryGetValue(rootType, out Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>? trackedMap)
                && trackedMap.TryGetValue(key, out (object Entity, ValueBuffer Buffer) trackedEntry))
            {
                state.PromotedEntities[candidateEntity] = trackedEntry.Entity;
                state.EntityBuffers[trackedEntry.Entity] = buffer;
                state.EntityTypesByInstance[trackedEntry.Entity] = concreteType;
                _ = state.AlreadyTrackedEntities.Add(trackedEntry.Entity);
                TransferPopulatedNavigations(
                    concreteType,
                    candidateEntity,
                    trackedEntry.Entity,
                    queryContext,
                    plan,
                    state,
                    existingTrackedByType);
                return trackedEntry.Entity;
            }

            if (!concreteType.IsOwned())
            {
#pragma warning disable EF1001 // QueryContext.TryGetEntry provides O(1) StateManager lookup by key.
                InternalEntityEntry? existingStateEntry = queryContext.TryGetEntry(
                    primaryKey,
                    key.Values!,
                    throwOnNullKey: false,
                    out bool hasNullKey);
                if (!hasNullKey
                    && existingStateEntry is { EntityState: not EntityState.Detached, Entity: object stateManagerEntity })
#pragma warning restore EF1001
                {
                    if (!existingTrackedByType.TryGetValue(rootType, out Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>? map))
                    {
                        map = [];
                        existingTrackedByType[rootType] = map;
                    }

                    map[key] = (stateManagerEntity, buffer);
                    state.PromotedEntities[candidateEntity] = stateManagerEntity;
                    state.EntityBuffers[stateManagerEntity] = buffer;
                    state.EntityTypesByInstance[stateManagerEntity] = concreteType;
                    _ = state.AlreadyTrackedEntities.Add(stateManagerEntity);
                    TransferPopulatedNavigations(
                        concreteType,
                        candidateEntity,
                        stateManagerEntity,
                        queryContext,
                        plan,
                        state,
                        existingTrackedByType);
                    return stateManagerEntity;
                }
            }
        }

#pragma warning disable EF1001 // QueryContext.StartTracking attaches the already-materialized entity instance in-place without allocating a second CLR object.
        object tracked;
        if (!concreteType.IsOwned() && !HasComplexShadowProperties(concreteType))
        {
            foreach (INavigation nav in concreteType.GetNavigations())
            {
                if (!nav.IsCollection
                    && nav.TargetEntityType.IsOwned()
                    && GetNavigationValue(nav, candidateEntity) is object defaultOwned
                    && !state.EntityBuffers.ContainsKey(defaultOwned))
                {
                    SetNavigationValue(nav, candidateEntity, null);
                }
            }

            var runtimeType = (IRuntimeTypeBase)concreteType;
            ISnapshot shadowSnapshot;
            if (runtimeType.ShadowPropertyCount == 0)
            {
                shadowSnapshot = Snapshot.Empty;
            }
            else
            {
                Dictionary<string, object?> shadowValues = [with(StringComparer.Ordinal)];
                foreach (IProperty prop in concreteType.GetProperties())
                {
                    if (prop.IsShadowProperty())
                    {
                        int idx = prop.GetIndex();
                        object? val = (uint)idx < (uint)buffer.Count ? buffer[idx] : null;
                        if (val is not null)
                        {
                            shadowValues[prop.Name] = val;
                        }
                    }
                }

                shadowSnapshot = runtimeType.ShadowValuesFactory(shadowValues);
            }

            _ = queryContext.StartTracking(concreteType, candidateEntity, in shadowSnapshot);
            tracked = candidateEntity;
        }
        else
        {
            tracked = plan.GetTrackedMaterializer(queryContext, concreteType)(queryContext, buffer);
        }
#pragma warning restore EF1001
        state.PromotedEntities[candidateEntity] = tracked;
        state.EntityBuffers[tracked] = buffer;
        state.EntityTypesByInstance[tracked] = concreteType;
        _ = state.AlreadyTrackedEntities.Add(tracked);
        if (primaryKey is not null && extractedKey is StructuralKey resolvedKey)
        {
            if (!existingTrackedByType.TryGetValue(rootType, out Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>? map))
            {
                map = [];
                existingTrackedByType[rootType] = map;
            }

            map[resolvedKey] = (tracked, buffer);
        }

        TransferPopulatedNavigations(
            concreteType,
            candidateEntity,
            tracked,
            queryContext,
            plan,
            state,
            existingTrackedByType);
        return tracked;
    }

    private static bool HasComplexShadowProperties(IEntityType entityType)
    {
        foreach (IComplexProperty complexProperty in entityType.GetComplexProperties())
        {
            foreach (IProperty prop in complexProperty.ComplexType.GetFlattenedProperties())
            {
                if (prop.IsShadowProperty())
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static void TransferPopulatedNavigations(
        IEntityType concreteType,
        object sourceEntity,
        object targetEntity,
        QueryContext queryContext,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType)
    {
        if (plan.QueryReferencedNavigations.Count == 0)
        {
            return;
        }

        bool sameInstance = ReferenceEquals(sourceEntity, targetEntity);

        if (state.SkipJoinEntitiesByTarget.TryGetValue(sourceEntity, out List<(object JoinEntity, ValueBuffer JoinBuffer)>? skipJoins))
        {
            foreach ((object rawJoin, ValueBuffer joinBuffer) in skipJoins)
            {
                _ = PromoteEntityInstance(rawJoin, joinBuffer, queryContext, plan, state, existingTrackedByType);
            }
        }

        foreach (ISkipNavigation skipNav in concreteType.GetSkipNavigations())
        {
            if (!skipNav.IsCollection)
            {
                continue;
            }

            object? sourceCollection = GetNavigationValue(skipNav, sourceEntity);
            if (sourceCollection is not IEnumerable enumerable)
            {
                continue;
            }

            List<object> items = [.. enumerable.Cast<object>()];
            if (items.Count == 0)
            {
                continue;
            }

            IClrCollectionAccessor? collectionAccessor = skipNav.GetCollectionAccessor();
            _ = collectionAccessor?.GetOrCreate(targetEntity, forMaterialization: true);
            IClrCollectionAccessor? inverseAccessor = skipNav.Inverse is { IsCollection: true } invSkip
                ? invSkip.GetCollectionAccessor()
                : null;
            foreach (object item in items)
            {
                object promotedItem = state.EntityBuffers.TryGetValue(item, out ValueBuffer itemBuf)
                    ? PromoteEntityInstance(item, itemBuf, queryContext, plan, state, existingTrackedByType)
                    : item;
                if (sameInstance && !ReferenceEquals(item, promotedItem))
                {
                    _ = collectionAccessor?.Remove(targetEntity, item);
                }

                if (collectionAccessor?.Contains(targetEntity, promotedItem) == false)
                {
                    collectionAccessor.Add(targetEntity, promotedItem, forMaterialization: true);
                }

                if (inverseAccessor is not null)
                {
                    if (!sameInstance)
                    {
                        _ = inverseAccessor.Remove(promotedItem, sourceEntity);
                    }

                    if (!inverseAccessor.Contains(promotedItem, targetEntity))
                    {
                        inverseAccessor.Add(promotedItem, targetEntity, forMaterialization: true);
                    }
                }
            }
        }

        foreach (INavigation nav in concreteType.GetNavigations())
        {
            if (nav.IsCollection)
            {
                object? sourceCollection = GetNavigationValue(nav, sourceEntity);
                if (sourceCollection is not IEnumerable enumerable)
                {
                    continue;
                }

                List<object> items = [.. enumerable.Cast<object>()];
                if (items.Count == 0)
                {
                    continue;
                }

                IClrCollectionAccessor? collectionAccessor = nav.GetCollectionAccessor();
                _ = collectionAccessor?.GetOrCreate(targetEntity, forMaterialization: true);
                INavigation? inverse = nav.Inverse;
                foreach (object item in items)
                {
                    object promotedItem = state.EntityBuffers.TryGetValue(item, out ValueBuffer itemBuf)
                        ? PromoteEntityInstance(item, itemBuf, queryContext, plan, state, existingTrackedByType)
                        : item;
                    if (sameInstance && !ReferenceEquals(item, promotedItem))
                    {
                        _ = collectionAccessor?.Remove(targetEntity, item);
                    }

                    if (collectionAccessor?.Contains(targetEntity, promotedItem) == false)
                    {
                        collectionAccessor.Add(targetEntity, promotedItem, forMaterialization: true);
                    }

                    if (inverse is { IsCollection: false })
                    {
                        SetNavigationValue(inverse, promotedItem, targetEntity);
                    }
                }
            }
            else
            {
                object? related = GetNavigationValue(nav, sourceEntity);
                if (related is null)
                {
                    continue;
                }

                object promotedRelated = state.EntityBuffers.TryGetValue(related, out ValueBuffer relBuf)
                    ? PromoteEntityInstance(related, relBuf, queryContext, plan, state, existingTrackedByType)
                    : related;
                if (GetNavigationValue(nav, targetEntity) is null || !ReferenceEquals(related, promotedRelated))
                {
                    SetNavigationValue(nav, targetEntity, promotedRelated);
                }

                INavigation? inverse = nav.Inverse;
                if (inverse is { IsCollection: true })
                {
                    IClrCollectionAccessor? invCol = inverse.GetCollectionAccessor();
                    if (invCol is not null)
                    {
                        if (!sameInstance)
                        {
                            _ = invCol.Remove(promotedRelated, sourceEntity);
                        }

                        if (!invCol.Contains(promotedRelated, targetEntity))
                        {
                            invCol.Add(promotedRelated, targetEntity, forMaterialization: true);
                        }
                    }
                }
                else if (inverse is not null && ReferenceEquals(GetNavigationValue(inverse, promotedRelated), sourceEntity))
                {
                    SetNavigationValue(inverse, promotedRelated, targetEntity);
                }
            }
        }
    }

    private static object MaterializeSingleCandidateRow(
        QueryContext queryContext,
        IEntityType rootEntityType,
        ValueBuffer buffer,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        bool deduplicateCandidates,
        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps)
    {
        Func<QueryContext, ValueBuffer, object> untrackedMaterializer = plan.GetUntrackedMaterializer(queryContext, rootEntityType);
        IKey? primaryKey = rootEntityType.FindPrimaryKey();

        Dictionary<StructuralKey, object>? untrackedMap = null;
        if (deduplicateCandidates
            && primaryKey is not null
            && !untrackedIdentityMaps.TryGetValue(rootEntityType, out untrackedMap))
        {
            untrackedMap = [];
            untrackedIdentityMaps[rootEntityType] = untrackedMap;
        }

        object entity;
        if (primaryKey is not null && untrackedMap is not null)
        {
            StructuralKey key = ExtractKey(primaryKey, buffer);
            if (!untrackedMap.TryGetValue(key, out object? existing))
            {
                existing = untrackedMaterializer(queryContext, buffer);
                untrackedMap[key] = existing;
            }

            entity = existing;
        }
        else
        {
            entity = untrackedMaterializer(queryContext, buffer);
        }

        state.EntityBuffers[entity] = buffer;
        state.EntityTypesByInstance[entity] = ResolveConcreteEntityType(rootEntityType, entity);
        return entity;
    }

    private static IEntityType ResolveConcreteEntityType(IEntityType rootEntityType, object entity)
    {
        Type clrType = entity.GetType();
        if (rootEntityType.ClrType == clrType)
        {
            return rootEntityType;
        }

        foreach (IEntityType derived in rootEntityType.GetDerivedTypes())
        {
            if (derived.ClrType == clrType)
            {
                return derived;
            }
        }

        return rootEntityType;
    }

    private static List<(object Entity, ValueBuffer Buffer)> MaterializeCandidateRows(
        QueryContext queryContext,
        IEntityType rootEntityType,
        IReadOnlyList<ValueBuffer> buffers,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        bool deduplicateCandidates,
        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps,
        CancellationToken cancellationToken)
    {
        List<(object Entity, ValueBuffer Buffer)> loadedRows = [];
        loadedRows.Capacity = buffers.Count;
        foreach (ValueBuffer buffer in buffers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object entity = MaterializeSingleCandidateRow(
                queryContext,
                rootEntityType,
                buffer,
                plan,
                state,
                deduplicateCandidates,
                untrackedIdentityMaps);
            loadedRows.Add((entity, buffer));
        }

        return loadedRows;
    }

    private static List<IQueryFilter> GetAllQueryFilters(IEntityType entityType)
    {
        List<IQueryFilter> filters = [];
        foreach (IEntityType current in entityType.GetAllBaseTypesInclusive())
        {
            filters.AddRange(current.GetDeclaredQueryFilters());
        }

        return filters;
    }

    private static List<(object Entity, ValueBuffer Buffer)> ApplyEntityQueryFilters(
        QueryContext queryContext,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        IEntityType entityType,
        List<(object Entity, ValueBuffer Buffer)> rows)
    {
        if (rows.Count == 0)
        {
            return rows;
        }

        List<IQueryFilter> declaredFilters = GetAllQueryFilters(entityType);
        if (declaredFilters.Count == 0)
        {
            return rows;
        }

        List<Func<QueryContext, JetExecutionMaterializedState, object, bool>> predicates = [];
        predicates.Capacity = declaredFilters.Count;
        foreach (IQueryFilter filter in declaredFilters)
        {
            Func<QueryContext, JetExecutionMaterializedState, object, bool>? compiled =
                plan.GetCompiledQueryFilter(queryContext.Context.Model, filter);
            if (compiled is not null)
            {
                predicates.Add(compiled);
            }
        }

        if (predicates.Count == 0)
        {
            return rows;
        }

        List<(object Entity, ValueBuffer Buffer)> filtered = [];
        foreach ((object Entity, ValueBuffer Buffer) row in rows)
        {
            bool keep = true;
            for (int i = 0; i < predicates.Count; i++)
            {
                if (!predicates[i](queryContext, state, row.Entity))
                {
                    keep = false;
                    break;
                }
            }

            if (keep)
            {
                filtered.Add(row);
            }
        }

        return filtered;
    }

    private static List<(object Entity, ValueBuffer Buffer)> FixupNavigationAndPromoteMatchedTargets(
        QueryContext queryContext,
        INavigation navigation,
        List<(object Entity, ValueBuffer Buffer)> declaringRows,
        List<(object Entity, ValueBuffer Buffer)> candidateTargetRows,
        Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>? collectionFilter,
        JetQueryPlan? plan,
        JetExecutionMaterializedState state,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>>? existingTrackedByType,
        HashSet<(object Entity, INavigationBase Navigation)>? initializedCollections,
        bool isTracking)
    {
        object PromoteTargetIfNeeded(object targetEntity, ValueBuffer targetBuffer)
        {
            if (!isTracking || plan is null || existingTrackedByType is null)
            {
                return targetEntity;
            }

            return PromoteEntityInstance(targetEntity, targetBuffer, queryContext, plan, state, existingTrackedByType);
        }

        if (navigation.IsCollection)
        {
            IClrCollectionAccessor? collectionAccessor = navigation.GetCollectionAccessor();
            foreach ((object declaringEntity, _) in declaringRows)
            {
                object? collectionInstance = collectionAccessor?.GetOrCreate(declaringEntity, forMaterialization: true);
                if (initializedCollections?.Add((declaringEntity, navigation)) == true)
                {
                    ClearCollectionInstance(collectionInstance);
                }

                if (isTracking)
                {
                    queryContext.SetNavigationIsLoaded(declaringEntity, navigation);
                }
            }
        }
        else if (isTracking)
        {
            foreach ((object declaringEntity, _) in declaringRows)
            {
                queryContext.SetNavigationIsLoaded(declaringEntity, navigation);
            }
        }

        IForeignKey foreignKey = navigation.ForeignKey;
        IReadOnlyList<IProperty> fkProperties = foreignKey.Properties;
        IReadOnlyList<IProperty> pkProperties = foreignKey.PrincipalKey.Properties;
        if (fkProperties.Count != pkProperties.Count || fkProperties.Count == 0)
        {
            return [];
        }

        List<(object Entity, ValueBuffer Buffer)> promotedTargets = [];
        HashSet<object> promotedSet = [with(ReferenceEqualityComparer.Instance)];

        if (navigation.IsOnDependent)
        {
            Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)> principalByKey = [];
            foreach ((object targetEntity, ValueBuffer targetBuffer) in candidateTargetRows)
            {
                if (TryExtractNonNullKey(pkProperties, targetBuffer, out StructuralKey pk))
                {
                    principalByKey[pk] = (targetEntity, targetBuffer);
                }
            }

            INavigation? inverse = navigation.Inverse;
            IClrCollectionAccessor? inverseCollection = inverse is { IsCollection: true } ? inverse.GetCollectionAccessor() : null;

            foreach ((object dependentEntity, ValueBuffer dependentBuffer) in declaringRows)
            {
                if (!TryExtractNonNullKey(fkProperties, dependentBuffer, out StructuralKey fk)
                    || !principalByKey.TryGetValue(fk, out (object Entity, ValueBuffer Buffer) principalEntry))
                {
                    continue;
                }

                object principalEntity = PromoteTargetIfNeeded(principalEntry.Entity, principalEntry.Buffer);
                principalByKey[fk] = (principalEntity, principalEntry.Buffer);
                if (promotedSet.Add(principalEntity))
                {
                    promotedTargets.Add((principalEntity, principalEntry.Buffer));
                }

                SetNavigationValue(navigation, dependentEntity, principalEntity);
                if (inverseCollection is not null)
                {
                    if (!inverseCollection.Contains(principalEntity, dependentEntity))
                    {
                        inverseCollection.Add(principalEntity, dependentEntity, forMaterialization: true);
                    }
                }
                else if (inverse is not null)
                {
                    SetNavigationValue(inverse, principalEntity, dependentEntity);
                }
            }
        }
        else
        {
            Dictionary<StructuralKey, object> principalByKey = [];
            foreach ((object declaringEntity, ValueBuffer declaringBuffer) in declaringRows)
            {
                if (TryExtractNonNullKey(pkProperties, declaringBuffer, out StructuralKey pk))
                {
                    principalByKey[pk] = declaringEntity;
                }
            }

            Dictionary<object, List<(object Entity, ValueBuffer Buffer)>> childrenByPrincipal = [with(ReferenceEqualityComparer.Instance)];
            foreach ((object dependentEntity, ValueBuffer dependentBuffer) in candidateTargetRows)
            {
                if (!TryExtractNonNullKey(fkProperties, dependentBuffer, out StructuralKey fk)
                    || !principalByKey.TryGetValue(fk, out object? principalEntity))
                {
                    continue;
                }

                if (!childrenByPrincipal.TryGetValue(principalEntity, out List<(object Entity, ValueBuffer Buffer)>? list))
                {
                    list = [];
                    childrenByPrincipal[principalEntity] = list;
                }

                list.Add((dependentEntity, dependentBuffer));
            }

            IClrCollectionAccessor? collectionAccessor = navigation.IsCollection ? navigation.GetCollectionAccessor() : null;
            INavigation? inverse = navigation.Inverse;

            foreach (KeyValuePair<object, List<(object Entity, ValueBuffer Buffer)>> kvp in childrenByPrincipal)
            {
                object principalEntity = kvp.Key;
                List<(object Entity, ValueBuffer Buffer)> childEntries = kvp.Value;

                if (collectionFilter is not null && navigation.IsCollection)
                {
                    Dictionary<object, ValueBuffer> bufByObj = [with(ReferenceEqualityComparer.Instance)];
                    List<object> rawChildren = [];
                    foreach ((object childObj, ValueBuffer childBuf) in childEntries)
                    {
                        bufByObj[childObj] = childBuf;
                        rawChildren.Add(childObj);
                    }

                    List<(object Entity, ValueBuffer Buffer)> filteredChildren = [];
                    foreach (object survivingChild in collectionFilter(queryContext, state, rawChildren))
                    {
                        if (bufByObj.TryGetValue(survivingChild, out ValueBuffer childBuf))
                        {
                            filteredChildren.Add((survivingChild, childBuf));
                        }
                    }

                    childEntries = filteredChildren;
                }

                foreach ((object rawChild, ValueBuffer childBuffer) in childEntries)
                {
                    object dependentEntity = PromoteTargetIfNeeded(rawChild, childBuffer);
                    if (promotedSet.Add(dependentEntity))
                    {
                        promotedTargets.Add((dependentEntity, childBuffer));
                    }

                    if (collectionAccessor is not null)
                    {
                        if (!collectionAccessor.Contains(principalEntity, dependentEntity))
                        {
                            collectionAccessor.Add(principalEntity, dependentEntity, forMaterialization: true);
                        }
                    }
                    else
                    {
                        SetNavigationValue(navigation, principalEntity, dependentEntity);
                    }

                    if (inverse is { IsCollection: false })
                    {
                        SetNavigationValue(inverse, dependentEntity, principalEntity);
                    }
                }
            }
        }

        return promotedTargets;
    }

    private static void ClearCollectionInstance(object? collectionInstance)
    {
        if (collectionInstance is IList list)
        {
            list.Clear();
            return;
        }

        if (collectionInstance is not null)
        {
            MethodInfo? clearMethod = collectionInstance.GetType().GetMethod("Clear", Type.EmptyTypes);
            _ = clearMethod?.Invoke(collectionInstance, null);
        }
    }

    private static void FixupNavigation(
        QueryContext queryContext,
        INavigation navigation,
        List<(object Entity, ValueBuffer Buffer)> declaringRows,
        List<(object Entity, ValueBuffer Buffer)> targetRows,
        Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>? collectionTransform,
        JetExecutionMaterializedState state,
        bool markNavigationLoaded)
        => _ = FixupNavigationAndPromoteMatchedTargets(
            queryContext,
            navigation,
            declaringRows,
            targetRows,
            collectionTransform,
            plan: null,
            state,
            existingTrackedByType: null,
            initializedCollections: null,
            isTracking: markNavigationLoaded);

    private static object? GetNavigationValue(INavigationBase navigation, object entity)
    {
        if (navigation.PropertyInfo is PropertyInfo { CanRead: true } propertyInfo)
        {
            return propertyInfo.GetValue(entity);
        }

        if (navigation.FieldInfo is FieldInfo fieldInfo)
        {
            return fieldInfo.GetValue(entity);
        }

        return null;
    }

    private static void SetNavigationValue(INavigationBase navigation, object entity, object? value)
    {
        if (navigation.FieldInfo is FieldInfo fieldInfo)
        {
            fieldInfo.SetValue(entity, value);
        }
        else if (navigation.PropertyInfo is PropertyInfo { CanWrite: true } propertyInfo)
        {
            propertyInfo.SetValue(entity, value);
        }
    }

    private static StructuralKey ExtractKey(IKey key, ValueBuffer buffer)
    {
        object?[] values = new object?[key.Properties.Count];
        for (int i = 0; i < key.Properties.Count; i++)
        {
            values[i] = buffer[key.Properties[i].GetIndex()];
        }

        return new StructuralKey(values);
    }

    private static StructuralKey ExtractKeyFromEntity(IKey key, object entity)
    {
        object?[] values = new object?[key.Properties.Count];
        for (int i = 0; i < key.Properties.Count; i++)
        {
            values[i] = key.Properties[i].GetGetter().GetClrValueUsingContainingEntity(entity);
        }

        return new StructuralKey(values);
    }

    private static bool EvaluateContains<TSource>(
        QueryContext queryContext,
        IEnumerable<TSource> source,
        TSource item)
    {
        if (item is not null
            && queryContext.Context.Model.FindEntityType(typeof(TSource))?.FindPrimaryKey() is IKey primaryKey)
        {
            StructuralKey itemKey = ExtractKeyFromEntity(primaryKey, item);
            foreach (TSource candidate in source)
            {
                if (candidate is not null && ExtractKeyFromEntity(primaryKey, candidate).Equals(itemKey))
                {
                    return true;
                }
            }

            return false;
        }

        return source.Contains(item);
    }

    private static bool TryExtractNonNullKey(IReadOnlyList<IProperty> properties, ValueBuffer buffer, out StructuralKey key)
    {
        object?[] values = new object?[properties.Count];
        for (int i = 0; i < properties.Count; i++)
        {
            int idx = properties[i].GetIndex();
            object? val = (uint)idx < (uint)buffer.Count ? buffer[idx] : null;
            if (val is null or DBNull)
            {
                key = default;
                return false;
            }

            values[i] = val;
        }

        key = new StructuralKey(values);
        return true;
    }

    private static List<TEntity> GetRootEnumerable<TEntity>(JetExecutionMaterializedState state, int rootId)
    {
        if (!state.RootRows.TryGetValue(rootId, out List<(object Entity, ValueBuffer Buffer)>? rows) || rows.Count == 0)
        {
            return [];
        }

        List<TEntity> typed = [];
        typed.Capacity = rows.Count;
        foreach ((object entity, _) in rows)
        {
            if (entity is TEntity castEntity)
            {
                typed.Add(castEntity);
            }
        }

        return typed;
    }

    private static TParam GetParameterValue<TParam>(QueryContext queryContext, string parameterName)
    {
        if (!queryContext.Parameters.TryGetValue(parameterName, out object? raw) || raw is null)
        {
            return default!;
        }

        if (raw is TParam direct)
        {
            return direct;
        }

        return (TParam)JetEntityTypeMetadata.CoerceToType(raw, typeof(TParam))!;
    }

    private static bool EvaluateLike(string? matchExpression, string? pattern, string? escapeCharacter)
    {
        if (matchExpression is null || pattern is null)
        {
            return false;
        }

        if (!string.IsNullOrEmpty(escapeCharacter) && escapeCharacter.Length != 1)
        {
            throw new ArgumentException("Escape character must be a single character.", nameof(escapeCharacter));
        }

        char? escape = !string.IsNullOrEmpty(escapeCharacter) ? escapeCharacter[0] : null;
        return MatchLikePattern(matchExpression.AsSpan(), pattern.AsSpan(), escape);
    }

    private static bool MatchLikePattern(ReadOnlySpan<char> input, ReadOnlySpan<char> pattern, char? escape)
    {
        int i = 0;
        int p = 0;
        int starInputIdx = -1;
        int starPatternIdx = -1;

        while (i < input.Length)
        {
            if (p < pattern.Length)
            {
                char pc = pattern[p];
                if (escape.HasValue && pc == escape.Value)
                {
                    p++;
                    if (p < pattern.Length && char.ToUpperInvariant(pattern[p]) == char.ToUpperInvariant(input[i]))
                    {
                        i++;
                        p++;
                        continue;
                    }
                }
                else if (pc is '%' or '*')
                {
                    starPatternIdx = p++;
                    starInputIdx = i;
                    continue;
                }
                else if (pc == '_'
                    || char.ToUpperInvariant(pc) == char.ToUpperInvariant(input[i]))
                {
                    i++;
                    p++;
                    continue;
                }
            }

            if (starPatternIdx != -1)
            {
                p = starPatternIdx + 1;
                i = ++starInputIdx;
                continue;
            }

            return false;
        }

        while (p < pattern.Length && pattern[p] is '%' or '*')
        {
            p++;
        }

        return p == pattern.Length;
    }

    private static TProperty ReadEfProperty<TProperty>(
        QueryContext queryContext,
        IReadOnlyDictionary<object, ValueBuffer> entityBuffers,
        object? entity,
        string propertyName)
    {
        if (entity is null)
        {
            return default!;
        }

        IEntityType? entityType = queryContext.Context.Model.FindEntityType(entity.GetType());
        IProperty? property = entityType?.FindProperty(propertyName);
        if (property is not null)
        {
            if (property.PropertyInfo is not null || property.FieldInfo is not null)
            {
                object? clrVal = property.GetGetter().GetClrValueUsingContainingEntity(entity);
                return clrVal is TProperty typedClr ? typedClr : default!;
            }

            if (entityBuffers.TryGetValue(entity, out ValueBuffer buffer))
            {
                int idx = property.GetIndex();
                if ((uint)idx < (uint)buffer.Count && buffer[idx] is TProperty typedBuf)
                {
                    return typedBuf;
                }
            }

            object? trackedVal = queryContext.Context.Entry(entity).Property(propertyName).CurrentValue;
            return trackedVal is TProperty typedTracked ? typedTracked : default!;
        }

        INavigationBase? navigation = entityType?.FindNavigation(propertyName) ?? (INavigationBase?)entityType?.FindSkipNavigation(propertyName);
        if (navigation is not null && GetNavigationValue(navigation, entity) is TProperty navVal)
        {
            return navVal;
        }

        if (entity is IDictionary<string, object?> dict && dict.TryGetValue(propertyName, out object? dictVal))
        {
            return dictVal is TProperty typedDict ? typedDict : default!;
        }

        PropertyInfo? propInfo = entity.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        if (propInfo?.GetValue(entity) is TProperty reflectedVal)
        {
            return reflectedVal;
        }

        return default!;
    }

    private readonly struct StructuralKey(object?[] values) : IEquatable<StructuralKey>
    {
        public object?[] Values { get; } = values;

        public bool Equals(StructuralKey other)
        {
            if (this.Values.Length != other.Values.Length)
            {
                return false;
            }

            for (int i = 0; i < this.Values.Length; i++)
            {
                if (!Equals(this.Values[i], other.Values[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj)
            => obj is StructuralKey other && this.Equals(other);

        public override int GetHashCode()
        {
            HashCode hash = default;
            foreach (object? val in this.Values)
            {
                hash.Add(val);
            }

            return hash.ToHashCode();
        }
    }

    private sealed record JetJoinKeyLoadSpec(
        JetTableLoadPlan OuterPlan,
        IReadOnlyList<IProperty> OuterKeyProperties,
        IReadOnlyList<IProperty> InnerKeyProperties);

    private sealed class JetTableLoadPlan(int rootId, IEntityType rootEntityType, IEntityType queryEntityType, List<JetIndexSeekCondition> seekConditions)
    {
        public int RootId { get; } = rootId;

        public IEntityType RootEntityType { get; } = rootEntityType;

        public IEntityType QueryEntityType { get; } = queryEntityType;

        public List<JetIndexSeekCondition> SeekConditions { get; } = seekConditions;

        public IReadOnlySet<IProperty>? RequiredProperties { get; set; }

        public Func<IReadOnlyDictionary<string, object?>, int?>? MaxRowsEvaluator { get; set; }

        public IReadOnlyList<IProperty>? OrderedAscendingProperties { get; set; }

        public Func<QueryContext, ValueBuffer, bool>? ValueBufferRowFilter { get; set; }

        public Func<QueryContext, JetExecutionMaterializedState, object, bool>? CandidateRowFilter { get; set; }

        public JetJoinKeyLoadSpec? JoinLoadSpec { get; set; }
    }

    private sealed class EntityNavigationPairComparer : IEqualityComparer<(object Entity, INavigationBase Navigation)>
    {
        public static EntityNavigationPairComparer Instance { get; } = new();

        public bool Equals((object Entity, INavigationBase Navigation) x, (object Entity, INavigationBase Navigation) y)
            => ReferenceEquals(x.Entity, y.Entity) && Equals(x.Navigation, y.Navigation);

        public int GetHashCode((object Entity, INavigationBase Navigation) obj)
            => HashCode.Combine(RuntimeHelpers.GetHashCode(obj.Entity), obj.Navigation);
    }

    private sealed class JetIncludeStep(
        INavigationBase navigation,
        Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>? collectionFilter)
    {
        public INavigationBase Navigation { get; } = navigation;

        public Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>? CollectionFilter { get; } = collectionFilter;
    }

    private sealed class JetIncludeChain(IReadOnlyList<JetIncludeStep> steps)
    {
        public IReadOnlyList<JetIncludeStep> Steps { get; } = steps;
    }

    private sealed class JetQueryPlan(
        List<JetTableLoadPlan> rootTables,
        Dictionary<EntityQueryRootExpression, JetTableLoadPlan> rootPlansByExpression,
        List<JetIncludeChain> includeChains,
        List<INavigationBase> queryReferencedNavigations,
        QueryTrackingBehavior? trackingBehaviorOverride,
        bool ignoreQueryFilters)
    {
        private readonly Dictionary<IEntityType, Func<QueryContext, ValueBuffer, object>> trackedMaterializers = [];
        private readonly Dictionary<IEntityType, Func<QueryContext, ValueBuffer, object>> untrackedMaterializers = [];
        private readonly Dictionary<IQueryFilter, Func<QueryContext, JetExecutionMaterializedState, object, bool>?> compiledQueryFilters = [];

        public List<JetTableLoadPlan> RootTables { get; } = rootTables;

        public Dictionary<EntityQueryRootExpression, JetTableLoadPlan> RootPlansByExpression { get; } = rootPlansByExpression;

        public List<JetIncludeChain> IncludeChains { get; } = includeChains;

        public List<INavigationBase> QueryReferencedNavigations { get; } = queryReferencedNavigations;

        public QueryTrackingBehavior? TrackingBehaviorOverride { get; } = trackingBehaviorOverride;

        public bool IgnoreQueryFilters { get; } = ignoreQueryFilters;

        public Func<QueryContext, ValueBuffer, object> GetTrackedMaterializer(QueryContext queryContext, IEntityType entityType)
        {
            lock (this.trackedMaterializers)
            {
                if (!this.trackedMaterializers.TryGetValue(entityType, out Func<QueryContext, ValueBuffer, object>? materializer))
                {
                    JetShapedQueryCompilingExpressionVisitor visitor = CreateShaperVisitor(queryContext, entityType, trackAll: true);
                    materializer = visitor.CreateEntityMaterializer(entityType);
                    this.trackedMaterializers[entityType] = materializer;
                }

                return materializer;
            }
        }

        public Func<QueryContext, ValueBuffer, object> GetUntrackedMaterializer(QueryContext queryContext, IEntityType entityType)
        {
            lock (this.untrackedMaterializers)
            {
                if (!this.untrackedMaterializers.TryGetValue(entityType, out Func<QueryContext, ValueBuffer, object>? materializer))
                {
                    JetShapedQueryCompilingExpressionVisitor visitor = CreateShaperVisitor(queryContext, entityType, trackAll: false);
                    materializer = visitor.CreateEntityMaterializer(entityType);
                    this.untrackedMaterializers[entityType] = materializer;
                }

                return materializer;
            }
        }

        public Func<QueryContext, JetExecutionMaterializedState, object, bool>? GetCompiledQueryFilter(IModel model, IQueryFilter filter)
        {
            lock (this.compiledQueryFilters)
            {
                if (!this.compiledQueryFilters.TryGetValue(filter, out Func<QueryContext, JetExecutionMaterializedState, object, bool>? compiled))
                {
                    if (filter.Expression is LambdaExpression { Parameters.Count: 1 } filterLambda)
                    {
                        ParameterExpression qcParam = Expression.Parameter(typeof(QueryContext), "queryContext");
                        ParameterExpression stateParam = Expression.Parameter(typeof(JetExecutionMaterializedState), "state");
                        ParameterExpression objParam = Expression.Parameter(typeof(object), "entity");

                        var rewrittenFilter = (LambdaExpression)new JetCompiledExpressionRewriter(
                            qcParam,
                            stateParam,
                            model,
                            new Dictionary<EntityQueryRootExpression, JetTableLoadPlan>(ReferenceEqualityComparer.Instance),
                            ignoreQueryFilters: false).Visit(filterLambda);

                        InvocationExpression invoke = Expression.Invoke(
                            rewrittenFilter,
                            Expression.Convert(objParam, filterLambda.Parameters[0].Type));
                        compiled = Expression.Lambda<Func<QueryContext, JetExecutionMaterializedState, object, bool>>(
                            invoke,
                            qcParam,
                            stateParam,
                            objParam).Compile();
                    }

                    this.compiledQueryFilters[filter] = compiled;
                }

                return compiled;
            }
        }

        private static JetShapedQueryCompilingExpressionVisitor CreateShaperVisitor(
            QueryContext queryContext,
            IEntityType entityType,
            bool trackAll)
        {
            IQueryCompilationContextFactory compilationContextFactory =
                queryContext.Context.GetService<IQueryCompilationContextFactory>();
            IQueryTranslationPreprocessorFactory preprocessorFactory =
                queryContext.Context.GetService<IQueryTranslationPreprocessorFactory>();
            IShapedQueryCompilingExpressionVisitorFactory shaperFactory =
                queryContext.Context.GetService<IShapedQueryCompilingExpressionVisitorFactory>();

            QueryCompilationContext context = compilationContextFactory.Create(async: true);
            string methodName = trackAll
                ? nameof(EntityFrameworkQueryableExtensions.AsTracking)
                : nameof(EntityFrameworkQueryableExtensions.AsNoTracking);
            MethodCallExpression trackingExpr = Expression.Call(
                typeof(EntityFrameworkQueryableExtensions),
                methodName,
                [entityType.ClrType],
                new EntityQueryRootExpression(entityType));
            _ = preprocessorFactory.Create(context).Process(trackingExpr);
            return (JetShapedQueryCompilingExpressionVisitor)shaperFactory.Create(context);
        }
    }

    private sealed class MaterializedIncludableQueryable<TElement, TProperty>(IOrderedQueryable<TElement> inner)
        : IIncludableQueryable<TElement, TProperty>, IOrderedQueryable<TElement>
    {
        public Type ElementType => inner.ElementType;

        public Expression Expression => inner.Expression;

        public IQueryProvider Provider => inner.Provider;

        public IEnumerator<TElement> GetEnumerator() => inner.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)inner).GetEnumerator();
    }

    private sealed class JetExecutionMaterializedState
    {
        public Dictionary<int, List<(object Entity, ValueBuffer Buffer)>> RootRows { get; } = [];

        public Dictionary<IEntityType, List<(object Entity, ValueBuffer Buffer)>> TablesByEntityType { get; } = [];

        public Dictionary<object, ValueBuffer> EntityBuffers { get; } = [with(ReferenceEqualityComparer.Instance)];

        public Dictionary<object, IEntityType> EntityTypesByInstance { get; } = [with(ReferenceEqualityComparer.Instance)];

        public Dictionary<object, object> PromotedEntities { get; } = [with(ReferenceEqualityComparer.Instance)];

        public HashSet<object> AlreadyTrackedEntities { get; } = [with(ReferenceEqualityComparer.Instance)];

        public Dictionary<object, List<(object JoinEntity, ValueBuffer JoinBuffer)>> SkipJoinEntitiesByTarget { get; } =
            [with(ReferenceEqualityComparer.Instance)];
    }

    private sealed class JetQueryPlanExtractor(IModel model) : ExpressionVisitor
    {
        private readonly List<JetTableLoadPlan> rootTables = [];
        private readonly Dictionary<EntityQueryRootExpression, JetTableLoadPlan> rootPlansByExpression = [with(ReferenceEqualityComparer.Instance)];
        private readonly List<JetIncludeChain> includeChains = [];
        private readonly List<INavigationBase> queryReferencedNavigations = [];
        private readonly HashSet<(int HopDepth, INavigationBase Navigation)> queryReferencedNavigationSet = [];
        private bool ignoreAutoIncludes;
        private bool inIncludeArgument;

        public QueryTrackingBehavior? TrackingBehaviorOverride { get; private set; }

        public bool IgnoreQueryFilters { get; private set; }

        public static JetQueryPlan Analyze(
            Expression query,
            IModel model)
        {
            JetQueryPlanExtractor extractor = new(model);
            _ = extractor.Visit(query);

            if (!extractor.ignoreAutoIncludes && !IsNonEntityTerminalQuery(query))
            {
                foreach (JetTableLoadPlan tablePlan in extractor.rootTables)
                {
                    extractor.AddAutoIncludes(tablePlan.QueryEntityType, [], []);
                }
            }

            if (!extractor.IgnoreQueryFilters)
            {
                foreach (JetTableLoadPlan tablePlan in extractor.rootTables)
                {
                    foreach (IQueryFilter filter in GetAllQueryFilters(tablePlan.QueryEntityType))
                    {
                        if (filter.Expression is LambdaExpression filterLambda)
                        {
                            tablePlan.SeekConditions.AddRange(
                                JetIndexSeekPlanner.ExtractSeekConditions(tablePlan.RootEntityType, filterLambda));
                        }
                    }
                }
            }

            if (extractor.rootTables.Count == 1
                && extractor.queryReferencedNavigations.Count == 0)
            {
                JetTableLoadPlan singlePlan = extractor.rootTables[0];
                if (extractor.includeChains.Count == 0)
                {
                    singlePlan.RequiredProperties = TryComputeRequiredProperties(
                        query,
                        model,
                        singlePlan,
                        extractor.IgnoreQueryFilters);
                }

                ConfigureSingleTableScanOptimizations(
                    query,
                    model,
                    singlePlan,
                    extractor.IgnoreQueryFilters);
            }

            return new JetQueryPlan(
                extractor.rootTables,
                extractor.rootPlansByExpression,
                extractor.includeChains,
                extractor.queryReferencedNavigations,
                extractor.TrackingBehaviorOverride,
                extractor.IgnoreQueryFilters);
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is EntityQueryRootExpression queryRoot)
            {
                _ = this.GetOrCreateRootPlan(queryRoot);
                return node;
            }

            return base.VisitExtension(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions))
            {
                if (node.Method.Name == nameof(EntityFrameworkQueryableExtensions.AsNoTracking))
                {
                    this.TrackingBehaviorOverride = QueryTrackingBehavior.NoTracking;
                }
                else if (node.Method.Name == nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution))
                {
                    this.TrackingBehaviorOverride = QueryTrackingBehavior.NoTrackingWithIdentityResolution;
                }
                else if (node.Method.Name == nameof(EntityFrameworkQueryableExtensions.AsTracking))
                {
                    this.TrackingBehaviorOverride = QueryTrackingBehavior.TrackAll;
                }
                else if (node.Method.Name == nameof(EntityFrameworkQueryableExtensions.IgnoreQueryFilters))
                {
                    this.IgnoreQueryFilters = true;
                }
                else if (node.Method.Name == nameof(EntityFrameworkQueryableExtensions.IgnoreAutoIncludes))
                {
                    this.ignoreAutoIncludes = true;
                }
                else if (node.Method.Name is nameof(EntityFrameworkQueryableExtensions.Include)
                         or nameof(EntityFrameworkQueryableExtensions.ThenInclude)
                         or "NotQuiteInclude")
                {
                    this.ExtractIncludeChain(node);
                    return node;
                }
            }
            else if (node.Method.DeclaringType == typeof(EF)
                     && node.Method.Name == nameof(EF.Property)
                     && node.Arguments.Count == 2
                     && !this.inIncludeArgument
                     && StripQuotes(node.Arguments[1]) is ConstantExpression { Value: string efPropName })
            {
                IEntityType? ownerType = model.FindEntityType(node.Arguments[0].Type);
                INavigationBase? nav = FindNavigationBase(ownerType, efPropName);
                if (nav is not null && this.queryReferencedNavigationSet.Add((0, nav)))
                {
                    this.queryReferencedNavigations.Add(nav);
                }
            }
            else if (node.Method.DeclaringType == typeof(Queryable))
            {
                if (node.Method.Name is nameof(Queryable.Where)
                    or nameof(Queryable.First)
                    or nameof(Queryable.FirstOrDefault)
                    or nameof(Queryable.Single)
                    or nameof(Queryable.SingleOrDefault)
                    or nameof(Queryable.Last)
                    or nameof(Queryable.LastOrDefault)
                    or nameof(Queryable.Count)
                    or nameof(Queryable.LongCount)
                    or nameof(Queryable.Any)
                    && node.Arguments.Count is 2 or 3
                    && StripQuotes(node.Arguments[1]) is LambdaExpression predicate
                    && TryFindDirectRootExpression(node.Arguments[0], out EntityQueryRootExpression? queryRoot))
                {
                    _ = this.Visit(node.Arguments[0]);
                    JetTableLoadPlan plan = this.GetOrCreateRootPlan(queryRoot!);
                    plan.SeekConditions.AddRange(
                        JetIndexSeekPlanner.ExtractSeekConditions(plan.RootEntityType, predicate));
                    _ = this.Visit(predicate);
                    if (node.Arguments.Count == 3)
                    {
                        _ = this.Visit(node.Arguments[2]);
                    }

                    return node;
                }

                if (node.Method.Name is nameof(Queryable.Join) or nameof(Queryable.GroupJoin)
                    && node.Arguments.Count >= 5
                    && TryFindDirectRootExpression(node.Arguments[0], out EntityQueryRootExpression? outerRoot)
                    && TryFindDirectRootExpression(node.Arguments[1], out EntityQueryRootExpression? innerRoot)
                    && StripQuotes(node.Arguments[2]) is LambdaExpression { Parameters.Count: 1 } outerKeyLambda
                    && StripQuotes(node.Arguments[3]) is LambdaExpression { Parameters.Count: 1 } innerKeyLambda
                    && TryExtractJoinKeyProperties(
                        outerRoot!.EntityType.GetRootType(),
                        outerKeyLambda.Parameters[0],
                        outerKeyLambda.Body,
                        out List<IProperty>? outerKeyProps)
                    && TryExtractJoinKeyProperties(
                        innerRoot!.EntityType.GetRootType(),
                        innerKeyLambda.Parameters[0],
                        innerKeyLambda.Body,
                        out List<IProperty>? innerKeyProps)
                    && outerKeyProps!.Count == innerKeyProps!.Count)
                {
                    _ = this.Visit(node.Arguments[0]);
                    _ = this.Visit(node.Arguments[1]);
                    JetTableLoadPlan outerPlan = this.GetOrCreateRootPlan(outerRoot);
                    JetTableLoadPlan innerPlan = this.GetOrCreateRootPlan(innerRoot);
                    innerPlan.JoinLoadSpec = new JetJoinKeyLoadSpec(outerPlan, outerKeyProps, innerKeyProps);
                    for (int i = 2; i < node.Arguments.Count; i++)
                    {
                        _ = this.Visit(node.Arguments[i]);
                    }

                    return node;
                }
            }

            return base.VisitMethodCall(node);
        }

        private static bool IsNonEntityTerminalQuery(Expression query)
        {
            Expression current = UnwrapEfQueryModifiers(query);
            if (current is not MethodCallExpression call)
            {
                return false;
            }

            if (call.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
                && call.Method.Name is nameof(EntityFrameworkQueryableExtensions.ExecuteDelete)
                    or nameof(EntityFrameworkQueryableExtensions.ExecuteUpdate))
            {
                return true;
            }

            return call.Method.DeclaringType == typeof(Queryable)
                && call.Method.Name is nameof(Queryable.Count)
                    or nameof(Queryable.LongCount)
                    or nameof(Queryable.Any)
                    or nameof(Queryable.All)
                    or nameof(Queryable.Sum)
                    or nameof(Queryable.Average);
        }

        private static bool TryExtractJoinKeyProperties(
            IEntityType rootEntityType,
            ParameterExpression param,
            Expression body,
            out List<IProperty>? properties)
        {
            Expression unwrapped = StripQuotes(body);
            if (JetIndexSeekPlanner.TryExtractPropertyAccess(rootEntityType, param, unwrapped, out IProperty? singleProp))
            {
                properties = [singleProp!];
                return true;
            }

            if (unwrapped is NewExpression { Arguments.Count: > 0 } newExpr)
            {
                List<IProperty> list = [];
                foreach (Expression arg in newExpr.Arguments)
                {
                    if (!JetIndexSeekPlanner.TryExtractPropertyAccess(rootEntityType, param, arg, out IProperty? prop))
                    {
                        properties = null;
                        return false;
                    }

                    list.Add(prop!);
                }

                properties = list;
                return true;
            }

            properties = null;
            return false;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            Expression visited = base.VisitMember(node);
            if (!this.inIncludeArgument && node.Expression is not null)
            {
                IEntityType? ownerType = model.FindEntityType(node.Expression.Type);
                INavigationBase? nav = FindNavigationBase(ownerType, node.Member);
                if (nav is not null && this.queryReferencedNavigationSet.Add((this.GetNavigationHopDepth(node), nav)))
                {
                    this.queryReferencedNavigations.Add(nav);
                }
            }

            return visited;
        }

        private static INavigationBase? FindNavigationBase(IEntityType? ownerType, MemberInfo member)
            => ownerType?.FindNavigation(member) ?? (INavigationBase?)ownerType?.FindSkipNavigation(member);

        private static INavigationBase? FindNavigationBase(IEntityType? ownerType, string name)
            => ownerType?.FindNavigation(name) ?? (INavigationBase?)ownerType?.FindSkipNavigation(name);

        private static Expression StripQuotes(Expression expression)
        {
            Expression current = expression;
            while (current is UnaryExpression { NodeType: ExpressionType.Quote } quote)
            {
                current = quote.Operand;
            }

            return current;
        }

        private static bool TryFindDirectRootExpression(Expression source, out EntityQueryRootExpression? rootExpression)
        {
            Expression current = source;
            while (current is MethodCallExpression call)
            {
                if (call.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions))
                {
                    current = call.Arguments[0];
                    continue;
                }

                if (call.Method.DeclaringType == typeof(Queryable)
                    && call.Method.Name is nameof(Queryable.Where)
                        or nameof(Queryable.OrderBy)
                        or nameof(Queryable.OrderByDescending)
                        or nameof(Queryable.ThenBy)
                        or nameof(Queryable.ThenByDescending))
                {
                    current = call.Arguments[0];
                    continue;
                }

                break;
            }

            if (current is EntityQueryRootExpression root)
            {
                rootExpression = root;
                return true;
            }

            rootExpression = null;
            return false;
        }

        private static bool TryFindRootEntityTypeRecursive(Expression expr, out IEntityType? entityType)
        {
            Expression current = expr;
            while (current is MethodCallExpression call && call.Arguments.Count > 0)
            {
                current = call.Arguments[0];
            }

            if (current is EntityQueryRootExpression root)
            {
                entityType = root.EntityType;
                return true;
            }

            entityType = null;
            return false;
        }

        private static bool TryExtractIncludeMemberAndFilter(
            LambdaExpression lambda,
            IModel model,
            out MemberExpression? memberExpression,
            out Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>? collectionFilter)
        {
            memberExpression = null;
            collectionFilter = null;

            if (lambda.Parameters.Count != 1)
            {
                return false;
            }

            ParameterExpression rootParam = lambda.Parameters[0];
            Expression current = lambda.Body;
            while (current is MethodCallExpression call && call.Arguments.Count > 0)
            {
                current = call.Arguments[0];
            }

            if (current is not MemberExpression member || StripQuotes(member.Expression!) != rootParam)
            {
                return false;
            }

            memberExpression = member;
            if (ReferenceEquals(member, lambda.Body))
            {
                return true;
            }

            IEntityType? ownerType = model.FindEntityType(rootParam.Type);
            INavigationBase? nav = FindNavigationBase(ownerType, member.Member);
            if (nav is not { IsCollection: true })
            {
                return true;
            }

            Type elementType = nav.TargetEntityType.ClrType;
            ParameterExpression queryContextParam = Expression.Parameter(typeof(QueryContext), "queryContext");
            ParameterExpression stateParam = Expression.Parameter(typeof(JetExecutionMaterializedState), "state");
            ParameterExpression rawCollectionParam = Expression.Parameter(typeof(IEnumerable<object>), "rawItems");
            MethodCallExpression castCall = Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Cast),
                [elementType],
                rawCollectionParam);

            Expression replacedBody = new ExpressionReplacer(member, castCall).Visit(lambda.Body)!;
            Expression rewrittenBody = new JetCompiledExpressionRewriter(
                queryContextParam,
                stateParam,
                model,
                new Dictionary<EntityQueryRootExpression, JetTableLoadPlan>(ReferenceEqualityComparer.Instance),
                ignoreQueryFilters: true).Visit(replacedBody);

            MethodCallExpression boxResult = Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Cast),
                [typeof(object)],
                rewrittenBody);
            collectionFilter = Expression.Lambda<Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>>(
                boxResult,
                queryContextParam,
                stateParam,
                rawCollectionParam).Compile();
            return true;
        }

        private static HashSet<IProperty>? TryComputeRequiredProperties(
            Expression query,
            IModel model,
            JetTableLoadPlan tablePlan,
            bool ignoreQueryFilters)
        {
            Type unwrappedResultType = UnwrapSequenceType(query.Type);
            if (model.FindEntityType(unwrappedResultType) is not null)
            {
                return null;
            }

            RootEntityPropertyUsageAnalyzer analyzer = new(model, tablePlan.RootEntityType);
            _ = analyzer.Visit(query);
            if (analyzer.EntityEscapes)
            {
                return null;
            }

            if (!ignoreQueryFilters)
            {
                foreach (IQueryFilter filter in GetAllQueryFilters(tablePlan.QueryEntityType))
                {
                    if (filter.Expression is LambdaExpression filterLambda)
                    {
                        _ = analyzer.Visit(filterLambda);
                        if (analyzer.EntityEscapes)
                        {
                            return null;
                        }
                    }
                }
            }

            IKey? pk = tablePlan.RootEntityType.FindPrimaryKey();
            if (pk is not null)
            {
                foreach (IProperty pkProp in pk.Properties)
                {
                    _ = analyzer.ReferencedProperties.Add(pkProp);
                }
            }

            IProperty? discriminator = tablePlan.RootEntityType.FindDiscriminatorProperty();
            if (discriminator is not null)
            {
                _ = analyzer.ReferencedProperties.Add(discriminator);
            }

            return analyzer.ReferencedProperties;
        }

        private static void ConfigureSingleTableScanOptimizations(
            Expression query,
            IModel model,
            JetTableLoadPlan tablePlan,
            bool ignoreQueryFilters)
        {
            Func<IReadOnlyDictionary<string, object?>, int?>? limitEvaluator = null;
            List<LambdaExpression> rowPredicates = [];
            List<IProperty> ascendingOrderProps = [];
            bool sawUnsupportedForLimit = false;
            bool sawOrderOperator = false;
            bool orderIsPureAscendingProperties = true;

            Expression current = query;
            while (current is MethodCallExpression call)
            {
                if (call.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions))
                {
                    if (call.Method.Name is nameof(EntityFrameworkQueryableExtensions.AsNoTracking)
                        or nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution)
                        or nameof(EntityFrameworkQueryableExtensions.AsTracking)
                        or nameof(EntityFrameworkQueryableExtensions.IgnoreQueryFilters)
                        or nameof(EntityFrameworkQueryableExtensions.IgnoreAutoIncludes)
                        or nameof(EntityFrameworkQueryableExtensions.TagWith)
                        or nameof(EntityFrameworkQueryableExtensions.TagWithCallSite))
                    {
                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name is nameof(EntityFrameworkQueryableExtensions.ExecuteDelete)
                        or nameof(EntityFrameworkQueryableExtensions.ExecuteUpdate))
                    {
                        sawUnsupportedForLimit = true;
                        current = call.Arguments[0];
                        continue;
                    }
                }

                if (call.Method.DeclaringType == typeof(Queryable))
                {
                    if (call.Method.Name == nameof(Queryable.Select) && call.Arguments.Count == 2)
                    {
                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name is nameof(Queryable.Count) or nameof(Queryable.LongCount))
                    {
                        if (call.Arguments.Count >= 2 && StripQuotes(call.Arguments[1]) is LambdaExpression countPred)
                        {
                            rowPredicates.Add(countPred);
                        }

                        sawUnsupportedForLimit = true;
                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name is nameof(Queryable.All)
                        or nameof(Queryable.Sum)
                        or nameof(Queryable.Average)
                        or nameof(Queryable.Min)
                        or nameof(Queryable.Max)
                        or nameof(Queryable.MinBy)
                        or nameof(Queryable.MaxBy)
                        or nameof(Queryable.Distinct)
                        or nameof(Queryable.Last)
                        or nameof(Queryable.LastOrDefault)
                        or nameof(Queryable.ElementAt)
                        or nameof(Queryable.ElementAtOrDefault))
                    {
                        sawUnsupportedForLimit = true;
                        if (sawOrderOperator)
                        {
                            orderIsPureAscendingProperties = false;
                        }

                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name is nameof(Queryable.Any)
                        or nameof(Queryable.First)
                        or nameof(Queryable.FirstOrDefault))
                    {
                        if (rowPredicates.Count > 0)
                        {
                            rowPredicates.Clear();
                        }

                        if (call.Arguments.Count >= 2 && StripQuotes(call.Arguments[1]) is LambdaExpression termPred)
                        {
                            rowPredicates.Add(termPred);
                        }

                        if (!sawOrderOperator && !sawUnsupportedForLimit)
                        {
                            limitEvaluator = CombineMinLimit(limitEvaluator, static _ => 1);
                        }
                        else if (sawOrderOperator)
                        {
                            orderIsPureAscendingProperties = false;
                        }

                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name is nameof(Queryable.Single)
                        or nameof(Queryable.SingleOrDefault))
                    {
                        if (rowPredicates.Count > 0)
                        {
                            rowPredicates.Clear();
                        }

                        if (call.Arguments.Count >= 2 && StripQuotes(call.Arguments[1]) is LambdaExpression termPred)
                        {
                            rowPredicates.Add(termPred);
                        }

                        if (!sawOrderOperator && !sawUnsupportedForLimit)
                        {
                            limitEvaluator = CombineMinLimit(limitEvaluator, static _ => 2);
                        }
                        else if (sawOrderOperator)
                        {
                            orderIsPureAscendingProperties = false;
                        }

                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name == nameof(Queryable.Take)
                        && call.Arguments.Count == 2
                        && TryBuildIntEvaluator(call.Arguments[1], out Func<IReadOnlyDictionary<string, object?>, int?>? takeEval))
                    {
                        if (rowPredicates.Count > 0)
                        {
                            rowPredicates.Clear();
                        }

                        if (!sawOrderOperator && !sawUnsupportedForLimit)
                        {
                            limitEvaluator = CombineMinLimit(limitEvaluator, takeEval!);
                        }
                        else if (sawOrderOperator)
                        {
                            orderIsPureAscendingProperties = false;
                        }

                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name == nameof(Queryable.Skip)
                        && call.Arguments.Count == 2
                        && limitEvaluator is not null
                        && !sawOrderOperator
                        && !sawUnsupportedForLimit
                        && TryBuildIntEvaluator(call.Arguments[1], out Func<IReadOnlyDictionary<string, object?>, int?>? skipEval))
                    {
                        if (rowPredicates.Count > 0)
                        {
                            rowPredicates.Clear();
                        }

                        Func<IReadOnlyDictionary<string, object?>, int?> prevLimit = limitEvaluator;
                        Func<IReadOnlyDictionary<string, object?>, int?> resolvedSkip = skipEval!;
                        limitEvaluator = parameters =>
                        {
                            if (prevLimit(parameters) is int l && resolvedSkip(parameters) is int s && l >= 0 && s >= 0)
                            {
                                return l + s;
                            }

                            return null;
                        };
                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name == nameof(Queryable.Where)
                        && call.Arguments.Count == 2
                        && StripQuotes(call.Arguments[1]) is LambdaExpression wherePred)
                    {
                        rowPredicates.Add(wherePred);
                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.ThenBy)
                        && call.Arguments.Count == 2
                        && StripQuotes(call.Arguments[1]) is LambdaExpression { Parameters.Count: 1 } orderLambda)
                    {
                        sawOrderOperator = true;
                        if (JetIndexSeekPlanner.TryExtractPropertyAccess(
                            tablePlan.RootEntityType,
                            orderLambda.Parameters[0],
                            orderLambda.Body,
                            out IProperty? orderProp))
                        {
                            ascendingOrderProps.Add(orderProp!);
                        }
                        else
                        {
                            orderIsPureAscendingProperties = false;
                        }

                        current = call.Arguments[0];
                        continue;
                    }

                    if (call.Method.Name is nameof(Queryable.OrderByDescending) or nameof(Queryable.ThenByDescending))
                    {
                        sawOrderOperator = true;
                        orderIsPureAscendingProperties = false;
                        current = call.Arguments[0];
                        continue;
                    }
                }

                sawUnsupportedForLimit = true;
                break;
            }

            if (current is not EntityQueryRootExpression)
            {
                return;
            }

            if (sawOrderOperator
                && orderIsPureAscendingProperties
                && ascendingOrderProps.Count > 0
                && tablePlan.SeekConditions.Count == 0)
            {
                ascendingOrderProps.Reverse();
                tablePlan.OrderedAscendingProperties = ascendingOrderProps;
            }

            if (!ignoreQueryFilters)
            {
                foreach (IQueryFilter queryFilter in GetAllQueryFilters(tablePlan.QueryEntityType))
                {
                    if (queryFilter.Expression is LambdaExpression { Parameters.Count: 1 } qfLambda)
                    {
                        rowPredicates.Add(qfLambda);
                    }
                }
            }

            if (!sawUnsupportedForLimit
                && limitEvaluator is not null
                && (!sawOrderOperator || tablePlan.OrderedAscendingProperties is not null))
            {
                tablePlan.MaxRowsEvaluator = limitEvaluator;
            }

            if (rowPredicates.Count > 0 || tablePlan.QueryEntityType != tablePlan.RootEntityType)
            {
                if (TryCompileValueBufferFilter(
                    model,
                    tablePlan.RootEntityType,
                    tablePlan.QueryEntityType,
                    rowPredicates,
                    out Func<QueryContext, ValueBuffer, bool>? vbFilter))
                {
                    tablePlan.ValueBufferRowFilter = vbFilter;
                }
                else
                {
                    tablePlan.CandidateRowFilter = CompileCombinedCandidateFilter(model, tablePlan.QueryEntityType, rowPredicates);
                }
            }
        }

        private static bool TryCompileValueBufferFilter(
            IModel model,
            IEntityType rootEntityType,
            IEntityType queryEntityType,
            List<LambdaExpression> predicates,
            out Func<QueryContext, ValueBuffer, bool>? filter)
        {
            filter = null;
            ParameterExpression qcParam = Expression.Parameter(typeof(QueryContext), "queryContext");
            ParameterExpression bufParam = Expression.Parameter(typeof(ValueBuffer), "buffer");
            Expression combinedBody = Expression.Constant(true);

            if (queryEntityType != rootEntityType)
            {
                IProperty? discProp = rootEntityType.FindDiscriminatorProperty();
                if (discProp is null)
                {
                    return false;
                }

                HashSet<object> allowedDiscriminators = [];
                foreach (IEntityType concreteType in queryEntityType.GetConcreteDerivedTypesInclusive())
                {
                    object? discVal = concreteType.GetDiscriminatorValue();
                    if (discVal is not null)
                    {
                        _ = allowedDiscriminators.Add(discVal);
                    }
                }

                MethodInfo isDiscAllowedMethod = typeof(JetQueryPlanExtractor).GetMethod(
                    nameof(IsDiscriminatorAllowed),
                    BindingFlags.NonPublic | BindingFlags.Static)!;
                combinedBody = Expression.Call(
                    isDiscAllowedMethod,
                    Expression.Constant(allowedDiscriminators),
                    bufParam,
                    Expression.Constant(discProp.GetIndex()));
            }

            for (int i = predicates.Count - 1; i >= 0; i--)
            {
                LambdaExpression lambda = predicates[i];
                if (lambda.Parameters.Count != 1)
                {
                    return false;
                }

                ValueBufferPredicateRewriter rewriter = new(
                    lambda.Parameters[0],
                    rootEntityType,
                    model,
                    qcParam,
                    bufParam);
                Expression rewrittenBody = rewriter.Visit(lambda.Body)!;
                if (!rewriter.CanUseValueBuffer)
                {
                    return false;
                }

                combinedBody = ReferenceEquals(combinedBody, Expression.Constant(true))
                    ? rewrittenBody
                    : Expression.AndAlso(combinedBody, rewrittenBody);
            }

            filter = Expression.Lambda<Func<QueryContext, ValueBuffer, bool>>(
                combinedBody,
                qcParam,
                bufParam).Compile();
            return true;
        }

        private static bool IsDiscriminatorAllowed(HashSet<object> allowed, ValueBuffer buffer, int index)
        {
            object? value = (uint)index < (uint)buffer.Count ? buffer[index] : null;
            return value is not null && allowed.Contains(value);
        }

        private static Func<QueryContext, JetExecutionMaterializedState, object, bool> CompileCombinedCandidateFilter(
            IModel model,
            IEntityType queryEntityType,
            List<LambdaExpression> predicates)
        {
            ParameterExpression qcParam = Expression.Parameter(typeof(QueryContext), "queryContext");
            ParameterExpression stateParam = Expression.Parameter(typeof(JetExecutionMaterializedState), "state");
            ParameterExpression entityObjParam = Expression.Parameter(typeof(object), "entity");
            ParameterExpression typedEntityVar = Expression.Variable(queryEntityType.ClrType, "typedEntity");

            JetCompiledExpressionRewriter rewriter = new(
                qcParam,
                stateParam,
                model,
                new Dictionary<EntityQueryRootExpression, JetTableLoadPlan>(ReferenceEqualityComparer.Instance),
                ignoreQueryFilters: false);

            Expression combinedBody = Expression.Constant(true);
            for (int i = predicates.Count - 1; i >= 0; i--)
            {
                LambdaExpression lambda = predicates[i];
                if (lambda.Parameters.Count != 1)
                {
                    continue;
                }

                var rewrittenLambda = (LambdaExpression)rewriter.Visit(lambda);
                Expression targetArg = rewrittenLambda.Parameters[0].Type == queryEntityType.ClrType
                    ? typedEntityVar
                    : Expression.Convert(typedEntityVar, rewrittenLambda.Parameters[0].Type);
                Expression replaced = new ExpressionReplacer(rewrittenLambda.Parameters[0], targetArg)
                    .Visit(rewrittenLambda.Body)!;
                combinedBody = ReferenceEquals(combinedBody, Expression.Constant(true))
                    ? replaced
                    : Expression.AndAlso(combinedBody, replaced);
            }

            BlockExpression block = Expression.Block(
                [typedEntityVar],
                Expression.Assign(typedEntityVar, Expression.Convert(entityObjParam, queryEntityType.ClrType)),
                combinedBody);
            return Expression.Lambda<Func<QueryContext, JetExecutionMaterializedState, object, bool>>(
                block,
                qcParam,
                stateParam,
                entityObjParam).Compile();
        }

        private static Func<IReadOnlyDictionary<string, object?>, int?> CombineMinLimit(
            Func<IReadOnlyDictionary<string, object?>, int?>? first,
            Func<IReadOnlyDictionary<string, object?>, int?> second)
        {
            if (first is null)
            {
                return second;
            }

            return parameters =>
            {
                int? a = first(parameters);
                int? b = second(parameters);
                if (a is int va && b is int vb)
                {
                    return Math.Min(va, vb);
                }

                return a ?? b;
            };
        }

        private static bool TryBuildIntEvaluator(
            Expression expression,
            out Func<IReadOnlyDictionary<string, object?>, int?>? evaluator)
        {
            Expression unwrapped = StripQuotes(expression);
            if (unwrapped is ConstantExpression { Value: int count } && count >= 0)
            {
                evaluator = _ => count;
                return true;
            }

            if (unwrapped is QueryParameterExpression queryParam && queryParam.Type == typeof(int))
            {
                string paramName = queryParam.Name;
                evaluator = parameters =>
                    parameters.TryGetValue(paramName, out object? val) && val is int n && n >= 0 ? n : null;
                return true;
            }

            evaluator = null;
            return false;
        }

        private static Type UnwrapSequenceType(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>))
            {
                type = type.GetGenericArguments()[0];
            }

            if (type == typeof(string) || type == typeof(byte[]))
            {
                return type;
            }

            if (type.IsArray)
            {
                return type.GetElementType()!;
            }

            if (type.IsGenericType)
            {
                Type genericDef = type.GetGenericTypeDefinition();
                if (genericDef == typeof(IQueryable<>)
                    || genericDef == typeof(IOrderedQueryable<>)
                    || genericDef == typeof(IIncludableQueryable<,>)
                    || genericDef == typeof(IEnumerable<>)
                    || genericDef == typeof(IAsyncEnumerable<>))
                {
                    return type.GetGenericArguments()[0];
                }
            }

            foreach (Type iface in type.GetInterfaces())
            {
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    return iface.GetGenericArguments()[0];
                }
            }

            return type;
        }

        private JetTableLoadPlan GetOrCreateRootPlan(EntityQueryRootExpression queryRoot)
        {
            if (!this.rootPlansByExpression.TryGetValue(queryRoot, out JetTableLoadPlan? plan))
            {
                plan = new JetTableLoadPlan(
                    this.rootTables.Count,
                    queryRoot.EntityType.GetRootType(),
                    queryRoot.EntityType,
                    []);
                this.rootTables.Add(plan);
                this.rootPlansByExpression[queryRoot] = plan;
            }

            return plan;
        }

        private void ExtractIncludeChain(MethodCallExpression outerIncludeCall)
        {
            List<MethodCallExpression> chainCalls = [];
            Expression current = outerIncludeCall;
            while (current is MethodCallExpression call
                   && call.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
                   && call.Method.Name is nameof(EntityFrameworkQueryableExtensions.Include)
                       or nameof(EntityFrameworkQueryableExtensions.ThenInclude)
                       or "NotQuiteInclude")
            {
                chainCalls.Add(call);
                current = call.Arguments[0];
            }

            chainCalls.Reverse();
            _ = this.Visit(current);

            bool previousInInclude = this.inIncludeArgument;
            this.inIncludeArgument = true;
            try
            {
                List<JetIncludeStep> steps = [];
                foreach (MethodCallExpression includeCall in chainCalls)
                {
                    if (includeCall.Arguments.Count < 2)
                    {
                        continue;
                    }

                    Expression navArg = StripQuotes(includeCall.Arguments[1]);
                    if (navArg is LambdaExpression lambda)
                    {
                        IEntityType? sourceType = steps.Count > 0 && steps[^1].Navigation.TargetEntityType.ClrType == lambda.Parameters[0].Type
                            ? steps[^1].Navigation.TargetEntityType
                            : model.FindEntityType(lambda.Parameters[0].Type);
                        if (sourceType is not null
                            && TryExtractIncludeMemberAndFilter(
                                lambda,
                                model,
                                out MemberExpression? memberExpr,
                                out Func<QueryContext, JetExecutionMaterializedState, IEnumerable<object>, IEnumerable<object>>? collectionFilter))
                        {
                            INavigationBase? nav = FindNavigationBase(sourceType, memberExpr!.Member);
                            if (nav is not null)
                            {
                                steps.Add(new JetIncludeStep(nav, collectionFilter));
                            }
                        }
                    }
                    else if (navArg is ConstantExpression { Value: string path }
                             && TryFindRootEntityTypeRecursive(includeCall.Arguments[0], out IEntityType? rootType))
                    {
                        IEntityType currentType = rootType!;
                        foreach (string segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
                        {
                            INavigationBase? nav = FindNavigationBase(currentType, segment);
                            if (nav is null)
                            {
                                break;
                            }

                            steps.Add(new JetIncludeStep(nav, collectionFilter: null));
                            currentType = nav.TargetEntityType;
                        }
                    }
                }

                if (steps.Count > 0)
                {
                    this.includeChains.Add(new JetIncludeChain(steps));
                }
            }
            finally
            {
                this.inIncludeArgument = previousInInclude;
            }
        }

        private void AddAutoIncludes(IEntityType entityType, HashSet<INavigationBase> visited, List<JetIncludeStep> currentPath)
        {
            foreach (INavigation nav in entityType.GetNavigations())
            {
                bool isOwnedForward = !nav.IsOnDependent && nav.TargetEntityType.IsOwned();
                if ((nav.IsEagerLoaded || isOwnedForward) && visited.Add(nav))
                {
                    currentPath.Add(new JetIncludeStep(nav, collectionFilter: null));
                    this.includeChains.Add(new JetIncludeChain([.. currentPath]));
                    this.AddAutoIncludes(nav.TargetEntityType, visited, currentPath);
                    currentPath.RemoveAt(currentPath.Count - 1);
                }
            }

            foreach (ISkipNavigation skipNav in entityType.GetSkipNavigations())
            {
                if (skipNav.IsEagerLoaded && visited.Add(skipNav))
                {
                    currentPath.Add(new JetIncludeStep(skipNav, collectionFilter: null));
                    this.includeChains.Add(new JetIncludeChain([.. currentPath]));
                    this.AddAutoIncludes(skipNav.TargetEntityType, visited, currentPath);
                    currentPath.RemoveAt(currentPath.Count - 1);
                }
            }
        }

        private int GetNavigationHopDepth(MemberExpression node)
        {
            int depth = 0;
            Expression? current = node;
            while (current is MemberExpression member && member.Expression is not null)
            {
                IEntityType? ownerType = model.FindEntityType(member.Expression.Type);
                if (FindNavigationBase(ownerType, member.Member) is null)
                {
                    break;
                }

                depth++;
                current = StripQuotes(member.Expression);
            }

            return depth;
        }
    }

    private sealed class ExpressionReplacer(Expression target, Expression replacement) : ExpressionVisitor
    {
        public override Expression? Visit(Expression? node)
            => ReferenceEquals(node, target) ? replacement : base.Visit(node);
    }

    private sealed class ValueBufferPredicateRewriter(
        ParameterExpression entityParam,
        IEntityType rootEntityType,
        IModel model,
        ParameterExpression queryContextExpression,
        ParameterExpression bufferExpression) : ExpressionVisitor
    {
        public bool CanUseValueBuffer { get; private set; } = true;

        public override Expression? Visit(Expression? node)
        {
            if (!this.CanUseValueBuffer || node is null)
            {
                return node;
            }

            return base.Visit(node);
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is QueryParameterExpression queryParam)
            {
                MethodInfo getParamMethod = GetParameterValueMethod.MakeGenericMethod(queryParam.Type);
                return Expression.Call(
                    getParamMethod,
                    queryContextExpression,
                    Expression.Constant(queryParam.Name));
            }

            this.CanUseValueBuffer = false;
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null)
            {
                List<MemberInfo> chain = [];
                Expression? cursor = node;
                while (cursor is MemberExpression step)
                {
                    chain.Add(step.Member);
                    cursor = step.Expression is not null ? UnwrapParamExpression(step.Expression) : null;
                }

                if (ReferenceEquals(cursor, entityParam) && chain.Count > 0)
                {
                    chain.Reverse();
                    ITypeBase currentType = model.FindEntityType(entityParam.Type) ?? rootEntityType;
                    for (int i = 0; i < chain.Count - 1; i++)
                    {
                        IComplexProperty? complexProp = currentType.FindComplexProperty(chain[i]);
                        if (complexProp?.IsNullable != false)
                        {
                            this.CanUseValueBuffer = false;
                            return node;
                        }

                        currentType = complexProp.ComplexType;
                    }

                    IProperty? prop = currentType.FindProperty(chain[^1]);
                    if (prop?.DeclaringType.ContainingEntityType.IsOwned() == false)
                    {
                        MethodInfo readMethod = ReadValueBufferPropertyMethod.MakeGenericMethod(node.Type);
                        return Expression.Call(
                            readMethod,
                            bufferExpression,
                            Expression.Constant(prop.GetIndex()));
                    }

                    this.CanUseValueBuffer = false;
                    return node;
                }
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(EF)
                && node.Method.Name == nameof(EF.Property)
                && node.Arguments.Count == 2
                && ReferenceEquals(UnwrapParamExpression(node.Arguments[0]), entityParam)
                && UnwrapParamExpression(node.Arguments[1]) is ConstantExpression { Value: string propName })
            {
                IEntityType paramType = model.FindEntityType(entityParam.Type) ?? rootEntityType;
                IProperty? prop = paramType.FindProperty(propName) ?? rootEntityType.FindProperty(propName);
                if (prop?.DeclaringType.ContainingEntityType.IsOwned() == false)
                {
                    MethodInfo readMethod = ReadValueBufferPropertyMethod.MakeGenericMethod(node.Type);
                    return Expression.Call(
                        readMethod,
                        bufferExpression,
                        Expression.Constant(prop.GetIndex()));
                }

                this.CanUseValueBuffer = false;
                return node;
            }

            if (node.Method.DeclaringType == typeof(DbFunctionsExtensions)
                && node.Method.Name == nameof(DbFunctionsExtensions.Like))
            {
                if (node.Arguments.Count == 3)
                {
                    return Expression.Call(
                        EvaluateLikeMethod,
                        this.Visit(node.Arguments[1])!,
                        this.Visit(node.Arguments[2])!,
                        Expression.Constant(null, typeof(string)));
                }

                if (node.Arguments.Count == 4)
                {
                    return Expression.Call(
                        EvaluateLikeMethod,
                        this.Visit(node.Arguments[1])!,
                        this.Visit(node.Arguments[2])!,
                        this.Visit(node.Arguments[3])!);
                }
            }

            if (node.Method.DeclaringType == typeof(Queryable))
            {
                if (node.Method.Name == nameof(Queryable.Contains)
                    && node.Arguments.Count == 2
                    && node.Method.IsGenericMethod)
                {
                    Expression arg0 = this.Visit(node.Arguments[0])!;
                    Expression arg1 = this.Visit(node.Arguments[1])!;
                    Type elemType = node.Method.GetGenericArguments()[0];
                    if (model.FindEntityType(elemType) is null)
                    {
                        MethodInfo enumerableContains = EnumerableStaticMethods.Single(
                            static m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2)
                            .MakeGenericMethod(elemType);
                        return Expression.Call(enumerableContains, arg0, arg1);
                    }
                }

                this.CanUseValueBuffer = false;
                return node;
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (ReferenceEquals(node, entityParam))
            {
                this.CanUseValueBuffer = false;
            }

            return base.VisitParameter(node);
        }

        private static Expression UnwrapParamExpression(Expression expression)
        {
            Expression current = expression;
            while (current is UnaryExpression unary
                   && unary.NodeType is ExpressionType.Quote or ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                current = unary.Operand;
            }

            return current;
        }
    }

    private sealed class RootEntityPropertyUsageAnalyzer(IModel model, IEntityType rootEntityType) : ExpressionVisitor
    {
        public HashSet<IProperty> ReferencedProperties { get; } = [];

        public bool EntityEscapes { get; private set; }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            _ = this.Visit(node.Body);
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null)
            {
                List<MemberInfo> chain = [];
                Expression? cursor = node;
                while (cursor is MemberExpression step)
                {
                    chain.Add(step.Member);
                    cursor = step.Expression is not null ? UnwrapQuotesAndConverts(step.Expression) : null;
                }

                if (cursor is ParameterExpression param
                    && model.FindEntityType(param.Type) is IEntityType paramEntityType
                    && paramEntityType.GetRootType() == rootEntityType
                    && chain.Count > 0)
                {
                    chain.Reverse();
                    ITypeBase currentType = paramEntityType;
                    bool resolvedComplexChain = true;
                    for (int i = 0; i < chain.Count - 1; i++)
                    {
                        IComplexProperty? complexProp = currentType.FindComplexProperty(chain[i]);
                        if (complexProp is null)
                        {
                            resolvedComplexChain = false;
                            break;
                        }

                        currentType = complexProp.ComplexType;
                    }

                    if (resolvedComplexChain)
                    {
                        IProperty? prop = currentType.FindProperty(chain[^1]);
                        if (prop is not null)
                        {
                            _ = this.ReferencedProperties.Add(prop);
                            return node;
                        }

                        IComplexProperty? leafComplex = currentType.FindComplexProperty(chain[^1]);
                        if (leafComplex is not null)
                        {
                            foreach (IProperty complexScalar in leafComplex.ComplexType.GetFlattenedProperties())
                            {
                                _ = this.ReferencedProperties.Add(complexScalar);
                            }

                            return node;
                        }
                    }
                }
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(EF.Property)
                && node.Method.DeclaringType == typeof(EF)
                && node.Arguments.Count == 2
                && UnwrapQuotesAndConverts(node.Arguments[0]) is ParameterExpression param
                && model.FindEntityType(param.Type) is IEntityType paramEntityType
                && paramEntityType.GetRootType() == rootEntityType
                && UnwrapQuotesAndConverts(node.Arguments[1]) is ConstantExpression { Value: string propName })
            {
                IProperty? prop = paramEntityType.FindProperty(propName);
                if (prop is not null)
                {
                    _ = this.ReferencedProperties.Add(prop);
                    return node;
                }
            }

            if (node.Method.DeclaringType == typeof(Queryable)
                && node.Method.Name is nameof(Queryable.GroupBy)
                    or nameof(Queryable.Contains)
                    or nameof(Queryable.SequenceEqual))
            {
                this.EntityEscapes = true;
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (model.FindEntityType(node.Type) is IEntityType paramEntityType
                && paramEntityType.GetRootType() == rootEntityType)
            {
                this.EntityEscapes = true;
            }

            return base.VisitParameter(node);
        }

        private static Expression UnwrapQuotesAndConverts(Expression expression)
        {
            Expression current = expression;
            while (current is UnaryExpression unary
                   && unary.NodeType is ExpressionType.Quote or ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                current = unary.Operand;
            }

            return current;
        }
    }

    private sealed class JetCompiledExpressionRewriter(
        Expression queryContextExpression,
        Expression stateExpression,
        IModel model,
        IReadOnlyDictionary<EntityQueryRootExpression, JetTableLoadPlan> rootPlansByExpression,
        bool ignoreQueryFilters) : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node)
        {
            if (node is EntityQueryRootExpression queryRoot)
            {
                int rootId = rootPlansByExpression.TryGetValue(queryRoot, out JetTableLoadPlan? plan)
                    ? plan.RootId
                    : 0;

                MethodInfo getRootMethod = GetRootEnumerableMethod.MakeGenericMethod(queryRoot.EntityType.ClrType);
                Expression current = Expression.Call(
                    getRootMethod,
                    stateExpression,
                    Expression.Constant(rootId));

                if (!ignoreQueryFilters)
                {
                    foreach (IQueryFilter filter in GetAllQueryFilters(queryRoot.EntityType))
                    {
                        if (filter.Expression is LambdaExpression filterLambda && filterLambda.Parameters.Count == 1)
                        {
                            var visitedFilter = (LambdaExpression)this.Visit(filterLambda);
                            if (visitedFilter.Parameters[0].Type != queryRoot.EntityType.ClrType)
                            {
                                ParameterExpression derivedParam = Expression.Parameter(
                                    queryRoot.EntityType.ClrType,
                                    visitedFilter.Parameters[0].Name);
                                Expression adaptedBody = new ExpressionReplacer(visitedFilter.Parameters[0], derivedParam)
                                    .Visit(visitedFilter.Body)!;
                                visitedFilter = Expression.Lambda(adaptedBody, derivedParam);
                            }

                            current = Expression.Call(
                                typeof(Enumerable),
                                nameof(Enumerable.Where),
                                [queryRoot.EntityType.ClrType],
                                current,
                                visitedFilter);
                        }
                    }
                }

                return current;
            }

            if (node is QueryParameterExpression queryParam)
            {
                MethodInfo getParamMethod = GetParameterValueMethod.MakeGenericMethod(queryParam.Type);
                return Expression.Call(
                    getParamMethod,
                    queryContextExpression,
                    Expression.Constant(queryParam.Name));
            }

            return base.VisitExtension(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
                && node.Method.Name is nameof(EntityFrameworkQueryableExtensions.Include)
                    or nameof(EntityFrameworkQueryableExtensions.ThenInclude)
                    or "NotQuiteInclude"
                    or nameof(EntityFrameworkQueryableExtensions.AsNoTracking)
                    or nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution)
                    or nameof(EntityFrameworkQueryableExtensions.AsTracking)
                    or nameof(EntityFrameworkQueryableExtensions.IgnoreQueryFilters)
                    or nameof(EntityFrameworkQueryableExtensions.IgnoreAutoIncludes)
                    or nameof(EntityFrameworkQueryableExtensions.TagWith)
                    or nameof(EntityFrameworkQueryableExtensions.TagWithCallSite))
            {
                return this.Visit(node.Arguments[0]);
            }

            if (node.Method.DeclaringType == typeof(EF)
                && node.Method.Name == nameof(EF.Property)
                && node.Arguments.Count == 2)
            {
                Expression target = this.Visit(node.Arguments[0]);
                Expression propName = this.Visit(node.Arguments[1]);
                MethodInfo readMethod = ReadEfPropertyMethod.MakeGenericMethod(node.Type);
                MemberExpression entityBuffersExpr = Expression.Property(stateExpression, nameof(JetExecutionMaterializedState.EntityBuffers));
                return Expression.Call(
                    readMethod,
                    queryContextExpression,
                    entityBuffersExpr,
                    Expression.Convert(target, typeof(object)),
                    propName);
            }

            if (node.Method.DeclaringType == typeof(DbFunctionsExtensions)
                && node.Method.Name == nameof(DbFunctionsExtensions.Like))
            {
                if (node.Arguments.Count == 3)
                {
                    return Expression.Call(
                        EvaluateLikeMethod,
                        this.Visit(node.Arguments[1]),
                        this.Visit(node.Arguments[2]),
                        Expression.Constant(null, typeof(string)));
                }

                if (node.Arguments.Count == 4)
                {
                    return Expression.Call(
                        EvaluateLikeMethod,
                        this.Visit(node.Arguments[1]),
                        this.Visit(node.Arguments[2]),
                        this.Visit(node.Arguments[3]));
                }
            }

            if (node.Method.DeclaringType == typeof(Queryable))
            {
                return this.VisitQueryableMethodCall(node);
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType is ExpressionType.Equal
                    or ExpressionType.NotEqual
                    or ExpressionType.LessThan
                    or ExpressionType.LessThanOrEqual
                    or ExpressionType.GreaterThan
                    or ExpressionType.GreaterThanOrEqual
                && (this.IsNullableNavigationValueMember(node.Left) || this.IsNullableNavigationValueMember(node.Right))
                && node.Left.Type.IsValueType
                && Nullable.GetUnderlyingType(node.Left.Type) is null
                && node.Left.Type == node.Right.Type)
            {
                Type liftedType = typeof(Nullable<>).MakeGenericType(node.Left.Type);
                Expression liftedLeft = this.VisitAsLiftedNullable(node.Left, liftedType);
                Expression liftedRight = this.VisitAsLiftedNullable(node.Right, liftedType);
                return Expression.MakeBinary(node.NodeType, liftedLeft, liftedRight);
            }

            return base.VisitBinary(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null)
            {
                Expression visitedTarget = this.Visit(node.Expression);
                if (!visitedTarget.Type.IsValueType
                    && visitedTarget is not ParameterExpression
                    && this.IsNullSafeNavigableReceiverType(visitedTarget.Type))
                {
                    ParameterExpression targetVar = Expression.Variable(visitedTarget.Type, "navTarget");
                    Expression fallback = !node.Type.IsValueType || Nullable.GetUnderlyingType(node.Type) is not null
                        ? Expression.Constant(null, node.Type)
                        : Expression.Default(node.Type);
                    return Expression.Block(
                        [targetVar],
                        Expression.Assign(targetVar, visitedTarget),
                        Expression.Condition(
                            Expression.ReferenceEqual(targetVar, Expression.Constant(null, visitedTarget.Type)),
                            fallback,
                            node.Update(targetVar)));
                }

                return node.Update(visitedTarget);
            }

            return base.VisitMember(node);
        }

        private static Expression StripQuotes(Expression expression)
        {
            Expression current = expression;
            while (current is UnaryExpression { NodeType: ExpressionType.Quote } quote)
            {
                current = quote.Operand;
            }

            return current;
        }

        private static MethodInfo? TryFindEnumerableMethod(string name, Type[] typeArguments, Expression[] arguments)
        {
            foreach (MethodInfo candidate in EnumerableStaticMethods)
            {
                if (!string.Equals(candidate.Name, name, StringComparison.Ordinal))
                {
                    continue;
                }

                ParameterInfo[] parameters = candidate.GetParameters();
                if (parameters.Length != arguments.Length)
                {
                    continue;
                }

                if (candidate.IsGenericMethod != (typeArguments.Length > 0))
                {
                    continue;
                }

                if (candidate.IsGenericMethod && candidate.GetGenericArguments().Length != typeArguments.Length)
                {
                    continue;
                }

                MethodInfo closed;
                try
                {
                    closed = candidate.IsGenericMethod ? candidate.MakeGenericMethod(typeArguments) : candidate;
                }
                catch (ArgumentException)
                {
                    continue;
                }

                ParameterInfo[] closedParams = closed.GetParameters();
                bool matches = true;
                for (int i = 0; i < closedParams.Length; i++)
                {
                    if (!closedParams[i].ParameterType.IsAssignableFrom(arguments[i].Type))
                    {
                        matches = false;
                        break;
                    }
                }

                if (matches)
                {
                    return closed;
                }
            }

            return null;
        }

        private bool IsNullSafeNavigableReceiverType(Type clrType)
        {
            if (model.FindEntityType(clrType) is not null)
            {
                return true;
            }

            foreach (IEntityType entityType in model.GetEntityTypes())
            {
                if (entityType.ClrType == clrType)
                {
                    return true;
                }

                foreach (IComplexProperty complexProp in entityType.GetComplexProperties())
                {
                    if (complexProp.ClrType == clrType)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private bool IsNullableNavigationValueMember(Expression expr)
        {
            Expression current = expr;
            while (current is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary
                   && unary.Type.IsValueType
                   && Nullable.GetUnderlyingType(unary.Type) is null)
            {
                current = unary.Operand;
            }

            return current is MemberExpression { Expression: not null and not ParameterExpression } member
                && !member.Expression.Type.IsValueType
                && this.IsNullSafeNavigableReceiverType(member.Expression.Type);
        }

        private Expression VisitAsLiftedNullable(Expression expr, Type liftedType)
        {
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary
                && unary.Type.IsValueType
                && Nullable.GetUnderlyingType(unary.Type) is null
                && this.IsNullableNavigationValueMember(unary.Operand))
            {
                Type innerLifted = typeof(Nullable<>).MakeGenericType(unary.Operand.Type);
                Expression innerVisited = this.VisitAsLiftedNullable(unary.Operand, innerLifted);
                return innerLifted == liftedType ? innerVisited : Expression.Convert(innerVisited, liftedType);
            }

            if (expr is MemberExpression { Expression: not null and not ParameterExpression } member
                && !member.Expression.Type.IsValueType
                && this.IsNullSafeNavigableReceiverType(member.Expression.Type))
            {
                Expression visitedTarget = this.Visit(member.Expression);
                ParameterExpression targetVar = Expression.Variable(visitedTarget.Type, "navTarget");
                return Expression.Block(
                    [targetVar],
                    Expression.Assign(targetVar, visitedTarget),
                    Expression.Condition(
                        Expression.ReferenceEqual(targetVar, Expression.Constant(null, visitedTarget.Type)),
                        Expression.Constant(null, liftedType),
                        Expression.Convert(member.Update(targetVar), liftedType)));
            }

            return Expression.Convert(this.Visit(expr), liftedType);
        }

        private MethodCallExpression VisitQueryableMethodCall(MethodCallExpression node)
        {
            ParameterInfo[] queryableParams = node.Method.GetParameters();
            var enumerableArgs = new Expression[node.Arguments.Count];
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                Expression arg = node.Arguments[i];
                if (typeof(Expression).IsAssignableFrom(queryableParams[i].ParameterType))
                {
                    enumerableArgs[i] = this.Visit(StripQuotes(arg));
                }
                else
                {
                    enumerableArgs[i] = this.Visit(arg);
                }
            }

            Type[] typeArgs = node.Method.IsGenericMethod ? node.Method.GetGenericArguments() : [];
            if (node.Method.Name == nameof(Queryable.Contains)
                && enumerableArgs.Length == 2
                && typeArgs.Length == 1
                && model.FindEntityType(typeArgs[0]) is not null)
            {
                return Expression.Call(
                    EvaluateContainsMethod.MakeGenericMethod(typeArgs[0]),
                    queryContextExpression,
                    enumerableArgs[0],
                    enumerableArgs[1]);
            }

            MethodInfo? enumerableMethod = TryFindEnumerableMethod(node.Method.Name, typeArgs, enumerableArgs);
            if (enumerableMethod is not null)
            {
                return Expression.Call(enumerableMethod, enumerableArgs);
            }

            var fallbackArgs = new Expression[node.Arguments.Count];
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                Expression visited = enumerableArgs[i];
                Type expectedType = queryableParams[i].ParameterType;
                if (typeof(Expression).IsAssignableFrom(expectedType) && visited is LambdaExpression lambda)
                {
                    fallbackArgs[i] = Expression.Quote(lambda);
                }
                else if (expectedType.IsGenericType
                    && expectedType.GetGenericTypeDefinition() == typeof(IQueryable<>)
                    && !expectedType.IsAssignableFrom(visited.Type))
                {
                    Type elemType = expectedType.GetGenericArguments()[0];
                    fallbackArgs[i] = Expression.Call(
                        AsQueryableGenericMethod.MakeGenericMethod(elemType),
                        visited);
                }
                else
                {
                    fallbackArgs[i] = visited;
                }
            }

            return Expression.Call(node.Method, fallbackArgs);
        }
    }
}
