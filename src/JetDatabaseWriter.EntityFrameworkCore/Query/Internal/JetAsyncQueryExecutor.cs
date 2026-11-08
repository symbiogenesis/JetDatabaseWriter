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
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Compiles and asynchronously executes EF Core LINQ queries over <see cref="JetDatabaseWriter"/> with B-tree index seek pushdown,
/// structural entity materialization, change-tracker identity resolution, and eager navigation loading.
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

    private static readonly MethodInfo ReadEfPropertyMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(ReadEfProperty),
            BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo EvaluateLikeMethod =
        typeof(JetAsyncQueryExecutor).GetMethod(
            nameof(EvaluateLike),
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
    /// <typeparam name="TResult">The query result type (<see cref="IAsyncEnumerable{T}"/> or <see cref="Task{TResult}"/>).</typeparam>
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

        if (!async)
        {
            return static _ => throw new NotSupportedException(
                "Synchronous query execution is not supported by JetDatabaseWriter.EntityFrameworkCore. Use asynchronous EF Core query methods such as ToListAsync, FirstOrDefaultAsync, SingleOrDefaultAsync, CountAsync, or AnyAsync.");
        }

        QueryCompilationContext compilationContext = dependencies.QueryCompilationContextFactory.Create(async: true);
        JetQueryPlan plan = JetQueryPlanExtractor.Analyze(
            query,
            compilationContext,
            dependencies.QueryCompilationContextFactory,
            preprocessorFactory,
            shaperFactory);

        Type resultType = typeof(TResult);
        if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            Type elementType = resultType.GetGenericArguments()[0];
            MethodInfo genericMethod = BuildSequenceExecutorMethod.MakeGenericMethod(elementType);
            return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, compilationContext])!;
        }

        if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(Task<>))
        {
            Type scalarType = resultType.GetGenericArguments()[0];
            MethodInfo genericMethod = BuildScalarExecutorMethod.MakeGenericMethod(scalarType);
            return (Func<QueryContext, TResult>)genericMethod.Invoke(null, [query, plan, compilationContext])!;
        }

        throw new NotSupportedException($"Unsupported asynchronous query result type '{resultType}'.");
    }

    private static Func<QueryContext, IAsyncEnumerable<TElement>> BuildSequenceExecutor<TElement>(
        Expression query,
        JetQueryPlan plan,
        QueryCompilationContext compilationContext)
    {
        Func<QueryContext, JetExecutionMaterializedState, IEnumerable<TElement>> compiledQuery =
            CompileSequenceDelegate<TElement>(query, plan, compilationContext.Model);
        IEntityType? resultEntityType = compilationContext.Model.FindEntityType(typeof(TElement));
        return queryContext => ExecuteSequenceAsync(queryContext, plan, compiledQuery, resultEntityType);
    }

    private static Func<QueryContext, Task<TScalar>> BuildScalarExecutor<TScalar>(
        Expression query,
        JetQueryPlan plan,
        QueryCompilationContext compilationContext)
    {
        Func<QueryContext, JetExecutionMaterializedState, TScalar> compiledQuery =
            CompileScalarDelegate<TScalar>(query, plan, compilationContext.Model);
        IEntityType? resultEntityType = compilationContext.Model.FindEntityType(typeof(TScalar));
        return queryContext => ExecuteScalarAsync(queryContext, plan, compiledQuery, resultEntityType);
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
        IEntityType? resultEntityType,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        CancellationToken effectiveToken = cancellationToken != default ? cancellationToken : queryContext.CancellationToken;
        effectiveToken.ThrowIfCancellationRequested();

        QueryTrackingBehavior trackingBehavior = plan.TrackingBehaviorOverride
            ?? queryContext.Context.ChangeTracker.QueryTrackingBehavior;
        bool isTracking = trackingBehavior == QueryTrackingBehavior.TrackAll && resultEntityType is not null;
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

        List<TElement> results = [.. compiledQuery(queryContext, state)];

        if (resultEntityType is not null && results.Count > 0)
        {
            IEntityType rootResultType = resultEntityType.GetRootType();
            List<(object Entity, ValueBuffer Buffer)> FinalizeResultEntities()
            {
                List<(object Entity, ValueBuffer Buffer)> finalized = [];
                for (int i = 0; i < results.Count; i++)
                {
                    if (results[i] is not object entityObj)
                    {
                        continue;
                    }

                    if (!state.EntityBuffers.TryGetValue(entityObj, out ValueBuffer buffer))
                    {
                        continue;
                    }

                    object effectiveEntity = entityObj;
                    if (isTracking && !state.AlreadyTrackedEntities.Contains(entityObj))
                    {
                        IEntityType concreteType = queryContext.Context.Model.FindEntityType(entityObj.GetType()) ?? rootResultType;
                        effectiveEntity = plan.GetTrackedMaterializer(concreteType)(queryContext, buffer);
                        state.EntityBuffers[effectiveEntity] = buffer;
                        _ = state.AlreadyTrackedEntities.Add(effectiveEntity);
                        results[i] = (TElement)effectiveEntity;
                    }

                    finalized.Add((effectiveEntity, buffer));
                }

                return finalized;
            }

            List<(object Entity, ValueBuffer Buffer)> rootRowsForIncludes = FinalizeResultEntities();
            if (plan.IncludeSpecs.Count > 0 && rootRowsForIncludes.Count > 0)
            {
                await LoadIncludesForResultEntitiesAsync(
                    (JetQueryContext)queryContext,
                    plan,
                    state,
                    rootResultType,
                    rootRowsForIncludes,
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
        Func<QueryContext, JetExecutionMaterializedState, TScalar> compiledQuery,
        IEntityType? resultEntityType)
    {
        CancellationToken effectiveToken = queryContext.CancellationToken;
        effectiveToken.ThrowIfCancellationRequested();

        QueryTrackingBehavior trackingBehavior = plan.TrackingBehaviorOverride
            ?? queryContext.Context.ChangeTracker.QueryTrackingBehavior;
        bool isTracking = trackingBehavior == QueryTrackingBehavior.TrackAll && resultEntityType is not null;
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

        if (resultEntityType is not null && result is object entityObj && state.EntityBuffers.TryGetValue(entityObj, out ValueBuffer buffer))
        {
            IEntityType rootResultType = resultEntityType.GetRootType();
            object effectiveEntity = entityObj;
            if (isTracking && !state.AlreadyTrackedEntities.Contains(entityObj))
            {
                IEntityType concreteType = queryContext.Context.Model.FindEntityType(entityObj.GetType()) ?? rootResultType;
                effectiveEntity = plan.GetTrackedMaterializer(concreteType)(queryContext, buffer);
                state.EntityBuffers[effectiveEntity] = buffer;
                _ = state.AlreadyTrackedEntities.Add(effectiveEntity);
                result = (TScalar)effectiveEntity;
            }

            if (plan.IncludeSpecs.Count > 0)
            {
                await LoadIncludesForResultEntitiesAsync(
                    (JetQueryContext)queryContext,
                    plan,
                    state,
                    rootResultType,
                    [(effectiveEntity, buffer)],
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

        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType = [];
        if (isTracking)
        {
            PopulateExistingTrackedEntities(queryContext, state, existingTrackedByType);
        }

        Dictionary<IEntityType, IReadOnlyList<ValueBuffer>> unindexedBufferCache = [];
        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps = [];

        foreach (JetTableLoadPlan tablePlan in plan.RootTables)
        {
            IEntityType rootEntityType = tablePlan.RootEntityType;
            IReadOnlyList<ValueBuffer> buffers;
            if (tablePlan.SeekConditions.Count == 0 && unindexedBufferCache.TryGetValue(rootEntityType, out IReadOnlyList<ValueBuffer>? cachedBuffers))
            {
                buffers = cachedBuffers;
            }
            else
            {
                buffers = await jetContext.Connection.ReadTableValueBuffersAsync(
                    rootEntityType,
                    tablePlan.SeekConditions,
                    queryContext.Parameters,
                    cancellationToken).ConfigureAwait(false);
                if (tablePlan.SeekConditions.Count == 0)
                {
                    unindexedBufferCache[rootEntityType] = buffers;
                }
            }

            List<(object Entity, ValueBuffer Buffer)> rows = MaterializeCandidateRows(
                queryContext,
                rootEntityType,
                buffers,
                plan,
                state,
                isTracking,
                resolveIdentityUntracked,
                existingTrackedByType,
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

        foreach (INavigation navigation in plan.QueryReferencedNavigations)
        {
            IEntityType declaringRoot = navigation.DeclaringEntityType.GetRootType();
            IEntityType targetRoot = navigation.TargetEntityType.GetRootType();

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
                    isTracking,
                    resolveIdentityUntracked,
                    existingTrackedByType,
                    untrackedIdentityMaps,
                    cancellationToken);
                state.TablesByEntityType[declaringRoot] = declaringRows;
            }

            if (!state.TablesByEntityType.TryGetValue(targetRoot, out List<(object Entity, ValueBuffer Buffer)>? targetRows))
            {
                IReadOnlyList<ValueBuffer> targetBuffers = await LoadRelatedValueBuffersAsync(
                    jetContext,
                    navigation,
                    declaringRows,
                    cancellationToken).ConfigureAwait(false);
                targetRows = MaterializeCandidateRows(
                    queryContext,
                    targetRoot,
                    targetBuffers,
                    plan,
                    state,
                    isTracking,
                    resolveIdentityUntracked,
                    existingTrackedByType,
                    untrackedIdentityMaps,
                    cancellationToken);
                if (!plan.IgnoreQueryFilters)
                {
                    targetRows = ApplyEntityQueryFilters(queryContext, state, targetRoot, targetRows);
                }

                state.TablesByEntityType[targetRoot] = targetRows;
            }

            FixupNavigation(
                queryContext,
                navigation,
                declaringRows,
                targetRows,
                collectionTransform: null,
                markNavigationLoaded: false);
        }

        return state;
    }

    private static async ValueTask LoadIncludesForResultEntitiesAsync(
        JetQueryContext queryContext,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        IEntityType rootResultType,
        List<(object Entity, ValueBuffer Buffer)> resultRows,
        bool isTracking,
        bool resolveIdentityUntracked,
        CancellationToken cancellationToken)
    {
        Dictionary<IEntityType, List<(object Entity, ValueBuffer Buffer)>> activeParentsByType = [];
        activeParentsByType[rootResultType] = resultRows;

        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType = [];
        if (isTracking)
        {
            PopulateExistingTrackedEntities(queryContext, state, existingTrackedByType);
        }

        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps = [];

        foreach (JetIncludeSpec includeSpec in plan.IncludeSpecs)
        {
            INavigation navigation = includeSpec.Navigation;
            IEntityType declaringRoot = navigation.DeclaringEntityType.GetRootType();
            IEntityType targetRoot = navigation.TargetEntityType.GetRootType();

            if (!activeParentsByType.TryGetValue(declaringRoot, out List<(object Entity, ValueBuffer Buffer)>? declaringRows)
                || declaringRows.Count == 0)
            {
                continue;
            }

            IReadOnlyList<ValueBuffer> targetBuffers = await LoadRelatedValueBuffersAsync(
                queryContext,
                navigation,
                declaringRows,
                cancellationToken).ConfigureAwait(false);

            List<(object Entity, ValueBuffer Buffer)> candidateTargetRows = MaterializeCandidateRows(
                queryContext,
                targetRoot,
                targetBuffers,
                plan,
                state,
                isTracking,
                resolveIdentityUntracked,
                existingTrackedByType,
                untrackedIdentityMaps,
                cancellationToken);

            if (!plan.IgnoreQueryFilters)
            {
                candidateTargetRows = ApplyEntityQueryFilters(queryContext, state, targetRoot, candidateTargetRows);
            }

            List<(object Entity, ValueBuffer Buffer)> matchedTargetRows = FixupNavigationAndPromoteMatchedTargets(
                queryContext,
                navigation,
                declaringRows,
                candidateTargetRows,
                includeSpec.CollectionFilter,
                plan,
                state,
                isTracking);

            if (!activeParentsByType.TryGetValue(targetRoot, out List<(object Entity, ValueBuffer Buffer)>? existingTargetRows))
            {
                activeParentsByType[targetRoot] = matchedTargetRows;
            }
            else
            {
                HashSet<object> seen = [with(ReferenceEqualityComparer.Instance), .. existingTargetRows.Select(static r => r.Entity)];
                foreach ((object Entity, ValueBuffer Buffer) row in matchedTargetRows)
                {
                    if (seen.Add(row.Entity))
                    {
                        existingTargetRows.Add(row);
                    }
                }
            }
        }
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

    private static void PopulateExistingTrackedEntities(
        QueryContext queryContext,
        JetExecutionMaterializedState state,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType)
    {
        foreach (EntityEntry entry in queryContext.Context.ChangeTracker.Entries())
        {
            if (entry.State == EntityState.Detached || entry.Entity is null)
            {
                continue;
            }

            IEntityType rootType = entry.Metadata.GetRootType();
            IKey? pk = rootType.FindPrimaryKey();
            if (pk is null)
            {
                continue;
            }

            object?[] keyVals = new object?[pk.Properties.Count];
            bool allNonNull = true;
            for (int i = 0; i < pk.Properties.Count; i++)
            {
                object? val = entry.Property(pk.Properties[i].Name).CurrentValue;
                if (val is null)
                {
                    allNonNull = false;
                    break;
                }

                keyVals[i] = val;
            }

            if (!allNonNull)
            {
                continue;
            }

            List<IProperty> props = [.. entry.Metadata.GetProperties()];
            int bufLen = props.Count;
            for (int i = 0; i < props.Count; i++)
            {
                int idx = props[i].GetIndex();
                if (idx + 1 > bufLen)
                {
                    bufLen = idx + 1;
                }
            }

            object?[] rawBuf = new object?[bufLen];
            for (int i = 0; i < props.Count; i++)
            {
                int idx = props[i].GetIndex();
                if ((uint)idx < (uint)rawBuf.Length)
                {
                    rawBuf[idx] = entry.Property(props[i].Name).CurrentValue;
                }
            }

            ValueBuffer vb = new(rawBuf);
            state.EntityBuffers[entry.Entity] = vb;
            _ = state.AlreadyTrackedEntities.Add(entry.Entity);

            if (!existingTrackedByType.TryGetValue(rootType, out Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>? map))
            {
                map = [];
                existingTrackedByType[rootType] = map;
            }

            map[new StructuralKey(keyVals)] = (entry.Entity, vb);
        }
    }

    private static List<(object Entity, ValueBuffer Buffer)> MaterializeCandidateRows(
        QueryContext queryContext,
        IEntityType rootEntityType,
        IReadOnlyList<ValueBuffer> buffers,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        bool isTracking,
        bool resolveIdentityUntracked,
        Dictionary<IEntityType, Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>> existingTrackedByType,
        Dictionary<IEntityType, Dictionary<StructuralKey, object>> untrackedIdentityMaps,
        CancellationToken cancellationToken)
    {
        Func<QueryContext, ValueBuffer, object> untrackedMaterializer = plan.GetUntrackedMaterializer(rootEntityType);
        IKey? primaryKey = rootEntityType.FindPrimaryKey();

        Dictionary<StructuralKey, (object Entity, ValueBuffer Buffer)>? trackedMap = null;
        if (isTracking && primaryKey is not null)
        {
            _ = existingTrackedByType.TryGetValue(rootEntityType, out trackedMap);
        }

        Dictionary<StructuralKey, object>? untrackedMap = null;
        if ((resolveIdentityUntracked || isTracking)
            && primaryKey is not null
            && !untrackedIdentityMaps.TryGetValue(rootEntityType, out untrackedMap))
        {
            untrackedMap = [];
            untrackedIdentityMaps[rootEntityType] = untrackedMap;
        }

        List<(object Entity, ValueBuffer Buffer)> loadedRows = [];
        foreach (ValueBuffer buffer in buffers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object entity;
            if (primaryKey is not null)
            {
                StructuralKey key = ExtractKey(primaryKey, buffer);
                if (trackedMap is not null && trackedMap.TryGetValue(key, out (object Entity, ValueBuffer Buffer) trackedEntry))
                {
                    entity = trackedEntry.Entity;
                }
                else if (untrackedMap is not null)
                {
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
            }
            else
            {
                entity = untrackedMaterializer(queryContext, buffer);
            }

            state.EntityBuffers[entity] = buffer;
            loadedRows.Add((entity, buffer));
        }

        return loadedRows;
    }

    private static List<(object Entity, ValueBuffer Buffer)> ApplyEntityQueryFilters(
        QueryContext queryContext,
        JetExecutionMaterializedState state,
        IEntityType entityType,
        List<(object Entity, ValueBuffer Buffer)> rows)
    {
        if (rows.Count == 0)
        {
            return rows;
        }

        IReadOnlyCollection<IQueryFilter> declaredFilters = entityType.GetDeclaredQueryFilters();
        if (declaredFilters.Count == 0)
        {
            return rows;
        }

        List<Func<object, bool>> predicates = [];
        foreach (IQueryFilter filter in declaredFilters)
        {
            if (filter.Expression is not LambdaExpression filterLambda || filterLambda.Parameters.Count != 1)
            {
                continue;
            }

            ConstantExpression queryContextParam = Expression.Constant(queryContext);
            ConstantExpression stateParam = Expression.Constant(state);
            var rewrittenFilter = (LambdaExpression)new JetCompiledExpressionRewriter(
                queryContextParam,
                stateParam,
                queryContext.Context.Model,
                new Dictionary<EntityQueryRootExpression, JetTableLoadPlan>(ReferenceEqualityComparer.Instance),
                ignoreQueryFilters: false).Visit(filterLambda);

            ParameterExpression objParam = Expression.Parameter(typeof(object), "entity");
            InvocationExpression invoke = Expression.Invoke(
                rewrittenFilter,
                Expression.Convert(objParam, filterLambda.Parameters[0].Type));
            predicates.Add(Expression.Lambda<Func<object, bool>>(invoke, objParam).Compile());
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
                if (!predicates[i](row.Entity))
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
        Func<QueryContext, IEnumerable<object>, IEnumerable<object>>? collectionFilter,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        bool isTracking)
    {
        IEntityType targetRoot = navigation.TargetEntityType.GetRootType();

        object PromoteTargetIfNeeded(object targetEntity, ValueBuffer targetBuffer)
        {
            if (!isTracking || state.AlreadyTrackedEntities.Contains(targetEntity))
            {
                return targetEntity;
            }

            IEntityType concreteType = queryContext.Context.Model.FindEntityType(targetEntity.GetType()) ?? targetRoot;
            object tracked = plan.GetTrackedMaterializer(concreteType)(queryContext, targetBuffer);
            state.EntityBuffers[tracked] = targetBuffer;
            _ = state.AlreadyTrackedEntities.Add(tracked);
            return tracked;
        }

        if (navigation.IsCollection)
        {
            IClrCollectionAccessor? collectionAccessor = navigation.GetCollectionAccessor();
            foreach ((object declaringEntity, _) in declaringRows)
            {
                _ = collectionAccessor?.GetOrCreate(declaringEntity, forMaterialization: true);
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
                    foreach (object survivingChild in collectionFilter(queryContext, rawChildren))
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

    private static void FixupNavigation(
        QueryContext queryContext,
        INavigation navigation,
        List<(object Entity, ValueBuffer Buffer)> declaringRows,
        List<(object Entity, ValueBuffer Buffer)> targetRows,
        Func<QueryContext, IEnumerable<object>, IEnumerable<object>>? collectionTransform,
        bool markNavigationLoaded)
        => _ = FixupNavigationAndPromoteMatchedTargets(
            queryContext,
            navigation,
            declaringRows,
            targetRows,
            collectionTransform,
            plan: null!,
            state: null!,
            isTracking: markNavigationLoaded);

    private static void SetNavigationValue(INavigation navigation, object entity, object? value)
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

        return (TParam)Metadata.Internal.JetEntityTypeMetadata.CoerceToType(raw, typeof(TParam))!;
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

    private sealed class JetTableLoadPlan(int rootId, IEntityType rootEntityType, IEntityType queryEntityType, List<JetIndexSeekCondition> seekConditions)
    {
        public int RootId { get; } = rootId;

        public IEntityType RootEntityType { get; } = rootEntityType;

        public IEntityType QueryEntityType { get; } = queryEntityType;

        public List<JetIndexSeekCondition> SeekConditions { get; } = seekConditions;
    }

    private sealed class JetIncludeSpec(
        INavigation navigation,
        Func<QueryContext, IEnumerable<object>, IEnumerable<object>>? collectionFilter)
    {
        public INavigation Navigation { get; } = navigation;

        public Func<QueryContext, IEnumerable<object>, IEnumerable<object>>? CollectionFilter { get; set; } = collectionFilter;
    }

    private sealed class JetQueryPlan(
        List<JetTableLoadPlan> rootTables,
        Dictionary<EntityQueryRootExpression, JetTableLoadPlan> rootPlansByExpression,
        List<JetIncludeSpec> includeSpecs,
        List<INavigation> queryReferencedNavigations,
        QueryTrackingBehavior? trackingBehaviorOverride,
        bool ignoreQueryFilters,
        IQueryCompilationContextFactory compilationContextFactory,
        IQueryTranslationPreprocessorFactory preprocessorFactory,
        IShapedQueryCompilingExpressionVisitorFactory shaperFactory)
    {
        private readonly Dictionary<IEntityType, Func<QueryContext, ValueBuffer, object>> trackedMaterializers = [];
        private readonly Dictionary<IEntityType, Func<QueryContext, ValueBuffer, object>> untrackedMaterializers = [];

        public List<JetTableLoadPlan> RootTables { get; } = rootTables;

        public Dictionary<EntityQueryRootExpression, JetTableLoadPlan> RootPlansByExpression { get; } = rootPlansByExpression;

        public List<JetIncludeSpec> IncludeSpecs { get; } = includeSpecs;

        public List<INavigation> QueryReferencedNavigations { get; } = queryReferencedNavigations;

        public QueryTrackingBehavior? TrackingBehaviorOverride { get; } = trackingBehaviorOverride;

        public bool IgnoreQueryFilters { get; } = ignoreQueryFilters;

        public Func<QueryContext, ValueBuffer, object> GetTrackedMaterializer(IEntityType entityType)
        {
            lock (this.trackedMaterializers)
            {
                if (!this.trackedMaterializers.TryGetValue(entityType, out Func<QueryContext, ValueBuffer, object>? materializer))
                {
                    JetShapedQueryCompilingExpressionVisitor visitor = this.CreateShaperVisitor(entityType, trackAll: true);
                    materializer = visitor.CreateEntityMaterializer(entityType);
                    this.trackedMaterializers[entityType] = materializer;
                }

                return materializer;
            }
        }

        public Func<QueryContext, ValueBuffer, object> GetUntrackedMaterializer(IEntityType entityType)
        {
            lock (this.untrackedMaterializers)
            {
                if (!this.untrackedMaterializers.TryGetValue(entityType, out Func<QueryContext, ValueBuffer, object>? materializer))
                {
                    JetShapedQueryCompilingExpressionVisitor visitor = this.CreateShaperVisitor(entityType, trackAll: false);
                    materializer = visitor.CreateEntityMaterializer(entityType);
                    this.untrackedMaterializers[entityType] = materializer;
                }

                return materializer;
            }
        }

        private JetShapedQueryCompilingExpressionVisitor CreateShaperVisitor(IEntityType entityType, bool trackAll)
        {
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

    private sealed class JetExecutionMaterializedState
    {
        public Dictionary<int, List<(object Entity, ValueBuffer Buffer)>> RootRows { get; } = [];

        public Dictionary<IEntityType, List<(object Entity, ValueBuffer Buffer)>> TablesByEntityType { get; } = [];

        public Dictionary<object, ValueBuffer> EntityBuffers { get; } = [with(ReferenceEqualityComparer.Instance)];

        public HashSet<object> AlreadyTrackedEntities { get; } = [with(ReferenceEqualityComparer.Instance)];
    }

    private sealed class JetQueryPlanExtractor(IModel model) : ExpressionVisitor
    {
        private readonly List<JetTableLoadPlan> rootTables = [];
        private readonly Dictionary<EntityQueryRootExpression, JetTableLoadPlan> rootPlansByExpression = [with(ReferenceEqualityComparer.Instance)];
        private readonly List<JetIncludeSpec> includeSpecs = [];
        private readonly Dictionary<INavigation, JetIncludeSpec> includeByNavigation = [];
        private readonly List<INavigation> queryReferencedNavigations = [];
        private readonly HashSet<INavigation> queryReferencedNavigationSet = [];
        private bool ignoreAutoIncludes;
        private bool inIncludeArgument;

        public QueryTrackingBehavior? TrackingBehaviorOverride { get; private set; }

        public bool IgnoreQueryFilters { get; private set; }

        public static JetQueryPlan Analyze(
            Expression query,
            QueryCompilationContext compilationContext,
            IQueryCompilationContextFactory compilationContextFactory,
            IQueryTranslationPreprocessorFactory preprocessorFactory,
            IShapedQueryCompilingExpressionVisitorFactory shaperFactory)
        {
            JetQueryPlanExtractor extractor = new(compilationContext.Model);
            _ = extractor.Visit(query);

            if (!extractor.ignoreAutoIncludes)
            {
                foreach (JetTableLoadPlan tablePlan in extractor.rootTables)
                {
                    extractor.AddAutoIncludes(tablePlan.QueryEntityType, []);
                }
            }

            if (!extractor.IgnoreQueryFilters)
            {
                foreach (JetTableLoadPlan tablePlan in extractor.rootTables)
                {
                    foreach (IQueryFilter filter in tablePlan.QueryEntityType.GetDeclaredQueryFilters())
                    {
                        if (filter.Expression is LambdaExpression filterLambda)
                        {
                            tablePlan.SeekConditions.AddRange(
                                JetIndexSeekPlanner.ExtractSeekConditions(tablePlan.RootEntityType, filterLambda));
                        }
                    }
                }
            }

            return new JetQueryPlan(
                extractor.rootTables,
                extractor.rootPlansByExpression,
                extractor.includeSpecs,
                extractor.queryReferencedNavigations,
                extractor.TrackingBehaviorOverride,
                extractor.IgnoreQueryFilters,
                compilationContextFactory,
                preprocessorFactory,
                shaperFactory);
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
                         or nameof(EntityFrameworkQueryableExtensions.ThenInclude))
                {
                    _ = this.Visit(node.Arguments[0]);
                    bool previousInInclude = this.inIncludeArgument;
                    this.inIncludeArgument = true;
                    try
                    {
                        this.ExtractIncludeNavigations(node);
                    }
                    finally
                    {
                        this.inIncludeArgument = previousInInclude;
                    }

                    return node;
                }
            }
            else if (node.Method.DeclaringType == typeof(Queryable))
            {
                if (node.Method.Name is nameof(Queryable.Where)
                    or nameof(Queryable.First)
                    or nameof(Queryable.FirstOrDefault)
                    or nameof(Queryable.Single)
                    or nameof(Queryable.SingleOrDefault)
                    && node.Arguments.Count == 2
                    && StripQuotes(node.Arguments[1]) is LambdaExpression predicate
                    && TryFindDirectRootExpression(node.Arguments[0], out EntityQueryRootExpression? queryRoot))
                {
                    _ = this.Visit(node.Arguments[0]);
                    JetTableLoadPlan plan = this.GetOrCreateRootPlan(queryRoot!);
                    plan.SeekConditions.AddRange(
                        JetIndexSeekPlanner.ExtractSeekConditions(plan.RootEntityType, predicate));
                    _ = this.Visit(predicate);
                    return node;
                }
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (!this.inIncludeArgument && node.Expression is not null)
            {
                IEntityType? ownerType = model.FindEntityType(node.Expression.Type);
                INavigation? nav = ownerType?.FindNavigation(node.Member);
                if (nav is not null && this.queryReferencedNavigationSet.Add(nav))
                {
                    this.queryReferencedNavigations.Add(nav);
                }
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
            out Func<QueryContext, IEnumerable<object>, IEnumerable<object>>? collectionFilter)
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
            INavigation? nav = ownerType?.FindNavigation(member.Member);
            if (nav is not { IsCollection: true })
            {
                return true;
            }

            Type elementType = nav.TargetEntityType.ClrType;
            ParameterExpression queryContextParam = Expression.Parameter(typeof(QueryContext), "queryContext");
            ParameterExpression rawCollectionParam = Expression.Parameter(typeof(IEnumerable<object>), "rawItems");
            MethodCallExpression castCall = Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Cast),
                [elementType],
                rawCollectionParam);

            Expression replacedBody = new ExpressionReplacer(member, castCall).Visit(lambda.Body)!;
            ParameterExpression dummyStateParam = Expression.Parameter(typeof(JetExecutionMaterializedState), "state");
            Expression rewrittenBody = new JetCompiledExpressionRewriter(
                queryContextParam,
                dummyStateParam,
                model,
                new Dictionary<EntityQueryRootExpression, JetTableLoadPlan>(ReferenceEqualityComparer.Instance),
                ignoreQueryFilters: true).Visit(replacedBody);

            MethodCallExpression boxResult = Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Cast),
                [typeof(object)],
                rewrittenBody);
            collectionFilter = Expression.Lambda<Func<QueryContext, IEnumerable<object>, IEnumerable<object>>>(
                boxResult,
                queryContextParam,
                rawCollectionParam).Compile();
            return true;
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

        private void ExtractIncludeNavigations(MethodCallExpression includeCall)
        {
            if (includeCall.Arguments.Count < 2)
            {
                return;
            }

            Expression navArg = StripQuotes(includeCall.Arguments[1]);
            if (navArg is LambdaExpression lambda)
            {
                IEntityType? sourceType = model.FindEntityType(lambda.Parameters[0].Type);
                if (sourceType is not null
                    && TryExtractIncludeMemberAndFilter(
                        lambda,
                        model,
                        out MemberExpression? memberExpr,
                        out Func<QueryContext, IEnumerable<object>, IEnumerable<object>>? collectionFilter))
                {
                    INavigation? nav = sourceType.FindNavigation(memberExpr!.Member);
                    if (nav is not null)
                    {
                        this.AddIncludeNavigation(nav, collectionFilter);
                    }
                }
            }
            else if (navArg is ConstantExpression { Value: string path }
                     && TryFindRootEntityTypeRecursive(includeCall.Arguments[0], out IEntityType? rootType))
            {
                IEntityType currentType = rootType!;
                foreach (string segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
                {
                    INavigation? nav = currentType.FindNavigation(segment);
                    if (nav is null)
                    {
                        break;
                    }

                    this.AddIncludeNavigation(nav, collectionFilter: null);
                    currentType = nav.TargetEntityType;
                }
            }
        }

        private void AddAutoIncludes(IEntityType entityType, HashSet<INavigation> visited)
        {
            foreach (INavigation nav in entityType.GetNavigations())
            {
                if (nav.IsEagerLoaded && visited.Add(nav))
                {
                    this.AddIncludeNavigation(nav, collectionFilter: null);
                    this.AddAutoIncludes(nav.TargetEntityType, visited);
                }
            }
        }

        private void AddIncludeNavigation(
            INavigation navigation,
            Func<QueryContext, IEnumerable<object>, IEnumerable<object>>? collectionFilter)
        {
            if (!this.includeByNavigation.TryGetValue(navigation, out JetIncludeSpec? spec))
            {
                spec = new JetIncludeSpec(navigation, collectionFilter);
                this.includeByNavigation[navigation] = spec;
                this.includeSpecs.Add(spec);
            }
            else if (collectionFilter is not null)
            {
                spec.CollectionFilter = collectionFilter;
            }
        }
    }

    private sealed class ExpressionReplacer(Expression target, Expression replacement) : ExpressionVisitor
    {
        public override Expression? Visit(Expression? node)
            => ReferenceEquals(node, target) ? replacement : base.Visit(node);
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
                    foreach (IQueryFilter filter in queryRoot.EntityType.GetDeclaredQueryFilters())
                    {
                        if (filter.Expression is LambdaExpression filterLambda)
                        {
                            var visitedFilter = (LambdaExpression)this.Visit(filterLambda);
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

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null)
            {
                Expression visitedTarget = this.Visit(node.Expression);
                if (!visitedTarget.Type.IsValueType
                    && model.FindEntityType(visitedTarget.Type) is not null
                    && (!node.Type.IsValueType || Nullable.GetUnderlyingType(node.Type) is not null))
                {
                    return Expression.Condition(
                        Expression.Equal(visitedTarget, Expression.Constant(null, visitedTarget.Type)),
                        Expression.Constant(null, node.Type),
                        node.Update(visitedTarget));
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
