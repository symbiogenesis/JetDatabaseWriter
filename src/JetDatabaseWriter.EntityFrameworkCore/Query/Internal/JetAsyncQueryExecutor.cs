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
        => queryContext => ExecuteSequenceAsync<TElement>(queryContext, query, plan, compilationContext);

    private static Func<QueryContext, Task<TScalar>> BuildScalarExecutor<TScalar>(
        Expression query,
        JetQueryPlan plan,
        QueryCompilationContext compilationContext)
        => queryContext => ExecuteScalarAsync<TScalar>(queryContext, query, plan, compilationContext);

    private static async IAsyncEnumerable<TElement> ExecuteSequenceAsync<TElement>(
        QueryContext queryContext,
        Expression query,
        JetQueryPlan plan,
        QueryCompilationContext compilationContext,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        CancellationToken effectiveToken = cancellationToken != default ? cancellationToken : queryContext.CancellationToken;
        effectiveToken.ThrowIfCancellationRequested();

        JetExecutionMaterializedState state = await LoadTablesAndNavigationsAsync(
            queryContext,
            plan,
            effectiveToken).ConfigureAwait(false);

        Expression rewritten = new JetRuntimeExpressionRewriter(
            queryContext,
            compilationContext.Model,
            state,
            plan.IgnoreQueryFilters).Visit(query);

        foreach (TElement element in EvaluateSequenceExpression<TElement>(rewritten))
        {
            effectiveToken.ThrowIfCancellationRequested();
            yield return element;
        }
    }

    private static async Task<TScalar> ExecuteScalarAsync<TScalar>(
        QueryContext queryContext,
        Expression query,
        JetQueryPlan plan,
        QueryCompilationContext compilationContext)
    {
        CancellationToken effectiveToken = queryContext.CancellationToken;
        effectiveToken.ThrowIfCancellationRequested();

        JetExecutionMaterializedState state = await LoadTablesAndNavigationsAsync(
            queryContext,
            plan,
            effectiveToken).ConfigureAwait(false);

        Expression rewritten = new JetRuntimeExpressionRewriter(
            queryContext,
            compilationContext.Model,
            state,
            plan.IgnoreQueryFilters).Visit(query);

        var lambda = Expression.Lambda<Func<TScalar>>(
            rewritten.Type == typeof(TScalar) ? rewritten : Expression.Convert(rewritten, typeof(TScalar)));
        Func<TScalar> compiled = lambda.Compile();
        return compiled();
    }

    private static IEnumerable<TElement> EvaluateSequenceExpression<TElement>(Expression expression)
    {
        if (typeof(IEnumerable<TElement>).IsAssignableFrom(expression.Type))
        {
            var lambda = Expression.Lambda<Func<IEnumerable<TElement>>>(
                Expression.Convert(expression, typeof(IEnumerable<TElement>)));
            return lambda.Compile()();
        }

        var objectLambda = Expression.Lambda<Func<object?>>(
            Expression.Convert(expression, typeof(object)));
        object? raw = objectLambda.Compile()();
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

    private static async ValueTask<JetExecutionMaterializedState> LoadTablesAndNavigationsAsync(
        QueryContext queryContext,
        JetQueryPlan plan,
        CancellationToken cancellationToken)
    {
        var jetContext = (JetQueryContext)queryContext;
        QueryTrackingBehavior trackingBehavior = plan.TrackingBehaviorOverride
            ?? queryContext.Context.ChangeTracker.QueryTrackingBehavior;
        bool isTracking = trackingBehavior == QueryTrackingBehavior.TrackAll;
        bool resolveIdentityUntracked = trackingBehavior == QueryTrackingBehavior.NoTrackingWithIdentityResolution;

        if (isTracking)
        {
            queryContext.InitializeStateManager(standAlone: false);
        }

        JetExecutionMaterializedState state = new();

        foreach (JetTableLoadPlan tablePlan in plan.RootTables)
        {
            await LoadEntityTableAsync(
                jetContext,
                tablePlan.EntityType,
                tablePlan.SeekConditions,
                plan,
                state,
                isTracking,
                resolveIdentityUntracked,
                cancellationToken).ConfigureAwait(false);
        }

        foreach (INavigation navigation in plan.NavigationsToLoad)
        {
            IEntityType declaringRoot = navigation.DeclaringEntityType.GetRootType();
            IEntityType targetRoot = navigation.TargetEntityType.GetRootType();

            if (!state.Tables.ContainsKey(declaringRoot))
            {
                await LoadEntityTableAsync(
                    jetContext,
                    declaringRoot,
                    seekConditions: null,
                    plan,
                    state,
                    isTracking,
                    resolveIdentityUntracked,
                    cancellationToken).ConfigureAwait(false);
            }

            if (!state.Tables.ContainsKey(targetRoot))
            {
                await LoadEntityTableAsync(
                    jetContext,
                    targetRoot,
                    seekConditions: null,
                    plan,
                    state,
                    isTracking,
                    resolveIdentityUntracked,
                    cancellationToken).ConfigureAwait(false);
            }

            FixupNavigation(
                queryContext,
                navigation,
                state.Tables[declaringRoot],
                state.Tables[targetRoot],
                isTracking);
        }

        return state;
    }

    private static async ValueTask LoadEntityTableAsync(
        JetQueryContext queryContext,
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        JetQueryPlan plan,
        JetExecutionMaterializedState state,
        bool isTracking,
        bool resolveIdentityUntracked,
        CancellationToken cancellationToken)
    {
        IEntityType rootEntityType = entityType.GetRootType();
        if (state.Tables.ContainsKey(rootEntityType))
        {
            return;
        }

        IReadOnlyList<ValueBuffer> buffers = await queryContext.Connection.ReadTableValueBuffersAsync(
            rootEntityType,
            seekConditions,
            queryContext.Parameters,
            cancellationToken).ConfigureAwait(false);

        Func<QueryContext, ValueBuffer, object> materializer = isTracking
            ? plan.GetTrackedMaterializer(rootEntityType)
            : plan.GetUntrackedMaterializer(rootEntityType);

        IKey? primaryKey = rootEntityType.FindPrimaryKey();
        Dictionary<StructuralKey, object>? untrackedIdentityMap = resolveIdentityUntracked && primaryKey is not null
            ? []
            : null;

        List<(object Entity, ValueBuffer Buffer)> loadedRows = [];
        foreach (ValueBuffer buffer in buffers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object entity;
            if (untrackedIdentityMap is not null && primaryKey is not null)
            {
                StructuralKey key = ExtractKey(primaryKey, buffer);
                if (!untrackedIdentityMap.TryGetValue(key, out object? existing))
                {
                    existing = materializer(queryContext, buffer);
                    untrackedIdentityMap[key] = existing;
                }

                entity = existing;
            }
            else
            {
                entity = materializer(queryContext, buffer);
            }

            state.EntityBuffers[entity] = buffer;
            loadedRows.Add((entity, buffer));
        }

        state.Tables[rootEntityType] = loadedRows;
    }

    private static void FixupNavigation(
        QueryContext queryContext,
        INavigation navigation,
        List<(object Entity, ValueBuffer Buffer)> declaringRows,
        List<(object Entity, ValueBuffer Buffer)> targetRows,
        bool isTracking)
    {
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
            return;
        }

        if (navigation.IsOnDependent)
        {
            Dictionary<StructuralKey, object> principalByKey = [];
            foreach ((object targetEntity, ValueBuffer targetBuffer) in targetRows)
            {
                if (TryExtractNonNullKey(pkProperties, targetBuffer, out StructuralKey pk))
                {
                    principalByKey[pk] = targetEntity;
                }
            }

            INavigation? inverse = navigation.Inverse;
            IClrCollectionAccessor? inverseCollection = inverse is { IsCollection: true } ? inverse.GetCollectionAccessor() : null;

            foreach ((object dependentEntity, ValueBuffer dependentBuffer) in declaringRows)
            {
                if (!TryExtractNonNullKey(fkProperties, dependentBuffer, out StructuralKey fk)
                    || !principalByKey.TryGetValue(fk, out object? principalEntity))
                {
                    continue;
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

            IClrCollectionAccessor? collectionAccessor = navigation.IsCollection ? navigation.GetCollectionAccessor() : null;
            INavigation? inverse = navigation.Inverse;

            foreach ((object dependentEntity, ValueBuffer dependentBuffer) in targetRows)
            {
                if (!TryExtractNonNullKey(fkProperties, dependentBuffer, out StructuralKey fk)
                    || !principalByKey.TryGetValue(fk, out object? principalEntity))
                {
                    continue;
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
        private readonly object?[] values = values;

        public bool Equals(StructuralKey other)
        {
            if (this.values.Length != other.values.Length)
            {
                return false;
            }

            for (int i = 0; i < this.values.Length; i++)
            {
                if (!Equals(this.values[i], other.values[i]))
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
            foreach (object? val in this.values)
            {
                hash.Add(val);
            }

            return hash.ToHashCode();
        }
    }

    private sealed class JetTableLoadPlan(IEntityType entityType, List<JetIndexSeekCondition> seekConditions)
    {
        public IEntityType EntityType { get; } = entityType;

        public List<JetIndexSeekCondition> SeekConditions { get; } = seekConditions;
    }

    private sealed class JetQueryPlan(
        List<JetTableLoadPlan> rootTables,
        List<INavigation> navigationsToLoad,
        QueryTrackingBehavior? trackingBehaviorOverride,
        bool ignoreQueryFilters,
        IQueryCompilationContextFactory compilationContextFactory,
        IQueryTranslationPreprocessorFactory preprocessorFactory,
        IShapedQueryCompilingExpressionVisitorFactory shaperFactory)
    {
        private readonly Dictionary<IEntityType, Func<QueryContext, ValueBuffer, object>> trackedMaterializers = [];
        private readonly Dictionary<IEntityType, Func<QueryContext, ValueBuffer, object>> untrackedMaterializers = [];

        public List<JetTableLoadPlan> RootTables { get; } = rootTables;

        public List<INavigation> NavigationsToLoad { get; } = navigationsToLoad;

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
        public Dictionary<IEntityType, List<(object Entity, ValueBuffer Buffer)>> Tables { get; } = [];

        public Dictionary<object, ValueBuffer> EntityBuffers { get; } = [with(ReferenceEqualityComparer.Instance)];
    }

    private sealed class JetQueryPlanExtractor(IModel model) : ExpressionVisitor
    {
        private readonly Dictionary<IEntityType, JetTableLoadPlan> rootTables = [];
        private readonly List<INavigation> navigations = [];
        private readonly HashSet<INavigation> navigationSet = [];
        private bool ignoreAutoIncludes;

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
                foreach (IEntityType rootType in extractor.rootTables.Keys.ToList())
                {
                    extractor.AddAutoIncludes(rootType, []);
                }
            }

            if (!extractor.IgnoreQueryFilters)
            {
                foreach (JetTableLoadPlan tablePlan in extractor.rootTables.Values)
                {
                    foreach (IQueryFilter filter in tablePlan.EntityType.GetDeclaredQueryFilters())
                    {
                        if (filter.Expression is LambdaExpression filterLambda)
                        {
                            tablePlan.SeekConditions.AddRange(
                                JetIndexSeekPlanner.ExtractSeekConditions(tablePlan.EntityType, filterLambda));
                        }
                    }
                }
            }

            return new JetQueryPlan(
                [.. extractor.rootTables.Values],
                extractor.navigations,
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
                IEntityType rootType = queryRoot.EntityType.GetRootType();
                if (!this.rootTables.ContainsKey(rootType))
                {
                    this.rootTables[rootType] = new JetTableLoadPlan(rootType, []);
                }

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
                    this.ExtractIncludeNavigations(node);
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
                    && TryFindDirectRootEntityType(node.Arguments[0], out IEntityType? rootEntityType))
                {
                    _ = this.Visit(node.Arguments[0]);
                    if (this.rootTables.TryGetValue(rootEntityType!.GetRootType(), out JetTableLoadPlan? plan))
                    {
                        plan.SeekConditions.AddRange(
                            JetIndexSeekPlanner.ExtractSeekConditions(rootEntityType.GetRootType(), predicate));
                    }

                    _ = this.Visit(predicate);
                    return node;
                }
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is not null)
            {
                IEntityType? ownerType = model.FindEntityType(node.Expression.Type);
                INavigation? nav = ownerType?.FindNavigation(node.Member);
                if (nav is not null)
                {
                    this.AddNavigation(nav);
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

        private static bool TryFindDirectRootEntityType(Expression source, out IEntityType? entityType)
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
                entityType = root.EntityType;
                return true;
            }

            entityType = null;
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
                if (sourceType is not null && lambda.Body is MemberExpression memberExpr)
                {
                    INavigation? nav = sourceType.FindNavigation(memberExpr.Member);
                    if (nav is not null)
                    {
                        this.AddNavigation(nav);
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

                    this.AddNavigation(nav);
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
                    this.AddNavigation(nav);
                    this.AddAutoIncludes(nav.TargetEntityType, visited);
                }
            }
        }

        private void AddNavigation(INavigation navigation)
        {
            if (this.navigationSet.Add(navigation))
            {
                this.navigations.Add(navigation);
            }
        }
    }

    private sealed class JetRuntimeExpressionRewriter(
        QueryContext queryContext,
        IModel model,
        JetExecutionMaterializedState state,
        bool ignoreQueryFilters) : ExpressionVisitor
    {
        private static readonly MethodInfo CreateQueryableMethod =
            typeof(JetRuntimeExpressionRewriter).GetMethod(
                nameof(CreateTypedQueryable),
                BindingFlags.NonPublic | BindingFlags.Static)!;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is EntityQueryRootExpression queryRoot)
            {
                IEntityType rootType = queryRoot.EntityType.GetRootType();
                List<(object Entity, ValueBuffer Buffer)> rows = state.Tables.TryGetValue(rootType, out List<(object Entity, ValueBuffer Buffer)>? found)
                    ? found
                    : [];

                MethodInfo typedCreator = CreateQueryableMethod.MakeGenericMethod(queryRoot.EntityType.ClrType);
                var queryable = (IQueryable)typedCreator.Invoke(null, [rows])!;
                Expression current = Expression.Constant(queryable, typeof(IQueryable<>).MakeGenericType(queryRoot.EntityType.ClrType));

                if (!ignoreQueryFilters)
                {
                    foreach (IQueryFilter filter in queryRoot.EntityType.GetDeclaredQueryFilters())
                    {
                        if (filter.Expression is LambdaExpression filterLambda)
                        {
                            var visitedFilter = (LambdaExpression)this.Visit(filterLambda);
                            current = Expression.Call(
                                typeof(Queryable),
                                nameof(Queryable.Where),
                                [queryRoot.EntityType.ClrType],
                                current,
                                Expression.Quote(visitedFilter));
                        }
                    }
                }

                return current;
            }

            if (node is QueryParameterExpression queryParam)
            {
                object? val = queryContext.Parameters.TryGetValue(queryParam.Name, out object? found) ? found : null;
                return Expression.Constant(val, queryParam.Type);
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
                return Expression.Call(
                    readMethod,
                    Expression.Constant(queryContext),
                    Expression.Constant(state.EntityBuffers, typeof(IReadOnlyDictionary<object, ValueBuffer>)),
                    Expression.Convert(target, typeof(object)),
                    propName);
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

        private static IQueryable<TEntity> CreateTypedQueryable<TEntity>(List<(object Entity, ValueBuffer Buffer)> rows)
        {
            List<TEntity> typed = [];
            foreach ((object entity, _) in rows)
            {
                if (entity is TEntity castEntity)
                {
                    typed.Add(castEntity);
                }
            }

            return typed.AsQueryable();
        }
    }
}
