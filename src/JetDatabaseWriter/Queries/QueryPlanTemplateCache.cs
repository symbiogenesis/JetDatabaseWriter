namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Mapping;

/// <summary>
/// Caches bounded structural query translation templates keyed by expression shape so repeated
/// executions of the same LINQ query structure skip expression-tree boundary discovery,
/// operator classification, and referenced-column projection analysis while still evaluating
/// live closure arguments per execution.
/// </summary>
internal static class QueryPlanTemplateCache
{
    internal const int MaxCachedOperators = 64;
    private const int CacheCapacity = 64;
    private const int MaximumNodes = 512;
    private const int MaximumDepth = 64;

    private static readonly BoundedCache<Shape, QueryPlanTemplate> Cache = new(CacheCapacity);

#if !NET5_0_OR_GREATER
    private static readonly Func<Type, bool>? CollectibleGetter =
        typeof(Type).GetProperty("IsCollectible")?.GetMethod is { } getter
            ? (Func<Type, bool>)getter.CreateDelegate(typeof(Func<Type, bool>))
            : null;
#endif

    internal enum EngineOperatorKind
    {
        Where = 0,
        OrderBy = 1,
        OrderByDescending = 2,
        ThenBy = 3,
        ThenByDescending = 4,
        Skip = 5,
        Take = 6,
        Include = 7,
        ThenInclude = 8,
    }

    internal enum TailStepKind
    {
        Ordering = 0,
        Where = 1,
        Select = 2,
        Cast = 3,
        OfType = 4,
        Skip = 5,
        Take = 6,
        SkipWhile = 7,
        TakeWhile = 8,
        SkipLast = 9,
        TakeLast = 10,
        Distinct = 11,
        Union = 12,
        Intersect = 13,
        Except = 14,
        Concat = 15,
        Reverse = 16,
        DefaultIfEmpty = 17,
        Append = 18,
        Prepend = 19,
    }

    internal static int CachedPlanCount => Cache.Count;

    public static bool TryGetOrAdd(
        Type sourceType,
        EntityMap? rootMap,
        IReadOnlyList<MethodCallExpression> calls,
        [NotNullWhen(true)] out QueryPlanTemplate? template)
    {
        template = null;
        if (calls.Count == 0 || calls.Count > MaxCachedOperators || HasCollectibleType(sourceType))
        {
            return false;
        }

        Type rootElementType = rootMap?.EntityType ?? sourceType;
        if (HasCollectibleType(rootElementType))
        {
            return false;
        }

        var visitor = new QueryShapeVisitor();
        for (int i = 0; i < calls.Count; i++)
        {
            MethodCallExpression call = calls[i];
            if (!visitor.VisitOperatorCall(call))
            {
                return false;
            }
        }

        var shape = new Shape(rootElementType, rootMap is not null, visitor.Tokens.ToArray());
        if (Cache.TryGetValue(shape, out QueryPlanTemplate? existing))
        {
            template = existing;
            return true;
        }

        if (!BuildTemplateStructure(rootMap, calls, out QueryPlanTemplate? candidate))
        {
            return false;
        }

        template = Cache.GetOrCreate(shape, _ => candidate);
        return true;
    }

    internal static bool HasCollectibleType(Type type)
    {
        if (IsTypeCollectible(type))
        {
            return true;
        }

        if (type.IsArray && type.GetElementType() is { } elementType)
        {
            return HasCollectibleType(elementType);
        }

        if (type.IsGenericType)
        {
            foreach (Type argument in type.GetGenericArguments())
            {
                if (HasCollectibleType(argument))
                {
                    return true;
                }
            }
        }

        return false;
    }

#if NET5_0_OR_GREATER
    private static bool IsTypeCollectible(Type type) => type.IsCollectible;
#else
    private static bool IsTypeCollectible(Type type) =>
        type.Assembly.IsDynamic || (CollectibleGetter is not null && CollectibleGetter(type));
#endif

    private static bool BuildTemplateStructure(
        EntityMap? rootMap,
        IReadOnlyList<MethodCallExpression> calls,
        [NotNullWhen(true)] out QueryPlanTemplate? template)
    {
        template = null;
        int engineCallCount = FindEngineBoundaryIndex(calls);

        var engineOperators = new EngineOperatorTemplate[engineCallCount];
        bool hasIncludes = false;
        for (int i = 0; i < engineCallCount; i++)
        {
            if (!TryClassifyEngineOperator(calls[i], ref hasIncludes, out EngineOperatorTemplate op))
            {
                return false;
            }

            engineOperators[i] = op;
        }

        TailGroupTemplate[]? tailGroups = TryBuildTailGroups(calls, engineCallCount);
        ComputeProjectionSets(
            rootMap,
            calls,
            engineCallCount,
            engineOperators,
            hasIncludes,
            tailGroups,
            out HashSet<string>? referencedColumns,
            out HashSet<string>? engineOnlyReferencedColumns);

        template = new QueryPlanTemplate
        {
            EngineCallCount = engineCallCount,
            EngineOperators = engineOperators,
            TailGroups = tailGroups,
            ReferencedColumns = referencedColumns,
            EngineOnlyReferencedColumns = engineOnlyReferencedColumns,
        };
        return true;
    }

    private static int FindEngineBoundaryIndex(IReadOnlyList<MethodCallExpression> calls)
    {
        int boundaryIndex = 0;
        for (int i = 0; i < calls.Count; i++)
        {
            MethodCallExpression call = calls[i];
            if (!AccessQueryTranslator.IsEngineSupported(call))
            {
                if (AccessQueryTranslator.IsThenBy(call))
                {
                    while (boundaryIndex > 0 && AccessQueryTranslator.IsThenBy(calls[boundaryIndex - 1]))
                    {
                        boundaryIndex--;
                    }

                    if (boundaryIndex > 0 && calls[boundaryIndex - 1].Method.Name is "OrderBy" or "OrderByDescending")
                    {
                        boundaryIndex--;
                    }
                }

                return boundaryIndex;
            }

            boundaryIndex = i + 1;
        }

        return boundaryIndex;
    }

    private static bool TryClassifyEngineOperator(
        MethodCallExpression call,
        ref bool hasIncludes,
        out EngineOperatorTemplate template)
    {
        template = default;
        if (call.Method.DeclaringType == typeof(Queryable))
        {
            if (!TryMapPipelineOperatorKind(call.Method.Name, out EngineOperatorKind kind))
            {
                return false;
            }

            template = new EngineOperatorTemplate(kind);
            return true;
        }

        if (AccessQueryExtensions.IsIncludeMethod(call.Method))
        {
            if (!TryClassifyIncludeLambda(call.Arguments[1], out PropertyInfo? nav, out EngineOperatorKind[]? ops))
            {
                return false;
            }

            hasIncludes = true;
            template = new EngineOperatorTemplate(EngineOperatorKind.Include, nav, ops);
            return true;
        }

        if (AccessQueryExtensions.IsThenIncludeMethod(call.Method))
        {
            if (!hasIncludes || !TryClassifyIncludeLambda(call.Arguments[1], out PropertyInfo? nav, out EngineOperatorKind[]? ops))
            {
                return false;
            }

            template = new EngineOperatorTemplate(EngineOperatorKind.ThenInclude, nav, ops);
            return true;
        }

        return false;
    }

    private static bool TryClassifyIncludeLambda(
        Expression argument,
        [NotNullWhen(true)] out PropertyInfo? navigation,
        [NotNullWhen(true)] out EngineOperatorKind[]? operations)
    {
        navigation = null;
        operations = null;
        if (TryExtractLambda(argument) is not LambdaExpression lambda)
        {
            return false;
        }

        Expression body = AccessQueryTranslator.StripConvert(lambda.Body);
        var inlineCalls = new List<MethodCallExpression>();
        while (body is MethodCallExpression call && AccessQueryTranslator.IsIncludeOperation(call))
        {
            inlineCalls.Add(call);
            body = AccessQueryTranslator.StripConvert(call.Arguments[0]);
        }

        if (AccessQueryTranslator.StripConvert(body) is not MemberExpression { Member: PropertyInfo property })
        {
            return false;
        }

        inlineCalls.Reverse();
        var kinds = new EngineOperatorKind[inlineCalls.Count];
        for (int i = 0; i < inlineCalls.Count; i++)
        {
            if (!TryMapPipelineOperatorKind(inlineCalls[i].Method.Name, out EngineOperatorKind kind))
            {
                return false;
            }

            kinds[i] = kind;
        }

        navigation = property;
        operations = kinds;
        return true;
    }

    private static bool TryMapPipelineOperatorKind(string methodName, out EngineOperatorKind kind)
    {
        switch (methodName)
        {
            case "Where":
                kind = EngineOperatorKind.Where;
                return true;
            case "OrderBy":
                kind = EngineOperatorKind.OrderBy;
                return true;
            case "OrderByDescending":
                kind = EngineOperatorKind.OrderByDescending;
                return true;
            case "ThenBy":
                kind = EngineOperatorKind.ThenBy;
                return true;
            case "ThenByDescending":
                kind = EngineOperatorKind.ThenByDescending;
                return true;
            case "Skip":
                kind = EngineOperatorKind.Skip;
                return true;
            case "Take":
                kind = EngineOperatorKind.Take;
                return true;
            default:
                kind = default;
                return false;
        }
    }

    private static TailGroupTemplate[]? TryBuildTailGroups(IReadOnlyList<MethodCallExpression> calls, int engineCallCount)
    {
        int tailCount = calls.Count - engineCallCount;
        if (tailCount == 0)
        {
            return [];
        }

        var groups = new List<TailGroupTemplate>(tailCount);
        for (int i = 0; i < tailCount; i++)
        {
            MethodCallExpression call = calls[engineCallCount + i];
            if (AccessQueryExtensions.IsIncludeMethod(call.Method)
                || AccessQueryExtensions.IsThenIncludeMethod(call.Method)
                || call.Method.DeclaringType != typeof(Queryable))
            {
                return null;
            }

            if (InMemoryTail.IsOrdering(call))
            {
                var keys = new List<TailOrderingKeyTemplate>();
                if (!TryBuildOrderingKeyTemplate(call, i, out TailOrderingKeyTemplate firstKey))
                {
                    return null;
                }

                keys.Add(firstKey);
                while (i + 1 < tailCount && InMemoryTail.IsThenBy(calls[engineCallCount + i + 1]))
                {
                    i++;
                    if (!TryBuildOrderingKeyTemplate(calls[engineCallCount + i], i, out TailOrderingKeyTemplate nextKey))
                    {
                        return null;
                    }

                    keys.Add(nextKey);
                }

                groups.Add(new TailGroupTemplate
                {
                    Kind = TailStepKind.Ordering,
                    TailCallIndex = firstKey.TailCallIndex,
                    OrderingKeys = keys.ToArray(),
                });
                continue;
            }

            if (!TryBuildSingleTailStep(call, i, out TailGroupTemplate? step))
            {
                return null;
            }

            groups.Add(step);
        }

        return groups.ToArray();
    }

    private static bool TryBuildOrderingKeyTemplate(
        MethodCallExpression call,
        int tailCallIndex,
        out TailOrderingKeyTemplate keyTemplate)
    {
        keyTemplate = default;
        Type[] typeArguments = call.Method.GetGenericArguments();
        bool descending = call.Method.Name.EndsWith("Descending", StringComparison.Ordinal);
        bool isElementOrder = call.Method.Name is "Order" or "OrderDescending";
        if (!isElementOrder && !AccessQueryTranslator.IsSingleParameterLambda(call.Arguments[1]))
        {
            return false;
        }

        Type keyType = isElementOrder ? typeArguments[0] : typeArguments[1];
        int comparerArgIndex = isElementOrder ? 1 : 2;
        bool hasComparer = call.Arguments.Count > comparerArgIndex;
        keyTemplate = new TailOrderingKeyTemplate(
            tailCallIndex,
            isElementOrder,
            descending,
            keyType,
            hasComparer);
        return true;
    }

    private static bool TryBuildSingleTailStep(
        MethodCallExpression call,
        int tailCallIndex,
        [NotNullWhen(true)] out TailGroupTemplate? step)
    {
        step = null;
        switch (call.Method.Name)
        {
            case "Where":
            case "Select":
            case "SkipWhile":
            case "TakeWhile":
                if (!AccessQueryTranslator.IsSingleParameterLambda(call.Arguments[1]))
                {
                    return false;
                }

                TailStepKind lambdaKind = call.Method.Name switch
                {
                    "Where" => TailStepKind.Where,
                    "Select" => TailStepKind.Select,
                    "SkipWhile" => TailStepKind.SkipWhile,
                    _ => TailStepKind.TakeWhile,
                };
                step = new TailGroupTemplate { Kind = lambdaKind, TailCallIndex = tailCallIndex };
                return true;

            case "Skip":
            case "Take":
            case "SkipLast":
            case "TakeLast":
                if (call.Arguments[1].Type != typeof(int))
                {
                    return false;
                }

                TailStepKind countKind = call.Method.Name switch
                {
                    "Skip" => TailStepKind.Skip,
                    "Take" => TailStepKind.Take,
                    "SkipLast" => TailStepKind.SkipLast,
                    _ => TailStepKind.TakeLast,
                };
                step = new TailGroupTemplate { Kind = countKind, TailCallIndex = tailCallIndex };
                return true;

            case "Cast":
                step = new TailGroupTemplate
                {
                    Kind = TailStepKind.Cast,
                    TailCallIndex = tailCallIndex,
                    TargetType = call.Method.GetGenericArguments()[0],
                };
                return true;

            case "OfType":
                step = new TailGroupTemplate
                {
                    Kind = TailStepKind.OfType,
                    TailCallIndex = tailCallIndex,
                    TargetType = call.Method.GetGenericArguments()[0],
                };
                return true;

            case "Distinct":
                step = new TailGroupTemplate
                {
                    Kind = TailStepKind.Distinct,
                    TailCallIndex = tailCallIndex,
                    TargetType = call.Method.GetGenericArguments()[0],
                    HasComparerArgument = call.Arguments.Count > 1,
                };
                return true;

            case "Union":
            case "Intersect":
            case "Except":
                TailStepKind setKind = call.Method.Name switch
                {
                    "Union" => TailStepKind.Union,
                    "Intersect" => TailStepKind.Intersect,
                    _ => TailStepKind.Except,
                };
                step = new TailGroupTemplate
                {
                    Kind = setKind,
                    TailCallIndex = tailCallIndex,
                    TargetType = call.Method.GetGenericArguments()[0],
                    HasComparerArgument = call.Arguments.Count > 2,
                };
                return true;

            case "Concat":
                step = new TailGroupTemplate { Kind = TailStepKind.Concat, TailCallIndex = tailCallIndex };
                return true;

            case "Reverse":
                step = new TailGroupTemplate { Kind = TailStepKind.Reverse, TailCallIndex = tailCallIndex };
                return true;

            case "DefaultIfEmpty":
                step = new TailGroupTemplate { Kind = TailStepKind.DefaultIfEmpty, TailCallIndex = tailCallIndex };
                return true;

            case "Append":
                step = new TailGroupTemplate { Kind = TailStepKind.Append, TailCallIndex = tailCallIndex };
                return true;

            case "Prepend":
                step = new TailGroupTemplate { Kind = TailStepKind.Prepend, TailCallIndex = tailCallIndex };
                return true;

            default:
                return false;
        }
    }

    private static void ComputeProjectionSets(
        EntityMap? rootMap,
        IReadOnlyList<MethodCallExpression> calls,
        int engineCallCount,
        EngineOperatorTemplate[] engineOperators,
        bool hasIncludes,
        TailGroupTemplate[]? tailGroups,
        out HashSet<string>? referencedColumns,
        out HashSet<string>? engineOnlyReferencedColumns)
    {
        referencedColumns = null;
        engineOnlyReferencedColumns = null;

        if (rootMap is null || hasIncludes || tailGroups is null || !rootMap.EntityType.IsClass)
        {
            return;
        }

        Type rootElementType = rootMap.EntityType;
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < engineCallCount; i++)
        {
            EngineOperatorKind kind = engineOperators[i].Kind;
            if (kind is EngineOperatorKind.Where
                or EngineOperatorKind.OrderBy
                or EngineOperatorKind.OrderByDescending
                or EngineOperatorKind.ThenBy
                or EngineOperatorKind.ThenByDescending)
            {
                if (TryExtractLambda(calls[i].Arguments[1]) is not LambdaExpression lambda
                    || !TryCollectReferencedColumns(lambda, rootElementType, rootMap, columns))
                {
                    return;
                }
            }
        }

        if (tailGroups.Length == 0)
        {
            if (engineCallCount > 0)
            {
                engineOnlyReferencedColumns = columns;
            }

            return;
        }

        for (int i = 0; i < tailGroups.Length; i++)
        {
            TailGroupTemplate group = tailGroups[i];
            switch (group.Kind)
            {
                case TailStepKind.Select:
                    MethodCallExpression selectCall = calls[engineCallCount + group.TailCallIndex];
                    if (TryExtractLambda(selectCall.Arguments[1]) is LambdaExpression selectLambda
                        && TryCollectReferencedColumns(selectLambda, rootElementType, rootMap, columns))
                    {
                        referencedColumns = columns;
                    }

                    return;

                case TailStepKind.Where:
                case TailStepKind.SkipWhile:
                case TailStepKind.TakeWhile:
                    MethodCallExpression filterCall = calls[engineCallCount + group.TailCallIndex];
                    if (TryExtractLambda(filterCall.Arguments[1]) is not LambdaExpression filterLambda
                        || !TryCollectReferencedColumns(filterLambda, rootElementType, rootMap, columns))
                    {
                        return;
                    }

                    break;

                case TailStepKind.Ordering:
                    foreach (TailOrderingKeyTemplate key in group.OrderingKeys!)
                    {
                        if (key.IsElementOrder || key.HasComparerArgument)
                        {
                            return;
                        }

                        MethodCallExpression orderCall = calls[engineCallCount + key.TailCallIndex];
                        if (TryExtractLambda(orderCall.Arguments[1]) is not LambdaExpression keyLambda
                            || !TryCollectReferencedColumns(keyLambda, rootElementType, rootMap, columns))
                        {
                            return;
                        }
                    }

                    break;

                case TailStepKind.Skip:
                case TailStepKind.Take:
                case TailStepKind.SkipLast:
                case TailStepKind.TakeLast:
                case TailStepKind.Reverse:
                    break;

                case TailStepKind.Cast
                    or TailStepKind.OfType
                    or TailStepKind.Distinct
                    or TailStepKind.Union
                    or TailStepKind.Intersect
                    or TailStepKind.Except
                    or TailStepKind.Concat
                    or TailStepKind.DefaultIfEmpty
                    or TailStepKind.Append
                    or TailStepKind.Prepend
                    or _:
                    return;
            }
        }
    }

    private static bool TryCollectReferencedColumns(
        LambdaExpression lambda,
        Type rootElementType,
        EntityMap map,
        HashSet<string> columns)
    {
        if (lambda.Parameters.Count != 1 || lambda.Parameters[0].Type != rootElementType)
        {
            return false;
        }

        var visitor = new ParameterUsageVisitor(lambda.Parameters[0], map, columns);
        _ = visitor.Visit(lambda.Body);
        return visitor.AllDirectMappedProperties;
    }

    private static LambdaExpression? TryExtractLambda(Expression expression) => expression switch
    {
        UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression quoted } => quoted,
        LambdaExpression lambda => lambda,
        _ => null,
    };

    internal readonly record struct EngineOperatorTemplate(
        EngineOperatorKind Kind,
        PropertyInfo? Navigation = null,
        EngineOperatorKind[]? IncludeOperations = null);

    internal readonly record struct TailOrderingKeyTemplate(
        int TailCallIndex,
        bool IsElementOrder,
        bool Descending,
        Type KeyType,
        bool HasComparerArgument);

    internal sealed class TailGroupTemplate
    {
        public TailStepKind Kind { get; init; }

        public int TailCallIndex { get; init; }

        public TailOrderingKeyTemplate[]? OrderingKeys { get; init; }

        public Type? TargetType { get; init; }

        public bool HasComparerArgument { get; init; }
    }

    internal sealed class QueryPlanTemplate
    {
        public int EngineCallCount { get; init; }

        public EngineOperatorTemplate[] EngineOperators { get; init; } = [];

        public TailGroupTemplate[]? TailGroups { get; init; }

        public HashSet<string>? ReferencedColumns { get; init; }

        public HashSet<string>? EngineOnlyReferencedColumns { get; init; }
    }

    private sealed class ParameterUsageVisitor(
        ParameterExpression rootParameter,
        EntityMap map,
        HashSet<string> columns) : ExpressionVisitor
    {
        public bool AllDirectMappedProperties { get; private set; } = true;

        public override Expression? Visit(Expression? node) =>
            this.AllDirectMappedProperties ? base.Visit(node) : node;

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == rootParameter
                && node.Member is PropertyInfo property
                && map.FindByMember(property) is EntityProperty mapped
                && mapped.Property.Equals(property))
            {
                columns.Add(mapped.ColumnName);
                return node;
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == rootParameter)
            {
                this.AllDirectMappedProperties = false;
            }

            return base.VisitParameter(node);
        }
    }

    private sealed class Shape(Type rootElementType, bool hasRootMap, object?[] tokens) : IEquatable<Shape>
    {
        private Type RootElementType => rootElementType;

        private bool HasRootMap => hasRootMap;

        private object?[] Tokens => tokens;

        public bool Equals(Shape? other)
        {
            if (other is null
                || rootElementType != other.RootElementType
                || hasRootMap != other.HasRootMap
                || tokens.Length != other.Tokens.Length)
            {
                return false;
            }

            for (int i = 0; i < tokens.Length; i++)
            {
                if (!Equals(tokens[i], other.Tokens[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is Shape other && this.Equals(other);

        public override int GetHashCode()
        {
            HashCode hash = default;
            hash.Add(rootElementType);
            hash.Add(hasRootMap);
            foreach (object? token in tokens)
            {
                hash.Add(token);
            }

            return hash.ToHashCode();
        }
    }

    private sealed class QueryShapeVisitor : ExpressionVisitor
    {
        private readonly Dictionary<ParameterExpression, int> parameters = [];
        private int nodes;
        private int depth;

        internal List<object?> Tokens { get; } = [];

        internal bool Supported { get; private set; } = true;

        public bool VisitOperatorCall(MethodCallExpression call)
        {
            if (!this.Supported || ++this.nodes > MaximumNodes || HasCollectibleMethod(call.Method))
            {
                this.Supported = false;
                return false;
            }

            this.Tokens.Add(call.Method);
            this.Tokens.Add(call.Arguments.Count);
            for (int i = 1; i < call.Arguments.Count; i++)
            {
                _ = this.Visit(call.Arguments[i]);
                if (!this.Supported)
                {
                    return false;
                }
            }

            return true;
        }

        public override Expression? Visit(Expression? node)
        {
            if (!this.Supported)
            {
                return node;
            }

            if (node is null)
            {
                this.Tokens.Add(null);
                return null;
            }

            if (++this.nodes > MaximumNodes || this.depth >= MaximumDepth || HasCollectibleType(node.Type))
            {
                this.Supported = false;
                return node;
            }

            this.Tokens.Add(node.NodeType);
            this.Tokens.Add(node.Type);
            switch (node)
            {
                case BinaryExpression binary:
                    this.Tokens.Add(binary.Method);
                    this.Tokens.Add(binary.IsLiftedToNull);
                    break;
                case UnaryExpression unary:
                    this.Tokens.Add(unary.Method);
                    this.Tokens.Add(unary.IsLiftedToNull);
                    break;
                case MemberExpression member:
                    this.Tokens.Add(member.Member);
                    break;
                case MethodCallExpression call:
                    if (HasCollectibleMethod(call.Method))
                    {
                        this.Supported = false;
                        return node;
                    }

                    this.Tokens.Add(call.Method);
                    this.Tokens.Add(call.Arguments.Count);
                    break;
                case LambdaExpression lambda:
                    this.Tokens.Add(lambda.Parameters.Count);
                    this.Tokens.Add(lambda.TailCall);
                    break;
                case NewExpression creation:
                    this.Tokens.Add(creation.Constructor);
                    this.Tokens.Add(creation.Arguments.Count);
                    this.Tokens.Add(creation.Members?.Count ?? -1);
                    if (creation.Members is { } members)
                    {
                        this.Tokens.AddRange(members);
                    }

                    break;
                case ConstantExpression:
                    return node;
                case ParameterExpression parameter:
                    if (!this.parameters.TryGetValue(parameter, out int index))
                    {
                        index = this.parameters.Count;
                        this.parameters.Add(parameter, index);
                    }

                    this.Tokens.Add(index);
                    this.Tokens.Add(parameter.Name);
                    return node;
                default:
                    this.Tokens.Add(null);
                    break;
            }

            this.depth++;
            Expression? result = base.Visit(node);
            this.depth--;
            return result;
        }

        private static bool HasCollectibleMethod(MethodInfo method)
        {
            if (method.DeclaringType is { } declaring && HasCollectibleType(declaring))
            {
                return true;
            }

            if (method.IsGenericMethod)
            {
                foreach (Type arg in method.GetGenericArguments())
                {
                    if (HasCollectibleType(arg))
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}
