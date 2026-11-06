namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Mapping;
using static JetDatabaseWriter.Queries.QueryPlanTemplateCache;

/// <summary>
/// Splits a LINQ query expression tree into the operators the provider can execute
/// natively against the table — the longest innermost run of supported operators that
/// still yields the entity type (collected into an <see cref="AccessQueryPlan"/>) — and
/// the <em>tail</em> above it (projection and anything after it). The boundary between
/// the two is returned so the provider can run the tail through <see cref="InMemoryTail"/>
/// over the rows the engine streams. Operators in the engine boundary keep their
/// index-inference fast path; the tail handles <c>Select</c>, post-projection operators, and
/// any operator the engine does not translate.
/// </summary>
internal static class AccessQueryTranslator
{
    /// <summary>
    /// Finds the engine-evaluable boundary inside <paramref name="expression"/> and
    /// translates that prefix into a plan. The boundary is the deepest sub-expression that
    /// is a contiguous innermost run of supported operators; everything outside it is the
    /// in-memory tail.
    /// </summary>
    /// <param name="expression">The full query expression tree.</param>
    /// <returns>
    /// The translated plan for the engine prefix and the boundary sub-expression. When the
    /// boundary is reference-equal to <paramref name="expression"/> the whole query runs in
    /// the engine and there is no tail.
    /// </returns>
    /// <exception cref="JetNotSupportedException">The query source is not a constant expression.</exception>
    public static (AccessQueryPlan Plan, Expression Boundary) Translate(Expression expression) =>
        Translate(expression, rootMap: null);

    /// <summary>
    /// Translates <paramref name="expression"/> using the cached query plan template and
    /// mapped column metadata of <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The root entity type.</typeparam>
    /// <param name="expression">The full query expression tree.</param>
    /// <returns>The translated plan and engine boundary.</returns>
    public static (AccessQueryPlan Plan, Expression Boundary) Translate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression expression) =>
        Translate(expression, EntityMap.For(typeof(T)));

    internal static bool IsThenBy(MethodCallExpression call) =>
        call.Method.DeclaringType == typeof(Queryable) && call.Method.Name is "ThenBy" or "ThenByDescending";

    /// <summary>
    /// Determines whether <paramref name="call"/> is one of the operators the engine
    /// translates in its native pipeline, restricted to the simple forms it can honor:
    /// a single-parameter <c>Where</c> predicate and orderings without a custom comparer.
    /// </summary>
    /// <param name="call">The operator call to classify.</param>
    /// <returns><see langword="true"/> when the engine can translate the operator.</returns>
    internal static bool IsEngineSupported(MethodCallExpression call)
    {
        if (AccessQueryExtensions.IsIncludeMethod(call.Method) || AccessQueryExtensions.IsThenIncludeMethod(call.Method))
        {
            return true;
        }

        return call.Method.DeclaringType == typeof(Queryable) && IsPipelineOperator(call)
            && (call.Method.Name is not ("Skip" or "Take") || call.Arguments[1].Type == typeof(int));
    }

    internal static bool IsSingleParameterLambda(Expression argument)
    {
        Expression operand = argument is UnaryExpression { NodeType: ExpressionType.Quote } quote ? quote.Operand : argument;
        return operand is LambdaExpression lambda && lambda.Parameters.Count == 1;
    }

    /// <summary>
    /// Determines whether <paramref name="call"/> is one of the inline collection operators a
    /// filtered / ordered include may carry (<c>Where</c>, the four orderings, <c>Skip</c>,
    /// <c>Take</c>), declared on <see cref="Enumerable"/> or <see cref="Queryable"/>. The
    /// indexed <c>Where</c> overload is excluded — only a single-parameter predicate qualifies.
    /// </summary>
    /// <param name="call">The candidate operator call inside the navigation lambda.</param>
    /// <returns><see langword="true"/> when the call is a supported include operator.</returns>
    internal static bool IsIncludeOperation(MethodCallExpression call) =>
        (call.Method.DeclaringType == typeof(Enumerable) || call.Method.DeclaringType == typeof(Queryable)) && IsPipelineOperator(call);

    internal static Expression StripConvert(Expression expression) =>
        expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary
            ? unary.Operand
            : expression;

    private static (AccessQueryPlan Plan, Expression Boundary) Translate(Expression expression, EntityMap? rootMap)
    {
        if (expression is ConstantExpression)
        {
            return (new AccessQueryPlan(), expression);
        }

        // The stack yields the operators from the source outward without making query
        // depth depend on the process call stack. Reuse it for the boundary and the plan.
        var calls = new Stack<MethodCallExpression>();
        Expression source = expression;
        while (source is MethodCallExpression { Arguments.Count: > 0 } call)
        {
            calls.Push(call);
            source = call.Arguments[0];
        }

        if (source is not ConstantExpression)
        {
            throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, $"Unsupported query expression node '{source.NodeType}'.");
        }

        if (calls.Count <= QueryPlanTemplateCache.MaxCachedOperators)
        {
            MethodCallExpression[] orderedCalls = calls.ToArray();
            if (QueryPlanTemplateCache.TryGetOrAdd(source.Type, rootMap, orderedCalls, out QueryPlanTemplate? template))
            {
                return InstantiateTemplate(source, orderedCalls, template);
            }
        }

        Expression boundary = FindEngineBoundary(source, calls);
        var plan = new AccessQueryPlan();
        if (!ReferenceEquals(boundary, source))
        {
            foreach (MethodCallExpression call in calls)
            {
                Apply(call, plan);
                if (ReferenceEquals(call, boundary))
                {
                    break;
                }
            }
        }

        return (plan, boundary);
    }

    private static (AccessQueryPlan Plan, Expression Boundary) InstantiateTemplate(
        Expression source,
        MethodCallExpression[] calls,
        QueryPlanTemplate template)
    {
        int engineCount = template.EngineCallCount;
        Expression boundary = engineCount == 0 ? source : calls[engineCount - 1];
        MethodCallExpression[]? tailCalls = null;
        if (engineCount < calls.Length)
        {
            tailCalls = new MethodCallExpression[calls.Length - engineCount];
            Array.Copy(calls, engineCount, tailCalls, 0, tailCalls.Length);
        }

        var plan = new AccessQueryPlan
        {
            ReferencedColumns = template.ReferencedColumns,
            EngineOnlyReferencedColumns = template.EngineOnlyReferencedColumns,
            TailTemplate = template.TailGroups,
            TailCalls = tailCalls,
        };

        for (int i = 0; i < engineCount; i++)
        {
            EngineOperatorTemplate op = template.EngineOperators[i];
            MethodCallExpression call = calls[i];
            switch (op.Kind)
            {
                case EngineOperatorKind.Where:
                case EngineOperatorKind.OrderBy:
                case EngineOperatorKind.OrderByDescending:
                case EngineOperatorKind.ThenBy:
                case EngineOperatorKind.ThenByDescending:
                case EngineOperatorKind.Skip:
                case EngineOperatorKind.Take:
                    AppendOperatorByKind(op.Kind, call.Arguments[1], plan.Stages);
                    break;

                case EngineOperatorKind.Include:
                    plan.StartInclude(InstantiateIncludeStep(call.Arguments[1], op.Navigation!, op.IncludeOperations!));
                    break;

                case EngineOperatorKind.ThenInclude:
                    plan.ExtendInclude(InstantiateIncludeStep(call.Arguments[1], op.Navigation!, op.IncludeOperations!));
                    break;

                default:
                    throw InMemoryTail.UnsupportedOperator(call.Method.Name);
            }
        }

        return (plan, boundary);
    }

    private static IncludeStep InstantiateIncludeStep(
        Expression navigationArg,
        PropertyInfo navigationProperty,
        EngineOperatorKind[] operationKinds)
    {
        if (operationKinds.Length == 0)
        {
            return new IncludeStep(navigationProperty, []);
        }

        LambdaExpression navigation = ExtractLambda(navigationArg);
        Expression body = StripConvert(navigation.Body);
        var inlineCalls = new MethodCallExpression[operationKinds.Length];
        for (int i = operationKinds.Length - 1; i >= 0; i--)
        {
            var call = (MethodCallExpression)body;
            inlineCalls[i] = call;
            body = StripConvert(call.Arguments[0]);
        }

        var stages = new List<QueryStage>(operationKinds.Length);
        var operations = new List<IncludeOperation>(operationKinds.Length);
        for (int i = 0; i < operationKinds.Length; i++)
        {
            AppendOperatorByKind(operationKinds[i], inlineCalls[i].Arguments[1], stages);
            QueryStage stage = stages[^1];
            if (stages.Count > operations.Count)
            {
                operations.Add(stage switch
                {
                    FilterStage filter => new IncludeFilterOperation(filter.Predicate),
                    OrderStage => new IncludeOrderOperation(),
                    SkipStage skip => new IncludeSkipOperation(skip.Count),
                    TakeStage take => new IncludeTakeOperation(take.Count),
                    _ => throw new JetOperationException(JetErrorCode.InvalidOperationState, "Unsupported include stage."),
                });
            }

            if (stage is OrderStage order)
            {
                OrderingKey key = order.Keys[^1];
                ((IncludeOrderOperation)operations[^1]).AddKey(key.KeySelector, key.Descending);
            }
        }

        return new IncludeStep(navigationProperty, operations);
    }

    private static void AppendOperatorByKind(EngineOperatorKind kind, Expression argument, List<QueryStage> stages)
    {
        switch (kind)
        {
            case EngineOperatorKind.Where:
                stages.Add(new FilterStage(ExtractLambda(argument)));
                break;
            case EngineOperatorKind.OrderBy:
            case EngineOperatorKind.OrderByDescending:
            case EngineOperatorKind.ThenBy:
            case EngineOperatorKind.ThenByDescending:
                OrderStage order;
                if (kind is EngineOperatorKind.ThenBy or EngineOperatorKind.ThenByDescending
                    && stages.Count > 0
                    && stages[^1] is OrderStage current)
                {
                    order = current;
                }
                else
                {
                    order = new OrderStage();
                    stages.Add(order);
                }

                bool descending = kind is EngineOperatorKind.OrderByDescending or EngineOperatorKind.ThenByDescending;
                order.Keys.Add(new OrderingKey(ExtractLambda(argument), descending));
                break;
            case EngineOperatorKind.Skip:
                stages.Add(new SkipStage(ToCount(argument)));
                break;
            case EngineOperatorKind.Take:
                stages.Add(new TakeStage(ToCount(argument)));
                break;
            case EngineOperatorKind.Include or EngineOperatorKind.ThenInclude or _:
                break;
        }
    }

    /// <summary>
    /// Walks the operator chain from the innermost source outward and returns the largest
    /// sub-expression that consists solely of supported operators. The walk stops at the
    /// first operator the engine cannot translate (for example <c>Select</c>, an indexed
    /// <c>Where</c>, an ordering with a custom comparer, or a scalar terminal); that
    /// operator and everything outside it form the in-memory tail. A <c>ThenBy</c> the engine
    /// cannot translate refines the ordering below it, which the tail would not see, so the
    /// boundary then drops below the <c>OrderBy</c> that starts that ordering and the tail
    /// sorts by every key.
    /// </summary>
    /// <param name="source">The innermost query source.</param>
    /// <param name="calls">The operator chain, enumerated from the source outward.</param>
    /// <returns>The largest engine-evaluable sub-expression.</returns>
    private static Expression FindEngineBoundary(Expression source, Stack<MethodCallExpression> calls)
    {
        Expression boundary = source;
        foreach (MethodCallExpression call in calls)
        {
            if (!IsEngineSupported(call))
            {
                return IsThenBy(call) ? OrderingSource(boundary) : boundary;
            }

            boundary = call;
        }

        return boundary;
    }

    /// <summary>
    /// Returns the source of the ordering that <paramref name="ordered"/> ends: below its
    /// <c>ThenBy</c> calls and the <c>OrderBy</c> or <c>OrderByDescending</c> that starts it.
    /// </summary>
    /// <param name="ordered">An engine-evaluable expression that ends in an ordering.</param>
    /// <returns>The expression the ordering sorts.</returns>
    private static Expression OrderingSource(Expression ordered)
    {
        while (ordered is MethodCallExpression call && IsThenBy(call))
        {
            ordered = call.Arguments[0];
        }

        return ordered is MethodCallExpression { Method.Name: "OrderBy" or "OrderByDescending" } orderBy
            ? orderBy.Arguments[0]
            : ordered;
    }

    private static void Apply(MethodCallExpression call, AccessQueryPlan plan)
    {
        if (call.Method.DeclaringType == typeof(Queryable))
        {
            AppendOperator(call, plan.Stages);
            return;
        }

        if (AccessQueryExtensions.IsIncludeMethod(call.Method))
        {
            plan.StartInclude(ResolveIncludeStep(ExtractLambda(call.Arguments[1])));
            return;
        }

        if (AccessQueryExtensions.IsThenIncludeMethod(call.Method))
        {
            plan.ExtendInclude(ResolveIncludeStep(ExtractLambda(call.Arguments[1])));
            return;
        }

        throw InMemoryTail.UnsupportedOperator(call.Method.Name);
    }

    private static void AppendOperator(MethodCallExpression call, List<QueryStage> stages)
    {
        switch (call.Method.Name)
        {
            case "Where":
                stages.Add(new FilterStage(ExtractLambda(call.Arguments[1])));
                break;
            case "OrderBy":
            case "OrderByDescending":
            case "ThenBy":
            case "ThenByDescending":
                OrderStage order;
                if (call.Method.Name is "ThenBy" or "ThenByDescending" && stages.Count > 0 && stages[^1] is OrderStage current)
                {
                    order = current;
                }
                else
                {
                    order = new OrderStage();
                    stages.Add(order);
                }

                order.Keys.Add(new OrderingKey(ExtractLambda(call.Arguments[1]), call.Method.Name.EndsWith("Descending", StringComparison.Ordinal)));
                break;
            case "Skip":
                stages.Add(new SkipStage(ToCount(call.Arguments[1])));
                break;
            case "Take":
                stages.Add(new TakeStage(ToCount(call.Arguments[1])));
                break;
            default:
                throw InMemoryTail.UnsupportedOperator(call.Method.Name);
        }
    }

    private static LambdaExpression ExtractLambda(Expression expression) => expression switch
    {
        UnaryExpression { NodeType: ExpressionType.Quote } quote => (LambdaExpression)quote.Operand,
        LambdaExpression lambda => lambda,
        _ => throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, "Expected a lambda argument in the query expression."),
    };

    /// <summary>
    /// Resolves an <c>Include</c> / <c>ThenInclude</c> navigation lambda into the navigation
    /// property plus any inline collection operators. The lambda is either a plain property
    /// access (<c>o =&gt; o.Customer</c>) or, for a collection navigation, that access wrapped
    /// in an EF-style filtered / ordered include chain
    /// (<c>o =&gt; o.Items.Where(...).OrderBy(...).Take(n)</c>). The chain is peeled from the
    /// outside in down to the property; the operators are returned in written (execution) order.
    /// </summary>
    /// <param name="navigation">The navigation lambda.</param>
    /// <returns>The navigation property and its inline operators (empty for a plain access).</returns>
    private static IncludeStep ResolveIncludeStep(LambdaExpression navigation)
    {
        Expression body = StripConvert(navigation.Body);
        var calls = new List<MethodCallExpression>();
        while (body is MethodCallExpression call && IsIncludeOperation(call))
        {
            calls.Add(call);
            body = StripConvert(call.Arguments[0]);
        }

        PropertyInfo property = ResolveMember(body);
        calls.Reverse();
        return new IncludeStep(property, BuildIncludeOperations(calls));
    }

    private static bool IsPipelineOperator(MethodCallExpression call) =>
        call.Arguments.Count == 2 && call.Method.Name switch
        {
            "Where" => IsSingleParameterLambda(call.Arguments[1]),
            "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "Skip" or "Take" => true,
            _ => false,
        };

    private static List<IncludeOperation> BuildIncludeOperations(List<MethodCallExpression> calls)
    {
        var stages = new List<QueryStage>(calls.Count);
        var operations = new List<IncludeOperation>(calls.Count);
        foreach (MethodCallExpression call in calls)
        {
            AppendOperator(call, stages);
            QueryStage stage = stages[^1];
            if (stages.Count > operations.Count)
            {
                operations.Add(stage switch
                {
                    FilterStage filter => new IncludeFilterOperation(filter.Predicate),
                    OrderStage => new IncludeOrderOperation(),
                    SkipStage skip => new IncludeSkipOperation(skip.Count),
                    TakeStage take => new IncludeTakeOperation(take.Count),
                    _ => throw new JetOperationException(JetErrorCode.InvalidOperationState, "Unsupported include stage."),
                });
            }

            // Prepare each delegate before reading the next operand, including a ThenBy
            // key appended to an existing run. Per-parent execution remains synchronous.
            if (stage is OrderStage order)
            {
                OrderingKey key = order.Keys[^1];
                ((IncludeOrderOperation)operations[^1]).AddKey(key.KeySelector, key.Descending);
            }
        }

        return operations;
    }

    /// <summary>Reads a paging operand once when translating this execution, preserving any getter exception.</summary>
    /// <param name="expression">The constant, captured or computed count.</param>
    /// <returns>The number of elements to skip or take.</returns>
    private static int ToCount(Expression expression) =>
        Convert.ToInt32(ClosureValueReader.Evaluate(expression), CultureInfo.InvariantCulture);

    private static PropertyInfo ResolveMember(Expression body)
    {
        if (StripConvert(body) is MemberExpression { Member: PropertyInfo property })
        {
            return property;
        }

        throw new JetNotSupportedException(
            JetErrorCode.FeatureNotSupported,
            "An Include navigation must be a property access, optionally followed by Where / OrderBy / OrderByDescending / ThenBy / ThenByDescending / Skip / Take on a collection navigation, for example o => o.Customer, c => c.Orders, or c => c.Orders.Where(o => o.Open).Take(5).");
    }
}
