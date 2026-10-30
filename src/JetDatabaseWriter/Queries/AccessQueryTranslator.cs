namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Linq;

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
    public static (AccessQueryPlan Plan, Expression Boundary) Translate(Expression expression)
    {
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

    private static bool IsThenBy(MethodCallExpression call) =>
        call.Method.DeclaringType == typeof(Queryable) && call.Method.Name is "ThenBy" or "ThenByDescending";

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

    /// <summary>
    /// Determines whether <paramref name="call"/> is one of the operators the engine
    /// translates in its native pipeline, restricted to the simple forms it can honor:
    /// a single-parameter <c>Where</c> predicate and orderings without a custom comparer.
    /// </summary>
    /// <param name="call">The operator call to classify.</param>
    /// <returns><see langword="true"/> when the engine can translate the operator.</returns>
    private static bool IsEngineSupported(MethodCallExpression call)
    {
        if (AccessQueryExtensions.IsIncludeMethod(call.Method) || AccessQueryExtensions.IsThenIncludeMethod(call.Method))
        {
            return true;
        }

        return call.Method.DeclaringType == typeof(Queryable) && IsPipelineOperator(call)
            && (call.Method.Name is not ("Skip" or "Take") || call.Arguments[1].Type == typeof(int));
    }

    private static bool IsSingleParameterLambda(Expression argument)
    {
        Expression operand = argument is UnaryExpression { NodeType: ExpressionType.Quote } quote ? quote.Operand : argument;
        return operand is LambdaExpression lambda && lambda.Parameters.Count == 1;
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

    /// <summary>
    /// Determines whether <paramref name="call"/> is one of the inline collection operators a
    /// filtered / ordered include may carry (<c>Where</c>, the four orderings, <c>Skip</c>,
    /// <c>Take</c>), declared on <see cref="Enumerable"/> or <see cref="Queryable"/>. The
    /// indexed <c>Where</c> overload is excluded — only a single-parameter predicate qualifies.
    /// </summary>
    /// <param name="call">The candidate operator call inside the navigation lambda.</param>
    /// <returns><see langword="true"/> when the call is a supported include operator.</returns>
    private static bool IsIncludeOperation(MethodCallExpression call) =>
        (call.Method.DeclaringType == typeof(Enumerable) || call.Method.DeclaringType == typeof(Queryable)) && IsPipelineOperator(call);

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

    private static Expression StripConvert(Expression expression) =>
        expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary
            ? unary.Operand
            : expression;

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
