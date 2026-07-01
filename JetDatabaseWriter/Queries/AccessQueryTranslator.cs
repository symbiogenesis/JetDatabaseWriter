namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

/// <summary>
/// Splits a LINQ query expression tree into the operators the provider can execute
/// natively against the table — the longest innermost run of supported operators that
/// still yields the entity type (collected into an <see cref="AccessQueryPlan"/>) — and
/// the <em>tail</em> above it (projection and anything after it). The boundary between
/// the two is returned so the provider can replay the tail with LINQ-to-Objects over the
/// materialized rows. Operators in the engine boundary keep their index-inference fast
/// path; the tail handles <c>Select</c>, post-projection operators, and any operator the
/// engine does not translate.
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
    public static (AccessQueryPlan Plan, Expression Boundary) Translate(Expression expression)
    {
        Expression boundary = FindEngineBoundary(expression);
        var plan = new AccessQueryPlan();
        Visit(boundary, plan);
        return (plan, boundary);
    }

    /// <summary>
    /// Walks the operator chain from the innermost source outward and returns the largest
    /// sub-expression that consists solely of supported operators. The walk stops at the
    /// first operator the engine cannot translate (for example <c>Select</c>, an indexed
    /// <c>Where</c>, an ordering with a custom comparer, or a scalar terminal); that
    /// operator and everything outside it form the in-memory tail.
    /// </summary>
    /// <param name="expression">The sub-expression to examine.</param>
    /// <returns>The largest engine-evaluable sub-expression.</returns>
    private static Expression FindEngineBoundary(Expression expression)
    {
        if (expression is MethodCallExpression call && call.Arguments.Count >= 1)
        {
            Expression innerBoundary = FindEngineBoundary(call.Arguments[0]);

            // Only extend the boundary through this call when nothing below it was cut
            // (the inner part is fully engine-evaluable) and this operator is supported.
            if (ReferenceEquals(innerBoundary, call.Arguments[0]) && IsEngineSupported(call))
            {
                return call;
            }

            return innerBoundary;
        }

        // The innermost source (a ConstantExpression wrapping the queryable) is the floor.
        return expression;
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

        if (call.Method.DeclaringType != typeof(Queryable))
        {
            return false;
        }

        return call.Method.Name switch
        {
            // The indexed Where overload (Func<T,int,bool>) cannot be pushed as a row
            // predicate, so only the single-parameter form stays in the engine.
            "Where" => call.Arguments.Count == 2 && IsSingleParameterLambda(call.Arguments[1]),

            // A trailing IComparer<TKey> argument would be ignored by the engine's sort,
            // so only the two-argument ordering forms stay in the engine.
            "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" => call.Arguments.Count == 2,
            "Skip" or "Take" => call.Arguments.Count == 2,
            _ => false,
        };
    }

    private static bool IsSingleParameterLambda(Expression argument)
    {
        Expression operand = argument is UnaryExpression { NodeType: ExpressionType.Quote } quote ? quote.Operand : argument;
        return operand is LambdaExpression lambda && lambda.Parameters.Count == 1;
    }

    private static void Visit(Expression expression, AccessQueryPlan plan)
    {
        // The root source is a ConstantExpression wrapping the queryable; stop there.
        if (expression is ConstantExpression)
        {
            return;
        }

        if (expression is MethodCallExpression call && call.Arguments.Count >= 1)
        {
            Visit(call.Arguments[0], plan);
            Apply(call, plan);
            return;
        }

        throw new NotSupportedException($"Unsupported query expression node '{expression.NodeType}'.");
    }

    private static void Apply(MethodCallExpression call, AccessQueryPlan plan)
    {
        if (call.Method.DeclaringType == typeof(Queryable))
        {
            ApplyQueryableOperator(call, plan);
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

        throw NotSupported(call.Method.Name);
    }

    private static void ApplyQueryableOperator(MethodCallExpression call, AccessQueryPlan plan)
    {
        switch (call.Method.Name)
        {
            case "Where":
                plan.Stages.Add(new FilterStage(ExtractLambda(call.Arguments[1])));
                break;
            case "OrderBy":
                plan.Stages.Add(NewOrderStage(ExtractLambda(call.Arguments[1]), descending: false));
                break;
            case "OrderByDescending":
                plan.Stages.Add(NewOrderStage(ExtractLambda(call.Arguments[1]), descending: true));
                break;
            case "ThenBy":
                AppendOrdering(plan, ExtractLambda(call.Arguments[1]), descending: false);
                break;
            case "ThenByDescending":
                AppendOrdering(plan, ExtractLambda(call.Arguments[1]), descending: true);
                break;
            case "Skip":
                plan.Stages.Add(new SkipStage(Convert.ToInt32(EvaluateConstant(call.Arguments[1]), CultureInfo.InvariantCulture)));
                break;
            case "Take":
                plan.Stages.Add(new TakeStage(Convert.ToInt32(EvaluateConstant(call.Arguments[1]), CultureInfo.InvariantCulture)));
                break;
            default:
                throw NotSupported(call.Method.Name);
        }
    }

    private static OrderStage NewOrderStage(LambdaExpression keySelector, bool descending)
    {
        var stage = new OrderStage();
        stage.Keys.Add(new OrderingKey(keySelector, descending));
        return stage;
    }

    private static void AppendOrdering(AccessQueryPlan plan, LambdaExpression keySelector, bool descending)
    {
        // ThenBy refines the most recent ordering run; if an operator intervened (the
        // last stage is not an OrderStage), start a fresh ordering instead of crashing.
        if (plan.Stages.Count > 0 && plan.Stages[^1] is OrderStage order)
        {
            order.Keys.Add(new OrderingKey(keySelector, descending));
            return;
        }

        plan.Stages.Add(NewOrderStage(keySelector, descending));
    }

    private static LambdaExpression ExtractLambda(Expression expression) => expression switch
    {
        UnaryExpression { NodeType: ExpressionType.Quote } quote => (LambdaExpression)quote.Operand,
        LambdaExpression lambda => lambda,
        _ => throw new NotSupportedException("Expected a lambda argument in the query expression."),
    };

    private static object? EvaluateConstant(Expression expression) => expression switch
    {
        ConstantExpression constant => constant.Value,
        _ => Expression.Lambda(expression).Compile().DynamicInvoke(),
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
    private static bool IsIncludeOperation(MethodCallExpression call)
    {
        if (call.Arguments.Count != 2 ||
            (call.Method.DeclaringType != typeof(Enumerable) && call.Method.DeclaringType != typeof(Queryable)))
        {
            return false;
        }

        return call.Method.Name switch
        {
            "Where" => IsSingleParameterLambda(call.Arguments[1]),
            "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "Skip" or "Take" => true,
            _ => false,
        };
    }

    private static List<IncludeOperation> BuildIncludeOperations(List<MethodCallExpression> calls)
    {
        if (calls.Count == 0)
        {
            return [];
        }

        var operations = new List<IncludeOperation>(calls.Count);
        IncludeOrderOperation? currentOrder = null;
        foreach (MethodCallExpression call in calls)
        {
            switch (call.Method.Name)
            {
                case "Where":
                    currentOrder = null;
                    operations.Add(new IncludeFilterOperation(ExtractLambda(call.Arguments[1])));
                    break;
                case "OrderBy":
                    currentOrder = StartOrdering(operations, ExtractLambda(call.Arguments[1]), descending: false);
                    break;
                case "OrderByDescending":
                    currentOrder = StartOrdering(operations, ExtractLambda(call.Arguments[1]), descending: true);
                    break;
                case "ThenBy":
                    currentOrder = ContinueOrdering(currentOrder, operations, ExtractLambda(call.Arguments[1]), descending: false);
                    break;
                case "ThenByDescending":
                    currentOrder = ContinueOrdering(currentOrder, operations, ExtractLambda(call.Arguments[1]), descending: true);
                    break;
                case "Skip":
                    currentOrder = null;
                    operations.Add(new IncludeSkipOperation(ToCount(call.Arguments[1])));
                    break;
                case "Take":
                    currentOrder = null;
                    operations.Add(new IncludeTakeOperation(ToCount(call.Arguments[1])));
                    break;
                default:
                    throw NotSupported(call.Method.Name);
            }
        }

        return operations;
    }

    private static IncludeOrderOperation StartOrdering(List<IncludeOperation> operations, LambdaExpression selector, bool descending)
    {
        var order = new IncludeOrderOperation();
        order.AddKey(selector, descending);
        operations.Add(order);
        return order;
    }

    private static IncludeOrderOperation ContinueOrdering(
        IncludeOrderOperation? current,
        List<IncludeOperation> operations,
        LambdaExpression selector,
        bool descending)
    {
        // ThenBy refines the most recent ordering run; if none is open (a malformed tree),
        // start a fresh ordering rather than crash.
        IncludeOrderOperation order = current ?? StartOrdering(operations, selector, descending);
        if (current is not null)
        {
            order.AddKey(selector, descending);
        }

        return order;
    }

    private static int ToCount(Expression expression) =>
        Convert.ToInt32(EvaluateConstant(expression), CultureInfo.InvariantCulture);

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

        throw new NotSupportedException(
            "An Include navigation must be a property access, optionally followed by Where / OrderBy / "
            + "OrderByDescending / ThenBy / ThenByDescending / Skip / Take on a collection navigation, "
            + "for example o => o.Customer, c => c.Orders, or c => c.Orders.Where(o => o.Open).Take(5).");
    }

    private static NotSupportedException NotSupported(string operatorName) =>
        new($"The query operator '{operatorName}' is not supported. Materialize with ToListAsync(...) and use LINQ-to-Objects for it.");
}
