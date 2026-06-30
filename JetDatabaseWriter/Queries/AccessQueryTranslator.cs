namespace JetDatabaseWriter.Queries;

using System;
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
            plan.StartInclude(ResolveProperty(ExtractLambda(call.Arguments[1])));
            return;
        }

        if (AccessQueryExtensions.IsThenIncludeMethod(call.Method))
        {
            plan.ExtendInclude(ResolveProperty(ExtractLambda(call.Arguments[1])));
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

    private static PropertyInfo ResolveProperty(LambdaExpression navigation)
    {
        Expression body = navigation.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression { Member: PropertyInfo property })
        {
            return property;
        }

        throw new NotSupportedException(
            "An Include navigation must be a property access, for example o => o.Customer or c => c.Orders.");
    }

    private static NotSupportedException NotSupported(string operatorName) =>
        new($"The query operator '{operatorName}' is not supported. Materialize with ToListAsync(...) and use LINQ-to-Objects for it.");
}
