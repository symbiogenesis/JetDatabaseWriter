namespace JetDatabaseWriter.Queries;

using System;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

/// <summary>
/// Walks a LINQ query expression tree and collects the operators the provider can
/// execute into an <see cref="AccessQueryPlan"/>. Unsupported operators throw
/// <see cref="NotSupportedException"/>.
/// </summary>
internal static class AccessQueryTranslator
{
    public static AccessQueryPlan Translate(Expression expression)
    {
        var plan = new AccessQueryPlan();
        Visit(expression, plan);
        return plan;
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
            plan.Includes.Add(ResolveProperty(ExtractLambda(call.Arguments[1])));
            return;
        }

        throw NotSupported(call.Method.Name);
    }

    private static void ApplyQueryableOperator(MethodCallExpression call, AccessQueryPlan plan)
    {
        switch (call.Method.Name)
        {
            case "Where":
                plan.Predicates.Add(ExtractLambda(call.Arguments[1]));
                break;
            case "OrderBy":
                plan.Orderings.Clear();
                plan.Orderings.Add((ExtractLambda(call.Arguments[1]), false));
                break;
            case "OrderByDescending":
                plan.Orderings.Clear();
                plan.Orderings.Add((ExtractLambda(call.Arguments[1]), true));
                break;
            case "ThenBy":
                plan.Orderings.Add((ExtractLambda(call.Arguments[1]), false));
                break;
            case "ThenByDescending":
                plan.Orderings.Add((ExtractLambda(call.Arguments[1]), true));
                break;
            case "Skip":
                plan.Skip = Convert.ToInt32(EvaluateConstant(call.Arguments[1]), CultureInfo.InvariantCulture);
                break;
            case "Take":
                plan.Take = Convert.ToInt32(EvaluateConstant(call.Arguments[1]), CultureInfo.InvariantCulture);
                break;
            default:
                throw NotSupported(call.Method.Name);
        }
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
