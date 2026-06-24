namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

/// <summary>
/// Extracts the index-seekable necessary conditions from a typed row predicate so
/// the reader can infer an index for it.
/// </summary>
/// <remarks>
/// <para>
/// Only conjuncts combined with logical AND (<c>&amp;&amp;</c>) are extracted.
/// Each AND conjunct is a necessary condition that every matching row must
/// satisfy, so an index seek built from any subset of them returns a
/// <em>superset</em> of the true matches; the caller then applies the fully
/// compiled predicate to discard the surplus. Conjuncts the translator cannot
/// model — OR branches, method calls, computed members such as
/// <c>o.When.Year</c>, column-to-column comparisons, and so on — are simply
/// omitted and left to that client-side filter, so the extracted criteria are
/// always sound.
/// </para>
/// <para>
/// Property names are used verbatim as column names; the reader matches them to
/// index key columns case-insensitively, exactly as the POCO row mapper matches
/// properties to columns.
/// </para>
/// </remarks>
internal static class IndexPredicateTranslator
{
    private static readonly Dictionary<ExpressionType, ColumnPredicateOperator> OperatorMap = new()
    {
        [ExpressionType.Equal] = ColumnPredicateOperator.Equal,
        [ExpressionType.GreaterThan] = ColumnPredicateOperator.GreaterThan,
        [ExpressionType.GreaterThanOrEqual] = ColumnPredicateOperator.GreaterThanOrEqual,
        [ExpressionType.LessThan] = ColumnPredicateOperator.LessThan,
        [ExpressionType.LessThanOrEqual] = ColumnPredicateOperator.LessThanOrEqual,
    };

    /// <summary>
    /// Pulls the AND-combined comparisons that can be modelled as
    /// <see cref="ColumnPredicate"/> values out of <paramref name="predicate"/>.
    /// </summary>
    /// <param name="predicate">The row predicate to inspect.</param>
    /// <returns>
    /// The pushable conjuncts as a <see cref="RowCriteria"/>. The result is always
    /// a (possibly empty) subset of the predicate's necessary conditions.
    /// </returns>
    public static RowCriteria ExtractPushableCriteria(LambdaExpression predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));

        var criteria = new RowCriteria();
        if (predicate.Parameters.Count != 1)
        {
            return criteria;
        }

        ParameterExpression parameter = predicate.Parameters[0];
        var conjuncts = new List<Expression>();
        CollectAndConjuncts(predicate.Body, conjuncts);

        foreach (Expression conjunct in conjuncts)
        {
            if (TryTranslateComparison(conjunct, parameter, out ColumnPredicate? model))
            {
                criteria.Add(model);
            }
        }

        return criteria;
    }

    private static void CollectAndConjuncts(Expression expression, List<Expression> conjuncts)
    {
        Expression unwrapped = StripConvert(expression);
        if (unwrapped is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
        {
            CollectAndConjuncts(and.Left, conjuncts);
            CollectAndConjuncts(and.Right, conjuncts);
            return;
        }

        conjuncts.Add(unwrapped);
    }

    private static bool TryTranslateComparison(
        Expression expression,
        ParameterExpression parameter,
        [NotNullWhen(true)] out ColumnPredicate? predicate)
    {
        predicate = null;
        if (expression is not BinaryExpression binary)
        {
            return false;
        }

        if (MapOperator(binary.NodeType) is not ColumnPredicateOperator @operator)
        {
            return false;
        }

        if (TryResolveColumn(binary.Left, parameter, out string? leftColumn)
            && IsConstant(binary.Right, parameter))
        {
            predicate = Build(leftColumn!, @operator, EvaluateValue(binary.Right));
            return predicate is not null;
        }

        if (TryResolveColumn(binary.Right, parameter, out string? rightColumn)
            && IsConstant(binary.Left, parameter))
        {
            // `value < o.X`  ==>  `o.X > value`.
            predicate = Build(rightColumn!, Flip(@operator), EvaluateValue(binary.Left));
            return predicate is not null;
        }

        return false;
    }

    private static ColumnPredicate? Build(string column, ColumnPredicateOperator @operator, object? value) => @operator switch
    {
        ColumnPredicateOperator.Equal => ColumnPredicate.EqualTo(column, value),
        ColumnPredicateOperator.GreaterThan => value is null ? null : ColumnPredicate.GreaterThan(column, value),
        ColumnPredicateOperator.GreaterThanOrEqual => value is null ? null : ColumnPredicate.GreaterThanOrEqual(column, value),
        ColumnPredicateOperator.LessThan => value is null ? null : ColumnPredicate.LessThan(column, value),
        ColumnPredicateOperator.LessThanOrEqual => value is null ? null : ColumnPredicate.LessThanOrEqual(column, value),
        ColumnPredicateOperator.NotEqual => null,
        ColumnPredicateOperator.Between => null,
        ColumnPredicateOperator.In => null,
        ColumnPredicateOperator.IsNull => null,
        ColumnPredicateOperator.IsNotNull => null,
        _ => null,
    };

    private static ColumnPredicateOperator? MapOperator(ExpressionType nodeType) =>
        OperatorMap.TryGetValue(nodeType, out ColumnPredicateOperator @operator) ? @operator : null;

    private static ColumnPredicateOperator Flip(ColumnPredicateOperator @operator) => @operator switch
    {
        ColumnPredicateOperator.GreaterThan => ColumnPredicateOperator.LessThan,
        ColumnPredicateOperator.GreaterThanOrEqual => ColumnPredicateOperator.LessThanOrEqual,
        ColumnPredicateOperator.LessThan => ColumnPredicateOperator.GreaterThan,
        ColumnPredicateOperator.LessThanOrEqual => ColumnPredicateOperator.GreaterThanOrEqual,
        ColumnPredicateOperator.Equal => ColumnPredicateOperator.Equal,
        ColumnPredicateOperator.NotEqual => ColumnPredicateOperator.NotEqual,
        ColumnPredicateOperator.Between => ColumnPredicateOperator.Between,
        ColumnPredicateOperator.In => ColumnPredicateOperator.In,
        ColumnPredicateOperator.IsNull => ColumnPredicateOperator.IsNull,
        ColumnPredicateOperator.IsNotNull => ColumnPredicateOperator.IsNotNull,
        _ => @operator,
    };

    private static bool TryResolveColumn(Expression expression, ParameterExpression parameter, out string? columnName)
    {
        columnName = null;
        if (StripConvert(expression) is not MemberExpression member)
        {
            return false;
        }

        if (member.Member is not PropertyInfo property)
        {
            return false;
        }

        // Only a direct member of the lambda parameter (`o.Column`) maps to a
        // column. Nested members such as `o.When.Year` resolve their inner
        // expression to another member, not the parameter, and are left to the
        // client-side filter.
        if (member.Expression is null || StripConvert(member.Expression) != parameter)
        {
            return false;
        }

        columnName = property.Name;
        return true;
    }

    private static bool IsConstant(Expression expression, ParameterExpression parameter)
    {
        var finder = new ParameterUsageFinder(parameter);
        finder.Visit(expression);
        return !finder.Found;
    }

    private static object? EvaluateValue(Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        // Compile the operand to recover captured variables, field/property
        // reads, and computed constants (for example a closed-over `start`
        // variable or `DateTime.Today`). Box value types so the delegate
        // returns object.
        Expression body = expression.Type.IsValueType
            ? Expression.Convert(expression, typeof(object))
            : expression;
        Func<object?> evaluator = Expression.Lambda<Func<object?>>(body).Compile();
        return evaluator();
    }

    private static Expression StripConvert(Expression expression)
    {
        Expression current = expression;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            current = unary.Operand;
        }

        return current;
    }

    private sealed class ParameterUsageFinder(ParameterExpression parameter) : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == parameter)
            {
                this.Found = true;
            }

            return base.VisitParameter(node);
        }
    }
}
