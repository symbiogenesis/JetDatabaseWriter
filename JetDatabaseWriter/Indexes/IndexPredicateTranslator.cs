namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Mapping;
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
/// Each property resolves to its column through <see cref="EntityMap"/>, the same
/// mapping the POCO row mapper uses, so <c>[Column("...")]</c> renames are honoured and
/// <c>[NotMapped]</c> properties are never pushed. The reader matches the column names
/// to index key columns case-insensitively.
/// </para>
/// <para>
/// The translator does not know the columns' types. Before it plans a seek, the reader keeps
/// only the comparisons <see cref="IndexSeekFilter"/> finds a seek answers exactly, for
/// example not an enum property bound to a Text column, whose operand is the enum's integer.
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
    [RequiresUnreferencedCode("Runtime predicates discover the entity mapping from expression metadata.")]
    public static RowCriteria ExtractPushableCriteria(LambdaExpression predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        return ExtractPushableCriteria(predicate, predicate.Parameters.Count == 1 ? EntityMap.For(predicate.Parameters[0].Type) : null);
    }

    /// <summary>Extracts typed predicates while preserving the entity's mapped properties.</summary>
    /// <typeparam name="T">The mapped entity type.</typeparam>
    /// <param name="predicate">The row predicate.</param>
    /// <returns>The index-seekable criteria.</returns>
    public static RowCriteria ExtractPushableCriteria<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, bool>> predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        return ExtractPushableCriteria(predicate, EntityMap.For(typeof(T)));
    }

    private static RowCriteria ExtractPushableCriteria(LambdaExpression predicate, EntityMap? map)
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
            if (TryTranslateComparison(conjunct, parameter, map!, out ColumnPredicate? model))
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
        EntityMap map,
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

        if (TryResolveColumn(binary.Left, parameter, map, out string? leftColumn)
            && IsConstant(binary.Right, parameter))
        {
            predicate = Build(leftColumn!, @operator, EvaluateValue(binary.Right));
            return predicate is not null;
        }

        if (TryResolveColumn(binary.Right, parameter, map, out string? rightColumn)
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

    private static bool TryResolveColumn(Expression expression, ParameterExpression parameter, EntityMap map, out string? columnName)
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

        // The column is the one the row mapper binds the property to, so a
        // [Column("Last Name")] property seeks the "Last Name" index. A
        // [NotMapped] or read-only property is not a column and is left to the
        // client-side filter.
        if (map.FindByMember(property) is not EntityProperty mapped)
        {
            return false;
        }

        columnName = mapped.ColumnName;
        return true;
    }

    private static bool IsConstant(Expression expression, ParameterExpression parameter)
    {
        var finder = new ParameterUsageFinder(parameter);
        finder.Visit(expression);
        return !finder.Found;
    }

    /// <summary>
    /// Evaluates the value side of a comparison. Constants and captured variables, including
    /// field and property chains such as <c>filter.Range.Low</c>, are read without compiling;
    /// a computed operand such as <c>DateTime.Today</c> or <c>id + 1</c> is compiled once.
    /// </summary>
    /// <param name="expression">An operand that does not reference the predicate's parameter.</param>
    /// <returns>The operand's value.</returns>
    private static object? EvaluateValue(Expression expression) => ClosureValueReader.Evaluate(expression);

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
