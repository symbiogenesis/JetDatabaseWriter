namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

/// <summary>
/// Extracts seekable predicates from LINQ expressions and selects optimal B-tree index queries on <see cref="AccessReader"/>.
/// </summary>
internal static class JetIndexSeekPlanner
{
    /// <summary>
    /// Extracts index-seekable conditions from <paramref name="predicate"/> for <paramref name="entityType"/>.
    /// </summary>
    /// <param name="entityType">The target entity type.</param>
    /// <param name="predicate">The filter lambda expression.</param>
    /// <returns>A list of extracted seek conditions.</returns>
    internal static List<JetIndexSeekCondition> ExtractSeekConditions(IEntityType entityType, LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(predicate);

        List<JetIndexSeekCondition> conditions = [];
        if (predicate.Parameters.Count != 1)
        {
            return conditions;
        }

        ParameterExpression entityParam = predicate.Parameters[0];
        ExtractConjunctions(entityType, entityParam, predicate.Body, conditions);
        return conditions;
    }

    /// <summary>
    /// Attempts to construct a B-tree index query on <paramref name="reader"/> when <paramref name="conditions"/> match an available index.
    /// </summary>
    /// <param name="reader">The open <see cref="AccessReader"/>.</param>
    /// <param name="tableName">The physical table name.</param>
    /// <param name="indexes">The table's available indexes.</param>
    /// <param name="conditions">The extracted seek conditions.</param>
    /// <param name="parameters">The runtime query parameter values.</param>
    /// <returns>An index-backed row sequence, or <see langword="null"/> if a full table scan is required.</returns>
    internal static IAsyncEnumerable<object[]>? TryBuildIndexQuery(
        AccessReader reader,
        string tableName,
        IReadOnlyList<IndexMetadata> indexes,
        IReadOnlyList<JetIndexSeekCondition> conditions,
        IReadOnlyDictionary<string, object?> parameters)
    {
        if (indexes.Count == 0 || conditions.Count == 0)
        {
            return null;
        }

        Dictionary<string, object?> equalities = [with(StringComparer.OrdinalIgnoreCase)];
        Dictionary<string, (object? Value, bool Inclusive)> lowerBounds = [with(StringComparer.OrdinalIgnoreCase)];
        Dictionary<string, (object? Value, bool Inclusive)> upperBounds = [with(StringComparer.OrdinalIgnoreCase)];

        foreach (JetIndexSeekCondition condition in conditions)
        {
            object? evaluated = condition.ValueEvaluator(parameters);
            if (evaluated is null or DBNull)
            {
                continue;
            }

            if (condition.Operator == JetIndexSeekOperator.Equal)
            {
                equalities[condition.ColumnName] = evaluated;
            }
            else if (condition.Operator == JetIndexSeekOperator.GreaterThan)
            {
                lowerBounds[condition.ColumnName] = (evaluated, false);
            }
            else if (condition.Operator == JetIndexSeekOperator.GreaterThanOrEqual)
            {
                lowerBounds[condition.ColumnName] = (evaluated, true);
            }
            else if (condition.Operator == JetIndexSeekOperator.LessThan)
            {
                upperBounds[condition.ColumnName] = (evaluated, false);
            }
            else if (condition.Operator == JetIndexSeekOperator.LessThanOrEqual)
            {
                upperBounds[condition.ColumnName] = (evaluated, true);
            }
        }

        List<IndexMetadata> orderedIndexes =
        [
            .. indexes
                .Where(static idx => idx.Columns.Count > 0)
                .OrderByDescending(static idx => idx.Kind == IndexKind.PrimaryKey)
                .ThenByDescending(static idx => idx.EnforcesUniqueness)
                .ThenBy(static idx => idx.Columns.Count),
        ];

        foreach (IndexMetadata index in orderedIndexes)
        {
            object?[] keyValues = new object?[index.Columns.Count];
            bool allMatched = true;
            for (int i = 0; i < index.Columns.Count; i++)
            {
                if (!equalities.TryGetValue(index.Columns[i].Name, out object? val) || val is null)
                {
                    allMatched = false;
                    break;
                }

                keyValues[i] = val;
            }

            if (allMatched)
            {
                return reader.FromIndex(tableName, index.Name).WhereEquals(keyValues).ToRowsAsync();
            }
        }

        foreach (IndexMetadata index in orderedIndexes)
        {
            if (index.Columns.Count <= 1)
            {
                continue;
            }

            List<object?> prefixValues = [];
            for (int i = 0; i < index.Columns.Count; i++)
            {
                if (equalities.TryGetValue(index.Columns[i].Name, out object? val) && val is not null)
                {
                    prefixValues.Add(val);
                }
                else
                {
                    break;
                }
            }

            if (prefixValues.Count > 0)
            {
                return reader.FromIndex(tableName, index.Name).WhereKeyPrefix([.. prefixValues]).ToRowsAsync();
            }
        }

        foreach (IndexMetadata index in orderedIndexes)
        {
            IndexColumnReference firstCol = index.Columns[0];
            if (!firstCol.IsAscending)
            {
                continue;
            }

            bool hasLower = lowerBounds.TryGetValue(firstCol.Name, out (object? Value, bool Inclusive) lower);
            bool hasUpper = upperBounds.TryGetValue(firstCol.Name, out (object? Value, bool Inclusive) upper);
            if (!hasLower && !hasUpper)
            {
                continue;
            }

            IndexKeyBound? lowerBound = CreateKeyBound(hasLower, lower);
            IndexKeyBound? upperBound = CreateKeyBound(hasUpper, upper);
            return reader.FromIndex(tableName, index.Name).WhereRange(lowerBound, upperBound).ToRowsAsync();
        }

        return null;
    }

    /// <summary>
    /// Builds a <see cref="RowCriteria"/> from exact equality conditions for snapshot index seeks inside an active transaction.
    /// </summary>
    /// <param name="conditions">The extracted seek conditions.</param>
    /// <param name="parameters">The runtime query parameter values.</param>
    /// <returns>A <see cref="RowCriteria"/> if at least one non-null equality condition exists; otherwise, <see langword="null"/>.</returns>
    internal static RowCriteria? TryBuildExactRowCriteria(
        IReadOnlyList<JetIndexSeekCondition> conditions,
        IReadOnlyDictionary<string, object?> parameters)
    {
        RowCriteria criteria = [];
        foreach (JetIndexSeekCondition condition in conditions)
        {
            if (condition.Operator != JetIndexSeekOperator.Equal)
            {
                continue;
            }

            object? evaluated = condition.ValueEvaluator(parameters);
            if (evaluated is null or DBNull)
            {
                continue;
            }

            criteria.Add(ColumnPredicate.EqualTo(condition.ColumnName, evaluated));
        }

        return criteria.Predicates.Count > 0 ? criteria : null;
    }

    private static IndexKeyBound? CreateKeyBound(bool hasBound, (object? Value, bool Inclusive) bound)
    {
        if (!hasBound)
        {
            return null;
        }

        return bound.Inclusive
            ? IndexKeyBound.Inclusive(bound.Value)
            : IndexKeyBound.Exclusive(bound.Value);
    }

    private static void ExtractConjunctions(
        IEntityType entityType,
        ParameterExpression entityParam,
        Expression expression,
        List<JetIndexSeekCondition> conditions)
    {
        Expression unwrapped = StripQuotesAndConverts(expression);
        if (unwrapped is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            ExtractConjunctions(entityType, entityParam, andAlso.Left, conditions);
            ExtractConjunctions(entityType, entityParam, andAlso.Right, conditions);
            return;
        }

        if (unwrapped is not BinaryExpression binary)
        {
            return;
        }

        if (TryMapOperator(binary.NodeType, reversed: false, out JetIndexSeekOperator op)
            && TryExtractPropertyAccess(entityType, entityParam, binary.Left, out IProperty? leftProperty)
            && !ReferencesParameter(binary.Right, entityParam)
            && TryBuildValueEvaluator(leftProperty!, binary.Right, out Func<IReadOnlyDictionary<string, object?>, object?>? rightEval))
        {
            conditions.Add(new JetIndexSeekCondition(
                leftProperty!,
                JetEntityTypeMetadata.GetColumnName(leftProperty!),
                op,
                rightEval!));
            return;
        }

        if (TryMapOperator(binary.NodeType, reversed: true, out JetIndexSeekOperator reversedOp)
            && TryExtractPropertyAccess(entityType, entityParam, binary.Right, out IProperty? rightProperty)
            && !ReferencesParameter(binary.Left, entityParam)
            && TryBuildValueEvaluator(rightProperty!, binary.Left, out Func<IReadOnlyDictionary<string, object?>, object?>? leftEval))
        {
            conditions.Add(new JetIndexSeekCondition(
                rightProperty!,
                JetEntityTypeMetadata.GetColumnName(rightProperty!),
                reversedOp,
                leftEval!));
        }
    }

    private static bool TryMapOperator(ExpressionType nodeType, bool reversed, out JetIndexSeekOperator seekOperator)
    {
        if (nodeType == ExpressionType.Equal)
        {
            seekOperator = JetIndexSeekOperator.Equal;
            return true;
        }

        if (nodeType == ExpressionType.GreaterThan)
        {
            seekOperator = reversed ? JetIndexSeekOperator.LessThan : JetIndexSeekOperator.GreaterThan;
            return true;
        }

        if (nodeType == ExpressionType.GreaterThanOrEqual)
        {
            seekOperator = reversed ? JetIndexSeekOperator.LessThanOrEqual : JetIndexSeekOperator.GreaterThanOrEqual;
            return true;
        }

        if (nodeType == ExpressionType.LessThan)
        {
            seekOperator = reversed ? JetIndexSeekOperator.GreaterThan : JetIndexSeekOperator.LessThan;
            return true;
        }

        if (nodeType == ExpressionType.LessThanOrEqual)
        {
            seekOperator = reversed ? JetIndexSeekOperator.GreaterThanOrEqual : JetIndexSeekOperator.LessThanOrEqual;
            return true;
        }

        seekOperator = default;
        return false;
    }

    private static bool TryExtractPropertyAccess(
        IEntityType entityType,
        ParameterExpression entityParam,
        Expression candidate,
        out IProperty? property)
    {
        Expression current = StripQuotesAndConverts(candidate);
        if (current is MemberExpression memberExpr
            && StripQuotesAndConverts(memberExpr.Expression!) == entityParam)
        {
            property = entityType.FindProperty(memberExpr.Member);
            return property is not null;
        }

        if (current is MethodCallExpression { Method.Name: nameof(EF.Property) } callExpr
            && callExpr.Method.DeclaringType == typeof(EF)
            && callExpr.Arguments.Count == 2
            && StripQuotesAndConverts(callExpr.Arguments[0]) == entityParam
            && StripQuotesAndConverts(callExpr.Arguments[1]) is ConstantExpression { Value: string propName })
        {
            property = entityType.FindProperty(propName);
            return property is not null;
        }

        property = null;
        return false;
    }

    private static bool TryBuildValueEvaluator(
        IProperty property,
        Expression valueExpression,
        out Func<IReadOnlyDictionary<string, object?>, object?>? evaluator)
    {
        Expression unwrapped = StripQuotesAndConverts(valueExpression);
        if (unwrapped is ConstantExpression constantExpr)
        {
            object? converted = JetEntityTypeMetadata.ConvertToProvider(property, constantExpr.Value);
            evaluator = _ => converted;
            return true;
        }

        if (unwrapped is QueryParameterExpression queryParam)
        {
            string paramName = queryParam.Name;
            evaluator = parameters =>
                parameters.TryGetValue(paramName, out object? raw)
                    ? JetEntityTypeMetadata.ConvertToProvider(property, raw)
                    : null;
            return true;
        }

        ParameterExpression dictParam = Expression.Parameter(typeof(IReadOnlyDictionary<string, object?>), "parameters");
        ParameterRewritingVisitor rewriter = new(dictParam);
        Expression rewritten = rewriter.Visit(valueExpression);
        if (!rewriter.IsSupported)
        {
            evaluator = null;
            return false;
        }

        Expression boxed = Expression.Convert(rewritten, typeof(object));
        Func<IReadOnlyDictionary<string, object?>, object?> compiled =
            Expression.Lambda<Func<IReadOnlyDictionary<string, object?>, object?>>(boxed, dictParam).Compile();
        evaluator = parameters => JetEntityTypeMetadata.ConvertToProvider(property, compiled(parameters));
        return true;
    }

    private static Expression StripQuotesAndConverts(Expression expression)
    {
        Expression current = expression;
        while (current is UnaryExpression unary
               && unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.Quote)
        {
            current = unary.Operand;
        }

        return current;
    }

    private static bool ReferencesParameter(Expression expression, ParameterExpression parameter)
    {
        ParameterReferenceDetector detector = new(parameter);
        _ = detector.Visit(expression);
        return detector.Found;
    }

    private sealed class ParameterReferenceDetector(ParameterExpression target) : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == target)
            {
                this.Found = true;
            }

            return base.VisitParameter(node);
        }
    }

    private sealed class ParameterRewritingVisitor(ParameterExpression dictionaryParameter) : ExpressionVisitor
    {
        private static readonly MethodInfo GetValueOrDefaultMethod =
            typeof(ParameterRewritingVisitor).GetMethod(
                nameof(GetParameterValue),
                BindingFlags.NonPublic | BindingFlags.Static)!;

        public bool IsSupported { get; private set; } = true;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is QueryParameterExpression queryParam)
            {
                MethodInfo typedMethod = GetValueOrDefaultMethod.MakeGenericMethod(queryParam.Type);
                return Expression.Call(
                    typedMethod,
                    dictionaryParameter,
                    Expression.Constant(queryParam.Name));
            }

            this.IsSupported = false;
            return node;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node != dictionaryParameter)
            {
                this.IsSupported = false;
            }

            return base.VisitParameter(node);
        }

        private static T? GetParameterValue<T>(IReadOnlyDictionary<string, object?> parameters, string name)
            => parameters.TryGetValue(name, out object? val) && val is T typed ? typed : default;
    }
}
