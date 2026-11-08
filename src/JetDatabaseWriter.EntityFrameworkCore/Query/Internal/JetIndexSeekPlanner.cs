namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
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
    /// Attempts to construct a B-tree index query on <paramref name="reader"/> when <paramref name="conditions"/> match a seekable index on <paramref name="resolved"/>.
    /// </summary>
    /// <param name="reader">The open <see cref="AccessReader"/>.</param>
    /// <param name="resolved">The resolved local table.</param>
    /// <param name="conditions">The extracted seek conditions.</param>
    /// <param name="parameters">The runtime query parameter values.</param>
    /// <param name="wantedColumns">Optional column projection mask.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>An index-backed row sequence, or <see langword="null"/> if a full table scan is required.</returns>
    internal static async ValueTask<IAsyncEnumerable<object[]>?> TryBuildIndexQueryAsync(
        AccessReader reader,
        ResolvedTable resolved,
        IReadOnlyList<JetIndexSeekCondition> conditions,
        IReadOnlyDictionary<string, object?> parameters,
        bool[]? wantedColumns,
        CancellationToken cancellationToken)
    {
        if (conditions.Count == 0 || !reader.Services.Indexes.CanSeek)
        {
            return null;
        }

        RowCriteria seekable = [];
        TableDef tableDef = resolved.Definition;
        foreach (JetIndexSeekCondition condition in conditions)
        {
            object? evaluated = condition.ValueEvaluator(parameters);
            if (evaluated is null or DBNull)
            {
                continue;
            }

            ColumnInfo? column = tableDef.FindColumn(condition.ColumnName);
            if (column is null)
            {
                continue;
            }

            ColumnPredicateOperator rawOp = MapToColumnPredicateOperator(condition.Operator);
            Type storageType = JetEntityTypeMetadata.GetStorageClrType(condition.Property);
            if (IndexSeekFilter.TryRewriteSeekOperator(column, storageType, rawOp, evaluated, out ColumnPredicateOperator rewrittenOp))
            {
                seekable.Add(CreateColumnPredicate(condition.ColumnName, rewrittenOp, evaluated));
            }
        }

        if (seekable.Count == 0)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.Services.Indexes.ListSeekableIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
        if (indexes.Count == 0)
        {
            return null;
        }

        string tableName = resolved.Entry.Name;
        JetFormat format = reader.Database.Format;
        IndexPlan? plan = IndexPlanner.TryPlan(
            indexes,
            seekable,
            candidate => CanEncodePlan(format, tableDef, tableName, candidate));
        if (plan is null)
        {
            return null;
        }

        return reader.Services.Indexes.ReadIndexRowsAsObjectsAsync(
            tableName,
            plan.Index.Name,
            plan.Criteria,
            wantedColumns,
            cancellationToken);
    }

    /// <summary>
    /// Attempts to read rows matching any of <paramref name="keyTuples"/> using a B-tree index on <paramref name="resolved"/>.
    /// </summary>
    /// <param name="reader">The open <see cref="AccessReader"/>.</param>
    /// <param name="resolved">The resolved local table.</param>
    /// <param name="keyProperties">The key properties corresponding to each tuple element.</param>
    /// <param name="keyTuples">The distinct provider-converted key tuples to seek.</param>
    /// <param name="wantedColumns">Optional column projection mask.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The matched rows, or <see langword="null"/> if no covering seekable index is available.</returns>
    internal static async ValueTask<List<object[]>?> TryReadMultiKeyIndexRowsAsync(
        AccessReader reader,
        ResolvedTable resolved,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        bool[]? wantedColumns,
        CancellationToken cancellationToken)
    {
        if (keyProperties.Count == 0 || keyTuples.Count == 0 || !reader.Services.Indexes.CanSeek)
        {
            return null;
        }

        TableDef tableDef = resolved.Definition;
        string[] keyColumnNames = new string[keyProperties.Count];
        var keyColumns = new ColumnInfo[keyProperties.Count];
        var keyStorageTypes = new Type[keyProperties.Count];

        for (int i = 0; i < keyProperties.Count; i++)
        {
            string colName = JetEntityTypeMetadata.GetColumnName(keyProperties[i]);
            ColumnInfo? col = tableDef.FindColumn(colName);
            if (col is null)
            {
                return null;
            }

            keyColumnNames[i] = colName;
            keyColumns[i] = col;
            keyStorageTypes[i] = JetEntityTypeMetadata.GetStorageClrType(keyProperties[i]);
        }

        List<object?[]> providerTuples = [];
        foreach (object?[] rawTuple in keyTuples)
        {
            if (rawTuple.Length != keyProperties.Count)
            {
                return null;
            }

            object?[] convertedTuple = new object?[keyProperties.Count];
            for (int i = 0; i < keyProperties.Count; i++)
            {
                object? providerVal = JetEntityTypeMetadata.ConvertToProvider(keyProperties[i], rawTuple[i]);
                if (providerVal is null or DBNull
                    || !IndexSeekFilter.IsSeekable(keyColumns[i], keyStorageTypes[i], ColumnPredicateOperator.Equal, providerVal))
                {
                    return null;
                }

                convertedTuple[i] = providerVal;
            }

            providerTuples.Add(convertedTuple);
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.Services.Indexes.ListSeekableIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
        IndexMetadata? chosenIndex = SelectPrefixMatchingIndex(indexes, keyColumnNames);
        if (chosenIndex is null)
        {
            return null;
        }

        string tableName = resolved.Entry.Name;
        if (!await reader.Services.Indexes.CanSeekJoinKeysAsync(tableName, chosenIndex, providerTuples, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        bool isExact = chosenIndex.Columns.Count == keyColumnNames.Length;
        List<object[]> results = [];
        foreach (object?[] tuple in providerTuples)
        {
            IndexQueryCriteria criteria = isExact
                ? IndexQueryCriteria.Exact(tuple)
                : IndexQueryCriteria.KeyPrefix(tuple);
            await foreach (object[] row in reader.Services.Indexes.ReadIndexRowsAsObjectsAsync(
                tableName,
                chosenIndex.Name,
                criteria,
                wantedColumns,
                cancellationToken).ConfigureAwait(false))
            {
                results.Add(row);
            }
        }

        return results;
    }

    /// <summary>
    /// Builds a <see cref="RowCriteria"/> from exact equality conditions for snapshot index seeks inside an active transaction.
    /// </summary>
    /// <param name="conditions">The extracted seek conditions.</param>
    /// <param name="parameters">The runtime query parameter values.</param>
    /// <param name="tableDef">The resolved table definition.</param>
    /// <returns>A <see cref="RowCriteria"/> if at least one seekable equality condition exists; otherwise, <see langword="null"/>.</returns>
    internal static RowCriteria? TryBuildExactRowCriteria(
        IReadOnlyList<JetIndexSeekCondition> conditions,
        IReadOnlyDictionary<string, object?> parameters,
        TableDef tableDef)
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

            ColumnInfo? column = tableDef.FindColumn(condition.ColumnName);
            if (column is null)
            {
                continue;
            }

            Type storageType = JetEntityTypeMetadata.GetStorageClrType(condition.Property);
            if (IndexSeekFilter.IsSeekable(column, storageType, ColumnPredicateOperator.Equal, evaluated))
            {
                criteria.Add(ColumnPredicate.EqualTo(condition.ColumnName, evaluated));
            }
        }

        return criteria.Predicates.Count > 0 ? criteria : null;
    }

    private static ColumnPredicate CreateColumnPredicate(string columnName, ColumnPredicateOperator op, object value)
    {
        if (op == ColumnPredicateOperator.GreaterThan)
        {
            return ColumnPredicate.GreaterThan(columnName, value);
        }

        if (op == ColumnPredicateOperator.GreaterThanOrEqual)
        {
            return ColumnPredicate.GreaterThanOrEqual(columnName, value);
        }

        if (op == ColumnPredicateOperator.LessThan)
        {
            return ColumnPredicate.LessThan(columnName, value);
        }

        if (op == ColumnPredicateOperator.LessThanOrEqual)
        {
            return ColumnPredicate.LessThanOrEqual(columnName, value);
        }

        return ColumnPredicate.EqualTo(columnName, value);
    }

    private static bool CanEncodePlan(JetFormat format, TableDef definition, string tableName, IndexPlan plan)
        => (plan.Criteria.Values is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Values))
            && (plan.Criteria.Lower is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Lower.Values))
            && (plan.Criteria.Upper is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Upper.Values));

    private static IndexMetadata? SelectPrefixMatchingIndex(IReadOnlyList<IndexMetadata> indexes, string[] keyColumnNames)
    {
        List<IndexMetadata> candidates = [];
        foreach (IndexMetadata index in indexes)
        {
            if (index.FirstDp <= 0 || index.Columns.Count < keyColumnNames.Length)
            {
                continue;
            }

            bool matchesPrefix = true;
            for (int i = 0; i < keyColumnNames.Length; i++)
            {
                if (!string.Equals(index.Columns[i].Name, keyColumnNames[i], StringComparison.OrdinalIgnoreCase))
                {
                    matchesPrefix = false;
                    break;
                }
            }

            if (matchesPrefix)
            {
                candidates.Add(index);
            }
        }

        return candidates
            .OrderByDescending(idx => idx.Columns.Count == keyColumnNames.Length)
            .ThenByDescending(static idx => idx.Kind == IndexKind.PrimaryKey)
            .ThenByDescending(static idx => idx.EnforcesUniqueness)
            .ThenBy(static idx => idx.Columns.Count)
            .FirstOrDefault();
    }

    private static ColumnPredicateOperator MapToColumnPredicateOperator(JetIndexSeekOperator op)
        => op switch
        {
            JetIndexSeekOperator.Equal => ColumnPredicateOperator.Equal,
            JetIndexSeekOperator.GreaterThan => ColumnPredicateOperator.GreaterThan,
            JetIndexSeekOperator.GreaterThanOrEqual => ColumnPredicateOperator.GreaterThanOrEqual,
            JetIndexSeekOperator.LessThan => ColumnPredicateOperator.LessThan,
            JetIndexSeekOperator.LessThanOrEqual => ColumnPredicateOperator.LessThanOrEqual,
            _ => ColumnPredicateOperator.Equal,
        };

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
            && IsOperatorSupportedForProperty(leftProperty!, op)
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
            && IsOperatorSupportedForProperty(rightProperty!, reversedOp)
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

    private static bool IsOperatorSupportedForProperty(IProperty property, JetIndexSeekOperator seekOperator)
    {
        if (seekOperator == JetIndexSeekOperator.Equal)
        {
            return true;
        }

        return property.GetValueConverter() is null && property.FindTypeMapping()?.Converter is null;
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
