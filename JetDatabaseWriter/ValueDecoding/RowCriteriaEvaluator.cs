namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

/// <summary>
/// Compiles a <see cref="RowCriteria"/> against a specific table's column layout
/// once, then evaluates it against decoded <c>object?[]</c> rows. Resolving column
/// names to indices up front keeps per-row evaluation allocation-free and surfaces
/// unknown-column errors before any row is scanned.
/// </summary>
internal sealed class RowCriteriaEvaluator
{
    private readonly CompiledPredicate[] predicates;

    private RowCriteriaEvaluator(CompiledPredicate[] predicates) => this.predicates = predicates;

    /// <summary>
    /// Resolves every predicate's column name against <paramref name="tableDef"/>.
    /// </summary>
    /// <param name="criteria">The criteria to compile.</param>
    /// <param name="tableDef">The target table definition.</param>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="parameterName">The public parameter name, for <see cref="ArgumentException"/>.</param>
    /// <returns>A compiled evaluator.</returns>
    /// <exception cref="ArgumentException">Thrown when a predicate names a column not in the table.</exception>
    public static RowCriteriaEvaluator Compile(
        RowCriteria criteria,
        TableDef tableDef,
        string tableName,
        string parameterName)
    {
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(tableDef, nameof(tableDef));

        var compiled = new CompiledPredicate[criteria.Predicates.Count];
        for (int i = 0; i < compiled.Length; i++)
        {
            ColumnPredicate predicate = criteria.Predicates[i];
            int columnIndex = tableDef.FindColumnIndex(predicate.ColumnName);
            if (columnIndex < 0)
            {
                throw new ArgumentException(
                    $"Column '{predicate.ColumnName}' was not found in table '{tableName}'.",
                    parameterName);
            }

            compiled[i] = new CompiledPredicate(columnIndex, predicate);
        }

        return new RowCriteriaEvaluator(compiled);
    }

    /// <summary>
    /// Evaluates the compiled criteria against one decoded row. An empty criteria
    /// matches every row.
    /// </summary>
    /// <param name="row">The decoded row values, in table-column order.</param>
    /// <returns><see langword="true"/> when every predicate is satisfied.</returns>
    public bool Matches(IReadOnlyList<object?> row)
    {
        foreach (CompiledPredicate compiled in this.predicates)
        {
            object? cellValue = compiled.ColumnIndex < row.Count ? row[compiled.ColumnIndex] : null;
            if (!Evaluate(compiled.Predicate, cellValue))
            {
                return false;
            }
        }

        return true;
    }

    private static bool Evaluate(ColumnPredicate predicate, object? cellValue)
    {
        bool cellIsNull = cellValue is null or DBNull;

        return predicate.Operator switch
        {
            ColumnPredicateOperator.Equal => ValuesEqual(cellValue, predicate.Operand),
            ColumnPredicateOperator.NotEqual => !ValuesEqual(cellValue, predicate.Operand),
            ColumnPredicateOperator.IsNull => cellIsNull,
            ColumnPredicateOperator.IsNotNull => !cellIsNull,
            ColumnPredicateOperator.In => EvaluateIn(predicate, cellValue),
            ColumnPredicateOperator.GreaterThan => !cellIsNull && Compare(cellValue!, predicate.Operand!) > 0,
            ColumnPredicateOperator.GreaterThanOrEqual => !cellIsNull && Compare(cellValue!, predicate.Operand!) >= 0,
            ColumnPredicateOperator.LessThan => !cellIsNull && Compare(cellValue!, predicate.Operand!) < 0,
            ColumnPredicateOperator.LessThanOrEqual => !cellIsNull && Compare(cellValue!, predicate.Operand!) <= 0,
            ColumnPredicateOperator.Between => !cellIsNull
                && Compare(cellValue!, predicate.Operand!) >= 0
                && Compare(cellValue!, predicate.UpperOperand!) <= 0,
            _ => throw new ArgumentOutOfRangeException(
                nameof(predicate),
                predicate.Operator,
                "Unsupported column-predicate operator."),
        };
    }

    private static bool EvaluateIn(ColumnPredicate predicate, object? cellValue)
    {
        foreach (object? candidate in predicate.Operands ?? [])
        {
            if (ValuesEqual(cellValue, candidate))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ValuesEqual(object? left, object? right)
    {
        bool leftDbNull = left is null or DBNull;
        bool rightDbNull = right is null or DBNull;
        if (leftDbNull || rightDbNull)
        {
            return leftDbNull && rightDbNull;
        }

        if (Equals(left, right))
        {
            return true;
        }

        // Numeric operands frequently arrive as a different CLR type than the
        // decoded column value (e.g. caller passes int, column decodes to long
        // or decimal). Fall back to a coerced ordered comparison for the
        // numeric/IComparable case so equality is type-tolerant.
        return TryCompareCoerced(left!, right!, out int order) && order == 0;
    }

    private static int Compare(object cellValue, object operand)
    {
        if (TryCompareCoerced(cellValue, operand, out int order))
        {
            return order;
        }

        throw new ArgumentException(
            $"Values of type '{cellValue.GetType()}' and '{operand.GetType()}' cannot be ordered for comparison.");
    }

    private static bool TryCompareCoerced(object left, object right, out int order)
    {
        order = 0;

        // Same-type IComparable is the common, exact path.
        if (left.GetType() == right.GetType() && left is IComparable sameTyped)
        {
            order = sameTyped.CompareTo(right);
            return true;
        }

        // Coerce the operand to the cell value's runtime type and compare.
        try
        {
            if (left is IComparable comparable)
            {
                object coercedRight = Convert.ChangeType(right, left.GetType(), CultureInfo.InvariantCulture);
                order = comparable.CompareTo(coercedRight);
                return true;
            }
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            // Fall through to the operand-typed attempt below.
        }

        try
        {
            if (right is IComparable comparableRight)
            {
                object coercedLeft = Convert.ChangeType(left, right.GetType(), CultureInfo.InvariantCulture);
                order = -comparableRight.CompareTo(coercedLeft);
                return true;
            }
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            return false;
        }

        return false;
    }

    private readonly struct CompiledPredicate(int columnIndex, ColumnPredicate predicate)
    {
        public int ColumnIndex { get; } = columnIndex;

        public ColumnPredicate Predicate { get; } = predicate;
    }
}
