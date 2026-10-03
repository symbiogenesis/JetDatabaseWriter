namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionEvaluationContext
{
    private readonly IReadOnlyList<ColumnConstraint> constraints;
    private readonly object[] values;
    private readonly bool force;
    private readonly string? tableName;
    private readonly Dictionary<string, int> columnIndexes;
    private readonly bool[] evaluating;
    private readonly bool[] evaluated;
    private double? lastRandomValue;

    public CalculatedExpressionEvaluationContext(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, bool force, string? tableName = null)
    {
        this.tableName = tableName;
        this.constraints = constraints;
        this.values = values;
        this.force = force;
        this.evaluating = new bool[constraints.Count];
        this.evaluated = new bool[constraints.Count];
        this.columnIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            this.columnIndexes[tableDef.Columns[i].Name] = i;
        }
    }

    public object EvaluateColumn(int index)
    {
        object current = this.values[index];
        ColumnConstraint constraint = this.constraints[index];
        if (!constraint.IsCalculated)
        {
            return current ?? DBNull.Value;
        }

        if (this.evaluated[index])
        {
            return this.values[index] ?? DBNull.Value;
        }

        if (!this.force && !IsNull(current))
        {
            this.evaluated[index] = true;
            return current;
        }

        if (this.evaluating[index])
        {
            throw new InvalidOperationException($"Calculated column '{constraint.Name}' participates in a circular expression dependency.");
        }

        if (string.IsNullOrWhiteSpace(constraint.CalculationExpression))
        {
            return current ?? DBNull.Value;
        }

        this.evaluating[index] = true;
        object? raw = null;
        bool storing = false;
        try
        {
            constraint.CalculatedExpressionPlan ??= CalculatedExpressionPlan.Parse(constraint.CalculationExpression);
            raw = constraint.CalculatedExpressionPlan.Root.Evaluate(this, constraint.CalculatedExpressionPlan);
            storing = true;
            object coerced = CoerceResult(raw, constraint.ClrType);
            this.values[index] = coerced;
            this.evaluated[index] = true;
            return coerced;
        }
        catch (NotSupportedException) when (!this.force && !IsNull(current))
        {
            this.evaluated[index] = true;
            return current;
        }
        catch (Exception ex) when (CalculatedExpressionErrors.IsWrappable(ex) && !CalculatedExpressionErrors.NamesColumn(ex))
        {
            // Name the column, table and expression; a failure in a calculated
            // column this one references already names that column.
            throw CalculatedExpressionErrors.ForColumn(constraint, this.tableName, ex, storing, raw);
        }
        finally
        {
            this.evaluating[index] = false;
        }
    }

    public object GetNameValue(string name, CalculatedExpressionPlan plan)
    {
        if (plan.PlaceholderToColumn.TryGetValue(name, out string? columnName))
        {
            name = columnName;
        }

        if (!this.columnIndexes.TryGetValue(name, out int index))
        {
            throw new InvalidOperationException($"Calculated-column expression references unknown name '{name}'.");
        }

        // Callers may pass "10" for an Integer field or 10 for a Text field;
        // the expression must see the value as the field holds it.
        ColumnConstraint referenced = this.constraints[index];
        object value = referenced.IsCalculated ? this.EvaluateColumn(index) : this.values[index];
        return CoerceInput(value, referenced.ClrType);
    }

    public double NextRandom(object? seed)
    {
        if (IsNull(seed))
        {
            return this.GenerateRandomValue();
        }

        double seedValue = ToDouble(seed);
        if (seedValue < 0d)
        {
            this.lastRandomValue = DeterministicRandomValue(seedValue);
            return this.lastRandomValue.Value;
        }

        if (seedValue == 0d && this.lastRandomValue.HasValue)
        {
            return this.lastRandomValue.Value;
        }

        return this.GenerateRandomValue();
    }

    private double GenerateRandomValue()
    {
        this.lastRandomValue = RandomNumberGenerator.GetInt32(0, int.MaxValue) / (double)int.MaxValue;
        return this.lastRandomValue.Value;
    }
}
