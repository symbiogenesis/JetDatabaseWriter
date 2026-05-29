namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using ClosedXML.Parser;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static class CalculatedExpressionEvaluator
{
    public static void Apply(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, bool force)
    {
        var context = new EvaluationContext(tableDef, constraints, values, force);
        for (int i = 0; i < constraints.Count; i++)
        {
            ColumnConstraint constraint = constraints[i];
            if (constraint.IsCalculated && (force || IsNull(values[i])))
            {
                values[i] = context.EvaluateColumn(i);
            }
        }
    }

    internal sealed class Plan
    {
        private Plan(CalculatedExpressionNode root, Dictionary<string, string> placeholderToColumn)
        {
            this.Root = root;
            this.PlaceholderToColumn = placeholderToColumn;
        }

        public CalculatedExpressionNode Root { get; }

        public Dictionary<string, string> PlaceholderToColumn { get; }

        public static Plan Parse(string expression)
        {
            ValidateExpressionShape(expression, MaxExpressionLength, "Calculated-column expression");
            string normalized = CalculatedExpressionNormalizer.Normalize(expression, out Dictionary<string, string>? placeholderToColumn);
            ValidateExpressionShape(normalized, MaxNormalizedExpressionLength, "Normalized calculated-column expression");
            try
            {
                CalculatedExpressionNode root = FormulaParser<CalculatedExpressionNode, CalculatedExpressionNode, Dictionary<string, string>>.CellFormulaA1(
                    normalized,
                    placeholderToColumn,
                    CalculatedExpressionAstFactory.Instance);
                return new Plan(root, placeholderToColumn);
            }
            catch (ParsingException ex)
            {
                throw new ArgumentException($"Calculated-column expression '{expression}' is not valid expression syntax.", nameof(expression), ex);
            }
        }
    }

    internal sealed class EvaluationContext
    {
        private readonly IReadOnlyList<ColumnConstraint> constraints;
        private readonly object[] values;
        private readonly bool force;
        private readonly Dictionary<string, int> columnIndexes;
        private readonly bool[] evaluating;
        private readonly bool[] evaluated;
        private double? lastRandomValue;

        public EvaluationContext(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, bool force)
        {
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
            try
            {
                constraint.CalculatedExpressionPlan ??= Plan.Parse(constraint.CalculationExpression);
                object raw = constraint.CalculatedExpressionPlan.Root.Evaluate(this, constraint.CalculatedExpressionPlan);
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
            finally
            {
                this.evaluating[index] = false;
            }
        }

        public object GetNameValue(string name, Plan plan)
        {
            if (plan.PlaceholderToColumn.TryGetValue(name, out string? columnName))
            {
                name = columnName;
            }

            if (!this.columnIndexes.TryGetValue(name, out int index))
            {
                throw new InvalidOperationException($"Calculated-column expression references unknown name '{name}'.");
            }

            ColumnConstraint referenced = this.constraints[index];
            return referenced.IsCalculated ? this.EvaluateColumn(index) : this.values[index] ?? DBNull.Value;
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
}
