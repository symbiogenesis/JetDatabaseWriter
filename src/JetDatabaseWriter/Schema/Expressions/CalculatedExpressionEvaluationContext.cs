namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionEvaluationContext
{
    private readonly IReadOnlyList<ColumnConstraint> constraints;
    private readonly IReadOnlyList<ColumnInfo> columns;
    private readonly object[] values;
    private readonly string? tableName;
    private readonly CultureInfo calculatedCulture;
    private readonly Dictionary<string, int> columnIndexes;
    private readonly bool[] evaluating;
    private readonly bool[] evaluated;
    private double? lastRandomValue;
    private int calculationDepth;
    private bool liveStaleReferences;

    public CalculatedExpressionEvaluationContext(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, string? tableName = null, JetDatabaseWriter.Indexes.Collation.JetTextCollation? textCollation = null, CultureInfo? calculatedCulture = null)
    {
        this.tableName = tableName;
        this.calculatedCulture = calculatedCulture ?? CultureInfo.GetCultureInfo("en-US");
        this.TextCollation = textCollation ?? JetDatabaseWriter.Indexes.Collation.JetTextCollation.GeneralLegacy;
        this.constraints = constraints;
        this.columns = tableDef.Columns;
        this.values = values;
        this.evaluating = new bool[constraints.Count];
        this.evaluated = new bool[constraints.Count];
        this.columnIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            this.columnIndexes[tableDef.Columns[i].Name] = i;
        }
    }

    internal JetDatabaseWriter.Indexes.Collation.JetTextCollation TextCollation { get; }

    internal bool IsCalculatingColumn => this.calculationDepth != 0;

    public static CalculatedExpressionEvaluationContext FromCalculatedRow(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, string? tableName = null, JetDatabaseWriter.Indexes.Collation.JetTextCollation? textCollation = null, bool liveStaleReferences = false)
    {
        var context = new CalculatedExpressionEvaluationContext(tableDef, constraints, values, tableName, textCollation) { liveStaleReferences = liveStaleReferences };

        // Validation consumes the finished row, including calculated Nulls and
        // volatile results, without running those expressions a second time.
        Array.Fill(context.evaluated, true);
        return context;
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

        if (this.evaluating[index])
        {
            throw new JetOperationException(JetErrorCode.InvalidExpression, $"Calculated column '{constraint.Name}' participates in a circular expression dependency.");
        }

        this.evaluating[index] = true;
        this.calculationDepth++;
        CultureInfo? previousCulture = CalculatedExpressionRegionalProfile.Exchange(this.calculatedCulture);
        object? raw = null;
        bool storing = false;
        try
        {
            if (string.IsNullOrWhiteSpace(constraint.CalculationExpression))
            {
                throw new JetOperationException(JetErrorCode.InvalidExpression, "The calculated column has no calculation expression.");
            }

            if (constraint.CalculatedExpressionPlan is null)
            {
                CalculatedExpressionBindings.ValidatePlan(this.constraints, index);
                constraint.CalculatedExpressionPlan = CalculatedExpressionParser.Parse(constraint.CalculationExpression);
            }

            if (this.tableName is not null && constraint.CalculatedExpressionPlan.HasStaleQualifier(this.tableName))
            {
                // Native table rename leaves the old cache untouched, even for
                // an unused branch. Missing fields still refuse mutation.
                CalculatedExpressionBindings.ValidatePlan(this.constraints, index);
                this.evaluated[index] = true;
                return current ?? CalculatedError.Uncached;
            }

            raw = constraint.CalculatedExpressionPlan.Evaluate(this);
            storing = true;
            object coerced;
            if (raw is not CalculatedError && !IsNull(raw) && constraint.CalculatedResultType == ColumnType.DateTimeType)
            {
                coerced = ToNativeDate(raw);
            }
            else
            {
                coerced = raw is CalculatedError ? raw : CoerceResult(raw, constraint.ClrType, isCalculatedColumn: true);
            }

            if (constraint.CalculatedResultType == Enums.ColumnType.MoneyType && coerced is decimal currency)
            {
                coerced = decimal.FromOACurrency(decimal.ToOACurrency(currency));
            }

            this.values[index] = coerced;
            this.evaluated[index] = true;
            return coerced;
        }
        catch (Exception ex) when (storing && ex is OverflowException or InvalidCastException or FormatException)
        {
            var error = new CalculatedError(ex is OverflowException ? 6 : 13);
            this.values[index] = error;
            this.evaluated[index] = true;
            return error;
        }
        catch (Exception ex) when (CalculatedExpressionErrors.IsWrappable(ex) && !CalculatedExpressionErrors.NamesColumn(ex))
        {
            // Name the column, table and expression; a failure in a calculated
            // column this one references already names that column.
            throw CalculatedExpressionErrors.ForColumn(constraint, this.tableName, ex, storing, raw);
        }
        finally
        {
            CalculatedExpressionRegionalProfile.Exchange(previousCulture);
            this.evaluating[index] = false;
            this.calculationDepth--;
        }
    }

    public object GetFieldValue(string name, string? qualifier = null, bool rowLocalQualifier = false)
    {
        // DAO binds a bare qualifier (even another existing table name) to this
        // row. A stale bracketed qualifier yields Null after a table rename.
        if (qualifier is not null && this.tableName is null)
        {
            throw new JetNotSupportedException(JetErrorCode.UnsupportedExpression, $"Expression reference to table '{qualifier}' cannot be evaluated without the current table context.");
        }

        if (!this.columnIndexes.TryGetValue(name, out int index))
        {
            throw new JetOperationException(JetErrorCode.InvalidExpression, $"Calculated-column expression references unknown name '{name}'.");
        }

        if (qualifier is not null && !rowLocalQualifier && !string.Equals(qualifier, this.tableName, StringComparison.OrdinalIgnoreCase))
        {
            return DBNull.Value;
        }

        // Callers may pass "10" for an Integer field or 10 for a Text field;
        // the expression must see the value as the field holds it.
        ColumnConstraint referenced = this.constraints[index];
        if (this.liveStaleReferences && referenced.IsCalculated && this.tableName is not null
            && (referenced.CalculatedExpressionPlan ??= CalculatedExpressionParser.Parse(referenced.CalculationExpression!)).HasStaleQualifier(this.tableName))
        {
            return CalculatedError.Uncached;
        }

        object value = referenced.IsCalculated ? this.EvaluateColumn(index) : this.values[index];
        if (value is CalculatedError error)
        {
            return new CalculatedError(error.NativeCode is null ? -5617 : -3030);
        }

        object coerced = CoerceInput(value, referenced.ClrType);
        if (referenced.IsCalculated && referenced.CalculatedResultType == ColumnType.TextType && coerced is string text)
        {
            int size = referenced.PhysicalColumn?.Size ?? 0;
            int payloadLimit = size > Constants.CalculatedColumn.ExtraDataLen ? size - Constants.CalculatedColumn.ExtraDataLen : Constants.CalculatedColumn.MaxTextResultBytes;
            return JetFormat.NormalizeCalculatedTextReference(text, payloadLimit);
        }

        if (!referenced.IsCalculated && referenced.StorageType == ColumnType.TextType && coerced is string source
            && referenced.PhysicalColumn is { Size: > 0 } column && this.IsCalculatingColumn)
        {
            int limit = column.Size / 2;
            return source.Length > limit ? source[..limit] : source;
        }

        ColumnType storageType = referenced.IsCalculated ? referenced.CalculatedResultType : referenced.StorageType;
        if (storageType == ColumnType.BooleanType && (!referenced.IsCalculated || !IsNull(coerced)))
        {
            return (short)(!IsNull(coerced) && Convert.ToBoolean(coerced, CultureInfo.InvariantCulture) ? -1 : 0);
        }

        if (storageType == ColumnType.ByteType && !IsNull(coerced))
        {
            return (int)Convert.ToByte(coerced, CultureInfo.InvariantCulture);
        }

        if (!referenced.IsCalculated && storageType == ColumnType.NumericType && !IsNull(coerced))
        {
            return NumericEncoder.NormalizeStoredValue(this.columns[index], ToDecimal(coerced));
        }

        if (this.IsCalculatingColumn && storageType == ColumnType.DateTimeType && !IsNull(coerced))
        {
            return ToNativeDate(coerced);
        }

        return storageType == ColumnType.MoneyType && !IsNull(coerced) ? ToCurrency(coerced) : coerced;
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
