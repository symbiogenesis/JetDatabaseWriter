namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using ClosedXML.Parser;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;

internal static class CalculatedExpressionEvaluator
{
    private const string PlaceholderPrefix = "__JdwCalcCol";

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
        private Plan(ExpressionNode root, Dictionary<string, string> placeholderToColumn)
        {
            Root = root;
            PlaceholderToColumn = placeholderToColumn;
        }

        public ExpressionNode Root { get; }

        public Dictionary<string, string> PlaceholderToColumn { get; }

        public static Plan Parse(string expression)
        {
            string normalized = NormalizeExpression(expression, out Dictionary<string, string> placeholderToColumn);
            var parseContext = new ParseContext(placeholderToColumn);
            try
            {
                ExpressionNode root = FormulaParser<ExpressionNode, ExpressionNode, ParseContext>.CellFormulaA1(
                    normalized,
                    parseContext,
                    AstFactory.Instance);
                return new Plan(root, placeholderToColumn);
            }
            catch (ParsingException ex)
            {
                throw new NotSupportedException($"Calculated-column expression '{expression}' is not supported by the Phase 2 parser.", ex);
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

        public EvaluationContext(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, bool force)
        {
            this.constraints = constraints;
            this.values = values;
            this.force = force;
            evaluating = new bool[constraints.Count];
            evaluated = new bool[constraints.Count];
            columnIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < tableDef.Columns.Count; i++)
            {
                columnIndexes[tableDef.Columns[i].Name] = i;
            }
        }

        public object EvaluateColumn(int index)
        {
            object current = values[index];
            ColumnConstraint constraint = constraints[index];
            if (!constraint.IsCalculated)
            {
                return current ?? DBNull.Value;
            }

            if (evaluated[index])
            {
                return values[index] ?? DBNull.Value;
            }

            if (!force && !IsNull(current))
            {
                evaluated[index] = true;
                return current;
            }

            if (evaluating[index])
            {
                throw new NotSupportedException($"Calculated column '{constraint.Name}' participates in a circular expression dependency.");
            }

            if (string.IsNullOrWhiteSpace(constraint.CalculationExpression))
            {
                return current ?? DBNull.Value;
            }

            evaluating[index] = true;
            try
            {
                constraint.CalculatedExpressionPlan ??= Plan.Parse(constraint.CalculationExpression);
                object raw = constraint.CalculatedExpressionPlan.Root.Evaluate(this, constraint.CalculatedExpressionPlan);
                object coerced = CoerceResult(raw, constraint.ClrType);
                values[index] = coerced;
                evaluated[index] = true;
                return coerced;
            }
            catch (NotSupportedException) when (!force && !IsNull(current))
            {
                evaluated[index] = true;
                return current;
            }
            finally
            {
                evaluating[index] = false;
            }
        }

        public object GetNameValue(string name, Plan plan)
        {
            if (plan.PlaceholderToColumn.TryGetValue(name, out string? columnName))
            {
                name = columnName;
            }

            if (!columnIndexes.TryGetValue(name, out int index))
            {
                throw new NotSupportedException($"Calculated-column expression references unknown name '{name}'.");
            }

            ColumnConstraint referenced = constraints[index];
            return referenced.IsCalculated ? EvaluateColumn(index) : values[index] ?? DBNull.Value;
        }
    }

    internal abstract class ExpressionNode
    {
        public abstract object Evaluate(EvaluationContext context, Plan plan);
    }

    private sealed class ValueNode(object? value) : ExpressionNode
    {
        public override object Evaluate(EvaluationContext context, Plan plan) => value ?? DBNull.Value;
    }

    private sealed class NameNode(string name) : ExpressionNode
    {
        public override object Evaluate(EvaluationContext context, Plan plan)
        {
            return name.ToUpperInvariant() switch
            {
                "TRUE" => true,
                "FALSE" => false,
                "NULL" => DBNull.Value,
                _ => context.GetNameValue(name, plan),
            };
        }
    }

    private sealed class BinaryNode(BinaryOperation operation, ExpressionNode left, ExpressionNode right) : ExpressionNode
    {
        public override object Evaluate(EvaluationContext context, Plan plan)
        {
            object leftValue = left.Evaluate(context, plan);
            object rightValue = right.Evaluate(context, plan);
            return operation switch
            {
                BinaryOperation.Concat => ToText(leftValue) + ToText(rightValue),
                BinaryOperation.Addition => EvaluateNumeric(leftValue, rightValue, static (l, r) => l + r),
                BinaryOperation.Subtraction => EvaluateNumeric(leftValue, rightValue, static (l, r) => l - r),
                BinaryOperation.Multiplication => EvaluateNumeric(leftValue, rightValue, static (l, r) => l * r),
                BinaryOperation.Division => EvaluateNumeric(leftValue, rightValue, static (l, r) => l / r),
                BinaryOperation.Power => IsNull(leftValue) || IsNull(rightValue) ? DBNull.Value : Math.Pow(ToDouble(leftValue), ToDouble(rightValue)),
                BinaryOperation.Equal => CompareValues(leftValue, rightValue, static c => c == 0),
                BinaryOperation.NotEqual => CompareValues(leftValue, rightValue, static c => c != 0),
                BinaryOperation.GreaterThan => CompareValues(leftValue, rightValue, static c => c > 0),
                BinaryOperation.GreaterOrEqualThan => CompareValues(leftValue, rightValue, static c => c >= 0),
                BinaryOperation.LessThan => CompareValues(leftValue, rightValue, static c => c < 0),
                BinaryOperation.LessOrEqualThan => CompareValues(leftValue, rightValue, static c => c <= 0),
                _ => throw new NotSupportedException($"Calculated-column binary operation '{operation}' is not supported."),
            };
        }
    }

    private sealed class UnaryNode(UnaryOperation operation, ExpressionNode operand) : ExpressionNode
    {
        public override object Evaluate(EvaluationContext context, Plan plan)
        {
            object value = operand.Evaluate(context, plan);
            if (IsNull(value))
            {
                return DBNull.Value;
            }

            return operation switch
            {
                UnaryOperation.Plus => ToDecimal(value),
                UnaryOperation.Minus => -ToDecimal(value),
                UnaryOperation.Percent => ToDecimal(value) / 100m,
                _ => throw new NotSupportedException($"Calculated-column unary operation '{operation}' is not supported."),
            };
        }
    }

    private sealed class FunctionNode(string name, IReadOnlyList<ExpressionNode> args) : ExpressionNode
    {
        public override object Evaluate(EvaluationContext context, Plan plan)
        {
            string upperName = name.ToUpperInvariant();
            object Arg(int index) => index < args.Count ? args[index].Evaluate(context, plan) : DBNull.Value;

            switch (upperName)
            {
                case "IF":
                case "IIF":
                    RequireArgCount(upperName, args.Count, 3, 3);
                    return ToBoolean(Arg(0)) ? Arg(1) : Arg(2);
                case "NZ":
                    RequireArgCount(upperName, args.Count, 1, 2);
                    object nzValue = Arg(0);
                    return IsNull(nzValue) ? (args.Count > 1 ? Arg(1) : string.Empty) : nzValue;
                case "ISBLANK":
                case "ISNULL":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return IsNull(Arg(0));
                case "ISNUMBER":
                case "ISNUMERIC":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return TryConvertDecimal(Arg(0), out _);
                case "ISDATE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return TryConvertDateTime(Arg(0), out _);
                case "LEN":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToText(Arg(0)).Length;
                case "LEFT":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return Left(ToText(Arg(0)), checked((int)ToDecimal(Arg(1))));
                case "RIGHT":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return Right(ToText(Arg(0)), checked((int)ToDecimal(Arg(1))));
                case "MID":
                    RequireArgCount(upperName, args.Count, 2, 3);
                    return Mid(ToText(Arg(0)), checked((int)ToDecimal(Arg(1))), args.Count > 2 ? checked((int)ToDecimal(Arg(2))) : int.MaxValue);
                case "UCASE":
                case "UPPER":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToText(Arg(0)).ToUpperInvariant();
                case "LCASE":
                case "LOWER":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return CultureInfo.InvariantCulture.TextInfo.ToLower(ToText(Arg(0)));
                case "TRIM":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToText(Arg(0)).Trim();
                case "LTRIM":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToText(Arg(0)).TrimStart();
                case "RTRIM":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToText(Arg(0)).TrimEnd();
                case "ABS":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Abs(ToDecimal(Arg(0)));
                case "ROUND":
                    RequireArgCount(upperName, args.Count, 1, 2);
                    return Math.Round(ToDecimal(Arg(0)), args.Count > 1 ? checked((int)ToDecimal(Arg(1))) : 0, MidpointRounding.ToEven);
                case "INT":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Floor(ToDecimal(Arg(0)));
                case "FIX":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    decimal fixValue = ToDecimal(Arg(0));
                    return fixValue < 0 ? Math.Ceiling(fixValue) : Math.Floor(fixValue);
                case "DATE":
                case "TODAY":
                    RequireArgCount(upperName, args.Count, 0, 0);
                    return DateTime.Today;
                case "NOW":
                    RequireArgCount(upperName, args.Count, 0, 0);
                    return DateTime.Now;
                case "TIME":
                    RequireArgCount(upperName, args.Count, 0, 0);
                    return DateTime.Today + DateTime.Now.TimeOfDay;
                case "DATEVALUE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ParseDate(ToText(Arg(0)));
                case "YEAR":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0)).Year;
                case "MONTH":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0)).Month;
                case "DAY":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0)).Day;
                case "HOUR":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0)).Hour;
                case "MINUTE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0)).Minute;
                case "SECOND":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0)).Second;
                case "CINT":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToInt16(Arg(0), CultureInfo.InvariantCulture);
                case "CLNG":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToInt32(Arg(0), CultureInfo.InvariantCulture);
                case "CDBL":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToDouble(Arg(0), CultureInfo.InvariantCulture);
                case "CSNG":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToSingle(Arg(0), CultureInfo.InvariantCulture);
                case "CCUR":
                case "CDEC":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDecimal(Arg(0));
                case "CSTR":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToText(Arg(0));
                case "CDATE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0));
                case "CBOOL":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToBoolean(Arg(0));
                case "CBYTE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToByte(Arg(0), CultureInfo.InvariantCulture);
                default:
                    throw new NotSupportedException($"Calculated-column function '{name}' is not supported.");
            }
        }
    }

    private sealed class UnsupportedNode(string reason) : ExpressionNode
    {
        public override object Evaluate(EvaluationContext context, Plan plan) => throw new NotSupportedException(reason);
    }

    private sealed class ParseContext(Dictionary<string, string> placeholderToColumn)
    {
        public Dictionary<string, string> PlaceholderToColumn { get; } = placeholderToColumn;
    }

    private sealed class AstFactory : IAstFactory<ExpressionNode, ExpressionNode, ParseContext>
    {
        public static readonly AstFactory Instance = new();

        public ExpressionNode LogicalValue(ParseContext context, SymbolRange range, bool value) => new ValueNode(value);

        public ExpressionNode NumberValue(ParseContext context, SymbolRange range, double value) => new ValueNode(value);

        public ExpressionNode TextValue(ParseContext context, SymbolRange range, string value) => new ValueNode(value);

        public ExpressionNode ErrorValue(ParseContext context, SymbolRange range, ReadOnlySpan<char> error) => new UnsupportedNode($"Calculated-column error literal '{error.ToString()}' is not supported.");

        public ExpressionNode ArrayNode(ParseContext context, SymbolRange range, int rows, int columns, IReadOnlyList<ExpressionNode> elements) => new UnsupportedNode("Calculated-column array literals are not supported.");

        public ExpressionNode BlankNode(ParseContext context, SymbolRange range) => new ValueNode(DBNull.Value);

        public ExpressionNode LogicalNode(ParseContext context, SymbolRange range, bool value) => new ValueNode(value);

        public ExpressionNode ErrorNode(ParseContext context, SymbolRange range, ReadOnlySpan<char> error) => new UnsupportedNode($"Calculated-column error literal '{error.ToString()}' is not supported.");

        public ExpressionNode NumberNode(ParseContext context, SymbolRange range, double value) => new ValueNode(value);

        public ExpressionNode TextNode(ParseContext context, SymbolRange range, string value) => new ValueNode(value);

        public ExpressionNode Reference(ParseContext context, SymbolRange range, ReferenceArea reference) => new UnsupportedNode("Calculated-column cell references are not supported; use column names instead.");

        public ExpressionNode SheetReference(ParseContext context, SymbolRange range, string sheet, ReferenceArea reference) => new UnsupportedNode("Calculated-column sheet references are not supported.");

        public ExpressionNode BangReference(ParseContext context, SymbolRange range, ReferenceArea reference) => new UnsupportedNode("Calculated-column sheet references are not supported.");

        public ExpressionNode Reference3D(ParseContext context, SymbolRange range, string firstSheet, string lastSheet, ReferenceArea reference) => new UnsupportedNode("Calculated-column 3D references are not supported.");

        public ExpressionNode ExternalSheetReference(ParseContext context, SymbolRange range, int workbookIndex, string sheet, ReferenceArea reference) => new UnsupportedNode("Calculated-column external references are not supported.");

        public ExpressionNode ExternalReference3D(ParseContext context, SymbolRange range, int workbookIndex, string firstSheet, string lastSheet, ReferenceArea reference) => new UnsupportedNode("Calculated-column external references are not supported.");

        public ExpressionNode Function(ParseContext context, SymbolRange range, ReadOnlySpan<char> functionName, IReadOnlyList<ExpressionNode> args) => new FunctionNode(functionName.ToString(), args);

        public ExpressionNode Function(ParseContext context, SymbolRange range, string prefix, ReadOnlySpan<char> functionName, IReadOnlyList<ExpressionNode> args) => new FunctionNode(functionName.ToString(), args);

        public ExpressionNode ExternalFunction(ParseContext context, SymbolRange range, int workbookIndex, string prefix, ReadOnlySpan<char> functionName, IReadOnlyList<ExpressionNode> args) => new UnsupportedNode("Calculated-column external functions are not supported.");

        public ExpressionNode ExternalFunction(ParseContext context, SymbolRange range, int workbookIndex, ReadOnlySpan<char> functionName, IReadOnlyList<ExpressionNode> args) => new UnsupportedNode("Calculated-column external functions are not supported.");

        public ExpressionNode CellFunction(ParseContext context, SymbolRange range, RowCol cell, IReadOnlyList<ExpressionNode> args) => new UnsupportedNode("Calculated-column cell functions are not supported.");

        public ExpressionNode StructureReference(ParseContext context, SymbolRange range, StructuredReferenceArea area, string? firstColumn, string? lastColumn) => new UnsupportedNode("Calculated-column structured references are not supported.");

        public ExpressionNode StructureReference(ParseContext context, SymbolRange range, string table, StructuredReferenceArea area, string? firstColumn, string? lastColumn) => new UnsupportedNode("Calculated-column structured references are not supported.");

        public ExpressionNode ExternalStructureReference(ParseContext context, SymbolRange range, int workbookIndex, string table, StructuredReferenceArea area, string? firstColumn, string? lastColumn) => new UnsupportedNode("Calculated-column structured references are not supported.");

        public ExpressionNode Name(ParseContext context, SymbolRange range, string name) => new NameNode(name);

        public ExpressionNode SheetName(ParseContext context, SymbolRange range, string sheet, string name) => new UnsupportedNode("Calculated-column sheet names are not supported.");

        public ExpressionNode BangName(ParseContext context, SymbolRange range, string name) => new UnsupportedNode("Calculated-column sheet names are not supported.");

        public ExpressionNode ExternalName(ParseContext context, SymbolRange range, int workbookIndex, string name) => new UnsupportedNode("Calculated-column external names are not supported.");

        public ExpressionNode ExternalSheetName(ParseContext context, SymbolRange range, int workbookIndex, string sheet, string name) => new UnsupportedNode("Calculated-column external names are not supported.");

        public ExpressionNode BinaryNode(ParseContext context, SymbolRange range, BinaryOperation operation, ExpressionNode leftNode, ExpressionNode rightNode) => new BinaryNode(operation, leftNode, rightNode);

        public ExpressionNode Unary(ParseContext context, SymbolRange range, UnaryOperation operation, ExpressionNode node) => new UnaryNode(operation, node);

        public ExpressionNode Nested(ParseContext context, SymbolRange range, ExpressionNode node) => node;
    }

    private static string NormalizeExpression(string expression, out Dictionary<string, string> placeholderToColumn)
    {
        placeholderToColumn = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        string trimmed = expression.Trim();
        if (trimmed.StartsWith('='))
        {
            trimmed = trimmed.Substring(1).TrimStart();
        }

        var builder = new StringBuilder(trimmed.Length + 16);
        int placeholderIndex = 0;
        for (int i = 0; i < trimmed.Length; i++)
        {
            char ch = trimmed[i];
            if (ch == '"')
            {
                builder.Append(ch);
                i++;
                while (i < trimmed.Length)
                {
                    builder.Append(trimmed[i]);
                    if (trimmed[i] == '"')
                    {
                        if (i + 1 < trimmed.Length && trimmed[i + 1] == '"')
                        {
                            i++;
                            builder.Append(trimmed[i]);
                            i++;
                            continue;
                        }

                        break;
                    }

                    i++;
                }
            }
            else if (ch == '[')
            {
                int end = trimmed.IndexOf(']', i + 1);
                if (end < 0)
                {
                    builder.Append(ch);
                    continue;
                }

                string columnName = trimmed.Substring(i + 1, end - i - 1);
                string placeholder = PlaceholderPrefix + placeholderIndex.ToString(CultureInfo.InvariantCulture);
                placeholderIndex++;
                placeholderToColumn[placeholder] = columnName;
                builder.Append(placeholder);
                i = end;
            }
            else if (ch == '#')
            {
                int end = trimmed.IndexOf('#', i + 1);
                if (end < 0)
                {
                    builder.Append(ch);
                    continue;
                }

                string dateLiteral = trimmed.Substring(i + 1, end - i - 1).Replace("\"", "\"\"", StringComparison.Ordinal);
                builder.Append("DATEVALUE(\"");
                builder.Append(dateLiteral);
                builder.Append("\")");
                i = end;
            }
            else
            {
                builder.Append(ch);
            }
        }

        return builder.ToString();
    }

    private static object CoerceResult(object? value, Type targetType)
    {
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        if (targetType == typeof(string))
        {
            return ToText(value);
        }

        if (targetType == typeof(bool))
        {
            return ToBoolean(value);
        }

        if (targetType == typeof(byte))
        {
            return Convert.ToByte(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(short))
        {
            return Convert.ToInt16(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(int))
        {
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(long))
        {
            return Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(float))
        {
            return Convert.ToSingle(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(double))
        {
            return Convert.ToDouble(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(decimal))
        {
            return ToDecimal(value);
        }

        if (targetType == typeof(DateTime))
        {
            return ToDateTime(value);
        }

        if (targetType == typeof(Guid))
        {
            return value is Guid guid ? guid : Guid.Parse(ToText(value));
        }

        return value!;
    }

    private static object EvaluateNumeric(object left, object right, Func<decimal, decimal, decimal> operation)
        => IsNull(left) || IsNull(right) ? DBNull.Value : operation(ToDecimal(left), ToDecimal(right));

    private static object CompareValues(object left, object right, Func<int, bool> predicate)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        int comparison;
        if (TryConvertDecimal(left, out decimal leftDecimal) && TryConvertDecimal(right, out decimal rightDecimal))
        {
            comparison = leftDecimal.CompareTo(rightDecimal);
        }
        else if (TryConvertDateTime(left, out DateTime leftDate) && TryConvertDateTime(right, out DateTime rightDate))
        {
            comparison = leftDate.CompareTo(rightDate);
        }
        else if (left is bool || right is bool)
        {
            comparison = ToBoolean(left).CompareTo(ToBoolean(right));
        }
        else
        {
            comparison = string.Compare(ToText(left), ToText(right), StringComparison.OrdinalIgnoreCase);
        }

        return predicate(comparison);
    }

    private static bool IsNull(object? value) => value is null or DBNull;

    private static string ToText(object? value)
        => IsNull(value) ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

    private static decimal ToDecimal(object? value)
        => Convert.ToDecimal(value, CultureInfo.InvariantCulture);

    private static double ToDouble(object? value)
        => Convert.ToDouble(value, CultureInfo.InvariantCulture);

    private static bool ToBoolean(object? value)
    {
        if (IsNull(value))
        {
            return false;
        }

        if (value is bool boolean)
        {
            return boolean;
        }

        if (TryConvertDecimal(value, out decimal numeric))
        {
            return numeric != 0m;
        }

        string text = ToText(value);
        return bool.TryParse(text, out bool parsed) ? parsed : !string.IsNullOrEmpty(text);
    }

    private static DateTime ToDateTime(object? value)
    {
        if (value is DateTime dateTime)
        {
            return dateTime;
        }

        if (value is double oaDouble)
        {
            return DateTime.FromOADate(oaDouble);
        }

        if (value is decimal oaDecimal)
        {
            return DateTime.FromOADate((double)oaDecimal);
        }

        return ParseDate(ToText(value));
    }

    private static DateTime ParseDate(string text)
    {
        string[] formats = [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            "M/d/yyyy h:mm:ss tt",
            "M/d/yyyy",
            "MM/dd/yyyy",
        ];
        if (DateTime.TryParseExact(text, formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime exact))
        {
            return exact;
        }

        return DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces);
    }

    private static bool TryConvertDecimal(object? value, out decimal result)
    {
        if (IsNull(value))
        {
            result = 0;
            return false;
        }

        try
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch (FormatException)
        {
        }
        catch (InvalidCastException)
        {
        }
        catch (OverflowException)
        {
        }

        result = 0;
        return false;
    }

    private static bool TryConvertDateTime(object? value, out DateTime result)
    {
        if (IsNull(value))
        {
            result = default;
            return false;
        }

        try
        {
            result = ToDateTime(value);
            return true;
        }
        catch (FormatException)
        {
        }
        catch (InvalidCastException)
        {
        }
        catch (OverflowException)
        {
        }

        result = default;
        return false;
    }

    private static string Left(string text, int count)
    {
        count = Math.Clamp(count, 0, text.Length);
        return text.Substring(0, count);
    }

    private static string Right(string text, int count)
    {
        count = Math.Clamp(count, 0, text.Length);
        return text.Substring(text.Length - count, count);
    }

    private static string Mid(string text, int start, int count)
    {
        int zeroBasedStart = Math.Clamp(start - 1, 0, text.Length);
        count = Math.Clamp(count, 0, text.Length - zeroBasedStart);
        return text.Substring(zeroBasedStart, count);
    }

    private static void RequireArgCount(string functionName, int actual, int min, int max)
    {
        if (actual < min || actual > max)
        {
            throw new NotSupportedException(
                $"Calculated-column function '{functionName}' expects {min}" + (min == max ? string.Empty : $"..{max}") + $" argument(s), got {actual}.");
        }
    }
}
