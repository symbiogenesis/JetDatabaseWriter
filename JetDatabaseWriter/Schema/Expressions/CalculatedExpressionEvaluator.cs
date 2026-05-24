namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
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
                throw new InvalidOperationException($"Calculated column '{constraint.Name}' participates in a circular expression dependency.");
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
                throw new InvalidOperationException($"Calculated-column expression references unknown name '{name}'.");
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
            if (TryGetBuiltinConstant(name, out object constantValue))
            {
                return constantValue;
            }

            return name.ToUpperInvariant() switch
            {
                "TRUE" => true,
                "FALSE" => false,
                "YES" => true,
                "NO" => false,
                "ON" => true,
                "OFF" => false,
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
            string upperName = NormalizeFunctionName(name);
            object Arg(int index) => index < args.Count ? args[index].Evaluate(context, plan) : DBNull.Value;

            switch (upperName)
            {
                case "IF":
                case "IIF":
                    RequireArgCount(upperName, args.Count, 3, 3);
                    return ToBoolean(Arg(0)) ? Arg(1) : Arg(2);
                case "AND":
                    RequireArgCount(upperName, args.Count, 1, int.MaxValue);
                    for (int argIndex = 0; argIndex < args.Count; argIndex++)
                    {
                        if (!ToBoolean(Arg(argIndex)))
                        {
                            return false;
                        }
                    }

                    return true;
                case "OR":
                    RequireArgCount(upperName, args.Count, 1, int.MaxValue);
                    for (int argIndex = 0; argIndex < args.Count; argIndex++)
                    {
                        if (ToBoolean(Arg(argIndex)))
                        {
                            return true;
                        }
                    }

                    return false;
                case "NOT":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return !ToBoolean(Arg(0));
                case "XOR":
                    RequireArgCount(upperName, args.Count, 2, int.MaxValue);
                    bool xorResult = false;
                    for (int argIndex = 0; argIndex < args.Count; argIndex++)
                    {
                        xorResult ^= ToBoolean(Arg(argIndex));
                    }

                    return xorResult;
                case "EQV":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return ToBoolean(Arg(0)) == ToBoolean(Arg(1));
                case "IMP":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return !ToBoolean(Arg(0)) || ToBoolean(Arg(1));
                case "MOD":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return EvaluateNumeric(Arg(0), Arg(1), static (leftValue, rightValue) => leftValue % rightValue);
                case "INTDIV":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return IsNull(Arg(0)) || IsNull(Arg(1))
                        ? DBNull.Value
                        : Math.Truncate(ToDecimal(Arg(0)) / ToDecimal(Arg(1)));
                case "LIKE":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return IsNull(Arg(0)) || IsNull(Arg(1))
                        ? DBNull.Value
                        : AccessLike(ToText(Arg(0)), ToText(Arg(1)));
                case "BETWEEN":
                    RequireArgCount(upperName, args.Count, 3, 3);
                    return IsNull(Arg(0)) || IsNull(Arg(1)) || IsNull(Arg(2))
                        ? DBNull.Value
                        : ToBoolean(CompareValues(Arg(0), Arg(1), static comparison => comparison >= 0))
                            && ToBoolean(CompareValues(Arg(0), Arg(2), static comparison => comparison <= 0));
                case "IN":
                    RequireArgCount(upperName, args.Count, 2, int.MaxValue);
                    object inValue = Arg(0);
                    if (IsNull(inValue))
                    {
                        return DBNull.Value;
                    }

                    for (int argIndex = 1; argIndex < args.Count; argIndex++)
                    {
                        if (ToBoolean(CompareValues(inValue, Arg(argIndex), static comparison => comparison == 0)))
                        {
                            return true;
                        }
                    }

                    return false;
                case "CHOOSE":
                    RequireArgCount(upperName, args.Count, 2, int.MaxValue);
                    int choiceIndex = checked((int)ToDecimal(Arg(0)));
                    return choiceIndex >= 1 && choiceIndex < args.Count ? Arg(choiceIndex) : DBNull.Value;
                case "SWITCH":
                    RequireArgCount(upperName, args.Count, 2, int.MaxValue);
                    if ((args.Count % 2) != 0)
                    {
                        throw new ArgumentException("Calculated-column function 'SWITCH' expects condition/value argument pairs.");
                    }

                    for (int argIndex = 0; argIndex < args.Count; argIndex += 2)
                    {
                        if (ToBoolean(Arg(argIndex)))
                        {
                            return Arg(argIndex + 1);
                        }
                    }

                    return DBNull.Value;
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
                case "ATAN":
                case "ATN":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Atan(ToDouble(Arg(0)));
                case "COS":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Cos(ToDouble(Arg(0)));
                case "EXP":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Exp(ToDouble(Arg(0)));
                case "LOG":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Log(ToDouble(Arg(0)));
                case "SGN":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Sign(ToDecimal(Arg(0)));
                case "SIN":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Sin(ToDouble(Arg(0)));
                case "SQR":
                case "SQRT":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Sqrt(ToDouble(Arg(0)));
                case "TAN":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Math.Tan(ToDouble(Arg(0)));
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
                case "DATESERIAL":
                    RequireArgCount(upperName, args.Count, 3, 3);
                    return DateSerial(checked((int)ToDecimal(Arg(0))), checked((int)ToDecimal(Arg(1))), checked((int)ToDecimal(Arg(2))));
                case "DATEADD":
                    RequireArgCount(upperName, args.Count, 3, 3);
                    return DateAdd(ToText(Arg(0)), checked((int)ToDecimal(Arg(1))), ToDateTime(Arg(2)));
                case "DATEDIFF":
                    RequireArgCount(upperName, args.Count, 3, 5);
                    return DateDiff(ToText(Arg(0)), ToDateTime(Arg(1)), ToDateTime(Arg(2)));
                case "DATEPART":
                    RequireArgCount(upperName, args.Count, 2, 4);
                    return DatePart(ToText(Arg(0)), ToDateTime(Arg(1)));
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
                case "TIMEVALUE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return DateTime.Today + ToDateTime(Arg(0)).TimeOfDay;
                case "TIMESERIAL":
                    RequireArgCount(upperName, args.Count, 3, 3);
                    return DateTime.Today
                        .AddHours(ToDouble(Arg(0)))
                        .AddMinutes(ToDouble(Arg(1)))
                        .AddSeconds(ToDouble(Arg(2)));
                case "TIMER":
                    RequireArgCount(upperName, args.Count, 0, 0);
                    return DateTime.Now.TimeOfDay.TotalSeconds;
                case "MONTHNAME":
                    RequireArgCount(upperName, args.Count, 1, 2);
                    return CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(checked((int)ToDecimal(Arg(0))));
                case "WEEKDAY":
                    RequireArgCount(upperName, args.Count, 1, 2);
                    return Weekday(ToDateTime(Arg(0)), args.Count > 1 ? checked((int)ToDecimal(Arg(1))) : 1);
                case "WEEKDAYNAME":
                    RequireArgCount(upperName, args.Count, 1, 3);
                    return WeekdayName(checked((int)ToDecimal(Arg(0))), args.Count > 1 && ToBoolean(Arg(1)), args.Count > 2 ? checked((int)ToDecimal(Arg(2))) : 1);
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
                case "CVDATE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToDateTime(Arg(0));
                case "CBOOL":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return ToBoolean(Arg(0));
                case "CBYTE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToByte(Arg(0), CultureInfo.InvariantCulture);
                case "CVAR":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Arg(0);
                case "HEX":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToInt64(Arg(0), CultureInfo.InvariantCulture).ToString("X", CultureInfo.InvariantCulture);
                case "OCT":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Convert.ToString(Convert.ToInt64(Arg(0), CultureInfo.InvariantCulture), 8) ?? string.Empty;
                case "VAL":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Val(ToText(Arg(0)));
                case "ASC":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Asc(ToText(Arg(0)), asciiOnly: true);
                case "ASCW":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return Asc(ToText(Arg(0)), asciiOnly: false);
                case "CHR":
                case "CHRW":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return char.ConvertFromUtf32(checked((int)ToDecimal(Arg(0))));
                case "STR":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    decimal strValue = ToDecimal(Arg(0));
                    string strText = strValue.ToString(CultureInfo.InvariantCulture);
                    return strValue >= 0 ? " " + strText : strText;
                case "INSTR":
                    RequireArgCount(upperName, args.Count, 2, 4);
                    return InStr(args, Arg);
                case "INSTRREV":
                    RequireArgCount(upperName, args.Count, 2, 4);
                    return InStrRev(args, Arg);
                case "REPLACE":
                    RequireArgCount(upperName, args.Count, 3, 6);
                    return ReplaceText(args, Arg);
                case "SPACE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return new string(' ', Math.Max(0, checked((int)ToDecimal(Arg(0)))));
                case "STRCOMP":
                    RequireArgCount(upperName, args.Count, 2, 3);
                    return Math.Sign(string.Compare(ToText(Arg(0)), ToText(Arg(1)), CompareOptions(args.Count > 2 ? Arg(2) : null)));
                case "STRING":
                    RequireArgCount(upperName, args.Count, 2, 2);
                    return RepeatChar(checked((int)ToDecimal(Arg(0))), ToText(Arg(1)));
                case "STRREVERSE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    char[] chars = ToText(Arg(0)).ToCharArray();
                    Array.Reverse(chars);
                    return new string(chars);
                case "STRCONV":
                    RequireArgCount(upperName, args.Count, 2, 3);
                    return IsNull(Arg(0)) ? DBNull.Value : StrConv(ToText(Arg(0)), checked((int)ToDecimal(Arg(1))));
                case "FORMAT":
                    RequireArgCount(upperName, args.Count, 1, 4);
                    return args.Count == 1 ? ToText(Arg(0)) : FormatValue(Arg(0), ToText(Arg(1)));
                case "FORMATNUMBER":
                    RequireArgCount(upperName, args.Count, 1, 6);
                    return FormatNumber(ToDecimal(Arg(0)), args.Count > 1 ? checked((int)ToDecimal(Arg(1))) : 2, percent: false, currency: false);
                case "FORMATPERCENT":
                    RequireArgCount(upperName, args.Count, 1, 6);
                    return FormatNumber(ToDecimal(Arg(0)), args.Count > 1 ? checked((int)ToDecimal(Arg(1))) : 2, percent: true, currency: false);
                case "FORMATCURRENCY":
                    RequireArgCount(upperName, args.Count, 1, 6);
                    return FormatNumber(ToDecimal(Arg(0)), args.Count > 1 ? checked((int)ToDecimal(Arg(1))) : 2, percent: false, currency: true);
                case "FORMATDATETIME":
                    RequireArgCount(upperName, args.Count, 1, 2);
                    return FormatDateTime(ToDateTime(Arg(0)), args.Count > 1 ? checked((int)ToDecimal(Arg(1))) : 0);
                case "FV":
                    RequireArgCount(upperName, args.Count, 3, 5);
                    return FinancialFutureValue(args, Arg);
                case "PV":
                    RequireArgCount(upperName, args.Count, 3, 5);
                    return FinancialPresentValue(args, Arg);
                case "PMT":
                    RequireArgCount(upperName, args.Count, 3, 5);
                    return FinancialPayment(args, Arg);
                case "NPER":
                    RequireArgCount(upperName, args.Count, 3, 5);
                    return FinancialPeriods(args, Arg);
                case "IPMT":
                    RequireArgCount(upperName, args.Count, 4, 6);
                    return FinancialInterestPayment(args, Arg);
                case "PPMT":
                    RequireArgCount(upperName, args.Count, 4, 6);
                    return FinancialPrincipalPayment(args, Arg);
                case "DDB":
                    RequireArgCount(upperName, args.Count, 4, 5);
                    return FinancialDoubleDecliningBalance(args, Arg);
                case "SLN":
                    RequireArgCount(upperName, args.Count, 3, 3);
                    return (ToDouble(Arg(0)) - ToDouble(Arg(1))) / ToDouble(Arg(2));
                case "SYD":
                    RequireArgCount(upperName, args.Count, 4, 4);
                    return FinancialSumOfYearsDepreciation(args, Arg);
                case "RATE":
                    RequireArgCount(upperName, args.Count, 3, 6);
                    return FinancialRate(args, Arg);
                case "VARTYPE":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return VarType(Arg(0));
                case "TYPENAME":
                    RequireArgCount(upperName, args.Count, 1, 1);
                    return TypeName(Arg(0));
                case "DLOOKUP":
                case "DCOUNT":
                case "DSUM":
                case "DAVG":
                case "DMIN":
                case "DMAX":
                    throw new NotSupportedException(
                        $"Calculated-column function '{name}' requires domain/query evaluation, which Access calculated columns do not support.");
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

        return AccessExpressionNormalizer.Normalize(builder.ToString());
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

    private static string NormalizeFunctionName(string name)
    {
        string upperName = name.ToUpperInvariant();
        return upperName.EndsWith('$') ? upperName.Substring(0, upperName.Length - 1) : upperName;
    }

    private static bool TryGetBuiltinConstant(string name, out object value)
    {
        switch (name.ToUpperInvariant())
        {
            case "VBEMPTY":
            case "VBFALSE":
            case "VBUSESYSTEM":
            case "VBGENERALDATE":
            case "VBBINARYCOMPARE":
                value = 0;
                return true;
            case "VBTRUE":
            case "VBUSECOMPAREOPTION":
                value = -1;
                return true;
            case "VBINTEGER":
            case "VBMONDAY":
            case "VBSHORTDATE":
            case "VBLOWERCASE":
            case "VBTEXTCOMPARE":
            case "VBFIRSTFOURDAYS":
                value = 2;
                return true;
            case "VBLONG":
            case "VBTUESDAY":
            case "VBLONGTIME":
            case "VBPROPERCASE":
            case "VBFIRSTFULLWEEK":
                value = 3;
                return true;
            case "VBSINGLE":
            case "VBWEDNESDAY":
            case "VBSHORTTIME":
                value = 4;
                return true;
            case "VBDOUBLE":
            case "VBTHURSDAY":
                value = 5;
                return true;
            case "VBCURRENCY":
            case "VBFRIDAY":
                value = 6;
                return true;
            case "VBDATE":
            case "VBSATURDAY":
                value = 7;
                return true;
            case "VBSTRING":
                value = 8;
                return true;
            case "VBOBJECT":
                value = 9;
                return true;
            case "VBERROR":
                value = 10;
                return true;
            case "VBBOOLEAN":
                value = 11;
                return true;
            case "VBVARIANT":
                value = 12;
                return true;
            case "VBDECIMAL":
                value = 14;
                return true;
            case "VBBYTE":
                value = 17;
                return true;
            case "VBUSEDEFAULT":
                value = -2;
                return true;
            case "VBSUNDAY":
            case "VBNULL":
            case "VBLONGDATE":
            case "VBUPPERCASE":
            case "VBFIRSTJAN1":
                value = 1;
                return true;
            case "VBUNICODE":
                value = 64;
                return true;
            case "VBDATABASECOMPARE":
                value = 2;
                return true;
            default:
                value = DBNull.Value;
                return false;
        }
    }

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

    private static bool AccessLike(string text, string pattern)
    {
        var builder = new StringBuilder(pattern.Length * 2);
        builder.Append('^');
        for (int patternIndex = 0; patternIndex < pattern.Length; patternIndex++)
        {
            char current = pattern[patternIndex];
            switch (current)
            {
                case '*':
                    builder.Append(".*");
                    break;
                case '?':
                    builder.Append('.');
                    break;
                case '#':
                    builder.Append("\\d");
                    break;
                case '[':
                    int endBracket = pattern.IndexOf(']', patternIndex + 1);
                    if (endBracket < 0)
                    {
                        return false;
                    }

                    string charClass = pattern.Substring(patternIndex + 1, endBracket - patternIndex - 1);
                    builder.Append('[');
                    if (charClass.StartsWith('!'))
                    {
                        builder.Append('^').Append(charClass, 1, charClass.Length - 1);
                    }
                    else
                    {
                        builder.Append(charClass);
                    }

                    builder.Append(']');
                    patternIndex = endBracket;
                    break;
                default:
                    builder.Append(Regex.Escape(current.ToString()));
                    break;
            }
        }

        builder.Append('$');
        try
        {
            return Regex.IsMatch(text, builder.ToString(), RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static DateTime DateSerial(int year, int month, int day)
    {
        if (year is >= 0 and < 100)
        {
            year += year <= 29 ? 2000 : 1900;
        }

        return new DateTime(year, 1, 1).AddMonths(month - 1).AddDays(day - 1);
    }

    private static DateTime DateAdd(string interval, int value, DateTime dateTime)
    {
        return interval.Trim().ToUpperInvariant() switch
        {
            "YYYY" => dateTime.AddYears(value),
            "Q" => dateTime.AddMonths(value * 3),
            "M" => dateTime.AddMonths(value),
            "Y" or "D" or "W" => dateTime.AddDays(value),
            "WW" => dateTime.AddDays(value * 7),
            "H" => dateTime.AddHours(value),
            "N" => dateTime.AddMinutes(value),
            "S" => dateTime.AddSeconds(value),
            _ => throw new ArgumentException($"Calculated-column DateAdd interval '{interval}' is not valid."),
        };
    }

    private static int DatePart(string interval, DateTime dateTime)
    {
        return interval.Trim().ToUpperInvariant() switch
        {
            "YYYY" => dateTime.Year,
            "Q" => ((dateTime.Month - 1) / 3) + 1,
            "M" => dateTime.Month,
            "Y" => dateTime.DayOfYear,
            "D" => dateTime.Day,
            "W" => Weekday(dateTime, 1),
            "WW" => CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dateTime, CalendarWeekRule.FirstDay, DayOfWeek.Sunday),
            "H" => dateTime.Hour,
            "N" => dateTime.Minute,
            "S" => dateTime.Second,
            _ => throw new ArgumentException($"Calculated-column DatePart interval '{interval}' is not valid."),
        };
    }

    private static int DateDiff(string interval, DateTime start, DateTime end)
    {
        int sign = start <= end ? 1 : -1;
        DateTime lower = sign > 0 ? start : end;
        DateTime upper = sign > 0 ? end : start;
        TimeSpan span = upper - lower;
        int result = interval.Trim().ToUpperInvariant() switch
        {
            "YYYY" => upper.Year - lower.Year,
            "Q" => ((upper.Year - lower.Year) * 4) + (((upper.Month - 1) / 3) - ((lower.Month - 1) / 3)),
            "M" => ((upper.Year - lower.Year) * 12) + (upper.Month - lower.Month),
            "Y" or "D" => (int)Math.Truncate(span.TotalDays),
            "W" or "WW" => (int)Math.Truncate(span.TotalDays / 7d),
            "H" => (int)Math.Truncate(span.TotalHours),
            "N" => (int)Math.Truncate(span.TotalMinutes),
            "S" => (int)Math.Truncate(span.TotalSeconds),
            _ => throw new ArgumentException($"Calculated-column DateDiff interval '{interval}' is not valid."),
        };
        return result * sign;
    }

    private static int Weekday(DateTime dateTime, int firstDay)
    {
        int sundayBased = ((int)dateTime.DayOfWeek) + 1;
        return (((sundayBased - 1) - (firstDay - 1) + 7) % 7) + 1;
    }

    private static string WeekdayName(int weekday, bool abbreviate, int firstDay)
    {
        int sundayBased = ((firstDay - 1) + (weekday - 1)) % 7;
        string[] names = abbreviate
            ? CultureInfo.InvariantCulture.DateTimeFormat.AbbreviatedDayNames
            : CultureInfo.InvariantCulture.DateTimeFormat.DayNames;
        return names[sundayBased];
    }

    private static int Asc(string text, bool asciiOnly)
    {
        if (string.IsNullOrEmpty(text))
        {
            throw new ArgumentException("Calculated-column Asc/AscW requires a non-empty string.");
        }

        int codePoint = text[0];
        if (asciiOnly && codePoint > 255)
        {
            throw new ArgumentException($"Calculated-column Asc character code '{codePoint}' is outside the ANSI byte range.");
        }

        return codePoint;
    }

    private static double Val(string text)
    {
        string compact = Regex.Replace(text, "\\s+", string.Empty);
        Match match = Regex.Match(compact, "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?", RegexOptions.CultureInvariant);
        return match.Success && double.TryParse(match.Value, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed)
            ? parsed
            : 0d;
    }

    private static int InStr(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        int start = 1;
        int textIndex = 0;
        if (args.Count > 2)
        {
            start = checked((int)ToDecimal(arg(0)));
            textIndex = 1;
        }

        string text = ToText(arg(textIndex));
        string search = ToText(arg(textIndex + 1));
        if (search.Length == 0)
        {
            return Math.Clamp(start, 1, text.Length + 1);
        }

        int zeroBasedStart = Math.Clamp(start - 1, 0, text.Length);
        int found = text.IndexOf(search, zeroBasedStart, args.Count > 3 ? CompareOptions(arg(3)) : StringComparison.OrdinalIgnoreCase);
        return found < 0 ? 0 : found + 1;
    }

    private static int InStrRev(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        string text = ToText(arg(0));
        string search = ToText(arg(1));
        int start = args.Count > 2 ? checked((int)ToDecimal(arg(2))) : -1;
        if (start == -1 || start > text.Length)
        {
            start = text.Length;
        }

        if (search.Length == 0)
        {
            return start;
        }

        int found = text.LastIndexOf(search, start - 1, args.Count > 3 ? CompareOptions(arg(3)) : StringComparison.OrdinalIgnoreCase);
        return found < 0 ? 0 : found + 1;
    }

    private static string ReplaceText(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        string text = ToText(arg(0));
        string search = ToText(arg(1));
        string replacement = ToText(arg(2));
        int start = args.Count > 3 ? Math.Max(1, checked((int)ToDecimal(arg(3)))) : 1;
        int count = args.Count > 4 ? checked((int)ToDecimal(arg(4))) : -1;
        StringComparison comparison = args.Count > 5 ? CompareOptions(arg(5)) : StringComparison.OrdinalIgnoreCase;
        if (start > text.Length || search.Length == 0 || count == 0)
        {
            return start > text.Length ? string.Empty : text.Substring(start - 1);
        }

        string prefix = text.Substring(0, start - 1);
        string tail = text.Substring(start - 1);
        var builder = new StringBuilder(prefix.Length + tail.Length);
        builder.Append(prefix);
        int replacements = 0;
        int position = 0;
        while (position < tail.Length)
        {
            int found = tail.IndexOf(search, position, comparison);
            if (found < 0 || (count >= 0 && replacements >= count))
            {
                builder.Append(tail, position, tail.Length - position);
                break;
            }

            builder.Append(tail, position, found - position);
            builder.Append(replacement);
            position = found + search.Length;
            replacements++;
        }

        return builder.ToString();
    }

    private static StringComparison CompareOptions(object? value)
    {
        if (IsNull(value))
        {
            return StringComparison.OrdinalIgnoreCase;
        }

        return checked((int)ToDecimal(value)) == 0 ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
    }

    private static string RepeatChar(int count, string value)
        => count <= 0 ? string.Empty : new string(string.IsNullOrEmpty(value) ? '\0' : value[0], count);

    private static string StrConv(string text, int conversion)
    {
        int caseConversion = conversion & 0x03;
        int characterConversion = conversion & ~0x03;
        string result = caseConversion switch
        {
            0 => text,
            1 => text.ToUpperInvariant(),
            2 => CultureInfo.InvariantCulture.TextInfo.ToLower(text),
            3 => CultureInfo.InvariantCulture.TextInfo.ToTitleCase(CultureInfo.InvariantCulture.TextInfo.ToLower(text)),
            _ => text,
        };

        if (characterConversion is not 0 and not 64)
        {
            throw new ArgumentException($"Calculated-column StrConv character conversion '{characterConversion}' is not valid in this row-local evaluator.");
        }

        return result;
    }

    private static int VarType(object? value)
    {
        if (IsNull(value))
        {
            return 1;
        }

        return value switch
        {
            bool => 11,
            byte => 17,
            short => 2,
            int or long => 3,
            float => 4,
            double => 5,
            decimal => 14,
            DateTime => 7,
            string => 8,
            _ => 12,
        };
    }

    private static string TypeName(object? value)
    {
        if (IsNull(value))
        {
            return "Null";
        }

        return value switch
        {
            bool => "Boolean",
            byte => "Byte",
            short => "Integer",
            int or long => "Long",
            float => "Single",
            double => "Double",
            decimal => "Decimal",
            DateTime => "Date",
            string => "String",
            _ => "Variant",
        };
    }

    private static string FormatValue(object value, string format)
    {
        if (IsNull(value))
        {
            return string.Empty;
        }

        string upperFormat = format.Trim().ToUpperInvariant();
        if (value is DateTime dateTime)
        {
            return upperFormat switch
            {
                "GENERAL DATE" => dateTime.ToString("G", CultureInfo.InvariantCulture),
                "LONG DATE" => dateTime.ToString("D", CultureInfo.InvariantCulture),
                "SHORT DATE" => dateTime.ToString("d", CultureInfo.InvariantCulture),
                "LONG TIME" => dateTime.ToString("T", CultureInfo.InvariantCulture),
                "SHORT TIME" => dateTime.ToString("t", CultureInfo.InvariantCulture),
                _ => dateTime.ToString(format, CultureInfo.InvariantCulture),
            };
        }

        if (value is IFormattable formattable)
        {
            return upperFormat switch
            {
                "GENERAL NUMBER" => formattable.ToString(null, CultureInfo.InvariantCulture),
                "CURRENCY" => formattable.ToString("C", CultureInfo.InvariantCulture),
                "FIXED" => formattable.ToString("F2", CultureInfo.InvariantCulture),
                "STANDARD" => formattable.ToString("N2", CultureInfo.InvariantCulture),
                "PERCENT" => formattable.ToString("P2", CultureInfo.InvariantCulture),
                "SCIENTIFIC" => formattable.ToString("E2", CultureInfo.InvariantCulture),
                _ => formattable.ToString(format, CultureInfo.InvariantCulture),
            };
        }

        return ToText(value);
    }

    private static string FormatNumber(decimal value, int decimalDigits, bool percent, bool currency)
    {
        decimal displayValue = percent ? value * 100m : value;
        string prefix = currency ? CultureInfo.InvariantCulture.NumberFormat.CurrencySymbol : string.Empty;
        string suffix = percent ? "%" : string.Empty;
        return prefix + displayValue.ToString("N" + Math.Max(0, decimalDigits).ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture) + suffix;
    }

    private static string FormatDateTime(DateTime value, int formatType)
    {
        return formatType switch
        {
            0 => value.ToString("G", CultureInfo.InvariantCulture),
            1 => value.ToString("D", CultureInfo.InvariantCulture),
            2 => value.ToString("d", CultureInfo.InvariantCulture),
            3 => value.ToString("T", CultureInfo.InvariantCulture),
            4 => value.ToString("t", CultureInfo.InvariantCulture),
            _ => throw new ArgumentException($"Calculated-column FormatDateTime type '{formatType}' is not valid."),
        };
    }

    private static double FinancialFutureValue(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
        => CalculateFutureValue(ToDouble(arg(0)), ToDouble(arg(1)), ToDouble(arg(2)), GetOptionalDouble(args, arg, 3, 0d), GetPaymentType(args, arg, 4));

    private static double FinancialPresentValue(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
        => CalculatePresentValue(ToDouble(arg(0)), ToDouble(arg(1)), ToDouble(arg(2)), GetOptionalDouble(args, arg, 3, 0d), GetPaymentType(args, arg, 4));

    private static double FinancialPayment(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
        => CalculatePayment(ToDouble(arg(0)), ToDouble(arg(1)), ToDouble(arg(2)), GetOptionalDouble(args, arg, 3, 0d), GetPaymentType(args, arg, 4));

    private static double FinancialPeriods(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        double rate = ToDouble(arg(0));
        double payment = ToDouble(arg(1));
        double presentValue = ToDouble(arg(2));
        double futureValue = GetOptionalDouble(args, arg, 3, 0d);
        int paymentType = GetPaymentType(args, arg, 4);
        if (rate == 0d)
        {
            return -1d * (futureValue + presentValue) / payment;
        }

        double compoundPayment = ((paymentType == 1) ? 1d + rate : 1d) * payment / rate;
        double numerator = Math.Log(Math.Abs(futureValue - compoundPayment));
        double denominator = Math.Log(Math.Abs(-presentValue - compoundPayment));
        return (numerator - denominator) / Math.Log(1d + rate);
    }

    private static double FinancialInterestPayment(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        double rate = ToDouble(arg(0));
        double period = ToDouble(arg(1));
        double periods = ToDouble(arg(2));
        double presentValue = ToDouble(arg(3));
        double futureValue = GetOptionalDouble(args, arg, 4, 0d);
        int paymentType = GetPaymentType(args, arg, 5);
        if (period == 1d && paymentType == 1)
        {
            return 0d;
        }

        double payment = CalculatePayment(rate, periods, presentValue, futureValue, paymentType);
        double result = CalculateFutureValue(rate, period - 1d, payment, presentValue, paymentType) * rate;
        return paymentType == 1 ? result / (1d + rate) : result;
    }

    private static double FinancialPrincipalPayment(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        double rate = ToDouble(arg(0));
        double periods = ToDouble(arg(2));
        double presentValue = ToDouble(arg(3));
        double futureValue = GetOptionalDouble(args, arg, 4, 0d);
        int paymentType = GetPaymentType(args, arg, 5);
        double payment = CalculatePayment(rate, periods, presentValue, futureValue, paymentType);
        double interestPayment = FinancialInterestPayment(args, arg);
        return payment - interestPayment;
    }

    private static double FinancialDoubleDecliningBalance(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        double cost = ToDouble(arg(0));
        double salvage = ToDouble(arg(1));
        double life = ToDouble(arg(2));
        double period = ToDouble(arg(3));
        double factor = GetOptionalDouble(args, arg, 4, 2d);
        if (cost < 0d || (life == 2d && period > 1d))
        {
            return 0d;
        }

        if (life < 2d || (life == 2d && period <= 1d))
        {
            return cost - salvage;
        }

        double firstPeriod = (factor * cost) / life;
        if (period <= 1d)
        {
            return Math.Min(firstPeriod, cost - salvage);
        }

        double decline = (life - factor) / life;
        double salvageAdjustment = Math.Max(salvage - (cost * Math.Pow(decline, period)), 0d);
        return Math.Max((firstPeriod * Math.Pow(decline, period - 1d)) - salvageAdjustment, 0d);
    }

    private static double FinancialSumOfYearsDepreciation(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        double cost = ToDouble(arg(0));
        double salvage = ToDouble(arg(1));
        double life = ToDouble(arg(2));
        double period = ToDouble(arg(3));
        return ((cost - salvage) * (life - period + 1d) * 2d) / (life * (life + 1d));
    }

    private static double FinancialRate(IReadOnlyList<ExpressionNode> args, Func<int, object> arg)
    {
        double periods = ToDouble(arg(0));
        double payment = ToDouble(arg(1));
        double presentValue = ToDouble(arg(2));
        double futureValue = GetOptionalDouble(args, arg, 3, 0d);
        int paymentType = GetPaymentType(args, arg, 4);
        double rate = GetOptionalDouble(args, arg, 5, 0.1d);
        double previousRate = 0d;
        double previousValue = presentValue + (payment * periods) + futureValue;
        for (int iteration = 0; iteration < 20; iteration++)
        {
            double factor = Math.Abs(rate) < 0.0000001d ? 1d + (periods * rate) : Math.Pow(1d + rate, periods);
            double currentValue = Math.Abs(rate) < 0.0000001d
                ? (presentValue * (1d + (periods * rate))) + (payment * (1d + (rate * paymentType)) * periods) + futureValue
                : (presentValue * factor) + (payment * ((1d / rate) + paymentType) * (factor - 1d)) + futureValue;
            if (Math.Abs(previousValue - currentValue) <= 0.0000001d)
            {
                return rate;
            }

            double nextRate = ((currentValue * previousRate) - (previousValue * rate)) / (currentValue - previousValue);
            previousRate = rate;
            previousValue = currentValue;
            rate = nextRate;
        }

        return rate;
    }

    private static double GetOptionalDouble(IReadOnlyList<ExpressionNode> args, Func<int, object> arg, int index, double defaultValue)
        => args.Count > index ? ToDouble(arg(index)) : defaultValue;

    private static int GetPaymentType(IReadOnlyList<ExpressionNode> args, Func<int, object> arg, int index)
        => args.Count > index && ToDecimal(arg(index)) != 0m ? 1 : 0;

    private static double CalculateFutureValue(double rate, double periods, double payment, double presentValue, int paymentType)
    {
        if (rate == 0d)
        {
            return -1d * (presentValue + (periods * payment));
        }

        double paymentFactor = paymentType == 1 ? rate + 1d : 1d;
        double compound = Math.Pow(rate + 1d, periods);
        return (((1d - compound) * paymentFactor * payment) / rate) - (presentValue * compound);
    }

    private static double CalculatePresentValue(double rate, double periods, double payment, double futureValue, int paymentType)
    {
        if (rate == 0d)
        {
            return -1d * ((periods * payment) + futureValue);
        }

        double paymentFactor = paymentType == 1 ? rate + 1d : 1d;
        double compound = Math.Pow(rate + 1d, periods);
        return ((((1d - compound) / rate) * paymentFactor * payment) - futureValue) / compound;
    }

    private static double CalculatePayment(double rate, double periods, double presentValue, double futureValue, int paymentType)
    {
        if (rate == 0d)
        {
            return -1d * (futureValue + presentValue) / periods;
        }

        double paymentFactor = paymentType == 1 ? rate + 1d : 1d;
        double compound = Math.Pow(rate + 1d, periods);
        return (futureValue + (presentValue * compound)) * rate / (paymentFactor * (1d - compound));
    }

    private static void RequireArgCount(string functionName, int actual, int min, int max)
    {
        if (actual < min || actual > max)
        {
            throw new ArgumentException(
                $"Calculated-column function '{functionName}' expects {min}" + (min == max ? string.Empty : $"..{max}") + $" argument(s), got {actual}.");
        }
    }

    private sealed class AccessExpressionNormalizer
    {
        private static readonly Dictionary<string, string> WordOperators = new(StringComparer.OrdinalIgnoreCase)
        {
            ["AND"] = "AND",
            ["OR"] = "OR",
            ["XOR"] = "XOR",
            ["EQV"] = "EQV",
            ["IMP"] = "IMP",
            ["MOD"] = "MOD",
            ["LIKE"] = "LIKE",
            ["BETWEEN"] = "BETWEEN",
            ["IN"] = "IN",
            ["IS"] = "IS",
            ["NOT"] = "NOT",
            ["NULL"] = "NULL",
            ["TRUE"] = "TRUE",
            ["FALSE"] = "FALSE",
            ["YES"] = "YES",
            ["NO"] = "NO",
            ["ON"] = "ON",
            ["OFF"] = "OFF",
        };

        private static BinaryOperatorInfo? GetBinaryOperator(Token token)
        {
            if (token.Kind == TokenKind.Backslash)
            {
                return new BinaryOperatorInfo("INTDIV", 10, false);
            }

            if (token.Kind == TokenKind.Operator)
            {
                return token.Text switch
                {
                    "^" => new BinaryOperatorInfo("^", 12, true),
                    "*" => new BinaryOperatorInfo("*", 11, false),
                    "/" => new BinaryOperatorInfo("/", 11, false),
                    "+" => new BinaryOperatorInfo("+", 8, false),
                    "-" => new BinaryOperatorInfo("-", 8, false),
                    "&" => new BinaryOperatorInfo("&", 7, false),
                    "=" or "<>" or "<" or "<=" or ">" or ">=" => new BinaryOperatorInfo(token.Text, 6, false),
                    _ => null,
                };
            }

            if (token.Kind != TokenKind.Word)
            {
                return null;
            }

            return token.Text.ToUpperInvariant() switch
            {
                "IMP" => new BinaryOperatorInfo("IMP", 1, false),
                "EQV" => new BinaryOperatorInfo("EQV", 2, false),
                "XOR" => new BinaryOperatorInfo("XOR", 3, false),
                "OR" => new BinaryOperatorInfo("OR", 4, false),
                "AND" => new BinaryOperatorInfo("AND", 5, false),
                "IS" => new BinaryOperatorInfo("IS", 6, false),
                "LIKE" => new BinaryOperatorInfo("LIKE", 6, false),
                "BETWEEN" => new BinaryOperatorInfo("BETWEEN", 6, false),
                "IN" => new BinaryOperatorInfo("IN", 6, false),
                "NOT" => new BinaryOperatorInfo("NOT", 6, false),
                "MOD" => new BinaryOperatorInfo("MOD", 9, false),
                _ => null,
            };
        }

        private readonly List<Token> tokens;
        private int position;
        private bool stopAtBetweenAnd;

        private AccessExpressionNormalizer(List<Token> tokens)
        {
            this.tokens = tokens;
        }

        public static string Normalize(string expression)
        {
            List<Token> tokens = Tokenize(expression);
            if (!tokens.Exists(static token => token.Kind is TokenKind.Word or TokenKind.Backslash || (token.Kind == TokenKind.Identifier && token.Text.EndsWith('$'))))
            {
                return expression;
            }

            var normalizer = new AccessExpressionNormalizer(tokens);
            string normalized = normalizer.ParseExpression(0);
            return normalizer.Peek().Kind == TokenKind.End ? normalized : expression;
        }

        private static List<Token> Tokenize(string expression)
        {
            var result = new List<Token>();
            for (int charIndex = 0; charIndex < expression.Length;)
            {
                char current = expression[charIndex];
                if (char.IsWhiteSpace(current))
                {
                    charIndex++;
                    continue;
                }

                if (current == '"')
                {
                    int start = charIndex;
                    charIndex++;
                    while (charIndex < expression.Length)
                    {
                        if (expression[charIndex] == '"')
                        {
                            if (charIndex + 1 < expression.Length && expression[charIndex + 1] == '"')
                            {
                                charIndex += 2;
                                continue;
                            }

                            charIndex++;
                            break;
                        }

                        charIndex++;
                    }

                    result.Add(new Token(TokenKind.Value, expression.Substring(start, charIndex - start)));
                    continue;
                }

                if (char.IsLetter(current) || current == '_')
                {
                    int start = charIndex;
                    charIndex++;
                    while (charIndex < expression.Length && (char.IsLetterOrDigit(expression[charIndex]) || expression[charIndex] == '_' || expression[charIndex] == '.'))
                    {
                        charIndex++;
                    }

                    if (charIndex < expression.Length && expression[charIndex] == '$')
                    {
                        charIndex++;
                    }

                    string text = expression.Substring(start, charIndex - start);
                    result.Add(new Token(WordOperators.ContainsKey(text) ? TokenKind.Word : TokenKind.Identifier, text));
                    continue;
                }

                if (char.IsDigit(current) || current == '.')
                {
                    int start = charIndex;
                    charIndex++;
                    while (charIndex < expression.Length && (char.IsDigit(expression[charIndex]) || expression[charIndex] == '.' || expression[charIndex] == 'E' || expression[charIndex] == 'e' || expression[charIndex] == '+' || expression[charIndex] == '-'))
                    {
                        char previous = expression[charIndex - 1];
                        char next = expression[charIndex];
                        if ((next == '+' || next == '-') && previous != 'E' && previous != 'e')
                        {
                            break;
                        }

                        charIndex++;
                    }

                    result.Add(new Token(TokenKind.Value, expression.Substring(start, charIndex - start)));
                    continue;
                }

                if (current == '(')
                {
                    result.Add(new Token(TokenKind.OpenParen, "("));
                    charIndex++;
                    continue;
                }

                if (current == ')')
                {
                    result.Add(new Token(TokenKind.CloseParen, ")"));
                    charIndex++;
                    continue;
                }

                if (current == ',')
                {
                    result.Add(new Token(TokenKind.Comma, ","));
                    charIndex++;
                    continue;
                }

                if (current == '\\')
                {
                    result.Add(new Token(TokenKind.Backslash, "\\"));
                    charIndex++;
                    continue;
                }

                if (charIndex + 1 < expression.Length)
                {
                    string twoChar = expression.Substring(charIndex, 2);
                    if (twoChar is "<>" or "<=" or ">=")
                    {
                        result.Add(new Token(TokenKind.Operator, twoChar));
                        charIndex += 2;
                        continue;
                    }
                }

                result.Add(new Token(TokenKind.Operator, current.ToString()));
                charIndex++;
            }

            result.Add(new Token(TokenKind.End, string.Empty));
            return result;
        }

        private string ParseExpression(int minimumPrecedence)
        {
            string left = ParsePrefix();
            while (true)
            {
                Token token = Peek();
                if (token.Kind is TokenKind.End or TokenKind.CloseParen or TokenKind.Comma)
                {
                    break;
                }

                if (stopAtBetweenAnd && token.IsWord("AND"))
                {
                    break;
                }

                BinaryOperatorInfo? info = GetBinaryOperator(token);
                if (info is null || info.Value.Precedence < minimumPrecedence)
                {
                    break;
                }

                Read();
                left = info.Value.Name switch
                {
                    "IS" => ParseIs(left),
                    "NOT" => ParsePostfixNot(left, info.Value.Precedence),
                    "BETWEEN" => ParseBetween(left, negate: false),
                    "IN" => ParseIn(left, negate: false),
                    "LIKE" => ParseFunctionBinary("LIKE", left, info.Value),
                    "MOD" => ParseFunctionBinary("MOD", left, info.Value),
                    "INTDIV" => ParseFunctionBinary("INTDIV", left, info.Value),
                    "AND" or "OR" or "XOR" or "EQV" or "IMP" => ParseFunctionBinary(info.Value.Name, left, info.Value),
                    _ => ParseInfix(left, info.Value),
                };
            }

            return left;
        }

        private string ParsePrefix()
        {
            Token token = Peek();
            if (token.IsWord("NOT"))
            {
                Read();
                return "NOT(" + ParseExpression(6) + ")";
            }

            if (token.Kind == TokenKind.Operator && (token.Text == "+" || token.Text == "-"))
            {
                Read();
                return token.Text + ParseExpression(12);
            }

            return ParsePrimary();
        }

        private string ParsePrimary()
        {
            Token token = Read();
            switch (token.Kind)
            {
                case TokenKind.Identifier:
                    if (Peek().Kind == TokenKind.OpenParen)
                    {
                        return ParseFunctionCall(token.Text);
                    }

                    return token.Text;
                case TokenKind.Word:
                    return token.Text.ToUpperInvariant() switch
                    {
                        "YES" or "ON" => "TRUE",
                        "NO" or "OFF" => "FALSE",
                        _ => token.Text,
                    };
                case TokenKind.Value:
                    return token.Text;
                case TokenKind.OpenParen:
                    string inner = ParseExpression(0);
                    Expect(TokenKind.CloseParen, ")");
                    return "(" + inner + ")";
                default:
                    throw new ArgumentException($"Unexpected token '{token.Text}' in calculated-column expression.");
            }
        }

        private string ParseFunctionCall(string name)
        {
            if (name.EndsWith('$'))
            {
                name = name.Substring(0, name.Length - 1);
            }

            Expect(TokenKind.OpenParen, "(");
            var arguments = new List<string>();
            if (Peek().Kind != TokenKind.CloseParen)
            {
                do
                {
                    arguments.Add(ParseExpression(0));
                    if (Peek().Kind != TokenKind.Comma)
                    {
                        break;
                    }

                    Read();
                }
                while (true);
            }

            Expect(TokenKind.CloseParen, ")");
            return name + "(" + string.Join(",", arguments) + ")";
        }

        private string ParseIs(string left)
        {
            bool negate = false;
            if (Peek().IsWord("NOT"))
            {
                Read();
                negate = true;
            }

            Token token = Read();
            if (!token.IsWord("NULL"))
            {
                throw new ArgumentException("Calculated-column 'Is' expressions are only supported for Null checks.");
            }

            string call = "ISNULL(" + left + ")";
            return negate ? "NOT(" + call + ")" : call;
        }

        private string ParsePostfixNot(string left, int precedence)
        {
            Token token = Read();
            if (token.IsWord("LIKE"))
            {
                return "NOT(" + ParseFunctionBinary("LIKE", left, new BinaryOperatorInfo("LIKE", precedence, false)) + ")";
            }

            if (token.IsWord("IN"))
            {
                return ParseIn(left, negate: true);
            }

            if (token.IsWord("BETWEEN"))
            {
                return ParseBetween(left, negate: true);
            }

            throw new ArgumentException($"Unexpected token '{token.Text}' after postfix Not in calculated-column expression.");
        }

        private string ParseBetween(string left, bool negate)
        {
            bool previousStop = stopAtBetweenAnd;
            stopAtBetweenAnd = true;
            string lower;
            try
            {
                lower = ParseExpression(0);
            }
            finally
            {
                stopAtBetweenAnd = previousStop;
            }

            Token separator = Read();
            if (!separator.IsWord("AND"))
            {
                throw new ArgumentException("Calculated-column Between expression is missing the And separator.");
            }

            string upper = ParseExpression(7);
            string call = "BETWEEN(" + left + "," + lower + "," + upper + ")";
            return negate ? "NOT(" + call + ")" : call;
        }

        private string ParseIn(string left, bool negate)
        {
            Expect(TokenKind.OpenParen, "(");
            var values = new List<string> { left };
            if (Peek().Kind != TokenKind.CloseParen)
            {
                do
                {
                    values.Add(ParseExpression(0));
                    if (Peek().Kind != TokenKind.Comma)
                    {
                        break;
                    }

                    Read();
                }
                while (true);
            }

            Expect(TokenKind.CloseParen, ")");
            string call = "IN(" + string.Join(",", values) + ")";
            return negate ? "NOT(" + call + ")" : call;
        }

        private string ParseFunctionBinary(string functionName, string left, BinaryOperatorInfo info)
        {
            string right = ParseExpression(info.RightAssociative ? info.Precedence : info.Precedence + 1);
            return functionName + "(" + left + "," + right + ")";
        }

        private string ParseInfix(string left, BinaryOperatorInfo info)
        {
            string right = ParseExpression(info.RightAssociative ? info.Precedence : info.Precedence + 1);
            return "(" + left + info.Name + right + ")";
        }

        private Token Peek() => tokens[position];

        private Token Read() => tokens[position++];

        private void Expect(TokenKind kind, string text)
        {
            Token token = Read();
            if (token.Kind != kind || (text.Length > 0 && token.Text != text))
            {
                throw new ArgumentException($"Expected '{text}' in calculated-column expression, got '{token.Text}'.");
            }
        }

        private readonly record struct BinaryOperatorInfo(string Name, int Precedence, bool RightAssociative);

        private readonly record struct Token(TokenKind Kind, string Text)
        {
            public bool IsWord(string text) => Kind == TokenKind.Word && Text.Equals(text, StringComparison.OrdinalIgnoreCase);
        }

        private enum TokenKind
        {
            End,
            Identifier,
            Value,
            Word,
            Operator,
            Backslash,
            OpenParen,
            CloseParen,
            Comma,
        }
    }
}
