namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>
/// An Access column <c>DefaultValue</c> expression compiled for evaluation on insert.
/// Numeric and <c>{guid ...}</c> literals are parsed directly. A numeric literal is
/// parsed once as a double, a float and a decimal: a Double or Single column takes its
/// own parse, which is exact, and every other column takes the decimal, so a decimal
/// default keeps all its digits. Anything else (<c>"text"</c>, <c>True</c>,
/// <c>#2020-01-31#</c>, <c>Date()</c>, <c>=Now()</c>, <c>1+2</c>) is evaluated by the
/// calculated-column expression engine, and the result is converted to the column's CLR
/// type.
/// </summary>
internal sealed class ColumnDefaultValue
{
    /// <summary>
    /// The default for a column whose expression this library cannot parse. It never
    /// produces a value.
    /// </summary>
    public static readonly ColumnDefaultValue Unsupported = new(null, null, null);

    private readonly object? literal;
    private readonly NumericLiteral? number;
    private readonly CalculatedExpressionPlan? plan;

    private ColumnDefaultValue(object? literal, NumericLiteral? number, CalculatedExpressionPlan? plan)
    {
        this.literal = literal;
        this.number = number;
        this.plan = plan;
    }

    /// <summary>
    /// Compiles <paramref name="expression"/>, the text of an Access <c>DefaultValue</c>
    /// property. Returns <see cref="Unsupported"/> when it is empty or is not syntax this
    /// library can parse.
    /// </summary>
    /// <param name="expression">The expression text; a leading <c>=</c> is ignored.</param>
    /// <returns>The compiled default.</returns>
    public static ColumnDefaultValue Compile(string expression)
    {
        string text = expression.Trim();
        if (text.StartsWith('='))
        {
            text = text[1..].TrimStart();
        }

        if (text.Length == 0)
        {
            return Unsupported;
        }

        if (TryParseGuidLiteral(text, out Guid guid))
        {
            return new ColumnDefaultValue(guid, null, null);
        }

        if (NumericLiteral.TryParse(text, out NumericLiteral number))
        {
            return new ColumnDefaultValue(null, number, null);
        }

        try
        {
            return new ColumnDefaultValue(null, null, CalculatedExpressionPlan.Parse(text));
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or InvalidOperationException or FormatException)
        {
            return Unsupported;
        }
    }

    /// <summary>
    /// Reads a numeric CLR <c>DefaultValue</c> back from the literal it is persisted as
    /// (<see cref="JetExpressionConverter.ToJetExpression"/>), the way a writer that
    /// loads the literal from the file evaluates it for a column of
    /// <paramref name="clrType"/>. The declaring writer applies this value, so it stores
    /// what every later writer stores: <c>0.1f</c> on a Double column is 0.1, and a
    /// <see cref="decimal"/> on a Double or Single column is parsed rather than converted.
    /// </summary>
    /// <param name="value">The CLR default.</param>
    /// <param name="clrType">The CLR type the column stores.</param>
    /// <param name="stored">
    /// The value the literal gives, converted to <paramref name="clrType"/>; or
    /// <see langword="null"/> when <paramref name="value"/> is not a number or its literal
    /// gives no default: a NaN or infinity, which has no literal, or a number the column's
    /// type cannot hold, such as 1e39 in a Single column or 300 in a Byte column.
    /// </param>
    /// <returns>Whether <paramref name="value"/> is a number of any CLR numeric type.</returns>
    public static bool TryReadBackNumber(object? value, Type clrType, out object? stored)
    {
        stored = null;
        if (value is not (byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal))
        {
            return false;
        }

        if (NumericLiteral.TryParse(JetExpressionConverter.ToJetExpression(value)!, out NumericLiteral number)
            && new ColumnDefaultValue(null, number, null).TryEvaluate(
                clrType,
                static () => throw new InvalidOperationException("A numeric literal needs no evaluation context."),
                out object evaluated))
        {
            stored = evaluated;
        }

        return true;
    }

    /// <summary>
    /// Evaluates the default and converts it to <paramref name="clrType"/>. Returns
    /// <see langword="false"/>, leaving the column null, when the expression is
    /// unsupported, evaluates to Null, uses a function or name this library cannot
    /// evaluate, or yields a value that cannot be converted to the column's type
    /// (including a numeric literal too large for a Single column).
    /// </summary>
    /// <param name="clrType">The CLR type the column stores.</param>
    /// <param name="createContext">
    /// Creates the evaluation context over the row being inserted. A numeric or
    /// <c>{guid ...}</c> literal never calls it.
    /// </param>
    /// <param name="value">The default value, converted to <paramref name="clrType"/>.</param>
    /// <returns>Whether a default value was produced.</returns>
    public bool TryEvaluate(Type clrType, Func<CalculatedExpressionEvaluationContext> createContext, out object value)
    {
        value = DBNull.Value;
        if ((this.literal is null && this.number is null && this.plan is null) || !IsSupportedTarget(clrType))
        {
            return false;
        }

        if (this.number is NumericLiteral number)
        {
            if (clrType == typeof(double))
            {
                value = number.AsDouble;
                return true;
            }

            if (clrType == typeof(float))
            {
                if (number.AsSingle is not float single)
                {
                    return false;
                }

                value = single;
                return true;
            }
        }

        try
        {
            object raw = this.number?.ForOtherTypes ?? this.literal ?? this.plan!.Root.Evaluate(createContext(), this.plan);
            if (IsNull(raw))
            {
                return false;
            }

            value = CoerceResult(raw, clrType);
            return true;
        }
        catch (Exception ex) when (ColumnValidationRule.IsEvaluationFailure(ex))
        {
            return false;
        }
    }

    private static bool TryParseGuidLiteral(string text, out Guid guid)
    {
        // Access writes GUID defaults as {guid {xxxxxxxx-...}}; JetExpressionConverter
        // writes {guid xxxxxxxx-...}. Guid.TryParse accepts both inner forms.
        guid = Guid.Empty;
        return text.StartsWith("{guid", StringComparison.OrdinalIgnoreCase)
            && text.EndsWith('}')
            && Guid.TryParse(text[5..^1].Trim(), out guid);
    }

    private static bool IsSupportedTarget(Type clrType) =>
        clrType == typeof(string)
        || clrType == typeof(bool)
        || clrType == typeof(byte)
        || clrType == typeof(short)
        || clrType == typeof(int)
        || clrType == typeof(long)
        || clrType == typeof(float)
        || clrType == typeof(double)
        || clrType == typeof(decimal)
        || clrType == typeof(DateTime)
        || clrType == typeof(Guid);

    /// <summary>
    /// A finite numeric literal, parsed once as each type a column can take it as.
    /// Parsing it as a decimal first and converting would turn anything below about
    /// 1e-28 into 0 and round the rest twice.
    /// </summary>
    /// <param name="AsDouble">The literal as a double.</param>
    /// <param name="AsSingle">The literal as a float, or <see langword="null"/> when it is too large for one.</param>
    /// <param name="AsDecimal">The literal as a decimal, or <see langword="null"/> when it is out of the decimal range.</param>
    private readonly record struct NumericLiteral(double AsDouble, float? AsSingle, decimal? AsDecimal)
    {
        /// <summary>
        /// Gets the value a column of any other type converts from: the decimal, which
        /// keeps every digit of a decimal default, or the double when the literal is out
        /// of the decimal range.
        /// </summary>
        public object ForOtherTypes => this.AsDecimal is decimal m ? m : this.AsDouble;

        /// <summary>
        /// Parses <paramref name="text"/> as a finite invariant-culture number. The
        /// <c>NaN</c> and <c>Infinity</c> symbols are not Access literals and do not parse.
        /// </summary>
        /// <param name="text">The trimmed expression text.</param>
        /// <param name="literal">The parsed literal.</param>
        /// <returns>Whether <paramref name="text"/> is a finite numeric literal.</returns>
        public static bool TryParse(string text, out NumericLiteral literal)
        {
            literal = default;
            if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d) || !double.IsFinite(d))
            {
                return false;
            }

            float? single = float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out float f) && float.IsFinite(f) ? f : null;
            decimal? m = decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal parsed) ? parsed : null;
            literal = new NumericLiteral(d, single, m);
            return true;
        }
    }
}
