namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>
/// An Access column <c>DefaultValue</c> expression compiled for evaluation on insert.
/// Numeric and <c>{guid ...}</c> literals are parsed directly, so a decimal default keeps
/// all its digits. Anything else (<c>"text"</c>, <c>True</c>, <c>#2020-01-31#</c>,
/// <c>Date()</c>, <c>=Now()</c>, <c>1+2</c>) is evaluated by the calculated-column
/// expression engine, and the result is converted to the column's CLR type.
/// </summary>
internal sealed class ColumnDefaultValue
{
    /// <summary>
    /// The default for a column whose expression this library cannot parse. It never
    /// produces a value.
    /// </summary>
    public static readonly ColumnDefaultValue Unsupported = new(null, null);

    private readonly object? literal;
    private readonly CalculatedExpressionPlan? plan;

    private ColumnDefaultValue(object? literal, CalculatedExpressionPlan? plan)
    {
        this.literal = literal;
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
            return new ColumnDefaultValue(guid, null);
        }

        if (decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal number))
        {
            return new ColumnDefaultValue(number, null);
        }

        try
        {
            return new ColumnDefaultValue(null, CalculatedExpressionPlan.Parse(text));
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or InvalidOperationException or FormatException)
        {
            return Unsupported;
        }
    }

    /// <summary>
    /// Evaluates the default and converts it to <paramref name="clrType"/>. Returns
    /// <see langword="false"/>, leaving the column null, when the expression is
    /// unsupported, evaluates to Null, uses a function or name this library cannot
    /// evaluate, or yields a value that cannot be converted to the column's type.
    /// </summary>
    /// <param name="clrType">The CLR type the column stores.</param>
    /// <param name="createContext">Creates the evaluation context over the row being inserted.</param>
    /// <param name="value">The default value, converted to <paramref name="clrType"/>.</param>
    /// <returns>Whether a default value was produced.</returns>
    public bool TryEvaluate(Type clrType, Func<CalculatedExpressionEvaluationContext> createContext, out object value)
    {
        value = DBNull.Value;
        if ((this.literal is null && this.plan is null) || !IsSupportedTarget(clrType))
        {
            return false;
        }

        try
        {
            object raw = this.literal ?? this.plan!.Root.Evaluate(createContext(), this.plan);
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
}
