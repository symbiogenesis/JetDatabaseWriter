namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>
/// The Access (VBA) operators whose result depends on their operands' types, following
/// the OLE Automation Variant rules VBA uses (<c>VarAdd</c>, <c>VarSub</c>,
/// <c>VarIdiv</c>, <c>VarMod</c> and friends, measured with oleaut32 on Windows; Access
/// itself was not checked). Null in any operand gives Null.
/// </summary>
/// <remarks>
/// Arithmetic on two non-date operands stays in <see cref="decimal"/>, as before. A date
/// operand switches to date-serial arithmetic: a date plus or minus a number, a Boolean,
/// numeric text or another date is a date, a date minus a date is a <see cref="double"/>
/// number of days, and <c>*</c>, <c>/</c> and <c>^</c> with a date give a
/// <see cref="double"/>. A date plus or minus a Decimal value is also a date: the engine
/// holds Currency and Decimal fields alike as <see cref="decimal"/>, and VBA returns a
/// date for Currency.
/// </remarks>
internal static class AccessVariantOperators
{
    /// <summary>Returns <c>left + right</c>.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The sum, the joined text when both operands are text, or Null.</returns>
    internal static object Add(object left, object right)
    {
        if (left is string && right is string)
        {
            return CalculatedExpressionTextFunctions.ConcatText(left, right);
        }

        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        return left is DateTime || right is DateTime
            ? FromOleDate(ToDateSerial(left) + ToDateSerial(right))
            : ToDecimal(left) + ToDecimal(right);
    }

    /// <summary>Returns <c>left - right</c>.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The difference, or Null.</returns>
    internal static object Subtract(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        if (left is DateTime && right is DateTime)
        {
            return ToDateSerial(left) - ToDateSerial(right);
        }

        return left is DateTime || right is DateTime
            ? FromOleDate(ToDateSerial(left) - ToDateSerial(right))
            : ToDecimal(left) - ToDecimal(right);
    }

    /// <summary>Returns <c>left * right</c>.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The product, or Null.</returns>
    internal static object Multiply(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        return left is DateTime || right is DateTime
            ? Finite(ToDateSerial(left) * ToDateSerial(right))
            : ToDecimal(left) * ToDecimal(right);
    }

    /// <summary>Returns <c>left / right</c>.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The quotient, or Null.</returns>
    /// <exception cref="DivideByZeroException"><paramref name="right"/> is zero.</exception>
    internal static object Divide(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        if (left is DateTime || right is DateTime)
        {
            double divisor = ToDateSerial(right);
            return divisor == 0d
                ? throw new DivideByZeroException()
                : Finite(ToDateSerial(left) / divisor);
        }

        return ToDecimal(left) / ToDecimal(right);
    }

    /// <summary>Returns <c>left ^ right</c>.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The power, or Null.</returns>
    internal static object Power(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        return left is DateTime || right is DateTime
            ? Finite(Math.Pow(ToDateSerial(left), ToDateSerial(right)))
            : Math.Pow(ToDouble(left), ToDouble(right));
    }

    /// <summary>Returns <c>-value</c>; the negation of a date is a date.</summary>
    /// <param name="value">The operand.</param>
    /// <returns>The negated value, or Null.</returns>
    internal static object Negate(object value)
    {
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        return value is DateTime ? FromOleDate(-ToDateSerial(value)) : -ToDecimal(value);
    }

    /// <summary>Returns <c>+value</c>; a date stays a date.</summary>
    /// <param name="value">The operand.</param>
    /// <returns>The value as a number or date, or Null.</returns>
    internal static object Identity(object value)
    {
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        return value is DateTime ? value : ToDecimal(value);
    }

    /// <summary>
    /// Returns <c>left \ right</c>: both operands are rounded half to even to a Long
    /// first, then the quotient is truncated toward zero.
    /// </summary>
    /// <param name="left">The dividend.</param>
    /// <param name="right">The divisor.</param>
    /// <returns>The quotient as an <see cref="int"/>, or Null.</returns>
    /// <exception cref="DivideByZeroException">The rounded divisor is zero.</exception>
    internal static object IntegerDivide(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        int dividend = ToRoundedLong(left);
        int divisor = ToRoundedLong(right);
        return divisor == 0 ? throw new DivideByZeroException() : dividend / divisor;
    }

    /// <summary>
    /// Returns <c>left Mod right</c>: both operands are rounded half to even to a Long
    /// first, and the remainder takes the dividend's sign.
    /// </summary>
    /// <param name="left">The dividend.</param>
    /// <param name="right">The divisor.</param>
    /// <returns>The remainder as an <see cref="int"/>, or Null.</returns>
    /// <exception cref="DivideByZeroException">The rounded divisor is zero.</exception>
    internal static object Modulo(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        int dividend = ToRoundedLong(left);
        int divisor = ToRoundedLong(right);
        return divisor == 0 ? throw new DivideByZeroException() : dividend % divisor;
    }

    /// <summary>
    /// Converts an operand to a date serial (days since 1899-12-30): a date by its
    /// serial, a Boolean as -1 or 0, a number as itself, and text that parses as a
    /// number. Anything else is a type mismatch.
    /// </summary>
    /// <param name="value">The non-null operand.</param>
    /// <returns>The serial.</returns>
    /// <exception cref="InvalidCastException">The operand is not a date or a number.</exception>
    internal static double ToDateSerial(object value) => value switch
    {
        DateTime date => date.ToOADate(),
        bool boolean => boolean ? -1d : 0d,
        byte or short or int or long or float or double or decimal => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        string text when double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed) => parsed,
        _ => throw TypeMismatch(value),
    };

    /// <summary>
    /// Rounds an operand half to even to a VBA Long, the operand form of <c>\</c>,
    /// <c>Mod</c> and the bitwise operators.
    /// </summary>
    /// <param name="value">The non-null operand.</param>
    /// <returns>The rounded value.</returns>
    /// <exception cref="InvalidCastException">The operand is not a date or a number.</exception>
    /// <exception cref="OverflowException">The rounded value is outside the Long range.</exception>
    internal static int ToRoundedLong(object value)
    {
        decimal number = value switch
        {
            bool boolean => boolean ? -1m : 0m,
            byte or short or int or long or decimal => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
            float or double => RoundedDouble(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
            DateTime date => RoundedDouble(date.ToOADate()),
            string text when decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal parsed) => parsed,
            _ => throw TypeMismatch(value),
        };

        decimal rounded = Math.Round(number, MidpointRounding.ToEven);
        return rounded is < int.MinValue or > int.MaxValue
            ? throw new OverflowException($"{rounded.ToString(CultureInfo.InvariantCulture)} is outside the range of a Long.")
            : (int)rounded;
    }

    /// <summary>Returns the "Type mismatch" error VBA raises for an operand of the wrong type.</summary>
    /// <param name="value">The operand.</param>
    /// <returns>The exception to throw.</returns>
    internal static InvalidCastException TypeMismatch(object value)
        => new($"Type mismatch: {ToText(value)} ({value.GetType().Name}) is not a number or a date.");

    private static decimal RoundedDouble(double value)
    {
        double rounded = Math.Round(value, MidpointRounding.ToEven);
        return double.IsNaN(rounded) || rounded < int.MinValue || rounded > int.MaxValue
            ? throw new OverflowException($"{value.ToString("R", CultureInfo.InvariantCulture)} is outside the range of a Long.")
            : (decimal)rounded;
    }

    private static double Finite(double value)
        => double.IsInfinity(value) || double.IsNaN(value)
            ? throw new OverflowException("The result is outside the range of a Double.")
            : value;
}
