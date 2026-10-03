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
    /// <summary>The operand and result types of the bitwise operators, narrowest first.</summary>
    private enum OperandKind
    {
        Bool = 0,
        Byte = 1,
        Int16 = 2,
        Int32 = 3,
        Int64 = 4,
        Other = 5,
    }

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
    /// Returns <c>left And right</c>: logical on two Booleans, bitwise on numbers. With
    /// Null, a False or zero operand decides the result; otherwise the result is Null.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The result, typed by <see cref="ResultKind"/>, or Null.</returns>
    /// <exception cref="InvalidCastException">An operand is not a number, Boolean or date.</exception>
    /// <exception cref="OverflowException">An operand is outside the Long range.</exception>
    internal static object And(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            object other = IsNull(left) ? right : left;
            return !IsNull(other) && IsZero(other) ? Bitwise(other, other, static (l, r) => l & r) : DBNull.Value;
        }

        return Bitwise(left, right, static (l, r) => l & r);
    }

    /// <summary>
    /// Returns <c>left Or right</c>: logical on two Booleans, bitwise on numbers. With
    /// Null, a True or nonzero operand decides the result; otherwise the result is Null.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The result, typed by <see cref="ResultKind"/>, or Null.</returns>
    /// <exception cref="InvalidCastException">An operand is not a number, Boolean or date.</exception>
    /// <exception cref="OverflowException">An operand is outside the Long range.</exception>
    internal static object Or(object left, object right)
    {
        if (IsNull(left) || IsNull(right))
        {
            object other = IsNull(left) ? right : left;
            return !IsNull(other) && !IsZero(other) ? Bitwise(other, other, static (l, r) => l | r) : DBNull.Value;
        }

        return Bitwise(left, right, static (l, r) => l | r);
    }

    /// <summary>Returns <c>left Xor right</c>; Null when either operand is Null.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The result, typed by <see cref="ResultKind"/>, or Null.</returns>
    /// <exception cref="InvalidCastException">An operand is not a number, Boolean or date.</exception>
    /// <exception cref="OverflowException">An operand is outside the Long range.</exception>
    internal static object Xor(object left, object right)
        => IsNull(left) || IsNull(right) ? DBNull.Value : Bitwise(left, right, static (l, r) => l ^ r);

    /// <summary>Returns <c>left Eqv right</c>, which is <c>Not (left Xor right)</c>.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The result, or Null.</returns>
    internal static object Eqv(object left, object right) => Not(Xor(left, right));

    /// <summary>
    /// Returns <c>left Imp right</c>, which is <c>(Not left) Or right</c>, so
    /// <c>False Imp Null</c> is True and <c>True Imp Null</c> is Null.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>The result, or Null.</returns>
    internal static object Imp(object left, object right) => Or(Not(left), right);

    /// <summary>
    /// Returns <c>Not value</c>: logical on a Boolean, the bitwise complement of a
    /// number (Byte stays Byte, Integer stays Integer), and Null for Null.
    /// </summary>
    /// <param name="value">The operand.</param>
    /// <returns>The result, or Null.</returns>
    /// <exception cref="InvalidCastException">The operand is not a number, Boolean or date.</exception>
    /// <exception cref="OverflowException">The operand is outside the Long range.</exception>
    internal static object Not(object value) => value switch
    {
        DBNull or null => DBNull.Value,
        bool boolean => !boolean,
        byte b => unchecked((byte)~b),
        short s => unchecked((short)~s),
        int i => ~i,
        long l => ~l,
        _ => ~ToRoundedLong(value),
    };

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

    /// <summary>
    /// Returns the type of a bitwise result, as OLE Automation picks it: two Booleans
    /// give a Boolean, two Bytes a Byte, any mix of Boolean, Byte and Integer an
    /// Integer, anything with a 64-bit integer a 64-bit integer, and everything else
    /// (Long, Single, Double, Decimal, dates and numeric text) a Long.
    /// </summary>
    /// <param name="left">The left operand's kind.</param>
    /// <param name="right">The right operand's kind.</param>
    /// <returns>The result kind.</returns>
    private static OperandKind ResultKind(OperandKind left, OperandKind right)
    {
        if (left == right && left is OperandKind.Bool or OperandKind.Byte)
        {
            return left;
        }

        if (left <= OperandKind.Int16 && right <= OperandKind.Int16)
        {
            return OperandKind.Int16;
        }

        return left == OperandKind.Int64 || right == OperandKind.Int64 ? OperandKind.Int64 : OperandKind.Int32;
    }

    private static OperandKind KindOf(object value) => value switch
    {
        bool => OperandKind.Bool,
        byte => OperandKind.Byte,
        short => OperandKind.Int16,
        int => OperandKind.Int32,
        long => OperandKind.Int64,
        _ => OperandKind.Other,
    };

    private static object Bitwise(object left, object right, Func<long, long, long> operation)
    {
        OperandKind kind = ResultKind(KindOf(left), KindOf(right));
        long result = operation(ToBitwiseOperand(left), ToBitwiseOperand(right));
        return kind switch
        {
            OperandKind.Bool => result != 0,
            OperandKind.Byte => unchecked((byte)result),
            OperandKind.Int16 => unchecked((short)result),
            OperandKind.Int64 => result,
            OperandKind.Int32 or OperandKind.Other => unchecked((int)result),
            _ => throw new InvalidOperationException($"Unexpected bitwise result kind '{kind}'."),
        };
    }

    private static long ToBitwiseOperand(object value) => value switch
    {
        bool boolean => boolean ? -1L : 0L,
        byte or short or int or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        _ => ToRoundedLong(value),
    };

    private static bool IsZero(object value) => ToBitwiseOperand(value) == 0;

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
