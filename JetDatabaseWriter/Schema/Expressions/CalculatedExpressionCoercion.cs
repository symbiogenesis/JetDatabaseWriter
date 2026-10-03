namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;

internal static class CalculatedExpressionCoercion
{
    internal static object CoerceResult(object? value, Type targetType)
    {
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        // Access stores True as -1 in a numeric column and "-1" in a text
        // column (Excel and Convert use 1 and "True"); ToText handles text.
        if (value is bool boolean && targetType != typeof(bool) && targetType != typeof(string))
        {
            value = boolean ? -1 : 0;
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

    /// <summary>
    /// Converts a caller-supplied column value to the column's declared type
    /// before an expression sees it, with the same <see cref="Convert"/> call
    /// the row encoder uses to store it. The writer accepts <c>"10"</c> for an
    /// Integer column and <c>10</c> for a Text column, and the evaluator's text
    /// rules (<c>+</c> joins two strings, two strings compare as text) must
    /// follow the field's type, not the CLR type the caller happened to pass.
    /// A value the encoder could not store either is passed through unchanged,
    /// so the encoder still reports it.
    /// </summary>
    /// <param name="value">The value supplied for the column.</param>
    /// <param name="declaredType">The column's declared CLR type.</param>
    /// <returns>The value as the column will hold it.</returns>
    internal static object CoerceInput(object? value, Type declaredType)
    {
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        if (declaredType.IsInstanceOfType(value))
        {
            return value!;
        }

        // Hyperlinks, byte[] payloads and other non-scalar values keep their shape.
        if (value is not IConvertible)
        {
            return value!;
        }

        try
        {
            if (declaredType == typeof(Guid))
            {
                return value is string guidText ? Guid.Parse(guidText) : value;
            }

            return IsStoredScalarType(declaredType)
                ? Convert.ChangeType(value, declaredType, CultureInfo.InvariantCulture) ?? value
                : value;
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

        return value;
    }

    internal static object EvaluateNumeric(object left, object right, Func<decimal, decimal, decimal> operation)
        => IsNull(left) || IsNull(right) ? DBNull.Value : operation(ToDecimal(left), ToDecimal(right));

    internal static object CompareValues(object left, object right, Func<int, bool> predicate)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        return CompareNonNullValues(left, right, predicate);
    }

    internal static bool CompareNonNullValues(object left, object right, Func<int, bool> predicate)
    {
        int comparison;
        if (left is string leftText && right is string rightText)
        {
            // Two strings compare as text in Access, even when both look numeric ("10" < "9").
            comparison = string.Compare(leftText, rightText, StringComparison.OrdinalIgnoreCase);
        }
        else if (TryConvertDecimal(left, out decimal leftDecimal) && TryConvertDecimal(right, out decimal rightDecimal))
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

    internal static bool IsNull(object? value) => value is null or DBNull;

    internal static double DeterministicRandomValue(double seed)
    {
        uint state = unchecked((uint)(int)Math.Truncate(seed * 1000000d));
        state ^= 0x6C8E9CF5u;
        state = unchecked((state * 1664525u) + 1013904223u);
        return (state & 0x00FFFFFFu) / 16777216d;
    }

    internal static string NormalizeFunctionName(string name)
    {
        string upperName = name.ToUpperInvariant();
        return upperName.EndsWith('$') ? upperName[..^1] : upperName;
    }

    internal static bool TryGetBuiltinConstant(string name, out object value)
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

    /// <summary>
    /// Converts a value to text the way the Access expression service does:
    /// True is <c>"-1"</c> and False is <c>"0"</c>, so <c>"x" &amp; (1 &lt; 2)</c>
    /// is <c>"x-1"</c> (<see cref="Convert"/> gives <c>"True"</c>).
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <returns>The text value; empty for Null.</returns>
    internal static string ToText(object? value)
    {
        if (value is bool boolean)
        {
            return boolean ? "-1" : "0";
        }

        return IsNull(value) ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    /// <summary>
    /// Maps a Boolean argument to Access's -1 / 0 before a <see cref="Convert"/>
    /// call, which would otherwise turn True into 1 (<c>CInt(True)</c> is -1).
    /// </summary>
    /// <param name="value">The function argument.</param>
    /// <returns>The argument, with a Boolean replaced by -1 or 0.</returns>
    internal static object? AsAccessNumber(object? value)
    {
        if (value is bool boolean)
        {
            return boolean ? -1 : 0;
        }

        return value;
    }

    /// <summary>
    /// Converts a value to a number the way Access does: True is -1 and False
    /// is 0 (<see cref="Convert"/> and Excel map True to 1).
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <returns>The numeric value.</returns>
    internal static decimal ToDecimal(object? value)
    {
        if (value is bool boolean)
        {
            return boolean ? -1m : 0m;
        }

        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
    }

    internal static double ToDouble(object? value)
    {
        if (value is bool boolean)
        {
            return boolean ? -1d : 0d;
        }

        return Convert.ToDouble(value, CultureInfo.InvariantCulture);
    }

    internal static bool ToBoolean(object? value)
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

    internal static DateTime ToDateTime(object? value)
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

    internal static DateTime ParseDate(string text)
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

    internal static bool TryConvertDecimal(object? value, out decimal result)
    {
        if (IsNull(value))
        {
            result = 0;
            return false;
        }

        try
        {
            result = ToDecimal(value);
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

    internal static bool TryConvertDateTime(object? value, out DateTime result)
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

    private static bool IsStoredScalarType(Type type)
        => type == typeof(string) || type == typeof(bool) || type == typeof(byte) || type == typeof(short)
            || type == typeof(int) || type == typeof(long) || type == typeof(float) || type == typeof(double)
            || type == typeof(decimal) || type == typeof(DateTime);
}
