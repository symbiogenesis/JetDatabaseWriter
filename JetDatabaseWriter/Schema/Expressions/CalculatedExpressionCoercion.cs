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

    internal static string ToText(object? value)
        => IsNull(value) ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

    internal static decimal ToDecimal(object? value)
        => Convert.ToDecimal(value, CultureInfo.InvariantCulture);

    internal static double ToDouble(object? value)
        => Convert.ToDouble(value, CultureInfo.InvariantCulture);

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
}
