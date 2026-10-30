namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;

internal static class CalculatedExpressionNumericFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, "PARTITION", 4, 4, static function => EvaluatePartition(function));
        AddFunction(functions, "MOD", 2, 2, static function => AccessVariantOperators.Modulo(function.Arg(0), function.Arg(1)));
        AddFunction(functions, "INTDIV", 2, 2, static function => AccessVariantOperators.IntegerDivide(function.Arg(0), function.Arg(1)));
        AddFunction(functions, "ABS", 1, 1, static function => EvaluateAbsolute(function.Arg(0)));
        AddFunction(functions, "ATAN", 1, 1, static function => Math.Atan(ToDouble(function.Arg(0))), "ATN");
        AddFunction(functions, "COS", 1, 1, static function => Math.Cos(ToDouble(function.Arg(0))));
        AddFunction(functions, "EXP", 1, 1, static function => Math.Exp(ToDouble(function.Arg(0))));
        AddFunction(functions, "LOG", 1, 1, static function => Math.Log(ToDouble(function.Arg(0))));
        AddFunction(functions, "RND", 0, 1, static function => function.Context.NextRandom(function.Count > 0 ? function.Arg(0) : DBNull.Value));
        AddFunction(functions, "SGN", 1, 1, static function => Math.Sign(ToDecimal(function.Arg(0))));
        AddFunction(functions, "SIN", 1, 1, static function => Math.Sin(ToDouble(function.Arg(0))));
        AddFunction(functions, "SQR", 1, 1, static function => Math.Sqrt(ToDouble(function.Arg(0))), "SQRT");
        AddFunction(functions, "TAN", 1, 1, static function => Math.Tan(ToDouble(function.Arg(0))));
        AddFunction(functions, "ROUND", 1, 2, static function => EvaluateRound(function));
        AddFunction(functions, "INT", 1, 1, static function => EvaluateIntegral(function.Arg(0), truncate: false));
        AddFunction(functions, "FIX", 1, 1, static function => EvaluateIntegral(function.Arg(0), truncate: true));
        AddFunction(functions, "CINT", 1, 1, static function => Convert.ToInt16(AsAccessNumber(function.Arg(0)), CultureInfo.InvariantCulture));
        AddFunction(functions, "CLNG", 1, 1, static function => Convert.ToInt32(AsAccessNumber(function.Arg(0)), CultureInfo.InvariantCulture));
        AddFunction(functions, "CDBL", 1, 1, static function => AccessVariantOperators.Finite(ToDouble(function.Arg(0))));
        AddFunction(functions, "CSNG", 1, 1, static function => (float)AccessVariantOperators.Finite(ToSingle(function.Arg(0))));
        AddFunction(functions, "CDEC", 1, 1, static function => ToDecimal(function.Arg(0)));
        AddFunction(functions, "CCUR", 1, 1, static function => ToCurrency(function.Arg(0)));
        AddFunction(functions, "CBYTE", 1, 1, static function => ToByte(function.Arg(0)));
    }

    private static object EvaluateRound(CalculatedFunctionInvocation function)
    {
        object value = function.Arg(0);
        int digits = function.Count > 1 ? AccessVariantOperators.ToRoundedLong(function.Arg(1)) : 0;
        if (digits < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(function), "Round requires a nonnegative number of decimal places.");
        }

        // Native Round validates its precision even when the result is Null.
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        return value switch
        {
            AccessCurrency currency => digits >= 4 ? currency : AccessCurrency.FromDecimal(Math.Round(currency.Value, digits, MidpointRounding.ToEven)),
            bool boolean => (short)(boolean ? -1 : 0),
            byte number => (short)number,
            short or int or long => value,
            float number => (float)RoundFloating(number, digits),
            double number => RoundFloating(number, digits),
            DateTime date => FromOleDate(RoundFloating(date.ToOADate(), digits)),
            decimal number => digits >= 28 ? number : Math.Round(number, digits, MidpointRounding.ToEven),
            _ => RoundFloating(ToDouble(value), digits),
        };
    }

    private static double RoundFloating(double number, int digits)
    {
        number = AccessVariantOperators.Finite(number);
        if (digits > 22)
        {
            throw new ArgumentOutOfRangeException(nameof(digits), "Floating Round supports at most 22 decimal places.");
        }

        // DAO rounds the binary floating value after decimal scaling. Converting
        // it to Decimal first changes native midpoints such as 1.225 and Single
        // values, and rejects otherwise valid large Double values.
        double scale = 1d;
        for (int index = 0; index < digits; index++)
        {
            scale *= 10d;
        }

        double scaled = number * scale;
        return double.IsInfinity(scaled)
            ? number
            : Math.Round(scaled, MidpointRounding.ToEven) / scale;
    }

#pragma warning disable CA1859 // Partition returns either a range string or Access Null (DBNull).
    private static object EvaluatePartition(CalculatedFunctionInvocation function)
    {
        object numberValue = function.Arg(0);
        object startValue = function.Arg(1);
        object stopValue = function.Arg(2);
        object intervalValue = function.Arg(3);
        if (IsNull(numberValue) || IsNull(startValue) || IsNull(stopValue) || IsNull(intervalValue))
        {
            return DBNull.Value;
        }

        long number = Convert.ToInt32(AsAccessNumber(numberValue), CultureInfo.InvariantCulture);
        long start = Convert.ToInt32(AsAccessNumber(startValue), CultureInfo.InvariantCulture);
        long stop = Convert.ToInt32(AsAccessNumber(stopValue), CultureInfo.InvariantCulture);
        long interval = Convert.ToInt32(AsAccessNumber(intervalValue), CultureInfo.InvariantCulture);
        if (start < 0 || stop <= start || interval < 1)
        {
            throw new ArgumentException("Partition requires start >= 0, stop > start and interval >= 1.");
        }

        string lower;
        string upper;
        if (number < start)
        {
            lower = string.Empty;
            upper = (start - 1).ToString(CultureInfo.InvariantCulture);
        }
        else if (number > stop)
        {
            lower = (stop + 1).ToString(CultureInfo.InvariantCulture);
            upper = string.Empty;
        }
        else
        {
            long lowerBound = start + ((number - start) / interval * interval);
            long upperBound = Math.Min(stop, lowerBound + interval - 1);
            lower = lowerBound.ToString(CultureInfo.InvariantCulture);
            upper = upperBound.ToString(CultureInfo.InvariantCulture);
        }

        int width = Math.Max((stop + 1).ToString(CultureInfo.InvariantCulture).Length, (start - 1).ToString(CultureInfo.InvariantCulture).Length);
        return lower.PadLeft(width) + ":" + upper.PadLeft(width);
    }

#pragma warning restore CA1859

    private static object EvaluateAbsolute(object value) => value switch
    {
        AccessCurrency currency => AccessCurrency.FromDecimal(Math.Abs(currency.Value)),
        bool boolean => (short)(boolean ? 1 : 0),
        byte number => (short)number,
        short number when number != short.MinValue => Math.Abs(number),
        short number => -number,
        int number when number != int.MinValue => Math.Abs(number),
        int number => -(double)number,
        float number => Math.Abs((float)AccessVariantOperators.Finite(number)),
        double number => Math.Abs(AccessVariantOperators.Finite(number)),
        _ => Math.Abs(ToDecimal(value)),
    };

    private static object EvaluateIntegral(object value, bool truncate)
    {
        if (value is AccessCurrency currency)
        {
            return AccessCurrency.FromDecimal(truncate ? Math.Truncate(currency.Value) : Math.Floor(currency.Value));
        }

        if (value is bool boolean)
        {
            return (short)(boolean ? -1 : 0);
        }

        if (value is byte byteValue)
        {
            return (short)byteValue;
        }

        if (value is short or int)
        {
            return value;
        }

        if (value is double doubleValue)
        {
            doubleValue = AccessVariantOperators.Finite(doubleValue);
            return truncate ? Math.Truncate(doubleValue) : Math.Floor(doubleValue);
        }

        if (value is float singleValue)
        {
            singleValue = (float)AccessVariantOperators.Finite(singleValue);
            return (float)(truncate ? Math.Truncate(singleValue) : Math.Floor(singleValue));
        }

        decimal number = ToDecimal(value);
        return truncate ? Math.Truncate(number) : Math.Floor(number);
    }
}
