namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static class CalculatedExpressionFormattingFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Formatting, "FORMAT", 1, 4, static function => function.Count == 1 ? ToText(function.Arg(0)) : FormatValue(function.Arg(0), ToText(function.Arg(1)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Formatting, "FORMATNUMBER", 1, 6, static function => FormatNumber(ToDecimal(function.Arg(0)), function.Count > 1 ? checked((int)ToDecimal(function.Arg(1))) : 2, percent: false, currency: false)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Formatting, "FORMATPERCENT", 1, 6, static function => FormatNumber(ToDecimal(function.Arg(0)), function.Count > 1 ? checked((int)ToDecimal(function.Arg(1))) : 2, percent: true, currency: false)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Formatting, "FORMATCURRENCY", 1, 6, static function => FormatNumber(ToDecimal(function.Arg(0)), function.Count > 1 ? checked((int)ToDecimal(function.Arg(1))) : 2, percent: false, currency: true)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Formatting, "FORMATDATETIME", 1, 2, static function => FormatDateTime(ToDateTime(function.Arg(0)), function.Count > 1 ? checked((int)ToDecimal(function.Arg(1))) : 0)));
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
        int safeDecimalDigits = Math.Clamp(decimalDigits, 0, MaxFormatDecimalDigits);
        return prefix + displayValue.ToString("N" + safeDecimalDigits.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture) + suffix;
    }

    private static string FormatDateTime(DateTime value, int formatType) => formatType switch
    {
        0 => value.ToString("G", CultureInfo.InvariantCulture),
        1 => value.ToString("D", CultureInfo.InvariantCulture),
        2 => value.ToString("d", CultureInfo.InvariantCulture),
        3 => value.ToString("T", CultureInfo.InvariantCulture),
        4 => value.ToString("t", CultureInfo.InvariantCulture),
        _ => throw new ArgumentException($"Calculated-column FormatDateTime type '{formatType}' is not valid."),
    };
}
