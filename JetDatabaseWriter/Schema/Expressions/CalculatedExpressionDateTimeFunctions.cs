namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;

internal static class CalculatedExpressionDateTimeFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "DATE", 0, 0, static _ => CurrentAccessLocalDateTime().Date, "TODAY"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "NOW", 0, 0, static _ => CurrentAccessLocalDateTime()));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "TIME", 0, 0, static _ => CurrentAccessLocalDateTime()));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "DATEVALUE", 1, 1, static function => ParseDate(ToText(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "DATESERIAL", 3, 3, static function => DateSerial(checked((int)ToDecimal(function.Arg(0))), checked((int)ToDecimal(function.Arg(1))), checked((int)ToDecimal(function.Arg(2))))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "DATEADD", 3, 3, static function => DateAdd(ToText(function.Arg(0)), checked((int)ToDecimal(function.Arg(1))), ToDateTime(function.Arg(2)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "DATEDIFF", 3, 5, static function => DateDiff(ToText(function.Arg(0)), ToDateTime(function.Arg(1)), ToDateTime(function.Arg(2)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "DATEPART", 2, 4, static function => DatePart(ToText(function.Arg(0)), ToDateTime(function.Arg(1)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "YEAR", 1, 1, static function => ToDateTime(function.Arg(0)).Year));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "MONTH", 1, 1, static function => ToDateTime(function.Arg(0)).Month));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "DAY", 1, 1, static function => ToDateTime(function.Arg(0)).Day));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "HOUR", 1, 1, static function => ToDateTime(function.Arg(0)).Hour));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "MINUTE", 1, 1, static function => ToDateTime(function.Arg(0)).Minute));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "SECOND", 1, 1, static function => ToDateTime(function.Arg(0)).Second));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "TIMEVALUE", 1, 1, static function => CurrentAccessLocalDateTime().Date + ToDateTime(function.Arg(0)).TimeOfDay));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "TIMESERIAL", 3, 3, static function => EvaluateTimeSerial(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "TIMER", 0, 0, static _ => CurrentAccessLocalDateTime().TimeOfDay.TotalSeconds));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "MONTHNAME", 1, 2, static function => CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(checked((int)ToDecimal(function.Arg(0))))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "WEEKDAY", 1, 2, static function => Weekday(ToDateTime(function.Arg(0)), function.Count > 1 ? checked((int)ToDecimal(function.Arg(1))) : 1)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "WEEKDAYNAME", 1, 3, static function => WeekdayName(checked((int)ToDecimal(function.Arg(0))), function.Count > 1 && ToBoolean(function.Arg(1)), function.Count > 2 ? checked((int)ToDecimal(function.Arg(2))) : 1)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.DateTime, "CDATE", 1, 1, static function => ToDateTime(function.Arg(0)), "CVDATE"));
    }

#pragma warning disable RS0030 // Access DATE/NOW/TIME/TIMER evaluate against the host local clock.
    private static DateTime CurrentAccessLocalDateTime() => DateTime.Now;
#pragma warning restore RS0030

    private static DateTime EvaluateTimeSerial(CalculatedFunctionInvocation function)
        => CurrentAccessLocalDateTime().Date
            .AddHours(ToDouble(function.Arg(0)))
            .AddMinutes(ToDouble(function.Arg(1)))
            .AddSeconds(ToDouble(function.Arg(2)));

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
        var lower = sign > 0 ? start : end;
        var upper = sign > 0 ? end : start;
        var span = upper - lower;
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
}
