namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;

internal static class CalculatedExpressionDateTimeFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, "DATE", 0, 0, static _ => CurrentAccessLocalDateTime().Date, "TODAY");
        AddFunction(functions, "NOW", 0, 0, static _ => CurrentAccessLocalDateTime());
        AddFunction(functions, "TIME", 0, 0, static _ => TimeOnDayZero(CurrentAccessLocalDateTime()));
        AddFunction(functions, "DATEVALUE", 1, 1, static function => (function.Arg(0) is DateTime date ? date : ParseDate(ToText(function.Arg(0)))).Date);
        AddFunction(functions, "DATESERIAL", 3, 3, static function => EvaluateDateFunction(function, DateFunction.DateSerial));
        AddFunction(functions, "DATEADD", 3, 3, static function => DateAdd(ToText(function.Arg(0)), checked((int)ToDecimal(function.Arg(1))), ToDateTime(function.Arg(2))));
        AddFunction(functions, "DATEDIFF", 3, 5, static function => DateDiff(ToText(function.Arg(0)), ToDateTime(function.Arg(1)), ToDateTime(function.Arg(2))));
        AddFunction(functions, "DATEPART", 2, 4, static function => DatePart(ToText(function.Arg(0)), ToDateTime(function.Arg(1))));
        AddFunction(functions, "YEAR", 1, 1, static function => EvaluateDateFunction(function, DateFunction.Year));
        AddFunction(functions, "MONTH", 1, 1, static function => EvaluateDateFunction(function, DateFunction.Month));
        AddFunction(functions, "DAY", 1, 1, static function => EvaluateDateFunction(function, DateFunction.Day));
        AddFunction(functions, "HOUR", 1, 1, static function => EvaluateDateFunction(function, DateFunction.Hour));
        AddFunction(functions, "MINUTE", 1, 1, static function => EvaluateDateFunction(function, DateFunction.Minute));
        AddFunction(functions, "SECOND", 1, 1, static function => EvaluateDateFunction(function, DateFunction.Second));
        AddFunction(functions, "TIMEVALUE", 1, 1, static function => TimeOnDayZero(ToDateTime(function.Arg(0))));
        AddFunction(functions, "TIMESERIAL", 3, 3, static function => EvaluateDateFunction(function, DateFunction.TimeSerial));
        AddFunction(functions, "TIMER", 0, 0, static _ => TimerSeconds(CurrentLocalDateTime()));
        AddFunction(functions, "MONTHNAME", 1, 2, static function => EvaluateDateFunction(function, DateFunction.MonthName));
        AddFunction(functions, "WEEKDAY", 1, 2, static function => EvaluateDateFunction(function, DateFunction.Weekday));
        AddFunction(functions, "WEEKDAYNAME", 1, 3, static function => EvaluateDateFunction(function, DateFunction.WeekdayName));
        AddFunction(functions, "CDATE", 1, 1, static function => function.Context.IsCalculatingColumn ? ToNativeDate(function.Arg(0)) : ToDateTime(function.Arg(0)), "CVDATE");
    }

#pragma warning disable RS0030 // Access DATE/NOW/TIME/TIMER evaluate against the host local clock.
    private static DateTime CurrentLocalDateTime() => DateTime.Now;

    private static DateTime CurrentAccessLocalDateTime() => TruncateToWholeSeconds(CurrentLocalDateTime());
#pragma warning restore RS0030

    /// <summary>Removes fractional seconds from an Access clock date.</summary>
    /// <param name="value">The local clock reading.</param>
    /// <returns>The reading truncated to the preceding whole second.</returns>
    internal static DateTime TruncateToWholeSeconds(DateTime value)
        => value.AddTicks(-(value.Ticks % TimeSpan.TicksPerSecond));

    /// <summary>Returns Timer's fractional seconds since local midnight.</summary>
    /// <param name="value">The untruncated local clock reading.</param>
    /// <returns>The elapsed seconds, including the fractional part.</returns>
    internal static double TimerSeconds(DateTime value) => value.TimeOfDay.TotalSeconds;

    /// <summary>
    /// VBA's TimeSerial, as measured with VBScript. Each argument rounds half to even
    /// to an Integer, and the total seconds become an OLE date serial, which is a time
    /// on day 0, 1899-12-30. So 25 hours is 12/31/1899 1:00 AM. A negative total keeps
    /// OLE's reading of a negative serial: -1 hour (-0.0417) is 1:00 AM on day 0.
    /// </summary>
    /// <param name="function">The call, with the hours, minutes and seconds.</param>
    /// <returns>The time.</returns>
    /// <exception cref="OverflowException">An argument is outside the Integer range.</exception>
    private static object EvaluateTimeSerial(CalculatedFunctionInvocation function)
    {
        int seconds = (ToVbaInteger(DateArgument(function, 0)) * 3600) + (ToVbaInteger(DateArgument(function, 1)) * 60) + ToVbaInteger(DateArgument(function, 2));
        double serial = seconds / 86400d;
        return function.Context.IsCalculatingColumn ? NativeDate.FromSerial(serial) : FromOleDate(serial);
    }

    private static object EvaluateDateFunction(CalculatedFunctionInvocation function, DateFunction operation)
    {
        try
        {
            return operation switch
            {
                DateFunction.DateSerial => EvaluateDateSerial(function),
                DateFunction.TimeSerial => EvaluateTimeSerial(function),
                DateFunction.MonthName => EvaluateMonthName(function),
                DateFunction.WeekdayName => EvaluateWeekdayName(function),
                DateFunction.Year or DateFunction.Month or DateFunction.Day or DateFunction.Hour or DateFunction.Minute or DateFunction.Second or DateFunction.Weekday => EvaluateDatePart(function, operation),
                _ => throw new ArgumentOutOfRangeException(nameof(operation)),
            };
        }
        catch (Exception exception) when (function.Context.IsCalculatingColumn && exception is ArgumentException or OverflowException or InvalidCastException or FormatException)
        {
            return new CalculatedError(exception switch { OverflowException => 6, ArgumentException => 5, _ => 13 });
        }
    }

    private static object EvaluateDateSerial(CalculatedFunctionInvocation function)
    {
        DateTime date = DateSerial(ToVbaInteger(DateArgument(function, 0)), ToVbaInteger(DateArgument(function, 1)), ToVbaInteger(DateArgument(function, 2)));
        return function.Context.IsCalculatingColumn ? NativeDate.FromDateTime(date) : date;
    }

    private static object EvaluateDatePart(CalculatedFunctionInvocation function, DateFunction operation)
    {
        object value = function.Arg(0);
        int firstDay = operation == DateFunction.Weekday && function.Count > 1 ? DateIntegerArgument(function, 1) : 1;
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        DateTime date;
        try
        {
            double serial = value switch
            {
                NativeDate native => native.Serial,
                DateTime dateTime => dateTime.ToOADate(),
                string text => ParseDate(text).ToOADate(),
                _ => ToDouble(value),
            };
            _ = FromOleDate(serial);
            double day = Math.Truncate(serial);
            double seconds = Math.Floor((Math.Abs(serial - day) * 86400d) + 0.5d);
            date = FromOleDate(day).AddSeconds(seconds);
        }
        catch (Exception exception) when (exception is ArgumentException or OverflowException or FormatException)
        {
            throw new InvalidCastException("The date function argument is not a valid date.", exception);
        }

        int result = operation switch
        {
            DateFunction.Year => date.Year,
            DateFunction.Month => date.Month,
            DateFunction.Day => date.Day,
            DateFunction.Hour => date.Hour,
            DateFunction.Minute => date.Minute,
            DateFunction.Second => date.Second,
            DateFunction.Weekday => Weekday(date, firstDay),
            DateFunction.DateSerial or DateFunction.TimeSerial or DateFunction.MonthName or DateFunction.WeekdayName => throw new ArgumentOutOfRangeException(nameof(operation)),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };
        return function.Context.IsCalculatingColumn ? (object)checked((short)result) : result;
    }

    private static string EvaluateMonthName(CalculatedFunctionInvocation function)
    {
        int month = DateIntegerArgument(function, 0);
        bool abbreviate = function.Count > 1 && DateBooleanArgument(function, 1);
        if (month is < 1 or > 12)
        {
            throw new ArgumentOutOfRangeException(nameof(function), "The month must be between 1 and 12.");
        }

        return abbreviate ? CultureInfo.InvariantCulture.DateTimeFormat.GetAbbreviatedMonthName(month) : CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(month);
    }

    private static string EvaluateWeekdayName(CalculatedFunctionInvocation function)
    {
        int day = DateIntegerArgument(function, 0);
        bool abbreviate = function.Count > 1 && DateBooleanArgument(function, 1);
        int firstDay = function.Count > 2 ? DateIntegerArgument(function, 2) : 0;
        return WeekdayName(day, abbreviate, firstDay);
    }

    private static int DateIntegerArgument(CalculatedFunctionInvocation function, int index)
        => AccessVariantOperators.ToRoundedLong(DateArgument(function, index));

    private static bool DateBooleanArgument(CalculatedFunctionInvocation function, int index)
    {
        object value = DateArgument(function, index);
        return value is string text && bool.TryParse(text, out bool boolean) ? boolean : ToDouble(value) != 0d;
    }

    private static object DateArgument(CalculatedFunctionInvocation function, int index)
    {
        object value = function.Arg(index);
        return IsNull(value) ? throw new InvalidCastException("The date function argument cannot be Null.") : value;
    }

    /// <summary>
    /// Rounds an argument half to even to a VBA Integer (-32,768 to 32,767), as VBA
    /// does for a function's Integer parameter.
    /// </summary>
    /// <param name="value">The argument.</param>
    /// <returns>The rounded value.</returns>
    /// <exception cref="OverflowException">The rounded value is outside the Integer range.</exception>
    private static int ToVbaInteger(object value)
    {
        int rounded = AccessVariantOperators.ToRoundedLong(value);
        return rounded is < short.MinValue or > short.MaxValue
            ? throw new OverflowException($"{rounded.ToString(CultureInfo.InvariantCulture)} is outside the range of an Integer.")
            : rounded;
    }

    private static DateTime DateSerial(int year, int month, int day)
    {
        if (year < 100)
        {
            int maximum = new GregorianCalendar().TwoDigitYearMax;
            int century = maximum - (maximum % 100);
            year += century - (year > maximum % 100 ? 100 : 0);
        }

        DateTime result = new DateTime(year, 1, 1).AddMonths(month - 1).AddDays(day - 1);
        return result.Year < 100 ? throw new ArgumentOutOfRangeException(nameof(year), "The date must be in year 100 or later.") : result;
    }

    private static DateTime DateAdd(string interval, int value, DateTime dateTime) => interval.Trim().ToUpperInvariant() switch
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

    private static int DatePart(string interval, DateTime dateTime) => interval.Trim().ToUpperInvariant() switch
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
        firstDay = FirstDay(firstDay);
        int sundayBased = ((int)dateTime.DayOfWeek) + 1;
        return ((sundayBased - 1 - (firstDay - 1) + 7) % 7) + 1;
    }

    private static string WeekdayName(int weekday, bool abbreviate, int firstDay)
    {
        firstDay = FirstDay(firstDay);
        if (weekday is < 1 or > 7)
        {
            throw new ArgumentOutOfRangeException(nameof(weekday), "The weekday must be between 1 and 7.");
        }

        int sundayBased = (firstDay - 1 + (weekday - 1)) % 7;
        string[] names = abbreviate
            ? CultureInfo.InvariantCulture.DateTimeFormat.AbbreviatedDayNames
            : CultureInfo.InvariantCulture.DateTimeFormat.DayNames;
        return names[sundayBased];
    }

    private static int FirstDay(int value)
    {
        if (value is < 0 or > 7)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "The first day must be between 0 and 7.");
        }

        return value == 0 ? 1 : value;
    }

    private enum DateFunction
    {
        Year = 0,
        Month = 1,
        Day = 2,
        Hour = 3,
        Minute = 4,
        Second = 5,
        Weekday = 6,
        DateSerial = 7,
        TimeSerial = 8,
        MonthName = 9,
        WeekdayName = 10,
    }
}
