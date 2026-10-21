namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>Renders VBA date tokens without exposing them to .NET's different format grammar.</summary>
internal static class CalculatedExpressionDateFormatter
{
    private static readonly string[] Tokens = ["AM/PM", "AMPM", "A/P", "dddddd", "ddddd", "dddd", "ddd", "dd", "d", "mmmm", "mmm", "mm", "m", "yyyy", "yy", "y", "ww", "w", "q", "hh", "h", "nn", "n", "ss", "s", "ttttt", "c", "/", ":"];

    /// <summary>Formats a date using VBA's Gregorian date and time tokens.</summary>
    /// <param name="value">The date value.</param>
    /// <param name="format">The custom format.</param>
    /// <param name="firstDay">VBA's first-day-of-week setting.</param>
    /// <param name="firstWeek">VBA's first-week-of-year setting.</param>
    /// <returns>The formatted date.</returns>
    /// <exception cref="ArgumentException">A week setting is outside its supported range.</exception>
    internal static string Format(DateTime value, string format, int firstDay, int firstWeek)
    {
        DayOfWeek day = firstDay switch
        {
            0 => CultureInfo.InvariantCulture.DateTimeFormat.FirstDayOfWeek,
            >= 1 and <= 7 => (DayOfWeek)(firstDay - 1),
            _ => throw new ArgumentException("Format first day of week must be between 0 and 7."),
        };
        CalendarWeekRule week = firstWeek switch
        {
            0 => CultureInfo.InvariantCulture.DateTimeFormat.CalendarWeekRule,
            1 => CalendarWeekRule.FirstDay,
            2 => CalendarWeekRule.FirstFourDayWeek,
            3 => CalendarWeekRule.FirstFullWeek,
            _ => throw new ArgumentException("Format first week of year must be between 0 and 3."),
        };
        List<(string Text, bool Literal)> parts = Tokenize(format);
        bool twelveHour = parts.Exists(static part => !part.Literal && (part.Text.Equals("AM/PM", StringComparison.OrdinalIgnoreCase) || part.Text.Equals("A/P", StringComparison.OrdinalIgnoreCase) || part.Text.Equals("AMPM", StringComparison.OrdinalIgnoreCase)));
        var result = new StringBuilder();
        string previous = string.Empty;
        for (int index = 0; index < parts.Count; index++)
        {
            (string text, bool literal) = parts[index];
            if (literal)
            {
                result.Append(text);
                continue;
            }

            string token = text.ToUpperInvariant();
            bool minute = previous is "H" or "HH";
            int hour = twelveHour ? ((value.Hour + 11) % 12) + 1 : value.Hour;
            string rendered = token switch
            {
                "D" => Number(value.Day),
                "DD" => Number(value.Day, "00"),
                "DDD" => CultureInfo.InvariantCulture.DateTimeFormat.GetAbbreviatedDayName(value.DayOfWeek),
                "DDDD" => CultureInfo.InvariantCulture.DateTimeFormat.GetDayName(value.DayOfWeek),
                "DDDDD" => value.ToString("M/d/yyyy", CultureInfo.InvariantCulture),
                "DDDDDD" => value.ToString("dddd, MMMM d, yyyy", CultureInfo.InvariantCulture),
                "M" => Number(minute ? value.Minute : value.Month),
                "MM" => Number(minute ? value.Minute : value.Month, "00"),
                "MMM" => CultureInfo.InvariantCulture.DateTimeFormat.GetAbbreviatedMonthName(value.Month),
                "MMMM" => CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(value.Month),
                "Y" => Number(value.DayOfYear),
                "YY" => Number(value.Year % 100, "00"),
                "YYYY" => Number(value.Year, "0000"),
                "W" => Number((((int)value.DayOfWeek - (int)day + 7) % 7) + 1),
                "WW" => Number(CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(value, week, day)),
                "Q" => Number(((value.Month - 1) / 3) + 1),
                "H" => Number(hour),
                "HH" => Number(hour, "00"),
                "N" => Number(value.Minute),
                "NN" => Number(value.Minute, "00"),
                "S" => Number(value.Second),
                "SS" => Number(value.Second, "00"),
                "TTTTT" => value.ToString("h:mm:ss tt", CultureInfo.InvariantCulture),
                "C" => ToGeneralDateText(value),
                "AMPM" => value.Hour < 12 ? CultureInfo.InvariantCulture.DateTimeFormat.AMDesignator : CultureInfo.InvariantCulture.DateTimeFormat.PMDesignator,
                "AM/PM" => text.Substring(value.Hour < 12 ? 0 : 3, 2),
                "A/P" => text.Substring(value.Hour < 12 ? 0 : 2, 1),
                "/" => CultureInfo.InvariantCulture.DateTimeFormat.DateSeparator,
                ":" => CultureInfo.InvariantCulture.DateTimeFormat.TimeSeparator,
                _ => text,
            };
            result.Append(rendered);
            if (token is not "/" and not ":")
            {
                previous = token;
            }
        }

        return result.ToString();
    }

    private static string Number(int value, string format = "0") => value.ToString(format, CultureInfo.InvariantCulture);

    private static List<(string Text, bool Literal)> Tokenize(string format)
    {
        var parts = new List<(string Text, bool Literal)>();
        for (int index = 0; index < format.Length;)
        {
            char character = format[index];
            if (character == '\\')
            {
                index++;
                if (index < format.Length)
                {
                    parts.Add((format[index++].ToString(), true));
                }

                continue;
            }

            if (character == '"')
            {
                int start = ++index;
                while (index < format.Length && format[index] != '"')
                {
                    index++;
                }

                parts.Add((format[start..index], true));
                if (index < format.Length)
                {
                    index++;
                }

                continue;
            }

            string? matched = null;
            foreach (string token in Tokens)
            {
                if (index + token.Length <= format.Length && string.Compare(format, index, token, 0, token.Length, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    matched = format.Substring(index, token.Length);
                    break;
                }
            }

            parts.Add((matched ?? character.ToString(), matched is null));
            index += matched?.Length ?? 1;
        }

        return parts;
    }
}
