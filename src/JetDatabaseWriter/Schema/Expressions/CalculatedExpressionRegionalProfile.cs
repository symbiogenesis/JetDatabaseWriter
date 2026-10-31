namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;

/// <summary>The synchronous calculation's regional settings, separate from shared rule/default coercion.</summary>
internal static class CalculatedExpressionRegionalProfile
{
    [ThreadStatic]
    private static CultureInfo? current;

    internal static CultureInfo Culture => current ?? CultureInfo.InvariantCulture;

    internal static bool IsActive => current is not null;

    internal static string BooleanText(bool value) => Culture.TwoLetterISOLanguageName switch
    {
        "de" => value ? "Wahr" : "Falsch",
        "fr" => value ? "Vrai" : "Faux",
        "tr" => value ? "Doğru" : "Yanlış",
        _ => value ? "True" : "False",
    };

    internal static string NumericText(string text)
    {
        string result = text.Trim();
        bool parenthesized = result.Length >= 2 && result[0] == '(' && result[^1] == ')';
        if (parenthesized)
        {
            result = result[1..^1].Trim();
        }

        NumberFormatInfo number = Culture.NumberFormat;
        if (number.NumberDecimalSeparator != "." && number.NumberGroupSeparator != "." && result.Contains('.', StringComparison.Ordinal))
        {
            throw new FormatException("The number separator does not match the regional settings.");
        }

        if (result.StartsWith(number.CurrencySymbol, StringComparison.Ordinal))
        {
            result = result[number.CurrencySymbol.Length..].TrimStart();
        }
        else if (result.EndsWith(number.CurrencySymbol, StringComparison.Ordinal))
        {
            result = result[..^number.CurrencySymbol.Length].TrimEnd();
        }

        // Automation ignores grouping even after a decimal point; spaces are
        // an additional group spelling only when the locale's group is a space.
        if (number.NumberGroupSeparator.Length != 0)
        {
            result = result.Replace(number.NumberGroupSeparator, string.Empty, StringComparison.Ordinal);
            if (number.NumberGroupSeparator is " " or "\u00A0" or "\u202F")
            {
                result = result.Replace(" ", string.Empty, StringComparison.Ordinal);
            }
        }

        result = result.Replace(number.NumberDecimalSeparator, ".", StringComparison.Ordinal)
            .Replace(number.NegativeSign, "-", StringComparison.Ordinal);
        return parenthesized ? "-" + result : result;
    }

    internal static bool TryNumber(string text, out double number)
    {
        try
        {
            number = CalculatedExpressionCoercion.ToDouble(text);
            return true;
        }
        catch (Exception exception) when (exception is FormatException or InvalidCastException or OverflowException)
        {
            number = 0d;
            return false;
        }
    }

    internal static string DateText(DateTime value)
    {
        DateTimeFormatInfo format = Culture.DateTimeFormat;
        string pattern = format.ShortDatePattern.Replace("yyyy", "yyy", StringComparison.Ordinal);
        if (value.Year < 1000 && !pattern.Contains("yyy", StringComparison.Ordinal))
        {
            pattern = pattern.Replace("yy", "yyy", StringComparison.Ordinal);
        }

        string date = value.ToString(pattern, Culture);
        string time = value.ToString(format.LongTimePattern, Culture);
        if (value.Date == new DateTime(1899, 12, 30))
        {
            return time;
        }

        return value.TimeOfDay == TimeSpan.Zero ? date : date + " " + time;
    }

    internal static DateTime ParseDate(string text)
    {
        const DateTimeStyles styles = DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault;

        // Access accepts slash and hyphen dates across profiles, but a dot date
        // is legal only where the user date separator is itself a dot.
        if (text.Contains('.', StringComparison.Ordinal) && Culture.DateTimeFormat.DateSeparator != "." && !ContainsLetter(text))
        {
            throw new FormatException("The date separator does not match the regional settings.");
        }

        string[] parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 3 && parts[1].EndsWith('月')
            && int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out _)
            && int.TryParse(parts[1].AsSpan(0, parts[1].Length - 1), NumberStyles.None, CultureInfo.InvariantCulture, out _)
            && int.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out _))
        {
            throw new FormatException("The localized month name is not a native Japanese date spelling.");
        }

        if (Culture.TwoLetterISOLanguageName == "en")
        {
            if (text.Contains('年', StringComparison.Ordinal))
            {
                throw new FormatException("Japanese date units are not a native English date spelling.");
            }

            text = text.Replace("Sept.", "Sep", StringComparison.OrdinalIgnoreCase);
        }

        // Native fallback tries yy/M/d before d/M/yy: 31/12/20 is 2031-12-20.
        if (!DateTime.TryParse(text, Culture, styles, out DateTime date)
            && !DateTime.TryParseExact(text, ["d/M/yyyy", "yyyy/M/d", "yyyy-M-d", "M/d/yy", "yy/M/d", "d/M/yy", "d-M-yyyy", "d/M/yyyy H:mm"], Culture, styles, out date))
        {
            throw new FormatException("The text is not a valid regional date.");
        }

        return date.Date == DateTime.MinValue.Date ? new DateTime(1899, 12, 30) + date.TimeOfDay : date;
    }

    private static bool ContainsLetter(string text)
    {
        foreach (char character in text)
        {
            if (char.IsLetter(character))
            {
                return true;
            }
        }

        return false;
    }

    internal static CultureInfo? Exchange(CultureInfo? culture)
    {
        CultureInfo? previous = current;
        current = culture;
        return previous;
    }

    internal static CultureInfo Snapshot(CultureInfo culture)
    {
        var copy = (CultureInfo)culture.Clone();

        // Table calculations use Gregorian OLE dates even under an Islamic user calendar.
        string shortDatePattern = copy.DateTimeFormat.ShortDatePattern;
        string longTimePattern = copy.DateTimeFormat.LongTimePattern;
        string amDesignator = copy.DateTimeFormat.AMDesignator;
        string pmDesignator = copy.DateTimeFormat.PMDesignator;
        copy.DateTimeFormat.Calendar = copy.DateTimeFormat.Calendar is GregorianCalendar gregorian
            ? (GregorianCalendar)gregorian.Clone()
            : new GregorianCalendar();
        copy.DateTimeFormat.ShortDatePattern = shortDatePattern;
        copy.DateTimeFormat.LongTimePattern = longTimePattern;
        copy.DateTimeFormat.AMDesignator = amDesignator;
        copy.DateTimeFormat.PMDesignator = pmDesignator;
        copy.DateTimeFormat.DateSeparator = copy.DateTimeFormat.DateSeparator
            .Replace("\u200F", string.Empty, StringComparison.Ordinal)
            .Replace("\u200E", string.Empty, StringComparison.Ordinal);
        string[] months = copy.DateTimeFormat.AbbreviatedMonthNames;
        for (int month = 0; month < 12; month++)
        {
            months[month] = copy.TwoLetterISOLanguageName == "ja"
                ? (month + 1).ToString(CultureInfo.InvariantCulture)
                : months[month].TrimEnd('.');
        }

        if (copy.TwoLetterISOLanguageName == "de")
        {
            months[2] = "Mrz";
            string[] days = copy.DateTimeFormat.AbbreviatedDayNames;
            for (int day = 0; day < days.Length; day++)
            {
                days[day] = days[day].TrimEnd('.');
            }

            copy.DateTimeFormat.AbbreviatedDayNames = days;
        }

        copy.DateTimeFormat.AbbreviatedMonthNames = months;
        copy.DateTimeFormat.AbbreviatedMonthGenitiveNames = months;
        if (copy.TwoLetterISOLanguageName == "ar")
        {
            string[] days = copy.DateTimeFormat.DayNames;
            string[] abbreviated = copy.DateTimeFormat.AbbreviatedDayNames;
            days[1] = "الإثنين";
            abbreviated[1] = "الإثنين";
            copy.DateTimeFormat.DayNames = days;
            copy.DateTimeFormat.AbbreviatedDayNames = abbreviated;
        }

        return CultureInfo.ReadOnly(copy);
    }
}
