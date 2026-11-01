namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;

/// <summary>Captured Access defaults, independent of the runtime globalization provider.</summary>
/// <remarks>The six exact profiles use independent native DAO120 source/compacted caches
/// and observations.json under tests/JetDatabaseWriter.Tests/Databases/CalculatedRegionalProfiles.
/// These controls do not qualify other locales.</remarks>
internal static class CalculatedNativeRegionalDefaults
{
    private static readonly DateTimeFormatInfo English = Create(
        "M/d/yyyy",
        "h:mm:ss tt",
        "/",
        "AM",
        "PM",
        DayOfWeek.Sunday,
        ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", string.Empty],
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", string.Empty],
        ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
        ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]);

    private static readonly DateTimeFormatInfo French = Create(
        "dd/MM/yyyy",
        "HH:mm:ss",
        "/",
        string.Empty,
        string.Empty,
        DayOfWeek.Monday,
        ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre", string.Empty],
        ["janv", "févr", "mars", "avr", "mai", "juin", "juil", "août", "sept", "oct", "nov", "déc", string.Empty],
        ["dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"],
        ["dim.", "lun.", "mar.", "mer.", "jeu.", "ven.", "sam."]);

    private static readonly DateTimeFormatInfo German = Create(
        "dd.MM.yyyy",
        "HH:mm:ss",
        ".",
        string.Empty,
        string.Empty,
        DayOfWeek.Monday,
        ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember", string.Empty],
        ["Jan", "Feb", "Mrz", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez", string.Empty],
        ["Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"],
        ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"]);

    private static readonly DateTimeFormatInfo Turkish = Create(
        "d.MM.yyyy",
        "HH:mm:ss",
        ".",
        "ÖÖ",
        "ÖS",
        DayOfWeek.Monday,
        ["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık", string.Empty],
        ["Oca", "Şub", "Mar", "Nis", "May", "Haz", "Tem", "Ağu", "Eyl", "Eki", "Kas", "Ara", string.Empty],
        ["Pazar", "Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi"],
        ["Paz", "Pzt", "Sal", "Çar", "Per", "Cum", "Cmt"]);

    private static readonly DateTimeFormatInfo Japanese = Create(
        "yyyy/MM/dd",
        "H:mm:ss",
        "/",
        "午前",
        "午後",
        DayOfWeek.Sunday,
        ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月", string.Empty],
        ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", string.Empty],
        ["日曜日", "月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日"],
        ["日", "月", "火", "水", "木", "金", "土"]);

    private static readonly DateTimeFormatInfo Arabic = Create(
        "dd/MM/yy",
        "hh:mm:ss tt",
        "/",
        "ص",
        "م",
        DayOfWeek.Sunday,
        ["يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو", "يوليو", "أغسطس", "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر", string.Empty],
        ["يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو", "يوليو", "أغسطس", "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر", string.Empty],
        ["الأحد", "الإثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت"],
        ["الأحد", "الإثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت"]);

    internal static bool Apply(string name, DateTimeFormatInfo target, DateTimeFormatInfo requested, DateTimeFormatInfo platform)
    {
        DateTimeFormatInfo? native = name switch
        {
            "en-US" => English,
            "fr-FR" => French,
            "de-DE" => German,
            "tr-TR" => Turkish,
            "ja-JP" => Japanese,
            "ar-SA" => Arabic,
            _ => null,
        };
        if (native is null)
        {
            return false;
        }

        // CultureInfo has no override-intent metadata. Observable differences
        // from the host defaults are retained; equal values use the captured
        // native default. Calendar conversion must not discard caller fields.
        target.ShortDatePattern = Select(requested.ShortDatePattern, platform.ShortDatePattern, native.ShortDatePattern);
        target.LongTimePattern = Select(requested.LongTimePattern, platform.LongTimePattern, native.LongTimePattern);
        target.DateSeparator = Select(requested.DateSeparator, platform.DateSeparator, native.DateSeparator);
        target.TimeSeparator = Select(requested.TimeSeparator, platform.TimeSeparator, native.TimeSeparator);
        target.AMDesignator = Select(requested.AMDesignator, platform.AMDesignator, native.AMDesignator);
        target.PMDesignator = Select(requested.PMDesignator, platform.PMDesignator, native.PMDesignator);
        target.FirstDayOfWeek = requested.FirstDayOfWeek == platform.FirstDayOfWeek ? native.FirstDayOfWeek : requested.FirstDayOfWeek;
        target.MonthNames = SelectNames(requested.MonthNames, platform.MonthNames, native.MonthNames);
        target.MonthGenitiveNames = SelectNames(requested.MonthGenitiveNames, platform.MonthGenitiveNames, native.MonthGenitiveNames);
        target.AbbreviatedMonthNames = SelectNames(requested.AbbreviatedMonthNames, platform.AbbreviatedMonthNames, native.AbbreviatedMonthNames);
        target.AbbreviatedMonthGenitiveNames = SelectNames(requested.AbbreviatedMonthGenitiveNames, platform.AbbreviatedMonthGenitiveNames, native.AbbreviatedMonthGenitiveNames);
        target.DayNames = SelectNames(requested.DayNames, platform.DayNames, native.DayNames);
        target.AbbreviatedDayNames = SelectNames(requested.AbbreviatedDayNames, platform.AbbreviatedDayNames, native.AbbreviatedDayNames);
        return true;
    }

    private static string Select(string requested, string platform, string native)
        => string.Equals(requested, platform, StringComparison.Ordinal) ? native : requested;

    private static string[] SelectNames(string[] requested, string[] platform, string[] native)
    {
        for (int index = 0; index < requested.Length; index++)
        {
            requested[index] = Select(requested[index], platform[index], native[index]);
        }

        return requested;
    }

    private static DateTimeFormatInfo Create(string date, string time, string separator, string am, string pm, DayOfWeek firstDay, string[] months, string[] abbreviatedMonths, string[] days, string[] abbreviatedDays)
        => DateTimeFormatInfo.ReadOnly(new DateTimeFormatInfo
        {
            ShortDatePattern = date,
            LongTimePattern = time,
            DateSeparator = separator,
            TimeSeparator = ":",
            AMDesignator = am,
            PMDesignator = pm,
            FirstDayOfWeek = firstDay,
            MonthNames = months,
            MonthGenitiveNames = months,
            AbbreviatedMonthNames = abbreviatedMonths,
            AbbreviatedMonthGenitiveNames = abbreviatedMonths,
            DayNames = days,
            AbbreviatedDayNames = abbreviatedDays,
        });
}
