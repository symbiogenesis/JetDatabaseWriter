namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using JetDatabaseWriter.Schema.Expressions;
using Xunit;

/// <summary>Native regional defaults remain stable when runtime globalization tables change.</summary>
public sealed class CalculatedRegionalPortabilityTests
{
    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("fr-FR")]
    [InlineData("tr-TR")]
    [InlineData("ja-JP")]
    [InlineData("ar-SA")]
    public async Task Snapshot_UsesNativeDefaultsInsteadOfPlatformTables(string name)
    {
        // Inject the runtime-default boundary: these represent ICU differences,
        // rather than explicit user overrides relative to those defaults.
        CultureInfo platform = PlatformCulture(name);
        CultureInfo actual = CalculatedExpressionRegionalProfile.SnapshotWithPlatformDefaults(platform, platform);
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedRegionalProfiles", name, "observations.json");
        using var report = JsonDocument.Parse(await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken));
        JsonElement metadata = report.RootElement.GetProperty("RuntimeCultureMetadata");
        Assert.Equal(metadata.GetProperty("ShortDatePattern").GetString(), actual.DateTimeFormat.ShortDatePattern);
        Assert.Equal(metadata.GetProperty("LongTimePattern").GetString(), actual.DateTimeFormat.LongTimePattern);
        Assert.Equal(metadata.GetProperty("AMDesignator").GetString(), actual.DateTimeFormat.AMDesignator);
        Assert.Equal(metadata.GetProperty("PMDesignator").GetString(), actual.DateTimeFormat.PMDesignator);
        Assert.Equal(Enum.Parse<DayOfWeek>(metadata.GetProperty("FirstDayOfWeek").GetString()!), actual.DateTimeFormat.FirstDayOfWeek);
        JsonElement months = report.RootElement.GetProperty("ExtendedTables").EnumerateArray().Single(table => table.GetProperty("Table").GetString() == "MonthNames").GetProperty("Observations");
        JsonElement days = report.RootElement.GetProperty("ExtendedTables").EnumerateArray().Single(table => table.GetProperty("Table").GetString() == "WeekNames").GetProperty("Observations");
        for (int month = 1; month <= 12; month++)
        {
            Assert.Equal(Observation(months, "Full" + month), actual.DateTimeFormat.GetMonthName(month));
            Assert.Equal(Observation(months, "Abbreviated" + month), actual.DateTimeFormat.GetAbbreviatedMonthName(month));
        }

        for (int day = 0; day < 7; day++)
        {
            Assert.Equal(Observation(days, "FullSunday" + (day + 1)), actual.DateTimeFormat.GetDayName((DayOfWeek)day));
            Assert.Equal(Observation(days, "AbbreviatedSunday" + (day + 1)), actual.DateTimeFormat.GetAbbreviatedDayName((DayOfWeek)day));
        }

        CultureInfo? previous = CalculatedExpressionRegionalProfile.Exchange(actual);
        try
        {
            string? expected = Observation(report.RootElement.GetProperty("Observations"), "DateText");
            Assert.Equal(expected, CalculatedExpressionRegionalProfile.DateText(new DateTime(2020, 3, 4, 13, 45, 0)));
        }
        finally
        {
            CalculatedExpressionRegionalProfile.Exchange(previous);
        }

        Assert.True(actual.IsReadOnly);
        Assert.Equal("yyyy-MM-dd", platform.DateTimeFormat.ShortDatePattern);
    }

    [Fact]
    public void Snapshot_PreservesExplicitOverridesIndependentlyOfPlatformDefaults()
    {
        CultureInfo platform = PlatformCulture("en-US");
        var supplied = (CultureInfo)platform.Clone();
        supplied.DateTimeFormat.ShortDatePattern = "dd!MM!yyyy";
        supplied.DateTimeFormat.LongTimePattern = "HH~mm~ss";
        supplied.DateTimeFormat.DateSeparator = "!";
        supplied.DateTimeFormat.TimeSeparator = "~";
        supplied.DateTimeFormat.MonthNames = Enumerable.Repeat("Caller month", 13).ToArray();
        supplied.DateTimeFormat.AbbreviatedDayNames = Enumerable.Repeat("Caller day", 7).ToArray();
        supplied.DateTimeFormat.FirstDayOfWeek = DayOfWeek.Thursday;
        supplied.DateTimeFormat.AMDesignator = "morning";
        supplied.DateTimeFormat.Calendar.TwoDigitYearMax = 2029;
        CultureInfo actual = CalculatedExpressionRegionalProfile.SnapshotWithPlatformDefaults(supplied, platform);
        Assert.Equal("dd!MM!yyyy", actual.DateTimeFormat.ShortDatePattern);
        Assert.Equal("HH~mm~ss", actual.DateTimeFormat.LongTimePattern);
        Assert.Equal("!", actual.DateTimeFormat.DateSeparator);
        Assert.Equal("~", actual.DateTimeFormat.TimeSeparator);
        Assert.Equal("Caller month", actual.DateTimeFormat.GetMonthName(3));
        Assert.Equal("Caller day", actual.DateTimeFormat.GetAbbreviatedDayName(DayOfWeek.Monday));
        Assert.Equal(DayOfWeek.Thursday, actual.DateTimeFormat.FirstDayOfWeek);
        Assert.Equal("morning", actual.DateTimeFormat.AMDesignator);
        Assert.Equal(2029, actual.DateTimeFormat.Calendar.TwoDigitYearMax);
    }

    [Theory]
    [InlineData("")]
    [InlineData("en-GB")]
    public void Snapshot_RetainsUncapturedCulturePatterns(string name)
    {
        var supplied = new CultureInfo(name, useUserOverride: false);
        supplied.DateTimeFormat.ShortDatePattern = "yyyy!MM!dd";
        supplied.DateTimeFormat.LongTimePattern = "HH~mm~ss";
        supplied.DateTimeFormat.DateSeparator = "!";
        supplied.DateTimeFormat.TimeSeparator = "~";
        CultureInfo actual = CalculatedExpressionRegionalProfile.Snapshot(supplied);
        Assert.Equal(name, actual.Name);
        Assert.Equal("yyyy!MM!dd", actual.DateTimeFormat.ShortDatePattern);
        Assert.Equal("HH~mm~ss", actual.DateTimeFormat.LongTimePattern);
        Assert.Equal("!", actual.DateTimeFormat.DateSeparator);
        Assert.Equal("~", actual.DateTimeFormat.TimeSeparator);
    }

    [Fact]
    public void Snapshot_PreservesPartialNameAndGenitiveOverridesAcrossCalendarReplacement()
    {
        CultureInfo platform = PlatformCulture("ar-SA");
        var supplied = (CultureInfo)platform.Clone();
        string[] names = supplied.DateTimeFormat.MonthNames;
        names[2] = "Caller March";
        supplied.DateTimeFormat.MonthNames = names;
        names = supplied.DateTimeFormat.MonthGenitiveNames;
        names[2] = "Caller genitive March";
        supplied.DateTimeFormat.MonthGenitiveNames = names;
        names = supplied.DateTimeFormat.AbbreviatedMonthNames;
        names[2] = "Caller Mar";
        supplied.DateTimeFormat.AbbreviatedMonthNames = names;
        names = supplied.DateTimeFormat.AbbreviatedMonthGenitiveNames;
        names[2] = "Caller genitive Mar";
        supplied.DateTimeFormat.AbbreviatedMonthGenitiveNames = names;
        CultureInfo actual = CalculatedExpressionRegionalProfile.SnapshotWithPlatformDefaults(supplied, platform);
        Assert.IsType<GregorianCalendar>(actual.DateTimeFormat.Calendar);
        Assert.Equal("Caller March", actual.DateTimeFormat.GetMonthName(3));
        Assert.Equal("Caller genitive March", actual.DateTimeFormat.MonthGenitiveNames[2]);
        Assert.Equal("Caller Mar", actual.DateTimeFormat.GetAbbreviatedMonthName(3));
        Assert.Equal("Caller genitive Mar", actual.DateTimeFormat.AbbreviatedMonthGenitiveNames[2]);
        Assert.Equal("يناير", actual.DateTimeFormat.GetMonthName(1));
    }

    private static CultureInfo PlatformCulture(string name)
    {
        var platform = new CultureInfo(name, useUserOverride: false);
        platform.DateTimeFormat.ShortDatePattern = "yyyy-MM-dd";
        platform.DateTimeFormat.LongTimePattern = "h:mm:ss\u202ftt";
        platform.DateTimeFormat.DateSeparator = " ";
        platform.DateTimeFormat.TimeSeparator = ".";
        platform.DateTimeFormat.MonthNames = Enumerable.Range(1, 13).Select(month => "Host month " + month).ToArray();
        platform.DateTimeFormat.AbbreviatedMonthNames = Enumerable.Range(1, 13).Select(month => "Host abbreviated month " + month).ToArray();
        platform.DateTimeFormat.DayNames = Enumerable.Range(1, 7).Select(day => "Host day " + day).ToArray();
        platform.DateTimeFormat.AbbreviatedDayNames = Enumerable.Range(1, 7).Select(day => "Host abbreviated day " + day).ToArray();
        platform.DateTimeFormat.FirstDayOfWeek = DayOfWeek.Saturday;
        platform.DateTimeFormat.AMDesignator = "Host AM";
        platform.DateTimeFormat.PMDesignator = "Host PM";
        return platform;
    }

    private static string? Observation(JsonElement observations, string name)
        => observations.EnumerateArray().Single(observation => observation.GetProperty("Name").GetString() == name).GetProperty("Value").GetString();
}
