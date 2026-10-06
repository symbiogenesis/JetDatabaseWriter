namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Checks VBA custom date and time formatting.</summary>
public sealed class CalculatedExpressionDateFormattingTests
{
    [Theory]
    [InlineData("mm/dd/yyyy", "01/31/2020")]
    [InlineData("m/d/yy", "1/31/20")]
    [InlineData("YYYY-MM-DD HH:NN:SS", "2020-01-31 17:04:23")]
    [InlineData("h:m:s", "17:4:23")]
    [InlineData("hh:mm:ss", "17:04:23")]
    [InlineData("nn:ss", "04:23")]
    [InlineData("mm:ss", "01:23")]
    [InlineData("mm hh:mm mm", "01 17:04 01")]
    [InlineData("ddddd", "1/31/2020")]
    [InlineData("dddddd", "Friday, January 31, 2020")]
    [InlineData("\\h mm", "h 01")]
    [InlineData("\"h\" mm", "h 01")]
    [InlineData("mm\\s", "01s")]
    [InlineData("mm\"s\"", "01s")]
    [InlineData("hh\" x \"mm", "17 x 04")]
    [InlineData("dddd, mmm d yyyy", "Friday, Jan 31 2020")]
    [InlineData("mmmm ddd", "January Fri")]
    [InlineData("w ww q y", "6 5 1 31")]
    [InlineData("hh:nn:ss AM/PM", "05:04:23 PM")]
    [InlineData("hh:nn:ss am/pm", "05:04:23 pm")]
    [InlineData("h A/P", "5 P")]
    [InlineData("h a/p", "5 p")]
    [InlineData("h AMPM", "5 PM")]
    [InlineData("ttttt", "5:04:23 PM")]
    [InlineData("c", "1/31/2020 5:04:23 PM")]
    [InlineData("\\m \\n \\y \\q", "m n y q")]
    [InlineData("\"AM/PM\" hh", "AM/PM 17")]
    [InlineData("\"month\" m", "month 1")]
    public void CustomFormats_UseVbaTokens(string format, string expected)
    {
        ArgumentNullException.ThrowIfNull(format);
        Assert.Equal(expected, Evaluate($"Format(#2020-01-31 17:04:23#, \"{format.Replace("\"", "\"\"", StringComparison.Ordinal)}\")"));
    }

    [Theory]
    [InlineData("#2020-01-31 00:00#", "12 AM")]
    [InlineData("#2020-01-31 12:00#", "12 PM")]
    public void TwelveHourFormats_HandleMidnightAndNoon(string date, string expected)
        => Assert.Equal(expected, Evaluate($"Format({date}, \"h AM/PM\")"));

    [Theory]
    [InlineData("Format(#2020-01-31#, \"w\", 2)", "5")]
    [InlineData("Format(#2021-01-01#, \"ww\", 2, 2)", "53")]
    [InlineData("Format(#2021-01-01#, \"ww\", 2, 3)", "52")]
    public void WeekFormats_RespectOptionalSettings(string expression, string expected)
        => Assert.Equal(expected, Evaluate(expression));

    [Theory]
    [InlineData("Format(#2020-12-31#, \"q y\")", "4 366")]
    [InlineData("Format(#2021-01-01#, \"q y\")", "1 1")]
    public void CalendarTokens_HandleQuarterAndLeapYearBoundaries(string expression, string expected)
        => Assert.Equal(expected, Evaluate(expression));

    private static string Evaluate(string expression)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Calc" }] };
        var constraints = new List<ColumnConstraint>
        {
            new() { Name = "Calc", ClrType = typeof(string), IsCalculated = true, CalculationExpression = expression },
        };
        object[] values = [DBNull.Value];
        CalculatedExpressionEvaluator.Apply(table, constraints, values, force: false);
        return Assert.IsType<string>(values[0]);
    }
}
