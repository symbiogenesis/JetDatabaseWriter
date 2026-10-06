namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Checks Access date conversion, integer arguments and clock precision.</summary>
public sealed class CalculatedExpressionDatePrecisionTests
{
    [Theory]
    [InlineData("DateValue(#2020-01-31 06:00#)", 2020, 1, 31, 0)]
    [InlineData("DateValue(\"2020-01-31 06:00\")", 2020, 1, 31, 0)]
    [InlineData("DateValue(\"6:00:00 AM\")", 1899, 12, 30, 0)]
    [InlineData("#2020-01-31 06:00#", 2020, 1, 31, 6)]
    [InlineData("#6:00:00 AM#", 1899, 12, 30, 6)]
    [InlineData("CDate(\"2020-01-31 06:00\")", 2020, 1, 31, 6)]
    public void DateConversions_PreserveTimeOnlyWhenRequired(string expression, int year, int month, int day, int hour)
        => Assert.Equal(new DateTime(year, month, day, hour, 0, 0), Evaluate<DateTime>(expression));

    [Theory]
    [InlineData("DateSerial(2020, 1, 1.5)", 2020, 1, 2)]
    [InlineData("DateSerial(2020, 1, 2.5)", 2020, 1, 2)]
    [InlineData("DateSerial(2020, 1.6, 1)", 2020, 2, 1)]
    [InlineData("DateSerial(2020, 2.5, 1)", 2020, 2, 1)]
    [InlineData("DateSerial(2020.5, 1, 1)", 2020, 1, 1)]
    [InlineData("DateSerial(2021.5, 1, 1)", 2022, 1, 1)]
    [InlineData("DateSerial(2020, 1, -1.5)", 2019, 12, 29)]
    public void DateSerial_RoundsEachArgumentHalfToEven(string expression, int year, int month, int day)
        => Assert.Equal(new DateTime(year, month, day), Evaluate<DateTime>(expression));

    [Theory]
    [InlineData("DateSerial(2020, 32768, 1)")]
    [InlineData("DateSerial(2020, 1, -32769)")]
    public void DateSerial_ArgumentsOutsideIntegerRangeOverflow(string expression)
        => Assert.Throws<OverflowException>(() => Evaluate<DateTime>(expression));

    [Theory]
    [InlineData("Now()")]
    [InlineData("Time()")]
    public void ClockDates_HaveWholeSecondPrecision(string expression)
        => Assert.Equal(0, Evaluate<DateTime>(expression).Ticks % TimeSpan.TicksPerSecond);

    [Fact]
    public void ClockPrecision_TruncatesDateWithoutReducingTimerPrecision()
    {
        DateTime clock = new DateTime(2020, 1, 31, 6, 0, 1, 987, DateTimeKind.Local).AddTicks(6543);

        Assert.Equal(new DateTime(2020, 1, 31, 6, 0, 1, DateTimeKind.Local), CalculatedExpressionDateTimeFunctions.TruncateToWholeSeconds(clock));
        Assert.Equal(clock.TimeOfDay.TotalSeconds, CalculatedExpressionDateTimeFunctions.TimerSeconds(clock));
        Assert.True(CalculatedExpressionDateTimeFunctions.TimerSeconds(clock) > 21601d);
    }

    private static T Evaluate<T>(string expression)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Calc" }] };
        var constraints = new List<ColumnConstraint>
        {
            new() { Name = "Calc", ClrType = typeof(T), IsCalculated = true, CalculationExpression = expression },
        };
        object[] values = [DBNull.Value];
        CalculatedExpressionEvaluator.Apply(table, constraints, values, force: false);
        return Assert.IsType<T>(values[0]);
    }
}
