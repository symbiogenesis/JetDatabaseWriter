namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
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
        => Assert.Throws<JetLimitationException>(() => Evaluate<DateTime>(expression));

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

    [Fact]
    public void ClosestUpperDateSerial_RefusesBeforeRoundedMillisecondsOverflow()
    {
        double serial = Math.BitDecrement(2958466d);
        Assert.False(CalculatedExpressionCoercion.TryConvertDateTime(serial, out _));
        Assert.Throws<OverflowException>(() => CalculatedExpressionCoercion.FromOleDate(serial));
    }

    [Fact]
    public void DateSerialRounding_PreservesLastMillisecondAndRefusesMidnightOverflow()
    {
        DateTime last = new(9999, 12, 31, 23, 59, 59, 999);
        double serial = last.ToOADate();
        Assert.Equal(last, CalculatedExpressionCoercion.FromOleDate(serial));
        Assert.True(CalculatedExpressionCoercion.TryConvertDateTime(serial, out DateTime converted));
        Assert.Equal(last, converted);

        const double roundedPastEnd = 2958466d - (0.25d / (TimeSpan.TicksPerDay / TimeSpan.TicksPerMillisecond));
        Assert.False(CalculatedExpressionCoercion.TryConvertDateTime(roundedPastEnd, out _));
        Assert.Throws<OverflowException>(() => CalculatedExpressionCoercion.FromOleDate(roundedPastEnd));
    }

    [Fact]
    public void RoundedDateSerialOverflow_ReportsStructuredNumericFailure()
    {
        JetLimitationException failure = Assert.Throws<JetLimitationException>(() => Evaluate<DateTime>("CDate(2958465.9999999995)"));
        Assert.Equal(JetErrorCode.NumericOverflow, failure.ErrorCode);
        Assert.Equal("Calc", failure.ErrorInfo.ColumnName);
    }

    [Theory]
    [InlineData(-1.25d)]
    [InlineData(-1.75d)]
    [InlineData(-0.25d)]
    [InlineData(-0.75d)]
    public void NegativeDateSerialFractions_KeepOleAutomationTimeOfDay(double serial)
    {
        var expected = DateTime.FromOADate(serial);
        Assert.Equal(expected, CalculatedExpressionCoercion.FromOleDate(serial));
        Assert.True(CalculatedExpressionCoercion.TryConvertDateTime(serial, out DateTime converted));
        Assert.Equal(expected, converted);
    }

    private static T Evaluate<T>(string expression)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Calc" }] };
        var constraints = new List<ColumnConstraint>
        {
            new() { Name = "Calc", ClrType = typeof(T), IsCalculated = true, CalculationExpression = expression, CalculatedExpressionPlan = CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false) },
        };
        object[] values = [DBNull.Value];
        CalculatedExpressionEvaluator.Apply(table, constraints, values, force: false);
        return Assert.IsType<T>(values[0]);
    }
}
