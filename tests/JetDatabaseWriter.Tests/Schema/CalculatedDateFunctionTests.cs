namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Native ACE controls for the legal calculated date functions.</summary>
public sealed class CalculatedDateFunctionTests
{
    [Fact]
    public void EnglishDateNamesAndSystemWeekStart_DoNotUseThreadCulture()
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
            Assert.Equal("February", Evaluate("MonthName(2)"));
            Assert.Equal("Sunday", Evaluate("WeekdayName(1,False,0)"));
            Assert.Equal((short)2, Evaluate("Weekday(#2025-02-03#,0)"));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Theory]
    [InlineData("Year(Null)")]
    [InlineData("Month(Null)")]
    [InlineData("Day(Null)")]
    [InlineData("Hour(Null)")]
    [InlineData("Minute(Null)")]
    [InlineData("Second(Null)")]
    [InlineData("Weekday(Null)")]
    [InlineData("Weekday(Null,8)")]
    public void NullDate_ReturnsOrdinaryNull(string expression)
        => Assert.Equal(DBNull.Value, Evaluate(expression));

    [Theory]
    [InlineData("Year(#2025-02-03#)", 2025)]
    [InlineData("Month(#2025-02-03#)", 2)]
    [InlineData("Day(#2025-02-03#)", 3)]
    [InlineData("Hour(#12:34:56#)", 12)]
    [InlineData("Minute(#12:34:56#)", 34)]
    [InlineData("Second(#12:34:56#)", 56)]
    [InlineData("Weekday(#2025-02-03#)", 2)]
    [InlineData("Weekday(#2025-02-03#,2.5)", 1)]
    [InlineData("Weekday(#2025-02-03#,1.5)", 1)]
    [InlineData("Year(45657.999995)", 2025)]
    [InlineData("Year(45657.999994)", 2024)]
    [InlineData("Second(0.0000058)", 1)]
    [InlineData("Second(0.0000057)", 0)]
    [InlineData("Second(-0.0000058)", 1)]
    [InlineData("Hour(-0.25)", 6)]
    public void DateParts_ReturnNativeIntegerAndRoundSeconds(string expression, int expected)
        => Assert.Equal(expected, Assert.IsType<short>(Evaluate(expression)));

    [Theory]
    [InlineData("MonthName(2)", "February")]
    [InlineData("MonthName(2,True)", "Feb")]
    [InlineData("MonthName(2.5,True)", "Feb")]
    [InlineData("MonthName(1.5,True)", "Feb")]
    [InlineData("MonthName(2,2)", "Feb")]
    [InlineData("WeekdayName(1)", "Sunday")]
    [InlineData("WeekdayName(1,True)", "Sun")]
    [InlineData("WeekdayName(1,False,2)", "Monday")]
    [InlineData("WeekdayName(1.5,True,1)", "Mon")]
    [InlineData("WeekdayName(2.5,True,1)", "Mon")]
    public void DateNames_RespectAbbreviationAndRoundedArguments(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(expression)));

    [Theory]
    [InlineData("Weekday(Null,Null)", 13)]
    [InlineData("Weekday(#2025-02-03#,Null)", 13)]
    [InlineData("Weekday(#2025-02-03#,8)", 5)]
    [InlineData("Weekday(#2025-02-03#,32768)", 5)]
    [InlineData("Weekday(#2025-02-03#,1E100)", 6)]
    [InlineData("Len(MonthName(0))", 5)]
    [InlineData("Len(MonthName(13))", 5)]
    [InlineData("Len(MonthName(Null))", 13)]
    [InlineData("Len(MonthName(2,Null))", 13)]
    [InlineData("Len(WeekdayName(0))", 5)]
    [InlineData("Len(WeekdayName(8))", 5)]
    [InlineData("Len(WeekdayName(1,Null))", 13)]
    [InlineData("Len(WeekdayName(1,False,Null))", 13)]
    [InlineData("Len(WeekdayName(1,False,8))", 5)]
    [InlineData("DateSerial(2025,Null,1)", 13)]
    [InlineData("DateSerial(9999,13,1)", 5)]
    [InlineData("DateSerial(2025,32768,1)", 6)]
    [InlineData("TimeSerial(Null,0,0)", 13)]
    [InlineData("TimeSerial(32768,0,0)", 6)]
    [InlineData("Year('invalid')", 13)]
    [InlineData("Year(1E100)", 13)]
    public void InvalidArguments_ReturnMeasuredNativeError(string expression, int expected)
        => Assert.Equal(expected, Assert.IsType<CalculatedError>(Evaluate(expression)).NativeCode);

    [Theory]
    [InlineData("TimeSerial(-1,0,0)", -1d / 24d)]
    [InlineData("TimeSerial(-25,0,0)", -25d / 24d)]
    [InlineData("TimeSerial(25,0,0)", 25d / 24d)]
    [InlineData("TimeSerial(1.5,2.5,3.5)", 7324d / 86400d)]
    public void TimeSerial_RetainsExactNativeDateSerial(string expression, double expected)
        => Assert.Equal(expected, Assert.IsType<NativeDate>(Evaluate(expression)).Serial);

    [Theory]
    [InlineData(-1900, 100)]
    [InlineData(-100, 1900)]
    [InlineData(-1, 1999)]
    [InlineData(0, 2000)]
    [InlineData(100, 100)]
    [InlineData(999, 999)]
    [InlineData(2024.5, 2024)]
    [InlineData(2025.5, 2026)]
    public void DateSerial_SourceYearRetainsDateSubtypeAndNativeRounding(double year, int expected)
        => Assert.Equal(expected, Assert.IsType<NativeDate>(Evaluate("DateSerial([Y],1,1)", year)).Value.Year);

    [Theory]
    [InlineData("MonthName(0)")]
    [InlineData("MonthName(2,Null)")]
    [InlineData("WeekdayName(8)")]
    [InlineData("WeekdayName(1,False,8)")]
    public async Task InvalidDateName_TextStorageRefusesAtomically(string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("T", [new("Value", typeof(string)) { IsCalculated = true, CalculationExpression = expression }], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("T", [DBNull.Value], TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
    }

    private static object Evaluate(string expression, double year = 2025)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Y" }, new ColumnInfo { Name = "Result" }] };
        ColumnConstraint[] constraints =
        [
            new() { Name = "Y", ClrType = typeof(double), StorageType = ColumnType.DoubleType },
            new() { Name = "Result", ClrType = typeof(object), IsCalculated = true, CalculationExpression = expression, CalculatedExpressionPlan = CalculatedExpressionParser.Parse(expression) },
        ];
        var context = new CalculatedExpressionEvaluationContext(table, constraints, [year, DBNull.Value]);
        return context.EvaluateColumn(1);
    }
}
