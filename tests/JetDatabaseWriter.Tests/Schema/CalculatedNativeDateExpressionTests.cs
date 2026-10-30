namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Native Date variants retain their original OLE serial throughout expression evaluation.</summary>
public sealed class CalculatedNativeDateExpressionTests
{
    [Theory]
    [InlineData("CDbl([D])", -0.25d)]
    [InlineData("CDbl([D]+0)", -0.25d)]
    [InlineData("CDbl(-[D])", 0.25d)]
    [InlineData("CDbl(TimeSerial(-1,0,0))", -1d / 24d)]
    [InlineData("CDbl(#1899-12-29 12:00#)", -1.5d)]
    public void NumericDateConversions_RetainSerial(string expression, double expected)
        => Assert.Equal(expected, Assert.IsType<double>(Evaluate(expression, -0.25d)));

    [Theory]
    [InlineData("Abs([D])", 1.25d)]
    [InlineData("Int([D])", -2d)]
    [InlineData("Fix([D])", -1d)]
    [InlineData("Round([D],1)", -1.2d)]
    [InlineData("+[D]", -1.25d)]
    [InlineData("-[D]", 1.25d)]
    [InlineData("[D]+0", -1.25d)]
    [InlineData("[D]-0", -1.25d)]
    [InlineData("0-[D]", 1.25d)]
    [InlineData("-#1899-12-30 6:00#", -0.25d)]
    public void DateOperations_RetainDateVariant(string expression, double expected)
        => Assert.Equal(expected, Assert.IsType<NativeDate>(Evaluate(expression, -1.25d)).Serial);

    [Theory]
    [InlineData("[D] < #1899-12-29 6:00#")]
    [InlineData("#1899-12-29 12:00# < #1899-12-29 6:00#")]
    public void NegativeDates_CompareRawSerials(string expression)
        => Assert.True(Assert.IsType<bool>(Evaluate(expression, -1.5d)));

    [Theory]
    [InlineData(0.4999d / 86400d, 0)]
    [InlineData(0.5001d / 86400d, 1)]
    public void NativeSourceDate_DoesNotRoundToMillisecondsBeforeDateFunctions(double serial, int expected)
        => Assert.Equal(expected, Assert.IsType<short>(Evaluate("Second([D])", serial)));

    [Theory]
    [InlineData(0.4999d / 86400d, "12:00:00 AM")]
    [InlineData(0.5001d / 86400d, "12:00:01 AM")]
    [InlineData(0.5d / 86400d, "12:00:01 AM")]
    [InlineData(1.5d / 86400d, "12:00:02 AM")]
    public void NativeDateText_RoundsFromOriginalSerial(double serial, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate("[D] & ''", serial)));

    [Fact]
    public void StoredCalculatedDate_KeepsOriginalSerialForDependencies()
    {
        NativeDate value = Assert.IsType<NativeDate>(Evaluate("TimeSerial(-1,0,0)", 0d, ColumnType.DateTimeType));
        Assert.Equal(-1d / 24d, value.Serial);
    }

    private static object Evaluate(string expression, double serial, ColumnType resultType = 0)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "D" }, new ColumnInfo { Name = "Result" }] };
        ColumnConstraint[] constraints =
        [
            new() { Name = "D", ClrType = typeof(DateTime), StorageType = ColumnType.DateTimeType },
            new() { Name = "Result", ClrType = resultType == ColumnType.DateTimeType ? typeof(DateTime) : typeof(object), IsCalculated = true, CalculatedResultType = resultType, CalculationExpression = expression, CalculatedExpressionPlan = CalculatedExpressionParser.Parse(expression) },
        ];
        var context = new CalculatedExpressionEvaluationContext(table, constraints, [NativeDate.FromSerial(serial), DBNull.Value]);
        return context.EvaluateColumn(1);
    }
}
