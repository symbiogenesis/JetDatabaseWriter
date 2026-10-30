namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Independent native en-US date text controls distinguish accepted orders from invalid separators.</summary>
public sealed class CalculatedUnambiguousDateTextTests
{
    [Theory]
    [InlineData("31/12/2020", 2020, 12, 31, 0, 0)]
    [InlineData("31-12-2020", 2020, 12, 31, 0, 0)]
    [InlineData("31/12/20", 2031, 12, 20, 0, 0)]
    [InlineData("31/12/49", 2049, 12, 31, 0, 0)]
    [InlineData("31/12/2020 13:45", 2020, 12, 31, 13, 45)]
    [InlineData("31-Dec-2020", 2020, 12, 31, 0, 0)]
    [InlineData("2020/12/31", 2020, 12, 31, 0, 0)]
    public void NativeAcceptedDateText_MatchesAllParts(string text, int year, int month, int day, int hour, int minute)
        => Assert.Equal([year, month, day, hour, minute], Evaluate(text).Select(static value => checked((int)Assert.IsType<double>(value))));

    [Fact]
    public void NativeRejectedDotSeparator_PreservesTypeMismatch()
        => Assert.All(Evaluate("31.12.2020"), static value => Assert.Equal(13, Assert.IsType<CalculatedError>(value).NativeCode));

    private static object[] Evaluate(string text)
    {
        string[] functions = ["Year", "Month", "Day", "Hour", "Minute"];
        var table = new TableDef { Columns = [new ColumnInfo { Name = "S" }, .. functions.Select(static name => new ColumnInfo { Name = name })] };
        ColumnConstraint[] constraints =
        [
            new() { Name = "S", ClrType = typeof(string), StorageType = ColumnType.TextType },
            .. functions.Select(static name => new ColumnConstraint { Name = name, ClrType = typeof(double), IsCalculated = true, CalculationExpression = name + "([S])", CalculatedResultType = ColumnType.DoubleType }),
        ];
        object[] values = [text, .. functions.Select(static _ => (object)DBNull.Value)];
        CalculatedExpressionEvaluator.Apply(table, constraints, values);
        return values[1..];
    }
}
