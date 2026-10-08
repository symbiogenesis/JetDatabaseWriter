namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Checks bounded Access Partition range formatting and argument semantics.</summary>
public sealed class CalculatedExpressionPartitionTests
{
    [Theory]
    [InlineData("Partition(-1,0,99,5)", "   : -1")]
    [InlineData("Partition(0,0,99,5)", "  0:  4")]
    [InlineData("Partition(99,0,99,5)", " 95: 99")]
    [InlineData("Partition(100,0,99,5)", "100:   ")]
    [InlineData("Partition(1001,100,1010,20)", "1000:1010")]
    [InlineData("Partition(1.5,0,10,2)", " 2: 3")]
    [InlineData("Partition(2.5,0,10,2)", " 2: 3")]
    [InlineData("Partition(2,0.5,10.5,1.5)", " 2: 3")]
    [InlineData("Partition(\"2\",0,10,2)", " 2: 3")]
    [InlineData("Partition(True,0,10,2)", "  :-1")]
    [InlineData("Partition(-1,0,10,1)", "  :-1")]
    [InlineData("Partition(11,0,10,1)", "11:  ")]
    [InlineData("Partition(2147483647,0,2147483647,2)", "2147483646:2147483647")]
    [InlineData("Partition(2,0,10,2147483647)", " 0:10")]
    [InlineData("Partition(-2147483648,0,1,2)", "  :-1")]
    public void Partition_FormatsAccessRanges(string expression, string expected)
        => Assert.Equal(expected, Evaluate(expression));

    [Theory]
    [InlineData("Partition(Null,0,10,2)")]
    [InlineData("Partition(1,Null,10,2)")]
    [InlineData("Partition(1,0,Null,2)")]
    [InlineData("Partition(1,0,10,Null)")]
    public void Partition_PropagatesNull(string expression)
        => Assert.IsType<DBNull>(Evaluate(expression));

    [Theory]
    [InlineData("Partition(1,-1,10,2)")]
    [InlineData("Partition(1,10,10,2)")]
    [InlineData("Partition(1,0,10,0)")]
    [InlineData("Partition(1,0,10)")]
    [InlineData("Partition(1,0,10,2,3)")]
    public void Partition_RejectsInvalidArguments(string expression)
        => Assert.Throws<ArgumentException>(() => Evaluate(expression));

    [Theory]
    [InlineData("Partition(2147483648,0,10,2)")]
    [InlineData("Partition(1,2147483648,10,2)")]
    [InlineData("Partition(1,0,2147483648,2)")]
    [InlineData("Partition(1,0,10,2147483648)")]
    public void Partition_RejectsLongOverflow(string expression)
        => Assert.Throws<OverflowException>(() => Evaluate(expression));

    [Fact]
    public void Partition_RejectsNonnumericText()
        => Assert.Throws<FormatException>(() => Evaluate("Partition(\"invalid\",0,10,2)"));

    private static object Evaluate(string expression)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Calc" }] };
        var constraints = new List<ColumnConstraint>
        {
            new() { Name = "Calc", ClrType = typeof(string), IsCalculated = true, CalculationExpression = expression },
        };
        object[] values = [DBNull.Value];
        CalculatedExpressionEvaluator.Apply(table, constraints, values, force: false);
        return values[0];
    }
}
