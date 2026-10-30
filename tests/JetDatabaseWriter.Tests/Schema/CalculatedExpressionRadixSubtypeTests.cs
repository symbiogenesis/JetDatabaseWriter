namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Checks that radix literals retain their VBA Variant subtype.</summary>
public sealed class CalculatedExpressionRadixSubtypeTests
{
    [Fact]
    public void RadixTextInBracketedFieldName_IsPreserved()
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse("[&H10] & \"&O17\"");
        var context = new CalculatedExpressionEvaluationContext(
            new TableDef { Columns = [new ColumnInfo { Name = "&H10" }] },
            [new ColumnConstraint { Name = "&H10", ClrType = typeof(string) }],
            ["field"]);

        Assert.Equal("field&O17", Assert.IsType<string>(plan.Evaluate(context)));
    }

    [Theory]
    [InlineData("TypeName(&H10)", "Integer")]
    [InlineData("TypeName(&HFFFF)", "Integer")]
    [InlineData("TypeName(&H10&)", "Long")]
    [InlineData("TypeName(&H10000)", "Long")]
    [InlineData("TypeName(&HFFFFFFFF)", "Long")]
    [InlineData("TypeName(&O17)", "Integer")]
    [InlineData("TypeName(&17)", "Integer")]
    [InlineData("TypeName(&O177777)", "Integer")]
    [InlineData("TypeName(&O177777&)", "Long")]
    [InlineData("TypeName(+&H10)", "Integer")]
    [InlineData("TypeName(-&H10)", "Integer")]
    [InlineData("TypeName(-&H8000)", "Long")]
    [InlineData("CStr(-&H8000)", "32768")]
    [InlineData("TypeName(-&H80000000&)", "Double")]
    [InlineData("CStr(-&H80000000&)", "2147483648")]
    [InlineData("CStr(Abs(&HFFFF))", "1")]
    [InlineData("\"prefix\" & &H10", "prefix16")]
    [InlineData("TypeName(Not &H10)", "Integer")]
    [InlineData("TypeName(&H10 And &H1)", "Integer")]
    [InlineData("TypeName(&H10& And &H1)", "Long")]
    [InlineData("CStr(VarType(&HFFFF))", "2")]
    [InlineData("CStr(VarType(&HFFFF&))", "3")]
    [InlineData("CStr(&HFFFF)", "-1")]
    [InlineData("CStr(&HFFFF&)", "65535")]
    [InlineData("CStr(&O177777)", "-1")]
    [InlineData("CStr(&O177777&)", "65535")]
    [InlineData("\"&H10\"", "&H10")]
    [InlineData("'&O17'", "&O17")]
    public void RadixLiteral_PreservesValueAndSubtype(string expression, string expected)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Calc" }] };
        ColumnConstraint[] constraints =
        [
            new ColumnConstraint
            {
                Name = "Calc",
                ClrType = typeof(string),
                IsCalculated = true,
                CalculationExpression = expression,
                CalculatedExpressionPlan = CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false),
            },
        ];
        object[] values = [DBNull.Value];

        CalculatedExpressionEvaluator.Apply(table, constraints, values);

        Assert.Equal(expected, Assert.IsType<string>(values[0]));
    }
}
