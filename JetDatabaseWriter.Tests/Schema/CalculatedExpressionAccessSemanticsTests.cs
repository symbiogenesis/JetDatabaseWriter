namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>
/// Pins the calculated-column evaluator to Access (VBA) operator precedence
/// and value semantics rather than the Excel grammar of the underlying
/// formula parser: <c>^</c> binds tighter than unary minus and associates
/// left to right, <c>*</c> and <c>/</c> bind tighter than <c>\</c>, which
/// binds tighter than <c>Mod</c>, and Excel's postfix <c>%</c> is rejected.
/// </summary>
public sealed class CalculatedExpressionAccessSemanticsTests
{
    [Theory]
    [InlineData("-2^2", -4d)]
    [InlineData("-2 ^ 2", -4d)]
    [InlineData("-2^2 + (0 Mod 5)", -4d)]
    [InlineData("(-2)^2", 4d)]
    [InlineData("0 - 2^2", -4d)]
    [InlineData("3 - -2^2", 7d)]
    [InlineData("2^3^2", 64d)]
    [InlineData("2^-1", 0.5d)]
    [InlineData("-2^-1", -0.5d)]
    [InlineData("2 + 3 * 4", 14d)]
    [InlineData("10 - 4 - 3", 3d)]
    [InlineData("100 / 10 / 5", 2d)]
    [InlineData("7 \\ 2 * 2", 1d)]
    [InlineData("7 \\ 2 Mod 2", 1d)]
    [InlineData("10 Mod 3 * 2", 4d)]
    [InlineData("1 + 5 Mod 3", 3d)]
    [InlineData("=-2^2", -4d)]
    public void NumericExpression_FollowsAccessPrecedence(string expression, double expected)
    {
        object result = Evaluate(expression, typeof(double));

        Assert.Equal(expected, Assert.IsType<double>(result), 12);
    }

    [Fact]
    public void LongOperatorChain_StaysWithinNestingLimit()
    {
        string expression = string.Join(" + ", Enumerable.Repeat("[A] * 2 - 1", 200));

        object result = Evaluate(expression, typeof(double), ("A", 3));

        Assert.Equal(1000d, Assert.IsType<double>(result));
    }

    [Theory]
    [InlineData("2 * 3 ^ 2", 18d)]
    [InlineData("(2 * 3) ^ 2", 36d)]
    [InlineData("1 - (2 - 3)", 2d)]
    [InlineData("-5 Mod 3", -2d)]
    [InlineData("-(5 Mod 3)", -2d)]
    [InlineData("2 ^ -(1 + 1)", 0.25d)]
    [InlineData("Abs(-2^2) * -1", -4d)]
    public void MixedOperators_KeepAccessGrouping(string expression, double expected)
    {
        object result = Evaluate(expression, typeof(double));

        Assert.Equal(expected, Assert.IsType<double>(result), 12);
    }

    [Fact]
    public void UnaryMinusOnFieldPower_AppliesAfterExponent()
    {
        object result = Evaluate("-[A]^2", typeof(double), ("A", 3));

        Assert.Equal(-9d, Assert.IsType<double>(result));
    }

    [Theory]
    [InlineData("-2^2 = -4", true)]
    [InlineData("1 < 2 < 3", true)]
    [InlineData("3 > 2 > 1", false)]
    [InlineData("(1 < 2) = -1", true)]
    [InlineData("True = -1", true)]
    [InlineData("True < False", true)]
    [InlineData("Not 1 = 2", true)]
    [InlineData("\"10\" < \"9\"", true)]
    [InlineData("\"abc\" = \"ABC\"", true)]
    public void BooleanExpression_UsesAccessComparisonSemantics(string expression, bool expected)
    {
        object result = Evaluate(expression, typeof(bool));

        Assert.Equal(expected, Assert.IsType<bool>(result));
    }

    [Theory]
    [InlineData("True + 1", 0)]
    [InlineData("False - True", 1)]
    [InlineData("-True", 1)]
    public void Booleans_AreMinusOneAndZeroInArithmetic(string expression, int expected)
    {
        object result = Evaluate(expression, typeof(int));

        Assert.Equal(expected, Assert.IsType<int>(result));
    }

    [Fact]
    public void BooleanResult_StoredInNumericColumn_IsMinusOne()
    {
        object result = Evaluate("[A] > 1", typeof(int), ("A", 3));

        Assert.Equal(-1, Assert.IsType<int>(result));
    }

    [Theory]
    [InlineData("CInt(True)", -1d)]
    [InlineData("CLng(True)", -1d)]
    [InlineData("CDbl(True)", -1d)]
    [InlineData("CSng(True)", -1d)]
    [InlineData("CCur(True)", -1d)]
    [InlineData("CDec(1 < 2)", -1d)]
    [InlineData("CInt(False)", 0d)]
    public void ConversionFunctions_MapTrueToMinusOne(string expression, double expected)
    {
        object result = Evaluate(expression, typeof(double));

        Assert.Equal(expected, Assert.IsType<double>(result));
    }

    [Theory]
    [InlineData("\"x\" & (1 < 2)", "x-1")]
    [InlineData("(1 < 2) & \"\"", "-1")]
    [InlineData("(1 > 2) & \"\"", "0")]
    [InlineData("CStr(True)", "-1")]
    [InlineData("1 < 2", "-1")]
    [InlineData("[Flag] & \"\"", "-1")]
    public void Booleans_AreMinusOneAndZeroAsText(string expression, string expected)
    {
        object result = Evaluate(expression, typeof(string), ("Flag", true));

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("[A] + [B]", "19")]
    [InlineData("[A] < [B]", "0")]
    [InlineData("[A] & [B]", "109")]
    public void TextValuesInNumericColumns_EvaluateAsNumbers(string expression, string expected)
    {
        object result = EvaluateDeclared(expression, typeof(string), ("A", typeof(int), "10"), ("B", typeof(int), "9"));

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("[A] + [B]", "109")]
    [InlineData("[A] < [B]", "-1")]
    public void NumericValuesInTextColumns_EvaluateAsText(string expression, string expected)
    {
        object result = EvaluateDeclared(expression, typeof(string), ("A", typeof(string), 10), ("B", typeof(string), 9));

        Assert.Equal(expected, result);
    }

    [Fact]
    public void TextValuesInDateColumns_CompareAsDates()
    {
        object result = EvaluateDeclared("[D1] < [D2]", typeof(bool), ("D1", typeof(DateTime), "10/1/2020"), ("D2", typeof(DateTime), "2/1/2020"));

        Assert.False(Assert.IsType<bool>(result));
    }

    [Theory]
    [InlineData("1 + 2 & 3", "33")]
    [InlineData("\"a\" & 1 + 2", "a3")]
    [InlineData("\"a\" + \"b\"", "ab")]
    [InlineData("\"say \"\"hi\"\"\"", "say \"hi\"")]
    [InlineData("CStr(Year(#2025-02-03#))", "2025")]
    public void TextExpression_FollowsAccessSemantics(string expression, string expected)
    {
        object result = Evaluate(expression, typeof(string));

        Assert.Equal(expected, result);
    }

    [Fact]
    public void PlusOnTextFields_Concatenates()
    {
        object result = Evaluate("[A] + [B]", typeof(string), ("A", "Al"), ("B", "pha"));

        Assert.Equal("Alpha", result);
    }

    [Theory]
    [InlineData("5%")]
    [InlineData("[A]%")]
    [InlineData("[A] Mod 2 + 5%")]
    [InlineData("IsNull([A]) = TRUE%")]
    public void Parse_PercentOperator_ThrowsArgumentExceptionNamingExpression(string expression)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => CalculatedExpressionPlan.Parse(expression));

        Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
        Assert.Contains("'%'", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\"5%\"", "5%")]
    [InlineData("[Pct%]", "7")]
    [InlineData("[A] Like \"5%\"", "0")]
    public void Parse_PercentInsideStringOrFieldName_IsAccepted(string expression, string expected)
    {
        object result = Evaluate(expression, typeof(string), ("A", "50"), ("Pct%", 7));

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("[A] + 1 )")]
    [InlineData("1 2")]
    [InlineData("[A] Mod 2 :")]
    [InlineData("{1, 2}")]
    public void Parse_TrailingOrSpreadsheetSyntax_ThrowsArgumentExceptionNamingExpression(string expression)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => CalculatedExpressionPlan.Parse(expression));

        Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
    }

    private static object Evaluate(string expression, Type resultType, params (string Name, object Value)[] inputs)
        => EvaluateDeclared(expression, resultType, [.. inputs.Select(input => (input.Name, input.Value.GetType(), input.Value))]);

    private static object EvaluateDeclared(string expression, Type resultType, params (string Name, Type ClrType, object Value)[] inputs)
    {
        var tableDef = new TableDef();
        var constraints = new List<ColumnConstraint>();
        object[] values = new object[inputs.Length + 1];
        for (int i = 0; i < inputs.Length; i++)
        {
            tableDef.Columns.Add(new ColumnInfo { Name = inputs[i].Name });
            constraints.Add(new ColumnConstraint { Name = inputs[i].Name, ClrType = inputs[i].ClrType });
            values[i] = inputs[i].Value;
        }

        tableDef.Columns.Add(new ColumnInfo { Name = "Calc" });
        constraints.Add(new ColumnConstraint
        {
            Name = "Calc",
            ClrType = resultType,
            IsCalculated = true,
            CalculationExpression = expression,
        });
        values[^1] = DBNull.Value;

        CalculatedExpressionEvaluator.Apply(tableDef, constraints, values, force: false);
        return values[^1] is IFormattable formattable && resultType == typeof(string)
            ? formattable.ToString(null, CultureInfo.InvariantCulture)
            : values[^1];
    }
}
