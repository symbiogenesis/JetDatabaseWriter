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

    /// <summary>
    /// A Boolean stored in a Byte result is 255 or 0, the OLE Automation
    /// Bool-to-Byte conversion (VBA's <c>CByte(True)</c> is 255), not -1.
    /// </summary>
    /// <param name="a">The field value.</param>
    /// <param name="expected">The stored byte.</param>
    [Theory]
    [InlineData(3, 255)]
    [InlineData(0, 0)]
    public void ByteResult_FromBoolean_Is255OrZero(int a, int expected)
    {
        object result = Evaluate("[A] > 1", typeof(byte), ("A", a));

        Assert.Equal((byte)expected, Assert.IsType<byte>(result));
    }

    [Theory]
    [InlineData("CByte(True)", 255)]
    [InlineData("CByte(False)", 0)]
    [InlineData("CByte(2.5)", 2)]
    [InlineData("CByte(3.5)", 4)]
    [InlineData("CByte(255)", 255)]
    [InlineData("CByte(\"12\")", 12)]
    public void CByte_FollowsOleAutomationConversion(string expression, int expected)
    {
        object result = Evaluate(expression, typeof(int));

        Assert.Equal(expected, Assert.IsType<int>(result));
    }

    [Theory]
    [InlineData("CByte(-1)")]
    [InlineData("CByte(256)")]
    public void CByte_OutOfRange_ThrowsOverflowNamingColumnAndExpression(string expression)
    {
        OverflowException exception = Assert.Throws<OverflowException>(() => Evaluate(expression, typeof(int)));

        Assert.Contains("'Calc'", exception.Message, StringComparison.Ordinal);
        Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(exception.InnerException);
    }

    [Fact]
    public void ResultOutOfRange_ThrowsOverflowNamingValueAndResultType()
    {
        OverflowException exception = Assert.Throws<OverflowException>(() => Evaluate("[A] * 100", typeof(byte), ("A", 5)));

        Assert.Contains("'Calc'", exception.Message, StringComparison.Ordinal);
        Assert.Contains("[A] * 100", exception.Message, StringComparison.Ordinal);
        Assert.Contains("500", exception.Message, StringComparison.Ordinal);
        Assert.Contains("Byte", exception.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(exception.InnerException);
    }

    /// <summary>
    /// A number or Boolean becomes a date through the OLE Automation date serial
    /// (days since 1899-12-30), so True is 1899-12-29.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="expected">The expected date, as invariant text.</param>
    [Theory]
    [InlineData("43861.25", "2020-01-31 06:00:00")]
    [InlineData("True", "1899-12-29 00:00:00")]
    [InlineData("[N]", "2020-01-31 00:00:00")]
    [InlineData("CDate([N])", "2020-01-31 00:00:00")]
    [InlineData("CDate([N] + 0.5)", "2020-01-31 12:00:00")]
    public void DateResult_FromNumberOrBoolean_UsesOleDateSerial(string expression, string expected)
    {
        object result = Evaluate(expression, typeof(DateTime), ("N", 43861));

        Assert.Equal(expected, Assert.IsType<DateTime>(result).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("IsDate(5)", false)]
    [InlineData("IsDate([N])", false)]
    [InlineData("IsDate(\"2025-02-03\")", true)]
    [InlineData("IsDate(#2025-02-03#)", true)]
    public void IsDate_Number_IsFalse(string expression, bool expected)
    {
        object result = Evaluate(expression, typeof(bool), ("N", 43861));

        Assert.Equal(expected, Assert.IsType<bool>(result));
    }

    [Theory]
    [InlineData("[D] > [N]", true)]
    [InlineData("[D] = [N]", false)]
    [InlineData("[D] = [N] + 1", true)]
    [InlineData("[D] = 43862", true)]
    public void DateComparison_WithIntegerField_ComparesAsDates(string expression, bool expected)
    {
        object result = EvaluateDeclared(
            expression,
            typeof(bool),
            ("D", typeof(DateTime), new DateTime(2020, 2, 1)),
            ("N", typeof(int), 43861));

        Assert.Equal(expected, Assert.IsType<bool>(result));
    }

    [Fact]
    public void NestedCalculatedFailure_NamesInnermostColumnOnce()
    {
        var tableDef = new TableDef();
        tableDef.Columns.Add(new ColumnInfo { Name = "Inner" });
        tableDef.Columns.Add(new ColumnInfo { Name = "Outer" });
        ColumnConstraint[] constraints =
        [
            new() { Name = "Inner", ClrType = typeof(int), IsCalculated = true, CalculationExpression = "CByte(-1)" },
            new() { Name = "Outer", ClrType = typeof(int), IsCalculated = true, CalculationExpression = "[Inner] + 1" },
        ];
        object[] values = [DBNull.Value, DBNull.Value];

        OverflowException exception = Assert.Throws<OverflowException>(() => CalculatedExpressionEvaluator.Apply(tableDef, constraints, values, force: false, "T"));

        Assert.Contains("'Inner'", exception.Message, StringComparison.Ordinal);
        Assert.Contains("'T'", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("'Outer'", exception.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(exception.InnerException);
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
