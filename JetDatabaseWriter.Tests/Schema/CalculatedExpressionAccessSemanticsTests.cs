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
        var tableDef = new TableDef { Columns = [new ColumnInfo { Name = "Inner" }, new ColumnInfo { Name = "Outer" }] };
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

    /// <summary>
    /// Date arithmetic follows the OLE Automation Variant rules VBA uses (measured with
    /// oleaut32's VarAdd/VarSub/VarNeg): a date plus or minus a number, Boolean, numeric
    /// text or another date is a date, and the result is the date serial sum.
    /// </summary>
    /// <param name="expression">The expression, over D = 2020-01-31 06:00, E = 2020-01-01 and D2 = 2020-01-31.</param>
    /// <param name="expected">The expected date, as invariant text.</param>
    [Theory]
    [InlineData("#2020-01-31 06:00# + 1", "2020-02-01 06:00:00")]
    [InlineData("[D] - 1", "2020-01-30 06:00:00")]
    [InlineData("[D] + 0.5", "2020-01-31 18:00:00")]
    [InlineData("1 + [D2]", "2020-02-01 00:00:00")]
    [InlineData("1 - [D2]", "1779-11-29 00:00:00")]
    [InlineData("[D] + [E]", "2140-02-02 06:00:00")]
    [InlineData("[D] + True", "2020-01-30 06:00:00")]
    [InlineData("[D] + \"1\"", "2020-02-01 06:00:00")]
    [InlineData("-#2020-01-31#", "1779-11-28 00:00:00")]
    [InlineData("+[D]", "2020-01-31 06:00:00")]
    [InlineData("[D] + CDec(2.5)", "2020-02-02 18:00:00")]
    public void DateArithmetic_FollowsVariantRules(string expression, string expected)
    {
        object result = EvaluateDates(expression, typeof(DateTime));

        Assert.Equal(expected, Assert.IsType<DateTime>(result).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
    }

    [Fact]
    public void DateDifference_IsDoubleDays()
    {
        object result = EvaluateDates("[D] - [E]", typeof(object));

        Assert.Equal(30.25d, Assert.IsType<double>(result));
    }

    [Theory]
    [InlineData("[D] * 2", 87722.5d)]
    [InlineData("[D] / 2", 21930.625d)]
    [InlineData("[E] ^ 1", 43831d)]
    public void DateTimesNumber_IsDouble(string expression, double expected)
    {
        object result = EvaluateDates(expression, typeof(object));

        Assert.Equal(expected, Assert.IsType<double>(result));
    }

    [Fact]
    public void DateArithmetic_InvalidOperand_ThrowsNamingColumn()
    {
        InvalidCastException mismatch = Assert.Throws<InvalidCastException>(() => EvaluateDates("[D] + \"abc\"", typeof(DateTime)));
        Assert.Contains("'Calc'", mismatch.Message, StringComparison.Ordinal);
        Assert.Contains("[D] + \"abc\"", mismatch.Message, StringComparison.Ordinal);

        OverflowException overflow = Assert.Throws<OverflowException>(() => EvaluateDates("[D] + 3000000", typeof(DateTime)));
        Assert.Contains("'Calc'", overflow.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>\</c> and <c>Mod</c> round both operands half to even before dividing, as
    /// OLE Automation's VarIdiv and VarMod do, and truncate toward zero.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="expected">The expected result.</param>
    [Theory]
    [InlineData("7.6 \\ 2", 4)]
    [InlineData("7 \\ 2.5", 3)]
    [InlineData("2.5 \\ 3", 0)]
    [InlineData("-7 \\ 2", -3)]
    [InlineData("7 Mod 2.5", 1)]
    [InlineData("3.5 Mod 7", 4)]
    [InlineData("-7 Mod 3", -1)]
    [InlineData("#2020-01-31# \\ 2", 21930)]
    [InlineData("#2020-01-31# Mod 2", 1)]
    [InlineData("True \\ 1", -1)]
    [InlineData("\"12\" Mod 5", 2)]
    public void IntegerDivisionAndMod_RoundOperandsFirst(string expression, int expected)
    {
        object result = Evaluate(expression, typeof(object));

        Assert.Equal(expected, Assert.IsType<int>(result));
    }

    [Theory]
    [InlineData("1 \\ 0")]
    [InlineData("5 Mod 0.4")]
    public void IntegerDivisionOrModByZero_ThrowsDivideByZero(string expression)
    {
        DivideByZeroException exception = Assert.Throws<DivideByZeroException>(() => Evaluate(expression, typeof(int)));

        Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void IntegerDivision_OperandBeyondLong_ThrowsOverflow()
        => Assert.Throws<OverflowException>(() => Evaluate("10000000000 \\ 1", typeof(int)));

    /// <summary>
    /// <c>And</c>, <c>Or</c>, <c>Xor</c>, <c>Eqv</c>, <c>Imp</c> and <c>Not</c> are bitwise
    /// on numbers, as in VBA (OLE Automation VarAnd and friends): numeric operands are
    /// rounded half to even to a Long, and True is -1.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="expected">The expected result.</param>
    [Theory]
    [InlineData("12 And 10", 8)]
    [InlineData("12 Or 10", 14)]
    [InlineData("12 Xor 10", 6)]
    [InlineData("12 Eqv 10", -7)]
    [InlineData("12 Imp 10", -5)]
    [InlineData("Not 5", -6)]
    [InlineData("Not 0", -1)]
    [InlineData("Not -1", 0)]
    [InlineData("True And 12", 12)]
    [InlineData("True Or 12", -1)]
    [InlineData("True Xor 12", -13)]
    [InlineData("2.5 And 3", 2)]
    [InlineData("3.5 And 7", 4)]
    [InlineData("\"12\" And 10", 8)]
    [InlineData("Not 2.5", -3)]
    [InlineData("#2020-01-31# And 1", 1)]
    [InlineData("[S] And 6", 4)]
    public void LogicalOperators_OnNumbers_AreBitwise(string expression, int expected)
    {
        object result = EvaluateDeclared(expression, typeof(int), ("S", typeof(short), (short)12));

        Assert.Equal(expected, Assert.IsType<int>(result));
    }

    [Theory]
    [InlineData("[B1] And [B2]", 64)]
    [InlineData("[B1] Or [B2]", 236)]
    [InlineData("Not [B1]", 55)]
    [InlineData("[B1] Imp [B2]", 119)]
    public void LogicalOperators_OnBytes_StayByte(string expression, int expected)
    {
        object result = EvaluateDeclared(expression, typeof(object), ("B1", typeof(byte), (byte)200), ("B2", typeof(byte), (byte)100));

        Assert.Equal((byte)expected, Assert.IsType<byte>(result));
    }

    [Theory]
    [InlineData("True And True", true)]
    [InlineData("True And False", false)]
    [InlineData("False And False", false)]
    [InlineData("True Or False", true)]
    [InlineData("False Or False", false)]
    [InlineData("True Xor True", false)]
    [InlineData("True Xor False", true)]
    [InlineData("True Eqv True", true)]
    [InlineData("True Eqv False", false)]
    [InlineData("True Imp False", false)]
    [InlineData("False Imp False", true)]
    [InlineData("Not True", false)]
    [InlineData("Not 1 = 2", true)]
    public void LogicalOperators_OnBooleans_StayBoolean(string expression, bool expected)
    {
        object result = Evaluate(expression, typeof(object));

        Assert.Equal(expected, Assert.IsType<bool>(result));
    }

    /// <summary>
    /// Null follows VBA's three-valued logic (measured with OLE Automation): And with a
    /// zero or False operand is that operand, Or with a nonzero or True operand is that
    /// operand, and every other combination with Null is Null.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="expected">The expected result as text: "Null", "True", "False" or a number.</param>
    [Theory]
    [InlineData("Null And False", "False")]
    [InlineData("Null And True", "Null")]
    [InlineData("Null And 0", "0")]
    [InlineData("Null And 12", "Null")]
    [InlineData("Null Or True", "True")]
    [InlineData("Null Or False", "Null")]
    [InlineData("Null Or 12", "12")]
    [InlineData("Not Null", "Null")]
    [InlineData("Null Xor True", "Null")]
    [InlineData("Null Eqv True", "Null")]
    [InlineData("Null Imp True", "True")]
    [InlineData("Null Imp 0", "Null")]
    [InlineData("False Imp Null", "True")]
    [InlineData("True Imp Null", "Null")]
    [InlineData("0 Imp Null", "-1")]
    [InlineData("[N] And False", "False")]
    public void LogicalOperators_WithNull_FollowVbaThreeValuedLogic(string expression, string expected)
    {
        object result = EvaluateDeclared(expression, typeof(object), ("N", typeof(int), DBNull.Value));

        string actual = result switch
        {
            DBNull => "Null",
            bool boolean => boolean ? "True" : "False",
            _ => Convert.ToString(result, CultureInfo.InvariantCulture)!,
        };
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void BitwiseOperand_OutOfRange_Throws()
    {
        Assert.Throws<OverflowException>(() => Evaluate("1E10 And 1", typeof(int)));
        Assert.Throws<InvalidCastException>(() => Evaluate("\"abc\" And 1", typeof(int)));
    }

    [Theory]
    [InlineData(1, "n")]
    [InlineData(3, "y")]
    public void IIfOnNumericAnd_UsesBitwiseResult(int a, string expected)
    {
        object result = Evaluate("IIf([A] And [B], \"y\", \"n\")", typeof(string), ("A", a), ("B", 2));

        Assert.Equal(expected, result);
    }

    /// <summary>
    /// <c>&amp;H</c> and <c>&amp;O</c> literals (and a bare <c>&amp;</c> before an octal
    /// digit) follow VBA's typing: up to <c>&amp;HFFFF</c> is a signed Integer, so
    /// <c>&amp;HFFFF</c> is -1; a larger value is a signed Long; a trailing <c>&amp;</c>
    /// forces Long.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="expected">The expected value.</param>
    [Theory]
    [InlineData("&H10", 16d)]
    [InlineData("&h1f", 31d)]
    [InlineData("&HFF", 255d)]
    [InlineData("&HFFFF", -1d)]
    [InlineData("&H8000", -32768d)]
    [InlineData("&H10000", 65536d)]
    [InlineData("&HFFFFFFFF", -1d)]
    [InlineData("&HFFFF&", 65535d)]
    [InlineData("&O17", 15d)]
    [InlineData("&o17", 15d)]
    [InlineData("&17", 15d)]
    [InlineData("[A] + &H10", 17d)]
    [InlineData("2 ^ &HFFFF", 0.5d)]
    [InlineData("&HFFFF ^ 2", 1d)]
    [InlineData("-&H10", -16d)]
    [InlineData("Abs(&HFFFF)", 1d)]
    [InlineData("[A] = &H1", -1d)]
    public void RadixLiterals_FollowVbaTyping(string expression, double expected)
    {
        object result = Evaluate(expression, typeof(double), ("A", 1));

        Assert.Equal(expected, Assert.IsType<double>(result));
    }

    [Theory]
    [InlineData("&H100000000")]
    [InlineData("&HG1")]
    [InlineData("&O8")]
    [InlineData("&H")]
    [InlineData("1 + &H")]
    public void RadixLiteral_Invalid_ThrowsArgumentExceptionNamingExpression(string expression)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => CalculatedExpressionPlan.Parse(expression));

        Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("[A]&10", "x10")]
    [InlineData("[A] & 10", "x10")]
    [InlineData("\"x\" & &H10", "x16")]
    [InlineData("[A] & &O17", "x15")]
    public void AmpersandAfterOperand_IsConcatenation(string expression, string expected)
    {
        object result = Evaluate(expression, typeof(string), ("A", "x"));

        Assert.Equal(expected, result);
    }

    /// <summary>
    /// A single-quoted string is a text literal, with <c>''</c> for a quote; brackets,
    /// <c>#</c> and <c>%</c> inside it are text, not a field, a date or an operator.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="expected">The expected text.</param>
    [Theory]
    [InlineData("'abc'", "abc")]
    [InlineData("'it''s'", "it's")]
    [InlineData("'say \"hi\"'", "say \"hi\"")]
    [InlineData("'a' & \"b\"", "ab")]
    [InlineData("'[Not a field]'", "[Not a field]")]
    [InlineData("'#1/1/2020#'", "#1/1/2020#")]
    [InlineData("'5%'", "5%")]
    [InlineData("[A] & ' ' & [B]", "Al pha")]
    [InlineData("\"it's\" & 'x'", "it'sx")]
    [InlineData("[O'Brien] & '!'", "O!")]
    [InlineData("''", "")]
    public void SingleQuotedStrings_AreTextLiterals(string expression, string expected)
    {
        object result = Evaluate(expression, typeof(string), ("A", "Al"), ("B", "pha"), ("O'Brien", "O"));

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("'abc")]
    [InlineData("[A] & 'x")]
    public void SingleQuotedString_Unterminated_ThrowsArgumentException(string expression)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => CalculatedExpressionPlan.Parse(expression));

        Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A date becomes text (<c>&amp;</c>, <c>CStr</c>, <c>Format</c> without a format, a
    /// Text result column) in VBA's General Date form for en-US, as OLE Automation's
    /// VarBstrFromDate gives it (measured with oleaut32, LCID 1033): no zero padding, a
    /// 12-hour clock, no time part at midnight, and no date part on day 0 (1899-12-30).
    /// </summary>
    /// <param name="expression">The expression, over D = 2020-01-31 06:00, E = 2020-01-01 and D2 = 2020-01-31.</param>
    /// <param name="expected">The expected text.</param>
    [Theory]
    [InlineData("[D]", "1/31/2020 6:00:00 AM")]
    [InlineData("[D] & \"\"", "1/31/2020 6:00:00 AM")]
    [InlineData("\"x\" & [D]", "x1/31/2020 6:00:00 AM")]
    [InlineData("CStr([D2])", "1/31/2020")]
    [InlineData("CStr([E])", "1/1/2020")]
    [InlineData("CStr(#2020-01-31 12:00:00#)", "1/31/2020 12:00:00 PM")]
    [InlineData("CStr(#2020-01-31 13:05:09#)", "1/31/2020 1:05:09 PM")]
    [InlineData("CStr(#2020-01-31 00:00:01#)", "1/31/2020 12:00:01 AM")]
    [InlineData("CStr(#2001-10-05 09:03:04#)", "10/5/2001 9:03:04 AM")]
    [InlineData("CStr(CDate(0.25))", "6:00:00 AM")]
    [InlineData("CStr(CDate(0))", "12:00:00 AM")]
    [InlineData("CStr(CDate(-1))", "12/29/1899")]
    [InlineData("CStr(CDate(-1.75))", "12/29/1899 6:00:00 PM")]
    [InlineData("CStr(#0100-01-01#)", "1/1/100")]
    [InlineData("CStr([D] + 1)", "2/1/2020 6:00:00 AM")]
    [InlineData("Format([D])", "1/31/2020 6:00:00 AM")]
    [InlineData("Format([D], \"General Date\")", "1/31/2020 6:00:00 AM")]
    [InlineData("FormatDateTime([D2], 0)", "1/31/2020")]
    [InlineData("FormatDateTime([D], vbGeneralDate)", "1/31/2020 6:00:00 AM")]
    [InlineData("Len([D])", "20")]
    [InlineData("CStr(DateValue(CDate(0.25)))", "12:00:00 AM")]
    [InlineData("IIf([D] Like \"1/31/2020 6:*\", \"y\", \"n\")", "y")]
    public void DateToText_IsEnUsGeneralDate(string expression, string expected)
    {
        object result = EvaluateDates(expression, typeof(string));

        Assert.Equal(expected, result);
    }

    /// <summary>
    /// General Date text drops fractions of a second the way VarBstrFromDate does: more
    /// than half a second rounds up, carrying into the next day, and half a second or
    /// less rounds down.
    /// </summary>
    /// <param name="millisecond">The milliseconds past 2020-01-31 at <paramref name="hour"/>:<paramref name="minute"/>:<paramref name="second"/>.</param>
    /// <param name="hour">The hour.</param>
    /// <param name="minute">The minute.</param>
    /// <param name="second">The second.</param>
    /// <param name="expected">The expected text.</param>
    [Theory]
    [InlineData(400, 0, 0, 0, "1/31/2020")]
    [InlineData(500, 0, 0, 0, "1/31/2020")]
    [InlineData(600, 0, 0, 0, "1/31/2020 12:00:01 AM")]
    [InlineData(600, 6, 0, 0, "1/31/2020 6:00:01 AM")]
    [InlineData(600, 23, 59, 59, "2/1/2020")]
    public void DateToText_RoundsToTheNearestSecond(int millisecond, int hour, int minute, int second, string expected)
    {
        var value = new DateTime(2020, 1, 31, hour, minute, second, millisecond);

        object result = EvaluateDeclared("CStr([T])", typeof(string), ("T", typeof(DateTime), value));

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("de-DE")]
    [InlineData("ja-JP")]
    [InlineData("en-GB")]
    public void DateToText_IgnoresTheCurrentCulture(string cultureName)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo previousUi = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(cultureName);
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(cultureName);

            Assert.Equal("1/31/2020 6:00:00 AM", EvaluateDates("[D] & \"\"", typeof(string)));
            Assert.Equal("1/31/2020 1:05:09 PM", EvaluateDates("CStr(#2020-01-31 13:05:09#)", typeof(string)));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
            CultureInfo.CurrentUICulture = previousUi;
        }
    }

    /// <summary>
    /// Text with a time and no date, and a <c>#...#</c> literal like it, is a time on day
    /// 0 (1899-12-30), as in VBA. This was measured with VBScript, whose date conversions
    /// are VBA's, under LCID 1033. So a day-0 date written as General Date text
    /// (<c>6:00:00 AM</c>) reads back as day 0. It used to take today's date.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="expected">The expected text.</param>
    [Theory]
    [InlineData("CStr(CDate(CStr(CDate(0.25))))", "6:00:00 AM")]
    [InlineData("CStr(CDate(CStr(CDate(0))))", "12:00:00 AM")]
    [InlineData("CStr(CDate(\"6:00:00 AM\"))", "6:00:00 AM")]
    [InlineData("CStr(CDate(\"18:30\"))", "6:30:00 PM")]
    [InlineData("CStr(CDate(\"6:00 PM\") + 1)", "12/31/1899 6:00:00 PM")]
    [InlineData("CStr(#6:00#)", "6:00:00 AM")]
    [InlineData("CStr(Year(CStr(CDate(0.25))))", "1899")]
    [InlineData("CStr(Day(\"6:00:00 AM\"))", "30")]
    public void TimeOnlyText_IsOnDayZero(string expression, string expected)
    {
        object result = EvaluateDates(expression, typeof(string));

        Assert.Equal(expected, result);
    }

    /// <summary>
    /// A day-0 date, which is how Access stores a time on its own, keeps its date when
    /// an expression turns it into text and back into a date.
    /// </summary>
    /// <param name="expression">The expression, over T = 1899-12-30 06:00, for a Date result.</param>
    [Theory]
    [InlineData("CDate([T] & \"\")")]
    [InlineData("CDate(CStr([T]))")]
    [InlineData("[T] & \"\"")]
    [InlineData("Format([T], \"General Date\")")]
    public void DayZeroDate_ThroughText_KeepsDayZero(string expression)
    {
        var time = new DateTime(1899, 12, 30, 6, 0, 0);

        object result = EvaluateDeclared(expression, typeof(DateTime), ("T", typeof(DateTime), time));

        Assert.Equal(time, result);
    }

    /// <summary>
    /// TimeValue and TimeSerial return a time on day 0, as VBA does (measured with
    /// VBScript). TimeSerial rounds each argument half to even to an Integer and turns
    /// the total into an OLE date serial. So 25 hours is 12/31/1899 1:00 AM, and a
    /// negative total keeps OLE's day-0 reading, so -1 hour is 1:00 AM. Both used to add
    /// today's date.
    /// </summary>
    /// <param name="expression">The expression, over D = 2020-01-31 06:00.</param>
    /// <param name="expected">The expected text.</param>
    [Theory]
    [InlineData("CStr(TimeValue(\"6:00\"))", "6:00:00 AM")]
    [InlineData("CStr(TimeValue(\"18:30:05\"))", "6:30:05 PM")]
    [InlineData("CStr(TimeValue(\"1/31/2020 6:00 PM\"))", "6:00:00 PM")]
    [InlineData("CStr(TimeValue([D]))", "6:00:00 AM")]
    [InlineData("CStr(TimeSerial(6, 0, 0))", "6:00:00 AM")]
    [InlineData("CStr(TimeSerial(0, 0, 0))", "12:00:00 AM")]
    [InlineData("CStr(TimeSerial(6, -15, 0))", "5:45:00 AM")]
    [InlineData("CStr(TimeSerial(0, 75, 0))", "1:15:00 AM")]
    [InlineData("CStr(TimeSerial(25, 0, 0))", "12/31/1899 1:00:00 AM")]
    [InlineData("CStr(TimeSerial(23, 59, 60))", "12/31/1899")]
    [InlineData("CStr(TimeSerial(-1, 0, 0))", "1:00:00 AM")]
    [InlineData("CStr(TimeSerial(-25, 0, 0))", "12/29/1899 1:00:00 AM")]
    [InlineData("CStr(TimeSerial(6.5, 0, 0))", "6:00:00 AM")]
    [InlineData("CStr(TimeSerial(7.5, 0, 0))", "8:00:00 AM")]
    [InlineData("CStr(TimeSerial(0, 0, 2.5))", "12:00:02 AM")]
    [InlineData("CStr(TimeSerial(\"6\", \"30\", 0))", "6:30:00 AM")]
    [InlineData("CStr(TimeSerial(True, 0, 0))", "1:00:00 AM")]
    public void TimeValueAndTimeSerial_AreOnDayZero(string expression, string expected)
    {
        object result = EvaluateDates(expression, typeof(string));

        Assert.Equal(expected, result);
    }

    /// <summary>
    /// A TimeSerial argument outside VBA's Integer range overflows, as in VBA.
    /// </summary>
    /// <param name="expression">The expression.</param>
    [Theory]
    [InlineData("TimeSerial(40000, 0, 0)")]
    [InlineData("TimeSerial(0, -32769, 0)")]
    public void TimeSerial_ArgumentOutsideInteger_Overflows(string expression)
        => Assert.Throws<OverflowException>(() => EvaluateDates(expression, typeof(DateTime)));

    /// <summary>
    /// Time() is the current time on day 0, as VBA's Time is, so it has no date part as
    /// text. It used to be today's date and time, the same as Now().
    /// </summary>
    [Fact]
    public void Time_IsTheCurrentTimeOnDayZero()
    {
        DateTime before = DateTimeOffset.Now.DateTime;
        DateTime time = Assert.IsType<DateTime>(EvaluateDates("Time()", typeof(DateTime)));
        string text = Assert.IsType<string>(EvaluateDates("CStr(Time())", typeof(string)));
        DateTime after = DateTimeOffset.Now.DateTime;

        Assert.Equal(new DateTime(1899, 12, 30), time.Date);
        if (before.Date == after.Date)
        {
            Assert.InRange(time.TimeOfDay, before.TimeOfDay - TimeSpan.FromTicks(before.Ticks % TimeSpan.TicksPerSecond), after.TimeOfDay);
        }

        Assert.DoesNotContain("/", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\"é\" < \"z\"", true)]
    [InlineData("\"É\" = \"é\"", true)]
    [InlineData("\"a\" = \"a \"", false)]
    [InlineData("\"é\" Between \"a\" And \"z\"", true)]
    [InlineData("\"É\" In (\"é\", \"z\")", true)]
    public void TextComparisons_UseAccessSortKeys(string expression, bool expected)
        => Assert.Equal(expected, Assert.IsType<bool>(Evaluate(expression, typeof(bool))));

    private static object EvaluateDates(string expression, Type resultType)
        => EvaluateDeclared(
            expression,
            resultType,
            ("D", typeof(DateTime), new DateTime(2020, 1, 31, 6, 0, 0)),
            ("E", typeof(DateTime), new DateTime(2020, 1, 1)),
            ("D2", typeof(DateTime), new DateTime(2020, 1, 31)));

    private static object Evaluate(string expression, Type resultType, params (string Name, object Value)[] inputs)
        => EvaluateDeclared(expression, resultType, [.. inputs.Select(input => (input.Name, input.Value.GetType(), input.Value))]);

    private static object EvaluateDeclared(string expression, Type resultType, params (string Name, Type ClrType, object Value)[] inputs)
    {
        var tableDef = new TableDef { Columns = [.. inputs.Select(input => new ColumnInfo { Name = input.Name }), new ColumnInfo { Name = "Calc" }] };
        var constraints = new List<ColumnConstraint>();
        object[] values = new object[inputs.Length + 1];
        for (int i = 0; i < inputs.Length; i++)
        {
            constraints.Add(new ColumnConstraint { Name = inputs[i].Name, ClrType = inputs[i].ClrType });
            values[i] = inputs[i].Value;
        }

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
