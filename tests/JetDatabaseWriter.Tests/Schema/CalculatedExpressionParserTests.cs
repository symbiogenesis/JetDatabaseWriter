namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Checks the Access parser's literal, name and resource-boundary behavior.</summary>
public sealed class CalculatedExpressionParserTests
{
    [Theory]
    [InlineData("1", "Long")]
    [InlineData(".5", "Decimal")]
    [InlineData("1e-3", "Decimal")]
    [InlineData("1E+3", "Long")]
    public void DecimalLiteral_RetainsNativeSubtype(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate("TypeName(" + expression + ")")));

    [Theory]
    [InlineData("'it''s [A1] #today# 5% &H10'", "it's [A1] #today# 5% &H10")]
    [InlineData("\"say \"\"hi\"\" [Null]\"", "say \"hi\" [Null]")]
    [InlineData("'a\"b' & \"c'd\"", "a\"bc'd")]
    [InlineData("  =  mId$ ('ABCD', 2, 2)  ", "BC")]
    [InlineData("iF$(True, 'yes', 'no')", "yes")]
    [InlineData("UCASE$('abc')", "ABC")]
    public void TextLiteralsAndFunctionSuffixes_EvaluateAsAccessSyntax(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(expression)));

    [Theory]
    [InlineData("A1")]
    [InlineData("True")]
    [InlineData("Null")]
    [InlineData("vbTrue")]
    [InlineData("And")]
    [InlineData("__JdwCalcCol0")]
    [InlineData("#Date# % &H10")]
    public void BracketedName_ResolvesFieldInsteadOfLiteralOrCell(string columnName)
        => Assert.Equal(7, Assert.IsType<int>(Evaluate("[" + columnName + "]", columnName)));

    [Theory]
    [InlineData("A1")]
    [InlineData("XFD1048576")]
    public void BareNameResemblingCellReference_ResolvesField(string expression)
        => Assert.Equal(7, Assert.IsType<int>(Evaluate(expression, expression)));

    [Theory]
    [InlineData("'unterminated")]
    [InlineData("\"unterminated")]
    [InlineData(".")]
    [InlineData("1.2.3")]
    [InlineData("1e+")]
    [InlineData("Abs(,1)")]
    [InlineData("Abs(1,)")]
    [InlineData("Abs(1;2)")]
    [InlineData("1 In (1,,2)")]
    [InlineData("1 Is 1")]
    [InlineData("[]")]
    [InlineData("[Missing")]
    [InlineData("[T].")]
    [InlineData("[T].[A].[B]")]
    [InlineData("#not-a-date#")]
    [InlineData("#2020-13-40#")]
    public void MalformedExpression_ThrowsArgumentException(string expression)
        => Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse(expression));

    [Fact]
    public void UnknownFunction_IsRejectedWhenEvaluated()
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse("UnknownAccessFunction(1)", isCalculatedColumn: false);

        Assert.Throws<JetNotSupportedException>(() => plan.Evaluate(Context()));
    }

    [Theory]
    [InlineData("AtN$(0)", 0d)]
    [InlineData("sQrT$(9)", 3d)]
    public void NumericFunctionAliases_KeepCaseAndSuffixNormalization(string expression, double expected)
        => Assert.Equal(expected, Assert.IsType<double>(Evaluate(expression)));

    [Theory]
    [InlineData("Abs()", "ABS", "1", 0)]
    [InlineData("sQrT$(1, 2)", "SQRT", "1", 2)]
    [InlineData("iF$(True, 1)", "IF", "3", 2)]
    [InlineData("Mid$('abcd')", "MID", "2..3", 1)]
    public void FunctionArity_ReportsNormalizedCalledName(string expression, string name, string expectedArity, int actualCount)
        => Assert.Equal(
            $"Calculated-column function '{name}' expects {expectedArity} argument(s), got {actualCount}.",
            Assert.Throws<ArgumentException>(() => Evaluate(expression)).Message);

    [Fact]
    public void Evaluation_PreservesOriginalExpressionText()
    {
        const string expression = "  = IIf(Null Is Null, 'it''s [A1] #literal#', 'unused')  ";
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Calc" }] };
        ColumnConstraint[] constraints =
        [
            new()
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

        Assert.Equal("it's [A1] #literal#", Assert.IsType<string>(values[0]));
        Assert.Equal(expression, constraints[0].CalculationExpression);
    }

    [Fact]
    public void ExpressionLengthLimit_AppliesToSourceText()
    {
        string literal = new('a', CalculatedExpressionLimits.MaxExpressionLength - 2);
        Assert.Equal(literal, Assert.IsType<string>(Evaluate("\"" + literal + "\"")));

        Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse("\"" + literal + "a\""));
    }

    [Fact]
    public void ParenthesisLimit_IgnoresQuotedTextAndBracketedNames()
    {
        string parentheses = new('(', CalculatedExpressionLimits.MaxExpressionNesting + 1);
        Assert.Equal(parentheses, Assert.IsType<string>(Evaluate("'" + parentheses + "'")));
        Assert.Equal(7, Assert.IsType<int>(Evaluate("[" + parentheses + "]", parentheses)));
    }

    [Fact]
    public void ParenthesisLimit_AcceptsBoundaryAndRejectsNextLevel()
    {
        string expression = new string('(', CalculatedExpressionLimits.MaxExpressionNesting) + "1" + new string(')', CalculatedExpressionLimits.MaxExpressionNesting);
        Assert.Equal(1, Assert.IsType<int>(Evaluate(expression)));
        Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse("(" + expression + ")"));
    }

    [Fact]
    public void FunctionArgumentLimit_IsCheckedWhileParsing()
    {
        string arguments = string.Join(",", Enumerable.Repeat("1", CalculatedExpressionLimits.MaxFunctionArguments));
        Assert.NotNull(CalculatedExpressionParser.Parse("Choose(" + arguments + ")"));

        Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse("Choose(" + arguments + ",1)"));
    }

    [Fact]
    public void InListLimit_IncludesComparedOperand()
    {
        string arguments = string.Join(",", Enumerable.Repeat("1", CalculatedExpressionLimits.MaxFunctionArguments - 1));
        Assert.True(Assert.IsType<bool>(Evaluate("1 In (" + arguments + ")")));

        Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse("1 In (" + arguments + ",1)"));
    }

    [Fact]
    public void ExcessiveUnaryNesting_IsRejectedBeforeEvaluation()
    {
        string expression = new string('-', CalculatedExpressionLimits.MaxExpressionNesting + 1) + "1";

        Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse(expression));
    }

    private static object Evaluate(string expression, string columnName = "Value")
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false);
        return plan.Evaluate(Context(columnName));
    }

    private static CalculatedExpressionEvaluationContext Context(string columnName = "Value")
        => new(
            new TableDef { Columns = [new ColumnInfo { Name = columnName }] },
            [new ColumnConstraint { Name = columnName, ClrType = typeof(int) }],
            [7]);
}
