namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Expressions;
using Xunit;

/// <summary>Native Access Text function controls, including Variant and dollar-returning distinctions.</summary>
public sealed class CalculatedTextFunctionTests
{
    [Fact]
    public void UnpairedSurrogatesAndNul_CaseFunctionsRetainCodeUnits()
    {
        string text = string.Create(4098, 0, static (characters, _) =>
        {
            for (int index = 0; index < 2048; index++)
            {
                characters[index * 2] = checked((char)(0xD800 + index));
                characters[(index * 2) + 1] = '|';
            }

            characters[4096] = '\0';
            characters[4097] = '|';
        });
        Assert.Equal(text, Function("UCASE", [text]));
        Assert.Equal(text, Function("LCASE", [text]));
    }

    [Fact]
    public void CollatedSearch_RefusesExcessiveComparisonWork()
    {
        JetLimitationException error = Assert.Throws<JetLimitationException>(() => Function("INSTR", [1, new string('\0', 512), "z", 1]));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
    }

    [Theory]
    [InlineData("LEN")]
    [InlineData("LEFT")]
    [InlineData("RIGHT")]
    [InlineData("MID")]
    [InlineData("UCASE")]
    [InlineData("LCASE")]
    [InlineData("TRIM")]
    [InlineData("STR")]
    [InlineData("STRING")]
    public void NullSource_PropagatesNull(string name)
    {
        object[] arguments = name switch
        {
            "LEFT" or "RIGHT" => [DBNull.Value, -1],
            "MID" => [DBNull.Value, 1, 1],
            "STRING" => [-1, DBNull.Value],
            _ => [DBNull.Value],
        };
        Assert.Equal(DBNull.Value, Function(name, arguments));
    }

    [Theory]
    [InlineData("LEFT$")]
    [InlineData("RIGHT$")]
    [InlineData("MID$")]
    [InlineData("UCASE$")]
    [InlineData("LCASE$")]
    [InlineData("TRIM$")]
    [InlineData("STR$")]
    public void DollarFunction_NullSourceRaisesTypeMismatch(string name)
    {
        object[] arguments = name switch
        {
            "LEFT$" or "RIGHT$" => [DBNull.Value, 1],
            "MID$" => [DBNull.Value, 1, 1],
            _ => [DBNull.Value],
        };
        Assert.Throws<InvalidCastException>(() => Function(name, arguments));
    }

    [Theory]
    [InlineData("Left('abcdef',1.5)", "ab")]
    [InlineData("Left('abcdef',2.5)", "ab")]
    [InlineData("Right('abcdef',2.5)", "ef")]
    [InlineData("Mid('abcdef',1.5,2.5)", "bc")]
    [InlineData("Trim(' \t x \n ')", "\t x \n")]
    [InlineData("UCase('ıßéς')", "ıßÉς")]
    [InlineData("LCase('İŸÉΣ')", "İÿéσ")]
    [InlineData("LCase('ϴẞΩKÅ')", "ϴẞΩKÅ")]
    [InlineData("String(3,65)", "AAA")]
    [InlineData("String(3,256)", "\0\0\0")]
    [InlineData("String(3,-1)", "ÿÿÿ")]
    [InlineData("String(3,True)", "ÿÿÿ")]
    [InlineData("String(2,128)", "€€")]
    [InlineData("String(2.5,'ab')", "aa")]
    [InlineData("Str(True)", "-1")]
    [InlineData("Str(False)", " 0")]
    [InlineData("Str(0.25)", " .25")]
    [InlineData("Str(-0.25)", "-.25")]
    public void NativeTextResults_Match(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(expression)));

    [Theory]
    [InlineData("Asc('€')", 128)]
    [InlineData("Asc('Ÿ')", 159)]
    [InlineData("Asc('中')", 63)]
    [InlineData("InStr(10,'abc','')", 10)]
    [InlineData("InStr(10,'abc','a')", 0)]
    [InlineData("InStr(2,'','a')", 0)]
    [InlineData("InStr(1,'Abc','a',0)", 0)]
    [InlineData("InStr(1,'Abc','a',1)", 1)]
    [InlineData("InStr(1,'café','e',1)", 0)]
    [InlineData("InStr(1,'œ','oe',1)", 1)]
    [InlineData("InStr(1,'Æ','ae',1)", 1)]
    public void NativeNumericTextResults_Match(string expression, int expected)
        => Assert.Equal(expected, Assert.IsType<int>(Evaluate(expression)));

    [Theory]
    [InlineData("Left('abc',-1)")]
    [InlineData("Right('abc',-1)")]
    [InlineData("Mid('abc',0,1)")]
    [InlineData("Mid('abc',1,-1)")]
    [InlineData("String(-1,'a')")]
    [InlineData("String(2,'')")]
    [InlineData("InStr(0,'abc','a')")]
    [InlineData("InStr(1,'abc','a',2)")]
    public void InvalidArguments_RejectRatherThanClamp(string expression)
        => Assert.ThrowsAny<ArgumentException>(() => Evaluate(expression));

    [Theory]
    [InlineData("Left('abc',Null)")]
    [InlineData("Right('abc',Null)")]
    [InlineData("Mid('abc',Null,1)")]
    [InlineData("Mid('abc',1,Null)")]
    [InlineData("String(Null,'a')")]
    public void NullCount_RejectsRatherThanBecomingZero(string expression)
        => Assert.Throws<InvalidCastException>(() => Evaluate(expression));

    private static object Evaluate(string expression)
        => CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], []));

    private static object Function(string name, object[] values)
        => CalculatedExpressionFunctionRegistry.Evaluate(name, values.Select(static value => (CalculatedExpressionNode)new CalculatedExpressionValueNode(value)).ToArray(), new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
}
