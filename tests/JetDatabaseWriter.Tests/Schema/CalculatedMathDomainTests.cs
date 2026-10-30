namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Math domain failures and preexisting financial nonfinite values follow native cached statuses.</summary>
public sealed class CalculatedMathDomainTests
{
    private const string NegativeInfinityExpression = "PV(-1,10,-100,1000)";
    private const string NaNExpression = "PMT(-1,10,1000,0,1)";
    private const string PositiveInfinityExpression = "SYD(1E308,0,1000,1)";

    public static TheoryData<string, int> NativeErrors { get; } = new()
    {
        { "Sqr(-1)", 5 },
        { "Log(0)", 5 },
        { "Log(-1)", 5 },
        { "Exp(1000)", 6 },
        { "1E100^10", 6 },
        { "0^-1", 440 },
        { "Sqr(" + NegativeInfinityExpression + ")", 5 },
        { "Log(" + NegativeInfinityExpression + ")", 5 },
        { "Sin(" + NegativeInfinityExpression + ")", 5 },
        { "Cos(" + NegativeInfinityExpression + ")", 5 },
        { "Tan(" + NegativeInfinityExpression + ")", 5 },
        { "Sin(" + PositiveInfinityExpression + ")", 5 },
        { "Cos(" + PositiveInfinityExpression + ")", 5 },
        { "Tan(" + PositiveInfinityExpression + ")", 5 },
        { "(" + NegativeInfinityExpression + ")+1", 6 },
        { "(" + NaNExpression + ")+1", 6 },
        { "(" + NegativeInfinityExpression + ")/1", 11 },
        { "(" + NaNExpression + ")/1", 6 },
        { "(" + PositiveInfinityExpression + ")/(" + NaNExpression + ")", 6 },
        { "(" + NegativeInfinityExpression + ")^0.5", 440 },
        { "(-1)^(" + NaNExpression + ")", 440 },
        { "CDbl(\"Infinity\")", 13 },
        { "CDbl(\"NaN\")", 13 },
        { "Round(CDbl(1.23),23)", 5 },
        { "1/\"Infinity\"", 13 },
        { "1/\"-Infinity\"", 13 },
        { "1/\"1E999\"", 6 },
    };

    public static TheoryData<string, double> NativeValues { get; } = CreateNativeValues();

    [Theory]
    [MemberData(nameof(NativeErrors))]
    public void InvalidMath_StoresMeasuredNativeStatus(string expression, int expectedCode)
        => Assert.Equal(expectedCode, Assert.IsType<CalculatedError>(Evaluate(expression)).NativeCode);

    [Theory]
    [MemberData(nameof(NativeValues))]
    public void ExistingNonfiniteMath_RetainsMeasuredNativeValue(string expression, double expected)
    {
        double actual = Assert.IsType<double>(Evaluate(expression));
        Assert.Equal(expected, actual);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_MathDomainsAndNonfiniteCompositionMatchNativeCaches()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.MathDomains");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string[] expressions = NativeErrors.Select(static item => item.Data.Item1).Concat(NativeValues.Select(static item => item.Data.Item1)).ToArray();
        string nativeCases = string.Join(",", expressions.Select(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $cases=@({{nativeCases}})
                for ($index=0;$index -lt $cases.Count;$index++) {
                    $tdf=$db.CreateTableDef('T'+$index)
                    $tdf.Fields.Append($tdf.CreateField('Id',4))
                    $db.TableDefs.Append($tdf)
                    $field=$tdf.CreateField('V',7)
                    $field.Expression=$cases[$index]
                    $tdf.Fields.Append($field)
                    $db.Execute('INSERT INTO T'+$index+' (Id) VALUES (1)',128)
                }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        var nativeRows = new List<object>();
        await using (AccessReader reader = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < expressions.Length; index++)
            {
                await foreach (object[] row in reader.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken))
                {
                    nativeRows.Add(row[1]);
                }
            }
        }

        Assert.Equal(expressions.Length, nativeRows.Count);
        for (int index = 0; index < NativeErrors.Count; index++)
        {
            int? actual = Assert.IsType<CalculatedError>(nativeRows[index]).NativeCode;
            Assert.True(actual == NativeErrors.ElementAt(index).Data.Item2, expressions[index] + " native status " + actual);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < expressions.Length; index++)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("T" + index, "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
            }
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        for (int index = 0; index < expressions.Length; index++)
        {
            await foreach (object[] row in reopened.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken))
            {
                if (nativeRows[index] is double expected)
                {
                    Assert.True(BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(Assert.IsType<double>(row[1])), expressions[index] + " differs from native cache bits");
                }
                else
                {
                    Assert.Equal(nativeRows[index], row[1]);
                }
            }
        }
    }

    private static TheoryData<string, double> CreateNativeValues()
    {
        var data = new TheoryData<string, double>
        {
            { "Exp(-1000)", 0d },
            { "Sgn(" + NegativeInfinityExpression + ")", -1d },
            { "Sgn(" + PositiveInfinityExpression + ")", 1d },
            { "Sgn(" + NaNExpression + ")", 0d },
            { "Atn(" + NegativeInfinityExpression + ")", -Math.PI / 2d },
            { "Atn(" + PositiveInfinityExpression + ")", Math.PI / 2d },
            { "Exp(" + NegativeInfinityExpression + ")", 0d },
            { "Exp(" + PositiveInfinityExpression + ")", double.PositiveInfinity },
            { "Abs(" + NegativeInfinityExpression + ")", double.PositiveInfinity },
            { "Sqr(" + PositiveInfinityExpression + ")", double.PositiveInfinity },
            { "Log(" + PositiveInfinityExpression + ")", double.PositiveInfinity },
            { "1/(" + NegativeInfinityExpression + ")", -0d },
            { "0/(" + NegativeInfinityExpression + ")", -0d },
            { "(-2)^(" + PositiveInfinityExpression + ")", double.PositiveInfinity },
            { "(-2)^(" + NegativeInfinityExpression + ")", 0d },
            { "(" + NaNExpression + ")^0", 1d },
            { "1^(" + NaNExpression + ")", 1d },
            { "(" + PositiveInfinityExpression + ")^0", 1d },
            { "+(" + PositiveInfinityExpression + ")", double.PositiveInfinity },
            { "+(" + NegativeInfinityExpression + ")", double.NegativeInfinity },
            { "+(" + NaNExpression + ")", double.NaN },
            { "-(" + PositiveInfinityExpression + ")", double.NegativeInfinity },
            { "-(" + NegativeInfinityExpression + ")", double.PositiveInfinity },
            { "-(" + NaNExpression + ")", double.NaN },
        };
        foreach (string function in new[] { "Abs", "Int", "Fix", "Round", "CDbl", "Sqr", "Log", "Exp", "Sin", "Cos", "Tan", "Atn" })
        {
            data.Add(function + "(" + NaNExpression + ")", double.NaN);
        }

        foreach (string function in new[] { "Int", "Fix", "Round", "CDbl" })
        {
            data.Add(function + "(" + NegativeInfinityExpression + ")", double.NegativeInfinity);
            data.Add(function + "(" + PositiveInfinityExpression + ")", double.PositiveInfinity);
        }

        return data;
    }

    private static object Evaluate(string expression)
    {
        var definition = new TableDef { Columns = [new ColumnInfo { Name = "V", Type = ColumnType.DoubleType, ExtraFlags = 0xC0, CalculatedResultType = ColumnType.DoubleType }] };
        ColumnConstraint[] constraints = [new() { Name = "V", IsCalculated = true, ClrType = typeof(double), CalculatedResultType = ColumnType.DoubleType, CalculationExpression = expression }];
        return new CalculatedExpressionEvaluationContext(definition, constraints, [DBNull.Value]).EvaluateColumn(0);
    }
}
