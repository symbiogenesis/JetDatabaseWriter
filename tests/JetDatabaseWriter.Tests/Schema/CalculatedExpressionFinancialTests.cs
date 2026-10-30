namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks financial functions against independent native ACE queries and calculated caches.</summary>
public sealed class CalculatedExpressionFinancialTests
{
    /// <summary>Gets measured native financial results, including nonfinite Double values.</summary>
    public static TheoryData<string, double> NativeCases { get; } = new()
    {
        { "FV(0.05,10,-100,1000)", -371.105373222558d },
        { "FV(0,10,-100,1000)", 0d },
        { "FV(-0.05,10,-100,1000)", 203.78918228486384d },
        { "FV(-2,3,-100,1000)", 1100d },
        { "FV(0.05,10,-100,1000,1)", -308.2159105448138d },
        { "FV(0.05,10,-100,1000,0.4)", -371.105373222558d },
        { "FV(0.05,10,-100,1000,0.5)", -371.105373222558d },
        { "FV(0.05,10,-100,1000,1.5)", -308.2159105448138d },
        { "PV(0.05,10,-100,1000)", 158.26023937772254d },
        { "PV(0,10,-100,1000)", 0d },
        { "PV(0.05,10,-100,1000,1)", 196.86891402364662d },
        { "PMT(0.05,10,1000)", -129.5045749654566d },
        { "PMT(0,10,1000)", -100d },
        { "NPer(0.05,-100,1000)", 14.206699082890465d },
        { "NPer(0,-100,1000)", 10d },
        { "NPer(0.05,100,1000,-100000)", 72.27605368103053d },
        { "IPmt(0.05,1,10,1000)", -50d },
        { "IPmt(0.05,2,10,1000,0,1)", -43.833115477835406d },
        { "IPmt(0.05,1,10,1000,0,1)", 0d },
        { "IPmt(0.05,10.5,10,1000)", -3.1210508094987177d },
        { "IPmt(0.05,2,10,1000,0,0.5)", -46.02477125172717d },
        { "PPmt(0.05,1,10,1000)", -79.50457496545661d },
        { "PPmt(0.05,2,10,1000,0,1)", -79.50457496545661d },
        { "DDB(1000,100,5,1)", 400d },
        { "DDB(1000,100,5,2)", 240d },
        { "DDB(1000,100,5,5)", 29.599999999999987d },
        { "DDB(1000,100,2,1)", 900d },
        { "DDB(1000,100,2,2)", 0d },
        { "DDB(1000,100,1,1)", 900d },
        { "DDB(-1,0,5,1)", 0d },
        { "DDB(0,100,5,1)", 0d },
        { "DDB(1000,2000,5,1)", -1000d },
        { "SLN(1000,100,5)", 180d },
        { "SLN(1000,100,-5)", -180d },
        { "SYD(1000,100,5,1)", 300d },
        { "SYD(1000,100,5,5)", 60d },
        { "Rate(12,-100,1000)", 0.02922854076913396d },
        { "Rate(10,-100,1000,0,1)", 4.189053923223741E-09d },
        { "Rate(10,-100,1000,0,0,0)", 0d },
        { "PMT(1E100,1,1E200)", -1E300d },
        { "PV(-1,10,-100,1000)", double.NegativeInfinity },
        { "PMT(-1,10,1000,0,1)", double.NaN },
        { "FV(1E100,1,1E300)", double.NegativeInfinity },
        { "PV(1E100,1,1E300)", double.NegativeInfinity },
        { "SYD(1E308,0,1000,1)", double.PositiveInfinity },
        { "DDB(1E308,0,10,1)", 1E308d },
    };

    /// <summary>Gets financial domain errors independently measured in native ACE.</summary>
    public static TheoryData<string> NativeInvalidCases { get; } =
    [
        "PMT(0.05,0,1000)", "NPer(0,0,1000)", "NPer(-1,-100,1000)",
        "NPer(0.05,100,1000,100000)", "IPmt(0.05,0,10,1000)", "IPmt(0.05,11,10,1000)",
        "PPmt(0.05,0,10,1000)", "DDB(1000,100,5,1,0)", "DDB(1000,-1,5,1)",
        "DDB(1000,100,5,0)", "DDB(1000,100,5,6)", "SLN(1000,100,0)",
        "SYD(1000,-1,5,1)", "SYD(1000,100,5,0)", "SYD(1000,100,5,6)",
        "Rate(10,-100,1000)", "Rate(0,-100,1000)", "Rate(10,100,1000)", "Rate(10,-100,1000,0,0,-1)",
    ];

    [Theory]
    [MemberData(nameof(NativeCases))]
    public void FinancialExpression_MatchesNativeResult(string expression, double expected)
        => AssertClose(expected, Assert.IsType<double>(Evaluate(expression)));

    [Theory]
    [MemberData(nameof(NativeInvalidCases))]
    public void FinancialQuery_InvalidDomainRefuses(string expression)
        => Assert.Throws<ArgumentException>(() => Evaluate(expression));

    [Theory]
    [InlineData("FV(Null,10,-100,1000)")]
    [InlineData("PV(Null,10,-100,1000)")]
    [InlineData("PMT(Null,10,1000)")]
    [InlineData("NPer(Null,-100,1000)")]
    [InlineData("IPmt(Null,1,10,1000)")]
    [InlineData("PPmt(Null,1,10,1000)")]
    [InlineData("DDB(1000,Null,5,1)")]
    [InlineData("SLN(1000,Null,5)")]
    [InlineData("SYD(1000,Null,5,1)")]
    [InlineData("Rate(10,Null,1000)")]
    public void FinancialQuery_NullArgumentRefuses(string expression)
        => Assert.Throws<InvalidCastException>(() => Evaluate(expression));

    [Fact]
    public void FinancialDue_RequiresNativeIntegerRange()
        => Assert.Throws<OverflowException>(() => Evaluate("FV(0.05,10,-100,1000,32768)"));

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_FinancialQueriesMatchIndependentNativeResults()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.FinancialQueries");
        var expressions = new List<string>();
        foreach (TheoryDataRow<string, double> row in NativeCases)
        {
            expressions.Add(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(row.Data.Item1));
        }

        string nativeExpressions = string.Join(",", expressions);
        var invalidExpressions = new List<string>();
        foreach (TheoryDataRow<string> row in NativeInvalidCases)
        {
            invalidExpressions.Add(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(row.Data));
        }

        string nativeInvalidExpressions = string.Join(",", invalidExpressions);
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                foreach ($expression in @({{nativeExpressions}})) {
                    $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                    try { Write-Output ($expression + '|' + [BitConverter]::DoubleToInt64Bits([double]$rs.Fields('R').Value)) }
                    finally { $rs.Close() }
                }
                foreach ($expression in @({{nativeInvalidExpressions}})) {
                    $refused = $false
                    try {
                        $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                        try { $null = $rs.Fields('R').Value }
                        finally { $rs.Close() }
                    } catch { $refused = $true }
                    if (!$refused) { throw ('Native invalid financial domain was accepted: ' + $expression) }
                    Write-Output ('INVALID|' + $expression)
                }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        string[] lines = result.StdOut.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(NativeCases.Count + NativeInvalidCases.Count, lines.Length);
        int index = 0;
        foreach (TheoryDataRow<string, double> row in NativeCases)
        {
            string[] parts = lines[index].Split('|');
            Assert.Equal(row.Data.Item1, parts[0]);
            AssertClose(row.Data.Item2, BitConverter.Int64BitsToDouble(long.Parse(parts[1], CultureInfo.InvariantCulture)));
            index++;
        }

        foreach (TheoryDataRow<string> row in NativeInvalidCases)
        {
            Assert.Contains("INVALID|" + row.Data, lines);
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_FinancialCalculatedCachesSurviveWriterRecomputeAndCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.FinancialCalculated");
        var expressions = new List<string>();
        var expected = new List<double>();
        foreach (TheoryDataRow<string, double> row in NativeCases)
        {
            if (double.IsFinite(row.Data.Item2))
            {
                expressions.Add(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(row.Data.Item1));
                expected.Add(row.Data.Item2);
            }
        }

        string nativeExpressions = string.Join(",", expressions);
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE T (N DOUBLE)')
                $tdf = $db.TableDefs('T')
                $index = 0
                foreach ($expression in @({{nativeExpressions}})) {
                    $field = $tdf.CreateField('F'+$index,7)
                    $field.Expression = $expression
                    $tdf.Fields.Append($field)
                    $index++
                }
                $db.Execute('INSERT INTO T (N) VALUES (1)')
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await AssertCachedRowAsync(session.SourcePath, expected);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "N", 1d, new Dictionary<string, object?> { ["N"] = 2d }, TestContext.Current.CancellationToken));
        }

        await AssertCachedRowAsync(session.SourcePath, expected);
        string compacted = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult accepted = AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(
            session.SourcePath,
            compacted,
            $$"""
            $rs = $db.OpenRecordset('T')
            try {
                $index = 0
                foreach ($expression in @({{nativeExpressions}})) {
                    $control = $db.OpenRecordset('SELECT ' + $expression + ' AS R')
                    try {
                        $native = [double]$control.Fields('R').Value
                        $cached = [double]$rs.Fields('F'+$index).Value
                        if ([Math]::Abs($native-$cached) -gt 0.000000000001*[Math]::Max([double]1,[Math]::Abs($native))) { throw ('Native financial cache changed: '+$expression) }
                    } finally { $control.Close() }
                    $index++
                }
                $rs.Edit()
                $rs.Fields('N').Value = 3
                $rs.Update()
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(accepted.ExitCode == 0, accepted.StdOut + accepted.StdErr);
        await AssertCachedRowAsync(compacted, expected);
    }

    private static async Task AssertCachedRowAsync(string path, List<double> expected)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        int rows = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < expected.Count; index++)
            {
                AssertClose(expected[index], Assert.IsType<double>(row[index + 1]));
            }

            rows++;
        }

        Assert.Equal(1, rows);
    }

    private static void AssertClose(double expected, double actual)
    {
        if (!double.IsFinite(expected))
        {
            Assert.Equal(expected, actual);
            return;
        }

        Assert.True(double.IsFinite(actual));
        Assert.InRange(Math.Abs(actual - expected), 0d, 1E-12d * Math.Max(1d, Math.Abs(expected)));
    }

    private static object Evaluate(string expression)
        => CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(
            new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
}
