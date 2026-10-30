namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks Currency identity and arithmetic against native ACE expression and calculated-field controls.</summary>
public sealed class CalculatedExpressionCurrencyTests
{
    /// <summary>Gets native ACE subtype and value controls outside table-calculated declarations.</summary>
    public static TheoryData<string, string> NativeExpressions { get; } = new()
    {
        { "CCur(1.23445)", "Currency:1.2344" },
        { "CCur(1.23455)", "Currency:1.2346" },
        { "CCur(1.23465)", "Currency:1.2346" },
        { "CCur(1.23475)", "Currency:1.2348" },
        { "CCur(True)", "Currency:-1" },
        { "CCur(1.2345)+CByte(2)", "Currency:3.2345" },
        { "CInt(2)-CCur(1.2345)", "Currency:0.7655" },
        { "CCur(1.2345)*CLng(2)", "Currency:2.469" },
        { "True+CCur(1.2345)", "Currency:0.2345" },
        { "CCur(1.2345)*True", "Currency:-1.2345" },
        { "CCur(1.2345)+CSng(2.3456)", "Currency:3.5801" },
        { "CDbl(2.3456)-CCur(1.2345)", "Currency:1.1111" },
        { "CCur(0.0001)+CDbl(0.00005)", "Currency:0.0002" },
        { "CCur(0.0001)-CDbl(0.00005)", "Currency:0" },
        { "CCur(0.0001)+CDbl(0.00015)", "Currency:0.0002" },
        { "CCur(0.0001)-CDbl(0.00015)", "Currency:0" },
        { "CCur(0.0001)+\"0.00005\"", "Currency:0.0001" },
        { "CCur(1.2345)*CSng(2.3456)", "Double:2.89564306390285" },
        { "CDbl(2.3456)*CCur(1.2345)", "Double:2.8956432" },
        { "CCur(1.2345)*\"2.3456\"", "Double:2.8956432" },
        { "CCur(0)*CDbl(1E100)", "Double:0" },
        { "CCur(1.2345)+CCur(2.3456)", "Currency:3.5801" },
        { "CCur(2.3456)-CCur(1.2345)", "Currency:1.1111" },
        { "CCur(1.2345)*CCur(2.3456)", "Currency:2.8956" },
        { "CCur(0.0001)*CCur(0.5)", "Currency:0" },
        { "CCur(0.0003)*CCur(0.5)", "Currency:0.0002" },
        { "CCur(0.0005)*CCur(0.5)", "Currency:0.0002" },
        { "CCur(1.2345)/CByte(2)", "Double:0.61725" },
        { "CCur(1.2345)/CCur(2.3456)", "Double:0.526304570259209" },
        { "CCur(2.3456)/CCur(1.2345)", "Double:1.90004050222762" },
        { "Abs(CCur(-1.2345))", "Currency:1.2345" },
        { "Int(CCur(-1.2345))", "Currency:-2" },
        { "Fix(CCur(-1.2345))", "Currency:-1" },
        { "Round(CCur(1.2345),2)", "Currency:1.23" },
        { "Round(CCur(1.2345),29)", "Currency:1.2345" },
        { "-CCur(1.2345)", "Currency:-1.2345" },
        { "+CCur(1.2345)", "Currency:1.2345" },
        { "CCur(\"922337203685477.5807\")", "Currency:922337203685477.5807" },
        { "CCur(\"-922337203685477.5808\")", "Currency:-922337203685477.5808" },
        { "Int(CCur(\"922337203685477.5807\"))", "Currency:922337203685477" },
        { "Fix(CCur(\"-922337203685477.5808\"))", "Currency:-922337203685477" },
    };

    /// <summary>Gets independent native Decimal-field and Currency-field result controls.</summary>
    public static TheoryData<string, string> NativeDecimalExpressions { get; } = new()
    {
        { "[C]+[N]", "Decimal:3.5801789012" },
        { "[N]+[C]", "Decimal:3.5801789012" },
        { "[C]-[N]", "Decimal:-1.1111789012" },
        { "[N]-[C]", "Decimal:1.1111789012" },
        { "[C]*[N]", "Decimal:2.8957406035314" },
        { "[N]*[C]", "Decimal:2.8957406035314" },
        { "[C]/[N]", "Decimal:0.5262868670423968768910031751" },
        { "[N]/[C]", "Decimal:1.9001044157148643175374645606" },
    };

    [Theory]
    [MemberData(nameof(NativeExpressions))]
    public void CurrencyExpression_RetainsNativeSubtypeAndValue(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(Describe(expression))));

    [Theory]
    [InlineData("CCur(\"922337203685477.5808\")")]
    [InlineData("CCur(\"-922337203685477.5809\")")]
    [InlineData("CCur(\"922337203685477.5807\")+CCur(\"0.0001\")")]
    [InlineData("CCur(\"-922337203685477.5808\")-CCur(\"0.0001\")")]
    [InlineData("CCur(1000000000)*CCur(1000000000)")]
    [InlineData("CCur(0)+CDbl(1E100)")]
    [InlineData("Abs(CCur(\"-922337203685477.5808\"))")]
    [InlineData("Int(CCur(\"-922337203685477.5808\"))")]
    [InlineData("Round(CCur(\"922337203685477.5807\"))")]
    [InlineData("-CCur(\"-922337203685477.5808\")")]
    public void CurrencyOverflow_Refuses(string expression)
        => Assert.Throws<OverflowException>(() => Evaluate(expression));

    [Theory]
    [MemberData(nameof(NativeDecimalExpressions))]
    public void DecimalMixing_PreservesDecimalPrecision(string expression, string expected)
    {
        ArgumentNullException.ThrowIfNull(expression);
        string substituted = expression.Replace("[C]", "CCur(1.2345)", StringComparison.Ordinal).Replace("[N]", "CDec(2.3456789012)", StringComparison.Ordinal);
        Assert.Equal(expected, Assert.IsType<string>(Evaluate(Describe(substituted))));
    }

    [Fact]
    public void CurrencyConversionAndPredicates_KeepNumericMeaning()
    {
        Assert.Equal(6, Evaluate("VarType(CCur(1))"));
        Assert.Equal(14, Evaluate("VarType(CDec(CCur(1)))"));
        Assert.Equal("Currency", Evaluate("TypeName(CVar(CCur(1)))"));
        Assert.Equal(1.2345d, Assert.IsType<double>(Evaluate("CDbl(CCur(1.2345))")));
        Assert.Equal(1.2345f, Assert.IsType<float>(Evaluate("CSng(CCur(1.2345))")));
        Assert.Equal((short)2, Evaluate("CInt(CCur(1.5))"));
        Assert.Equal((byte)2, Evaluate("CByte(CCur(1.5))"));
        Assert.Equal(true, Evaluate("IsNumeric(CCur(1))"));
        Assert.Equal(true, Evaluate("CCur(1.25)=CDec(1.25)"));
        Assert.Equal(true, Evaluate("CBool(CCur(1))"));
        Assert.Equal(false, Evaluate("CBool(CCur(0))"));
        Assert.Equal(1, Evaluate("CCur(3) And 1"));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_CurrencyExpressionsMatchIndependentNativeQueries()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CurrencyExpressions");
        var expressions = new List<string>();
        foreach (TheoryDataRow<string, string> row in NativeExpressions)
        {
            expressions.Add(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Describe(row.Data.Item1)));
        }

        string nativeExpressions = string.Join(",", expressions);
        var decimalExpressions = new List<string>();
        foreach (TheoryDataRow<string, string> row in NativeDecimalExpressions)
        {
            decimalExpressions.Add(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Describe(row.Data.Item1)));
        }

        string nativeDecimalExpressions = string.Join(",", decimalExpressions);
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                foreach ($expression in @({{nativeExpressions}})) {
                    $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                    try { Write-Output ($expression + '=' + $rs.Fields('R').Value) }
                    finally { $rs.Close() }
                }
            } finally { $db.Close() }
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.ACE.OLEDB.12.0;Data Source=' + {{path}})
                $connection.Execute('CREATE TABLE T (N DECIMAL(28,10),C CURRENCY)') | Out-Null
                $connection.Execute('INSERT INTO T VALUES (2.3456789012,1.2345)') | Out-Null
            } finally {
                $connection.Close()
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null
            }
            $db = $engine.OpenDatabase({{path}})
            try {
                foreach ($expression in @({{nativeDecimalExpressions}})) {
                    $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R] FROM T')
                    try { Write-Output ('DECIMAL|' + $expression + '=' + $rs.Fields('R').Value) }
                    finally { $rs.Close() }
                }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        string[] lines = result.StdOut.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(NativeExpressions.Count + NativeDecimalExpressions.Count, lines.Length);
        foreach (TheoryDataRow<string, string> row in NativeExpressions)
        {
            Assert.Contains(Describe(row.Data.Item1) + "=" + row.Data.Item2, lines);
        }

        foreach (TheoryDataRow<string, string> row in NativeDecimalExpressions)
        {
            Assert.Contains("DECIMAL|" + Describe(row.Data.Item1) + "=" + row.Data.Item2, lines);
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_CalculatedCurrencyRecomputesAndCompactsWithNativeCaches()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedCurrency");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE T (Amount CURRENCY, Other CURRENCY)')
                $tdf = $db.TableDefs('T')
                foreach ($case in @(@('Product',5,'[Amount]*[Other]'),@('Ratio',7,'[Amount]/[Other]'),@('Absolute',5,'Abs([Amount])'),@('Integral',5,'Int([Amount])'),@('Fixed',5,'Fix([Amount])'),@('Rounded',5,'Round([Amount],2)'),@('Promoted',7,'CDbl([Amount])'),@('Text',10,'[Amount] & ""'),@('ScaledProduct',5,'([Amount]*[Other])*10000'))) {
                    $field = if ($case[1] -eq 10) { $tdf.CreateField($case[0],10,80) } else { $tdf.CreateField($case[0],$case[1]) }
                    $field.Expression = $case[2]
                    $tdf.Fields.Append($field)
                }
                $db.Execute('INSERT INTO T (Amount,Other) VALUES (1.2345,2.3456)')
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await AssertNativeRowAsync(session.SourcePath, updated: false);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            int count = await writer.UpdateRowsAsync(
                "T", "Amount", 1.2345m, new Dictionary<string, object?> { ["Amount"] = -0.0003m, ["Other"] = 0.5m }, TestContext.Current.CancellationToken);
            Assert.Equal(1, count);
        }

        await AssertNativeRowAsync(session.SourcePath, updated: true);
        string compacted = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult accepted = AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(
            session.SourcePath,
            compacted,
            """
            $rs = $db.OpenRecordset('T')
            try {
                if ($rs.Fields('Product').Value -ne [decimal]'-0.0002') { throw 'Native cached Currency product changed' }
                if ($rs.Fields('Ratio').Value -ne [double]'-0.0006') { throw 'Native cached Currency division changed' }
                if ($rs.Fields('Absolute').Value -ne [decimal]'0.0003') { throw 'Native cached Currency Abs changed' }
                if ($rs.Fields('Integral').Value -ne -1 -or $rs.Fields('Fixed').Value -ne 0) { throw 'Native Currency integral result changed' }
                if ($rs.Fields('ScaledProduct').Value -ne -2) { throw 'Native intermediate Currency rounding changed' }
                $rs.Edit()
                $rs.Fields('Other').Value = [decimal]'0.5'
                $rs.Update()
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(accepted.ExitCode == 0, accepted.StdOut + accepted.StdErr);
        await AssertNativeRowAsync(compacted, updated: true);
    }

    private static async Task AssertNativeRowAsync(string path, bool updated)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(updated ? -0.0003m : 1.2345m, row[0]);
            Assert.Equal(updated ? 0.5m : 2.3456m, row[1]);
            Assert.Equal(updated ? -0.0002m : 2.8956m, row[2]);
            Assert.Equal(updated ? -0.0006d : 1.2345d / 2.3456d, row[3]);
            Assert.Equal(updated ? 0.0003m : 1.2345m, row[4]);
            Assert.Equal(updated ? -1m : 1m, row[5]);
            Assert.Equal(updated ? 0m : 1m, row[6]);
            Assert.Equal(updated ? 0m : 1.23m, row[7]);
            Assert.Equal(updated ? -0.0003d : 1.2345d, row[8]);
            Assert.Equal(updated ? "-0.0003" : "1.2345", row[9]);
            Assert.Equal(updated ? -2m : 28956m, row[10]);
            count++;
        }

        Assert.Equal(1, count);
    }

    private static string Describe(string expression) => $"TypeName({expression}) & \":\" & CStr({expression})";

    private static object Evaluate(string expression)
        => CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(
            new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
}
