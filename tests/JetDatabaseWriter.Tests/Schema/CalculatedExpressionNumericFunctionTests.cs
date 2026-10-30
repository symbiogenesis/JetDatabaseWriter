namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks native sign, date-subtype and ordinary Null behavior of numeric functions.</summary>
public sealed class CalculatedExpressionNumericFunctionTests
{
    /// <summary>Gets independently measured native ACE numeric-function results.</summary>
    public static TheoryData<string, string> NativeExpressions { get; } = new()
    {
        { "Sgn(1)", "Integer:1" },
        { "Sgn(-1)", "Integer:-1" },
        { "Sgn(0)", "Integer:0" },
        { "Sgn(True)", "Integer:-1" },
        { "Sgn(False)", "Integer:0" },
        { "Sgn(CByte(1))", "Integer:1" },
        { "Sgn(CSng(-1))", "Integer:-1" },
        { "Sgn(CCur(1))", "Integer:1" },
        { "Sgn(CDbl(1E100))", "Integer:1" },
        { "Sgn(CDbl(-1E100))", "Integer:-1" },
        { "Sgn(\"1E100\")", "Integer:1" },
        { "Sgn(#1899-12-29#)", "Integer:-1" },
        { "Abs(#1899-12-29#)", "Date:12/31/1899" },
        { "Int(#2020-01-01 12:30:00#)", "Date:1/1/2020" },
        { "Fix(#2020-01-01 12:30:00#)", "Date:1/1/2020" },
        { "Int(#1899-12-29 06:00:00#)", "Date:12/28/1899" },
        { "Fix(#1899-12-29 06:00:00#)", "Date:12/29/1899" },
        { "CSng(1.225)*Sgn(1)", "Single:1.225" },
    };

    [Theory]
    [MemberData(nameof(NativeExpressions))]
    public void NumericFunction_RetainsNativeValueAndSubtype(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(Describe(expression))));

    [Theory]
    [InlineData("Abs(Null)")]
    [InlineData("Sgn(Null)")]
    [InlineData("Int(Null)")]
    [InlineData("Fix(Null)")]
    public void NullableNumericFunction_ReturnsOrdinaryNull(string expression)
        => Assert.Same(DBNull.Value, Evaluate(expression));

    [Theory]
    [InlineData("Atn(Null)")]
    [InlineData("Cos(Null)")]
    [InlineData("Exp(Null)")]
    [InlineData("Log(Null)")]
    [InlineData("Sin(Null)")]
    [InlineData("Sqr(Null)")]
    [InlineData("Tan(Null)")]
    [InlineData("CDbl(Null)")]
    public void NonnullableNumericFunction_QueryContextRefusesNull(string expression)
        => Assert.Throws<InvalidCastException>(() => Evaluate(expression));

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_NumericFunctionsMatchNativeQueriesAndCalculatedNulls()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NumericFunctions");
        var expressions = new List<string>();
        foreach (TheoryDataRow<string, string> row in NativeExpressions)
        {
            expressions.Add(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Describe(row.Data.Item1)));
        }

        string nativeExpressions = string.Join(",", expressions);
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
                $db.Execute('CREATE TABLE T (N DOUBLE)')
                foreach ($name in @('Abs','Sgn','Int','Fix')) {
                    $field = $db.TableDefs('T').CreateField('R'+$name,7)
                    $field.Expression = $name + '([N])'
                    $db.TableDefs('T').Fields.Append($field)
                    $rs = $db.OpenRecordset('SELECT ' + $name + '(Null) AS [R]')
                    try { if ($rs.Fields('R').Value -isnot [DBNull]) { throw ('Native query Null changed: ' + $name) } }
                    finally { $rs.Close() }
                }
                $db.Execute('INSERT INTO T (N) VALUES (Null)')
                $rs = $db.OpenRecordset('T')
                try {
                    foreach ($name in @('Abs','Sgn','Int','Fix')) {
                        if ($rs.Fields('R'+$name).Value -isnot [DBNull]) { throw ('Native calculated Null changed: ' + $name) }
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        string[] lines = result.StdOut.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(NativeExpressions.Count, lines.Length);
        foreach (TheoryDataRow<string, string> row in NativeExpressions)
        {
            Assert.Contains(Describe(row.Data.Item1) + "=" + row.Data.Item2, lines);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            int count = await writer.UpdateRowsAsync(
                "T", "N", DBNull.Value, new Dictionary<string, object?> { ["N"] = -1.5d }, TestContext.Current.CancellationToken);
            Assert.Equal(2, count);
        }

        string compacted = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult accepted = AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(
            session.SourcePath,
            compacted,
            """
            $rs = $db.OpenRecordset('T')
            try {
                while (!$rs.EOF) {
                    if ($rs.Fields('RAbs').Value -ne 1.5 -or $rs.Fields('RSgn').Value -ne -1 -or $rs.Fields('RInt').Value -ne -2 -or $rs.Fields('RFix').Value -ne -1) { throw 'Native numeric cache changed' }
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(accepted.ExitCode == 0, accepted.StdOut + accepted.StdErr);
        await using AccessReader reader = await AccessReader.OpenAsync(
            compacted, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        int rows = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(1.5d, row[1]);
            Assert.Equal(-1d, row[2]);
            Assert.Equal(-2d, row[3]);
            Assert.Equal(-1d, row[4]);
            rows++;
        }

        Assert.Equal(2, rows);
    }

    private static string Describe(string expression) => $"TypeName({expression}) & \":\" & CStr({expression})";

    private static object Evaluate(string expression)
        => CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(
            new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
}
