namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks narrow integral arithmetic against independent DAO3.6 Jet4 queries.</summary>
public sealed class CalculatedExpressionIntegralSubtypeTests
{
    /// <summary>Gets independently measured native subtype and value controls.</summary>
    public static TheoryData<string, string> NativeExpressions { get; } = new()
    {
        { "CByte(2)+CByte(2)", "Integer:4" },
        { "CByte(2)-CByte(2)", "Integer:0" },
        { "CByte(2)*CByte(2)", "Integer:4" },
        { "CByte(255)+CByte(1)", "Integer:256" },
        { "CByte(0)-CByte(1)", "Integer:-1" },
        { "CByte(255)*CByte(255)", "Long:65025" },
        { "CInt(2)+CByte(2)", "Integer:4" },
        { "CByte(2)-CInt(2)", "Integer:0" },
        { "CInt(2)*CByte(2)", "Integer:4" },
        { "CInt(32767)+CInt(1)", "Long:32768" },
        { "CInt(-32768)-CInt(1)", "Long:-32769" },
        { "CInt(32767)*CInt(32767)", "Long:1073676289" },
        { "CInt(-32768)*CInt(-1)", "Long:32768" },
        { "CByte(255)+CInt(32767)", "Long:33022" },
        { "CByte(0)+True", "Integer:-1" },
        { "True-CByte(2)", "Integer:-3" },
        { "CInt(2)*True", "Integer:-2" },
        { "True+True", "Integer:-2" },
        { "True-True", "Integer:0" },
        { "True*True", "Integer:1" },
        { "False+False", "Integer:0" },
        { "False-False", "Integer:0" },
        { "False*False", "Integer:0" },
        { "CByte(2)+CLng(2)", "Long:4" },
        { "CLng(2)-CInt(2)", "Long:0" },
        { "True*CLng(2)", "Long:-2" },
        { "+CByte(255)", "Integer:255" },
        { "-CByte(255)", "Integer:-255" },
        { "+True", "Integer:-1" },
        { "-True", "Integer:1" },
        { "+False", "Integer:0" },
        { "-False", "Integer:0" },
        { "+CInt(-32768)", "Integer:-32768" },
        { "-CInt(-2)", "Integer:2" },
        { "Abs(CByte(255))", "Integer:255" },
        { "Abs(CInt(-2))", "Integer:2" },
        { "Abs(CInt(-32768))", "Long:32768" },
        { "Abs(True)", "Integer:1" },
        { "Abs(False)", "Integer:0" },
        { "Int(CByte(255))", "Integer:255" },
        { "Fix(CByte(255))", "Integer:255" },
        { "Int(CInt(-32768))", "Integer:-32768" },
        { "Fix(CInt(-32768))", "Integer:-32768" },
        { "Int(True)", "Integer:-1" },
        { "Fix(True)", "Integer:-1" },
        { "Int(False)", "Integer:0" },
        { "Fix(False)", "Integer:0" },
    };

    [Theory]
    [MemberData(nameof(NativeExpressions))]
    public void IntegralExpression_RetainsNativeSubtypeAndValue(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(Describe(expression))));

    [Fact]
    public void IntegralArithmetic_PreservesNull()
    {
        Assert.Same(DBNull.Value, Evaluate("CByte(1)+Null"));
        Assert.Same(DBNull.Value, Evaluate("Null-CInt(1)"));
        Assert.Same(DBNull.Value, Evaluate("True*Null"));
        Assert.Same(DBNull.Value, Evaluate("+Null"));
        Assert.Same(DBNull.Value, Evaluate("-Null"));
    }

    [Fact]
    public async Task CalculatedIntegralArithmetic_UpdateRecomputesPromotedFloatingResult()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Amount", typeof(short)),
                    new ColumnDefinition("Adjustment", typeof(byte)),
                    new ColumnDefinition("Rate", typeof(float)),
                    new ColumnDefinition("Product", typeof(double))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Rate]*([Amount]+[Adjustment])",
                    },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [(short)2, (byte)2, 1.225f, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.Equal(4617202928078290944L, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(row[3])));
            }
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            int updated = await writer.UpdateRowsAsync(
                "T", "Amount", (short)2, new Dictionary<string, object?> { ["Amount"] = short.MaxValue, ["Adjustment"] = byte.MaxValue }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        stream.Position = 0;
        await using AccessReader reopened = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reopened.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(4675792487365312512L, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(row[3])));
            count++;
        }

        Assert.Equal(1, count);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_IntegralExpressionsMatchIndependentNativeQueries()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.IntegralSubtype", databaseExtension: ".mdb");
        var expressions = new List<string>();
        foreach (TheoryDataRow<string, string> row in NativeExpressions)
        {
            expressions.Add(Describe(row.Data.Item1));
        }

        string nativeExpressions = string.Join(",", expressions.ConvertAll(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            try {
                foreach ($expression in @({{nativeExpressions}})) {
                    $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                    try { Write-Output ($expression + '=' + $rs.Fields('R').Value) }
                    finally { $rs.Close() }
                }
                $index = 0
                foreach ($expression in @('CSng(1.225)*(CInt(2)+CByte(2))','CSng(1.225)*(CInt(32767)+CByte(255))')) {
                    $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                    try { Write-Output ('BITS|' + $index + '|' + [BitConverter]::DoubleToInt64Bits([double]$rs.Fields('R').Value)) }
                    finally { $rs.Close() }
                    $index++
                }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == 0, result.StandardOutput + result.StandardError);
        string[] lines = result.StandardOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(NativeExpressions.Count + 2, lines.Length);
        Assert.Contains("BITS|0|4617202928078290944", lines);
        Assert.Contains("BITS|1|4675792487365312512", lines);
        foreach (TheoryDataRow<string, string> row in NativeExpressions)
        {
            Assert.Contains(Describe(row.Data.Item1) + "=" + row.Data.Item2, lines);
        }
    }

    private static string Describe(string expression) => $"TypeName({expression}) & \":\" & ({expression})";

    private static object Evaluate(string expression)
        => CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(
            new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
}
