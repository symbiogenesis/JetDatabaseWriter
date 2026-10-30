namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks floating arithmetic against native DAO3.6 Jet4 query results.</summary>
public sealed class CalculatedExpressionFloatingSubtypeTests
{
    [Theory]
    [InlineData("CStr(1/3)", "0.333333333333333")]
    [InlineData("TypeName(1/3)", "Double")]
    [InlineData("CStr(CSng(1)/CSng(3))", "0.3333333")]
    [InlineData("TypeName(CSng(1)/CSng(3))", "Single")]
    [InlineData("TypeName(CSng(1)/CInt(3))", "Single")]
    [InlineData("TypeName(CSng(1)/CLng(3))", "Double")]
    [InlineData("TypeName(CSng(1)/CDbl(3))", "Double")]
    [InlineData("TypeName(CByte(1)/CByte(3))", "Double")]
    [InlineData("TypeName(CInt(1)/CInt(3))", "Double")]
    [InlineData("TypeName(CSng(1)+True)", "Single")]
    [InlineData("TypeName(CSng(1)/True)", "Single")]
    [InlineData("TypeName(CSng(1)+\"2\")", "Double")]
    [InlineData("TypeName(CSng(1)/\"3\")", "Double")]
    [InlineData("TypeName(CSng(1)+1)", "Double")]
    [InlineData("TypeName(CSng(1)+CDbl(3))", "Double")]
    [InlineData("TypeName(CSng(1)+CSng(3))", "Single")]
    [InlineData("TypeName(CSng(1)*CSng(3))", "Single")]
    [InlineData("TypeName(CSng(1)-CSng(3))", "Single")]
    [InlineData("TypeName(-CSng(1))", "Single")]
    [InlineData("TypeName(+CSng(1))", "Single")]
    [InlineData("TypeName(Abs(CSng(-1)))", "Single")]
    [InlineData("TypeName(Int(CSng(1.5)))", "Single")]
    [InlineData("TypeName(Fix(CSng(1.5)))", "Single")]
    [InlineData("TypeName(CSng(3.4E38)*CSng(2))", "Double")]
    [InlineData("CStr(CSng(3.4E38)*CSng(2))", "6.79999990428873E+38")]
    [InlineData("CStr(CDbl(1000000000000000)+1)", "1E+15")]
    [InlineData("CStr(CDbl(0.1)+CDbl(0.2))", "0.3")]
    [InlineData("CStr(CDbl(1E-15))", "0.000000000000001")]
    [InlineData("CStr(CDbl(1E-16))", "1E-16")]
    [InlineData("CStr(CSng(1E-5))", "0.00001")]
    [InlineData("CStr(CSng(1E-9))", "1E-09")]
    [InlineData("CStr(CDbl(1.23456789012345E-5))", "1.23456789012345E-05")]
    [InlineData("CStr(-CDbl(0))", "0")]
    public void FloatingExpression_RetainsNativeSubtypeAndText(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(expression)));

    [Theory]
    [InlineData("CStr(CDbl(1.23456789012345E-4))", "1.23456789012345E-04")]
    [InlineData("CStr(CDbl(1.23456789012345E-3))", "1.23456789012345E-03")]
    [InlineData("CStr(CDbl(-1.23456789012345E-4))", "-1.23456789012345E-04")]
    [InlineData("CStr(CDbl(9.99999999999999E-4))", "9.99999999999999E-04")]
    [InlineData("CStr(CDbl(1.234567E-4))", "0.0001234567")]
    [InlineData("CStr(CSng(1.234567E-3))", "1.234567E-03")]
    [InlineData("CStr(CSng(1.234567E-4))", "1.234567E-04")]
    [InlineData("CStr(CSng(1.23456789012345E-4))", "1.234568E-04")]
    [InlineData("CStr(CSng(9.99999999999999E-4))", "0.001")]
    public void NativeNotationBoundary_UsesCompleteFractionalWidth(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(expression)));

    [Theory]
    [InlineData("CDbl(1)/\"Infinity\"")]
    [InlineData("CDbl(1)/\"-Infinity\"")]
    [InlineData("CDbl(1)/\"1E999\"")]
    [InlineData("#2020-01-01#/\"Infinity\"")]
    public void NonfiniteDivisor_RefusesInsteadOfCollapsingToZero(string expression)
        => Assert.Throws<OverflowException>(() => Evaluate(expression));

    [Theory]
    [InlineData("CDbl(1E100)*CDbl(1E100)", 1E200)]
    [InlineData("CDbl(1E100)+CDbl(1E100)", 2E100)]
    [InlineData("-CDbl(1E100)", -1E100)]
    [InlineData("Abs(CDbl(-1E100))", 1E100)]
    [InlineData("Int(CDbl(1E100))", 1E100)]
    [InlineData("Fix(CDbl(-1E100))", -1E100)]
    public void DoubleArithmetic_DoesNotNarrowToDecimal(string expression, double expected)
        => Assert.Equal(expected, Assert.IsType<double>(Evaluate(expression)));

    [Theory]
    [InlineData("CDbl(1)/0")]
    [InlineData("CSng(1)/CSng(0)")]
    public void FloatingDivision_ByZeroRefuses(string expression)
        => Assert.Throws<DivideByZeroException>(() => Evaluate(expression));

    [Theory]
    [InlineData("CDbl(1E308)*CDbl(2)")]
    [InlineData("CDbl(1E308)+CDbl(1E308)")]
    public void DoubleOverflow_RefusesInsteadOfProducingInfinity(string expression)
        => Assert.Throws<OverflowException>(() => Evaluate(expression));

    [Theory]
    [InlineData("+CSng(1E100)")]
    [InlineData("-CSng(1E100)")]
    [InlineData("Abs(CSng(1E100))")]
    [InlineData("Int(CSng(1E100))")]
    [InlineData("Fix(CSng(1E100))")]
    [InlineData("CDbl(\"NaN\")")]
    [InlineData("CDbl(\"Infinity\")")]
    public void NonfiniteConversion_RefusesBeforeSubtypePreservation(string expression)
        => Assert.Throws<OverflowException>(() => Evaluate(expression));

    [Fact]
    public void FloatingArithmetic_PreservesNullAndDecimalInputs()
    {
        Assert.Same(DBNull.Value, Evaluate("CDbl(1)/Null"));
        Assert.Equal(1m / 3m, Assert.IsType<decimal>(Evaluate("CDec(1)/CDec(3)")));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task StoredDefaults_RetainFloatingTextThroughReopen(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("DoubleText", typeof(string)) { DefaultValueExpression = "CStr(1/3)" },
                    new ColumnDefinition("SingleText", typeof(string)) { DefaultValueExpression = "CStr(CSng(1)/CSng(3))" },
                ],
                TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [DbDefault.Value, DbDefault.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("0.333333333333333", row[0]);
            Assert.Equal("0.3333333", row[1]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task CalculatedDouble_UpdateRetainsFullRangeAndNativeText()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Amount", typeof(double)),
                    new ColumnDefinition("Third", typeof(string)) { IsCalculated = true, CalculationExpression = "[Amount]/3 & \"\"" },
                    new ColumnDefinition("Square", typeof(double)) { IsCalculated = true, CalculationExpression = "[Amount]*[Amount]" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1d, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            int updated = await writer.UpdateRowsAsync(
                "T", "Amount", 1d, new Dictionary<string, object?> { ["Amount"] = 1E100 }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(
                "T", "Amount", 1E100, new Dictionary<string, object?> { ["Amount"] = 1E308 }, TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(1E100, row[0]);
            Assert.Equal("3.33333333333333E+99", row[1]);
            Assert.Equal(1E200, row[2]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Theory]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    [InlineData("1E999")]
    public async Task CalculatedDivision_NonfiniteTextRefusesWithoutChangingImage(string divisor)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(
            "T",
            [
                new ColumnDefinition("Amount", typeof(double)),
                new ColumnDefinition("Divisor", typeof(string)),
                new ColumnDefinition("Quotient", typeof(double)) { IsCalculated = true, CalculationExpression = "[Amount]/[Divisor]" },
            ],
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("T", [1d, "2", DBNull.Value], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(
            "T", "Amount", 1d, new Dictionary<string, object?> { ["Divisor"] = divisor }, TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_FloatingExpressionsMatchIndependentNativeQueries()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.FloatingSubtype", databaseExtension: ".mdb");
        string[] expressions =
        [
            "CStr(1/3)", "TypeName(1/3)", "CStr(CSng(1)/CSng(3))", "TypeName(CSng(1)/CSng(3))",
            "TypeName(CSng(1)/CInt(3))", "TypeName(CSng(1)/CLng(3))", "TypeName(CByte(1)/CByte(3))",
            "TypeName(CSng(1)+True)", "TypeName(CSng(1)+\"2\")", "TypeName(CSng(3.4E38)*CSng(2))",
            "CStr(CSng(3.4E38)*CSng(2))", "TypeName(Abs(CSng(-1)))", "TypeName(Int(CSng(1.5)))",
            "TypeName(Fix(CSng(1.5)))", "CStr(CDbl(1E-15))", "CStr(CDbl(1E-16))", "CStr(-CDbl(0))",
            "CStr(CDbl(1.23456789012345E-4))", "CStr(CDbl(1.23456789012345E-3))",
            "CStr(CDbl(-1.23456789012345E-4))", "CStr(CDbl(9.99999999999999E-4))",
            "CStr(CDbl(1.234567E-4))", "CStr(CSng(1.234567E-3))", "CStr(CSng(1.234567E-4))",
            "CStr(CSng(1.23456789012345E-4))", "CStr(CSng(9.99999999999999E-4))",
        ];
        string nativeExpressions = string.Join(",", Array.ConvertAll(expressions, AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
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
                foreach ($expression in @('CDbl(1)/"Infinity"','CDbl(1)/"-Infinity"')) {
                    $refused = $false
                    try {
                        $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                        try { $null = $rs.Fields('R').Value }
                        finally { $rs.Close() }
                    } catch {
                        foreach ($daoError in $engine.Errors) {
                            if ($daoError.Number -eq 3464) { $refused = $true }
                        }
                        if (!$refused) { throw }
                    }
                    if (!$refused) { throw ('Native nonfinite divisor was accepted: ' + $expression) }
                    Write-Output ('REFUSED=' + $expression)
                }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == 0, result.StandardOutput + result.StandardError);
        string[] nativeResults = result.StandardOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(expressions.Length + 2, nativeResults.Length);
        Assert.Contains("REFUSED=CDbl(1)/\"Infinity\"", nativeResults);
        Assert.Contains("REFUSED=CDbl(1)/\"-Infinity\"", nativeResults);
        foreach (string expression in expressions)
        {
            Assert.Contains(expression + "=" + Assert.IsType<string>(Evaluate(expression)), nativeResults);
        }
    }

    private static object Evaluate(string expression)
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false);
        return plan.Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
    }
}
