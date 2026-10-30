namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
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

/// <summary>Checks Round against independent native DAO3.6 Jet4 query results.</summary>
public sealed class CalculatedExpressionRoundTests
{
    /// <summary>Gets exact native Double-field source and Round result bits.</summary>
    public static TheoryData<long, int, long> FloatingBoundaryData { get; } = new()
    {
        { 4636737291354636287L, 14, 4636737291354636287L },
        { 4636737291354636289L, 14, 4636737291354636289L },
        { 4621819117588971519L, 15, 4621819117588971519L },
        { 4621819117588971521L, 15, 4621819117588971521L },
        { 4607182418800017407L, 16, 4607182418800017406L },
        { 4607182418800017409L, 16, 4607182418800017409L },
        { 4517329193108106637L, 22, 4517329193108106637L },
        { 4517329193108106638L, 22, 4517329193108106638L },
        { 4593560419847042654L, 14, 4593560419847042966L },
        { 4593560419847042654L, 15, 4593560419847042678L },
        { 4593560419847042654L, 16, 4593560419847042657L },
        { 4593560419847042654L, 22, 4593560419847042653L },
        { -4629811617007733154L, 22, -4629811617007733155L },
        { 6103021453049119613L, 2, 6103021453049119613L },
        { 9214871658872686752L, 2, 9214871658872686752L },
        { 9214871658872686752L, 22, 9214871658872686752L },
    };

    [Theory]
    [InlineData("TypeName(Round(CDbl(1.25),1))", "Double")]
    [InlineData("TypeName(Round(CSng(1.25),1))", "Single")]
    [InlineData("TypeName(Round(CInt(1)))", "Integer")]
    [InlineData("TypeName(Round(CInt(1),23))", "Integer")]
    [InlineData("TypeName(Round(CLng(1)))", "Long")]
    [InlineData("TypeName(Round(CLng(1),2147483647))", "Long")]
    [InlineData("TypeName(Round(CByte(1)))", "Integer")]
    [InlineData("TypeName(Round(True))", "Integer")]
    [InlineData("TypeName(Round(\"1.25\",1))", "Double")]
    [InlineData("TypeName(Round(#2020-01-01#,0))", "Date")]
    [InlineData("CStr(Round(CDbl(1.225),2))", "1.23")]
    [InlineData("CStr(Round(CDbl(-1.225),2))", "-1.23")]
    [InlineData("CStr(Round(CDbl(2.685),2))", "2.68")]
    [InlineData("CStr(Round(CDbl(-2.685),2))", "-2.68")]
    [InlineData("CStr(Round(CSng(1.225),2))", "1.23")]
    [InlineData("CStr(Round(CDbl(1E100),2))", "1E+100")]
    [InlineData("CStr(Round(CDbl(1.25),0.5))", "1")]
    [InlineData("CStr(Round(CDbl(1.25),1.5))", "1.25")]
    [InlineData("CStr(Round(CDbl(1.25),-0.5))", "1")]
    [InlineData("CStr(Round(CDbl(1.23456E-20),20))", "1E-20")]
    [InlineData("CStr(Round(CDbl(1.23456E-20),22))", "1.23E-20")]
    public void Round_RetainsNativeSubtypeAndValue(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(expression)));

    [Theory]
    [MemberData(nameof(FloatingBoundaryData))]
    public void Round_DoubleScalingMatchesNativeFieldBits(long inputBits, int digits, long expectedBits)
    {
        var function = new CalculatedExpressionFunctionNode(
            "Round", [new CalculatedExpressionValueNode(BitConverter.Int64BitsToDouble(inputBits)), new CalculatedExpressionValueNode(digits)]);
        object result = function.Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], [], force: false));
        Assert.Equal(expectedBits, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(result)));
    }

    [Theory]
    [InlineData("Round(CDec(1.225),0)", "1")]
    [InlineData("Round(CDec(1.225),2)", "1.22")]
    [InlineData("Round(CDec(1.225),28)", "1.225")]
    [InlineData("Round(CDec(1.225),29)", "1.225")]
    [InlineData("Round(CDec(1.225),2147483647)", "1.225")]
    public void Round_DecimalRetainsNativePrecision(string expression, string expected)
        => Assert.Equal(decimal.Parse(expected, CultureInfo.InvariantCulture), Assert.IsType<decimal>(Evaluate(expression)));

    [Fact]
    public void Round_PreservesNullAfterValidatingPrecision()
    {
        Assert.Same(DBNull.Value, Evaluate("Round(Null)"));
        Assert.Same(DBNull.Value, Evaluate("Round(Null,2)"));
        Assert.Same(DBNull.Value, Evaluate("Round(Null,23)"));
        Assert.Throws<ArgumentOutOfRangeException>(() => Evaluate("Round(Null,-1)"));
        Assert.Throws<InvalidCastException>(() => Evaluate("Round(Null,Null)"));
    }

    [Theory]
    [InlineData("Round(CDbl(1),-1)")]
    [InlineData("Round(CDbl(1),23)")]
    [InlineData("Round(CSng(1),23)")]
    public void FloatingRound_InvalidPrecisionRefuses(string expression)
        => Assert.Throws<ArgumentOutOfRangeException>(() => Evaluate(expression));

    [Theory]
    [InlineData("Round(CDbl(1),2147483648)")]
    [InlineData("Round(CDbl(\"Infinity\"),2)")]
    public void Round_OverflowRefuses(string expression)
        => Assert.Throws<OverflowException>(() => Evaluate(expression));

    [Fact]
    public void Round_EvaluatesValueBeforePrecision()
        => Assert.Throws<DivideByZeroException>(() => Evaluate("Round(1/0,CLng(\"bad\"))"));

    [Fact]
    public async Task CalculatedRound_UpdateRetainsFloatingRangeAndRefusesInvalidPrecision()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Amount", typeof(double)),
                    new ColumnDefinition("Digits", typeof(int)),
                    new ColumnDefinition("Rounded", typeof(double)) { IsCalculated = true, CalculationExpression = "Round([Amount],[Digits])" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1.225d, 2, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            int updated = await writer.UpdateRowsAsync(
                "T", "Amount", 1.225d, new Dictionary<string, object?> { ["Amount"] = 1E100 }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.UpdateRowsAsync(
                "T", "Amount", 1E100, new Dictionary<string, object?> { ["Digits"] = 23 }, TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(1E100, row[0]);
            Assert.Equal(2, row[1]);
            Assert.Equal(1E100, row[2]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_RoundMatchesIndependentNativeQueries()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Round", databaseExtension: ".mdb");
        string[] expressions =
        [
            "TypeName(Round(CDbl(1.25),1))", "TypeName(Round(CSng(1.25),1))",
            "TypeName(Round(CInt(1)))", "TypeName(Round(CLng(1)))", "TypeName(Round(CByte(1)))",
            "TypeName(Round(CInt(1),23))", "TypeName(Round(CLng(1),2147483647))",
            "TypeName(Round(True))", "TypeName(Round(\"1.25\",1))", "TypeName(Round(#2020-01-01#,0))",
            "CStr(Round(CDbl(1.225),2))", "CStr(Round(CDbl(-1.225),2))",
            "CStr(Round(CDbl(2.685),2))", "CStr(Round(CDbl(-2.685),2))", "CStr(Round(CSng(1.225),2))",
            "CStr(Round(CDbl(1E100),2))", "CStr(Round(CDbl(1.25),0.5))",
            "CStr(Round(CDbl(1.25),1.5))", "CStr(Round(CDbl(1.25),-0.5))",
            "CStr(Round(CDbl(1.23456E-20),20))", "CStr(Round(CDbl(1.23456E-20),22))",
            "TypeName(Round(Null))", "TypeName(Round(Null,2))", "TypeName(Round(Null,23))",
        ];
        var nativeBoundaries = new List<string>();
        foreach (TheoryDataRow<long, int, long> row in FloatingBoundaryData)
        {
            (long inputBits, int digits, long expectedBits) = row.Data;
            nativeBoundaries.Add(string.Create(CultureInfo.InvariantCulture, $"@({inputBits}L,{digits},{expectedBits}L)"));
        }

        string boundaryRows = string.Join(",", nativeBoundaries);
        string nativeExpressions = string.Join(",", Array.ConvertAll(expressions, AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            try {
                $db.Execute('CREATE TABLE [T] ([V] DOUBLE,[P] LONG)')
                $rs = $db.OpenRecordset('T',2)
                try { $rs.AddNew(); $rs.Fields('V').Value=0.0; $rs.Fields('P').Value=0; $rs.Update() }
                finally { $rs.Close() }
                foreach ($row in @({{boundaryRows}})) {
                    $rs = $db.OpenRecordset('T',2)
                    try {
                        $rs.Edit()
                        $rs.Fields('V').Value=[BitConverter]::Int64BitsToDouble($row[0])
                        $rs.Fields('P').Value=[int]$row[1]
                        $rs.Update()
                    } finally { $rs.Close() }
                    $rs = $db.OpenRecordset('SELECT [V],Round([V],[P]) AS [R] FROM [T]')
                    try {
                        if ([BitConverter]::DoubleToInt64Bits($rs.Fields('V').Value) -ne $row[0]) { throw 'Native source bits changed' }
                        if ([BitConverter]::DoubleToInt64Bits($rs.Fields('R').Value) -ne $row[2]) { throw 'Native Round bits changed' }
                        Write-Output ('BOUNDARY=' + $row[0] + ',' + $row[1] + ',' + $row[2])
                    } finally { $rs.Close() }
                }
                foreach ($expression in @({{nativeExpressions}})) {
                    $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                    try { Write-Output ($expression + '=' + $rs.Fields('R').Value) }
                    finally { $rs.Close() }
                }
                foreach ($expression in @('Round(CDbl(1),-1)','Round(CDbl(1),23)','Round(CSng(1),23)','Round(Null,-1)','Round(Null,Null)','Round(CDbl(1),2147483648)')) {
                    $refused = $false
                    try {
                        $rs = $db.OpenRecordset('SELECT ' + $expression + ' AS [R]')
                        try { $null = $rs.Fields('R').Value }
                        finally { $rs.Close() }
                    } catch { $refused = $true }
                    if (!$refused) { throw ('Native invalid precision was accepted: ' + $expression) }
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
        Assert.Equal(expressions.Length + FloatingBoundaryData.Count + 6, nativeResults.Length);
        foreach (string expression in expressions)
        {
            Assert.Contains(expression + "=" + Assert.IsType<string>(Evaluate(expression)), nativeResults);
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_DecimalRoundMatchesNativeField()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.DecimalRound", databaseExtension: ".mdb");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            $db.Close()
            # Jet4 DAO reads Decimal fields but cannot author their descriptors.
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.Jet.OLEDB.4.0;Data Source=' + {{path}})
                $connection.Execute('CREATE TABLE [T] ([N] DECIMAL(28,12))') | Out-Null
                $connection.Execute('INSERT INTO [T] VALUES (1.225)') | Out-Null
            } finally {
                $connection.Close()
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null
            }
            $db = $engine.OpenDatabase({{path}})
            try {
                foreach ($digits in @(0,2,22,23,28,29,2147483647)) {
                    $rs = $db.OpenRecordset('SELECT CStr(Round([N],' + $digits + ')) AS [R], TypeName(Round([N],' + $digits + ')) AS [T] FROM [T]')
                    try {
                        if ($rs.Fields('T').Value -ne 'Decimal') { throw 'Native Round lost its Decimal subtype' }
                        Write-Output ('DECIMAL=' + $digits + '=' + $rs.Fields('R').Value)
                    } finally { $rs.Close() }
                }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == 0, result.StandardOutput + result.StandardError);
        string[] nativeResults = result.StandardOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        int[] precisions = [0, 2, 22, 23, 28, 29, int.MaxValue];
        Assert.Equal(precisions.Length, nativeResults.Length);
        foreach (int digits in precisions)
        {
            string expression = string.Create(CultureInfo.InvariantCulture, $"Round(CDec(1.225),{digits})");
            string value = Assert.IsType<decimal>(Evaluate(expression)).ToString("G29", CultureInfo.InvariantCulture);
            Assert.Contains(string.Create(CultureInfo.InvariantCulture, $"DECIMAL={digits}={value}"), nativeResults);
        }
    }

    private static object Evaluate(string expression)
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false);
        return plan.Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], [], force: false));
    }
}
