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

/// <summary>Checks native mixed Decimal arithmetic when a floating operand exceeds Decimal.</summary>
public sealed class CalculatedExpressionDecimalPromotionTests
{
    /// <summary>Gets operations, Decimal operands and exact native Double result bits.</summary>
    public static TheoryData<string, decimal, long> NativeCases { get; } = new()
    {
        { "[N]+[F]", 0m, 5039527983027585024L },
        { "[F]+[N]", 0m, 5039527983027585024L },
        { "[N]-[F]", 0m, -4183844053827190784L },
        { "[F]-[N]", 0m, 5039527983027585024L },
        { "[N]*[F]", 0m, 0L },
        { "[F]*[N]", 0m, 0L },
        { "[N]/[F]", 0m, 0L },
        { "[N]+[F]", 1m, 5039527983027585024L },
        { "[F]+[N]", 1m, 5039527983027585024L },
        { "[N]-[F]", 1m, -4183844053827190784L },
        { "[F]-[N]", 1m, 5039527983027585024L },
        { "[N]*[F]", 1m, 5039527983027585024L },
        { "[F]*[N]", 1m, 5039527983027585024L },
        { "[N]/[F]", 1m, 4174836854572449792L },
        { "[F]/[N]", 1m, 5039527983027585024L },
    };

    [Theory]
    [MemberData(nameof(NativeCases))]
    public void DoubleOperand_OverflowPromotesBeforeArithmetic(string expression, decimal number, long expectedBits)
        => AssertPromotion(expression, number, BitConverter.Int64BitsToDouble(5039527983027585024L), expectedBits);

    [Theory]
    [MemberData(nameof(NativeCases))]
    public void SingleOperand_OverflowPromotesBeforeArithmetic(string expression, decimal number, long expectedBits)
        => AssertPromotion(expression, number, BitConverter.Int32BitsToSingle(1870659584), expectedBits);

    [Theory]
    [MemberData(nameof(NativeCases))]
    public void NegativeDoubleOperand_PreservesNativeResultBits(string expression, decimal number, long expectedBits)
        => AssertPromotion(expression, number, BitConverter.Int64BitsToDouble(-4183844053827190784L), expectedBits ^ long.MinValue);

    [Theory]
    [MemberData(nameof(NativeCases))]
    public void NegativeSingleOperand_PreservesNativeResultBits(string expression, decimal number, long expectedBits)
        => AssertPromotion(expression, number, BitConverter.Int32BitsToSingle(-276824064), expectedBits ^ long.MinValue);

    [Theory]
    [InlineData("+")]
    [InlineData("-")]
    [InlineData("*")]
    [InlineData("/")]
    public void NonfiniteMixedOperand_StillRefuses(string operation)
    {
        foreach (double value in new[] { double.PositiveInfinity, double.NegativeInfinity, double.NaN })
        {
            Assert.Throws<OverflowException>(() => Evaluate(operation, 0m, value));
            Assert.Throws<OverflowException>(() => Evaluate(operation, value, 0m));
        }
    }

    [Fact]
    public void MixedArithmetic_PreservesDecimalRangeErrorsAndNull()
    {
        const double boundary = 79228162514264337593543950336d;
        Assert.Same(DBNull.Value, Evaluate("*", DBNull.Value, boundary));
        Assert.Same(DBNull.Value, Evaluate("/", boundary, DBNull.Value));
        Assert.Throws<DivideByZeroException>(() => Evaluate("/", boundary, 0m));
        Assert.Throws<OverflowException>(() => Evaluate("*", 7000000000000000000000000000m, 100d));
        Assert.Equal(0m, Assert.IsType<decimal>(Evaluate("*", 0m, BitConverter.Int64BitsToDouble(5039527983027585023L))));
        Assert.Throws<OverflowException>(() => CalculatedExpressionCoercion.ToDecimal(boundary));
        Assert.False(CalculatedExpressionCoercion.TryConvertDecimal(boundary, out _));
        Assert.Throws<OverflowException>(() => Evaluate("*", 2m, double.MaxValue));
    }

    [Fact]
    public async Task CalculatedPromotion_InsertUpdateAndReopenPreserveDoubleResults()
    {
        const double boundary = 79228162514264337593543950336d;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("N", typeof(decimal)),
                    new ColumnDefinition("D", typeof(double)),
                    new ColumnDefinition("S", typeof(float)),
                    new ColumnDefinition("Sum", typeof(double)) { IsCalculated = true, CalculationExpression = "[N]+[D]" },
                    new ColumnDefinition("Product", typeof(double)) { IsCalculated = true, CalculationExpression = "[N]*[S]" },
                    new ColumnDefinition("Quotient", typeof(double)) { IsCalculated = true, CalculationExpression = "[N]/[D]" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [0m, boundary, (float)boundary, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            int updated = await writer.UpdateRowsAsync(
                "T", "N", 0m, new Dictionary<string, object?> { ["N"] = 1m }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(
                "T", "N", 1m, new Dictionary<string, object?> { ["D"] = double.PositiveInfinity }, TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(boundary, row[3]);
            Assert.Equal(boundary, row[4]);
            Assert.Equal(BitConverter.Int64BitsToDouble(4174836854572449792L), row[5]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_MixedDecimalPromotionMatchesNativeFieldResults()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.DecimalPromotion", databaseExtension: ".mdb");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        var cases = new List<string>();
        foreach (TheoryDataRow<string, decimal, long> row in NativeCases)
        {
            (string expression, decimal number, long expectedBits) = row.Data;
            cases.Add(string.Create(CultureInfo.InvariantCulture, $"@('{expression}',{number},{expectedBits}L)"));
        }

        string nativeCases = string.Join(",", cases);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            $db.Close()
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.Jet.OLEDB.4.0;Data Source=' + {{path}})
                $connection.Execute('CREATE TABLE [T] ([N] DECIMAL(28,0),[D] DOUBLE,[S] SINGLE)') | Out-Null
                $connection.Execute('INSERT INTO [T] VALUES (0,0,0)') | Out-Null
            } finally {
                $connection.Close()
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null
            }
            $db = $engine.OpenDatabase({{path}})
            try {
                foreach ($sourceBits in @(5039527983027585024L,-4183844053827190784L)) {
                    $rs = $db.OpenRecordset('T',2)
                    try {
                        $rs.Edit()
                        $rs.Fields('D').Value = [BitConverter]::Int64BitsToDouble($sourceBits)
                        $rs.Fields('S').Value = [double][single][BitConverter]::Int64BitsToDouble($sourceBits)
                        $rs.Update()
                    } finally { $rs.Close() }
                    foreach ($field in @('D','S')) {
                        foreach ($case in @({{nativeCases}})) {
                            $expectedBits = if ($sourceBits -lt 0) { $case[2] -bxor [long]::MinValue } else { $case[2] }
                            $db.Execute('UPDATE T SET N=' + $case[1])
                            $expression = $case[0].Replace('[F]','[' + $field + ']')
                            $rs = $db.OpenRecordset('SELECT [' + $field + '] AS [Source],TypeName(' + $expression + ') AS [T],' + $expression + ' AS [R] FROM [T]')
                            try {
                                if ([BitConverter]::DoubleToInt64Bits([double]$rs.Fields('Source').Value) -ne $sourceBits) { throw 'Native source bits changed' }
                                if ($rs.Fields('T').Value -ne 'Double') { throw ('Native promotion subtype mismatch: ' + $expression) }
                                if ([BitConverter]::DoubleToInt64Bits([double]$rs.Fields('R').Value) -ne $expectedBits) { throw ('Native promotion value mismatch: ' + $expression) }
                                Write-Output ('MATCH=' + $field + ':' + $sourceBits + ':' + $case[1] + ':' + $expression)
                            } finally { $rs.Close() }
                        }
                    }
                }
                $db.Execute('UPDATE T SET N=7000000000000000000000000000,D=100,S=100')
                foreach ($expression in @('[N]*[D]','[D]*[N]','[N]*[S]','[S]*[N]')) {
                    $rs = $db.OpenRecordset('SELECT TypeName(' + $expression + ') AS [T],' + $expression + ' AS [R] FROM [T]')
                    try {
                        if ($null -ne $rs.Fields('T').Value -or $null -ne $rs.Fields('R').Value) { throw 'Native Decimal result overflow unexpectedly promoted' }
                        Write-Output ('MISSING=' + $expression)
                    } finally { $rs.Close() }
                }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == 0, result.StandardOutput + result.StandardError);
        Assert.Equal((cases.Count * 4) + 4, result.StandardOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries).Length);
    }

    private static void AssertPromotion(string expression, decimal number, object floating, long expectedBits)
    {
        ArgumentNullException.ThrowIfNull(expression);
        bool decimalFirst = expression.StartsWith("[N]", StringComparison.Ordinal);
        string operation = expression.Substring(3, 1);
        object result = decimalFirst ? Evaluate(operation, number, floating) : Evaluate(operation, floating, number);
        Assert.Equal(expectedBits, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(result)));
    }

    private static object Evaluate(string operation, object left, object right)
        => new CalculatedExpressionBinaryNode(operation, new CalculatedExpressionValueNode(left), new CalculatedExpressionValueNode(right))
            .Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], [], force: false));
}
