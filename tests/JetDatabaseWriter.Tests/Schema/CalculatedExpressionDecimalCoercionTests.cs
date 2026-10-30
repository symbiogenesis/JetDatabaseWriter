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

/// <summary>Checks floating Decimal coercion against native Jet4 field expressions.</summary>
public sealed class CalculatedExpressionDecimalCoercionTests
{
    /// <summary>Gets Double source bits and native Decimal values before result-field marshaling.</summary>
    public static TheoryData<long, string> DoubleCases { get; } = new()
    {
        { 1L, "0" },
        { long.MinValue, "0" },
        { long.MinValue + 1, "0" },
        { 4608195728716175770L, "1.225" },
        { 4608238818662570491L, "1.23456789012346" },
        { 4608238818662570438L, "1.23456789012344" },
        { 4608238818662570483L, "1.23456789012345" },
        { -4615133218192205325L, "-1.23456789012345" },
        { 4674248675788921029L, "30993.6215965978" },
        { 4717113922069995173L, "23650794.6269824" },
        { 4692350167116523573L, "500967.414770726" },
        { 4814971657088491562L, "80113360262528.6" },
        { 4873533552919086281L, "658221533642254000" },
        { 4183757152651862041L, "0" },
        { 4183757152651862042L, "0" },
        { 4190534277386749971L, "0.0000000000000000000000000002" },
        { 4172965893781238600L, "0" },
        { 4188260752279232537L, "0.0000000000000000000000000001" },
        { 5026061058026967681L, "10000000000000000000000000000" },
        { 5039527983027585022L, "79228162514264300000000000000" },
        { 5039527983027585023L, "79228162514264300000000000000" },
    };

    /// <summary>Gets Single source bits and native Decimal values.</summary>
    public static TheoryData<int, string> SingleCases { get; } = new()
    {
        { 1, "0" },
        { int.MinValue, "0" },
        { int.MinValue + 1, "0" },
        { 1067240653, "1.225" },
        { 1067320913, "1.234568" },
        { 1067320902, "1.234566" },
        { 1067320911, "1.234568" },
        { -1080162737, "-1.234568" },
        { 256561912, "0" },
        { 276662198, "0.0000000000000000000000000001" },
        { 285050806, "0.0000000000000000000000000001" },
        { 289285576, "0.0000000000000000000000000001" },
        { 1845575481, "9999999000000000000000000000" },
        { 1870659581, "79228150000000000000000000000" },
        { 1870659583, "79228160000000000000000000000" },
    };

    [Theory]
    [MemberData(nameof(DoubleCases))]
    public void DoubleConversion_RetainsNativeDecimalValue(long bits, string expected)
        => AssertConversions(BitConverter.Int64BitsToDouble(bits), expected);

    [Theory]
    [MemberData(nameof(SingleCases))]
    public void SingleConversion_RetainsNativeDecimalValue(int bits, string expected)
        => AssertConversions(BitConverter.Int32BitsToSingle(bits), expected);

    [Theory]
    [InlineData(5039527983027585024L)]
    [InlineData(-4183844053827190784L)]
    [InlineData(9218868437227405312L)]
    [InlineData(-4503599627370496L)]
    [InlineData(9221120237041090560L)]
    public void DoubleConversion_OverflowAndNonfiniteRefuse(long bits)
        => AssertRefusal(BitConverter.Int64BitsToDouble(bits));

    [Theory]
    [InlineData(1870659584)]
    [InlineData(-276824064)]
    [InlineData(2139095040)]
    [InlineData(-8388608)]
    [InlineData(2143289344)]
    public void SingleConversion_OverflowAndNonfiniteRefuse(int bits)
        => AssertRefusal(BitConverter.Int32BitsToSingle(bits));

    [Fact]
    public async Task CalculatedCoercion_ReopenAndRefusalRetainNativeDecimalRounding()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("D", typeof(double)),
                    new ColumnDefinition("S", typeof(float)),
                    new ColumnDefinition("N", typeof(decimal)),
                    new ColumnDefinition("DR", typeof(string)) { IsCalculated = true, CalculationExpression = "Round([N]+[D],2) & \"\"" },
                    new ColumnDefinition("SR", typeof(string)) { IsCalculated = true, CalculationExpression = "Round([N]+[S],2) & \"\"" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1.225d, 1.225f, 0m, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            byte[] before = stream.ToArray();
            JetValidationRuleException refusal = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.UpdateRowsAsync(
                "T", "D", 1.225d, new Dictionary<string, object?> { ["D"] = double.PositiveInfinity }, TestContext.Current.CancellationToken));
            Assert.Equal("DR", refusal.ErrorInfo.ColumnName);
            Assert.Contains("#Error(6)", refusal.Message, StringComparison.Ordinal);
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("1.22", row[3]);
            Assert.Equal("1.22", row[4]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_FloatingFieldsMatchNativeDecimalCoercion()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.DecimalCoercion", databaseExtension: ".mdb");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        var nativeCases = new List<string>();
        foreach (TheoryDataRow<long, string> row in DoubleCases)
        {
            (long bits, string expected) = row.Data;
            nativeCases.Add(string.Create(CultureInfo.InvariantCulture, $"@('D',{bits}L,'{expected}')"));
        }

        foreach (TheoryDataRow<int, string> row in SingleCases)
        {
            (int bits, string expected) = row.Data;
            nativeCases.Add(string.Create(CultureInfo.InvariantCulture, $"@('S',{bits},'{expected}')"));
        }

        nativeCases.Add("@('D',5039527983027585024L,'Double')");
        nativeCases.Add("@('S',1870659584,'Double')");
        nativeCases.Add("@('D',9218868437227405312L,'Null')");
        nativeCases.Add("@('D',-4503599627370496L,'Null')");
        nativeCases.Add("@('D',9221120237041090560L,'Null')");
        string rows = string.Join(",", nativeCases);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            $db.Close()
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.Jet.OLEDB.4.0;Data Source=' + {{path}})
                $connection.Execute('CREATE TABLE [T] ([N] DECIMAL(28,28),[D] DOUBLE,[S] SINGLE)') | Out-Null
                $connection.Execute('INSERT INTO [T] VALUES (0,0,0)') | Out-Null
            } finally {
                $connection.Close()
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null
            }
            $db = $engine.OpenDatabase({{path}})
            try {
                foreach ($row in @({{rows}})) {
                    $name = $row[0]
                    $value = if ($name -eq 'D') { [BitConverter]::Int64BitsToDouble($row[1]) }
                             else { [BitConverter]::ToSingle([BitConverter]::GetBytes([int]$row[1]),0) }
                    $rs = $db.OpenRecordset('T',2)
                    try { $rs.Edit(); $rs.Fields($name).Value=[double]$value; $rs.Update() }
                    finally { $rs.Close() }
                    $rs = $db.OpenRecordset('SELECT [' + $name + '] AS [Source],TypeName([N]+[' + $name + ']) AS [T],CStr([N]+[' + $name + ']) AS [R] FROM [T]')
                    try {
                        $stored = $rs.Fields('Source').Value
                        $bits = if ($name -eq 'D') { [BitConverter]::DoubleToInt64Bits($stored) }
                                else { [BitConverter]::ToInt32([BitConverter]::GetBytes([single]$stored),0) }
                        if ($bits -ne $row[1]) { throw 'Native source bits changed' }
                        if ($row[2] -eq 'Double') {
                            if ($rs.Fields('T').Value -ne 'Double') { throw 'Native decimal overflow did not select Double fallback' }
                        } elseif ($row[2] -eq 'Null') {
                            if ($null -ne $rs.Fields('T').Value -or $null -ne $rs.Fields('R').Value) { throw 'Native nonfinite expression was not missing' }
                        } else {
                            if ($rs.Fields('T').Value -ne 'Decimal') { throw 'Native coercion did not produce Decimal' }
                            if ($rs.Fields('R').Value -ne $row[2]) { throw ('Native coercion mismatch: ' + $name + ':' + $bits + ':' + $rs.Fields('R').Value) }
                        }
                        Write-Output ('MATCH=' + $name + ':' + $bits)
                    } finally { $rs.Close() }
                }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == 0, result.StandardOutput + result.StandardError);
        Assert.Equal(nativeCases.Count, result.StandardOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries).Length);
    }

    private static void AssertConversions(object value, string expected)
    {
        decimal native = decimal.Parse(expected, CultureInfo.InvariantCulture);
        Assert.Equal(native, CalculatedExpressionCoercion.ToDecimal(value));
        Assert.True(CalculatedExpressionCoercion.TryConvertDecimal(value, out decimal converted));
        Assert.Equal(native, converted);
        var function = new CalculatedExpressionFunctionNode("CDec", [new CalculatedExpressionValueNode(value)]);
        Assert.Equal(native, Assert.IsType<decimal>(function.Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], []))));
    }

    private static void AssertRefusal(object value)
    {
        Assert.Throws<OverflowException>(() => CalculatedExpressionCoercion.ToDecimal(value));
        Assert.False(CalculatedExpressionCoercion.TryConvertDecimal(value, out _));
    }
}
