namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks numeric literal types and values against native Jet4 expression controls.</summary>
public sealed class CalculatedExpressionNumericLiteralTests
{
    /// <summary>Gets independently measured native Decimal-to-floating conversion bits.</summary>
    public static TheoryData<string, long, int> NativeDecimalConversionData { get; } = new()
    {
        { "99.99999999999999", 4636737291354636288L, 1120403456 },
        { "0.9999999999999999", 4607182418800017408L, 1065353216 },
        { "1.2345678901234567", 4608238818662570491L, 1067320914 },
        { "1.000000059604644775390625", 4607182419068452864L, 1065353216 },
        { "1.0000000596046448", 4607182419068452864L, 1065353216 },
        { "1.000000059604644775390626", 4607182419068452864L, 1065353216 },
        { "0.1234567890123456789012345678", 4593560419847042656L, 1039980266 },
        { "79228162514264337593543950335", 5039527983027585024L, 1870659584 },
        { "1E-28", 4188260752279232538L, 285050806 },
    };

    [Theory]
    [MemberData(nameof(NativeDecimalConversionData))]
    public void DecimalConversion_RetainsNativeFloatingBits(string text, long doubleBits, int singleBits)
    {
        decimal number = decimal.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
        Assert.Equal(doubleBits, BitConverter.DoubleToInt64Bits(CalculatedExpressionCoercion.ToDouble(number)));
        Assert.Equal(singleBits, BitConverter.SingleToInt32Bits(CalculatedExpressionCoercion.ToSingle(number)));
        Assert.Equal(doubleBits, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(CalculatedExpressionCoercion.CoerceResult(number, typeof(double)))));
        Assert.Equal(singleBits, BitConverter.SingleToInt32Bits(Assert.IsType<float>(CalculatedExpressionCoercion.CoerceResult(number, typeof(float)))));
    }

    [Theory]
    [InlineData("0", 0)]
    [InlineData("1", 1)]
    [InlineData("1.0", 1)]
    [InlineData("1E0", 1)]
    [InlineData("1E+3", 1000)]
    [InlineData("2147483647", int.MaxValue)]
    [InlineData("-2147483648", int.MinValue)]
    [InlineData("-2147483648.0", int.MinValue)]
    [InlineData("-2.147483648E9", int.MinValue)]
    public void IntegralLiteral_RetainsNativeLong(string expression, int expected)
        => Assert.Equal(expected, Assert.IsType<int>(Evaluate(expression)));

    [Theory]
    [InlineData(".5", "0.5")]
    [InlineData("1e-3", "0.001")]
    [InlineData("2147483648", "2147483648")]
    [InlineData("-(2147483648)", "-2147483648")]
    [InlineData("-2147483649", "-2147483649")]
    [InlineData("99.99999999999999", "99.99999999999999")]
    [InlineData("1E-28", "0.0000000000000000000000000001")]
    [InlineData("79228162514264337593543950335", "79228162514264337593543950335")]
    public void ExactDecimalLiteral_RetainsNativeDecimal(string expression, string expected)
        => Assert.Equal(decimal.Parse(expected, CultureInfo.InvariantCulture), Assert.IsType<decimal>(Evaluate(expression)));

    [Theory]
    [InlineData("1E-29", 4172965893781238599L)]
    [InlineData("1E29", 5040708725286295329L)]
    [InlineData("0.12345678901234567890123456789", 4593560419847042655L)]
    [InlineData("0.00000000000000000000000000011", 4188750217770867521L)]
    [InlineData("79228162514264337593543950336", 5039527983027585024L)]
    [InlineData("CDbl(99.99999999999999)", 4636737291354636288L)]
    [InlineData("CDbl(99.9999999999999900)", 4636737291354636288L)]
    [InlineData("CDbl(0.9999999999999999)", 4607182418800017408L)]
    [InlineData("CDbl(1.2345678901234567)", 4608238818662570491L)]
    public void FloatingLiteralAndConversion_RetainNativeBits(string expression, long expectedBits)
        => Assert.Equal(expectedBits, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(Evaluate(expression))));

    [Theory]
    [InlineData("TypeName(1+1)", "Long")]
    [InlineData("TypeName(32767+1)", "Long")]
    [InlineData("TypeName(CInt(1)+1)", "Long")]
    [InlineData("TypeName(1+True)", "Long")]
    [InlineData("TypeName(2147483647+1)", "Double")]
    [InlineData("TypeName(2147483647-(-1))", "Double")]
    [InlineData("TypeName(2147483647*2147483647)", "Double")]
    [InlineData("TypeName(2147483648+1)", "Decimal")]
    [InlineData("TypeName(-2147483648-1)", "Double")]
    [InlineData("TypeName(-2147483648*(-1))", "Double")]
    [InlineData("TypeName(-+2147483648)", "Decimal")]
    [InlineData("TypeName(--2147483648)", "Double")]
    [InlineData("TypeName(-2147483648^1)", "Double")]
    [InlineData("TypeName(Abs(1))", "Long")]
    [InlineData("TypeName(Int(1))", "Long")]
    [InlineData("TypeName(Fix(1))", "Long")]
    [InlineData("TypeName(Abs(CLng(-2147483648)))", "Double")]
    [InlineData("CStr(1.1+1.1)", "2.2")]
    [InlineData("CStr(1.10)", "1.1")]
    [InlineData("CStr(1.100)", "1.1")]
    [InlineData("CStr(0.5000)", "0.5")]
    [InlineData("CStr(1.10+0.20)", "1.3")]
    [InlineData("CStr(1.5+1.5)", "3")]
    [InlineData("CStr(1.2300E-1)", "0.123")]
    public void IntegralArithmetic_RetainsNativePromotion(string expression, string expected)
        => Assert.Equal(expected, Assert.IsType<string>(Evaluate(expression)));

    [Fact]
    public async Task LiteralConversion_DefaultAndCalculatedCacheSurviveInsertUpdateAndReopen()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Factor", typeof(int)),
                    new ColumnDefinition("Calculated", typeof(double)) { IsCalculated = true, CalculationExpression = "CDbl(99.99999999999999)*[Factor]" },
                    new ColumnDefinition("DefaultValue", typeof(double)) { DefaultValueExpression = "CDbl(99.99999999999999)" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, DBNull.Value, DbDefault.Value], TestContext.Current.CancellationToken);
            int updated = await writer.UpdateRowsAsync(
                "T", "Factor", 1, new Dictionary<string, object?> { ["Factor"] = 2 }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(2, row[0]);
            Assert.Equal(200d, row[1]);
            Assert.Equal(100d, row[2]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_LiteralsAndStoredDefaultsMatchIndependentNativeExpressions()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NumericLiterals", databaseExtension: ".mdb");
        string[] expressions =
        [
            "0", "1", "1.0", "1E0", "1E+3", "2147483647", "-2147483648", "-2147483648.0", "-2.147483648E9", "-(2147483648)", "-2147483649", ".5", "1e-3", "2147483648",
            "99.99999999999999", "1E-28", "79228162514264337593543950335", "1E-29", "1E29",
            "0.12345678901234567890123456789", "0.00000000000000000000000000011", "79228162514264337593543950336",
            "CDbl(99.99999999999999)", "CDbl(99.9999999999999900)", "CDbl(0.9999999999999999)", "CDbl(1.2345678901234567)",
            "1+1", "32767+1", "CInt(1)+1", "1+True", "2147483647+1", "2147483647-(-1)",
            "2147483647*2147483647", "2147483648+1", "1.1+1.1", "Abs(1)", "Int(1)", "Fix(1)", "Abs(CLng(-2147483648))",
            "-2147483648-1", "-2147483648*(-1)", "-+2147483648", "--2147483648", "-2147483648^1",
            "1.10", "1.100", "0.5000", "1.10+0.20", "1.5+1.5", "1.2300E-1",
        ];
        string nativeExpressions = string.Join(",", Array.ConvertAll(expressions, AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        var conversionRows = new List<string>();
        foreach (TheoryDataRow<string, long, int> row in NativeDecimalConversionData)
        {
            (string text, long doubleBits, int singleBits) = row.Data;
            conversionRows.Add(string.Create(CultureInfo.InvariantCulture, $"@('{text}',{doubleBits}L,{singleBits})"));
        }

        string nativeConversions = string.Join(",", conversionRows);
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',64)
            try {
                foreach($expression in @({{nativeExpressions}})) {
                    $rs=$db.OpenRecordset('SELECT TypeName('+ $expression +') AS [T],CStr('+ $expression +') AS [S]')
                    try { Write-Output ($expression+'|'+$rs.Fields('T').Value+'|'+$rs.Fields('S').Value) }
                    finally { $rs.Close() }
                }
                foreach($expression in @('CDbl(99.99999999999999)','CDbl(0.9999999999999999)','CDbl(1.2345678901234567)')) {
                    $rs=$db.OpenRecordset('SELECT '+$expression+' AS [V]')
                    try { Write-Output ('BITS|'+$expression+'|'+[BitConverter]::DoubleToInt64Bits($rs.Fields('V').Value)) }
                    finally { $rs.Close() }
                }
                foreach($row in @({{nativeConversions}})) {
                    $rs=$db.OpenRecordset('SELECT TypeName('+$row[0]+') AS [T],CDbl('+$row[0]+') AS [D],CSng('+$row[0]+') AS [S]')
                    try {
                        if($rs.Fields('T').Value -cne 'Decimal') { throw 'Conversion source is not Decimal' }
                        $doubleBits=[BitConverter]::DoubleToInt64Bits($rs.Fields('D').Value)
                        $singleBits=[BitConverter]::ToInt32([BitConverter]::GetBytes([single]$rs.Fields('S').Value),0)
                        if($doubleBits -ne $row[1] -or $singleBits -ne $row[2]) { throw 'Native Decimal conversion bits changed' }
                        Write-Output ('CONVERSION|'+$row[0])
                    } finally { $rs.Close() }
                }
                $db.Execute('CREATE TABLE [T] ([I] TEXT(255),[F] TEXT(255),[V] DOUBLE)')
                $table=$db.TableDefs('T')
                $table.Fields('I').DefaultValue='TypeName(1)'
                $table.Fields('F').DefaultValue='TypeName(1.1)'
                $table.Fields('V').DefaultValue='CDbl(99.99999999999999)'
                $rs=$db.OpenRecordset('T',2)
                try {
                    $rs.AddNew(); $rs.Update(); $rs.MoveFirst()
                    Write-Output ('DEFAULTS|'+$rs.Fields('I').Value+'|'+$rs.Fields('F').Value+'|'+[BitConverter]::DoubleToInt64Bits($rs.Fields('V').Value))
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == 0, result.StandardOutput + result.StandardError);
        string[] lines = result.StandardOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(expressions.Length + conversionRows.Count + 4, lines.Length);
        foreach (string expression in expressions)
        {
            Assert.Contains(expression + "|" + Evaluate("TypeName(" + expression + ")") + "|" + Evaluate("CStr(" + expression + ")"), lines);
        }

        Assert.Contains("DEFAULTS|Long|Decimal|4636737291354636288", lines);
        Assert.Contains("BITS|CDbl(99.99999999999999)|4636737291354636288", lines);
        Assert.Contains("BITS|CDbl(0.9999999999999999)|4607182418800017408", lines);
        Assert.Contains("BITS|CDbl(1.2345678901234567)|4608238818662570491", lines);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [DbDefault.Value, DbDefault.Value, DbDefault.Value], TestContext.Current.CancellationToken);
        }

        string compactedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        PowerShellRunResult compacted = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{path}})
            try {
                $rs=$db.OpenRecordset('SELECT [I],[F],[V] FROM [T]')
                try {
                    $count=0
                    while(!$rs.EOF) {
                        if($rs.Fields('I').Value -cne 'Long' -or $rs.Fields('F').Value -cne 'Decimal' -or [BitConverter]::DoubleToInt64Bits($rs.Fields('V').Value) -ne 4636737291354636288L) { throw 'Native and writer defaults differ' }
                        $count++; $rs.MoveNext()
                    }
                    if($count -ne 2) { throw 'Native and writer rows are missing' }
                } finally { $rs.Close() }
                $db.Execute('UPDATE [T] SET [V]=CDbl(99.99999999999999)*2')
            } finally { $db.Close() }
            $engine.CompactDatabase({{path}},{{compactedPath}})
            Write-Output 'DEFAULTS_READ_UPDATED_COMPACTED'
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(compacted.TimedOut);
        Assert.True(compacted.OutputComplete);
        Assert.True(compacted.ExitCode == 0, compacted.StandardOutput + compacted.StandardError);
        Assert.Contains("DEFAULTS_READ_UPDATED_COMPACTED", compacted.StandardOutput, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("Long", row[0]);
            Assert.Equal("Decimal", row[1]);
            Assert.Equal(200d, row[2]);
            count++;
        }

        Assert.Equal(2, count);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_DecimalFieldConversionRetainsStoredScale()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.DecimalFieldConversion", databaseExtension: ".mdb");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',64)
            $db.Close()
            $connection=New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.Jet.OLEDB.4.0;Data Source='+{{path}})
                $connection.Execute('CREATE TABLE [T] ([Id] LONG,[N] DECIMAL(28,16))') | Out-Null
            } finally {
                $connection.Close()
                [Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null
            }
            $db=$engine.OpenDatabase({{path}})
            try {
                $id=0
                foreach($value in @('99.99999999999999','0.9999999999999999','1.2345678901234567')) {
                    $id++
                    $db.Execute('INSERT INTO [T] VALUES ('+$id+','+$value+')')
                }
                $rs=$db.OpenRecordset('SELECT [Id],CDbl([N]) AS [D],CSng([N]) AS [S],CStr([N]) AS [Text] FROM [T]')
                try {
                    while(!$rs.EOF) {
                        $doubleBits=[BitConverter]::DoubleToInt64Bits($rs.Fields('D').Value)
                        $singleBits=[BitConverter]::ToInt32([BitConverter]::GetBytes([single]$rs.Fields('S').Value),0)
                        Write-Output ('FIELD|'+$rs.Fields('Id').Value+'|'+$doubleBits+'|'+$singleBits+'|'+$rs.Fields('Text').Value)
                        $rs.MoveNext()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == 0, result.StandardOutput + result.StandardError);
        string[] lines = result.StandardOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(3, lines.Length);
        Assert.Contains("FIELD|1|4636737291354636287|1120403456|99.99999999999999", lines);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            decimal number = Assert.IsType<decimal>(row[1]);
            Assert.Equal(16, (decimal.GetBits(number)[3] >> 16) & 0xFF);
            long doubleBits = BitConverter.DoubleToInt64Bits(CalculatedExpressionCoercion.ToDouble(number));
            int singleBits = BitConverter.SingleToInt32Bits(CalculatedExpressionCoercion.ToSingle(number));
            Assert.Contains(string.Create(CultureInfo.InvariantCulture, $"FIELD|{row[0]}|{doubleBits}|{singleBits}|{CalculatedExpressionCoercion.ToText(number)}"), lines);
            count++;
        }

        Assert.Equal(3, count);
    }

    private static object Evaluate(string expression)
        => CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(
            new CalculatedExpressionEvaluationContext(new TableDef(), [], [], force: false));
}
