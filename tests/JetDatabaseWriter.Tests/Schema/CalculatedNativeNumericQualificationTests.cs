namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Rechecks older query numeric controls in actual ACE table calculations.</summary>
/// <remarks>
/// Native controls use DAO.DBEngine.120 version 16.0 and Access Runtime
/// 16.0.20527.20112 on an en-US Windows profile. Original caches are authored
/// before any library mutation; native source values, subtype bytes and both
/// native compacted images remain independent of the writer under test.
/// Table-forbidden conversion calls from the older query matrices are replaced
/// by typed ordinary source fields, including native Byte-to-Long projection.
/// </remarks>
public sealed partial class CalculatedNativeNumericQualificationTests
{
    private const string AssignSources = """
        function Set-SourceValues($record,$values) {
            foreach($column in $values.Keys) {
                $field=$record.Fields($column)
                switch([int]$field.Type) {
                    2 { $field.Value=[byte]$values[$column] }
                    3 { $field.Value=[short]$values[$column] }
                    4 { $field.Value=[int]$values[$column] }
                    6 { $field.Value=[single]$values[$column] }
                    7 { $field.Value=[double]$values[$column] }
                    8 { $field.Value=[datetime]$values[$column] }
                    20 { $field.Value=[decimal]$values[$column] }
                    default { throw ('Unexpected source type '+$field.Type) }
                }
            }
        }
        """;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("Literals")]
    [InlineData("Integral")]
    [InlineData("Floating")]
    [InlineData("Round")]
    [InlineData("DecimalCoercion")]
    [InlineData("DecimalPromotion")]
    [InlineData("DateDecimal")]
    [InlineData("SignedZero")]
    [InlineData("BooleanText")]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_NumericCachesMatchIndependentAuthoringRecalculationUpdateAndCompact(string group)
    {
        NativeCase[] cases = CreateCases(group).ToArray();
        Assert.NotEmpty(cases);
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.NativeNumeric." + group);
        string writerPath = session.CreateDatabasePath("writer");
        string writerCompacted = session.CreateDatabasePath("writer-compacted");
        string source = Quote(session.SourcePath);
        string subject = Quote(writerPath);
        string definitions = string.Join(',', cases.Select((item, index) => item.ToPowerShell(index, updated: false)));
        string updatedDefinitions = string.Join(',', cases.Select((item, index) => item.ToPowerShell(index, updated: true)));
        AccessRoundTripEnvironment.CompactResult authored = session.RunDaoEngineScript(
            $$"""
            {{AssignSources}}
            $cases=@({{definitions}})
            $connection=New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.ACE.OLEDB.12.0;Data Source='+{{source}})
                foreach($case in $cases) { $connection.Execute($case.Ddl) | Out-Null }
                if({{Quote(group)}} -eq 'Literals') { $connection.Execute('CREATE TABLE NativeNumericLimit (Id LONG,N DECIMAL(28,0))') | Out-Null }
            } finally { $connection.Close(); [void][Runtime.InteropServices.Marshal]::ReleaseComObject($connection) }
            $db=$engine.OpenDatabase({{source}})
            try {
                if({{Quote(group)}} -eq 'Literals') {
                    $rs=$db.OpenRecordset('NativeNumericLimit',2)
                    try {
                        $rs.AddNew(); $rs.Fields('Id').Value=1
                        $refused=$false
                        try { $rs.Fields('N').Value=[decimal]::MaxValue; $rs.Update() }
                        catch { foreach($errorItem in $engine.Errors) { if($errorItem.Number -eq 3761) { $refused=$true } } }
                        if(!$refused) { throw 'Native Numeric precision did not refuse Decimal.MaxValue with 3761' }
                    } finally { $rs.Close() }
                    $rs=$db.OpenRecordset('SELECT COUNT(*) AS C FROM NativeNumericLimit')
                    try { if($rs.Fields('C').Value -ne 0) { throw 'Refused Numeric source fabricated a row' } } finally { $rs.Close() }
                }

                foreach($case in $cases) {
                    $tdf=$db.TableDefs($case.Name)
                    foreach($result in @(@('B',9,255,$case.Expression),@('V',7,0,$case.Expression),@('T',10,0,('('+ $case.Expression +') & ""')),@('DirectText',10,0,$case.Expression))) {
                        $field=$tdf.CreateField($result[0],[int]$result[1],[int]$result[2])
                        $field.Expression=[string]$result[3]
                        try { $tdf.Fields.Append($field) }
                        catch { throw ('Append '+$case.Name+'.'+$result[0]+' = '+$field.Expression+': '+$_.Exception.Message) }
                    }
                    $rs=$db.OpenRecordset($case.Name,2)
                    try {
                        $rs.AddNew(); $rs.Fields('Id').Value=1
                        Set-SourceValues $rs $case.Values
                        $rs.Update()
                    } catch { throw ($case.Name+': '+$case.Expression+': '+$_.Exception.Message) }
                    finally { $rs.Close() }
                    if({{Quote(group)}} -eq 'DateDecimal') {
                        $expectedKind=if($case.Expression.Contains('^')){'Double'}else{'Decimal'}
                        $rs=$db.OpenRecordset('SELECT TypeName('+$case.Expression+') AS K FROM ['+$case.Name+']')
                        try { if($rs.Fields('K').Value -ne $expectedKind) { throw 'Native query/table promotion context changed' } } finally { $rs.Close() }
                        foreach($missing in @('D','N')) {
                            $db.Execute('UPDATE ['+$case.Name+'] SET ['+$missing+']=Null',128)
                            $rs=$db.OpenRecordset('SELECT ['+$missing+'],[B],TypeName('+$case.Expression+') AS K FROM ['+$case.Name+']')
                            try {
                                $sourceNull=$null -eq $rs.Fields($missing).Value -or $rs.Fields($missing).Value -is [DBNull]
                                $cacheNull=$null -eq $rs.Fields('B').Value -or $rs.Fields('B').Value -is [DBNull]
                                if(!$sourceNull -or !$cacheNull -or $rs.Fields('K').Value -ne 'Null') { throw ('Native Date/Decimal Null control changed: '+$case.Name+' '+$case.Expression+' missing='+$missing) }
                            } finally { $rs.Close() }
                            $rs=$db.OpenRecordset($case.Name,2)
                            try { $rs.Edit(); Set-SourceValues $rs $case.Values; $rs.Update() } finally { $rs.Close() }
                        }
                    }
                }
            } finally { $db.Close() }
            Copy-Item -LiteralPath {{source}} -Destination {{subject}}
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        object[][] original = await ReadRowsAsync(session.SourcePath, cases);
        AssertSourceValues(cases, original, updated: false);
        File.Copy(session.SourcePath, session.CreateDatabasePath("native-original"));

        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < cases.Length; index++)
            {
                NativeCase item = cases[index];
                Assert.Equal(1, await writer.UpdateRowsAsync("T" + index, "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
                object?[] values = new object?[] { 3 }.Concat(item.Sources.Select(static column => column.Value)).Concat([null, null, null, null]).ToArray();
                await writer.InsertRowAsync("T" + index, values, TestContext.Current.CancellationToken);
            }
        }

        object[][] recomputed = await ReadRowsAsync(writerPath, cases, rowsPerTable: 2);
        for (int index = 0; index < cases.Length; index++)
        {
            AssertRow(cases[index], original[index], recomputed[index * 2]);
            AssertRow(cases[index], original[index], recomputed[(index * 2) + 1]);
        }

        AccessRoundTripEnvironment.CompactResult nativeUpdate = session.RunDaoDatabaseScript(
            session.SourcePath,
            $$"""
            {{AssignSources}}
            foreach($case in @({{updatedDefinitions}})) {
                $rs=$db.OpenRecordset($case.Name,2)
                try {
                    $rs.Edit(); $rs.Fields('Id').Value=2
                    Set-SourceValues $rs $case.Values
                    $rs.Update()
                } finally { $rs.Close() }
            }
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(nativeUpdate.ExitCode == 0, nativeUpdate.StdOut + nativeUpdate.StdErr);
        object[][] updated = await ReadRowsAsync(session.SourcePath, cases);
        AssertSourceValues(cases, updated, updated: true);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < cases.Length; index++)
            {
                Dictionary<string, object?> changes = cases[index].Sources.ToDictionary(static column => column.Name, static column => (object?)column.Updated);
                changes["Id"] = 2;
                Assert.Equal(1, await writer.UpdateRowsAsync("T" + index, "Id", 2, changes, TestContext.Current.CancellationToken));
            }
        }

        object[][] writerUpdated = await ReadRowsAsync(writerPath, cases, rowsPerTable: 2);
        for (int index = 0; index < cases.Length; index++)
        {
            AssertRow(cases[index], updated[index], writerUpdated[index * 2]);
        }

        string expectedCaches = string.Join(',', writerUpdated.Select((row, index) => $"@('T{index / 2}',{row[0]},'{Convert.ToHexString(Assert.IsType<byte[]>(row[^4]))}')"));
        string compactedSource = Quote(session.CompactedPath);
        string compactedSubject = Quote(writerCompacted);
        AccessRoundTripEnvironment.CompactResult compacted = session.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{subject}})
            try {
                foreach($case in @({{expectedCaches}})) {
                    $rs=$db.OpenRecordset('SELECT * FROM ['+$case[0]+'] WHERE [Id]='+$case[1],2)
                    try {
                        if([Convert]::ToHexString($rs.Fields('B').Value) -ne $case[2]) { throw ('Native read disagrees with writer cache: '+$case[0]) }
                        $rs.Edit(); $rs.Fields('Id').Value=$case[1]; $rs.Update()
                        if([Convert]::ToHexString($rs.Fields('B').Value) -ne $case[2]) { throw ('Native recalculation disagrees with writer cache: '+$case[0]) }
                    } finally { $rs.Close() }
                }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compactedSource}})
            $engine.CompactDatabase({{subject}},{{compactedSubject}})
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(compacted.ExitCode == 0, compacted.StdOut + compacted.StdErr);
        object[][] nativeCompacted = await ReadRowsAsync(session.CompactedPath, cases);
        object[][] writerCompactedRows = await ReadRowsAsync(writerCompacted, cases, rowsPerTable: 2);
        for (int index = 0; index < cases.Length; index++)
        {
            AssertRow(cases[index], updated[index], nativeCompacted[index]);
            AssertRow(cases[index], nativeCompacted[index], writerCompactedRows[index * 2]);
            AssertRow(cases[index], original[index], writerCompactedRows[(index * 2) + 1]);
        }
    }

    private static IEnumerable<NativeCase> CreateCases(string group)
    {
        switch (group)
        {
            case "Literals":
                foreach (string expression in new[] { "0", "1", "1.0", "1E0", "1E+3", "2147483647", "-2147483648", "-2147483648.0", "-2.147483648E9", "-(2147483648)", "-2147483649", ".5", "1e-3", "2147483648", "99.99999999999999", "1E-28", "79228162514264337593543950335", "1E-29", "1E29", "0.12345678901234567890123456789", "0.00000000000000000000000000011", "79228162514264337593543950336", "CDbl(99.99999999999999)", "CDbl(99.9999999999999900)", "CDbl(0.9999999999999999)", "CDbl(1.2345678901234567)", "1+1", "32767+1", "1+True", "2147483647+1", "2147483647-(-1)", "2147483647*2147483647", "2147483648+1", "-2147483648-1", "-2147483648*(-1)", "-+2147483648", "--2147483648", "-2147483648^1", "1.1+1.1", "1.10", "1.100", "0.5000", "1.10+0.20", "1.5+1.5", "1.2300E-1" })
                {
                    yield return new(expression, []);
                }

                foreach (TheoryDataRow<string, long, int> row in CalculatedExpressionNumericLiteralTests.NativeDecimalConversionData)
                {
                    yield return new("CDbl(" + row.Data.Item1 + ")", []);
                    decimal number = decimal.Parse(row.Data.Item1, NumberStyles.Float, CultureInfo.InvariantCulture);

                    // ACE source Numeric fields admit at most 28 coefficient digits;
                    // the wider Decimal Variant remains covered as a literal above.
                    if (Math.Abs(number) < 10000000000000000000000000000m)
                    {
                        yield return new("CDbl([N])", [new("N", number, -number)]);
                    }
                }

                break;
            case "Integral":
                foreach (TheoryDataRow<string, string> row in CalculatedExpressionIntegralSubtypeTests.NativeExpressions)
                {
                    yield return ReplaceConversions(row.Data.Item1);
                }

                break;
            case "Floating":
                foreach (string expression in new[] { "1/3", "CSng(1)/CSng(3)", "CSng(1)/CInt(3)", "CSng(1)/CLng(3)", "CSng(1)/CDbl(3)", "CByte(1)/CByte(3)", "CInt(1)/CInt(3)", "CSng(1)+True", "CSng(1)/True", "CSng(1)+\"2\"", "CSng(1)/\"3\"", "CSng(1)+1", "CSng(1)+CDbl(3)", "CSng(1)+CSng(3)", "CSng(1)*CSng(3)", "CSng(1)-CSng(3)", "-CSng(1)", "+CSng(1)", "Abs(CSng(-1))", "Int(CSng(1.5))", "Fix(CSng(1.5))", "CSng(3.4E38)*CSng(2)", "CDbl(1000000000000000)+1", "CDbl(0.1)+CDbl(0.2)", "CDbl(1E-15)", "CDbl(1E-16)", "CSng(1E-5)", "CSng(1E-9)", "CDbl(1.23456789012345E-5)", "-CDbl(0)", "CDbl(1.23456789012345E-4)", "CDbl(1.23456789012345E-3)", "CDbl(-1.23456789012345E-4)", "CDbl(9.99999999999999E-4)", "CDbl(1.234567E-4)", "CSng(1.234567E-3)", "CSng(1.234567E-4)", "CSng(1.23456789012345E-4)", "CSng(9.99999999999999E-4)", "CDbl(1E100)*CDbl(1E100)", "CDbl(1E100)+CDbl(1E100)", "-CDbl(1E100)", "Abs(CDbl(-1E100))", "Int(CDbl(1E100))", "Fix(CDbl(-1E100))" })
                {
                    yield return ReplaceConversions(expression);
                }

                break;
            case "Round":
                foreach (TheoryDataRow<long, int, long> row in CalculatedExpressionRoundTests.FloatingBoundaryData)
                {
                    double value = BitConverter.Int64BitsToDouble(row.Data.Item1);
                    yield return new("Round([F],[P])", [new("F", value, -value), new("P", row.Data.Item2, row.Data.Item2)]);
                }

                foreach (string expression in new[] { "Round(CSng(1.225),2)", "Round(CDbl(1.225),2)", "Round(CDbl(-1.225),2)", "Round(CDbl(2.685),2)", "Round(CDbl(-2.685),2)", "Round(CInt(1),23)", "Round(CLng(1),2147483647)", "Round(CByte(1))", "Round(True)", "Round(\"1.25\",1)", "Round(CDbl(1.25),0.5)", "Round(CDbl(1.25),1.5)", "Round(CDbl(1.25),-0.5)", "Round(CDbl(1.23456E-20),20)", "Round(CDbl(1.23456E-20),22)", "Round(CDec(1.225),0)", "Round(CDec(1.225),2)", "Round(CDec(1.225),28)", "Round(CDec(1.225),29)", "Round(CDec(1.225),2147483647)" })
                {
                    yield return ReplaceConversions(expression);
                }

                break;
            case "DecimalCoercion":
                foreach (TheoryDataRow<long, string> row in CalculatedExpressionDecimalCoercionTests.DoubleCases)
                {
                    double value = BitConverter.Int64BitsToDouble(row.Data.Item1);
                    yield return new("[N]+[F]", [new("F", value, -value), new("N", 0.0000000000000000000000000000m, 0.0000000000000000000000000000m)]);
                }

                foreach (TheoryDataRow<int, string> row in CalculatedExpressionDecimalCoercionTests.SingleCases)
                {
                    float value = BitConverter.Int32BitsToSingle(row.Data.Item1);
                    yield return new("[N]+[F]", [new("F", value, -value), new("N", 0.0000000000000000000000000000m, 0.0000000000000000000000000000m)]);
                }

                break;
            case "DecimalPromotion":
                foreach (TheoryDataRow<string, decimal, long> row in CalculatedExpressionDecimalPromotionTests.NativeCases)
                {
                    double value = BitConverter.Int64BitsToDouble(5039527983027585024L);
                    yield return new(row.Data.Item1, [new("F", value, -value), new("N", row.Data.Item2, row.Data.Item2)]);
                    yield return new(row.Data.Item1, [new("F", (float)value, (float)-value), new("N", row.Data.Item2, row.Data.Item2)]);
                }

                break;
            case "DateDecimal":
                foreach (string expression in new[] { "[D]+[N]", "[N]+[D]", "[D]-[N]", "[N]-[D]", "[D]*[N]", "[N]*[D]", "[D]/[N]", "[N]/[D]", "[D]^[N]", "[N]^[D]" })
                {
                    yield return new(expression, [new("D", new DateTime(1899, 12, 31, 12, 0, 0), new DateTime(1899, 12, 29, 12, 0, 0)), new("N", 2m, 2m)]);
                }

                foreach (string expression in new[] { "[D]+[N]", "[N]+[D]", "[D]-[N]", "[N]-[D]", "[D]*[N]", "[N]*[D]", "[D]/[N]", "[N]/[D]" })
                {
                    yield return new(expression, [new("D", new DateTime(1899, 12, 31, 12, 0, 0), new DateTime(1899, 12, 29, 12, 0, 0)), new("N", 1000000000000000000000000000m, 1000000000000000000000000000m)]);
                }

                break;
            case "SignedZero":
                foreach (string expression in new[] { "Str(\"-0\")", "Str(\"1E100\")", "Str(\"1.2345678901234567\")", "Str(\"1,5\")" })
                {
                    yield return new(expression, []);
                }

                foreach (string expression in new[] { "[D]", "+[D]", "-[D]", "-CDbl(0)", "CDbl(0)", "-0", "Str([D])", "Str(-CDbl(0))", "Str$([D])", "[D]&\"\"", "Left([D],2)", "Right([D],1)", "Mid([D],1,2)", "Len([D])", "Asc([D])", "String(2,[D])", "PV(0,1,0)", "PV(0,1,0)&\"\"", "Str(PV(0,1,0))", "Round([D],2)", "Int([D])", "Fix([D])", "Abs([D])", "[S]&\"\"", "Str([S])", "Left([S],2)", "[D]=\"-0\"", "[D] Like \"-0\"", "UCase([D])", "Trim([D])", "InStr(1,[D],\"-\")" })
                {
                    yield return new(expression, [new("D", BitConverter.Int64BitsToDouble(long.MinValue), 0d), new("S", BitConverter.Int32BitsToSingle(int.MinValue), 0f)]);
                }

                break;
            case "BooleanText":
                foreach (string expression in new[] { "True", "False", "True And True", "1 And 2", "2 Or 4", "Not 1", "[D]=0", "[D] Like \"-0\"", "IsNull([D])", "IsNull(Null)", "IsEmpty([D])", "[D] In(\"-0\")", "[D] In(\"0\")", "[D] Not In(\"-0\")", "\"-0\" In([D])", "[D] In(Null,\"-0\")", "[D] In(Null,\"0\")", "IIf(1=1,True,False)", "IIf(True,IsNull(Null),False)", "Choose(1,True,False)", "Choose(1,IsNull(Null),False)", "Len(True)", "Left(True,5)", "Asc(True)", "UCase(True)", "Len(IsNull(Null))", "Left(IsNull(Null),4)" })
                {
                    yield return new(expression, [new("D", BitConverter.Int64BitsToDouble(long.MinValue), 0d)]);
                }

                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(group));
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_StoredBooleanReferencesUseIntegerTextWhileInlineIntrinsicsRetainBoolean()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.NativeBooleanReference");
        string writerPath = session.CreateDatabasePath("writer");
        string source = Quote(session.SourcePath);
        string subject = Quote(writerPath);
        AccessRoundTripEnvironment.CompactResult authored = session.RunDaoDatabaseScript(
            session.SourcePath,
            """
            $db.Execute('CREATE TABLE T0 (Id LONG)',128)
            $tdf=$db.TableDefs('T0')
            foreach($case in @(@('Stored',1,'IsNull(Null)'),@('InlineText',10,'IsNull(Null) & ""'),@('ReferenceText',10,'[Stored] & ""'),@('B',9,'[Stored]'))) {
                $field=$tdf.CreateField($case[0],[int]$case[1],243)
                $field.Expression=$case[2]
                $tdf.Fields.Append($field)
            }
            $db.Execute('INSERT INTO T0 (Id) VALUES (1)',128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        NativeCase[] cases = [new("stored Boolean reference", [])];
        object[][] expected = await ReadRowsAsync(session.SourcePath, cases);
        Assert.True(Assert.IsType<bool>(expected[0][1]));
        Assert.Equal("True", expected[0][2]);
        Assert.Equal("-1", expected[0][3]);
        Assert.Equal(new byte[] { 255, 255 }, Assert.IsType<byte[]>(expected[0][4]));
        File.Copy(session.SourcePath, session.CreateDatabasePath("native-original"));
        File.Copy(session.SourcePath, writerPath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T0", "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
        }

        AssertRow(cases[0], expected[0], (await ReadRowsAsync(writerPath, cases))[0]);
        string writerCompacted = session.CreateDatabasePath("writer-compacted");
        AccessRoundTripEnvironment.CompactResult compacted = session.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{subject}})
            try { $db.Execute('UPDATE T0 SET Id=2',128) } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{Quote(session.CompactedPath)}})
            $engine.CompactDatabase({{subject}},{{Quote(writerCompacted)}})
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(compacted.ExitCode == 0, compacted.StdOut + compacted.StdErr);
        AssertRow(cases[0], expected[0], (await ReadRowsAsync(session.CompactedPath, cases))[0]);
        AssertRow(cases[0], expected[0], (await ReadRowsAsync(writerCompacted, cases))[0]);
    }

    private static NativeCase ReplaceConversions(string expression)
    {
        var sources = new List<SourceColumn>();
        string converted = ConversionPattern().Replace(expression, match =>
        {
            string name = "S" + sources.Count;
            string text = match.Groups[2].Value;
            object value = match.Groups[1].Value switch
            {
                "CByte" => byte.Parse(text, CultureInfo.InvariantCulture),
                "CInt" => short.Parse(text, CultureInfo.InvariantCulture),
                "CLng" => int.Parse(text, CultureInfo.InvariantCulture),
                "CSng" => float.Parse(text, CultureInfo.InvariantCulture),
                _ => decimal.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture),
            };
            object updated = value switch
            {
                byte number => (byte)(255 - number),
                short number => number == short.MinValue ? short.MaxValue : (short)-number,
                int number => number == int.MinValue ? int.MaxValue : -number,
                float number => -number,
                decimal number => -number,
                _ => throw new InvalidOperationException(),
            };
            sources.Add(new(name, value, updated));
            return "[" + name + "]";
        });
        return new(converted, sources.ToArray());
    }

    [GeneratedRegex(@"\b(CByte|CInt|CLng|CSng|CDec|CCur)\(([-+0-9.Ee]+)\)")]
    private static partial Regex ConversionPattern();

    private static async Task<object[][]> ReadRowsAsync(string path, NativeCase[] cases, int rowsPerTable = 1)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        var rows = new List<object[]>();
        for (int index = 0; index < cases.Length; index++)
        {
            object[][] tableRows = await reader.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
            Assert.Equal(rowsPerTable, tableRows.Length);
            rows.AddRange(tableRows.OrderBy(static row => (int)row[0]));
        }

        return rows.ToArray();
    }

    private static void AssertSourceValues(NativeCase[] cases, object[][] rows, bool updated)
    {
        for (int index = 0; index < cases.Length; index++)
        {
            for (int column = 0; column < cases[index].Sources.Length; column++)
            {
                SourceColumn source = cases[index].Sources[column];
                AssertValue(cases[index].Expression + " source " + source.Name, updated ? source.Updated : source.Value, rows[index][column + 1]);
            }
        }
    }

    private static void AssertRow(NativeCase item, object[] expected, object[] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int column = 1; column < expected.Length; column++)
        {
            AssertValue(item.Expression + " column " + column, expected[column], actual[column]);
        }
    }

    private static void AssertValue(string context, object expected, object actual)
    {
        bool equal = expected switch
        {
            double value => actual is double number && BitConverter.DoubleToInt64Bits(value) == BitConverter.DoubleToInt64Bits(number),
            float value => actual is float number && BitConverter.SingleToInt32Bits(value) == BitConverter.SingleToInt32Bits(number),
            byte[] value => actual is byte[] bytes && value.AsSpan().SequenceEqual(bytes),
            decimal value => actual is decimal number && decimal.GetBits(value).AsSpan().SequenceEqual(decimal.GetBits(number)),
            _ => Equals(expected, actual),
        };
        Assert.True(equal, context + ": expected " + Render(expected) + ", actual " + Render(actual));
    }

    private static string Render(object value) => value is byte[] bytes ? Convert.ToHexString(bytes) : Convert.ToString(value, CultureInfo.InvariantCulture) ?? "Null";

    private static string Quote(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private sealed record NativeCase(string Expression, SourceColumn[] Sources)
    {
        internal string ToPowerShell(int index, bool updated)
        {
            string columns = string.Concat(this.Sources.Select(static column => ",[" + column.Name + "] " + column.SqlType));
            string values = string.Join(';', this.Sources.Select(column => column.Name + "=" + SourceColumn.PowerShellValue(updated ? column.Updated : column.Value)));
            return "@{Name='T" + index + "';Ddl=" + Quote("CREATE TABLE [T" + index + "] ([Id] LONG" + columns + ")") + ";Expression=" + Quote(this.Expression) + ";Values=@{" + values + "}}";
        }
    }

    private sealed record SourceColumn(string Name, object Value, object Updated)
    {
        internal string SqlType => this.Value switch
        {
            byte => "BYTE",
            short => "SHORT",
            int => "LONG",
            float => "SINGLE",
            double => "DOUBLE",
            DateTime => "DATETIME",
            decimal number => "DECIMAL(28," + ((decimal.GetBits(number)[3] >> 16) & 255).ToString(CultureInfo.InvariantCulture) + ")",
            _ => throw new InvalidOperationException(),
        };

        internal static string PowerShellValue(object value) => value switch
        {
            double number => "[BitConverter]::Int64BitsToDouble([long]'" + BitConverter.DoubleToInt64Bits(number).ToString(CultureInfo.InvariantCulture) + "')",
            float number => "[BitConverter]::Int32BitsToSingle([int]'" + BitConverter.SingleToInt32Bits(number).ToString(CultureInfo.InvariantCulture) + "')",
            DateTime date => "[datetime]::new(" + date.Year + "," + date.Month + "," + date.Day + "," + date.Hour + "," + date.Minute + "," + date.Second + ")",
            decimal number => "[decimal]::Parse(" + Quote(number.ToString(CultureInfo.InvariantCulture)) + ",[cultureinfo]::InvariantCulture)",
            _ => Convert.ToString(value, CultureInfo.InvariantCulture)!,
        };
    }
}
