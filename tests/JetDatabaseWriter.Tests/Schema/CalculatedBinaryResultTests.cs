namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks native scalar-to-binary conversions and calculated binary long values.</summary>
public sealed class CalculatedBinaryResultTests
{
    [Theory]
    [InlineData("True", "FFFF")]
    [InlineData("False", "0000")]
    [InlineData("CInt(-1234)", "2EFB")]
    [InlineData("CLng(-123456789)", "EB32A4F8")]
    [InlineData("CCur(1.2345)", "3930000000000000")]
    [InlineData("CSng(1.25)", "0000A03F")]
    [InlineData("CDbl(1.25)", "000000000000F43F")]
    [InlineData("#1/1/2020 6:30:00 AM#", "ABAAAAAAE866E540")]
    [InlineData("\"ABC\"", "410042004300")]
    [InlineData("\"é漢\"", "E900226F")]
    [InlineData("CDec(\"1.23456789\")", "31002E0032003300340035003600370038003900")]
    public void ScalarResults_ConvertToNativeBytes(string expression, string expectedHex)
    {
        object value = CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
        object result = CalculatedExpressionCoercion.CoerceResult(value, typeof(byte[]), isCalculatedColumn: true);
        Assert.Equal(expectedHex, Convert.ToHexString(Assert.IsType<byte[]>(result)));
    }

    [Fact]
    public void GuidResult_RetainsNativeBinaryIdentity()
    {
        object value = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        object result = CalculatedExpressionCoercion.CoerceResult(value, typeof(byte[]), isCalculatedColumn: true);
        Assert.Equal("33221100554477668899AABBCCDDEEFF", Convert.ToHexString(Assert.IsType<byte[]>(result)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void EmptyBinaryConversion_IsOrdinaryNull(string? value)
        => Assert.Same(DBNull.Value, CalculatedExpressionCoercion.CoerceResult(value, typeof(byte[]), isCalculatedColumn: true));

    [Fact]
    public async Task BinaryAndOleCaches_UseLongValuesWithoutDeclaredSizeTruncation()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", CreateColumns(), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, new string('x', 5000), DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await AssertRowAsync(reader, new string('x', 5000));
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["N"] = new string('z', 8000) }, TestContext.Current.CancellationToken));
            await writer.AddColumnAsync("T", new ColumnDefinition("RolledBack", typeof(int)), TestContext.Current.CancellationToken);
            await transaction.RollbackAsync(TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await AssertRowAsync(reader, new string('x', 5000));
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["N"] = "é漢" }, TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using AccessReader reopened = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await AssertRowAsync(reopened, "é漢");
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_NativeBinaryAndOleCachesReadUpdateAndCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedBinaryNative");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE T (Id LONG,N MEMO)')
                $tdf = $db.TableDefs('T')
                foreach ($case in @(@('B',9),@('O',11))) {
                    $field = $tdf.CreateField($case[0],$case[1],255)
                    $field.Expression = '[N]'
                    $tdf.Fields.Append($field)
                }
                $db.Execute("INSERT INTO T (Id,N) VALUES (1,'"+('x'*5000)+"')")
                $rs = $db.OpenRecordset('T')
                try { foreach ($name in @('B','O')) { if ($rs.Fields($name).Value.Length -ne 10000) { throw 'Native binary source was truncated' } } }
                finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await using (AccessReader reader = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await AssertRowAsync(reader, new string('x', 5000));
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["N"] = "é漢" }, TestContext.Current.CancellationToken));
        }

        string compacted = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult checkedCaches = AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(
            session.SourcePath,
            compacted,
            """
            $rs = $db.OpenRecordset('T',2)
            try {
                foreach ($name in @('B','O')) { if ([Convert]::ToHexString($rs.Fields($name).Value) -ne 'E900226F') { throw 'Writer binary cache differs from native' } }
                $rs.Edit(); $rs.Fields('N').Value = 'é漢'; $rs.Update()
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(checkedCaches.ExitCode == 0, checkedCaches.StdOut + checkedCaches.StdErr);
        await using AccessReader reopened = await AccessReader.OpenAsync(
            compacted, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        await AssertRowAsync(reopened, "é漢");
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_WriterCreatedBinaryAndOleCachesAcceptNativeUpdatesAndCompaction()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedBinaryWriter");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            session.SourcePath, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", CreateColumns(), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, new string('x', 5000), DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync("T", "Extra", "Renamed", TestContext.Current.CancellationToken);
            await writer.DropColumnAsync("T", "Renamed", TestContext.Current.CancellationToken);
        }

        string compacted = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult checkedCaches = AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(
            session.SourcePath,
            compacted,
            """
            $rs = $db.OpenRecordset('T',2)
            try {
                foreach ($name in @('B','O')) { if ($rs.Fields($name).Value.Length -ne 10000) { throw 'Writer long binary cache differs from native' } }
                $rs.Edit(); $rs.Fields('N').Value = 'y'*1025; $rs.Update()
                foreach ($name in @('B','O')) { if ($rs.Fields($name).Value.Length -ne 2050) { throw 'Native binary update truncated the result' } }
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(checkedCaches.ExitCode == 0, checkedCaches.StdOut + checkedCaches.StdErr);
        await using AccessReader reopened = await AccessReader.OpenAsync(
            compacted, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        await AssertRowAsync(reopened, new string('y', 1025));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_ScalarBinaryCachesMatchNativeVariantsAfterWriterRecomputation()
    {
        (string Name, int Type, string Literal, string Hex)[] cases =
        [
            ("Bool", 1, "True", "FFFF"),
            ("BoolFalse", 1, "False", "0000"),
            ("Byte", 2, "42", "2A000000"),
            ("Integer", 3, "-1234", "2EFB"),
            ("Long", 4, "-123456789", "EB32A4F8"),
            ("Currency", 5, "1.2345", "3930000000000000"),
            ("Single", 6, "1.25", "0000A03F"),
            ("Double", 7, "1.25", "000000000000F43F"),
            ("Date", 8, "#2020-01-01 06:30:00#", "ABAAAAAAE866E540"),
            ("Text", 10, "'ABC'", "410042004300"),
            ("Unicode", 10, "'é漢'", "E900226F"),
            ("Decimal", 20, "1.23456789", "31002E0032003300340035003600370038003900"),
            ("Guid", 15, "{guid {00112233-4455-6677-8899-AABBCCDDEEFF}}", "33221100554477668899AABBCCDDEEFF"),
        ];
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedBinaryScalars");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string nativeCases = string.Join(",", cases.Select(static item => $"@('{item.Name}',{item.Type},{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(item.Literal)},'{item.Hex}')"));
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            $db.Close()
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.ACE.OLEDB.12.0;Data Source='+{{path}})
                $connection.Execute('CREATE TABLE [Decimal] (Id LONG,N DECIMAL(28,8))') | Out-Null
            } finally { $connection.Close(); [Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null }
            $db = $engine.OpenDatabase({{path}})
            try {
                foreach ($case in @({{nativeCases}})) {
                    $name = $case[0]
                    if ($case[1] -ne 20) {
                        $tdf = $db.CreateTableDef($name)
                        $tdf.Fields.Append($tdf.CreateField('Id',4))
                        $tdf.Fields.Append($tdf.CreateField('N',$case[1],32))
                        $db.TableDefs.Append($tdf)
                    }
                    $tdf = $db.TableDefs($name)
                    foreach ($result in @(@('B',9,255),@('O',11,0))) {
                        $field = $tdf.CreateField($result[0],$result[1],$result[2])
                        $field.Expression = '[N]'
                        $tdf.Fields.Append($field)
                    }
                    $db.Execute('INSERT INTO ['+$name+'] (Id,N) VALUES (1,'+$case[2]+')',128)
                    $rs = $db.OpenRecordset($name)
                    try { foreach ($field in @('B','O')) { if ([Convert]::ToHexString($rs.Fields($field).Value) -ne $case[3]) { throw ('Native binary conversion changed: '+$name) } } }
                    finally { $rs.Close() }
                }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            foreach ((string Name, int Type, string Literal, string Hex) item in cases)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync(item.Name, "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
            }
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            foreach ((string Name, int Type, string Literal, string Hex) item in cases)
            {
                await foreach (object[] row in reader.Rows(item.Name, cancellationToken: TestContext.Current.CancellationToken))
                {
                    Assert.Equal(item.Hex, Convert.ToHexString(Assert.IsType<byte[]>(row[2])));
                    Assert.Equal(item.Hex, Convert.ToHexString(Assert.IsType<byte[]>(row[3])));
                }
            }
        }

        AccessRoundTripEnvironment.CompactResult checkedCaches = AccessRoundTripEnvironment.RunDaoDatabaseScript(
            session.SourcePath,
            $$"""
            foreach ($case in @({{nativeCases}})) {
                $rs = $db.OpenRecordset($case[0])
                try { foreach ($field in @('B','O')) { if ([Convert]::ToHexString($rs.Fields($field).Value) -ne $case[3]) { throw ('Writer binary conversion differs: '+$case[0]) } } }
                finally { $rs.Close() }
            }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(checkedCaches.ExitCode == 0, checkedCaches.StdOut + checkedCaches.StdErr);
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task Dao120_BinaryErrorsRetainNativeStatusesAndRepair()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedBinaryErrors");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE T (Id LONG,N DOUBLE)',128)
                $db.TableDefs.Refresh()
                foreach($item in @(@('B',9),@('O',11))) {
                    $field=$db.TableDefs('T').CreateField($item[0],$item[1])
                    $field.Expression='1/[N]'
                    $db.TableDefs('T').Fields.Append($field)
                }
                $db.Execute('INSERT INTO T (Id,N) VALUES (1,0)',128)
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await AssertBinaryErrorsAsync(session.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
        }

        await AssertBinaryErrorsAsync(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult compacted = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs=$db.OpenRecordset('SELECT * FROM T')
            try {foreach($name in @('B','O')) {if($null -ne $rs.Fields($name).Value){throw 'Expected native calculation error'}}} finally {$rs.Close()}
            $db.Execute('UPDATE T SET N=2',128)
            $rs=$db.OpenRecordset('SELECT * FROM T')
            try {foreach($name in @('B','O')) {if([Convert]::ToHexString($rs.Fields($name).Value) -ne '000000000000E03F'){throw 'Native binary repair differs'}}} finally {$rs.Close()}
            $db.Execute('UPDATE T SET N=0',128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(compacted.ExitCode == 0, compacted.StdOut + compacted.StdErr);
        await AssertBinaryErrorsAsync(session.CompactedPath);
    }

    private static async Task AssertBinaryErrorsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        Assert.Equal(11, Assert.IsType<CalculatedError>(row[2]).NativeCode);
        Assert.Equal(11, Assert.IsType<CalculatedError>(row[3]).NativeCode);
        string[] strings = Assert.Single(await reader.RowsAsStrings("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        Assert.Equal("#Error(11)", strings[2]);
        Assert.Equal("#Error(11)", strings[3]);
        using System.Data.DataTable table = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(11, Assert.IsType<CalculatedError>(table.Rows[0][2]).NativeCode);
    }
    private static ColumnDefinition[] CreateColumns() =>
    [
        new("Id", typeof(int)),
        new("N", typeof(string)) { ColumnTypeOverride = ColumnType.MemoType },
        new("B", typeof(byte[]), 255) { CalculatedResultType = (byte)ColumnType.BinaryType, IsCalculated = true, CalculationExpression = "[N]" },
        new("O", typeof(byte[])) { CalculatedResultType = (byte)ColumnType.OleType, IsCalculated = true, CalculationExpression = "[N]" },
    ];

    private static async Task AssertRowAsync(AccessReader reader, string expected)
    {
        byte[] bytes = Encoding.Unicode.GetBytes(expected);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(bytes, Assert.IsType<byte[]>(row[2]));
            Assert.Equal(bytes, Assert.IsType<byte[]>(row[3]));
            count++;
        }

        Assert.Equal(1, count);
        await foreach (string[] row in reader.RowsAsStrings("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(Convert.ToHexString(bytes), row[2]);
            Assert.EndsWith(Convert.ToBase64String(bytes), row[3], StringComparison.Ordinal);
        }
    }
}
