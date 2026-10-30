namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Characterizes stale calculated-field qualifiers after a native table rename.</summary>
public sealed class TableRenamedQualifiedExpressionTests
{
    [Theory]
    [InlineData("Other.Price")]
    [InlineData("Other![Price]")]
    [InlineData("T.Price")]
    [InlineData("T![Price]")]
    public async Task BareQualifier_InsertUpdateAndColumnRenameBindToCurrentRow(string reference)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Price", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Other", [99], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                "Renamed T",
                [
                    new ColumnDefinition("Price", typeof(int)),
                    new ColumnDefinition("Calculated", typeof(int)) { IsCalculated = true, CalculationExpression = reference + " * 2" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Renamed T", [4, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.UpdateRowsAsync("Renamed T", "Price", 4, new Dictionary<string, object?> { ["Price"] = 5 }, TestContext.Current.CancellationToken);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("Renamed T", "Price", TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
            await writer.RenameColumnAsync("Renamed T", "Price", "Cost", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Renamed T", [3, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        var values = new Dictionary<int, int>();
        await foreach (object[] row in reader.Rows("Renamed T", cancellationToken: TestContext.Current.CancellationToken))
        {
            values.Add(Assert.IsType<int>(row[0]), Assert.IsType<int>(row[1]));
        }

        Assert.Equal(2, values.Count);
        Assert.Equal(10, values[5]);
        Assert.Equal(6, values[3]);
    }

    [Theory]
    [InlineData("[Other].[Price]")]
    [InlineData("[Other]![Price]")]
    [InlineData("[Other].Price")]
    public async Task BracketedForeignDeclaration_CreateAndAddRefuseBeforeChangingImage(string reference)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Price", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("T", [4], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        var calculated = new ColumnDefinition("Calculated", typeof(int)) { IsCalculated = true, CalculationExpression = reference + " * 2" };
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync(
            "New T", [new ColumnDefinition("Price", typeof(int)), calculated], TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.AddColumnAsync("T", calculated, TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData("[T].[Price]")]
    [InlineData("[T]![Price]")]
    [InlineData("[T].Price")]
    public async Task StaleBracketedQualifiedCalculation_InsertAndUpdateRetainUncachedState(string reference)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            // Seed persisted stale metadata through the rewrite path, bypassing declaration admission.
            await writer.Services.Schema.CreateTableAsync(
                "Renamed T",
                [
                    new ColumnDefinition("Price", typeof(int)),
                    new ColumnDefinition("Calculated", typeof(int)) { IsCalculated = true, CalculationExpression = reference + " * 2" },
                ],
                [],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Renamed T", [4, 8], TestContext.Current.CancellationToken);
        }

        await AssertWriterPreservesStaleCachesAsync(stream.ToArray());
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("[T].[Price]", true)]
    [InlineData("T.Price", false)]
    [InlineData("[T]![Price]", true)]
    [InlineData("T![Price]", false)]
    [InlineData("[T].Price", true)]
    public async Task NativeTableRename_PreservesQualifierWithSyntaxDependentEvaluation(string reference, bool returnsNull)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TableRenamedQualification");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string expression = reference + " * 2";
        string quotedExpression = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(expression);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [T] ([Price] LONG)')
                $db.TableDefs.Refresh()
                $tdf = $db.TableDefs('T')
                $field = $tdf.CreateField('Calculated', 4)
                $field.Properties('Expression').Value = {{quotedExpression}}
                $tdf.Fields.Append($field)
                $db.Execute('INSERT INTO [T] ([Price]) VALUES (4)', 128)
                $rs = $db.OpenRecordset('SELECT [Calculated] FROM [T]')
                try { Write-Output ('BEFORE=' + [string]$rs.Fields('Calculated').Value) }
                finally { $rs.Close() }
                $tdf.Name = 'Renamed T'
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        Assert.Contains("BEFORE=8", created.StdOut, StringComparison.Ordinal);

        // Stale bracketed references retain their caches and expose COM errors;
        // bare forms bind to the current row and have separate writer coverage.
        if (returnsNull)
        {
            byte[] writerImage = await AssertWriterPreservesStaleCachesAsync(await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken), expectedExisting: 8);
            string writerPath = session.CreateDatabasePath("writer_null");
            await File.WriteAllBytesAsync(writerPath, writerImage, TestContext.Current.CancellationToken);
            AccessRoundTripEnvironment.CompactResult writerRead = session.RunDaoDatabaseScript(
                writerPath,
                """
                $db.Execute('INSERT INTO [Renamed T] ([Price]) VALUES (3)', 128)
                $db.Execute('UPDATE [Renamed T] SET [Price]=6 WHERE [Price]=5', 128)
                $rs = $db.OpenRecordset('SELECT [Price],[Calculated] FROM [Renamed T] ORDER BY [Price]')
                try {
                    while (!$rs.EOF) {
                        $value = $rs.Fields('Calculated').Value
                        if ($null -ne $value) { throw 'Expected native COM error for the stale reference.' }
                        Write-Output ('WRITER_ROW=' + [string]$rs.Fields('Price').Value + ':NULL')
                        $rs.MoveNext()
                    }
                } finally { $rs.Close() }
                """,
                TimeSpan.FromMinutes(1));
            Assert.True(writerRead.ExitCode == 0, $"DAO failed: {writerRead.StdOut}\n{writerRead.StdErr}");
            Assert.Contains("WRITER_ROW=2:NULL", writerRead.StdOut, StringComparison.Ordinal);
            Assert.Contains("WRITER_ROW=3:NULL", writerRead.StdOut, StringComparison.Ordinal);
            Assert.Contains("WRITER_ROW=6:NULL", writerRead.StdOut, StringComparison.Ordinal);
        }

        const string updateScript = """
            Write-Output ('EXPRESSION=' + $db.TableDefs('Renamed T').Fields('Calculated').Properties('Expression').Value)
            $db.Execute('INSERT INTO [Renamed T] ([Price]) VALUES (3)', 128)
            $db.Execute('UPDATE [Renamed T] SET [Price]=5 WHERE [Price]=4', 128)
            $rs = $db.OpenRecordset('SELECT [Price],[Calculated] FROM [Renamed T] ORDER BY [Price]')
            try {
                while (!$rs.EOF) {
                    $value = $rs.Fields('Calculated').Value
                    $text = if ($null -eq $value -or [Convert]::IsDBNull($value)) { 'NULL' } else { [string]$value }
                    Write-Output ('ROW=' + [string]$rs.Fields('Price').Value + ':' + $text)
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult updated = session.RunDaoDatabaseScript(session.SourcePath, updateScript, TimeSpan.FromMinutes(1));
        Assert.True(updated.ExitCode == 0, $"DAO failed: {updated.StdOut}\n{updated.StdErr}");
        Assert.Contains("EXPRESSION=" + expression, updated.StdOut, StringComparison.Ordinal);
        Assert.Contains(returnsNull ? "ROW=3:NULL" : "ROW=3:6", updated.StdOut, StringComparison.Ordinal);
        Assert.Contains(returnsNull ? "ROW=5:NULL" : "ROW=5:10", updated.StdOut, StringComparison.Ordinal);

        if (returnsNull)
        {
            string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
            AccessRoundTripEnvironment.CompactResult rejected = session.RunDaoEngineScript(
                $$"""
                $rejected = $false
                try { $engine.CompactDatabase({{path}}, {{compacted}}) }
                catch [System.Runtime.InteropServices.COMException] {
                    foreach ($errorItem in $engine.Errors) {
                        if ($errorItem.Number -eq 3790) { $rejected = $true }
                    }
                }
                if (!$rejected) { throw 'DAO did not refuse the stale bracketed qualifier with error 3790.' }
                Write-Output 'COMPACT_REJECTED=3790'
                """,
                TimeSpan.FromMinutes(1));
            Assert.True(rejected.ExitCode == 0, $"DAO failed: {rejected.StdOut}\n{rejected.StdErr}");
            Assert.Contains("COMPACT_REJECTED=3790", rejected.StdOut, StringComparison.Ordinal);
            return;
        }

        session.RunDaoCompact();
        const string compactedReadScript = """
            $rs = $db.OpenRecordset('SELECT [Price],[Calculated] FROM [Renamed T] ORDER BY [Price]')
            try {
                while (!$rs.EOF) {
                    Write-Output ('ROW=' + [string]$rs.Fields('Price').Value + ':' + [string]$rs.Fields('Calculated').Value)
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult compactedRead = session.RunDaoDatabaseScript(session.CompactedPath, compactedReadScript, TimeSpan.FromMinutes(1));
        Assert.True(compactedRead.ExitCode == 0, $"DAO failed: {compactedRead.StdOut}\n{compactedRead.StdErr}");
        Assert.Contains("ROW=3:6", compactedRead.StdOut, StringComparison.Ordinal);
        Assert.Contains("ROW=5:10", compactedRead.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Renamed T", TestContext.Current.CancellationToken);
        Assert.Equal(expression, columns[1].CalculationExpression?.TrimEnd('\0'));
        var values = new Dictionary<int, int>();
        await foreach (object[] row in reader.Rows("Renamed T", cancellationToken: TestContext.Current.CancellationToken))
        {
            values.Add(Assert.IsType<int>(row[0]), Assert.IsType<int>(row[1]));
        }

        Assert.Equal(2, values.Count);
        Assert.Equal(6, values[3]);
        Assert.Equal(10, values[5]);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("T.Price")]
    [InlineData("T![Price]")]
    [InlineData("Other.Price")]
    [InlineData("Other![Price]")]
    public async Task NativeBareQualifier_WriterRecalculatesRenamesAndCompactsWithoutForeignLookup(string reference)
    {
        ArgumentNullException.ThrowIfNull(reference);
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeBareQualification");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string expression = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(reference + " * 2");
        string bracketedForeign = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(reference.Replace("Other", "[Other]", StringComparison.Ordinal) + " * 2");
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [T] ([Price] LONG)')
                $db.Execute('CREATE TABLE [Other] ([Price] LONG)')
                $db.Execute('INSERT INTO [Other] ([Price]) VALUES (99)', 128)
                $db.TableDefs.Refresh()
                $tdf = $db.TableDefs('T')
                if ({{bracketedForeign}} -like '[[]Other[]]*') {
                    $rejected = $tdf.CreateField('Rejected', 4)
                    $rejected.Properties('Expression').Value = {{bracketedForeign}}
                    $refused = $false
                    try { $tdf.Fields.Append($rejected) }
                    catch [System.Runtime.InteropServices.COMException] { $refused = $true }
                    if (!$refused) { throw 'DAO accepted a bracketed foreign-table qualifier.' }
                    $tdf.Fields.Refresh()
                    if ($tdf.Fields.Count -ne 1) { throw 'DAO retained the rejected field.' }
                }
                $field = $tdf.CreateField('Calculated', 4)
                $field.Properties('Expression').Value = {{expression}}
                $tdf.Fields.Append($field)
                $db.Execute('INSERT INTO [T] ([Price]) VALUES (4)', 128)
                $rs = $db.OpenRecordset('SELECT [Calculated] FROM [T]')
                try { Write-Output ('BEFORE=' + [string]$rs.Fields('Calculated').Value) }
                finally { $rs.Close() }
                $tdf.Name = 'Renamed T'
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        Assert.Contains("BEFORE=8", created.StdOut, StringComparison.Ordinal);

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("Renamed T", [3, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.UpdateRowsAsync("Renamed T", "Price", 4, new Dictionary<string, object?> { ["Price"] = 5 }, TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync("Renamed T", "Price", "Cost", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Renamed T", [2, DBNull.Value], TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult updated = session.RunDaoDatabaseScriptThenCompact(
            """
            $db.Execute('UPDATE [Renamed T] SET [Cost]=6 WHERE [Cost]=5', 128)
            $rs = $db.OpenRecordset('SELECT [Cost],[Calculated] FROM [Renamed T] ORDER BY [Cost]')
            try {
                while (!$rs.EOF) {
                    Write-Output ('ROW=' + [string]$rs.Fields('Cost').Value + ':' + [string]$rs.Fields('Calculated').Value)
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            $rs = $db.OpenRecordset('SELECT [Price] FROM [Other]')
            try { Write-Output ('OTHER=' + [string]$rs.Fields('Price').Value) }
            finally { $rs.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(updated.ExitCode == 0, $"DAO failed: {updated.StdOut}\n{updated.StdErr}");
        Assert.Contains("ROW=2:4", updated.StdOut, StringComparison.Ordinal);
        Assert.Contains("ROW=3:6", updated.StdOut, StringComparison.Ordinal);
        Assert.Contains("ROW=6:12", updated.StdOut, StringComparison.Ordinal);
        Assert.Contains("OTHER=99", updated.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Renamed T", TestContext.Current.CancellationToken);
        int separator = reference.IndexOfAny(['.', '!']);
        Assert.Equal(reference[..(separator + 1)] + "[Cost] * 2", columns[1].CalculationExpression?.TrimEnd('\0'));
        var values = new Dictionary<int, int>();
        await foreach (object[] row in reader.Rows("Renamed T", cancellationToken: TestContext.Current.CancellationToken))
        {
            values.Add(Assert.IsType<int>(row[0]), Assert.IsType<int>(row[1]));
        }

        Assert.Equal(3, values.Count);
        Assert.Equal(4, values[2]);
        Assert.Equal(6, values[3]);
        Assert.Equal(12, values[6]);
    }

    private static async Task<byte[]> AssertWriterPreservesStaleCachesAsync(byte[] image, int? expectedExisting = null)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("Renamed T", [2, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.UpdateRowsAsync(
                "Renamed T", "Price", 4, new Dictionary<string, object?> { ["Price"] = 5 }, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("Renamed T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.True(Assert.IsType<int>(row[0]) is 2 or 5);
            if ((int)row[0] == 5 && expectedExisting is int cached)
            {
                Assert.Equal(cached, row[1]);
            }
            else
            {
                Assert.Null(Assert.IsType<CalculatedError>(row[1]).NativeCode);
            }

            count++;
        }

        Assert.Equal(2, count);
        return stream.ToArray();
    }
}
