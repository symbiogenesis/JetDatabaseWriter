namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>DAO verifies calculated-field admissibility and safe Byte overflow refusal.</summary>
public sealed class CalculatedConversionDaoTests
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoConversions_RefusesUnavailableFunctionsAndCompactsWriterUpdatedAlternatives()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.Tests.CalculatedConversions");
        const string authoringScript = """
            $db.Execute('CREATE TABLE [T] ([Id] LONG, [Flag] BIT, [Amount] DOUBLE)',128)
            $db.TableDefs.Refresh()
            $tdf = $db.TableDefs('T')
            $expressions = @('CByte([Flag])','CInt([Amount])','CDate("6:00 PM")','TimeValue("6:00 PM")','DateValue("6:00 PM")','CBool([Amount])','CLng([Amount])','CSng([Amount])','CCur([Amount])','CDec([Amount])','CStr([Amount])')
            foreach ($expression in $expressions) {
                $field = $tdf.CreateField('Rejected',10)
                $field.Expression = $expression
                $rejected = $false
                try { $tdf.Fields.Append($field) }
                catch {
                    $expected = if ($expression.StartsWith('CDec')) { 3950 } else { 3859 }
                    foreach ($daoError in $engine.Errors) {
                        if ($daoError.Number -eq $expected) { $rejected = $true }
                    }
                }
                if (!$rejected -or $tdf.Fields.Count -ne 3) { throw ('Unexpected acceptance or field mutation: ' + $expression) }
                Write-Output ('REFUSED=' + $expression)
            }
            foreach ($definition in @(@('Numeric',7,'CDbl([Amount])'),@('Due',8,'DateSerial(2020,1,31)'),@('At',8,'TimeSerial(18,0,0)'),@('LiteralAt',8,'#6:00 PM#'))) {
                $field = $tdf.CreateField($definition[0],[int]$definition[1])
                $field.Expression = $definition[2]
                $tdf.Fields.Append($field)
            }
            $db.Execute('INSERT INTO [T] ([Id],[Flag],[Amount]) VALUES (1,False,4)',128)
            """;
        AccessRoundTripEnvironment.CompactResult authored = session.RunDaoDatabaseScript(session.SourcePath, authoringScript, TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, $"DAO authoring failed: {authored.StdOut}\n{authored.StdErr}");
        Assert.Contains("REFUSED=CByte([Flag])", authored.StdOut, StringComparison.Ordinal);
        Assert.Contains("REFUSED=CStr([Amount])", authored.StdOut, StringComparison.Ordinal);
        await AssertAlternativeValuesAsync(session.SourcePath, 4d);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Amount"] = 8d }, TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult native = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs = $db.OpenRecordset('SELECT [Numeric] FROM [T]')
            try {
                if ($rs.Fields('Numeric').Value -ne 8) { throw 'DAO did not read the writer updated calculated value.' }
                Write-Output 'WRITER_RESULT=8'
            } finally { $rs.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(native.ExitCode == 0, $"DAO compact failed: {native.StdOut}\n{native.StdErr}");
        Assert.Contains("WRITER_RESULT=8", native.StdOut, StringComparison.Ordinal);
        await AssertAlternativeValuesAsync(session.CompactedPath, 8d);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoByteBoolean_OverflowIsDistinctFromNullAndWriterRefusesAtomically()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.Tests.CalculatedByte");
        const string authoringScript = """
            $db.Execute('CREATE TABLE [T] ([Id] LONG, [Flag] BIT, [Amount] DOUBLE)',128)
            $db.TableDefs.Refresh()
            $tdf = $db.TableDefs('T')
            foreach ($name in @('ByteFlag','ByteAmount')) {
                $field = $tdf.CreateField($name,2)
                $field.Expression = if ($name -eq 'ByteFlag') { '[Flag]' } else { '[Amount]' }
                $tdf.Fields.Append($field)
            }
            $db.Execute('INSERT INTO [T] ([Id],[Flag],[Amount]) VALUES (1,True,-1)',128)
            $db.Execute('INSERT INTO [T] ([Id],[Flag],[Amount]) VALUES (2,False,Null)',128)
            $rs = $db.OpenRecordset('SELECT [Id],[ByteFlag],[ByteAmount],IsNull([ByteAmount]) AS [NullTest] FROM [T] ORDER BY [Id]')
            try {
                if ($null -ne $rs.Fields('ByteFlag').Value -or $null -ne $rs.Fields('ByteAmount').Value -or $null -ne $rs.Fields('NullTest').Value) { throw 'The overflow must expose a missing COM result.' }
                $rs.MoveNext()
                if ($rs.Fields('ByteFlag').Value -ne 0 -or $rs.Fields('ByteAmount').Value -isnot [DBNull] -or $rs.Fields('NullTest').Value -ne -1) { throw 'Ordinary Null must remain distinguishable from overflow.' }
                Write-Output 'BYTE_OVERFLOW_DISTINCT_FROM_NULL=OK'
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult native = session.RunDaoDatabaseScript(session.SourcePath, authoringScript, TimeSpan.FromMinutes(1));
        Assert.True(native.ExitCode == 0, $"DAO failed: {native.StdOut}\n{native.StdErr}");
        Assert.Contains("BYTE_OVERFLOW_DISTINCT_FROM_NULL=OK", native.StdOut, StringComparison.Ordinal);
        byte[] before = await System.IO.File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(
                "T", "Id", 1, new Dictionary<string, object?> { ["Id"] = 3 }, TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(
                "T", "Id", 2, new Dictionary<string, object?> { ["Flag"] = true }, TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync(
                "T", [3, true, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken));
        }

        Assert.Equal(before, await System.IO.File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken));
    }

    private static async Task AssertAlternativeValuesAsync(string path, double numeric)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);
        Assert.Equal("CDbl([Amount])", Assert.Single(columns, column => column.Name == "Numeric").CalculationExpression);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(numeric, row[3]);
            Assert.Equal(new DateTime(2020, 1, 31), row[4]);
            Assert.Equal(new DateTime(1899, 12, 30, 18, 0, 0), row[5]);
            Assert.Equal(row[5], row[6]);
            count++;
        }

        Assert.Equal(1, count);
    }
}
