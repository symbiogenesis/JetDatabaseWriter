namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native engines accept index removal on native-authored populated tables.</summary>
public sealed class DropIndexDaoTests
{
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NativeTable_IndexRemovalSurvivesReadWriteAndCompact(DatabaseFormat format)
        => await VerifyNativeIndexRemovalAsync(format);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Jet3NativeTable_IndexRemovalSurvivesReadWriteAndCompact()
        => await VerifyNativeIndexRemovalAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyNativeIndexRemovalAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.DropIndex", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        RunNativeScript(
            format,
            session,
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [Items] ([Id] LONG, [Value] LONG)', 128)
                $db.Execute('CREATE UNIQUE INDEX [PrimaryKey] ON [Items] ([Id]) WITH PRIMARY', 128)
                $db.Execute('CREATE UNIQUE INDEX [UniqueValue] ON [Items] ([Value])', 128)
                $db.Execute('CREATE INDEX [Keep] ON [Items] ([Value] DESC, [Id])', 128)
                $db.Execute('INSERT INTO [Items] ([Id],[Value]) VALUES (1,7)', 128)
                $db.Execute('INSERT INTO [Items] ([Id],[Value]) VALUES (2,8)', 128)
                $db.Execute('CREATE TABLE [Oracle] ([Id] LONG)', 128)
                $db.Execute('CREATE UNIQUE INDEX [PrimaryKey] ON [Oracle] ([Id]) WITH PRIMARY', 128)
                $db.Execute('INSERT INTO [Oracle] ([Id]) VALUES (1)', 128)
                $db.TableDefs('Oracle').Indexes.Delete('PrimaryKey')
                $db.Execute('INSERT INTO [Oracle] ([Id]) VALUES (1)', 128)
                if ($db.TableDefs('Oracle').Indexes.Count -ne 0) { throw 'Native primary-key deletion failed.' }
            } finally { $db.Close() }
            Write-Output 'NATIVE_DROP_CONTROL_OK'
            """,
            "NATIVE_DROP_CONTROL_OK");

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.DropIndexAsync("Items", "UniqueValue", TestContext.Current.CancellationToken);
            await writer.DropIndexAsync("Items", "PrimaryKey", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, 7], TestContext.Current.CancellationToken);
        }

        RunNativeScript(
            format,
            session,
            $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $false)
            try {
                $indexes = $db.TableDefs('Items').Indexes
                if ($indexes.Count -ne 1 -or $indexes.Item(0).Name -cne 'Keep') { throw 'Wrong surviving indexes.' }
                $db.Execute('INSERT INTO [Items] ([Id],[Value]) VALUES (1,7)', 128)
                $db.Execute('UPDATE [Items] SET [Value]=9 WHERE [Id]=2', 128)
                $rows = $db.OpenRecordset('SELECT COUNT(*) AS N FROM [Items]')
                try { if ([int]$rows.Fields('N').Value -ne 4) { throw 'Wrong surviving rows.' } } finally { $rows.Close() }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}})
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                if ($db.TableDefs('Items').Indexes.Count -ne 1) { throw 'Compact changed the index inventory.' }
                $rows = $db.OpenRecordset('SELECT COUNT(*) AS N FROM [Items]')
                try { if ([int]$rows.Fields('N').Value -ne 4) { throw 'Compact changed the rows.' } } finally { $rows.Close() }
            } finally { $db.Close() }
            Write-Output 'NATIVE_DROP_COMPACT_OK'
            """,
            "NATIVE_DROP_COMPACT_OK");

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new() { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.Equal(4, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
        Assert.Equal("Keep", Assert.Single(await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken)).Name);
        Assert.Equal([false, true], Assert.Single(await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken)).Columns.Select(column => column.IsAscending));
    }

    private static void RunNativeScript(DatabaseFormat format, AccessRoundTripSession session, string script, string marker)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO3.6 failed: {result.StandardOutput}\n{result.StandardError}");
            Assert.Contains(marker, result.StandardOutput, StringComparison.Ordinal);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 failed: {result.StdOut}\n{result.StdErr}");
            Assert.Contains(marker, result.StdOut, StringComparison.Ordinal);
        }
    }
}
