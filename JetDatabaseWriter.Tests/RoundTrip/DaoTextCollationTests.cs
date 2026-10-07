namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Compares persisted text rules with Microsoft's Access engine.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoTextCollationTests
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task TableValidationRule_RenameAndSetterRemainEffectiveInDao()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TableRules");
        string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{literal}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [Rules] ([Id] LONG, [Other] LONG)')
                $tdf = $db.TableDefs('Rules')
                $tdf.ValidationRule = '[Id]>0'
                $tdf.ValidationText = 'positive'
                $tdf = $null
                $db.Execute('INSERT INTO [Rules] ([Id], [Other]) VALUES (1, 0)')
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(new TableValidationRule("[Id]>0", "positive"), await reader.GetTableValidationRuleAsync("Rules", TestContext.Current.CancellationToken));
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("Rules", [-1, 0], TestContext.Current.CancellationToken));
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id]>0", "positive identifier"), TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync("Rules", "Id", "Value", TestContext.Current.CancellationToken);
            _ = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("Rules", "Value", TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $tdf = $db.TableDefs('Rules')
            Write-Output "RULE=$($tdf.ValidationRule)"
            Write-Output "TEXT=$($tdf.ValidationText)"
            $tdf = $null
            try { $db.Execute('INSERT INTO [Rules] ([Value], [Other]) VALUES (-1, 0)', 128); Write-Output 'INVALID=ACCEPTED' }
            catch { Write-Output 'INVALID=REFUSED' }
            $db.Execute('INSERT INTO [Rules] ([Value], [Other]) VALUES (2, 0)', 128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.CompactedPath), $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("RULE=[Value]>0", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("TEXT=positive identifier", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("INVALID=REFUSED", result.StdOut, StringComparison.Ordinal);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task TextValidationRule_AccentPunctuationAndTrailingSpace_MatchDao()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TextCollation");
        string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{literal}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $values = @('é', 'a!', 'a', 'a ')
                $bounds = @('z', 'a?', 'a ', 'a')
                for ($i = 0; $i -lt $values.Count; $i++) {
                    $table = "TextRules$i"
                    $db.Execute("CREATE TABLE [$table] ([Id] LONG, [Value] TEXT(100))")
                    $db.TableDefs.Refresh()
                    $tdf = $db.TableDefs($table)
                    $tdf.Fields('Value').ValidationRule = '< "' + [string]$bounds[$i] + '"'
                    $tdf = $null
                    $rs = $db.OpenRecordset($table, 2)
                    try {
                        $rs.AddNew()
                        $rs.Fields('Id').Value = $i
                        $rs.Fields('Value').Value = [string]$values[$i]
                        try { $rs.Update(); Write-Output "ACCEPT_$i=True" }
                        catch { Write-Output "ACCEPT_$i=False"; $rs.CancelUpdate() }
                    } finally { $rs.Close() }
                    $db.Execute("DELETE FROM [$table]")
                }
                $tdf = $null
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.SourcePath), $"DAO failed: {result.StdOut}\n{result.StdErr}");

        string[][] pairs = [["é", "z"], ["a!", "a?"], ["a", "a "], ["a ", "a"]];
        await using AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken);
        for (int index = 0; index < pairs.Length; index++)
        {
            bool accepted;
            try
            {
                await writer.InsertRowAsync($"TextRules{index}", [index, pairs[index][0]], TestContext.Current.CancellationToken);
                accepted = true;
            }
            catch (ArgumentException)
            {
                accepted = false;
            }

            Assert.Contains($"ACCEPT_{index}={accepted}", result.StdOut, StringComparison.Ordinal);
        }
    }
}
