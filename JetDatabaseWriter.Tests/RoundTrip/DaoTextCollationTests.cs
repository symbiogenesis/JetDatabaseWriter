namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Threading.Tasks;
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
    public async Task TextValidationRule_AccentPunctuationAndTrailingSpace_MatchDao()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TextCollation");
        string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{literal}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $pairs = @(@('é','z'), @('a!','a?'), @('a','a '), @('a ','a'))
                for ($i = 0; $i -lt $pairs.Count; $i++) {
                    $table = "TextRules$i"
                    $db.Execute("CREATE TABLE [$table] ([Id] LONG, [Value] TEXT(100))")
                    $tdf = $db.TableDefs($table)
                    $tdf.Fields('Value').ValidationRule = '< "' + $pairs[$i][1] + '"'
                    $tdf = $null
                    $rs = $db.OpenRecordset($table, 2)
                    try {
                        $rs.AddNew()
                        $rs.Fields('Id').Value = $i
                        $rs.Fields('Value').Value = $pairs[$i][0]
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
