namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks native relationship cleanup when deleting a linked table.</summary>
public sealed class LinkedTableRelationshipDaoTests
{
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(32)]
    [InlineData(64)]
    public Task Dao36_DropLinkRemovesUnenforcedRelationship(int version)
        => AssertNativeAsync(version, useDao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(64)]
    [InlineData(128)]
    public Task Dao120_DropLinkRemovesUnenforcedRelationship(int version)
        => AssertNativeAsync(version, useDao36: false);

    private static async Task AssertNativeAsync(int version, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.LinkedDropRelationships", databaseExtension: version == 128 ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CreateDatabasePath("linked-source"));
        string host = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string script = $$"""
            $source = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            $source.Execute('CREATE TABLE [T] ([Id] LONG CONSTRAINT [PK] PRIMARY KEY)', 128)
            $source.Close()
            $db = $engine.CreateDatabase({{host}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [Local] ([Id] LONG CONSTRAINT [PK] PRIMARY KEY)', 128)
                $link = $db.CreateTableDef('Linked')
                $link.Connect = ';DATABASE=' + {{source}}
                $link.SourceTableName = 'T'
                $db.TableDefs.Append($link)
                $relation = $db.CreateRelation('RelatedLink', 'Local', 'Linked', 2)
                $field = $relation.CreateField('Id')
                $field.ForeignName = 'Id'
                $relation.Fields.Append($field)
                $db.Relations.Append($relation)
                Write-Output ('BEFORE=' + $db.Relations('RelatedLink').Name)
                $db.TableDefs.Delete('Linked')
                $db.Relations.Refresh()
                $found = $false
                foreach ($relation in $db.Relations) { if ($relation.Name -eq 'RelatedLink') { $found = $true } }
                Write-Output ('AFTER=' + $found)
            } finally { $db.Close() }
            """;
        string output;
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(1));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
            output = result.StandardOutput;
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(1));
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
            output = result.StdOut;
        }

        Assert.Contains("BEFORE=RelatedLink", output, StringComparison.Ordinal);
        Assert.Contains("AFTER=False", output, StringComparison.Ordinal);
    }
}
